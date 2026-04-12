#!/usr/bin/env python3
"""
Scrape bond amounts from the AZ Registrar of Contractors public website.

Bond amounts are mandated by law to scale with the contractor's annual
gross volume. The ROC publishes the bond on every contractor's detail
page — it's public record, free, and directly tied to reported revenue.

For CR-39 (Dual) contractors, the bond schedule has 6 tiers that map
to revenue bands from under $150K to over $10M. The combined amounts
we see on the detail page include the contractor bond + recovery fund
bond, so the raw dollar figure needs a lookup table to map back to
the volume tier.

Uses Playwright (headless Chromium) because the ROC site is a
Salesforce Aura app that renders client-side — no usable API.

Cache: data/signals_raw/roc_bonds/{place_id}.json
       (keyed by place_id for consistency with rest of pipeline)

Usage:
  python pipeline/18_roc_bond_scraper.py --limit 3     # smoke test
  python pipeline/18_roc_bond_scraper.py                # full pool (70)
  python pipeline/18_roc_bond_scraper.py --force        # bypass cache
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
POOL_CSV = ROOT / "data" / "03_hidden_gems" / "complete.csv"
CACHE_DIR = ROOT / "data" / "signals_raw" / "roc_bonds"

ROC_SEARCH_URL = "https://azroc.my.site.com/AZRoc/s/contractor-search"

# Seconds to wait between contractor scrapes. The ROC site is a
# government Salesforce instance — be polite.
SLEEP_BETWEEN = 2.0

# Page-render wait. Salesforce Aura needs time to hydrate.
RENDER_WAIT_MS = 4000


def scrape_bond_for_license(browser, license_no: int) -> dict:
    """
    Scrape the ROC detail page for a single license number.

    Returns a dict with:
      - license_no, bond_amount, bond_type, bond_company, bond_status,
        bond_effective_date, bond_number, raw_text (for audit)
      - error (str) if something went wrong
    """
    page = browser.new_page()
    result = {"license_no": license_no}

    try:
        # Step 1: search by license number
        page.goto(ROC_SEARCH_URL, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(2000)

        search_input = page.locator(
            'input[type="search"], input[placeholder*="search"]'
        ).first
        search_input.fill(str(license_no))
        page.locator('button:has-text("Search")').first.click()
        page.wait_for_timeout(RENDER_WAIT_MS)

        # Step 2: extract Salesforce licenseId from search results.
        # Salesforce Aura is slow — if the first wait didn't produce
        # results, give it one more shot with a longer timeout.
        html = page.content()
        ids = re.findall(r"licenseId=([a-zA-Z0-9]+)", html)
        if not ids:
            page.wait_for_timeout(4000)
            html = page.content()
            ids = re.findall(r"licenseId=([a-zA-Z0-9]+)", html)
        if not ids:
            result["error"] = "no licenseId found in search results"
            return result

        # If multiple licenseIds found (contractor has multiple licenses),
        # pick the one whose ROC number matches our license_no. In the
        # HTML, each table row pairs a licenseId link with a nearby
        # "ROC NNNNNN" reference. We scan for licenseId links and check
        # the ~300 chars around each for our target ROC number.
        target_roc = str(license_no).zfill(6)
        lid = ids[0]  # default fallback
        for m in re.finditer(r"licenseId=([a-zA-Z0-9]+)", html):
            candidate_lid = m.group(1)
            start = max(0, m.start() - 300)
            end = min(len(html), m.end() + 300)
            context = html[start:end]
            if target_roc in context:
                lid = candidate_lid
                break

        # Step 3: load detail page
        detail_url = f"{ROC_SEARCH_URL}?licenseId={lid}"
        page.goto(detail_url, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(RENDER_WAIT_MS)

        text = page.inner_text("body")
        result["raw_text_sample"] = text[:3000]

        # Step 4: extract bond fields
        def extract(pattern, text, group=1):
            m = re.search(pattern, text)
            return m.group(group).strip() if m else None

        amount_str = extract(r"Amount\s*:?\s*\$\s*([\d,]+)", text)
        if amount_str:
            result["bond_amount"] = int(amount_str.replace(",", ""))
        else:
            result["bond_amount"] = None
            result["error"] = "bond amount not found on detail page"

        result["bond_type"] = extract(r"Bond Type\s*:?\s*(\w+)", text)
        result["bond_company"] = extract(
            r"Bond Company\s*:?\s*(.+?)(?:\n|Paid)", text
        )
        result["bond_status"] = extract(
            r"Status\s*:?\s*(Active|Inactive|Cancelled)", text
        )
        result["bond_effective_date"] = extract(
            r"Effective Date\s*:?\s*(\d{4}-\d{2}-\d{2})", text
        )
        result["bond_number"] = extract(
            r"Bond Number\s*:?\s*([A-Z0-9]+)", text
        )

        # Also grab the class from the detail page as a cross-check
        result["detail_class"] = extract(
            r"Class & Description\s*\n?\s*(.*?)(?:\n|Entity)", text
        )

    except Exception as e:
        result["error"] = f"{type(e).__name__}: {str(e)[:200]}"
    finally:
        page.close()

    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None,
                        help="Smoke test on first N contractors (by pre-score rank)")
    parser.add_argument("--license", type=int, default=None,
                        help="Scrape a single license number")
    parser.add_argument("--force", action="store_true",
                        help="Bypass cache and re-scrape")
    args = parser.parse_args()

    if not POOL_CSV.exists():
        sys.exit(f"missing {POOL_CSV}")

    df = pd.read_csv(POOL_CSV)
    pool = df.sort_values("rank") if "rank" in df.columns else df
    if args.license is not None:
        pool = pool[pool["license_no"] == args.license]
    elif args.limit is not None:
        pool = pool.head(args.limit)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"ROC Bond Scraper")
    print(f"Contractors to process: {len(pool)}")
    print(f"Cache dir: {CACHE_DIR.relative_to(ROOT)}")
    print()

    # Import playwright here so the module can be imported without it
    # installed (for inspection, etc.)
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        sys.exit(
            "playwright not installed. Run:\n"
            "  pip install playwright && playwright install chromium"
        )

    scraped = 0
    cached = 0
    errors = 0
    results: list[dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        for i, (_, row) in enumerate(pool.iterrows(), 1):
            license_no = int(row["license_no"])
            place_id = str(row.get("place_id") or "")
            biz = str(row.get("business_name") or "")
            rank = int(row["rank"]) if "rank" in row and pd.notna(row.get("rank")) else i

            cache_path = CACHE_DIR / f"{place_id}.json"

            # Check cache
            if cache_path.exists() and not args.force:
                try:
                    data = json.loads(cache_path.read_text())
                    if data.get("bond_amount") is not None:
                        cached += 1
                        amt = data["bond_amount"]
                        print(f"  [{i:>2}/{len(pool)}] rank={rank:<3} {biz[:38]:<40} ${amt:>7,}  (cached)")
                        results.append(data)
                        continue
                except (json.JSONDecodeError, OSError):
                    pass

            # Scrape
            result = scrape_bond_for_license(browser, license_no)
            result["place_id"] = place_id
            result["business_name"] = biz
            result["fetched_at"] = datetime.now(timezone.utc).isoformat()

            # Cache
            # Strip the raw_text_sample before caching to save space,
            # but keep it if there was an error (for debugging)
            cache_data = dict(result)
            if not result.get("error"):
                cache_data.pop("raw_text_sample", None)
            cache_path.write_text(json.dumps(cache_data, indent=2))

            amt = result.get("bond_amount")
            if amt is not None:
                scraped += 1
                print(f"  [{i:>2}/{len(pool)}] rank={rank:<3} {biz[:38]:<40} ${amt:>7,}")
            else:
                errors += 1
                err = result.get("error", "unknown")
                print(f"  [{i:>2}/{len(pool)}] rank={rank:<3} {biz[:38]:<40} ERROR: {err[:60]}")

            results.append(result)
            time.sleep(SLEEP_BETWEEN)

        browser.close()

    # Summary
    print()
    print("=" * 72)
    print("Summary")
    print("=" * 72)
    print(f"Contractors processed: {len(pool)}")
    print(f"Scraped this run:      {scraped}")
    print(f"Cache hits:            {cached}")
    print(f"Errors:                {errors}")

    # Bond amount distribution
    amounts = [r["bond_amount"] for r in results if r.get("bond_amount")]
    if amounts:
        print()
        print("Bond amount distribution:")
        from collections import Counter
        dist = Counter(amounts)
        for amt, count in sorted(dist.items()):
            print(f"  ${amt:>8,}  ×{count}")
        print()
        print(f"  min: ${min(amounts):,}  max: ${max(amounts):,}  "
              f"median: ${sorted(amounts)[len(amounts)//2]:,}")

    print()
    print(f"Cache dir: {CACHE_DIR.relative_to(ROOT)}")
    print(f"Next step: run pipeline/11_scoring.py — it reads bond amounts "
          f"from the cache to compute revenue_band")


if __name__ == "__main__":
    main()
