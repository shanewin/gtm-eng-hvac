#!/usr/bin/env python3
"""
Validation signal #1: FSM + site tooling detection on contractor websites.

Uses Playwright (headless Chromium) to render pages with full JavaScript
execution, then crawls every internal link from the homepage (up to
MAX_PAGES). This catches FSM widgets that load via JS (ServiceTitan,
Housecall Pro iframes, etc.) and booking pages at whatever URL the
contractor chose — no more guessing paths.

Also runs webanalyze on the homepage for broader tech fingerprinting
(CMS, site builder, page builder, form builders).

Usage:
  python pipeline/03_fsm_detection.py --limit 20   # smoke test
  python pipeline/03_fsm_detection.py              # full run (all 356)
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "data" / "01_contractors" / "tier_1_clean.csv"
ENRICH_DIR = ROOT / "data" / "02_enrichment"
OUT = ENRICH_DIR / "fsm_detection_sample.csv"
OUT_FULL = ENRICH_DIR / "fsm_detection.csv"
ERR_OUT = ENRICH_DIR / "fsm_detection_errors.csv"
SNIPPETS_OUT = ENRICH_DIR / "fsm_detection_phone_only_snippets.txt"

WEBANALYZE_BIN = ROOT / "tools" / "webanalyze" / "webanalyze"
WEBANALYZE_APPS = ROOT / "tools" / "webanalyze" / "technologies.json"

# How many internal pages to visit per contractor. 15 covers a
# typical small HVAC site (~8-12 pages) with room for deeper sites.
MAX_PAGES = 15

# Playwright page-load and render timeouts (ms)
NAV_TIMEOUT = 15000
RENDER_WAIT = 2000

SLEEP_BETWEEN_CONTRACTORS = 0.8

SOCIAL_HOSTS = {
    "facebook.com", "m.facebook.com", "instagram.com", "twitter.com",
    "x.com", "yelp.com", "nextdoor.com", "linkedin.com",
}

# Hosts that are definitely not part of the contractor's own site
EXTERNAL_SKIP_HOSTS = SOCIAL_HOSTS | {
    "google.com", "goo.gl", "maps.google.com", "youtube.com",
    "pinterest.com", "tiktok.com", "bbb.org", "angi.com",
    "homeadvisor.com", "thumbtack.com", "angieslist.com",
    "apple.com", "play.google.com",
}

# ---- FSM + form-builder regex patterns ----
FSM_PATTERNS: dict[str, list[str]] = {
    "ServiceTitan": [
        r"servicetitan\.com",
        r"servicetitan\.io",
        r"powered\s+by\s+servicetitan",
        r"schedule\.servicetitan",
        r"go\.servicetitan",
    ],
    "Jobber": [
        r"getjobber\.com",
        r"clienthub\.getjobber",
        r"secure\.getjobber",
    ],
    "Housecall Pro": [
        r"housecallpro\.com",
        r"housecall\.pro",
        r"book\.housecallpro",
        r"bookcall\.me",
    ],
    "BuildOps": [r"buildops\.com"],
    "FieldEdge": [r"fieldedge\.com", r"my\.fieldedge"],
    "ServiceFusion": [r"servicefusion\.com"],
    "Workiz": [r"workiz\.com", r"widget\.workiz"],
    "Kickserv": [r"kickserv\.com"],
    "mHelpDesk": [r"mhelpdesk\.com"],
    "Service Autopilot": [r"serviceautopilot\.com"],
    "FieldPulse": [r"fieldpulse\.com"],
    "GorillaDesk": [r"gorilladesk\.com"],
    "Tradify": [r"tradify\.com"],
    "Synchroteam": [r"synchroteam\.com"],
    "WorkWave": [r"workwave\.com"],
    "Neighborly": [
        r"\bneighborly\.com",
        r"aireserv\.com",
        r"onehourheatandair\.com",
        r"benjaminfranklinplumbing\.com",
        r"mistersparky\.com",
        r"mrrooter\.com",
        r"mrelectric\.com",
        r"team-neighborly",
        r"powered\s+by\s+neighborly",
    ],
}

FORM_BUILDER_PATTERNS: dict[str, list[str]] = {
    "JotForm": [r"jotform\.com", r"jotformeu\.com", r"form\.jotform"],
    "Wufoo": [r"wufoo\.com"],
    "Typeform": [r"typeform\.com", r"embed\.typeform"],
    "Formspree": [r"formspree\.io"],
    "Calendly": [r"calendly\.com"],
    "Acuity Scheduling": [r"acuityscheduling\.com", r"app\.acuityscheduling"],
}

# ---- webanalyze category mapping ----
SITE_BUILDER_NAMES = {
    "Wix", "GoDaddy Website Builder", "Squarespace", "Weebly",
    "Webflow", "Jimdo", "Site123", "Zyro", "Strikingly",
    "WebsiteBuilder", "Duda", "Tilda",
}
CMS_NAMES = {
    "WordPress", "Drupal", "Joomla", "Ghost", "HubSpot CMS", "Craft CMS",
    "Shopify", "Magento", "PrestaShop", "BigCommerce", "OpenCart",
    "concrete5", "TYPO3",
}
PAGE_BUILDER_NAMES = {
    "Divi", "Elementor", "WPBakery Page Builder", "Beaver Builder",
    "Oxygen", "Brizy", "Bricks", "Thrive Architect",
}
FORM_BUILDER_NAMES_WA = {
    "Gravity Forms", "Typeform", "Jotform", "Wufoo", "WPForms",
    "Ninja Forms", "Contact Form 7", "Formidable Forms", "Formstack",
    "HubSpot Form", "Marketo Forms", "JotForm",
}

PHONE_RE = re.compile(
    r"(?:\(\s*\d{3}\s*\)\s*\d{3}[-.\s]?\d{4}"
    r"|\b\d{3}[-.\s]\d{3}[-.\s]\d{4}\b"
    r"|tel:\+?1?\d{10})",
    re.IGNORECASE,
)

COMPILED_FSM = {
    v: [re.compile(p, re.IGNORECASE) for p in patterns]
    for v, patterns in FSM_PATTERNS.items()
}
COMPILED_FORM = {
    v: [re.compile(p, re.IGNORECASE) for p in patterns]
    for v, patterns in FORM_BUILDER_PATTERNS.items()
}

DEAD_ERROR_RE = re.compile(
    r"NameResolutionError|getaddrinfo|Name or service not known"
    r"|net::ERR_NAME_NOT_RESOLVED|NS_ERROR",
    re.IGNORECASE,
)


def strip_query(url: str) -> str:
    p = urlparse(url)
    return urlunparse(p._replace(query="", fragment=""))


def base_host(url: str) -> str:
    return (urlparse(url).hostname or "").lower().removeprefix("www.")


def is_same_site(url: str, site_host: str) -> bool:
    """True if a URL belongs to the same site (same root domain)."""
    host = base_host(url)
    if not host:
        return False
    # Handle subdomains: www.example.com and blog.example.com are same-site
    return host == site_host or host.endswith("." + site_host)


def normalize_url(url: str) -> str:
    """Normalize for dedup: strip query, fragment, trailing slash."""
    p = urlparse(url)
    path = p.path.rstrip("/") or "/"
    return urlunparse(p._replace(path=path, query="", fragment=""))


def detect_first_match(
    text: str, compiled: dict[str, list[re.Pattern[str]]]
) -> tuple[str | None, str | None]:
    for vendor, patterns in compiled.items():
        for rx in patterns:
            m = rx.search(text)
            if m:
                return vendor, m.group(0)
    return None, None


def phone_snippet(text: str, span: int = 160) -> str | None:
    m = PHONE_RE.search(text)
    if not m:
        return None
    start = max(0, m.start() - span // 2)
    end = min(len(text), m.end() + span // 2)
    return re.sub(r"\s+", " ", text[start:end]).strip()[:300]


def classify_total_failure(error: str | None) -> str:
    if error and DEAD_ERROR_RE.search(error):
        return "site_dead"
    return "all_pages_failed"


def crawl_site(browser, website: str) -> dict:
    """
    Crawl a contractor's website using Playwright.

    1. Load the homepage in a real browser
    2. Collect every internal link on the page
    3. Visit each internal page (up to MAX_PAGES total)
    4. Return all rendered page contents for scanning

    Returns {
        "pages": [(url, rendered_html, rendered_text), ...],
        "error": str | None,
        "ssl_ok": True,
    }
    """
    site_host = base_host(website)
    if not site_host:
        return {"pages": [], "error": "invalid URL", "ssl_ok": True}

    result = {"pages": [], "error": None, "ssl_ok": True}
    visited: set[str] = set()
    to_visit: list[str] = [strip_query(website)]

    page = browser.new_page()
    page.set_default_navigation_timeout(NAV_TIMEOUT)

    try:
        while to_visit and len(result["pages"]) < MAX_PAGES:
            url = to_visit.pop(0)
            norm = normalize_url(url)
            if norm in visited:
                continue
            visited.add(norm)

            try:
                # Use 'domcontentloaded' instead of 'networkidle' —
                # Wix, Squarespace, and other SPA frameworks keep
                # loading analytics/tracking resources forever, which
                # causes 'networkidle' to timeout. The DOM is ready
                # well before the network goes quiet, and we add an
                # explicit wait after for JS widgets to render.
                resp = page.goto(url, wait_until="domcontentloaded")
                if resp is None:
                    continue
                if resp.status >= 400:
                    if not result["pages"]:
                        result["error"] = f"HTTP {resp.status}"
                    continue

                page.wait_for_timeout(RENDER_WAIT)

                # Get rendered HTML (includes JS-injected content) and
                # visible text (for phone detection)
                html = page.content()
                text = page.inner_text("body")
                final_url = page.url

                result["pages"].append((final_url, html, text))

                # Collect internal links for crawling. Only on the first
                # few pages to avoid spending forever on deep sites.
                if len(result["pages"]) <= 5:
                    links = page.eval_on_selector_all(
                        "a[href]",
                        "els => els.map(e => e.href).filter(h => h && h.startsWith('http'))"
                    )
                    for link in links:
                        link_norm = normalize_url(link)
                        link_host = base_host(link)
                        if (
                            link_norm not in visited
                            and is_same_site(link, site_host)
                            and link_host not in EXTERNAL_SKIP_HOSTS
                            and not link.lower().endswith((".pdf", ".jpg", ".png", ".gif", ".svg", ".css", ".js"))
                        ):
                            to_visit.append(link)

            except Exception as e:
                err_str = str(e)[:200]
                if not result["pages"] and result["error"] is None:
                    result["error"] = err_str
                # SSL errors in Playwright manifest as navigation errors
                if "SSL" in err_str or "ERR_CERT" in err_str:
                    result["ssl_ok"] = False
                continue

    finally:
        page.close()

    return result


# ---- webanalyze integration (unchanged) ----

def run_webanalyze(urls: list[str]) -> dict[str, list[dict]]:
    if not WEBANALYZE_BIN.exists() or not WEBANALYZE_APPS.exists():
        print(
            f"  webanalyze not found at {WEBANALYZE_BIN.relative_to(ROOT)} — "
            "skipping enrichment pass"
        )
        return {}

    unique_urls = list(dict.fromkeys(urls))
    print(f"  running webanalyze on {len(unique_urls)} URL(s)...")

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", delete=False
    ) as f:
        for u in unique_urls:
            f.write(u + "\n")
        hosts_path = f.name

    try:
        proc = subprocess.run(
            [
                str(WEBANALYZE_BIN),
                "-hosts", hosts_path,
                "-apps", str(WEBANALYZE_APPS),
                "-output", "json",
                "-silent",
                "-redirect",
                "-worker", "4",
                "-search=false",
            ],
            capture_output=True,
            text=True,
            timeout=max(60, len(unique_urls) * 8),
        )
    except subprocess.TimeoutExpired:
        print("  webanalyze timed out — skipping enrichment")
        return {}
    finally:
        Path(hosts_path).unlink(missing_ok=True)

    detections: dict[str, list[dict]] = {}
    for line in (proc.stdout or "").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        host = data.get("hostname", "")
        matches = data.get("matches") or []
        detections[host] = matches

    print(f"  webanalyze returned results for {len(detections)} URL(s)")
    return detections


def categorize_webanalyze(matches: list[dict]) -> dict:
    out = {
        "webanalyze_cms": None,
        "webanalyze_site_builder": None,
        "webanalyze_page_builder": None,
        "webanalyze_form_builders": None,
        "webanalyze_tech_summary": None,
    }
    if not matches:
        return out

    form_builders: list[str] = []
    all_techs: list[str] = []

    for m in matches:
        name = m.get("app_name") or ""
        if not name:
            continue
        version = m.get("version") or ""
        display = f"{name} {version}".strip()
        all_techs.append(display)

        cats = (m.get("app") or {}).get("category_names") or []

        if name in SITE_BUILDER_NAMES:
            if not out["webanalyze_site_builder"]:
                out["webanalyze_site_builder"] = display
        elif name in CMS_NAMES or "CMS" in cats:
            if not out["webanalyze_cms"]:
                out["webanalyze_cms"] = display

        if (
            (name in PAGE_BUILDER_NAMES or "Page builders" in cats)
            and name not in SITE_BUILDER_NAMES
        ):
            if not out["webanalyze_page_builder"]:
                out["webanalyze_page_builder"] = display

        if name in FORM_BUILDER_NAMES_WA or "Form builders" in cats:
            form_builders.append(display)

    if form_builders:
        out["webanalyze_form_builders"] = "; ".join(
            sorted(set(form_builders))
        )
    if all_techs:
        summary = sorted(set(all_techs))
        if len(summary) > 20:
            summary = summary[:20] + [f"... (+{len(all_techs) - 20} more)"]
        out["webanalyze_tech_summary"] = "; ".join(summary)

    return out


def run() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument(
        "--skip-webanalyze", action="store_true",
        help="Skip the webanalyze enrichment pass (faster, less signal)",
    )
    args = ap.parse_args()

    if not SRC.exists():
        raise SystemExit(f"missing {SRC}")

    df = pd.read_csv(SRC)

    if args.limit:
        has_site = (
            df[df["place_website"].notna()]
            .sort_values("place_review_count", ascending=False, na_position="last")
            .reset_index(drop=True)
        )
        step = max(1, len(has_site) // args.limit)
        sample = has_site.iloc[::step].head(args.limit).copy()
        out_path = OUT
        print(
            f"Smoke test: {len(sample)} contractors "
            f"(step={step} across {len(has_site)} has-website rows)"
        )
    else:
        sample = df.copy()
        out_path = OUT_FULL
        print(f"Full run: {len(sample)} contractors")

    # Pre-pass: run webanalyze on all homepages.
    webanalyze_urls: list[str] = []
    for _, row in sample.iterrows():
        w = row.get("place_website")
        if pd.isna(w) or not str(w).strip():
            continue
        host = base_host(str(w))
        if host in SOCIAL_HOSTS:
            continue
        webanalyze_urls.append(strip_query(str(w)))

    wa_detections: dict[str, list[dict]] = {}
    if not args.skip_webanalyze and webanalyze_urls:
        wa_detections = run_webanalyze(webanalyze_urls)

    # Import Playwright
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise SystemExit(
            "playwright not installed. Run:\n"
            "  pip install playwright && playwright install chromium"
        )

    results: list[dict] = []
    errors: list[dict] = []
    phone_only_snippets: list[tuple[str, str, str]] = []

    N = len(sample)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        for i, row in enumerate(sample.itertuples(index=False), start=1):
            biz = getattr(row, "business_name", "") or ""
            website = getattr(row, "place_website", None)
            review_count = getattr(row, "place_review_count", None)
            city = getattr(row, "city", "") or ""
            license_no = getattr(row, "license_no", None)

            result = {
                "license_no": license_no,
                "business_name": biz,
                "city": city,
                "place_review_count": review_count,
                "place_website": website,
                "detected_fsm": None,
                "detected_form_builder": None,
                "has_any_booking_tool": False,
                "phone_only": False,
                "detection_evidence": None,
                "detection_source_url": None,
                "webanalyze_cms": None,
                "webanalyze_site_builder": None,
                "webanalyze_page_builder": None,
                "webanalyze_form_builders": None,
                "webanalyze_tech_summary": None,
                "pages_fetched": 0,
                "pages_crawled_urls": None,
                "ssl_ok": True,
                "status": None,
            }

            if pd.isna(website) or not str(website).strip():
                result["status"] = "no_website"
                results.append(result)
                print(f"  [{i:>3}/{N}] no_website            {biz}")
                continue

            host = base_host(str(website))
            if host in SOCIAL_HOSTS:
                result["status"] = f"social_only:{host}"
                results.append(result)
                print(f"  [{i:>3}/{N}] social_only ({host})  {biz}")
                time.sleep(SLEEP_BETWEEN_CONTRACTORS)
                continue

            # Merge webanalyze enrichment (done in pre-pass)
            wa_lookup = strip_query(str(website))
            wa_cats = categorize_webanalyze(wa_detections.get(wa_lookup, []))
            result.update(wa_cats)

            # Crawl the site with Playwright
            crawl = crawl_site(browser, str(website))
            result["ssl_ok"] = crawl["ssl_ok"]
            result["pages_fetched"] = len(crawl["pages"])

            if not crawl["pages"]:
                result["status"] = classify_total_failure(crawl.get("error"))
                if crawl.get("error"):
                    errors.append({
                        "business_name": biz,
                        "url": str(website),
                        "error": crawl["error"],
                    })
                results.append(result)
                print(f"  [{i:>3}/{N}] {result['status']:<20} {biz}")
                time.sleep(SLEEP_BETWEEN_CONTRACTORS)
                continue

            # Record which pages we actually visited (for audit)
            result["pages_crawled_urls"] = "; ".join(
                url for url, _, _ in crawl["pages"]
            )

            # FSM detection — scan rendered HTML of every crawled page.
            # This is the rendered DOM, not raw server HTML, so it
            # catches JS-loaded widgets (ServiceTitan, Housecall Pro
            # iframes, etc.) that the old requests-based fetcher missed.
            for url, html, text in crawl["pages"]:
                vendor, evidence = detect_first_match(html, COMPILED_FSM)
                if vendor:
                    result["detected_fsm"] = vendor
                    result["detection_evidence"] = evidence
                    result["detection_source_url"] = url
                    break

            # Also scan the visible text (catches "Powered by ServiceTitan"
            # rendered text that might not be in an href)
            if not result["detected_fsm"]:
                for url, html, text in crawl["pages"]:
                    vendor, evidence = detect_first_match(text, COMPILED_FSM)
                    if vendor:
                        result["detected_fsm"] = vendor
                        result["detection_evidence"] = evidence
                        result["detection_source_url"] = url
                        break

            # Form builder detection
            for url, html, text in crawl["pages"]:
                vendor, evidence = detect_first_match(html, COMPILED_FORM)
                if vendor:
                    result["detected_form_builder"] = vendor
                    if result["detection_evidence"] is None:
                        result["detection_evidence"] = evidence
                        result["detection_source_url"] = url
                    break

            result["has_any_booking_tool"] = bool(
                result["detected_fsm"]
                or result["detected_form_builder"]
                or result["webanalyze_form_builders"]
            )

            if not result["has_any_booking_tool"]:
                for url, html, text in crawl["pages"]:
                    snip = phone_snippet(text)
                    if snip:
                        result["phone_only"] = True
                        phone_only_snippets.append((biz, url, snip))
                        break

            result["status"] = "ok"
            results.append(result)

            marker_parts = []
            if result["detected_fsm"]:
                marker_parts.append(f"fsm:{result['detected_fsm']}")
            if result["detected_form_builder"] or result["webanalyze_form_builders"]:
                marker_parts.append("form")
            if result["webanalyze_site_builder"]:
                marker_parts.append(f"sb:{result['webanalyze_site_builder'].split()[0]}")
            if not marker_parts:
                marker_parts.append("phone_only" if result["phone_only"] else "no_tool")
            marker = ",".join(marker_parts)

            ssl_flag = "" if result["ssl_ok"] else " ssl-fallback"
            print(
                f"  [{i:>3}/{N}] {marker:<26} {biz}  "
                f"(pages={result['pages_fetched']}{ssl_flag})"
            )

            time.sleep(SLEEP_BETWEEN_CONTRACTORS)

        browser.close()

    out_df = pd.DataFrame(results)
    out_df.to_csv(out_path, index=False)

    if errors:
        pd.DataFrame(errors).to_csv(ERR_OUT, index=False)

    if phone_only_snippets:
        with SNIPPETS_OUT.open("w") as f:
            for biz, url, snip in phone_only_snippets:
                f.write(f"=== {biz}\n{url}\n{snip}\n\n")

    # ---- Summary ----
    print()
    print("=" * 72)
    print("Summary")
    print("=" * 72)
    print(f"  total:                 {len(out_df)}")

    print()
    print("Status breakdown:")
    for s, c in out_df["status"].value_counts(dropna=False).items():
        print(f"  {str(s):<22} {c}")

    print()
    print("Page crawl stats:")
    ok_df = out_df[out_df["status"] == "ok"]
    if len(ok_df):
        print(f"  mean pages per site:   {ok_df['pages_fetched'].mean():.1f}")
        print(f"  median:                {ok_df['pages_fetched'].median():.0f}")
        print(f"  max:                   {int(ok_df['pages_fetched'].max())}")

    print()
    print("FSM detection:")
    fsm_hits = out_df["detected_fsm"].notna().sum()
    print(f"  detected_fsm (any):    {fsm_hits}")
    if fsm_hits:
        for v, c in out_df["detected_fsm"].value_counts().items():
            print(f"    {v:<22} {c}")

    print()
    print("Form builder (regex + webanalyze union):")
    regex_form = out_df["detected_form_builder"].notna().sum()
    wa_form = out_df["webanalyze_form_builders"].notna().sum()
    print(f"  regex hits:            {regex_form}")
    print(f"  webanalyze hits:       {wa_form}")
    if wa_form:
        for v, c in out_df["webanalyze_form_builders"].value_counts().items():
            print(f"    {v[:50]:<50} {c}")

    print()
    print("webanalyze enrichment (site tooling):")
    sb = out_df["webanalyze_site_builder"].notna().sum()
    cms = out_df["webanalyze_cms"].notna().sum()
    pb = out_df["webanalyze_page_builder"].notna().sum()
    print(f"  site builder:          {sb}")
    if sb:
        for v, c in out_df["webanalyze_site_builder"].value_counts().items():
            print(f"    {v[:40]:<40} {c}")
    print(f"  CMS:                   {cms}")
    if cms:
        for v, c in out_df["webanalyze_cms"].value_counts().items():
            print(f"    {v[:40]:<40} {c}")
    print(f"  page builder:          {pb}")
    if pb:
        for v, c in out_df["webanalyze_page_builder"].value_counts().items():
            print(f"    {v[:40]:<40} {c}")

    print()
    print("Classification outcomes:")
    any_tool = int(out_df["has_any_booking_tool"].sum())
    phone_only_count = int(out_df["phone_only"].sum())
    no_tool = int(((out_df["status"] == "ok")
                   & (~out_df["has_any_booking_tool"])
                   & (~out_df["phone_only"])).sum())
    print(f"  has_any_booking_tool:  {any_tool}")
    print(f"  phone_only:            {phone_only_count}")
    print(f"  ok but no tool/phone:  {no_tool}")

    print()
    print("Fetch health:")
    print(f"  fetch errors:          {len(errors)}")
    print(f"  ssl issues:            {int((~out_df['ssl_ok']).sum())}")

    print()
    print(f"  output:   {out_path.relative_to(ROOT)}")
    if errors:
        print(f"  errors:   {ERR_OUT.relative_to(ROOT)}")
    if phone_only_snippets:
        print(f"  snippets: {SNIPPETS_OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    run()
