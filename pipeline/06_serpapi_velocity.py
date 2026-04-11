#!/usr/bin/env python3
"""
Review velocity via SerpAPI Google Maps Reviews endpoint.

Phase 1 feasibility: 5 contractors sampled evenly across the 70-row hidden
gems pool. Paginates newest-first, stops when we see a review older than
the 180-day window, computes 90-vs-90 velocity plus a 6-month average.

Usage:
  python pipeline/06_serpapi_velocity.py --limit 5      # phase 1 feasibility
  python pipeline/06_serpapi_velocity.py                # full 70-pool run
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

POOL_CSV = ROOT / "data" / "03_hidden_gems" / "filtered_pool.csv"
RAW_DIR = ROOT / "data" / "signals_raw" / "serpapi_reviews"
OUT_CSV = ROOT / "data" / "03_hidden_gems" / "with_velocity.csv"
SNAPSHOT_DIR = ROOT / "data" / "snapshots" / "velocity"

KEY = os.environ.get("SERPAPI_API_KEY")
API_URL = "https://serpapi.com/search.json"

LOOKBACK_DAYS = 180
RECENT_DAYS = 90
MAX_PAGES_PER_CONTRACTOR = 30
SLEEP_BETWEEN_PAGES = 0.3
REQ_TIMEOUT = 30


def parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def fetch_reviews_within_window(
    place_id: str, lookback_days: int, max_pages: int
) -> tuple[list[dict], int, str]:
    """Paginate newest-first until oldest review on current page is outside
    the lookback window, or until pagination exhausted, or max_pages hit.

    Returns (reviews, pages_used, status).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    reviews: list[dict] = []
    next_token: str | None = None
    pages = 0
    status = "unknown"

    while pages < max_pages:
        params = {
            "engine": "google_maps_reviews",
            "place_id": place_id,
            "api_key": KEY,
            "hl": "en",
            "sort_by": "newestFirst",
        }
        if next_token:
            params["next_page_token"] = next_token

        try:
            r = requests.get(API_URL, params=params, timeout=REQ_TIMEOUT)
        except requests.RequestException as e:
            status = f"request_error: {str(e)[:100]}"
            return reviews, pages, status

        pages += 1

        if r.status_code != 200:
            status = f"http_{r.status_code}"
            return reviews, pages, status

        data = r.json()
        page_reviews = data.get("reviews") or []

        if not page_reviews:
            status = "empty_page"
            break

        reviews.extend(page_reviews)

        # Reviews are newest-first, so the last one on the page is the
        # oldest on that page. If it's before the cutoff, the full lookback
        # window is already contained in what we've fetched; stop.
        oldest_iso = page_reviews[-1].get("iso_date")
        oldest_dt = parse_iso(oldest_iso)
        if oldest_dt and oldest_dt < cutoff:
            status = "complete_early_stop"
            break

        next_token = (data.get("serpapi_pagination") or {}).get("next_page_token")
        if not next_token:
            status = "no_more_pages"
            break

        time.sleep(SLEEP_BETWEEN_PAGES)

    if pages >= max_pages and status == "unknown":
        status = "max_pages_hit"

    return reviews, pages, status


def classify_velocity(recent: int, prior: int, ratio: float) -> str:
    """Categorize the velocity signal so downstream scoring can bucket
    instead of relying on the raw ratio (which has a divide-by-max(prior,1)
    artifact when prior == 0)."""
    total = recent + prior
    if total < 5:
        return "low_volume"
    if prior < 3 and recent >= 5:
        return "hot_new"
    if prior >= 3:
        if ratio >= 1.5:
            return "accelerating"
        if 0.7 <= ratio <= 1.5:
            return "steady"
        return "cooling"  # ratio < 0.7
    # Fallthrough: total >= 5 but prior < 3 and recent < 5
    # (e.g. prior=2, recent=3). Not enough baseline to classify confidently.
    return "low_volume"


def compute_velocity_metrics(reviews: list[dict]) -> dict:
    now = datetime.now(timezone.utc)
    recent_cut = now - timedelta(days=RECENT_DAYS)
    prior_cut = now - timedelta(days=LOOKBACK_DAYS)

    recent = 0
    prior = 0
    for r in reviews:
        dt = parse_iso(r.get("iso_date"))
        if not dt:
            continue
        if dt >= recent_cut:
            recent += 1
        elif dt >= prior_cut:
            prior += 1

    velocity_ratio = round(recent / max(prior, 1), 2)
    reviews_per_month_6mo = round((recent + prior) / 6.0, 1)
    return {
        "recent_90d_reviews": recent,
        "prior_90d_reviews": prior,
        "velocity_ratio": velocity_ratio,
        "reviews_per_month_6mo": reviews_per_month_6mo,
        "velocity_category": classify_velocity(recent, prior, velocity_ratio),
    }


def sample_across_pool(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """Evenly spaced sampling across the pool, which is already sorted by
    review count desc (rank 1 = highest)."""
    if len(df) <= n:
        return df.copy()
    step = (len(df) - 1) / (n - 1)
    indices = [round(i * step) for i in range(n)]
    return df.iloc[indices].copy()


def main() -> None:
    if not KEY:
        sys.exit("SERPAPI_API_KEY not set in .env")
    if not POOL_CSV.exists():
        sys.exit(f"missing {POOL_CSV}")

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--limit", type=int, default=None,
        help="Feasibility mode: sample N contractors across the pool",
    )
    args = ap.parse_args()

    df = pd.read_csv(POOL_CSV)
    print(f"Pool: {len(df)} contractors")

    if args.limit:
        sample = sample_across_pool(df, args.limit)
        print(f"Phase 1 feasibility: {len(sample)} contractors (even spread)")
    else:
        sample = df.copy()
        print(f"Full run: {len(sample)} contractors")
    print(f"Free-tier budget: 250 searches/month")
    print()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    total_api_calls = 0
    cache_hits = 0
    results: list[dict] = []

    for i, row in enumerate(sample.itertuples(index=False), 1):
        biz = getattr(row, "business_name", "") or ""
        place_id = getattr(row, "place_id", None)
        review_count = getattr(row, "place_review_count", None)
        rank = getattr(row, "rank", None)
        license_no = getattr(row, "license_no", None)

        if not place_id or pd.isna(place_id):
            print(f"[{i}/{len(sample)}] SKIP (no place_id)  {biz}")
            continue

        rev_str = int(review_count) if pd.notna(review_count) else "?"
        print(f"[{i}/{len(sample)}] {biz}  (rank={rank}, reviews={rev_str})")

        raw_path = RAW_DIR / f"{place_id}.json"
        cached = False

        if raw_path.exists():
            try:
                cached_data = json.loads(raw_path.read_text())
                reviews = cached_data.get("reviews", [])
                pages = cached_data.get("pages_used", 0)
                status = (cached_data.get("status") or "") + " (cached)"
                cached = True
                cache_hits += 1
            except (json.JSONDecodeError, OSError):
                cached = False

        if not cached:
            reviews, pages, status = fetch_reviews_within_window(
                place_id, LOOKBACK_DAYS, MAX_PAGES_PER_CONTRACTOR
            )
            total_api_calls += pages
            raw_path.write_text(json.dumps({
                "business_name": biz,
                "license_no": int(license_no) if pd.notna(license_no) else None,
                "place_id": place_id,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "pages_used": pages,
                "status": status,
                "reviews": reviews,
            }, indent=2))

        metrics = compute_velocity_metrics(reviews)

        print(f"  pages={pages}  reviews_fetched={len(reviews)}  status={status}")
        print(f"  recent_90d={metrics['recent_90d_reviews']}  "
              f"prior_90d={metrics['prior_90d_reviews']}  "
              f"velocity={metrics['velocity_ratio']}x  "
              f"category={metrics['velocity_category']}")
        print(f"  cumulative API calls: {total_api_calls}  (cache hits: {cache_hits})")
        print()

        results.append({
            "rank": rank,
            "license_no": int(license_no) if pd.notna(license_no) else None,
            "business_name": biz,
            "place_id": place_id,
            "total_reviews_google": review_count,
            "serpapi_pages_used": pages,
            "reviews_fetched_in_window": len(reviews),
            "fetch_status": status,
            "from_cache": cached,
            **metrics,
        })

    # ---- Summary ----
    print("=" * 72)
    print("Summary")
    print("=" * 72)
    res_df = pd.DataFrame(results)
    n = len(results)

    if args.limit:
        # Feasibility preview
        with pd.option_context("display.max_colwidth", 30, "display.width", 220):
            print(res_df.to_string(index=False))
        print()
        avg_pages = total_api_calls / max(n - cache_hits, 1) if (n - cache_hits) else 0
        pool_size = len(df)
        remaining = pool_size - n
        projected_remaining = int(round(avg_pages * remaining))
        projected_total = total_api_calls + projected_remaining

        print(f"API calls made (these {n}):          {total_api_calls}")
        print(f"Cache hits:                          {cache_hits}")
        print(f"Avg pages per fresh contractor:      {avg_pages:.2f}")
        print(f"Projected for remaining {remaining}:           ~{projected_remaining}")
        print(f"Projected total for all {pool_size}:          ~{projected_total}")
        print(f"Free tier monthly limit:             250")
        if projected_total <= 225:
            print("  GREEN: comfortable headroom.")
        elif projected_total <= 250:
            print("  YELLOW: fits but zero safety margin.")
        else:
            print(f"  RED: {projected_total - 250} calls over free tier.")
        print()
        print(f"(Feasibility mode — no merge to {OUT_CSV.name})")
        return

    # ---- Full-run merge ----
    print()
    print(f"Fetch complete. API calls made: {total_api_calls}  |  cache hits: {cache_hits}")
    print()

    # Distribution of velocity_category
    print("velocity_category distribution:")
    for cat, count in res_df["velocity_category"].value_counts().items():
        print(f"  {cat:<14} {count}")
    print()

    # Status failures
    failures = res_df[~res_df["fetch_status"].str.startswith(
        ("complete_early_stop", "no_more_pages")
    )]
    if len(failures):
        print(f"!! {len(failures)} contractor(s) with non-clean fetch status:")
        for _, fr in failures.iterrows():
            print(f"  {fr['business_name']:<40} status={fr['fetch_status']}")
        print()
    else:
        print("All fetches completed cleanly.")
        print()

    # Top 10 accelerating
    accel = res_df[res_df["velocity_category"] == "accelerating"].copy()
    accel = accel.sort_values("velocity_ratio", ascending=False)
    print(f"Top 10 accelerating contractors (velocity_ratio desc):")
    cols = ["rank", "business_name", "total_reviews_google",
            "recent_90d_reviews", "prior_90d_reviews", "velocity_ratio"]
    with pd.option_context("display.max_colwidth", 42, "display.width", 220):
        print(accel[cols].head(10).to_string(index=False))
    print()

    # Merge into pool CSV
    pool_df = pd.read_csv(POOL_CSV)
    merge_cols = [
        "license_no", "recent_90d_reviews", "prior_90d_reviews",
        "velocity_ratio", "reviews_per_month_6mo", "velocity_category",
        "serpapi_pages_used", "reviews_fetched_in_window", "fetch_status",
    ]
    merged = pool_df.merge(
        res_df[merge_cols], on="license_no", how="left", suffixes=("", "_v")
    )
    merged.to_csv(OUT_CSV, index=False)

    # Snapshot (per CLAUDE.md standing rule)
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    snapshot_path = SNAPSHOT_DIR / f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.csv"
    merged.to_csv(snapshot_path, index=False)

    print(f"Free tier usage this run:   {total_api_calls} / 250")
    print(f"Budget remaining:           {250 - total_api_calls}")
    print()
    print("Outputs:")
    print(f"  current: {OUT_CSV.relative_to(ROOT)}")
    print(f"  snapshot: {snapshot_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
