#!/usr/bin/env python3
"""
SerpAPI google_jobs extractor — pure extraction, no ownership judgment.

For each contractor in `with_velocity.csv`, queries Google's job aggregator
(Indeed / ZipRecruiter / Glassdoor / LinkedIn / SimplyHired / company
careers pages) with a quoted legal-name query and caches the full raw
response to `data/signals_raw/serpapi_jobs/{place_id}.json`.

**No fuzzy company-name match is applied here.** Google Jobs often returns
"related" postings from other HVAC contractors alongside the real match —
we keep all of them in the cache and let `pipeline/17_candidate_validator.py`
decide (via LLM) which postings actually belong to this contractor. Role
classification (ops_pain / capacity_growth) also moves downstream into
`pipeline/11_scoring.py`, which reads the validator's kept titles.

Writes `with_hiring.csv` as a passthrough of `with_velocity.csv` so the
downstream filename chain (step 08 reads with_hiring.csv) is unchanged.

Usage:
  python pipeline/07_serpapi_hiring.py --limit 5   # feasibility subset
  python pipeline/07_serpapi_hiring.py             # full 70 run
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parent.parent
POOL_CSV = ROOT / "data" / "03_hidden_gems" / "with_velocity.csv"
RAW_DIR = ROOT / "data" / "signals_raw" / "serpapi_jobs"
OUT_CSV = ROOT / "data" / "03_hidden_gems" / "with_hiring.csv"
SNAPSHOT_DIR = ROOT / "data" / "snapshots" / "hiring"

KEY = dotenv_values(ROOT / ".env").get("SERPAPI_API_KEY")
API_URL = "https://serpapi.com/search.json"

SLEEP_BETWEEN = 0.4
REQ_TIMEOUT = 30


def serpapi_jobs(query: str, location: str) -> dict:
    params = {
        "engine": "google_jobs",
        "q": query,
        "location": location,
        "api_key": KEY,
        "hl": "en",
    }
    r = requests.get(API_URL, params=params, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.json()


def fetch_with_retry(query: str, location: str) -> tuple[dict | None, str]:
    """Single retry on transient errors. Returns (response, status)."""
    for attempt in (1, 2):
        try:
            return serpapi_jobs(query, location), "ok"
        except requests.HTTPError as e:
            sc = getattr(e.response, "status_code", 0) if e.response is not None else 0
            if sc in (429, 500, 502, 503, 504) and attempt == 1:
                time.sleep(2)
                continue
            return None, f"http_{sc}"
        except requests.RequestException as e:
            if attempt == 1:
                time.sleep(1)
                continue
            return None, f"request_error: {str(e)[:100]}"
    return None, "unknown"


def process_contractor(row, cache_path: Path) -> dict:
    biz = str(row.get("business_name") or "")
    city = str(row.get("city") or "")
    place_id = str(row.get("place_id") or "")
    rank = row.get("rank")
    license_no = row.get("license_no")
    location = f"{city}, Arizona" if city else "Phoenix, Arizona"

    cached = False
    raw_response: dict | None = None
    status = ""
    if cache_path.exists():
        try:
            cached_data = json.loads(cache_path.read_text())
            raw_response = cached_data["response"]
            status = cached_data.get("status", "ok") + " (cached)"
            cached = True
        except (json.JSONDecodeError, KeyError, OSError):
            cached = False

    if not cached:
        api_response, status = fetch_with_retry(f'"{biz}"', location)
        if api_response is not None:
            cache_path.write_text(json.dumps({
                "business_name": biz,
                "license_no": int(license_no) if pd.notna(license_no) else None,
                "place_id": place_id,
                "query": f'"{biz}"',
                "location": location,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "status": status,
                "note": (
                    "Raw unfiltered SerpAPI response. Validator in "
                    "pipeline/17_candidate_validator.py decides which "
                    "postings belong to this contractor."
                ),
                "response": api_response,
            }, indent=2))
            raw_response = api_response

    jobs_results = (raw_response or {}).get("jobs_results") or []
    return {
        "license_no": int(license_no) if pd.notna(license_no) else None,
        "place_id": place_id,
        "business_name": biz,
        "rank": rank,
        "hiring_raw_count": len(jobs_results),
        "hiring_fetch_status": status,
        "hiring_from_cache": cached,
    }


def run() -> None:
    if not KEY:
        sys.exit("SERPAPI_API_KEY missing from .env")
    if not POOL_CSV.exists():
        sys.exit(f"missing {POOL_CSV}")

    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    # Budget check
    try:
        acc = requests.get(
            "https://serpapi.com/account", params={"api_key": KEY}, timeout=10
        ).json()
        left = acc.get("total_searches_left")
        print(f"SerpAPI budget: {left} searches left this month")
    except Exception:
        left = None
        print("SerpAPI budget: (account endpoint failed, proceeding)")

    df = pd.read_csv(POOL_CSV)
    if args.limit:
        sample = df.head(args.limit).copy()
    else:
        sample = df.copy()

    print(f"Processing {len(sample)} contractors")
    if left is not None and len(sample) > left:
        sys.exit(
            f"!! {len(sample)} contractors needed but only {left} searches left."
        )

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    results = []
    api_calls = 0
    cache_hits = 0

    for i, row in enumerate(sample.to_dict(orient="records"), 1):
        biz = row.get("business_name", "")
        place_id = row.get("place_id", "")
        rank = row.get("rank")

        cache_path = RAW_DIR / f"{place_id}.json"
        result = process_contractor(row, cache_path)

        if result.get("hiring_from_cache"):
            cache_hits += 1
        else:
            api_calls += 1

        results.append(result)

        status = result["hiring_fetch_status"]
        raw_n = result["hiring_raw_count"]

        print(
            f"  [{i:>2}/{len(sample)}] rank={rank}  "
            f"{biz[:36]:<36}  "
            f"raw={raw_n:<3}  "
            f"{status}"
        )

        if not result.get("hiring_from_cache"):
            time.sleep(SLEEP_BETWEEN)

    # ---- Pass-through write ----
    # with_hiring.csv used to carry fuzzy-matched ops_pain/capacity counts.
    # Those counts are now computed in pipeline/11_scoring.py from the
    # validator's kept titles. We still write with_hiring.csv (step 08
    # reads it as the pool) but only as a passthrough of with_velocity.csv.
    pool = pd.read_csv(POOL_CSV)
    pool.to_csv(OUT_CSV, index=False)

    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    snapshot_path = SNAPSHOT_DIR / f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.csv"
    pool.to_csv(snapshot_path, index=False)

    out_df = pd.DataFrame(results)

    # ---- Summary ----
    print()
    print("=" * 72)
    print("Summary")
    print("=" * 72)
    print(f"API calls made: {api_calls}  |  cache hits: {cache_hits}")
    print()
    print(f"Contractors with >0 raw postings returned: "
          f"{int((out_df['hiring_raw_count'] > 0).sum())}")
    print(f"Contractors with 0 postings:               "
          f"{int((out_df['hiring_raw_count'] == 0).sum())}")
    print()
    print("Raw postings distribution (pre-validator — includes unrelated 'related results'):")
    print(f"  mean:   {out_df['hiring_raw_count'].mean():.1f}")
    print(f"  median: {out_df['hiring_raw_count'].median():.1f}")
    print(f"  max:    {int(out_df['hiring_raw_count'].max())}")
    print()
    print(f"Next step: pipeline/07b_serpapi_hiring_retry.py (alt-name retry for zero-result contractors)")
    print(f"Outputs:")
    print(f"  {OUT_CSV.relative_to(ROOT)}  (passthrough of with_velocity.csv)")
    print(f"  {snapshot_path.relative_to(ROOT)}")


if __name__ == "__main__":
    run()
