#!/usr/bin/env python3
"""
Alt-name retry for SerpAPI google_jobs — pure extraction, no ownership judgment.

Some contractors run their public-facing brand under a DBA or under the
Google Places name, not under their legal ROC license name. A quoted
legal-name query often returns zero postings for those contractors even
when real jobs exist under the DBA. This script runs a second google_jobs
query using the alternate name and caches the raw response to
`data/signals_raw/serpapi_jobs/{place_id}_retry.json`.

Retry selection: any contractor whose `with_velocity.csv` row has a DBA
or place_name that differs from the legal name AND whose primary cache
has fewer than 3 raw postings. Threshold is intentionally permissive —
the LLM validator in step 17 does the real work; we just want to be sure
the cache has enough candidates for it to evaluate.

Priority for alternate name selection:
  1. doing_business_as (ROC registered DBA)  — canonical trade name
  2. place_name (Google Places)               — public brand
  3. (no retry if neither differs)

**No fuzzy matching is applied.** The validator in pipeline/17_candidate_validator.py
decides which postings belong to which contractor.
"""

from __future__ import annotations

import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parent.parent
POOL_CSV = ROOT / "data" / "03_hidden_gems" / "with_hiring.csv"
RAW_DIR = ROOT / "data" / "signals_raw" / "serpapi_jobs"
SNAPSHOT_DIR = ROOT / "data" / "snapshots" / "hiring"

KEY = dotenv_values(ROOT / ".env").get("SERPAPI_API_KEY")
API_URL = "https://serpapi.com/search.json"

SLEEP_BETWEEN = 0.4
REQ_TIMEOUT = 30

# Retry triggers when the primary cache has fewer than this many raw
# postings AND the contractor has an alternate name distinct from legal.
RETRY_THRESHOLD = 3

# Light normalization used only to decide "is the DBA actually different
# from the legal name, or just punctuation noise?" — never used for
# deciding whether a posting belongs to the contractor.
_PUNCT_RE = re.compile(r"[,.&'\"/()]")
_WS_RE = re.compile(r"\s+")


def looks_different(a: str, b: str) -> bool:
    """Return True if two name strings differ after lowercasing and
    stripping punctuation. Used only to decide whether the retry query
    is worth running — not to decide posting ownership."""
    def norm(s: str) -> str:
        s = (s or "").lower()
        s = _PUNCT_RE.sub(" ", s)
        s = _WS_RE.sub(" ", s).strip()
        return s
    na, nb = norm(a), norm(b)
    if not na or not nb:
        return False
    return na != nb


def pick_alt_name(row) -> tuple[str, str] | None:
    """Return (alt_name, source) for retry, or None if no retry needed."""
    legal = str(row.get("business_name") or "")
    dba = row.get("doing_business_as")
    place_name = row.get("place_name")
    dba_s = str(dba) if pd.notna(dba) else ""
    place_s = str(place_name) if pd.notna(place_name) else ""

    if dba_s and looks_different(dba_s, legal):
        return dba_s, "dba"
    if place_s and looks_different(place_s, legal):
        return place_s, "place_name"
    return None


def raw_count_in_primary_cache(place_id: str) -> int:
    """Return how many raw postings the primary (legal-name) cache
    produced for this contractor. Zero if the cache doesn't exist."""
    path = RAW_DIR / f"{place_id}.json"
    if not path.exists():
        return 0
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return 0
    return len((data.get("response") or {}).get("jobs_results") or [])


def serpapi_jobs(query: str, location: str) -> dict | None:
    params = {
        "engine": "google_jobs",
        "q": f'"{query}"',
        "location": location,
        "api_key": KEY,
        "hl": "en",
    }
    try:
        r = requests.get(API_URL, params=params, timeout=REQ_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"    ERROR: {str(e)[:150]}")
        return None


def main() -> None:
    if not KEY:
        sys.exit("SERPAPI_API_KEY missing")

    df = pd.read_csv(POOL_CSV)
    # Identify retry candidates: primary cache returned < threshold AND
    # contractor has an alternate name distinct from legal.
    candidates = []
    for _, r in df.iterrows():
        place_id = str(r.get("place_id") or "")
        primary_count = raw_count_in_primary_cache(place_id)
        if primary_count >= RETRY_THRESHOLD:
            continue
        pick = pick_alt_name(r)
        if pick is None:
            continue
        alt, source = pick
        candidates.append({
            "license_no": r["license_no"],
            "place_id": place_id,
            "rank": int(r["rank"]) if pd.notna(r.get("rank")) else 0,
            "business_name": r["business_name"],
            "city": r.get("city") or "",
            "alt_name": alt,
            "alt_source": source,
            "primary_raw_count": primary_count,
        })

    print(f"Retry candidates: {len(candidates)}")
    print(f"  (primary raw_count < {RETRY_THRESHOLD} AND has distinct DBA / place_name)")
    print()

    # Budget check
    try:
        acc = requests.get(
            "https://serpapi.com/account", params={"api_key": KEY}, timeout=10
        ).json()
        print(f"SerpAPI plan: {acc.get('plan_name')}  |  "
              f"left: {acc.get('total_searches_left')}")
    except Exception:
        pass
    print()

    api_calls = 0

    for i, c in enumerate(candidates, 1):
        place_id = c["place_id"]
        location = f'{c["city"]}, Arizona' if c["city"] else "Phoenix, Arizona"

        retry_cache = RAW_DIR / f"{place_id}_retry.json"
        if retry_cache.exists():
            raw_returned = 0
            try:
                cached_retry = json.loads(retry_cache.read_text())
                raw_returned = len((cached_retry.get("response") or {}).get("jobs_results") or [])
                status = "cached"
            except Exception:
                status = "cache_error"
        else:
            response = serpapi_jobs(c["alt_name"], location)
            api_calls += 1
            if response is not None:
                retry_cache.write_text(json.dumps({
                    "business_name": c["business_name"],
                    "alt_name": c["alt_name"],
                    "alt_source": c["alt_source"],
                    "query": f'"{c["alt_name"]}"',
                    "location": location,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "place_id": place_id,
                    "license_no": int(c["license_no"]) if pd.notna(c["license_no"]) else None,
                    "status": "ok",
                    "note": (
                        "Raw unfiltered SerpAPI response from DBA/place_name "
                        "retry. Validator in pipeline/17_candidate_validator.py "
                        "decides which postings belong."
                    ),
                    "response": response,
                }, indent=2))
                raw_returned = len(response.get("jobs_results") or [])
                status = "fetched"
            else:
                raw_returned = 0
                status = "failed"

        print(
            f"  [{i:>2}/{len(candidates)}] rank={c['rank']:<3}  "
            f"{c['business_name'][:32]:<32}  "
            f"alt[{c['alt_source']}]={c['alt_name'][:28]:<28}  "
            f"primary={c['primary_raw_count']:<2} retry_raw={raw_returned:<2}  "
            f"{status}"
        )
        if status == "fetched":
            time.sleep(SLEEP_BETWEEN)

    print()
    print("=" * 72)
    print("Retry summary")
    print("=" * 72)
    print(f"API calls made:      {api_calls}")
    print(f"Contractors retried: {len(candidates)}")
    print()
    print("Next step: run pipeline/17_candidate_validator.py — it reads both")
    print("the legal-name cache and the _retry.json cache and validates all")
    print("postings for every contractor in one pass.")


if __name__ == "__main__":
    main()
