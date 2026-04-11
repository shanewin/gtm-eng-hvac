#!/usr/bin/env python3
"""
Enrich data/contractors_filtered.csv with Google Places API (New) data.

Writes a timestamped snapshot to data/places_snapshots/YYYY-MM-DD.csv for
week-over-week diffing, and (on full runs only) overwrites
data/contractors_enriched.csv with the latest snapshot for downstream scripts.

Usage:
  python pipeline/02_enrich_places.py --limit 10     # smoke test
  python pipeline/02_enrich_places.py                # full run
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import date
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
FILTERED_CSV = ROOT / "data" / "01_contractors" / "filtered.csv"
SNAPSHOT_DIR = ROOT / "data" / "snapshots" / "places"
ENRICHED_CSV = ROOT / "data" / "01_contractors" / "enriched.csv"

API_URL = "https://places.googleapis.com/v1/places:searchText"

FIELD_MASK = ",".join([
    "places.id",
    "places.displayName",
    "places.formattedAddress",
    "places.rating",
    "places.userRatingCount",
    "places.websiteUri",
    "places.nationalPhoneNumber",
    "places.location",
    "places.primaryTypeDisplayName",
    "places.businessStatus",
])

SLEEP_BETWEEN = 0.2
PROGRESS_EVERY = 25
REQUEST_TIMEOUT = 20
RETRY_BACKOFF_SEC = 2.0

PLACE_COLUMNS = [
    "place_id",
    "place_name",
    "place_address",
    "place_rating",
    "place_review_count",
    "place_website",
    "place_phone",
    "place_latitude",
    "place_longitude",
    "place_primary_type",
    "place_business_status",
    "place_match",
    "place_match_confidence",
    "place_query",
    "place_query_source",
    "place_error",
]

CONFIDENCE_THRESHOLD = 85

_QUERY_PUNCT_RE = re.compile(r"[,./]+")
_WS_RE = re.compile(r"\s+")
_MATCH_NONWORD_RE = re.compile(r"[^a-z0-9 ]+")

# Stripped before fuzzy comparison so shared trade/entity words don't inflate
# the score (e.g. "Rockett Refrigeration LLC" vs "Ice Age Refrigeration LLC"
# otherwise shares ~18 chars of suffix).
MATCH_STOPWORDS = {
    "inc", "incorporated", "llc", "corp", "corporation", "co", "company",
    "companies", "ltd", "limited", "pllc", "lp", "llp", "plc",
    "air", "hvac", "heating", "cooling", "refrigeration", "mechanical",
    "services", "service", "ac",
    "the", "and",
}


def empty_place_row() -> dict:
    return {c: None for c in PLACE_COLUMNS}


def places_text_search(
    query: str, api_key: str, session: requests.Session
) -> tuple[dict | None, str | None]:
    """Return (response_json, error_message). Retries once on transient errors."""
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": FIELD_MASK,
    }
    body = {"textQuery": query, "maxResultCount": 1}

    last_err: str | None = None
    for attempt in range(2):  # original + 1 retry
        try:
            r = session.post(
                API_URL, json=body, headers=headers, timeout=REQUEST_TIMEOUT
            )
            if r.status_code == 200:
                return r.json(), None
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = f"HTTP {r.status_code}"
                if attempt == 0:
                    time.sleep(RETRY_BACKOFF_SEC)
                    continue
                return None, last_err
            return None, f"HTTP {r.status_code}: {r.text[:200]}"
        except requests.RequestException as e:
            last_err = f"request error: {e}"
            if attempt == 0:
                time.sleep(RETRY_BACKOFF_SEC)
                continue
            return None, last_err
    return None, last_err


def parse_place(resp: dict | None) -> dict:
    row = empty_place_row()
    places = (resp or {}).get("places", []) or []
    if not places:
        row["place_match"] = False
        return row

    p = places[0]
    loc = p.get("location") or {}
    display = p.get("displayName") or {}
    primary = p.get("primaryTypeDisplayName") or {}

    row["place_id"] = p.get("id")
    row["place_name"] = display.get("text")
    row["place_address"] = p.get("formattedAddress")
    row["place_rating"] = p.get("rating")
    row["place_review_count"] = p.get("userRatingCount")
    row["place_website"] = p.get("websiteUri")
    row["place_phone"] = p.get("nationalPhoneNumber")
    row["place_latitude"] = loc.get("latitude")
    row["place_longitude"] = loc.get("longitude")
    row["place_primary_type"] = primary.get("text")
    row["place_business_status"] = p.get("businessStatus")
    row["place_match"] = True
    return row


def _clean_cell(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    return "" if s.lower() == "nan" else s


def normalize_for_query(s: str) -> str:
    """Strip , . / and collapse whitespace. Preserves letters/digits/&/'."""
    s = _QUERY_PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def normalize_for_match(s: str) -> str:
    """Aggressive normalization for fuzzy comparison: lowercase, strip
    non-alphanumerics, remove entity/trade stopwords, collapse whitespace.
    If stripping removes everything, fall back to the pre-stripped form so
    single-word names like 'HVAC' don't collapse to empty."""
    s = s.lower()
    s = _MATCH_NONWORD_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    if not s:
        return ""
    tokens = [t for t in s.split(" ") if t and t not in MATCH_STOPWORDS]
    stripped = " ".join(tokens)
    return stripped if stripped else s


def build_query(
    business_name, doing_business_as, city
) -> tuple[str, str, str]:
    """Return (full_query, name_portion_for_confidence, query_source).

    Prefers DBA when populated (public-facing brand is what Google indexes).
    """
    dba = _clean_cell(doing_business_as)
    legal = _clean_cell(business_name)
    if dba:
        raw_name, source = dba, "dba"
    else:
        raw_name, source = legal, "legal"

    name_norm = normalize_for_query(raw_name)
    city_norm = normalize_for_query(_clean_cell(city))
    parts = [p for p in [name_norm, city_norm, "AZ"] if p]
    return " ".join(parts), name_norm, source


def compute_confidence(queried_name: str, place_name: str | None) -> int | None:
    if not place_name:
        return None
    a = normalize_for_match(queried_name)
    b = normalize_for_match(place_name)
    if not a or not b:
        return None
    return round(SequenceMatcher(None, a, b).ratio() * 100)


def run() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--limit", type=int, default=None,
        help="Only process the first N rows (smoke test).",
    )
    ap.add_argument(
        "--snapshot-date", default=None,
        help="Override snapshot date (defaults to today, YYYY-MM-DD).",
    )
    args = ap.parse_args()

    api_key = os.environ.get("GOOGLE_PLACES_API_KEY")
    if not api_key:
        sys.exit("GOOGLE_PLACES_API_KEY not set in .env")

    if not FILTERED_CSV.exists():
        sys.exit(f"missing {FILTERED_CSV} — run 01_load_and_filter.py first")

    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(FILTERED_CSV, dtype=str)
    if args.limit:
        df = df.head(args.limit).copy()
    df = df.reset_index(drop=True)

    snapshot_date = args.snapshot_date or date.today().isoformat()
    is_smoke = args.limit is not None

    print(
        f"Enriching {len(df):,} contractors  |  snapshot={snapshot_date}"
        + ("  |  SMOKE TEST" if is_smoke else "")
    )

    session = requests.Session()
    results: list[dict] = []
    errors = 0

    for i, row in enumerate(df.itertuples(index=False), start=1):
        biz = getattr(row, "business_name", "")
        dba = getattr(row, "doing_business_as", "")
        city = getattr(row, "city", "")
        query, queried_name, source = build_query(biz, dba, city)

        resp, err = places_text_search(query, api_key, session)
        place_row = parse_place(resp)
        place_row["place_query"] = query
        place_row["place_query_source"] = source
        if place_row.get("place_match") is True:
            place_row["place_match_confidence"] = compute_confidence(
                queried_name, place_row.get("place_name")
            )
        if err:
            place_row["place_error"] = err
            place_row["place_match"] = False
            errors += 1
        results.append(place_row)

        if i % PROGRESS_EVERY == 0 or i == len(df):
            matched = sum(1 for r in results if r.get("place_match") is True)
            print(
                f"  {i}/{len(df)}  matched={matched}  errors={errors}"
            )

        time.sleep(SLEEP_BETWEEN)

    places_df = pd.DataFrame(results, columns=PLACE_COLUMNS)
    out = pd.concat([df, places_df], axis=1)
    out["snapshot_date"] = snapshot_date

    snapshot_name = (
        f"{snapshot_date}_smoke.csv" if is_smoke else f"{snapshot_date}.csv"
    )
    snapshot_path = SNAPSHOT_DIR / snapshot_name
    out.to_csv(snapshot_path, index=False)

    if not is_smoke:
        out.to_csv(ENRICHED_CSV, index=False)

    # ---- Summary ----
    n = len(out)
    match_mask = out["place_match"] == True  # noqa: E712
    matched = int(match_mask.sum())
    confidence = pd.to_numeric(out["place_match_confidence"], errors="coerce")
    high_conf = int((confidence >= CONFIDENCE_THRESHOLD).sum())
    mid_conf = int(((confidence >= 70) & (confidence < CONFIDENCE_THRESHOLD)).sum())
    low_conf = int((confidence.notna() & (confidence < 70)).sum())
    no_score = int(matched - (high_conf + mid_conf + low_conf))
    dba_used = int((out["place_query_source"] == "dba").sum())
    legal_used = int((out["place_query_source"] == "legal").sum())
    has_web = int(out["place_website"].notna().sum())
    review_counts = pd.to_numeric(out["place_review_count"], errors="coerce")
    has_20_reviews = int((review_counts.fillna(0) >= 20).sum())
    closed_perm = int(
        (out["place_business_status"].fillna("") == "CLOSED_PERMANENTLY").sum()
    )
    median_reviews = review_counts[match_mask].median()
    median_reviews_str = (
        f"{median_reviews:.0f}" if pd.notna(median_reviews) else "n/a"
    )
    est_cost_usd = n * 35 / 1000.0

    print()
    print("Summary")
    print(f"  looked up:                       {n}")
    print(f"  matched (any confidence):        {matched}")
    print(f"  unmatched:                       {n - matched}")
    print()
    print("Confidence distribution (matched rows only)")
    print(f"  >= {CONFIDENCE_THRESHOLD}  (keepers):             {high_conf}")
    print(f"  70-{CONFIDENCE_THRESHOLD - 1} (borderline):          {mid_conf}")
    print(f"  <  70  (reject):              {low_conf}")
    if no_score:
        print(f"  no score (shouldn't happen): {no_score}")
    print()
    print("Query source")
    print(f"  queried by DBA:                  {dba_used}")
    print(f"  queried by legal name:           {legal_used}")
    if n:
        print(f"  DBA coverage:                    {dba_used / n * 100:.1f}%")
    print()
    print("Operational signals")
    print(f"  has website:                     {has_web}")
    print(f"  >= 20 reviews:                   {has_20_reviews}")
    print(f"  CLOSED_PERMANENTLY:              {closed_perm}")
    print(f"  median review count (matched):   {median_reviews_str}")
    print()
    print("Run")
    print(f"  errors (after retry):            {errors}")
    print(f"  est. API cost:                   ~${est_cost_usd:.2f}")
    print(f"  snapshot:                        {snapshot_path.relative_to(ROOT)}")
    if is_smoke:
        print("  (smoke test — contractors_enriched.csv NOT overwritten)")
    else:
        print(f"  enriched (latest):     {ENRICHED_CSV.relative_to(ROOT)}")


if __name__ == "__main__":
    run()
