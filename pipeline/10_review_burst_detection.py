#!/usr/bin/env python3
"""
Review burst anomaly detection.

Detects time windows where a contractor received an unusually large spike
of reviews relative to their own baseline. A negative burst in the last
30-60 days is the strongest "call them right now" buying signal in the
project — it's direct evidence of a recent triggering event.

Algorithm:
  1. Load cached SerpAPI reviews per contractor (iso_date, rating, snippet)
  2. Sort chronologically
  3. Compute baseline: reviews per week over the full cached window
  4. Slide a 7-day window through the review sequence
  5. Flag burst if: count_in_window >= 3 AND count_in_window >= 3x baseline
  6. Classify burst sentiment by average rating in the window:
     - negative:  avg < 3.0
     - positive:  avg > 4.5
     - mixed:     otherwise
  7. Per-contractor rollup:
     - active_crisis:    negative burst in the last 30 days
     - recent_crisis:    negative burst in the last 60 days
     - scaling_surge:    positive burst in the last 60 days
     - historical_burst: any burst >60 days ago
     - steady:           no bursts detected
     - low_data:         fewer than 10 cached reviews

Zero API cost. Runs in ~2 seconds across all 70 contractors.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
POOL_CSV = ROOT / "data" / "03_hidden_gems" / "complete.csv"
RAW_REVIEWS_DIR = ROOT / "data" / "signals_raw" / "serpapi_reviews"
SNAPSHOT_DIR = ROOT / "data" / "snapshots" / "burst"

MIN_REVIEWS_FOR_ANALYSIS = 10
WINDOW_DAYS = 7
MIN_BURST_COUNT = 3
BURST_MULTIPLIER_THRESHOLD = 3.0
NEGATIVE_RATING_CUTOFF = 3.0
POSITIVE_RATING_CUTOFF = 4.5
RECENT_DAYS_CRISIS = 30
RECENT_DAYS_BURST = 60


def parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def extract_snippet(review: dict) -> str:
    snippet = review.get("snippet") or ""
    if not snippet:
        ex = review.get("extracted_snippet") or {}
        if isinstance(ex, dict):
            snippet = ex.get("original") or ""
    return snippet or ""


def burst_sentiment(avg_rating: float) -> str:
    if avg_rating < NEGATIVE_RATING_CUTOFF:
        return "negative"
    if avg_rating > POSITIVE_RATING_CUTOFF:
        return "positive"
    return "mixed"


def detect_bursts(reviews: list[dict]) -> tuple[list[dict], dict]:
    """Return (burst_list, baseline_stats)."""
    dated: list[tuple[datetime, dict]] = []
    for r in reviews:
        dt = parse_iso(r.get("iso_date"))
        if dt:
            dated.append((dt, r))

    if len(dated) < MIN_REVIEWS_FOR_ANALYSIS:
        return [], {"n": len(dated), "insufficient": True}

    dated.sort(key=lambda x: x[0])
    date_range_days = max(1, (dated[-1][0] - dated[0][0]).days)
    avg_per_week = len(dated) * 7.0 / date_range_days

    baseline = {
        "n": len(dated),
        "date_range_days": date_range_days,
        "avg_per_week": round(avg_per_week, 2),
        "insufficient": False,
    }

    bursts: list[dict] = []
    last_burst_end: datetime | None = None

    for i, (anchor_date, _) in enumerate(dated):
        if last_burst_end and anchor_date <= last_burst_end:
            continue

        window_end = anchor_date + timedelta(days=WINDOW_DAYS)
        window_reviews = [
            (d, r) for d, r in dated
            if anchor_date <= d <= window_end
        ]
        count = len(window_reviews)
        if count < MIN_BURST_COUNT:
            continue

        multiplier = count / max(avg_per_week, 0.1)
        if multiplier < BURST_MULTIPLIER_THRESHOLD:
            continue

        ratings = [float(r.get("rating", 0) or 0) for _, r in window_reviews]
        avg_rating = sum(ratings) / len(ratings) if ratings else 0.0

        bursts.append({
            "start_date": anchor_date.isoformat(),
            "end_date": window_end.isoformat(),
            "review_count": count,
            "avg_rating": round(avg_rating, 2),
            "multiplier": round(multiplier, 2),
            "sentiment": burst_sentiment(avg_rating),
            "reviews": [r for _, r in window_reviews],
        })
        last_burst_end = window_end

    return bursts, baseline


def rollup_bursts(bursts: list[dict], baseline: dict, now: datetime) -> dict:
    """Aggregate burst detections into per-contractor columns."""
    if baseline.get("insufficient"):
        return {
            "burst_total_count": 0,
            "burst_negative_total": 0,
            "burst_positive_total": 0,
            "burst_recent_60d_count": 0,
            "burst_recent_negative_60d": 0,
            "burst_recent_positive_60d": 0,
            "burst_most_recent_date": None,
            "burst_most_recent_avg_rating": None,
            "burst_most_recent_sentiment": None,
            "burst_negative_sample_quote": None,
            "burst_negative_sample_date": None,
            "burst_positive_sample_quote": None,
            "burst_positive_sample_date": None,
            "burst_baseline_per_week": None,
            "burst_category": "low_data",
        }

    cutoff_30 = now - timedelta(days=RECENT_DAYS_CRISIS)
    cutoff_60 = now - timedelta(days=RECENT_DAYS_BURST)

    def burst_start(b: dict) -> datetime:
        return parse_iso(b["start_date"]) or datetime.min.replace(tzinfo=timezone.utc)

    total = len(bursts)
    negative_bursts = [b for b in bursts if b["sentiment"] == "negative"]
    positive_bursts = [b for b in bursts if b["sentiment"] == "positive"]

    recent_60 = [b for b in bursts if burst_start(b) >= cutoff_60]
    recent_neg_60 = [b for b in recent_60 if b["sentiment"] == "negative"]
    recent_pos_60 = [b for b in recent_60 if b["sentiment"] == "positive"]

    has_neg_30 = any(
        burst_start(b) >= cutoff_30 and b["sentiment"] == "negative"
        for b in bursts
    )

    # Classification priority
    if has_neg_30:
        category = "active_crisis"
    elif recent_neg_60:
        category = "recent_crisis"
    elif recent_pos_60:
        category = "scaling_surge"
    elif total > 0:
        category = "historical_burst"
    else:
        category = "steady"

    most_recent = max(bursts, key=burst_start) if bursts else None

    # Sample quotes: pick the most recent negative burst and grab the
    # lowest-rated review's snippet. Same for positive (highest rated).
    def pick_sample_quote(burst_list: list[dict], sentiment: str) -> tuple[str | None, str | None]:
        if not burst_list:
            return None, None
        # Most recent burst of this sentiment
        b = max(burst_list, key=burst_start)
        if sentiment == "negative":
            candidates = sorted(
                b["reviews"],
                key=lambda r: float(r.get("rating", 5) or 5),
            )
        else:
            candidates = sorted(
                b["reviews"],
                key=lambda r: -float(r.get("rating", 0) or 0),
            )
        for r in candidates:
            snippet = extract_snippet(r)
            if snippet:
                return snippet, b["start_date"][:10]
        return None, b["start_date"][:10]

    neg_quote, neg_date = pick_sample_quote(negative_bursts, "negative")
    pos_quote, pos_date = pick_sample_quote(positive_bursts, "positive")

    return {
        "burst_total_count": total,
        "burst_negative_total": len(negative_bursts),
        "burst_positive_total": len(positive_bursts),
        "burst_recent_60d_count": len(recent_60),
        "burst_recent_negative_60d": len(recent_neg_60),
        "burst_recent_positive_60d": len(recent_pos_60),
        "burst_most_recent_date": most_recent["start_date"][:10] if most_recent else None,
        "burst_most_recent_avg_rating": most_recent["avg_rating"] if most_recent else None,
        "burst_most_recent_sentiment": most_recent["sentiment"] if most_recent else None,
        "burst_negative_sample_quote": neg_quote,
        "burst_negative_sample_date": neg_date,
        "burst_positive_sample_quote": pos_quote,
        "burst_positive_sample_date": pos_date,
        "burst_baseline_per_week": baseline.get("avg_per_week"),
        "burst_category": category,
    }


def load_reviews(place_id: str) -> list[dict]:
    if not place_id:
        return []
    path = RAW_REVIEWS_DIR / f"{place_id}.json"
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return []
    return data.get("reviews") or []


def main() -> None:
    if not POOL_CSV.exists():
        raise SystemExit(f"missing {POOL_CSV}")

    df = pd.read_csv(POOL_CSV)
    print(f"Pool: {len(df)} contractors")
    print()

    now = datetime.now(timezone.utc)
    results = []

    for _, row in df.iterrows():
        place_id = str(row.get("place_id") or "")
        reviews = load_reviews(place_id)
        bursts, baseline = detect_bursts(reviews)
        metrics = rollup_bursts(bursts, baseline, now)
        metrics["license_no"] = row["license_no"]
        results.append(metrics)

    res_df = pd.DataFrame(results)

    # Merge into pool
    burst_cols = [c for c in res_df.columns if c.startswith("burst_")]
    merged = df.copy()
    for col in burst_cols:
        if col not in merged.columns:
            merged[col] = None
    for r in results:
        mask = merged["license_no"] == r["license_no"]
        for col in burst_cols:
            merged.loc[mask, col] = r.get(col)

    merged.to_csv(POOL_CSV, index=False)
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    snap_path = SNAPSHOT_DIR / f"{now.strftime('%Y-%m-%d')}.csv"
    merged.to_csv(snap_path, index=False)

    # ---- Summary ----
    print("=" * 74)
    print("Summary")
    print("=" * 74)

    print()
    print("burst_category distribution:")
    for cat, count in res_df["burst_category"].value_counts().items():
        print(f"  {cat:<22} {count}")
    print()

    # The critical finds — active crisis and recent crisis
    crisis_cats = ["active_crisis", "recent_crisis"]
    crisis = merged[merged["burst_category"].isin(crisis_cats)].sort_values("burst_category")
    if len(crisis) > 0:
        print("=" * 74)
        print(f"CRISIS BURSTS — the priority targets ({len(crisis)} contractors)")
        print("=" * 74)
        for _, r in crisis.iterrows():
            print()
            print(f"rank {int(r['rank'])}  {r['business_name']}  ({r['city']})")
            print(f"  category: {r['burst_category']}")
            print(f"  most recent burst: {r['burst_most_recent_date']}  "
                  f"sentiment={r['burst_most_recent_sentiment']}  "
                  f"avg_rating={r['burst_most_recent_avg_rating']}")
            print(f"  baseline: {r['burst_baseline_per_week']} reviews/week | "
                  f"total bursts: {r['burst_total_count']} | "
                  f"negative: {r['burst_negative_total']}")
            q = r.get("burst_negative_sample_quote")
            if q:
                print(f"  sample negative quote ({r['burst_negative_sample_date']}):")
                print(f'     "{str(q)[:250]}"')
        print()

    # Scaling surge bursts — also interesting
    surge = merged[merged["burst_category"] == "scaling_surge"].sort_values("rank")
    if len(surge) > 0:
        print("=" * 74)
        print(f"SCALING SURGES — growth moment targets ({len(surge)} contractors)")
        print("=" * 74)
        for _, r in surge.iterrows():
            print()
            print(f"rank {int(r['rank'])}  {r['business_name']}  ({r['city']})")
            print(f"  burst date: {r['burst_most_recent_date']}  "
                  f"avg_rating={r['burst_most_recent_avg_rating']}")
            print(f"  baseline: {r['burst_baseline_per_week']}/week | "
                  f"positive bursts total: {r['burst_positive_total']}")
            q = r.get("burst_positive_sample_quote")
            if q:
                print(f"  sample positive quote ({r['burst_positive_sample_date']}):")
                print(f'     "{str(q)[:200]}"')
        print()

    # Historical bursts
    hist = merged[merged["burst_category"] == "historical_burst"]
    print(f"historical_burst contractors: {len(hist)}")
    steady = merged[merged["burst_category"] == "steady"]
    print(f"steady (no bursts): {len(steady)}")
    lowdata = merged[merged["burst_category"] == "low_data"]
    print(f"low_data (<{MIN_REVIEWS_FOR_ANALYSIS} reviews): {len(lowdata)}")

    print()
    print("Outputs:")
    print(f"  {POOL_CSV.relative_to(ROOT)}  (updated)")
    print(f"  {snap_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
