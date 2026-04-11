#!/usr/bin/env python3
"""
Dispatch delay extraction via Claude Haiku.

For each contractor, send their cached reviews to Claude with a prompt that
extracts EXPLICIT timeline information — phrases where the customer describes
how long it took between initial contact and the contractor arriving.

Examples of extractable timelines:
  "I called Monday and they came Friday"  -> 96 hours
  "They showed up within 2 hours"         -> 2 hours
  "Same day service"                       -> 8 hours
  "Next morning they were out"             -> 16 hours
  "Had to wait 3 weeks"                    -> 504 hours

Then aggregate per contractor (median, distribution, sentiment) and classify
into dispatch_fast / dispatch_moderate / dispatch_slow / dispatch_strained /
dispatch_bimodal / dispatch_low_data.

The signal is NEW: a quantitative customer-measured dispatch delay per
contractor, derived entirely from review text.

Usage:
  python pipeline/09_dispatch_delay.py --limit 5   # smoke test
  python pipeline/09_dispatch_delay.py             # full 70 run
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from anthropic import Anthropic
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parent.parent
POOL_CSV = ROOT / "data" / "03_hidden_gems" / "complete.csv"
RAW_REVIEWS_DIR = ROOT / "data" / "signals_raw" / "serpapi_reviews"
DISPATCH_RAW_DIR = ROOT / "data" / "signals_raw" / "dispatch_delay"
SNAPSHOT_DIR = ROOT / "data" / "snapshots" / "dispatch_delay"

ENV = dotenv_values(ROOT / ".env")
ANTHROPIC_KEY = ENV.get("ANTHROPIC_API_KEY")
MODEL = "claude-haiku-4-5-20251001"

INPUT_COST_PER_MTOK = 1.0
OUTPUT_COST_PER_MTOK = 5.0

MAX_REVIEWS_PER_CONTRACTOR = 40
RECENCY_WINDOW_DAYS = 180   # only analyze reviews from the last 6 months
REQUEST_SLEEP = 0.2

SYSTEM_PROMPT = """You are analyzing customer reviews of HVAC contractors to \
extract explicit dispatch-delay information — phrases where the customer \
describes how long it took between their initial request for service and \
the contractor arriving.

You are extracting QUANTITATIVE TIMELINES, not judging service quality.

Examples of extractable timelines:
- "I called Monday and they came Friday"         → 96 hours (4 days)
- "They showed up within 2 hours"                 → 2 hours
- "Same day service"                              → 8 hours
- "Next morning they were out"                    → 16 hours
- "Came out the next day"                         → 24 hours
- "Had to wait 3 weeks"                           → 504 hours (21 days)
- "Emergency service, arrived in 30 minutes"     → 0.5 hours
- "Booked 2 weeks out"                            → 336 hours
- "Took over two weeks to get it installed"      → 336 hours
- "Called in the morning, tech was here by noon" → 4 hours

CRITICAL RULES:
1. ONLY extract timelines that are EXPLICITLY stated or clearly implied by \
the customer's own words. Do NOT infer from tone or "feel".
2. DO NOT extract job duration ("they worked for 3 hours"). We care about \
dispatch time (call-to-arrival), not service-execution time.
3. If the customer does not mention how long it took to get service, skip \
that review entirely.
4. For vague phrasings ("they fit us in quickly"), skip unless the customer \
provides an actual time reference.
5. Treat "same day" as 8 hours (mid-day turnaround estimate).
6. Treat "next day" / "next morning" as 16-24 hours.
7. Treat "right away" / "immediately" as 1-4 hours if no specific number.
8. For each extraction, classify the customer's sentiment about the delay:
   - positive: customer praised the speed or tolerance ("worth the wait")
   - neutral: customer stated the delay without judgment
   - negative: customer complained about waiting

Return ONLY valid JSON. No markdown, no prose, no code fences."""

USER_PROMPT_TEMPLATE = """Contractor: {business_name}

Analyze the {review_count} customer reviews below. For each review that \
contains an explicit dispatch-timeline description, extract it. Skip reviews \
that do not mention how long it took to get service.

Return JSON with this exact structure:

{{
  "extractions": [
    {{
      "review_index": <1-based int matching [N] label below>,
      "rating": <star rating of the review>,
      "estimated_delay_hours": <float, conservative estimate in hours>,
      "delay_category": "emergency" | "same_day" | "next_day" | "same_week" \
| "1_to_2_weeks" | "2_plus_weeks",
      "sentiment": "positive" | "neutral" | "negative",
      "verbatim_quote": "<the customer's exact words describing the timeline>"
    }}
  ]
}}

Delay category thresholds:
  emergency:    < 4 hours
  same_day:     4 to 24 hours
  next_day:     24 to 48 hours
  same_week:    48 to 168 hours (2-7 days)
  1_to_2_weeks: 168 to 336 hours
  2_plus_weeks: > 336 hours

If no reviews contain extractable timelines, return {{"extractions": []}}.

REVIEWS:

{reviews_block}

Return JSON only."""


def load_cached_reviews(place_id: str) -> list[dict]:
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


def format_reviews_for_prompt(
    reviews: list[dict],
    max_reviews: int,
    recency_cutoff_days: int = RECENCY_WINDOW_DAYS,
) -> tuple[str, int, list[dict]]:
    """
    Filter reviews to the last `recency_cutoff_days` days, sort
    most-recent-first, and format with 1-based indices.

    Returns (formatted_block, count_used, indexed_reviews) where
    indexed_reviews mirrors what the LLM was shown. Callers can use
    review_index from the LLM response to look up each extraction's
    original dated review.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=recency_cutoff_days)

    recent: list[tuple[datetime, dict, str]] = []
    for r in reviews:
        iso = r.get("iso_date") or r.get("date") or ""
        if not iso:
            continue
        try:
            dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue
        if dt < cutoff:
            continue
        snippet = r.get("snippet") or ""
        if not snippet:
            ex = r.get("extracted_snippet") or {}
            if isinstance(ex, dict):
                snippet = ex.get("original") or ""
        if not snippet:
            continue
        recent.append((dt, r, snippet.strip()))

    recent.sort(key=lambda t: t[0], reverse=True)
    recent = recent[:max_reviews]

    lines = []
    indexed_reviews = []
    for i, (dt, r, snippet) in enumerate(recent, 1):
        rating = r.get("rating")
        rating_str = f"{rating}★" if rating else "?★"
        date_str = dt.date().isoformat()
        lines.append(f"[{i}] {rating_str} ({date_str}): {snippet}")
        indexed_reviews.append({
            "review_index": i,
            "date": date_str,
            "rating": rating,
            "snippet": snippet,
        })
    return "\n\n".join(lines), len(recent), indexed_reviews


def call_claude(
    client: Anthropic, business_name: str, reviews_block: str, review_count: int
) -> tuple[dict | None, dict]:
    user_prompt = USER_PROMPT_TEMPLATE.format(
        business_name=business_name,
        review_count=review_count,
        reviews_block=reviews_block,
    )

    try:
        resp = client.messages.create(
            model=MODEL,
            max_tokens=2048,
            temperature=0,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
    except Exception as e:
        return None, {"error": str(e)[:200]}

    usage = {
        "input_tokens": resp.usage.input_tokens,
        "output_tokens": resp.usage.output_tokens,
    }

    text = ""
    for block in resp.content:
        if hasattr(block, "text"):
            text += block.text
    text = text.strip()

    if text.startswith("```"):
        lines = text.split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as e:
        return {"parse_error": str(e)[:100], "raw_text": text[:500]}, usage

    return parsed, usage


def classify_dispatch_pattern(
    n: int,
    median: float,
    max_h: float,
    same_day_pct: float,
    week_plus_pct: float,
) -> str:
    """
    Classify a dispatch pattern from aggregated stats. Must stay aligned
    with classify_dispatch_pattern in 14_dossier_cards.py so the scored
    CSV and the dossier agree.

    Returns one of:
      dispatch_fast | dispatch_fast_outlier | dispatch_bimodal |
      dispatch_strained | dispatch_slow | dispatch_low_data

    Bimodal requires >= 25% same-day AND >= 25% week+. A single 336-hour
    outlier in a pool of 5 is NOT bimodal — it's "mostly fast with one
    slow outlier," which gets its own label.
    """
    if n < 2:
        return "dispatch_low_data"
    if median <= 24 and max_h <= 72:
        return "dispatch_fast"
    if same_day_pct >= 0.25 and week_plus_pct >= 0.25:
        return "dispatch_bimodal"
    if same_day_pct >= 0.6 and median <= 24:
        return "dispatch_fast_outlier"
    if median > 48 or max_h > 168:
        return "dispatch_strained"
    return "dispatch_slow"


def _match_quote_to_review(quote: str, reviews: list[dict]) -> dict | None:
    """
    Match a verbatim quote back to the cached review it came from. Tries
    exact substring first (fast, authoritative), then falls back to
    word-bag overlap on 4+ letter distinctive words. Keeps this aligned
    with find_review_date in 14_dossier_cards.py.
    """
    if not quote or not reviews:
        return None
    import re as _re
    norm_quote = _re.sub(r"\s+", " ", quote).strip()

    # Exact substring pass
    for probe_len in (120, 80, 40, 20):
        sub = norm_quote[:probe_len]
        if len(sub) < 10:
            continue
        for r in reviews:
            snippet = _re.sub(r"\s+", " ", (r.get("snippet") or "")).strip()
            if sub and sub in snippet:
                return r

    # Fuzzy fallback: word-bag overlap on distinctive 4+ letter words
    def distinctive_words(text: str) -> set[str]:
        words = _re.findall(r"[a-zA-Z]{4,}", text.lower())
        stop = {
            "that", "this", "with", "have", "from", "were", "they", "them",
            "their", "would", "could", "which", "when", "will", "been",
            "very", "just", "some", "into", "also", "than", "only", "about",
            "like", "what", "your", "there", "after", "other", "before",
        }
        return {w for w in words if w not in stop}

    qw = distinctive_words(norm_quote)
    if len(qw) < 3:
        return None
    best = None
    best_score = 0
    for r in reviews:
        rw = distinctive_words(r.get("snippet") or "")
        if not rw:
            continue
        inter = qw & rw
        if len(inter) >= max(3, int(len(qw) * 0.3)):
            score = len(inter)
            if score > best_score:
                best_score = score
                best = r
    return best


def filter_extractions_by_recency(
    extractions: list[dict],
    reviews: list[dict],
    cutoff_days: int = RECENCY_WINDOW_DAYS,
) -> list[dict]:
    """
    Drop any extraction whose verbatim_quote can't be matched to a cached
    review within the recency window. Uses the same substring + word-bag
    fallback as the render layer so both classifiers see identical data.
    """
    if not extractions or not reviews:
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(days=cutoff_days)

    kept = []
    for e in extractions:
        quote = e.get("verbatim_quote") or ""
        matched = _match_quote_to_review(quote, reviews)
        if not matched:
            continue
        iso = matched.get("iso_date") or matched.get("date") or ""
        if not iso:
            continue
        try:
            dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue
        if dt >= cutoff:
            kept.append(e)
    return kept


def aggregate_extractions(extractions: list[dict]) -> dict:
    """Compute per-contractor dispatch metrics from LLM extractions."""
    if not extractions:
        return {
            "dispatch_extractable_count": 0,
            "dispatch_median_hours": None,
            "dispatch_mean_hours": None,
            "dispatch_stdev_hours": None,
            "dispatch_min_hours": None,
            "dispatch_max_hours": None,
            "dispatch_same_day_pct": 0.0,
            "dispatch_week_plus_pct": 0.0,
            "dispatch_negative_sentiment_pct": 0.0,
            "dispatch_fastest_quote": None,
            "dispatch_slowest_quote": None,
            "dispatch_negative_quote": None,
            "dispatch_category": "dispatch_low_data",
        }

    delays = []
    fastest = None
    slowest = None
    negative_quote = None
    same_day_count = 0
    week_plus_count = 0
    negative_count = 0

    for e in extractions:
        hours = e.get("estimated_delay_hours")
        if hours is None:
            continue
        try:
            hours = float(hours)
        except (TypeError, ValueError):
            continue
        if hours < 0:
            continue

        delays.append((hours, e))

        if e.get("sentiment") == "negative":
            negative_count += 1
            if negative_quote is None:
                negative_quote = e.get("verbatim_quote")

    n = len(delays)
    if n == 0:
        return aggregate_extractions([])

    delays.sort(key=lambda x: x[0])
    fastest = delays[0][1].get("verbatim_quote")
    slowest = delays[-1][1].get("verbatim_quote")

    hours_only = [d[0] for d in delays]
    median_h = statistics.median(hours_only)
    mean_h = statistics.mean(hours_only)
    stdev_h = statistics.stdev(hours_only) if n >= 2 else 0.0
    min_h = min(hours_only)
    max_h = max(hours_only)

    # Compute percentages from raw hours to stay identical with the render
    # layer in 14_dossier_cards.py. Previously this used the LLM's
    # delay_category string which could drift from the numeric threshold.
    same_day_count = sum(1 for h in hours_only if h <= 24)
    week_plus_count = sum(1 for h in hours_only if h >= 168)
    same_day_pct = same_day_count / n
    week_plus_pct = week_plus_count / n
    negative_pct = negative_count / n

    category = classify_dispatch_pattern(n, median_h, max_h, same_day_pct, week_plus_pct)

    return {
        "dispatch_extractable_count": n,
        "dispatch_median_hours": round(median_h, 1),
        "dispatch_mean_hours": round(mean_h, 1),
        "dispatch_stdev_hours": round(stdev_h, 1),
        "dispatch_min_hours": round(min_h, 1),
        "dispatch_max_hours": round(max_h, 1),
        "dispatch_same_day_pct": round(same_day_pct, 2),
        "dispatch_week_plus_pct": round(week_plus_pct, 2),
        "dispatch_negative_sentiment_pct": round(negative_pct, 2),
        "dispatch_fastest_quote": fastest,
        "dispatch_slowest_quote": slowest,
        "dispatch_negative_quote": negative_quote,
        "dispatch_category": category,
    }


def cost_from_usage(usage: dict) -> float:
    inp = usage.get("input_tokens", 0)
    out = usage.get("output_tokens", 0)
    return (inp / 1_000_000) * INPUT_COST_PER_MTOK + (out / 1_000_000) * OUTPUT_COST_PER_MTOK


def main() -> None:
    if not ANTHROPIC_KEY:
        sys.exit("ANTHROPIC_API_KEY missing from .env")
    if not POOL_CSV.exists():
        sys.exit(f"missing {POOL_CSV}")

    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="Smoke test: first N contractors")
    ap.add_argument("--ranks", type=str, default=None, help="Comma-sep ranks")
    args = ap.parse_args()

    df = pd.read_csv(POOL_CSV)
    if args.ranks:
        rank_list = [int(r.strip()) for r in args.ranks.split(",")]
        sample = df[df["rank"].isin(rank_list)].copy()
    elif args.limit:
        sample = df.head(args.limit).copy()
    else:
        sample = df.copy()

    print(f"Pool: {len(df)} contractors")
    print(f"Processing: {len(sample)}")
    print(f"Model: {MODEL}")
    print()

    client = Anthropic(api_key=ANTHROPIC_KEY)
    DISPATCH_RAW_DIR.mkdir(parents=True, exist_ok=True)

    results: list[dict] = []
    total_cost = 0.0
    api_calls = 0
    cache_hits = 0
    parse_errors = 0

    for i, row in enumerate(sample.to_dict(orient="records"), 1):
        place_id = str(row.get("place_id") or "")
        business_name = str(row.get("business_name") or "")
        rank = row.get("rank")
        license_no = row.get("license_no")

        cached_reviews = load_cached_reviews(place_id)
        if not cached_reviews:
            print(f"  [{i:>2}/{len(sample)}] rank={rank}  {business_name[:34]:<34}  NO REVIEWS")
            results.append({"license_no": license_no, **aggregate_extractions([])})
            continue

        reviews_block, review_count, indexed_reviews = format_reviews_for_prompt(
            cached_reviews, MAX_REVIEWS_PER_CONTRACTOR, RECENCY_WINDOW_DAYS
        )
        if review_count == 0:
            print(
                f"  [{i:>2}/{len(sample)}] rank={rank}  {business_name[:34]:<34}  "
                f"NO REVIEWS IN LAST {RECENCY_WINDOW_DAYS}d"
            )
            results.append({"license_no": license_no, **aggregate_extractions([])})
            continue

        cache_path = DISPATCH_RAW_DIR / f"{place_id}.json"
        from_cache = False
        usage: dict = {}

        if cache_path.exists():
            try:
                cached_payload = json.loads(cache_path.read_text())
                # Old schema caches don't have a recency_window_days field —
                # treat them as stale so the next run refreshes cleanly.
                if cached_payload.get("recency_window_days") != RECENCY_WINDOW_DAYS:
                    llm_result = None
                else:
                    llm_result = cached_payload.get("parsed")
                    usage = cached_payload.get("usage", {})
                    from_cache = True
                    cache_hits += 1
            except (json.JSONDecodeError, OSError):
                llm_result = None

        if not from_cache:
            llm_result, usage = call_claude(
                client, business_name, reviews_block, review_count
            )
            api_calls += 1
            if llm_result is not None and "parse_error" not in llm_result:
                cache_path.write_text(json.dumps({
                    "business_name": business_name,
                    "license_no": int(license_no) if pd.notna(license_no) else None,
                    "place_id": place_id,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "recency_window_days": RECENCY_WINDOW_DAYS,
                    "review_count_fed": review_count,
                    "indexed_reviews": indexed_reviews,
                    "usage": usage,
                    "parsed": llm_result,
                }, indent=2))

        call_cost = cost_from_usage(usage)
        if not from_cache:
            total_cost += call_cost

        if llm_result is None or "parse_error" in llm_result:
            parse_errors += 1
            print(f"  [{i:>2}/{len(sample)}] rank={rank}  {business_name[:34]:<34}  ERROR")
            results.append({"license_no": license_no, **aggregate_extractions([])})
            continue

        extractions = llm_result.get("extractions", []) or []

        # Cached extractions from earlier runs may include reviews older
        # than the current recency window (the cache predates the 180-day
        # filter). Apply the filter defensively before aggregating so the
        # CSV category and the dossier card always agree.
        extractions = filter_extractions_by_recency(
            extractions, cached_reviews, RECENCY_WINDOW_DAYS
        )

        aggregate = aggregate_extractions(extractions)
        aggregate["license_no"] = license_no

        results.append(aggregate)

        n = aggregate["dispatch_extractable_count"]
        median_h = aggregate["dispatch_median_hours"]
        cat = aggregate["dispatch_category"]
        cached_str = " (cached)" if from_cache else f" ${call_cost:.4f}"
        median_str = f"{median_h}h" if median_h is not None else "-"
        neg_pct = int(aggregate["dispatch_negative_sentiment_pct"] * 100)
        print(
            f"  [{i:>2}/{len(sample)}] rank={str(rank):<3}  "
            f"{business_name[:34]:<34}  "
            f"n={n:<2} median={median_str:<8} neg={neg_pct}%  "
            f"{cat:<20}{cached_str}"
        )

        if not from_cache:
            time.sleep(REQUEST_SLEEP)

    # Merge into pool
    res_df = pd.DataFrame(results)
    dispatch_cols = [c for c in res_df.columns if c.startswith("dispatch_")]

    merged = df.copy()
    for col in dispatch_cols:
        if col not in merged.columns:
            merged[col] = None
    for r in results:
        mask = merged["license_no"] == r["license_no"]
        for col in dispatch_cols:
            if col in r:
                merged.loc[mask, col] = r.get(col)

    merged.to_csv(POOL_CSV, index=False)

    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    snap_path = SNAPSHOT_DIR / f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.csv"
    merged.to_csv(snap_path, index=False)

    # ---- Summary ----
    print()
    print("=" * 74)
    print("Summary")
    print("=" * 74)
    print(f"API calls made:     {api_calls}")
    print(f"Cache hits:         {cache_hits}")
    print(f"Parse errors:       {parse_errors}")
    print(f"Cost this run:      ${total_cost:.4f}")
    print()

    if "dispatch_category" in res_df.columns:
        print("dispatch_category distribution:")
        for cat, c in res_df["dispatch_category"].value_counts().items():
            print(f"  {cat:<25} {c}")
        print()

        # Contractors with extracted timelines — top 10 fastest
        with_data = res_df[res_df["dispatch_extractable_count"] >= 3].copy()
        if len(with_data) > 0:
            with_data_median = with_data.sort_values("dispatch_median_hours")
            print("Top 10 FASTEST dispatch (median hours, n>=3):")
            for _, r in with_data_median.head(10).iterrows():
                lic = r["license_no"]
                biz = df[df["license_no"] == lic]["business_name"].iloc[0] if any(df["license_no"] == lic) else "?"
                rank_val = df[df["license_no"] == lic]["rank"].iloc[0] if any(df["license_no"] == lic) else "?"
                q = (r["dispatch_fastest_quote"] or "")[:70]
                print(f"  rank={int(rank_val):<3}  median={r['dispatch_median_hours']}h  n={int(r['dispatch_extractable_count'])}  {biz[:30]:<30}")
                if q:
                    print(f"     fast: \"{q}\"")
            print()

            with_data_slow = with_data.sort_values("dispatch_median_hours", ascending=False)
            print("Top 10 SLOWEST dispatch (median hours, n>=3):")
            for _, r in with_data_slow.head(10).iterrows():
                lic = r["license_no"]
                biz = df[df["license_no"] == lic]["business_name"].iloc[0] if any(df["license_no"] == lic) else "?"
                rank_val = df[df["license_no"] == lic]["rank"].iloc[0] if any(df["license_no"] == lic) else "?"
                q = (r["dispatch_slowest_quote"] or "")[:70]
                print(f"  rank={int(rank_val):<3}  median={r['dispatch_median_hours']}h  n={int(r['dispatch_extractable_count'])}  {biz[:30]:<30}")
                if q:
                    print(f"     slow: \"{q}\"")
            print()

            strained = res_df[res_df["dispatch_category"] == "dispatch_strained"]
            if len(strained) > 0:
                print(f"Dispatch-strained contractors ({len(strained)}):")
                for _, r in strained.iterrows():
                    lic = r["license_no"]
                    biz = df[df["license_no"] == lic]["business_name"].iloc[0] if any(df["license_no"] == lic) else "?"
                    rank_val = df[df["license_no"] == lic]["rank"].iloc[0] if any(df["license_no"] == lic) else "?"
                    print(f"  rank={int(rank_val):<3}  {biz[:38]:<38}  median={r['dispatch_median_hours']}h  neg_pct={int(r['dispatch_negative_sentiment_pct']*100)}%")
                    neg_q = r.get("dispatch_negative_quote")
                    if isinstance(neg_q, str) and neg_q:
                        print(f"     \"{neg_q[:100]}\"")
                print()

    print("Outputs:")
    print(f"  {POOL_CSV.relative_to(ROOT)}  (updated)")
    print(f"  {snap_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
