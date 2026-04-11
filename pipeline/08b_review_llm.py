#!/usr/bin/env python3
"""
LLM-based review sentiment + buying-signal analysis via Claude Haiku 4.5.

Runs alongside the regex-based 08_review_nlp.py as a second independent
classifier. Results are merged as `llm_*` columns into the existing pool
CSV, and cross-validation columns (`signal_agreement`, `signal_confidence`)
measure where regex and LLM agree.

For each contractor, reads cached review text from data/serpapi_raw/
and asks Claude to return a structured JSON assessment of operational
buying signals. Output is cached per-contractor in data/review_llm_raw/
so re-runs are free.

Usage:
  python pipeline/08b_review_llm.py --limit 5    # smoke test
  python pipeline/08b_review_llm.py              # full 70-contractor run
  python pipeline/08b_review_llm.py --contractors "Comfort Experts LLC,Grand Canyon Home Services LLC"
"""

from __future__ import annotations

import argparse
import json
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
LLM_RAW_DIR = ROOT / "data" / "signals_raw" / "review_llm"
OUT_CSV = ROOT / "data" / "03_hidden_gems" / "complete.csv"
SNAPSHOT_DIR = ROOT / "data" / "snapshots" / "review_llm"

ENV = dotenv_values(ROOT / ".env")
ANTHROPIC_KEY = ENV.get("ANTHROPIC_API_KEY")
MODEL = "claude-haiku-4-5-20251001"

# Haiku 4.5 pricing
INPUT_COST_PER_MTOK = 1.0
OUTPUT_COST_PER_MTOK = 5.0

MAX_REVIEWS_PER_CONTRACTOR = 40
RECENCY_WINDOW_DAYS = 180   # only look at reviews from the last 6 months
MAX_MENTIONS_PER_CATEGORY = 8
REQUEST_SLEEP = 0.2

SYSTEM_PROMPT = """You are a sales intelligence analyst. Your job is to read \
customer reviews of HVAC contractors and decide whether each contractor is \
experiencing operational pain or scaling strain — the conditions that drive \
Field Service Management (FSM) software purchases (ServiceTitan, Housecall \
Pro, Jobber, etc.).

You are looking for four distinct signal types:

1. PAIN SIGNALS — customers explicitly complaining about operational \
failures: no-shows, missed appointments, rescheduling, unanswered phones, \
long waits, rushed service, poor communication, lost appointments, lack of \
follow-up.

2. MOMENTUM SIGNALS (scaling strain without explicit complaints) — customers \
praising the shop while revealing that demand is outpacing infrastructure: \
"booked solid", "busy schedule", "took two weeks", "the owner came out \
himself", "finally found after trying several others".

3. SWITCHER SIGNALS — customers who explicitly describe leaving a different \
HVAC contractor to come to this one. Look for phrases like "switched from", \
"tried several others", "our previous company", "after being burned by", \
"fired our old", "went through a few companies before finding".

4. SMOOTH OPS SIGNALS (disqualifying) — customers praising OPERATIONAL \
SMOOTHNESS: text reminders, online booking, arrival ETAs, automated \
confirmations, technician tracking. These contractors have already bought \
FSM software and are NOT buying targets.

CRITICAL RULES:
- Base your findings strictly on the reviews you are given.
- For each mention you return, copy the customer language VERBATIM. Do not \
paraphrase or summarize. Copy a complete sentence (or two) that contains \
the signal.
- Each mention must reference its review number from the prompt.
- The reviews are sorted most-recent-first. When a category has more than \
{max_mentions} candidates, keep the {max_mentions} most recent (the \
lowest-numbered reviews in the prompt).
- Positive praise for a technician's friendliness, skill, or fair pricing \
is NOT a momentum signal unless it also reveals demand pressure, founder \
involvement, or customer-switcher status.

Return ONLY valid JSON. No markdown, no prose, no code fences."""

USER_PROMPT_TEMPLATE = """Contractor: {business_name}

Analyze the {review_count} recent customer reviews below and return a JSON \
assessment. The reviews are sorted most-recent-first: review [1] is the \
newest, review [{review_count}] is the oldest.

Follow this exact schema:

{{
  "pain_score": <int 0-10, overall intensity of operational-pain complaints>,
  "momentum_score": <int 0-10, overall intensity of scaling-strain signals>,
  "smooth_ops_score": <int 0-10, how much customers praise already-solved operations>,
  "buying_category": <"active_pain" | "scaling_strain" | "mixed_conviction" | "smooth_ops" | "low_signal">,
  "founder_involvement": <"heavy" | "moderate" | "none">,
  "key_person_dependency": <"heavy" | "moderate" | "none">,
  "one_sentence_summary": <string, 1-2 sentences suitable for a sales dossier describing the contractor's operational state>,
  "review_count_analyzed": <int, how many reviews you actually considered>,

  "pain_mentions": [
    {{
      "review_index": <int, the number in brackets from the prompt>,
      "quote": <string, verbatim customer language — full sentence>,
      "subtype": <"dispatch" | "communication" | "capacity" | "quality" | "billing" | "other">
    }}
  ],

  "momentum_mentions": [
    {{
      "review_index": <int>,
      "quote": <string, verbatim>,
      "subtype": <"demand_pressure" | "founder_owned" | "key_person" | "long_wait" | "capacity_strain" | "other">
    }}
  ],

  "switcher_mentions": [
    {{
      "review_index": <int>,
      "quote": <string, verbatim — include the sentence that describes switching>,
      "prior_company_hint": <string, any name or description of the prior contractor, or empty string>
    }}
  ],

  "smooth_ops_mentions": [
    {{
      "review_index": <int>,
      "quote": <string, verbatim>,
      "subtype": <"online_booking" | "text_reminders" | "technician_tracking" | "eta_notifications" | "automated_confirmation" | "other">
    }}
  ]
}}

CAPS:
- Each mention array capped at {max_mentions} entries.
- If a category has zero evidence, return an empty array [].

REVIEWS (sorted most-recent-first):

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
    Filter reviews to the last `recency_cutoff_days` days, sort most-recent
    first, and format with 1-based indices.

    Returns (formatted_block, count_used, indexed_reviews) where
    indexed_reviews is the exact list the LLM was shown (same order), so
    downstream code can look up `review_index` directly.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=recency_cutoff_days)

    # Filter to reviews inside the recency window AND that have usable snippets
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

    # Sort most-recent-first
    recent.sort(key=lambda t: t[0], reverse=True)
    recent = recent[:max_reviews]

    lines = []
    indexed_reviews = []
    for i, (dt, r, snippet) in enumerate(recent, 1):
        rating = r.get("rating")
        rating_str = f"{rating}★" if rating else "?★"
        date_str = dt.date().isoformat()
        lines.append(f"[{i}] {rating_str} ({date_str}): {snippet}")
        # Normalize the review record so downstream code has consistent keys
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
    """Call Claude with the review analysis prompt. Returns (parsed, usage_dict)."""
    user_prompt = USER_PROMPT_TEMPLATE.format(
        business_name=business_name,
        review_count=review_count,
        reviews_block=reviews_block,
        max_mentions=MAX_MENTIONS_PER_CATEGORY,
    )
    system_prompt = SYSTEM_PROMPT.format(max_mentions=MAX_MENTIONS_PER_CATEGORY)

    try:
        resp = client.messages.create(
            model=MODEL,
            max_tokens=2048,
            temperature=0,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
    except Exception as e:
        return None, {"error": str(e)[:200]}

    usage = {
        "input_tokens": resp.usage.input_tokens,
        "output_tokens": resp.usage.output_tokens,
    }

    # Extract text from response
    text = ""
    for block in resp.content:
        if hasattr(block, "text"):
            text += block.text

    # Strip potential markdown code fences (defensive)
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


def validate_llm_result(result: dict) -> bool:
    """Sanity-check that required fields exist."""
    required = {
        "pain_score", "momentum_score", "smooth_ops_score", "buying_category",
        "founder_involvement", "key_person_dependency", "one_sentence_summary",
        "pain_mentions", "momentum_mentions", "switcher_mentions",
        "smooth_ops_mentions",
    }
    if not all(k in result for k in required):
        return False
    for k in ("pain_mentions", "momentum_mentions", "switcher_mentions", "smooth_ops_mentions"):
        if not isinstance(result.get(k), list):
            return False
    return True


def compute_agreement(regex_cat: str, llm_cat: str) -> tuple[str, str]:
    """Return (signal_agreement, signal_confidence)."""
    strong_cats = {"active_pain", "scaling_strain", "mixed_conviction"}
    if regex_cat == llm_cat:
        if llm_cat in strong_cats:
            return "both", "high"
        if llm_cat == "smooth_ops":
            return "both", "high"
        return "both", "medium"  # low_signal or light_signal etc.

    regex_strong = regex_cat in strong_cats
    llm_strong = llm_cat in strong_cats

    if regex_strong and not llm_strong:
        return "regex_only", "low"
    if llm_strong and not regex_strong:
        return "llm_only", "medium"
    return "disagree", "low"


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
    ap.add_argument("--limit", type=int, default=None, help="Smoke test: process first N contractors")
    ap.add_argument("--contractors", type=str, default=None, help="Comma-separated business names to process")
    ap.add_argument("--ranks", type=str, default=None, help="Comma-separated ranks to process (e.g., 1,8,25,35,60)")
    args = ap.parse_args()

    df = pd.read_csv(POOL_CSV)
    original_len = len(df)

    if args.contractors:
        names = [n.strip() for n in args.contractors.split(",")]
        sample = df[df["business_name"].isin(names)].copy()
    elif args.ranks:
        rank_list = [int(r.strip()) for r in args.ranks.split(",")]
        sample = df[df["rank"].isin(rank_list)].copy()
    elif args.limit:
        sample = df.head(args.limit).copy()
    else:
        sample = df.copy()

    print(f"Pool: {original_len} contractors")
    print(f"Processing: {len(sample)}")
    print(f"Model: {MODEL}")
    print()

    client = Anthropic(api_key=ANTHROPIC_KEY)
    LLM_RAW_DIR.mkdir(parents=True, exist_ok=True)

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
        regex_category = str(row.get("buying_category") or "low_signal")

        cached_reviews = load_cached_reviews(place_id)
        if not cached_reviews:
            print(f"  [{i:>2}/{len(sample)}] rank={rank}  {business_name[:35]:<35}  NO REVIEWS CACHED")
            results.append({
                "license_no": license_no,
                "llm_status": "no_reviews",
            })
            continue

        reviews_block, review_count, indexed_reviews = format_reviews_for_prompt(
            cached_reviews, MAX_REVIEWS_PER_CONTRACTOR, RECENCY_WINDOW_DAYS
        )
        if review_count == 0:
            print(
                f"  [{i:>2}/{len(sample)}] rank={rank}  {business_name[:35]:<35}  "
                f"NO REVIEWS IN LAST {RECENCY_WINDOW_DAYS}d"
            )
            results.append({
                "license_no": license_no,
                "llm_status": "no_recent_reviews",
            })
            continue

        cache_path = LLM_RAW_DIR / f"{place_id}.json"
        from_cache = False
        usage: dict = {}
        cached_indexed_reviews: list[dict] = []

        if cache_path.exists():
            try:
                cached_payload = json.loads(cache_path.read_text())
                llm_result = cached_payload.get("parsed")
                usage = cached_payload.get("usage", {})
                cached_indexed_reviews = cached_payload.get("indexed_reviews") or []
                # Old schema caches won't have pain_mentions — treat as stale
                if llm_result and "pain_mentions" not in llm_result:
                    llm_result = None
                else:
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
                    "max_mentions_per_category": MAX_MENTIONS_PER_CATEGORY,
                    "review_count_fed": review_count,
                    "indexed_reviews": indexed_reviews,
                    "usage": usage,
                    "parsed": llm_result,
                }, indent=2))

        call_cost = cost_from_usage(usage)
        if not from_cache:
            total_cost += call_cost

        if llm_result is None:
            print(f"  [{i:>2}/{len(sample)}] rank={rank}  {business_name[:35]:<35}  API_ERROR {usage.get('error','')[:50]}")
            results.append({"license_no": license_no, "llm_status": "api_error"})
            continue

        if "parse_error" in llm_result:
            parse_errors += 1
            print(f"  [{i:>2}/{len(sample)}] rank={rank}  {business_name[:35]:<35}  PARSE_ERROR")
            results.append({"license_no": license_no, "llm_status": "parse_error"})
            continue

        if not validate_llm_result(llm_result):
            print(f"  [{i:>2}/{len(sample)}] rank={rank}  {business_name[:35]:<35}  SCHEMA_MISSING")
            results.append({"license_no": license_no, "llm_status": "schema_missing"})
            continue

        # Extract fields
        llm_cat = str(llm_result.get("buying_category", "low_signal"))
        pain = int(llm_result.get("pain_score", 0) or 0)
        mom = int(llm_result.get("momentum_score", 0) or 0)
        smooth = int(llm_result.get("smooth_ops_score", 0) or 0)

        pain_mentions = llm_result.get("pain_mentions") or []
        momentum_mentions = llm_result.get("momentum_mentions") or []
        switcher_mentions = llm_result.get("switcher_mentions") or []
        smooth_ops_mentions = llm_result.get("smooth_ops_mentions") or []

        # Derive rollup scalars from mention arrays. Count is authoritative —
        # never disagrees with what the dossier will render.
        refugee_count = len(switcher_mentions)
        first_pain_quote = pain_mentions[0].get("quote") if pain_mentions else None
        first_mom_quote = momentum_mentions[0].get("quote") if momentum_mentions else None
        first_smooth_quote = smooth_ops_mentions[0].get("quote") if smooth_ops_mentions else None
        first_switcher_quote = switcher_mentions[0].get("quote") if switcher_mentions else None

        agreement, confidence = compute_agreement(regex_category, llm_cat)

        cached_str = " (cached)" if from_cache else f" ${call_cost:.4f}"
        print(
            f"  [{i:>2}/{len(sample)}] rank={str(rank):<3}  "
            f"{business_name[:32]:<32}  "
            f"regex={regex_category[:14]:<14}  llm={llm_cat[:14]:<14}  "
            f"p={pain}({len(pain_mentions)}) m={mom}({len(momentum_mentions)}) "
            f"sw={len(switcher_mentions)} s={smooth}  [{agreement}/{confidence}]{cached_str}"
        )

        results.append({
            "license_no": license_no,
            "llm_status": "ok",
            "llm_pain_score": pain,
            "llm_momentum_score": mom,
            "llm_smooth_ops_score": smooth,
            "llm_buying_category": llm_cat,
            "llm_founder_involvement": llm_result.get("founder_involvement"),
            "llm_key_person_dependency": llm_result.get("key_person_dependency"),
            "llm_customer_refugee_mentions": refugee_count,
            "llm_pain_mention_count": len(pain_mentions),
            "llm_momentum_mention_count": len(momentum_mentions),
            "llm_smooth_ops_mention_count": len(smooth_ops_mentions),
            "llm_one_sentence_summary": llm_result.get("one_sentence_summary"),
            "llm_pain_evidence": first_pain_quote,
            "llm_momentum_evidence": first_mom_quote,
            "llm_smooth_ops_evidence": first_smooth_quote,
            "positive_switch_sample_quote": first_switcher_quote,
            "llm_review_count_analyzed": llm_result.get("review_count_analyzed"),
            "signal_agreement": agreement,
            "signal_confidence": confidence,
        })

        if not from_cache:
            time.sleep(REQUEST_SLEEP)

    # ---- Merge results back into pool ----
    res_df = pd.DataFrame(results)

    # Every column from res_df except license_no is an LLM-derived column
    # we want to write back onto the pool. We must OVERWRITE pre-existing
    # columns — not let pandas append '_new' suffixes — so downstream
    # scripts never see stale values from an earlier partial run.
    result_cols = [c for c in res_df.columns if c != "license_no"]

    if args.limit or args.contractors or args.ranks:
        # Smoke test: only update the sampled rows. Leave unsampled rows'
        # existing llm_* values (from prior full runs) intact.
        merged = df.copy()
        for col in result_cols:
            if col not in merged.columns:
                merged[col] = None
        for r in results:
            mask = merged["license_no"] == r["license_no"]
            for col in result_cols:
                if col in r:
                    merged.loc[mask, col] = r.get(col)
    else:
        # Full run: drop any pre-existing LLM columns from the pool first,
        # then merge cleanly. This guarantees the merged DataFrame has
        # exactly one copy of every llm_* column, populated from this run.
        cols_to_drop = [c for c in result_cols if c in df.columns]
        left = df.drop(columns=cols_to_drop)
        merged = left.merge(res_df, on="license_no", how="left")

    # Also scrub any leftover '_new' suffixed columns that a previous buggy
    # run may have persisted into the pool CSV.
    stale_suffixed = [c for c in merged.columns if c.endswith("_new")]
    if stale_suffixed:
        merged = merged.drop(columns=stale_suffixed)

    merged.to_csv(OUT_CSV, index=False)

    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    snap_path = (
        SNAPSHOT_DIR / f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.csv"
    )
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

    ok_rows = res_df[res_df["llm_status"] == "ok"] if "llm_status" in res_df.columns else pd.DataFrame()
    if len(ok_rows) > 0:
        print(f"LLM buying_category distribution (n={len(ok_rows)}):")
        for cat, c in ok_rows["llm_buying_category"].value_counts().items():
            print(f"  {cat:<20} {c}")
        print()

        print("Regex vs LLM cross-validation:")
        for agree, c in ok_rows["signal_agreement"].value_counts().items():
            print(f"  {agree:<15} {c}")
        print()

        print(f"Agreement cases (both classifiers matched):")
        both = ok_rows[ok_rows["signal_agreement"] == "both"].head(10)
        for _, r in both.iterrows():
            print(f"  {r['llm_buying_category']:<18} p={r['llm_pain_score']} m={r['llm_momentum_score']} s={r['llm_smooth_ops_score']}")

        llm_only = ok_rows[ok_rows["signal_agreement"] == "llm_only"]
        if len(llm_only) > 0:
            print()
            print(f"LLM-only targets (regex missed, worth reviewing):")
            for _, r in llm_only.head(10).iterrows():
                summary = (r.get("llm_one_sentence_summary") or "")[:100]
                print(f"  {r['llm_buying_category']:<18} {summary}")

    print()
    print("Outputs:")
    print(f"  {OUT_CSV.relative_to(ROOT)}")
    print(f"  {snap_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
