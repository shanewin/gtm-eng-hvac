#!/usr/bin/env python3
"""
Review text NLP for buying-signal extraction.

Reads cached SerpAPI review JSONs (one per contractor) from
data/serpapi_raw/, runs 9-category regex matching on every review snippet,
rolls up per-contractor metrics, and classifies each contractor into a
buying_category.

Nine categories, grouped into three signal types:

  PAIN (negative — customers complaining about operations):
    - pain_dispatch:  no-shows, rescheduling, lost appointments
    - pain_comms:     unanswered phones, missed callbacks, no confirmations
    - pain_capacity:  rushed, short-staffed, in-and-out

  MOMENTUM (positive — customers praising in ways that reveal strain):
    - positive_demand:     "booked solid", "hard to get", "worth the wait"
    - positive_founder:    "the owner came out himself"
    - positive_key_person: "ask for [name]", "[name] is the best"
    - positive_switch:     "finally found", "after trying several"
    - positive_growth:     "they've grown", "new trucks"

  CONTROL (positive — customers praising *solved* ops, i.e. NOT a buying
           target because they already have the tools):
    - control_positive:    text reminders, ETA tracking, online booking

Zero API cost. Runs against all 70 hidden gems from cached JSON.
"""

from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
POOL_CSV = ROOT / "data" / "03_hidden_gems" / "with_hiring.csv"
RAW_DIR = ROOT / "data" / "signals_raw" / "serpapi_reviews"
OUT_CSV = ROOT / "data" / "03_hidden_gems" / "complete.csv"
SNAPSHOT_DIR = ROOT / "data" / "snapshots" / "review_nlp"

# ---- Pattern libraries (case-insensitive, phrase-level) ----

PAIN_DISPATCH = [
    r"\bno[- ]?show(?:ed|s|\.)?\b",
    r"\bnever showed(?:\s+up)?\b",
    r"\bdidn'?t show(?:\s+up)?\b",
    r"\bdid not show(?:\s+up)?\b",
    r"\brescheduled (?:twice|three times|multiple|several|many)",
    r"\bkept rescheduling\b",
    r"\bcancell?ed (?:on (?:me|us)|at the last minute|our appointment|my appointment)",
    r"\b(?:they|company) cancell?ed\b",
    r"\blost (?:my|our|the) appointment\b",
    r"\bforgot (?:my|our|the|about) (?:appointment|job)\b",
    r"\bdouble[- ]?booked?\b",
    r"\bno one (?:came|showed|called|returned)",
    r"\bnobody (?:came|showed|called|returned)",
    r"\b(?:waited|waiting)(?:\s+for)?\s+(?:3|three|4|four|5|five|6|six|7|seven|\d+)\s+(?:hours|days|weeks)\b",
    r"\bwaited all day\b",
    r"\bwaited weeks?\b",
    r"\bweeks of waiting\b",
    r"\b(?:hours|days|weeks) late\b",
    r"\b(?:over )?an? hour late\b",
    r"\bmissed (?:the|my|our) appointment\b",
    r"\bstood (?:me|us) up\b",
    r"\bflaked on\b",
    # New: wait-length complaints (pain patterns filtered by rating <= 3,
    # so these only fire on negative reviews even though a similar phrase
    # could appear in a 4-5 star review praising patience)
    r"\btook (?:over )?(?:two|three|four|several|\d+) weeks to",
    r"\bstill waiting\b",
    r"\btook forever\b",
]

PAIN_COMMS = [
    r"\bcouldn'?t (?:get (?:through|a hold|ahold|in touch)|reach (?:them|anyone|anybody))",
    r"\bcould not (?:get (?:through|a hold|ahold|in touch)|reach (?:them|anyone|anybody))",
    r"\bno (?:response|callback|call[- ]?back|reply)\b",
    r"\bnever (?:called (?:me )?back|got back (?:to me|to us)|responded|returned (?:my|our) call|heard back)",
    r"\bnobody (?:answer(?:s|ed)|picks up|picked up|returned)",
    r"\bno one (?:answer(?:s|ed)|picks up|picked up|returned)",
    # New loosened patterns (from real review grep):
    r"\bdidn'?t (?:return|respond (?:to)?|call (?:me |us )?back|get back)",
    r"\bwouldn'?t (?:return|respond|call (?:me |us )?back|get back)",
    r"\bphone (?:just rings|goes straight to voicemail)",
    r"\bstraight to voicemail\b",
    r"\bpoor communication\b",
    r"\black of communication\b",
    r"\bno communication\b",
    r"\bhad to call (?:multiple|several|three|four|five|many|\d+)\s*times\b",
    r"\bkept calling (?:them|the office)",
    r"\bleft (?:multiple|several|\d+) (?:messages|voicemails?|voice mails?)\b",
    r"\bno (?:confirmation|confirm)",
    r"\bno (?:text|email|phone) (?:reminder|confirmation|notification)",
    r"\bterrible communication\b",
    r"\bhorrible communication\b",
    r"\bunresponsive\b",
    r"\bno follow[- ]?up\b",
]

PAIN_CAPACITY = [
    r"\bshort[- ]?(?:staffed|handed)\b",
    r"\bunderstaffed\b",
    r"\bstretched thin\b",
    r"\brushed through\b",
    r"\brushed (?:the|his|her|their) (?:job|work|service)",
    # Removed "in and out in N minutes" — false positive: customers praise
    # fast service ("in and out in 10 minutes, AC working great") and that
    # phrase pattern-matched pain_capacity when it's actually positive.
    r"\bdidn'?t (?:have time|take the time) to",
    # Narrowed from "too busy to|for" to require a specific action noun
    # after "to" so we don't match "too busy to come" = neutral wait mention.
    r"\btoo busy (?:to (?:do|finish|respond|properly|properly do|call back)|for a)",
    r"\bspread (?:too )?thin\b",
    r"\boverwhelmed\b",
]

POSITIVE_DEMAND = [
    # Rebuilt from grep of actual Phoenix HVAC reviews.
    # "booked solid" literally never appears — customers say:
    r"\bbusy schedule\b",
    r"\bbusy season\b",
    r"\bvery busy\b",
    # "took over two weeks to get it installed" — real phrase from Hub Heating
    r"\btook (?:over )?(?:two|three|several|\d+) weeks to (?:get|schedule|come|install)",
    # "A bit sad it took over two weeks" / "couldn't get to us for 2-3 days"
    r"\bcouldn'?t get (?:to us|an appointment)\b",
    r"\bhard to (?:get|schedule|book)\b",
    r"\bweeks (?:out|away|to get)",
    r"\bworth the wait\b",
    r"\balways busy\b",
    r"\bin high demand\b",
    r"\bso popular\b",
    # Original patterns retained in case they fire on other contractors
    r"\bbooked (?:solid|out|up)\b",
    r"\beveryone (?:uses|recommends) them\b",
]

# Loosened: bare "the owner" catches references like "David, the owner was
# thorough" or "The owner came out to inspect" — 19 grep hits in raw data
# vs my original 4. Guarded at per-review level by rating >= 4 (applied in
# analyze_review to avoid negative-context owner mentions).
POSITIVE_FOUNDER = [
    r"\bthe owner\b",
    r"\bowner (?:came (?:out|over)|showed up|did the|personally|himself|herself)",
    r"\bmet the owner\b",
    r"\b(?:talked|spoke) (?:to|with) the owner\b",
    r"\bowner[- ]operated\b",
    r"\bhis wife (?:answered|runs|helps)",
    r"\bhis son (?:does|runs|works)",
    r"\b(?:mom[- ]and[- ]pop|family run) (?:business|shop|outfit)",
]

POSITIVE_KEY_PERSON = [
    r"\bask for [A-Z][a-z]+\b",
    r"\brequest [A-Z][a-z]+\b",
    r"\bmake sure (?:to get|you get) [A-Z][a-z]+\b",
    r"\bonly (?:want|trust|use) [A-Z][a-z]+\b",
    r"\b[A-Z][a-z]+ (?:is|was) the best\b",
    r"\b[A-Z][a-z]+ always (?:takes care|comes through)",
    r"\bif you can,? (?:get|ask for) [A-Z][a-z]+",
    r"\bonly let [A-Z][a-z]+ (?:work|touch)",
]

POSITIVE_SWITCH = [
    r"\bfinally found\b",
    r"\bafter (?:trying|using|calling|dealing with) (?:several|many|multiple|three|four|five|a (?:few|couple)|other)",
    r"\bswitched (?:from|to them)\b",
    # Tightened: "used to use X" needs a company reference, not "used to have"
    # (which can mean "used to have a working AC" — false positive)
    r"\bused to (?:use|call) (?:another|a different|X)",
    r"\bstopped using\b",
    # CRITICAL FIX: "old unit/system/year" is a false positive (customer's
    # equipment, not a previous contractor). Restrict to business nouns.
    r"\bour (?:previous|former) (?:hvac|a/c|ac|company|contractor|guy|plumber|provider|service|tech)",
    r"\bour (?:old|last) (?:hvac company|a/c company|ac company|contractor|plumber|provider|service company|hvac guy)",
    r"\bprevious (?:company|hvac|contractor|provider|service)",
    r"\bformer (?:company|hvac|contractor|provider|service)",
    r"\bcame to them after\b",
    r"\btried (?:several|many|a bunch of|a few) (?:a/c|ac|hvac|other) (?:companies|contractors)",
    r"\b(?:a few|several|many|multiple) (?:a/c|ac|hvac|other) (?:companies|contractors) before",
    r"\bhad (?:bad|terrible|awful) experiences? with (?:other|another|previous)",
    r"\b(?:several|many) other (?:companies|contractors|hvac)",
]

POSITIVE_GROWTH = [
    r"\bthey'?ve (?:grown|expanded|gotten bigger)",
    r"\bused to be (?:smaller|just|a small|a one[- ]man)",
    r"\bnew (?:trucks|trailer|office|location|shop|building)",
    r"\bmore (?:trucks|employees|technicians|guys) (?:now|these days)",
    r"\bexpanded (?:to|into|their (?:operation|business))",
    r"\bgrowing (?:company|business|team|fast|quickly)",
    r"\bfamily has grown\b",
    r"\bdoubled in size\b",
]

CONTROL_POSITIVE = [
    r"\btext (?:reminder|notification|confirmation|update|message) (?:when|with|before)",
    r"\btexts? me (?:before|when|with an eta|the morning)",
    r"\breceived (?:a text|an email|a notification|an eta|a tracking)",
    r"\bcalled (?:ahead|before|to let me know|to confirm|to say they were)",
    r"\bkept (?:me|us) (?:updated|informed|in the loop|posted)",
    r"\beasy to (?:book|schedule|contact|reach)",
    r"\bonline (?:booking|scheduling|appointment|portal)",
    r"\barrived (?:on time|promptly|as scheduled|within the window|right on time)",
    r"\barrived when (?:they said|scheduled)",
    r"\bshowed up (?:on time|when they said|as promised|within the)",
    r"\b(?:confirmed|confirming) (?:the|my|our) appointment",
    r"\b(?:sent|got|received) (?:an? )?eta\b",
    r"\bgave (?:me|us) (?:an? )?eta\b",
    r"\btracking (?:link|text|the technician)",
    r"\bbooked (?:online|through the website|via the app)",
    r"\bapp (?:is|made it) (?:great|easy|simple)",
]

# Compile once
def _compile(patterns: list[str]) -> list[re.Pattern[str]]:
    return [re.compile(p, re.IGNORECASE) for p in patterns]


CATEGORIES = {
    "pain_dispatch": _compile(PAIN_DISPATCH),
    "pain_comms": _compile(PAIN_COMMS),
    "pain_capacity": _compile(PAIN_CAPACITY),
    "positive_demand": _compile(POSITIVE_DEMAND),
    "positive_founder": _compile(POSITIVE_FOUNDER),
    "positive_key_person": _compile(POSITIVE_KEY_PERSON),
    "positive_switch": _compile(POSITIVE_SWITCH),
    "positive_growth": _compile(POSITIVE_GROWTH),
    "control_positive": _compile(CONTROL_POSITIVE),
}

PAIN_CATS = ["pain_dispatch", "pain_comms", "pain_capacity"]
MOMENTUM_CATS = [
    "positive_demand",
    "positive_founder",
    "positive_key_person",
    "positive_switch",
    "positive_growth",
]
CONTROL_CATS = ["control_positive"]

# Negation scrub: if any of these words appear in the 2 tokens immediately
# before a PAIN match, skip it as a likely false positive ("wasn't rushed").
NEGATION_LOOKBACK = 2
NEGATION_WORDS = {
    "not", "no", "never", "wasn't", "werent", "weren't", "wasnt",
    "didn't", "didnt", "don't", "dont", "doesn't", "doesnt",
    "isn't", "isnt", "aren't", "arent", "couldn't", "couldnt",
    "cant", "can't", "hadn't", "hadnt", "hasn't", "hasnt",
}


def is_negated(text: str, match_start: int) -> bool:
    """Check if any of the NEGATION_LOOKBACK tokens immediately before
    match_start is a negation word."""
    preceding = text[:match_start].split()
    if not preceding:
        return False
    recent = preceding[-NEGATION_LOOKBACK:]
    return any(w.strip(".,!?\"';").lower() in NEGATION_WORDS for w in recent)


def find_first_match(
    text: str, patterns: list[re.Pattern[str]], check_negation: bool
) -> re.Match[str] | None:
    for rx in patterns:
        for m in rx.finditer(text):
            if check_negation and is_negated(text, m.start()):
                continue
            return m
    return None


def analyze_review(snippet: str, rating: float = 0.0) -> dict[str, str | None]:
    """Return a dict of category -> matched snippet substring (or None).

    Rating-gated logic:
      - PAIN categories require rating <= 3 (strong negative context)
      - POSITIVE categories (especially positive_founder which uses the bare
        "the owner" pattern) require rating >= 4 to exclude negative mentions
      - CONTROL categories require rating >= 4
    Reviews with rating == 0 (missing) are allowed through all filters.
    """
    hits: dict[str, str | None] = {cat: None for cat in CATEGORIES}
    if not snippet:
        return hits

    for cat, patterns in CATEGORIES.items():
        # Rating gate per category
        if cat in PAIN_CATS:
            if rating and rating > 3:
                continue  # don't match pain in 4-5 star reviews
            check_neg = True
        elif cat == "positive_founder":
            if rating and rating < 4:
                continue  # bare "the owner" pattern needs positive context
            check_neg = False
        elif cat in MOMENTUM_CATS or cat in CONTROL_CATS:
            if rating and rating < 4:
                continue
            check_neg = False
        else:
            check_neg = False

        m = find_first_match(snippet, patterns, check_neg)
        if m:
            start = max(0, m.start() - 40)
            end = min(len(snippet), m.end() + 40)
            hits[cat] = snippet[start:end].strip()
    return hits


def rollup_contractor(reviews: list[dict]) -> dict:
    """Aggregate per-contractor signal counts + sample quotes."""
    counts: dict[str, int] = defaultdict(int)
    samples: dict[str, list[tuple[int, str, str]]] = defaultdict(list)
    # Each sample is (rating, review_snippet, category_context_snippet)
    # sorted later; pick lowest-rating for pain, highest-rating for momentum/control.

    pain_ratings: list[float] = []
    reviews_with_any_pain = 0
    reviews_with_any_momentum = 0
    reviews_with_any_control = 0

    for rv in reviews:
        snippet = rv.get("snippet") or ""
        if not snippet:
            extracted = rv.get("extracted_snippet") or {}
            if isinstance(extracted, dict):
                snippet = extracted.get("original") or ""
        if not snippet:
            continue

        try:
            rating = float(rv.get("rating") or 0)
        except (TypeError, ValueError):
            rating = 0.0

        hits = analyze_review(snippet, rating)

        any_pain = False
        any_momentum = False
        any_control = False

        for cat, matched_snippet in hits.items():
            if not matched_snippet:
                continue
            counts[cat] += 1
            samples[cat].append((rating, snippet, matched_snippet))
            if cat in PAIN_CATS:
                any_pain = True
            elif cat in MOMENTUM_CATS:
                any_momentum = True
            elif cat in CONTROL_CATS:
                any_control = True

        if any_pain:
            reviews_with_any_pain += 1
            pain_ratings.append(rating)
        if any_momentum:
            reviews_with_any_momentum += 1
        if any_control:
            reviews_with_any_control += 1

    # Pick best samples: for pain use lowest rating, for positive use highest
    def best_sample(cat: str) -> str | None:
        if not samples[cat]:
            return None
        if cat in PAIN_CATS:
            samples[cat].sort(key=lambda x: (x[0], -len(x[2])))
        else:
            samples[cat].sort(key=lambda x: (-x[0], -len(x[2])))
        return samples[cat][0][1]  # full snippet

    metrics = {"pain_reviews_analyzed": len(reviews)}
    for cat in CATEGORIES:
        metrics[f"{cat}_count"] = counts[cat]
    for cat in CATEGORIES:
        metrics[f"{cat}_sample_quote"] = best_sample(cat)

    pain_score = sum(counts[c] for c in PAIN_CATS)
    momentum_score = sum(counts[c] for c in MOMENTUM_CATS)
    control_score = sum(counts[c] for c in CONTROL_CATS)
    total = pain_score + momentum_score + control_score

    metrics["pain_score"] = pain_score
    metrics["momentum_score"] = momentum_score
    metrics["control_score"] = control_score
    metrics["buying_signal_total"] = pain_score + momentum_score
    metrics["smooth_ops_ratio"] = round(control_score / total, 2) if total else 0.0
    metrics["reviews_with_any_pain"] = reviews_with_any_pain
    metrics["reviews_with_any_momentum"] = reviews_with_any_momentum
    metrics["reviews_with_any_control"] = reviews_with_any_control
    metrics["pain_avg_rating"] = (
        round(sum(pain_ratings) / len(pain_ratings), 2) if pain_ratings else None
    )

    return metrics


def classify(m: dict) -> str:
    """Assign a buying_category based on signal counts.

    Thresholds calibrated to the real hit distribution in Phoenix HVAC
    reviews (pain is rare, momentum is moderate, control is common).
    """
    pain = m["pain_score"]
    momentum = m["momentum_score"]
    control = m["control_score"]
    total = pain + momentum + control
    analyzed = m.get("pain_reviews_analyzed", 0)

    if analyzed < 10:
        return "low_volume"
    if total < 2:
        return "low_signal"

    # Smooth ops: control meaningfully dominates all other signals
    if control >= 3 and m["smooth_ops_ratio"] > 0.55:
        return "smooth_ops"

    # Any pain signal is notable (rare in Phoenix HVAC reviews)
    if pain >= 2:
        return "active_pain"

    # Strong momentum without pain = scaling strain narrative
    if momentum >= 3 and pain == 0:
        return "scaling_strain"

    # Mix of pain + momentum
    if pain >= 1 and momentum >= 2:
        return "mixed_conviction"
    if pain + momentum >= 3:
        return "mixed_conviction"

    if pain >= 1 or momentum >= 1:
        return "light_signal"
    return "low_signal"


def load_reviews(place_id: str) -> list[dict]:
    """Load cached SerpAPI review JSON for a contractor."""
    if not place_id:
        return []
    path = RAW_DIR / f"{place_id}.json"
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return []
    return data.get("reviews") or []


def main() -> None:
    if not POOL_CSV.exists():
        sys.exit(f"missing {POOL_CSV}")

    df = pd.read_csv(POOL_CSV)
    print(f"Pool: {len(df)} contractors")
    print()

    results = []
    for _, row in df.iterrows():
        place_id = row.get("place_id")
        reviews = load_reviews(str(place_id) if pd.notna(place_id) else "")
        metrics = rollup_contractor(reviews)
        metrics["buying_category"] = classify(metrics)
        metrics["license_no"] = row["license_no"]
        results.append(metrics)

    res_df = pd.DataFrame(results)

    # Merge new columns into pool
    merged = df.merge(res_df, on="license_no", how="left", suffixes=("", "_nlp"))

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT_CSV, index=False)

    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    snap_path = (
        SNAPSHOT_DIR / f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.csv"
    )
    merged.to_csv(snap_path, index=False)

    # ---- Summary ----
    print("=" * 74)
    print("Summary")
    print("=" * 74)

    print()
    print("buying_category distribution:")
    for cat, count in res_df["buying_category"].value_counts().items():
        print(f"  {cat:<18} {count}")

    print()
    print("Category hit counts across the pool:")
    for cat in PAIN_CATS + MOMENTUM_CATS + CONTROL_CATS:
        col = f"{cat}_count"
        total_hits = int(res_df[col].sum())
        contractors_hit = int((res_df[col] > 0).sum())
        tag = (
            "PAIN" if cat in PAIN_CATS
            else "MOMENT" if cat in MOMENTUM_CATS
            else "CONTROL"
        )
        print(f"  [{tag:<7}] {cat:<22} total_hits={total_hits:<4} contractors_hit={contractors_hit}")

    # Top contractors by composite buying signal
    print()
    print("Top 15 contractors by buying_signal_total (pain + momentum):")
    cols = [
        "rank", "business_name", "city", "pain_score", "momentum_score",
        "control_score", "buying_category",
    ]
    top = merged.sort_values(
        ["buying_signal_total", "pain_score"], ascending=False
    ).head(15)
    with pd.option_context("display.max_colwidth", 40, "display.width", 220):
        print(top[cols].to_string(index=False))

    # Sample quotes for the top 5
    print()
    print("Top 5 contractors — verbatim sample quotes:")
    for _, r in merged.sort_values(
        ["buying_signal_total", "pain_score"], ascending=False
    ).head(5).iterrows():
        print()
        print(f"  rank {int(r['rank'])}  {r['business_name']}  "
              f"({r['buying_category']}, pain={int(r['pain_score'])}, "
              f"momentum={int(r['momentum_score'])}, control={int(r['control_score'])})")
        for cat in PAIN_CATS + MOMENTUM_CATS:
            sample = r.get(f"{cat}_sample_quote")
            if isinstance(sample, str) and sample.strip():
                label = cat.replace("_", " ")
                print(f"    [{label}]:  \"{sample[:180].strip()}\"")

    print()
    print("Outputs:")
    print(f"  {OUT_CSV.relative_to(ROOT)}")
    print(f"  {snap_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
