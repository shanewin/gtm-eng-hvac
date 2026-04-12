#!/usr/bin/env python3
"""
Multi-signal scoring and re-ranking of the 70 hidden gem contractors.

Additive scoring model across 7 dimensions:
  1. Direct pain evidence           (0-40 points, thin-sample discounted)
  2. Scaling strain / momentum      (0-25 points, thin-sample discounted)
  3. Multi-signal convergence bonus (0-15 points)
  4. Operational readiness          (0-10 points)
  5. Demand pull (capacity ceiling) (0-20 points)
  6. ICP fit (license scope)        (0-5 points, new in V2)
  7. Disqualifiers                  (-30 to 0 points)

Non-scoring display fields:
  - size_tier         : S / M / L / XL, computed from license_years + review_count
  - confidence_tier   : low / medium / high (capped at low when sample < 10)
  - primary_narrative : active_pain | scaling_strain | demand_pull | mixed | unclear
  - final_rank        : 1 = highest score

Hard disqualifier (V2): a contractor whose cached SerpAPI job descriptions
explicitly mention an FSM platform as a required skill (ServiceTitan, Jobber,
Housecall Pro, FieldEdge, etc.) is flagged as `already_fsm_customer` and
receives -15 in addition to any other disqualifier points. This catches
contractors who already bought — the single most embarrassing failure mode
in any buying-signal list.

Thin-sample discount: when the LLM only analyzed fewer than 15 cached reviews,
score_direct_pain and score_scaling_strain are multiplied by
min(1, n/15). This prevents contractors with 5-8 cached reviews from
maxing out the pain dimension on a single angry review.

Input:  data/03_hidden_gems/complete.csv
Output: data/03_hidden_gems/scored.csv
        data/03_hidden_gems/already_fsm_dropped.csv  (audit sidecar)
        data/snapshots/scored/YYYY-MM-DD.csv
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
INPUT_CSV = ROOT / "data" / "03_hidden_gems" / "complete.csv"
OUTPUT_CSV = ROOT / "data" / "03_hidden_gems" / "scored.csv"
FSM_DROPPED_CSV = ROOT / "data" / "03_hidden_gems" / "already_fsm_dropped.csv"
SNAPSHOT_DIR = ROOT / "data" / "snapshots" / "scored"
VALIDATOR_DIR = ROOT / "data" / "signals_raw" / "validator"
SERP_JOBS_DIR = ROOT / "data" / "signals_raw" / "serpapi_jobs"
BOND_DIR = ROOT / "data" / "signals_raw" / "roc_bonds"


# ---- Revenue band from ROC bond amount ----
#
# Arizona law (ARS 32-1152) requires contractors to post a surety bond
# sized to their annual gross volume tier. The bond amount is public
# record on the ROC detail page. For CR-39 (Specialty Dual) contractors,
# the combined bond = commercial bond + residential bond. The combined
# amount maps to a revenue band.
#
# These thresholds are derived from the statutory tier tables and the
# empirical distribution of combined bond amounts across the 70-
# contractor hidden-gems pool. The mapping is conservative — we assign
# the floor of the range, not the ceiling.

def load_bond_amount(place_id: str) -> int | None:
    """Read the cached bond amount scraped from the ROC detail page."""
    path = BOND_DIR / f"{place_id}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None
    return data.get("bond_amount")


def classify_revenue_band(bond_amount: int | None) -> str:
    """Map a combined ROC bond amount to a revenue band label.

    The bands are intentionally labeled as ranges, not point estimates.
    A $32,500 bond means the ROC certified this contractor for $1.5M–$5M
    in annual gross volume — we don't know where in that range they fall,
    and claiming more precision than the bond schedule provides would be
    dishonest.

    Returns a human-readable string like "$1.5M–$5M" or "Unknown" if
    the bond amount is missing.
    """
    if bond_amount is None:
        return "Unknown"
    if bond_amount >= 50_000:
        return "$5M+"
    if bond_amount >= 20_000:
        return "$1.5M–$5M"
    if bond_amount >= 9_000:
        return "$500K–$1.5M"
    if bond_amount >= 4_000:
        return "$150K–$500K"
    return "Under $150K"


# ---- size_tier classifier ----
#
# Non-scoring. A display field that segments contractors by business
# size proxy (license tenure + Google review volume). Used by the
# render layer to show S/M/L/XL badges on dossiers and by buyers to
# filter the list by deal size expectations.
#
# Thresholds calibrated against the current hidden-gems pool (5-25
# years in business, 50-500 reviews). XL is a reserve tier for larger
# contractors outside this pool — nothing in the current top 25
# reaches it, which is expected.

def classify_size_tier(years: float, reviews: int) -> str:
    if years >= 20 and reviews >= 400:
        return "XL"
    if years >= 15 or reviews >= 300:
        return "L"
    if years >= 10 or reviews >= 150:
        return "M"
    return "S"


# ---- FSM-vendor detection (disqualifier) ----
#
# Brand-name matching against cached SerpAPI job descriptions. If a
# contractor's hiring posts require experience with a named FSM
# platform, they almost certainly already bought it — and we need to
# drop them from the list before a rep embarrasses themselves pitching
# a product the prospect already pays for.
#
# This is NOT fuzzy matching. These are exact brand names, case-
# insensitive, word-boundary anchored. No cultural-assumption issues.
# The no-fuzzy-match rule in CLAUDE.md is about ownership judgments
# ("is this contractor the same as that one"), not about exact-token
# brand detection, which is always safe.

FSM_VENDOR_PATTERNS = [
    r"\bservice\s*titan\b",
    r"\bhousecall\s*pro\b",
    r"\bjobber\b",
    r"\bfield\s*edge\b",
    r"\bservice\s*bridge\b",
    r"\bworkiz\b",
    r"\bfield\s*pulse\b",
    r"\bservice\s*fusion\b",
    r"\btradify\b",
    r"\bkickserv\b",
    r"\bmhelpdesk\b",
    r"\bsynchroteam\b",
    r"\bgorilla\s*desk\b",
    r"\bworkwave\b",
]
_COMPILED_FSM_VENDORS = [re.compile(p, re.IGNORECASE) for p in FSM_VENDOR_PATTERNS]


def detect_fsm_vendor_in_jobs(place_id: str) -> tuple[bool, str, str]:
    """
    Return (is_customer, vendor_matched, quote) by scanning all cached
    SerpAPI job postings (primary + retry) for FSM vendor brand names.
    Returns ("", "", "") when nothing matches or no cache exists.

    We scan job title + description + (where present) qualifications.
    """
    paths = [
        SERP_JOBS_DIR / f"{place_id}.json",
        SERP_JOBS_DIR / f"{place_id}_retry.json",
    ]
    for path in paths:
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        resp = data.get("response") or {}
        jobs = (resp.get("jobs_results") or []) + (data.get("rejected_jobs") or [])
        for j in jobs:
            blob_parts = [
                j.get("title") or "",
                j.get("description") or "",
            ]
            # Job highlights can carry the skills requirements
            for h in (j.get("job_highlights") or []):
                if isinstance(h, dict):
                    blob_parts.append(h.get("title") or "")
                    items = h.get("items") or []
                    if isinstance(items, list):
                        blob_parts.extend(str(x) for x in items)
            blob = "\n".join(blob_parts)
            for rx in _COMPILED_FSM_VENDORS:
                m = rx.search(blob)
                if m:
                    vendor = m.group(0)
                    # Extract a short context window around the match
                    start = max(0, m.start() - 60)
                    end = min(len(blob), m.end() + 60)
                    quote = re.sub(r"\s+", " ", blob[start:end]).strip()
                    return True, vendor, quote
    return False, "", ""


# ---- Hiring counts from the validator cache ----
#
# The old pipeline had step 07_serpapi_hiring.py apply a fuzzy
# token-subset match against the business name, classify the kept jobs
# as ops_pain / capacity_growth / other, and write the counts into
# with_hiring.csv. That match was the last regex-based "is this the
# same business" heuristic in the pipeline, and it contradicted the
# standing "extract then validate, never fuzzy-match" rule.
#
# Now: step 07 caches the raw SerpAPI response untouched, step 17 runs
# an LLM validator per contractor that marks each posting belongs /
# reject with a written reason, and this function reads those decisions
# back at score time. The role classifier that used to live in step 07
# moved here — it's the only remaining consumer.

OPS_PAIN_PATTERNS = [
    r"\bdispatch(er|ing)?\b",
    r"\bservice\s+manager\b",
    r"\boperations?\s+manager\b",
    r"\boffice\s+manager\b",
    r"\bcustomer\s+service\b",
    r"\bcsr\b",
    r"\bcall\s+(?:center|taker)\b",
    r"\bcoordinator\b",
    r"\bschedul(?:er|ing)\b",
    r"\badmin(?:istrative)?\s+assistant\b",
    r"\breceptionist\b",
]
CAPACITY_PATTERNS = [
    r"\b(?:hvac\s+)?technician\b",
    r"\binstaller?\b",
    r"\bapprentice\b",
    r"\bhelper\b",
    r"\bjourneyman\b",
    r"\bservice\s+tech(?:nician)?\b",
    r"\bfield\s+tech(?:nician)?\b",
]
_COMPILED_OPS = [re.compile(p, re.IGNORECASE) for p in OPS_PAIN_PATTERNS]
_COMPILED_CAP = [re.compile(p, re.IGNORECASE) for p in CAPACITY_PATTERNS]


def _classify_role(title: str) -> str:
    if not title:
        return "other"
    for rx in _COMPILED_OPS:
        if rx.search(title):
            return "ops_pain"
    for rx in _COMPILED_CAP:
        if rx.search(title):
            return "capacity_growth"
    return "other"


def hiring_counts_from_validator(place_id: str) -> dict:
    """Read the validator's kept jobs for this contractor and classify
    each title into ops_pain / capacity / other. Returns a dict with
    hiring_raw_count, hiring_ops_pain_count, hiring_capacity_count,
    hiring_other_count, hiring_signal, hiring_sample_ops_pain_title,
    hiring_sample_capacity_title. If no validator cache exists, returns
    all zeros — run pipeline/17_candidate_validator.py first."""
    path = VALIDATOR_DIR / f"{place_id}.json"
    empty = {
        "hiring_raw_count": 0,
        "hiring_ops_pain_count": 0,
        "hiring_capacity_count": 0,
        "hiring_other_count": 0,
        "hiring_signal": "no_hiring_detected",
        "hiring_sample_ops_pain_title": None,
        "hiring_sample_capacity_title": None,
    }
    if not path.exists():
        return empty
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return empty
    validated = data.get("validated") or {}
    jobs = validated.get("jobs") or []
    kept_titles = [
        (j.get("value") or "").strip()
        for j in jobs
        if j.get("belongs")
    ]
    if not kept_titles:
        return empty

    ops, cap, other = [], [], []
    for title in kept_titles:
        cat = _classify_role(title)
        if cat == "ops_pain":
            ops.append(title)
        elif cat == "capacity_growth":
            cap.append(title)
        else:
            other.append(title)

    if ops:
        signal = "ops_pain_active"
    elif cap:
        signal = "capacity_growth"
    elif other:
        signal = "other_only"
    else:
        signal = "no_hiring_detected"

    return {
        "hiring_raw_count": len(kept_titles),
        "hiring_ops_pain_count": len(ops),
        "hiring_capacity_count": len(cap),
        "hiring_other_count": len(other),
        "hiring_signal": signal,
        "hiring_sample_ops_pain_title": ops[0] if ops else None,
        "hiring_sample_capacity_title": cap[0] if cap else None,
    }


def safe_num(v, default=0.0) -> float:
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def safe_bool(v) -> bool:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in {"true", "1", "yes", "t"}
    return bool(v)


def safe_str(v, default="") -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return default
    return str(v)


# ----- Thin-sample discount -----

MIN_REVIEWS_FOR_FULL_CREDIT = 15


def thin_sample_discount(r: pd.Series) -> float:
    """Returns a 0-1 multiplier. 1.0 when >=15 reviews analyzed, linear below."""
    n = safe_num(r.get("llm_review_count_analyzed"))
    if n <= 0:
        return 0.5  # No LLM analysis = use 50% as a neutral default
    if n >= MIN_REVIEWS_FOR_FULL_CREDIT:
        return 1.0
    return n / MIN_REVIEWS_FOR_FULL_CREDIT


# ----- Scoring dimensions -----

def score_direct_pain(r: pd.Series) -> tuple[float, list[str]]:
    """Direct evidence customers are complaining about operational failures."""
    score = 0.0
    evidence = []

    llm_cat = safe_str(r.get("llm_buying_category"))
    if llm_cat == "active_pain":
        score += 25
        evidence.append("LLM classified as active_pain (+25)")

    llm_pain = safe_num(r.get("llm_pain_score"))
    if llm_pain > 0:
        pts = min(llm_pain * 1.0, 10)
        score += pts
        evidence.append(f"LLM pain_score={int(llm_pain)} (+{pts:.0f})")

    regex_pain = safe_num(r.get("pain_score"))
    if regex_pain > 0:
        pts = min(regex_pain * 1.0, 5)
        score += pts
        evidence.append(f"Regex pain hits={int(regex_pain)} (+{pts:.0f})")

    dispatch_cat = safe_str(r.get("dispatch_category"))
    if dispatch_cat == "dispatch_strained":
        score += 10
        evidence.append("Dispatch strained (+10)")
    elif dispatch_cat == "dispatch_bimodal":
        score += 5
        evidence.append("Dispatch bimodal (+5)")

    burst_cat = safe_str(r.get("burst_category"))
    if burst_cat == "active_crisis":
        score += 15
        evidence.append("Active crisis burst (<30 days) (+15)")
    elif burst_cat == "recent_crisis":
        score += 10
        evidence.append("Recent crisis burst (30-60 days) (+10)")

    neg_pct = safe_num(r.get("dispatch_negative_sentiment_pct"))
    if neg_pct >= 0.25:
        score += 5
        evidence.append(f"Dispatch negative pct={int(neg_pct*100)}% (+5)")

    raw = min(score, 40.0)
    discount = thin_sample_discount(r)
    if discount < 1.0 and raw > 0:
        n = int(safe_num(r.get("llm_review_count_analyzed")))
        discounted = raw * discount
        evidence.append(
            f"Thin-sample discount: only {n}/{MIN_REVIEWS_FOR_FULL_CREDIT} reviews "
            f"analyzed, score x{discount:.2f} ({raw:.1f} → {discounted:.1f})"
        )
        return discounted, evidence
    return raw, evidence


def score_scaling_strain(r: pd.Series) -> tuple[float, list[str]]:
    """Evidence of growth/strain — hiring, velocity, momentum. Founder /
    key-person / scaling_surge burst moved to score_demand_pull."""
    score = 0.0
    evidence = []

    llm_cat = safe_str(r.get("llm_buying_category"))
    if llm_cat == "scaling_strain":
        score += 15
        evidence.append("LLM classified as scaling_strain (+15)")

    regex_cat = safe_str(r.get("buying_category"))
    if regex_cat == "scaling_strain":
        score += 10
        evidence.append("Regex classified as scaling_strain (+10)")

    llm_mom = safe_num(r.get("llm_momentum_score"))
    if llm_mom > 0:
        pts = min(llm_mom * 0.5, 5)
        score += pts
        evidence.append(f"LLM momentum_score={int(llm_mom)} (+{pts:.1f})")

    reg_mom = safe_num(r.get("momentum_score"))
    if reg_mom > 0:
        pts = min(reg_mom * 1.0, 5)
        score += pts
        evidence.append(f"Regex momentum hits={int(reg_mom)} (+{pts:.0f})")

    ops = safe_num(r.get("hiring_ops_pain_count"))
    if ops > 0:
        pts = min(ops * 10, 20)
        score += pts
        evidence.append(f"Hiring ops_pain postings={int(ops)} (+{pts:.0f})")

    cap = safe_num(r.get("hiring_capacity_count"))
    if cap > 0:
        pts = min(cap * 2, 6)
        score += pts
        evidence.append(f"Hiring capacity postings={int(cap)} (+{pts:.0f})")

    vel_cat = safe_str(r.get("velocity_category"))
    if vel_cat == "accelerating":
        score += 10
        evidence.append("Velocity category accelerating (+10)")
    elif vel_cat == "hot_new":
        score += 8
        evidence.append("Velocity category hot_new (+8)")

    vel_ratio = safe_num(r.get("velocity_ratio"))
    if vel_ratio > 3.0:
        score += 3
        evidence.append(f"Velocity ratio {vel_ratio:.1f}x (+3)")

    raw = min(score, 25.0)
    discount = thin_sample_discount(r)
    if discount < 1.0 and raw > 0:
        n = int(safe_num(r.get("llm_review_count_analyzed")))
        discounted = raw * discount
        evidence.append(
            f"Thin-sample discount: only {n}/{MIN_REVIEWS_FOR_FULL_CREDIT} reviews "
            f"analyzed, score x{discount:.2f} ({raw:.1f} → {discounted:.1f})"
        )
        return discounted, evidence
    return raw, evidence


def score_demand_pull(r: pd.Series) -> tuple[float, list[str]]:
    """
    Rewards the 'success at capacity ceiling' pattern: contractors winning
    deals on personal responsiveness, with customers migrating from
    competitors, heavy dependency on one person, and no system to scale.

    Signals:
      - customer-switch mentions (refugees) : strongest direct demand signal
      - heavy founder involvement           : the owner IS the brand
      - heavy key-person dependency         : one named individual in reviews
      - scaling_surge burst                 : recent positive review cluster
      - fast dispatch + any of the above    : they're winning now, ceiling close

    Capped at 20 points.
    """
    score = 0.0
    evidence = []

    refugees = safe_num(r.get("llm_customer_refugee_mentions"))
    if refugees > 0:
        pts = min(refugees * 3, 12)
        score += pts
        evidence.append(f"Customer-switch mentions={int(refugees)} (+{pts:.0f})")

    founder = safe_str(r.get("llm_founder_involvement"))
    if founder == "heavy":
        score += 4
        evidence.append("Heavy founder involvement (+4)")

    key_person = safe_str(r.get("llm_key_person_dependency"))
    if key_person == "heavy":
        score += 4
        evidence.append("Heavy key-person dependency (+4)")

    # Only count scaling_surge when the contractor has a real baseline
    # review velocity. A "surge" of 3 reviews against a 0.15/week baseline
    # is mathematically 17x the baseline but says nothing real — the same
    # rule is already used in score_multi_signal below.
    burst_cat = safe_str(r.get("burst_category"))
    burst_baseline = safe_num(r.get("burst_baseline_per_week"))
    real_surge = burst_cat == "scaling_surge" and burst_baseline >= 0.5
    if real_surge:
        score += 4
        evidence.append(
            f"Scaling surge burst (baseline {burst_baseline:.1f}/wk) (+4)"
        )

    disp_cat = safe_str(r.get("dispatch_category"))
    has_dependency = (
        refugees > 0
        or founder == "heavy"
        or key_person == "heavy"
        or real_surge
    )
    if disp_cat == "dispatch_fast" and has_dependency:
        score += 3
        evidence.append("Fast dispatch + dependency = capacity ceiling (+3)")

    return min(score, 20.0), evidence


def score_multi_signal(r: pd.Series) -> tuple[float, list[str], int]:
    """Bonus for multiple independent signal sources converging."""
    sources_firing: list[str] = []

    llm_cat = safe_str(r.get("llm_buying_category"))
    if llm_cat in ("active_pain", "scaling_strain", "mixed_conviction"):
        sources_firing.append(f"LLM NLP ({llm_cat})")

    reg_cat = safe_str(r.get("buying_category"))
    if reg_cat in ("active_pain", "scaling_strain", "mixed_conviction"):
        sources_firing.append(f"Regex NLP ({reg_cat})")

    hir_cat = safe_str(r.get("hiring_signal"))
    if hir_cat in ("ops_pain_active", "capacity_growth"):
        sources_firing.append(f"Hiring ({hir_cat})")

    disp_cat = safe_str(r.get("dispatch_category"))
    if disp_cat in ("dispatch_strained", "dispatch_bimodal"):
        sources_firing.append(f"Dispatch ({disp_cat})")

    burst_cat = safe_str(r.get("burst_category"))
    burst_baseline = safe_num(r.get("burst_baseline_per_week"))
    if burst_cat in ("active_crisis", "recent_crisis"):
        sources_firing.append(f"Burst ({burst_cat})")
    elif burst_cat == "scaling_surge" and burst_baseline >= 0.5:
        sources_firing.append("Burst (scaling_surge)")

    vel_cat = safe_str(r.get("velocity_category"))
    if vel_cat in ("accelerating", "hot_new"):
        sources_firing.append(f"Velocity ({vel_cat})")

    n = len(sources_firing)
    if n >= 4:
        bonus = 15
    elif n == 3:
        bonus = 10
    elif n == 2:
        bonus = 5
    else:
        bonus = 0

    evidence = [f"{n} signal sources converging (+{bonus})"] if bonus > 0 else []
    if sources_firing:
        evidence.append("Sources: " + ", ".join(sources_firing))

    return float(bonus), evidence, n


def score_icp_fit(r: pd.Series) -> tuple[float, list[str]]:
    """
    ICP fit dimension (new in V2). Rewards license scope that matches
    enterprise FSM buyer personas. AZ ROC class codes:
      - CR-39 : Dual-scope license (commercial + residential)
      - R-39  : Residential Air Conditioning and Refrigeration Including Solar
      - R-39R : Residential Air Conditioning and Refrigeration

    A Dual-scope contractor can serve commercial customers, which
    typically means bigger truck fleets, bigger deal sizes, and a
    fit with enterprise FSM products (ServiceTitan's commercial play).
    A Residential-only contractor is a Jobber / Housecall Pro shape.

    Non-controversial, strictly additive, capped small so it doesn't
    dominate real buying-intent signals.
    """
    cls = safe_str(r.get("class"))
    if cls == "CR-39":
        return 5.0, ["Dual-scope license CR-39 (commercial + residential) (+5)"]
    if cls in {"R-39", "R-39R"}:
        return 2.0, [f"Residential-only license {cls} (+2)"]
    return 0.0, []


def score_operational_readiness(r: pd.Series) -> tuple[float, list[str]]:
    """Baseline indicators that this is a real, established business."""
    score = 0.0
    evidence = []

    has_website = not (pd.isna(r.get("place_website")) or not safe_str(r.get("place_website")))
    if has_website:
        score += 3
        evidence.append("Has website (+3)")

    if safe_bool(r.get("apollo_found")):
        score += 2
        evidence.append("Apollo data found (+2)")

    reviews = safe_num(r.get("place_review_count"))
    if reviews >= 100:
        score += 2
        evidence.append(f"{int(reviews)} reviews (+2)")

    ly = safe_num(r.get("license_years"))
    if ly >= 10:
        score += 2
        evidence.append(f"{ly:.1f} years licensed (+2)")

    rating = safe_num(r.get("place_rating"))
    if rating >= 4.7:
        score += 1
        evidence.append(f"Rating {rating:.1f} (+1)")

    return min(score, 10.0), evidence


def score_disqualifiers(r: pd.Series) -> tuple[float, list[str]]:
    """Negative adjustments for contractors with smooth-ops indicators.

    V2 addition: a hard -15 disqualifier fires when a contractor's cached
    SerpAPI job postings explicitly name an FSM platform as a required
    skill. This catches contractors who already bought — the single most
    embarrassing failure mode in any buying-signal list. See
    `detect_fsm_vendor_in_jobs` above for the brand-name list."""
    score = 0.0
    evidence = []

    # Hard disqualifier: already an FSM customer per job-posting evidence
    is_fsm_customer = bool(r.get("_already_fsm_customer", False))
    if is_fsm_customer:
        vendor = safe_str(r.get("_already_fsm_vendor"))
        score -= 15
        evidence.append(
            f"Job posting requires {vendor} experience — already an FSM "
            f"customer (-15)"
        )

    llm_cat = safe_str(r.get("llm_buying_category"))
    if llm_cat == "smooth_ops":
        score -= 10
        evidence.append("LLM classified as smooth_ops (-10)")

    reg_cat = safe_str(r.get("buying_category"))
    if reg_cat == "smooth_ops":
        score -= 5
        evidence.append("Regex classified as smooth_ops (-5)")

    llm_smooth = safe_num(r.get("llm_smooth_ops_score"))
    if llm_smooth >= 6:
        score -= 5
        evidence.append(f"LLM smooth_ops_score={int(llm_smooth)} (-5)")

    # Dispatch_fast with no pain AND no demand-pull signals = they've
    # already solved operations AND aren't at a capacity ceiling.
    # Contractors that are dispatch_fast WITH demand-pull signals get
    # scored positively via score_demand_pull, not disqualified here.
    disp_cat = safe_str(r.get("dispatch_category"))
    llm_pain = safe_num(r.get("llm_pain_score"))
    regex_pain = safe_num(r.get("pain_score"))
    refugees = safe_num(r.get("llm_customer_refugee_mentions"))
    founder = safe_str(r.get("llm_founder_involvement"))
    key_person = safe_str(r.get("llm_key_person_dependency"))
    burst_cat = safe_str(r.get("burst_category"))
    has_demand_pull = (
        refugees > 0
        or founder == "heavy"
        or key_person == "heavy"
        or burst_cat == "scaling_surge"
    )
    if (
        disp_cat == "dispatch_fast"
        and llm_pain < 2
        and regex_pain < 2
        and not has_demand_pull
    ):
        score -= 5
        evidence.append("Fast dispatch + no pain + no demand-pull signals (-5)")

    # New floor with hard-disqualifier expansion: -30 (vs old -15) so
    # the FSM-customer -15 can stack on top of other disqualifiers.
    return max(score, -30.0), evidence


def classify_narrative(
    direct_pain: float, scaling: float, demand_pull: float
) -> str:
    if demand_pull >= 12 and demand_pull >= direct_pain and demand_pull >= scaling:
        return "demand_pull"
    if direct_pain >= 20 and direct_pain > scaling:
        return "active_pain"
    if scaling >= 15 and scaling > direct_pain:
        return "scaling_strain"
    if direct_pain >= 10 and scaling >= 10:
        return "mixed"
    if direct_pain >= 5 or scaling >= 5 or demand_pull >= 5:
        return "light_signal"
    return "unclear"


def classify_confidence(signal_count: int, review_count: float) -> str:
    """Confidence is capped at 'low' when cached review sample is too thin."""
    if review_count < 10:
        return "low"
    if signal_count >= 3 and review_count >= 15:
        return "high"
    if signal_count >= 2 and review_count >= 10:
        return "medium"
    return "low"


def main() -> None:
    if not INPUT_CSV.exists():
        raise SystemExit(f"missing {INPUT_CSV}")

    df = pd.read_csv(INPUT_CSV)
    print(f"Scoring {len(df)} contractors")

    # Hiring counts are computed fresh from the step 17 validator cache.
    # Any stale hiring_* columns left over on the input CSV (from the
    # pre-validator era of step 07) are overwritten here so the scoring
    # functions see only LLM-validated belongs-true postings.
    missing_validator = 0
    for idx, row in df.iterrows():
        place_id = str(row.get("place_id") or "")
        counts = hiring_counts_from_validator(place_id)
        if not (VALIDATOR_DIR / f"{place_id}.json").exists():
            missing_validator += 1
        for col, val in counts.items():
            df.at[idx, col] = val
    if missing_validator:
        print(
            f"  WARNING: {missing_validator}/{len(df)} contractors have no "
            f"validator cache. Run pipeline/17_candidate_validator.py first — "
            f"their hiring scores will be zero."
        )

    # FSM-vendor detection: scan cached SerpAPI job descriptions for
    # explicit brand-name mentions. Flag any hit as already-FSM-customer
    # and feed that into score_disqualifiers. Write an audit sidecar so
    # we can see every contractor we dropped and verify the regex isn't
    # overreaching.
    fsm_hits: list[dict] = []
    for idx, row in df.iterrows():
        place_id = str(row.get("place_id") or "")
        is_customer, vendor, quote = detect_fsm_vendor_in_jobs(place_id)
        df.at[idx, "_already_fsm_customer"] = is_customer
        df.at[idx, "_already_fsm_vendor"] = vendor
        if is_customer:
            fsm_hits.append({
                "license_no": row.get("license_no"),
                "business_name": row.get("business_name"),
                "place_id": place_id,
                "fsm_vendor_matched": vendor,
                "context_quote": quote,
            })
    if fsm_hits:
        print(
            f"  FSM-vendor disqualifier fired for {len(fsm_hits)} contractor"
            f"{'s' if len(fsm_hits) != 1 else ''}:"
        )
        for hit in fsm_hits:
            print(f"    - {hit['business_name']}: matched '{hit['fsm_vendor_matched']}'")
        pd.DataFrame(fsm_hits).to_csv(FSM_DROPPED_CSV, index=False)
    else:
        print("  FSM-vendor disqualifier: no matches in cached job postings")
    print()

    rows = []
    for _, r in df.iterrows():
        dp, dp_evid = score_direct_pain(r)
        ss, ss_evid = score_scaling_strain(r)
        ms, ms_evid, sig_count = score_multi_signal(r)
        op, op_evid = score_operational_readiness(r)
        dm, dm_evid = score_demand_pull(r)
        icp, icp_evid = score_icp_fit(r)
        dq, dq_evid = score_disqualifiers(r)

        total = dp + ss + ms + op + dm + icp + dq
        narrative = classify_narrative(dp, ss, dm)
        review_count = safe_num(r.get("llm_review_count_analyzed"))
        confidence = classify_confidence(sig_count, review_count)

        # size_tier display field (non-scoring)
        size = classify_size_tier(
            safe_num(r.get("license_years")),
            int(safe_num(r.get("place_review_count"))),
        )

        # Revenue band from ROC bond amount (non-scoring, display only)
        place_id = safe_str(r.get("place_id"))
        bond_amt = load_bond_amount(place_id)
        rev_band = classify_revenue_band(bond_amt)

        rows.append({
            "license_no": r["license_no"],
            "score_direct_pain": round(dp, 1),
            "score_scaling_strain": round(ss, 1),
            "score_multi_signal": round(ms, 1),
            "score_operational_readiness": round(op, 1),
            "score_demand_pull": round(dm, 1),
            "score_icp_fit": round(icp, 1),
            "score_disqualifiers": round(dq, 1),
            "score_total": round(total, 1),
            "signal_source_count": sig_count,
            "confidence_tier": confidence,
            "primary_narrative": narrative,
            "size_tier": size,
            "revenue_band": rev_band,
            "bond_amount": bond_amt,
            "already_fsm_customer": bool(r.get("_already_fsm_customer", False)),
            "already_fsm_vendor": safe_str(r.get("_already_fsm_vendor")),
            "score_evidence": " | ".join(
                dp_evid + ss_evid + ms_evid + op_evid + dm_evid + icp_evid + dq_evid
            ),
        })

    score_df = pd.DataFrame(rows)

    # Drop the private helper columns we used to pass FSM-customer state
    # into score_disqualifiers. They're already preserved as public
    # columns (already_fsm_customer, already_fsm_vendor) on score_df.
    df = df.drop(columns=[c for c in ["_already_fsm_customer", "_already_fsm_vendor"] if c in df.columns])

    merged = df.merge(score_df, on="license_no", how="left")

    # Sort for final_rank. FSM-customer contractors are hard-pushed to
    # the bottom of the list regardless of score_total — we never want
    # a rep pitching them, even if their other signals look strong. The
    # -15 disqualifier keeps the audit trail, but the sort ordering is
    # what actually prevents them from showing up in the top 25.
    merged = merged.sort_values(
        by=["already_fsm_customer", "score_total"],
        ascending=[True, False],
    ).reset_index(drop=True)
    merged["final_rank"] = merged.index + 1

    merged.to_csv(OUTPUT_CSV, index=False)
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    snap = SNAPSHOT_DIR / f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.csv"
    merged.to_csv(snap, index=False)

    # ---- Summary ----
    print("=" * 78)
    print("Top 25 after scoring")
    print("=" * 78)
    cols_show = [
        "final_rank", "rank", "business_name", "city",
        "score_total", "score_direct_pain", "score_scaling_strain",
        "score_demand_pull", "score_icp_fit", "size_tier",
        "primary_narrative", "confidence_tier",
    ]
    with pd.option_context("display.max_colwidth", 35, "display.width", 220):
        print(merged[cols_show].head(25).to_string(index=False))

    print()
    print("Primary narrative distribution (top 25):")
    top25 = merged.head(25)
    for n, c in top25["primary_narrative"].value_counts().items():
        print(f"  {n:<18} {c}")

    print()
    print("Confidence tier distribution (top 25):")
    for c, n in top25["confidence_tier"].value_counts().items():
        print(f"  {c:<10} {n}")

    print()
    print("Size tier distribution (top 25):")
    for t in ["XL", "L", "M", "S"]:
        c = int((top25["size_tier"] == t).sum())
        if c:
            print(f"  {t:<4} {c}")

    print()
    print("ICP fit distribution (top 25):")
    dual = int((top25["class"] == "CR-39").sum())
    res = int((top25["class"].isin(["R-39", "R-39R"])).sum())
    print(f"  Dual (CR-39):        {dual}")
    print(f"  Residential only:    {res}")

    print()
    print("Revenue band distribution (top 25, from ROC bond amount):")
    for band in ["$5M+", "$1.5M–$5M", "$500K–$1.5M", "$150K–$500K", "Under $150K", "Unknown"]:
        c = int((top25["revenue_band"] == band).sum())
        if c:
            print(f"  {band:<16} {c}")

    # Biggest rank changes from original to scored
    print()
    print("Biggest rank promotions (moved UP in scoring):")
    merged["rank_delta"] = merged["rank"].astype(int) - merged["final_rank"].astype(int)
    promotions = merged.sort_values("rank_delta", ascending=False).head(10)
    for _, r in promotions.iterrows():
        print(f"  rank {int(r['rank']):>2} -> final_rank {int(r['final_rank']):>2}  "
              f"(+{int(r['rank_delta']):>2})  "
              f"{r['business_name'][:40]:<40}  "
              f"score={r['score_total']:.1f}  {r['primary_narrative']}")

    print()
    print("Biggest rank demotions (moved DOWN in scoring):")
    demotions = merged.sort_values("rank_delta", ascending=True).head(10)
    for _, r in demotions.iterrows():
        print(f"  rank {int(r['rank']):>2} -> final_rank {int(r['final_rank']):>2}  "
              f"({int(r['rank_delta']):>3})  "
              f"{r['business_name'][:40]:<40}  "
              f"score={r['score_total']:.1f}  {r['primary_narrative']}")

    print()
    print("Outputs:")
    print(f"  {OUTPUT_CSV.relative_to(ROOT)}")
    print(f"  {snap.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
