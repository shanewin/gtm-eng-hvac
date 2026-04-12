#!/usr/bin/env python3
"""
Card-based dossier renderer.

No LLM narrative. No executive summary. No synthesis.

Every card is pulled DIRECTLY from cached raw data or from specific columns in
the scored CSV. Each claim carries a citation pointing to its source
(review date, job apply link, verbatim quote). Cards only appear when their
underlying signal actually fires.

Cards rendered, in priority order:

  1. Header (business baseline: name, tier, years licensed, reviews, website, GBP)
  2. Decision maker (owner, booking phone, website, GBP) — always
  3. Hiring intelligence — if cached job postings exist
  4. Customers who switched from a competitor — if llm_customer_refugee_mentions > 0
  5. One-person operation — if founder_involvement or key_person_dependency is heavy
  6. Dispatch distribution — dot plot from per-review dispatch extractions
  7. Pain evidence — if any pain quote exists
  8. Growth momentum — if momentum quote exists
  9. Review velocity — month-by-month chart, if velocity is elevated
 10. Review burst — if crisis or surge burst detected
 11. Tech stack gap — always (shows detected stack + FSM absence)

Usage:
  python pipeline/18_dossier_cards.py                # final_rank 1
  python pipeline/18_dossier_cards.py --rank 13
  python pipeline/18_dossier_cards.py --all
"""

from __future__ import annotations

import argparse
import html
import json
import math
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
SCORED_CSV = ROOT / "data" / "03_hidden_gems" / "scored.csv"
CONTACTS_CSV = ROOT / "data" / "04_contacts" / "augmented.csv"
SERPAPI_RAW_DIR = ROOT / "data" / "signals_raw" / "serpapi_reviews"
JOBS_RAW_DIR = ROOT / "data" / "signals_raw" / "serpapi_jobs"
APOLLO_JOBS_DIR = ROOT / "data" / "signals_raw" / "apollo_jobs"
DISPATCH_RAW_DIR = ROOT / "data" / "signals_raw" / "dispatch_delay"
LLM_RAW_DIR = ROOT / "data" / "signals_raw" / "review_llm"
OUT_DIR = ROOT / "deliverables"

FSM_BUYER_ROLE_KEYWORDS = [
    "dispatch", "scheduler", "scheduling", "coordinator",
    "operations manager", "ops manager", "service manager",
    "customer service representative", "csr",
]


# ---------- helpers ----------

def esc(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return html.escape(str(v))


def nn(v) -> bool:
    if v is None:
        return False
    if isinstance(v, float) and pd.isna(v):
        return False
    s = str(v).strip()
    return bool(s) and s.lower() not in {"nan", "none", "—", "null"}


def s(v, default="") -> str:
    return str(v).strip() if nn(v) else default


def i(v, default=0) -> int:
    try:
        return int(float(v)) if nn(v) else default
    except (TypeError, ValueError):
        return default


def f(v, default=0.0) -> float:
    try:
        return float(v) if nn(v) else default
    except (TypeError, ValueError):
        return default


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def intent_tier(score: float) -> tuple[str, str]:
    if score >= 40:
        return "HIGH INTENT", "tier-high"
    if score >= 25:
        return "STRONG INTENT", "tier-strong"
    return "EMERGING INTENT", "tier-emerging"


def build_signal_chips(row: pd.Series, jobs: list[dict]) -> list[dict]:
    """
    Build the visual signal chips shown on the index row and dossier header.
    Each chip has: label, value (optional str), css_class.
    Colors communicate signal category at a glance:
      - red    = pain (customers complaining)
      - amber  = growth (hiring, momentum, velocity)
      - purple = demand pull (one-person shop winning on reputation)
      - slate  = no FSM platform
      - crimson = actively hiring FSM buyer role
      - yellow warning = thin sample size
    """
    chips = []

    direct_pain = f(row.get("score_direct_pain"))
    scaling = f(row.get("score_scaling_strain"))
    demand = f(row.get("score_demand_pull"))

    if direct_pain >= 5:
        chips.append({"label": "PAIN", "value": f"{direct_pain:.0f}", "cls": "chip-pain"})
    if scaling >= 5:
        chips.append({"label": "GROWTH", "value": f"{scaling:.0f}", "cls": "chip-growth"})
    if demand >= 5:
        chips.append({"label": "DEMAND PULL", "value": f"{demand:.0f}", "cls": "chip-demand"})

    # FSM stack gap
    if not row.get("has_any_booking_tool"):
        chips.append({"label": "NO FSM", "value": "", "cls": "chip-nofsm"})

    # FSM buyer role actively hiring
    buyer_jobs = [j for j in jobs if fsm_buyer_role(j["title"])]
    if buyer_jobs:
        n = len(buyer_jobs)
        chips.append({
            "label": f"HIRING DISPATCH" + ("" if n == 1 else f" ({n})"),
            "value": "",
            "cls": "chip-hiring",
        })

    # Thin sample warning
    sample = i(row.get("llm_review_count_analyzed"))
    if 0 < sample < 10:
        chips.append({"label": f"THIN SAMPLE ({sample})", "value": "", "cls": "chip-warn"})

    return chips


def dominant_signal_color(row: pd.Series) -> str:
    """Returns a CSS class for the dominant scoring dimension, used as a
    colored accent on the left edge of each index row."""
    direct_pain = f(row.get("score_direct_pain"))
    scaling = f(row.get("score_scaling_strain"))
    demand = f(row.get("score_demand_pull"))

    m = max(direct_pain, scaling, demand)
    if m < 5:
        return "accent-none"
    if direct_pain == m:
        return "accent-pain"
    if scaling == m:
        return "accent-growth"
    return "accent-demand"


def fsm_buyer_role(title: str) -> bool:
    t = (title or "").lower()
    return any(k in t for k in FSM_BUYER_ROLE_KEYWORDS)


# ---------- data loaders ----------

def load_reviews(place_id: str) -> list[dict]:
    data = load_json(SERPAPI_RAW_DIR / f"{place_id}.json")
    reviews = data.get("reviews") or []
    out = []
    for r in reviews:
        iso = r.get("iso_date") or ""
        snippet = r.get("snippet") or ""
        if not snippet:
            ex = r.get("extracted_snippet") or {}
            if isinstance(ex, dict):
                snippet = ex.get("original") or ""
        if iso:
            out.append({
                "date": iso[:10],
                "rating": r.get("rating"),
                "snippet": snippet.strip(),
            })
    out.sort(key=lambda r: r["date"], reverse=True)
    return out


def _parse_relative_posted(rel: str, anchor_iso: str) -> str:
    """Convert 'X days ago' / '2 weeks ago' / '1 month ago' to an ISO date,
    anchored to the fetched_at timestamp."""
    if not rel or not anchor_iso:
        return ""
    m = re.match(r"(\d+)\+?\s*(day|week|month|hour)s?\s*ago", rel.strip(), re.IGNORECASE)
    if not m:
        return ""
    n = int(m.group(1))
    unit = m.group(2).lower()
    try:
        anchor = datetime.fromisoformat(anchor_iso.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return ""
    if unit == "hour":
        delta_days = 0
    elif unit == "day":
        delta_days = n
    elif unit == "week":
        delta_days = n * 7
    elif unit == "month":
        delta_days = n * 30
    else:
        return ""
    from datetime import timedelta
    d = anchor - timedelta(days=delta_days)
    return d.date().isoformat()


VALIDATOR_DIR = ROOT / "data" / "signals_raw" / "validator"


def load_validator_cache(place_id: str) -> dict:
    """Return the LLM validator's belongs/reject decisions for this
    contractor, or an empty dict if no cache exists."""
    path = VALIDATOR_DIR / f"{place_id}.json"
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}
    return data.get("validated") or {}


def _validator_kept_job_titles(place_id: str) -> set[str]:
    """Set of job titles the validator flagged as belongs=True for this
    contractor. The SerpAPI loader consults this set instead of doing its
    own slug matching."""
    v = load_validator_cache(place_id)
    jobs = v.get("jobs") or []
    return {
        (item.get("value") or "").strip()
        for item in jobs
        if item.get("belongs")
    }


def load_jobs(place_id: str, business_name: str = "") -> tuple[list[dict], str]:
    """
    Load job postings for a contractor.

    Source of truth for "does this posting belong to this contractor":
    `pipeline/17_candidate_validator.py`, cached in
    `data/signals_raw/validator/{place_id}.json`. We keep every SerpAPI
    posting whose title appears in the validator's kept set, then merge
    in Apollo postings (which are scoped by Apollo org ID and are always
    trusted).

    Date source priority:
      1. Apollo posted_at (ISO timestamp, exact)
      2. SerpAPI 'X days ago' computed from fetched_at
      3. Fall back to fetched_at labeled "first observed on"

    Returns (jobs, fetched_at_date). Each job dict has:
      title, via, location, link, date_iso, date_source, apollo_url
    """
    serp_data = load_json(JOBS_RAW_DIR / f"{place_id}.json")
    fetched_at_raw = serp_data.get("fetched_at") or ""
    fetched_date = fetched_at_raw[:10] if fetched_at_raw else ""
    resp = serp_data.get("response") or {}
    serp_jobs_raw = resp.get("jobs_results") or []
    rejected_jobs = serp_data.get("rejected_jobs") or []

    # Build the pool of jobs to consider. Pre-filter at cache-write time
    # may have sidelined some into `rejected_jobs`. The validator decided
    # across the union, so we look in both.
    all_serp_jobs = list(serp_jobs_raw) + list(rejected_jobs)

    kept_titles = _validator_kept_job_titles(place_id)
    # If validator hasn't run yet, keep whatever the cache-time filter
    # left in jobs_results. This keeps the script runnable before the
    # validator has produced a cache.
    if kept_titles:
        serp_jobs = [
            j for j in all_serp_jobs
            if (j.get("title") or "").strip() in kept_titles
        ]
    else:
        serp_jobs = serp_jobs_raw

    apollo_data = load_json(APOLLO_JOBS_DIR / f"{place_id}.json")
    apollo_jobs = apollo_data.get("organization_job_postings") or []

    # Index Apollo jobs by exact title so SerpAPI postings can pick up
    # an Apollo posted_at when present.
    apollo_by_title: dict[str, dict] = {}
    for aj in apollo_jobs:
        t = (aj.get("title") or "").strip()
        if t and t not in apollo_by_title:
            apollo_by_title[t] = aj

    out = []
    seen_titles: set[str] = set()
    for j in serp_jobs:
        title = s(j.get("title"))
        apply_opts = j.get("apply_options") or []
        link = apply_opts[0].get("link") if apply_opts else ""
        relative_posted = s((j.get("detected_extensions") or {}).get("posted_at"))
        source_link = s(j.get("source_link"))

        date_iso = ""
        date_source = ""
        apollo_url = ""

        # 1. Try an exact-title Apollo date
        apollo_match = apollo_by_title.get(title.strip())
        if apollo_match:
            ap_posted = apollo_match.get("posted_at") or ""
            if ap_posted:
                date_iso = ap_posted[:10]
                date_source = "apollo"
                apollo_url = apollo_match.get("url") or ""

        # 2. Try SerpAPI relative
        if not date_iso and relative_posted:
            computed = _parse_relative_posted(relative_posted, fetched_at_raw)
            if computed:
                date_iso = computed
                date_source = "serpapi_relative"

        # 3. Fall back to fetched_at
        if not date_iso:
            date_iso = fetched_date
            date_source = "first_observed"

        out.append({
            "title": title,
            "via": s(j.get("via")),
            "location": s(j.get("location")),
            "link": s(link) or source_link,
            "apollo_url": apollo_url,
            "date_iso": date_iso,
            "date_source": date_source,
            "description_excerpt": s(j.get("description"))[:280] if j.get("description") else "",
        })
        seen_titles.add(title.strip())

    # Append Apollo jobs that weren't in SerpAPI (exact title not seen).
    for aj in apollo_jobs:
        title = (aj.get("title") or "").strip()
        if not title or title in seen_titles:
            continue
        ap_posted = aj.get("posted_at") or ""
        if not ap_posted:
            continue
        out.append({
            "title": title,
            "via": "LinkedIn",
            "location": f'{s(aj.get("city"))}, {s(aj.get("state"))}'.strip(", "),
            "link": s(aj.get("url")),
            "apollo_url": s(aj.get("url")),
            "date_iso": ap_posted[:10],
            "date_source": "apollo",
            "description_excerpt": "",
        })

    # Sort by date descending (most recent first)
    out.sort(key=lambda j: j.get("date_iso", ""), reverse=True)
    return out, fetched_date


def load_dispatch_extractions(place_id: str) -> list[dict]:
    data = load_json(DISPATCH_RAW_DIR / f"{place_id}.json")
    parsed = data.get("parsed") or {}
    return parsed.get("extractions") or []


def load_llm_analysis(place_id: str) -> tuple[dict, list[dict]]:
    """
    Load the structured LLM review analysis produced by 08b_review_llm.py.

    Returns (parsed, indexed_reviews) where:
      - parsed is the LLM response dict with pain_mentions / momentum_mentions /
        switcher_mentions / smooth_ops_mentions arrays
      - indexed_reviews is the list of reviews the LLM was shown, in the same
        order as the prompt (review_index 1 == most recent). Each entry has
        review_index, date, rating, snippet.

    Returns ({}, []) if the cache is missing or uses the old schema without
    per-mention arrays.
    """
    data = load_json(LLM_RAW_DIR / f"{place_id}.json")
    if not data:
        return {}, []
    parsed = data.get("parsed") or {}
    # Only trust caches from the new schema
    if "pain_mentions" not in parsed:
        return {}, []
    indexed_reviews = data.get("indexed_reviews") or []
    return parsed, indexed_reviews


def resolve_mention(
    mention: dict,
    indexed_reviews: list[dict],
) -> tuple[str, str, str]:
    """
    Return (date, rating_str, verbatim_quote) for a single LLM mention by
    looking up its review_index in the indexed_reviews list. Falls back to
    ('', '', mention_quote) when the index can't be resolved.
    """
    quote = mention.get("quote") or ""
    idx = mention.get("review_index")
    if not isinstance(idx, int) or idx < 1 or idx > len(indexed_reviews):
        return "", "", quote
    r = indexed_reviews[idx - 1]
    date = r.get("date") or ""
    rating = r.get("rating")
    rating_str = f"{float(rating):.0f}★" if rating is not None else ""
    return date, rating_str, quote


# ---------- quote handling: never display full reviews ----------

def find_review_date(quote: str, reviews: list[dict]) -> tuple[str, str, str]:
    """
    Match a quote back to the review it came from.
    Returns (date, rating_str, review_snippet_for_sentence_extraction).
    Fuzzy-matches by exact substring first, then by word-bag overlap.
    """
    if not quote or not reviews:
        return "", "", ""
    norm_quote = re.sub(r"\s+", " ", quote).strip()

    # Exact substring pass
    for probe_len in (120, 80, 40, 20):
        sub = norm_quote[:probe_len]
        if len(sub) < 10:
            continue
        for r in reviews:
            snippet = re.sub(r"\s+", " ", (r.get("snippet") or "")).strip()
            if sub and sub in snippet:
                rating = r.get("rating")
                rating_str = f"{float(rating):.0f}★" if rating is not None else ""
                return r.get("date", ""), rating_str, snippet

    # Fuzzy fallback: word-bag overlap on the 12 most distinctive words
    def distinctive_words(text: str) -> set[str]:
        words = re.findall(r"[a-zA-Z]{4,}", text.lower())
        stop = {"that", "this", "with", "have", "from", "were", "they", "them",
                "their", "would", "could", "which", "when", "will", "been",
                "very", "just", "some", "into", "also", "than", "only", "about",
                "like", "what", "your", "there", "after", "other", "before"}
        return {w for w in words if w not in stop}

    qw = distinctive_words(norm_quote)
    if len(qw) < 3:
        return "", "", ""
    best = None
    best_score = 0
    for r in reviews:
        snippet = r.get("snippet") or ""
        rw = distinctive_words(snippet)
        if not rw:
            continue
        inter = qw & rw
        if len(inter) >= max(3, int(len(qw) * 0.3)):
            score = len(inter)
            if score > best_score:
                best_score = score
                best = r
    if best:
        rating = best.get("rating")
        rating_str = f"{float(rating):.0f}★" if rating is not None else ""
        return best.get("date", ""), rating_str, best.get("snippet", "")

    return "", "", ""


def trim_to_relevant(quote: str, keywords: list[str], max_chars: int = 220) -> str:
    """
    Extract the most signal-relevant sentence(s) from a quote.
    Never returns more than max_chars. Never returns less than 1 sentence.
    """
    if not quote:
        return ""
    quote = re.sub(r"\s+", " ", quote).strip()
    if len(quote) <= max_chars:
        return quote

    sentences = re.split(r"(?<=[.!?])\s+", quote)
    sentences = [s.strip() for s in sentences if s.strip()]
    if not sentences:
        return quote[:max_chars].rstrip() + "…"

    kw_lower = [k.lower() for k in (keywords or [])]

    # Score each sentence by keyword hits
    scored: list[tuple[int, int, str]] = []
    for idx, sent in enumerate(sentences):
        sl = sent.lower()
        hits = sum(1 for k in kw_lower if k in sl)
        scored.append((hits, idx, sent))

    # Pick best (highest hits, earliest on tie)
    scored.sort(key=lambda x: (-x[0], x[1]))
    best_hits, best_idx, best = scored[0]

    # If nothing matched keywords, take first sentence
    if best_hits == 0:
        best_idx = 0
        best = sentences[0]

    result = best
    # Try to add a neighbor if still short enough
    if len(result) < 140 and len(sentences) > 1:
        neighbors = []
        if best_idx + 1 < len(sentences):
            neighbors.append(sentences[best_idx + 1])
        if best_idx > 0:
            neighbors.append(sentences[best_idx - 1])
        for nb in neighbors:
            candidate = (result + " " + nb) if best_idx + 1 < len(sentences) and nb == sentences[best_idx + 1] else (nb + " " + result)
            if len(candidate) <= max_chars:
                result = candidate
                break

    if len(result) > max_chars:
        result = result[: max_chars - 1].rstrip() + "…"
    return result


def make_cite(date: str, rating_str: str, extra: str = "") -> str:
    """Build a citation line for a review-derived blockquote."""
    bits = []
    if date:
        bits.append(date)
    if rating_str:
        bits.append(rating_str)
    if extra:
        bits.append(extra)
    return " · ".join(bits) if bits else "Source: Google review"


def count_name_mentions(reviews: list[dict], name: str) -> list[dict]:
    """
    Return reviews (with date/rating/snippet) that mention the given first name.
    Case-insensitive, whole-word match.
    """
    if not name or not reviews:
        return []
    pat = re.compile(r"\b" + re.escape(name) + r"\b", re.IGNORECASE)
    out = []
    for r in reviews:
        snippet = r.get("snippet") or ""
        if pat.search(snippet):
            out.append(r)
    return out


def monthly_review_counts(reviews: list[dict], months_back: int = 12) -> list[tuple[str, int]]:
    today = datetime.now(timezone.utc).date()
    year = today.year
    month = today.month
    months: list[tuple[int, int]] = []
    for _ in range(months_back):
        months.append((year, month))
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    months.reverse()

    counts: dict[tuple[int, int], int] = {m: 0 for m in months}
    for r in reviews:
        d = r.get("date") or ""
        if len(d) >= 7:
            try:
                y = int(d[:4])
                m = int(d[5:7])
                key = (y, m)
                if key in counts:
                    counts[key] += 1
            except (ValueError, TypeError):
                continue
    return [(f"{y:04d}-{m:02d}", counts[(y, m)]) for y, m in months]


# ---------- SVG charts ----------

def svg_dispatch_dotplot(extractions: list[dict]) -> str:
    """Dot plot of per-review dispatch times on linear 0-168h axis."""
    if not extractions:
        return ""
    width = 740
    height = 140
    left = 20
    right = 20
    chart_w = width - left - right
    axis_y = 88
    max_h = 168.0  # one week

    # Axis
    parts = [
        f'<line x1="{left}" y1="{axis_y}" x2="{left + chart_w}" y2="{axis_y}" stroke="#cfd3dc" stroke-width="2"/>'
    ]
    ticks = [0, 4, 8, 24, 48, 72, 168]
    tick_labels = ["0h", "4h", "8h", "24h", "48h", "72h", "1 week"]
    for t, lbl in zip(ticks, tick_labels):
        x = left + (t / max_h) * chart_w
        parts.append(f'<line x1="{x}" y1="{axis_y - 5}" x2="{x}" y2="{axis_y + 5}" stroke="#9aa2b0" stroke-width="1"/>')
        parts.append(f'<text x="{x}" y="{axis_y + 22}" text-anchor="middle" font-size="11" fill="#666">{lbl}</text>')

    # Dots
    # Group by rounded hour for jitter
    hour_groups: dict[int, list[dict]] = {}
    for e in extractions:
        h = min(e.get("estimated_delay_hours", 0) or 0, max_h)
        bucket = round(h)
        hour_groups.setdefault(bucket, []).append(e)

    for bucket, group in hour_groups.items():
        x = left + (bucket / max_h) * chart_w
        for idx, e in enumerate(group):
            sentiment = e.get("sentiment", "")
            if sentiment == "negative":
                color = "#c4332b"
            elif sentiment == "positive":
                color = "#1a8c4a"
            else:
                color = "#9aa2b0"
            jitter = (idx - (len(group) - 1) / 2) * 14
            y = axis_y - 24 + jitter
            parts.append(
                f'<circle cx="{x}" cy="{y}" r="8" fill="{color}" stroke="white" stroke-width="2" opacity="0.92"/>'
            )

    # Legend
    legend_y = 125
    parts.append(f'<circle cx="{left + 8}" cy="{legend_y}" r="6" fill="#1a8c4a"/>')
    parts.append(f'<text x="{left + 20}" y="{legend_y + 4}" font-size="11" fill="#555">positive</text>')
    parts.append(f'<circle cx="{left + 90}" cy="{legend_y}" r="6" fill="#9aa2b0"/>')
    parts.append(f'<text x="{left + 102}" y="{legend_y + 4}" font-size="11" fill="#555">neutral</text>')
    parts.append(f'<circle cx="{left + 168}" cy="{legend_y}" r="6" fill="#c4332b"/>')
    parts.append(f'<text x="{left + 180}" y="{legend_y + 4}" font-size="11" fill="#555">negative</text>')
    parts.append(f'<text x="{left + chart_w}" y="{legend_y + 4}" text-anchor="end" font-size="11" fill="#888">each dot = one review</text>')

    return f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}">{"".join(parts)}</svg>'


MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def svg_monthly_line(monthly: list[tuple[str, int]]) -> str:
    """Line chart of monthly review counts over the last 12 months. Each
    input tuple is ("YYYY-MM", count). X-axis labels render as "Mar 25"."""
    if not monthly:
        return ""
    width = 740
    height = 220
    left = 40
    right = 20
    top = 20
    bottom = 42
    chart_w = width - left - right
    chart_h = height - top - bottom

    n = len(monthly)
    max_count = max((c for _, c in monthly), default=1) or 1
    # Round up max_count to a nice gridline cap
    gridline_max = max(max_count, 5)
    if gridline_max > 10:
        gridline_max = int(math.ceil(gridline_max / 5.0) * 5)

    parts = []

    # Gridlines
    grid_steps = 4
    for gi in range(grid_steps + 1):
        val = gridline_max * gi / grid_steps
        y = top + chart_h - (val / gridline_max) * chart_h
        parts.append(
            f'<line x1="{left}" y1="{y}" x2="{left + chart_w}" y2="{y}" '
            f'stroke="#eef0f3" stroke-width="1"/>'
        )
        parts.append(
            f'<text x="{left - 6}" y="{y + 4}" text-anchor="end" '
            f'font-size="10" fill="#888">{int(val)}</text>'
        )

    # Point positions
    if n == 1:
        xs = [left + chart_w / 2]
    else:
        xs = [left + i * chart_w / (n - 1) for i in range(n)]
    ys = [
        top + chart_h - (c / gridline_max) * chart_h
        for _, c in monthly
    ]

    # Filled area under the line for visual weight
    area_pts = " ".join(f"{x},{y}" for x, y in zip(xs, ys))
    baseline = top + chart_h
    area_d = (
        f"M {xs[0]},{baseline} "
        f"L " + " L ".join(f"{x},{y}" for x, y in zip(xs, ys)) +
        f" L {xs[-1]},{baseline} Z"
    )
    parts.append(
        f'<path d="{area_d}" fill="#0066cc" fill-opacity="0.08"/>'
    )

    # The line itself
    line_d = "M " + " L ".join(f"{x},{y}" for x, y in zip(xs, ys))
    parts.append(
        f'<path d="{line_d}" fill="none" stroke="#0066cc" '
        f'stroke-width="2.2" stroke-linejoin="round" stroke-linecap="round"/>'
    )

    # Dots + count labels + x-axis month labels
    for (label, count), x, y in zip(monthly, xs, ys):
        color = "#0066cc" if count > 0 else "#d0d4db"
        parts.append(
            f'<circle cx="{x}" cy="{y}" r="3.5" fill="{color}" '
            f'stroke="white" stroke-width="1.5"/>'
        )
        if count > 0:
            parts.append(
                f'<text x="{x}" y="{y - 9}" text-anchor="middle" '
                f'font-size="11" fill="#333" font-weight="600">{count}</text>'
            )
        # X-axis label: "Mar 25"
        try:
            mo_num = int(label[5:7])
            yr_short = label[2:4]
            x_label = f"{MONTH_ABBR[mo_num - 1]} {yr_short}"
        except (ValueError, IndexError):
            x_label = label
        parts.append(
            f'<text x="{x}" y="{top + chart_h + 18}" text-anchor="middle" '
            f'font-size="10" fill="#666">{x_label}</text>'
        )

    return (
        f'<svg width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">{"".join(parts)}</svg>'
    )


# ---------- CSS ----------

CSS = """
* { box-sizing: border-box; }
body {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  color: #1a1a1a;
  background: #eceef2;
  margin: 0;
  padding: 32px 16px;
  line-height: 1.55;
}
.dossier {
  max-width: 860px;
  margin: 0 auto;
  background: white;
  border-radius: 14px;
  box-shadow: 0 4px 24px rgba(0,0,0,0.08);
  overflow: hidden;
}
.header {
  padding: 28px 36px 24px;
  background: linear-gradient(180deg, #fafbfd 0%, #f2f4f8 100%);
  border-bottom: 1px solid #e0e4eb;
  border-left: 8px solid #d8dce3;
}
.header.accent-pain { border-left-color: #c4332b; }
.header.accent-growth { border-left-color: #e6a700; }
.header.accent-demand { border-left-color: #7c4dff; }
.header.accent-none { border-left-color: #d8dce3; }
.chips {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin: 12px 0 0;
}
.chip {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 4px 10px;
  border-radius: 999px;
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.6px;
  white-space: nowrap;
  border: 1px solid transparent;
}
.chip .chip-val {
  background: rgba(255,255,255,0.55);
  color: inherit;
  padding: 1px 6px;
  border-radius: 999px;
  font-size: 9px;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
}
.chip-pain    { background: #fde8e8; color: #8f1a1a; border-color: #f5c2c2; }
.chip-growth  { background: #fff4d6; color: #7a4d00; border-color: #f0d896; }
.chip-demand  { background: #efe8ff; color: #4a2a9a; border-color: #d8c8ff; }
.chip-nofsm   { background: #eceff4; color: #3d4656; border-color: #d0d6e0; }
.chip-hiring  { background: #4a0808; color: #ffdcdc; border-color: #4a0808; }
.chip-hiring .chip-val { color: #ffdcdc; background: rgba(255,255,255,0.15); }
.chip-warn    { background: #fff9d6; color: #7a5c00; border-color: #f0d896; }
.header-top {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 16px;
}
.biz-name {
  font-size: 28px;
  font-weight: 700;
  letter-spacing: -0.4px;
  margin: 0 0 4px;
  color: #0a0a1a;
}
.biz-meta {
  color: #666;
  font-size: 13px;
  margin: 0 0 8px;
}
.links {
  font-size: 13px;
  margin: 6px 0 0;
}
.links a {
  color: #0066cc;
  text-decoration: none;
  margin-right: 14px;
  font-weight: 500;
}
.links a:hover { text-decoration: underline; }
.tier-badge {
  display: inline-block;
  padding: 7px 14px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.8px;
  white-space: nowrap;
}
.tier-high { background: #fde8e8; color: #9a1a1a; }
.tier-strong { background: #fff4d6; color: #8a5a00; }
.tier-emerging { background: #e6f2ff; color: #003b7a; }

.badge-stack {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 6px;
}
.size-badge {
  display: inline-block;
  padding: 4px 10px;
  border-radius: 4px;
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.5px;
  white-space: nowrap;
  background: #eef2f8;
  color: #2a3b55;
  border: 1px solid #d6dceb;
}
.size-xl { background: #e0e8f5; color: #1a2b4a; border-color: #c0cce0; }
.size-l  { background: #e8edf6; color: #1f3054; border-color: #ced7e6; }
.size-m  { background: #eef2f8; color: #2a3b55; border-color: #d6dceb; }
.size-s  { background: #f4f6fa; color: #445266; border-color: #e0e5ee; }
.fresh-badge {
  display: inline-block;
  padding: 4px 10px;
  border-radius: 4px;
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.5px;
  white-space: nowrap;
}
.fresh-hot  { background: #fff0e8; color: #8c3a14; border: 1px solid #f4c9b0; }
.fresh-warm { background: #fff8e6; color: #8a5a00; border: 1px solid #efdba6; }

.card {
  padding: 24px 36px;
  border-bottom: 1px solid #f0f2f5;
}
.card:last-child { border-bottom: none; }
.card-label {
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 1.4px;
  color: #888;
  font-weight: 700;
  margin: 0 0 6px;
  display: flex;
  align-items: center;
  gap: 10px;
}
.card-label .tag {
  background: #fde8e8;
  color: #9a1a1a;
  padding: 2px 7px;
  border-radius: 3px;
  font-size: 9px;
  letter-spacing: 0.6px;
}
.card-label .tag-amber {
  background: #fff4d6;
  color: #8a5a00;
}
.card-label .tag-green {
  background: #e6f4ea;
  color: #14532d;
}
.card-headline {
  font-size: 18px;
  font-weight: 700;
  color: #1a1a1a;
  margin: 0 0 10px;
  letter-spacing: -0.2px;
}
.card-subtitle {
  font-size: 13px;
  color: #666;
  margin: 0 0 12px;
}
.card-stats {
  display: flex;
  gap: 28px;
  margin: 12px 0 16px;
  flex-wrap: wrap;
}
.stat { flex: 0 0 auto; }
.stat .val {
  font-size: 22px;
  font-weight: 700;
  color: #1a1a1a;
  line-height: 1;
  display: block;
  font-variant-numeric: tabular-nums;
}
.stat .lbl {
  font-size: 11px;
  color: #888;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-top: 4px;
  display: block;
}

blockquote.evidence {
  margin: 10px 0;
  padding: 12px 16px;
  background: #fafbfd;
  border-left: 3px solid #0066cc;
  border-radius: 3px;
  font-style: italic;
  font-size: 14px;
  color: #333;
  line-height: 1.55;
}
blockquote.evidence .cite {
  display: block;
  margin-top: 8px;
  font-style: normal;
  font-size: 11px;
  color: #888;
  text-transform: uppercase;
  letter-spacing: 0.6px;
}
blockquote.evidence.negative { border-left-color: #c4332b; }
blockquote.evidence.positive { border-left-color: #1a8c4a; }
blockquote.evidence.warning { background: #fff8e1; border-left-color: #e6a700; }
blockquote.evidence.grouped {
  font-style: normal;
  padding: 10px 16px 12px;
}
blockquote.evidence.grouped .review-cite {
  font-size: 11px;
  color: #888;
  text-transform: uppercase;
  letter-spacing: 0.6px;
  margin-bottom: 8px;
  display: flex;
  align-items: baseline;
  gap: 8px;
}
blockquote.evidence.grouped .review-cite .obs-count {
  background: rgba(0,0,0,0.06);
  color: #555;
  padding: 1px 7px;
  border-radius: 999px;
  font-size: 10px;
}
blockquote.evidence.grouped .obs {
  font-style: italic;
  padding: 6px 0;
  border-top: 1px dashed rgba(0,0,0,0.08);
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 12px;
}
blockquote.evidence.grouped .obs:first-of-type { border-top: none; padding-top: 2px; }
blockquote.evidence.grouped .obs .obs-label {
  flex: 0 0 auto;
  font-style: normal;
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  color: #888;
  background: rgba(0,0,0,0.05);
  padding: 2px 8px;
  border-radius: 999px;
  margin-top: 2px;
}

.job-list { margin: 8px 0 0; padding: 0; list-style: none; }
.job-list li {
  padding: 12px 14px;
  margin-bottom: 8px;
  background: #fafbfd;
  border: 1px solid #e8ebf0;
  border-radius: 6px;
  font-size: 14px;
}
.job-list li.buyer-role {
  border-left: 3px solid #1a8c4a;
  background: #f1f8f3;
}
.job-list .title {
  font-weight: 600;
  color: #1a1a1a;
  margin-bottom: 2px;
}
.job-list .buyer-flag {
  display: inline-block;
  background: #1a8c4a;
  color: white;
  padding: 1px 7px;
  border-radius: 3px;
  font-size: 9px;
  font-weight: 700;
  letter-spacing: 0.6px;
  margin-left: 8px;
  vertical-align: 2px;
}
.job-list .meta {
  font-size: 12px;
  color: #666;
  margin-top: 3px;
}
.job-list .meta a {
  color: #0066cc;
  text-decoration: none;
  margin-left: 8px;
}
.job-list .meta a:hover { text-decoration: underline; }

.contact-card {
  background: #f8f9fb;
  border: 1px solid #e8ebf0;
  border-radius: 8px;
  padding: 18px 22px;
}
.contact-card .owner-line {
  font-size: 18px;
  font-weight: 700;
  margin: 0 0 2px;
  color: #0a0a1a;
}
.contact-card .owner-source {
  font-size: 12px;
  color: #1a8c4a;
  margin: 0 0 16px;
  font-weight: 500;
}
.contact-card .owner-source::before { content: "✓ "; }
.referenced-people {
  margin: 0 0 18px;
  padding: 12px 14px;
  background: #ffffff;
  border: 1px solid #e8ebf0;
  border-radius: 6px;
}
.referenced-people .rp-label {
  font-size: 11px;
  color: #666;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin: 0 0 8px;
}
.referenced-people .rp-list {
  margin: 0;
  padding: 0;
  list-style: none;
}
.referenced-people .rp-row {
  font-size: 13px;
  color: #1a1a1a;
  padding: 3px 0;
  line-height: 1.45;
}
.referenced-people .rp-row strong {
  color: #0a0a1a;
  font-weight: 600;
}
.referenced-people .rp-count {
  color: #888;
  font-size: 11px;
  font-weight: 500;
}
.referenced-people .rp-quote {
  color: #555;
  font-style: italic;
}
.contact-card dl {
  margin: 0;
  display: grid;
  grid-template-columns: 150px 1fr;
  gap: 10px 16px;
}
.contact-card dt {
  font-size: 11px;
  color: #666;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}
.contact-card dd {
  font-size: 14px;
  margin: 0;
  color: #1a1a1a;
}
.contact-card dd a { color: #0066cc; text-decoration: none; }
.contact-card dd a:hover { text-decoration: underline; }
.contact-card .note {
  font-size: 12px;
  color: #666;
  display: block;
  margin-top: 2px;
}

.tech-row {
  display: flex;
  gap: 12px;
  padding: 8px 0;
  border-bottom: 1px solid #f2f4f8;
  align-items: baseline;
}
.tech-row:last-child { border-bottom: none; }
.tech-row .k {
  width: 180px;
  font-size: 12px;
  color: #666;
  text-transform: uppercase;
  letter-spacing: 0.4px;
}
.tech-row .v { font-size: 14px; color: #1a1a1a; }
.tech-row .v.yes { color: #1a8c4a; font-weight: 600; }
.tech-row .v.no { color: #c4332b; font-weight: 600; }

.footer {
  padding: 16px 36px;
  background: #fafbfd;
  color: #888;
  font-size: 11px;
  text-align: center;
}

.why-card {
  background: linear-gradient(180deg, #fffbea 0%, #fff8e1 100%);
  border-bottom: 2px solid #e6a700;
}
.why-card .card-label { color: #8a5a00; }
.why-card .card-headline { color: #5c4a00; }
.why-list {
  margin: 4px 0 0;
  padding-left: 0;
  list-style: none;
}
.why-list li {
  padding: 10px 0 10px 28px;
  border-bottom: 1px solid rgba(230, 167, 0, 0.18);
  font-size: 14px;
  color: #2a2a2a;
  line-height: 1.55;
  position: relative;
}
.why-list li:last-child { border-bottom: none; }
.why-list li::before {
  content: "▸";
  position: absolute;
  left: 8px;
  top: 9px;
  color: #e6a700;
  font-weight: 700;
  font-size: 14px;
}
"""

INDEX_CSS = """
* { box-sizing: border-box; }
body {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  color: #1a1a1a;
  background: #eceef2;
  margin: 0;
  padding: 32px 16px;
  line-height: 1.55;
}
.page {
  max-width: 1120px;
  margin: 0 auto;
}
.page-header {
  background: white;
  border-radius: 14px;
  box-shadow: 0 4px 24px rgba(0,0,0,0.08);
  padding: 32px 40px;
  margin-bottom: 24px;
}
.page-header h1 {
  font-size: 30px;
  font-weight: 700;
  letter-spacing: -0.5px;
  margin: 0 0 6px;
  color: #0a0a1a;
}
.page-header .sub {
  font-size: 14px;
  color: #666;
  margin: 0 0 20px;
}
.summary-stats {
  display: flex;
  gap: 32px;
  flex-wrap: wrap;
  padding-top: 20px;
  border-top: 1px solid #eef0f3;
}
.summary-stats .ss {
  flex: 0 0 auto;
}
.summary-stats .ss .val {
  font-size: 28px;
  font-weight: 700;
  color: #0a0a1a;
  display: block;
  font-variant-numeric: tabular-nums;
  line-height: 1;
}
.summary-stats .ss .lbl {
  font-size: 11px;
  color: #888;
  text-transform: uppercase;
  letter-spacing: 0.6px;
  display: block;
  margin-top: 6px;
}

.legend {
  background: white;
  border-radius: 14px;
  box-shadow: 0 2px 14px rgba(0,0,0,0.05);
  padding: 22px 28px;
  margin-bottom: 24px;
}
.legend h2 {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 1.4px;
  color: #888;
  font-weight: 700;
  margin: 0 0 14px;
}
.legend-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 10px 28px;
}
.legend-item {
  display: flex;
  align-items: flex-start;
  gap: 10px;
  font-size: 12px;
  color: #333;
  line-height: 1.5;
  padding: 4px 0;
}
.legend-item .chip {
  flex: 0 0 auto;
  margin-top: 1px;
}
.legend-item .desc strong { color: #1a1a1a; }
.legend-sep {
  border-top: 1px solid #eef0f3;
  margin: 16px 0 14px;
}
.legend-scoring {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px 28px;
  font-size: 12px;
  color: #444;
}
.legend-scoring .line { display: flex; gap: 10px; align-items: baseline; }
.legend-scoring .dim {
  font-weight: 700;
  color: #1a1a1a;
  min-width: 115px;
}
.legend-scoring .cap {
  font-size: 11px;
  color: #888;
  font-variant-numeric: tabular-nums;
}
.legend-tiers {
  display: flex;
  gap: 14px;
  margin-top: 12px;
  align-items: center;
  font-size: 12px;
}
.legend-tiers .tier-badge {
  display: inline-block;
  padding: 4px 10px;
  border-radius: 999px;
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.6px;
  margin-right: 6px;
}
.legend-accent {
  display: flex;
  gap: 16px;
  margin-top: 12px;
  font-size: 12px;
  color: #555;
  flex-wrap: wrap;
}
.legend-accent .swatch {
  display: inline-block;
  width: 28px;
  height: 6px;
  border-radius: 3px;
  margin-right: 6px;
  vertical-align: middle;
}
.sw-pain { background: #c4332b; }
.sw-growth { background: #e6a700; }
.sw-demand { background: #7c4dff; }
.sw-none { background: #d8dce3; }

.lead {
  background: white;
  border-radius: 12px;
  box-shadow: 0 2px 14px rgba(0,0,0,0.06);
  padding: 22px 28px 22px 32px;
  margin-bottom: 14px;
  display: grid;
  grid-template-columns: 56px 1fr auto;
  gap: 20px;
  align-items: start;
  transition: box-shadow 0.15s ease, transform 0.15s ease;
  border-left: 6px solid #d8dce3;
}
.lead:hover {
  box-shadow: 0 6px 24px rgba(0,0,0,0.1);
  transform: translateY(-1px);
}
.lead.accent-pain { border-left-color: #c4332b; }
.lead.accent-growth { border-left-color: #e6a700; }
.lead.accent-demand { border-left-color: #7c4dff; }
.lead.accent-none { border-left-color: #d8dce3; }

.chips {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin: 4px 0 14px;
}
.chip {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 4px 10px;
  border-radius: 999px;
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.6px;
  white-space: nowrap;
  border: 1px solid transparent;
}
.chip .chip-val {
  background: rgba(255,255,255,0.55);
  color: inherit;
  padding: 1px 6px;
  border-radius: 999px;
  font-size: 9px;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
}
.chip-pain    { background: #fde8e8; color: #8f1a1a; border-color: #f5c2c2; }
.chip-growth  { background: #fff4d6; color: #7a4d00; border-color: #f0d896; }
.chip-demand  { background: #efe8ff; color: #4a2a9a; border-color: #d8c8ff; }
.chip-nofsm   { background: #eceff4; color: #3d4656; border-color: #d0d6e0; }
.chip-hiring  { background: #4a0808; color: #ffdcdc; border-color: #4a0808; }
.chip-hiring .chip-val { color: #ffdcdc; background: rgba(255,255,255,0.15); }
.chip-warn    { background: #fff9d6; color: #7a5c00; border-color: #f0d896; }
.lead-rank {
  font-size: 32px;
  font-weight: 800;
  color: #c9cdd5;
  letter-spacing: -1px;
  line-height: 1;
  font-variant-numeric: tabular-nums;
}
.lead-body h3 {
  margin: 0 0 4px;
  font-size: 20px;
  font-weight: 700;
  letter-spacing: -0.2px;
}
.lead-body h3 a {
  color: #0a0a1a;
  text-decoration: none;
}
.lead-body h3 a:hover { color: #0066cc; }
.lead-meta {
  font-size: 12px;
  color: #888;
  margin: 0 0 12px;
}
.lead-bullets {
  list-style: none;
  margin: 0;
  padding: 0;
}
.lead-bullets li {
  padding: 5px 0 5px 20px;
  font-size: 13px;
  color: #333;
  line-height: 1.5;
  position: relative;
}
.lead-bullets li::before {
  content: "▸";
  position: absolute;
  left: 4px;
  top: 5px;
  color: #e6a700;
  font-weight: 700;
}
.lead-action {
  text-align: right;
  min-width: 130px;
}
.lead-action .tier-badge {
  display: inline-block;
  padding: 5px 12px;
  border-radius: 999px;
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.6px;
  white-space: nowrap;
  margin-bottom: 10px;
}
.tier-high { background: #fde8e8; color: #9a1a1a; }
.tier-strong { background: #fff4d6; color: #8a5a00; }
.tier-emerging { background: #e6f2ff; color: #003b7a; }
.lead-action .view-link {
  display: inline-block;
  padding: 6px 12px;
  background: #0066cc;
  color: white;
  text-decoration: none;
  border-radius: 5px;
  font-size: 12px;
  font-weight: 600;
}
.lead-action .view-link:hover { background: #0052a3; }
.page-footer {
  text-align: center;
  color: #888;
  font-size: 11px;
  padding: 20px 0 8px;
}
"""


# ---------- card renderers ----------

def build_why_bullets(row: pd.Series, contact: pd.Series, reviews: list[dict], jobs: list[dict]) -> list[str]:
    """
    Plain-English bullet points, one per signal that actually fired.
    Every bullet carries specific numbers and dates where available.
    Bullets mirror the cards rendered below.
    """
    bullets: list[str] = []

    # 1. Tech stack gap
    has_booking = row.get("has_any_booking_tool")
    phone_only = row.get("phone_only")
    if not has_booking:
        stack = s(row.get("webanalyze_site_builder")) or s(row.get("webanalyze_cms"))
        stack_suffix = f" ({stack} site)" if stack else ""
        bullets.append(
            f"No field service platform detected. Bookings likely go through phone "
            f"or web form{stack_suffix}."
        )

    # 2. Hiring intelligence
    if jobs:
        buyer_count = sum(1 for j in jobs if fsm_buyer_role(j["title"]))
        dated_jobs = [j for j in jobs if j.get("date_iso") and j.get("date_source") in ("apollo", "serpapi_relative")]
        dated_jobs.sort(key=lambda j: j["date_iso"], reverse=True)

        if buyer_count > 0:
            buyer_titles = [j["title"] for j in jobs if fsm_buyer_role(j["title"])][:3]
            title_list = ", ".join(buyer_titles)
            most_recent = dated_jobs[0]["date_iso"] if dated_jobs else ""
            date_bit = f", most recent posted {most_recent}" if most_recent else ""
            bullets.append(
                f"{len(jobs)} open job posting{'s' if len(jobs) != 1 else ''} including {buyer_count} ops role{'s' if buyer_count != 1 else ''} "
                f"FSM software is designed to support ({title_list}){date_bit}."
            )
        elif dated_jobs:
            most_recent = dated_jobs[0]["date_iso"]
            oldest = dated_jobs[-1]["date_iso"]
            bullets.append(
                f"{len(jobs)} open job posting{'s' if len(jobs) != 1 else ''} (most recent {most_recent}, oldest {oldest}). Actively growing headcount."
            )
        else:
            bullets.append(f"{len(jobs)} open job posting{'s' if len(jobs) != 1 else ''}.")

    # 3. Customers who switched from competitor — authoritative count
    # comes from the LLM mention array, which is what the switchers card
    # also reads. Bullet and card will always agree.
    place_id = s(row.get("place_id"))
    parsed, indexed_reviews = load_llm_analysis(place_id)
    switcher_mentions = parsed.get("switcher_mentions") or []
    resolved_switchers = []
    for m in switcher_mentions:
        date, _, _ = resolve_mention(m, indexed_reviews)
        if date:
            resolved_switchers.append(date)
    if resolved_switchers:
        n = len(resolved_switchers)
        most_recent = max(resolved_switchers)
        plural = "s" if n > 1 else ""
        bullets.append(
            f"{n} review{plural} mention customers switching from a "
            f"competitor to this shop (most recent: {most_recent})."
        )

    # 4. One-person operation — same threshold as render_one_person_card:
    # >= 4 name mentions AND >= 20% of cached reviews. Keeps the card and
    # the bullet agreeing.
    owner_full = s(contact.get("primary_owner_name"))
    owner_first = s(contact.get("primary_owner_first_name"))
    if owner_first and reviews:
        matches = count_name_mentions(reviews, owner_first)
        n = len(matches)
        total = len(reviews)
        if n >= 4 and total > 0 and (n / total) >= 0.20:
            pct_str = f"{int(n / total * 100)}%"
            bullets.append(
                f"{owner_full or owner_first} is named by first name in "
                f"{n} of {total} cached reviews ({pct_str}). Customers "
                f"associate this shop with a specific person."
            )

    # 5. Dispatch pattern — classification is computed from the
    # recency-filtered stats, not the stale CSV category. If the fresh
    # data disagrees with the old label, the fresh data wins.
    _, disp_stats = resolve_recent_dispatch(place_id)
    if disp_stats.get("count", 0) >= 2:
        pattern = classify_dispatch_pattern(disp_stats)
        median_h = disp_stats["median_hours"]
        max_h = disp_stats["max_hours"]
        same_day_pct = disp_stats["same_day_pct"]
        if pattern == "fast":
            bullets.append(
                f"Fast dispatch in recent reviews: "
                f"{int(same_day_pct*100)}% same-day, {median_h:.0f}h median. "
                f"Responsiveness is the competitive edge here."
            )
        elif pattern == "fast_outlier":
            bullets.append(
                f"Mostly fast dispatch ({int(same_day_pct*100)}% same-day, "
                f"{median_h:.0f}h median) with one slow outlier in the "
                f"6-month window."
            )
        elif pattern == "bimodal":
            bullets.append(
                f"Two-speed dispatch: {int(same_day_pct*100)}% same-day, "
                f"but slowest job took {max_h:.0f} hours. Worth asking how "
                f"they triage."
            )
        elif pattern == "strained":
            bullets.append(
                f"Dispatch is strained: median {median_h:.0f} hours, "
                f"slowest job {max_h:.0f} hours. Customers are waiting."
            )
        elif pattern == "slow":
            bullets.append(
                f"Dispatch is slow: median {median_h:.0f} hours to respond."
            )

    # 6. Pain score / evidence — subtype list derived from the actual
    # pain_mentions array so the bullet never claims subtypes we don't
    # have evidence for.
    pain_score = i(row.get("llm_pain_score"))
    if pain_score >= 4:
        parsed_llm = parsed  # reuse from bullet 3's load above
        pain_mentions_list = parsed_llm.get("pain_mentions") or []
        subtypes = sorted({
            (m.get("subtype") or "other").replace("_", " ")
            for m in pain_mentions_list
            if m.get("subtype") and m.get("subtype") != "other"
        })
        if subtypes:
            subtype_phrase = ", ".join(subtypes)
            bullets.append(
                f"LLM review analysis scores customer pain at "
                f"{pain_score}/10. Specific complaints around {subtype_phrase}."
            )
        else:
            bullets.append(
                f"LLM review analysis scores customer pain at {pain_score}/10."
            )

    # 7. Review velocity
    vel_ratio = f(row.get("velocity_ratio"))
    vel_cat = s(row.get("velocity_category"))
    recent_90 = i(row.get("recent_90d_reviews"))
    prior_90 = i(row.get("prior_90d_reviews"))
    if vel_ratio >= 2.5 and recent_90 >= 5:
        bullets.append(
            f"Review volume surged: {recent_90} reviews in the last 90 days "
            f"versus {prior_90} in the prior 90 ({vel_ratio:.0f}x)."
        )
    elif vel_cat in ("accelerating", "hot_new") and recent_90 >= 5:
        bullets.append(
            f"Review volume accelerating: {recent_90} reviews in last 90 days versus {prior_90} prior."
        )

    # 8. Review burst — crisis bursts only, and only when the pain card
    # is NOT already absorbing this crisis into its own badge. If pain
    # mentions fall inside the burst window, the crisis is already
    # surfaced at the top of the pain card and a separate bullet would
    # duplicate the signal.
    burst_cat = s(row.get("burst_category"))
    if burst_cat in ("active_crisis", "recent_crisis"):
        burst_date = s(row.get("burst_negative_sample_date"))
        # Check if the pain card is absorbing this
        grouped_pain = _group_mentions_by_review(
            parsed.get("pain_mentions") or [],
            indexed_reviews,
            PAIN_SUBTYPE_LABELS,
            "Complaint",
        )
        absorbed = _pain_mentions_overlap_burst(grouped_pain, burst_date)
        if not absorbed:
            if burst_cat == "active_crisis":
                bullets.append(
                    f"CRISIS burst in last 30 days ({burst_date}). Negative review cluster at 3x+ normal volume — something triggered a wave of complaints recently."
                )
            else:
                bullets.append(
                    f"Recent crisis burst ({burst_date}). Negative review cluster at 3x+ normal volume within the last 60 days."
                )

    return bullets


def render_why_card(row: pd.Series, contact: pd.Series, reviews: list[dict], jobs: list[dict]) -> str:
    bullets = build_why_bullets(row, contact, reviews, jobs)
    if not bullets:
        return ""
    items = "".join(f'<li>{esc(b)}</li>' for b in bullets)
    return f"""
    <div class="card why-card">
      <p class="card-label">Why this is a good lead</p>
      <h3 class="card-headline">{len(bullets)} signal{'s' if len(bullets) != 1 else ''} fired</h3>
      <ul class="why-list">{items}</ul>
    </div>
    """


def compute_signal_freshness(place_id: str) -> tuple[float, int, int]:
    """
    Read the LLM review analysis cache and compute how concentrated
    the pain, momentum, and switcher mentions are in the last 30 days
    versus the full 180-day window.

    Returns (freshness_ratio, recent_30d_count, total_signals).
    - freshness_ratio = recent_30d_count / max(total_signals, 1)
    - Returns (0.0, 0, 0) when there are no dated signals to measure.

    Used by the header to render a small "FRESH" or "RECENT" badge so
    a rep can tell at a glance whether the pain is happening *right now*
    or spread evenly across the six-month window.
    """
    parsed, indexed_reviews = load_llm_analysis(place_id)
    if not parsed or not indexed_reviews:
        return 0.0, 0, 0

    # Build a review_index -> date lookup
    idx_to_date: dict[int, str] = {}
    for r in indexed_reviews:
        ri = r.get("review_index")
        d = r.get("date") or ""
        if isinstance(ri, int) and d:
            idx_to_date[ri] = d

    from datetime import date as _date
    today = datetime.now(timezone.utc).date()
    cutoff_30 = today - timedelta(days=30)

    signal_mentions = (
        (parsed.get("pain_mentions") or [])
        + (parsed.get("momentum_mentions") or [])
        + (parsed.get("switcher_mentions") or [])
    )
    total = 0
    recent = 0
    for m in signal_mentions:
        ri = m.get("review_index")
        if not isinstance(ri, int):
            continue
        d_str = idx_to_date.get(ri)
        if not d_str:
            continue
        try:
            d = _date.fromisoformat(d_str)
        except (ValueError, TypeError):
            continue
        total += 1
        if d >= cutoff_30:
            recent += 1
    ratio = (recent / total) if total > 0 else 0.0
    return ratio, recent, total


def render_header(row: pd.Series, jobs: list[dict] | None = None) -> str:
    biz = s(row.get("business_name"))
    city = s(row.get("city"))
    state = s(row.get("state"))
    years = f(row.get("license_years"))
    review_count = i(row.get("place_review_count"))
    rating = f(row.get("place_rating"))
    website = s(row.get("place_website"))
    place_id = s(row.get("place_id"))
    score = f(row.get("score_total"))
    tier_label, tier_class = intent_tier(score)

    # V2 display fields
    size_tier = s(row.get("size_tier"))
    freshness_ratio, recent_30d, total_signals = compute_signal_freshness(place_id)
    cls = s(row.get("class"))
    license_scope_label = ""
    if cls == "CR-39":
        license_scope_label = "Dual-scope HVAC license (commercial + residential)"
    elif cls in {"R-39", "R-39R"}:
        license_scope_label = "Residential-only HVAC license"

    gbp_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id else ""

    meta_bits = []
    if city:
        meta_bits.append(f"{esc(city)}, {esc(state)}")
    if years:
        meta_bits.append(f"{years:.0f} years licensed in AZ")
    if license_scope_label:
        meta_bits.append(license_scope_label)
    if review_count:
        meta_bits.append(f"{review_count} Google reviews · {rating:.1f}★")

    link_bits = []
    if website:
        link_bits.append(f'<a href="{esc(website)}" target="_blank" rel="noopener">Company website ↗</a>')
    if gbp_url:
        link_bits.append(f'<a href="{esc(gbp_url)}" target="_blank" rel="noopener">Google Business Profile ↗</a>')

    chips_html = ""
    if jobs is not None:
        chips = build_signal_chips(row, jobs)
        chips_html = _render_chips(chips)

    accent_cls = dominant_signal_color(row)

    # Right-side badge stack: intent tier, then optional size_tier, then
    # optional freshness badge. Freshness only fires when at least 25% of
    # the contractor's dated signals are in the last 30 days — below that,
    # it's silent (no stale marker; we're already inside 180 days).
    badges_html = [f'<span class="tier-badge {tier_class}">{tier_label}</span>']
    if size_tier:
        size_label = {"XL": "XL · Large", "L": "L · Established",
                      "M": "M · Mid", "S": "S · Small"}.get(size_tier, size_tier)
        badges_html.append(
            f'<span class="size-badge size-{size_tier.lower()}">{esc(size_label)}</span>'
        )
    if total_signals >= 3 and freshness_ratio >= 0.5:
        badges_html.append(
            f'<span class="fresh-badge fresh-hot" title="{recent_30d} of {total_signals} '
            f'dated signals are in the last 30 days">FRESH · {int(freshness_ratio*100)}% last 30d</span>'
        )
    elif total_signals >= 3 and freshness_ratio >= 0.25:
        badges_html.append(
            f'<span class="fresh-badge fresh-warm" title="{recent_30d} of {total_signals} '
            f'dated signals are in the last 30 days">RECENT · {int(freshness_ratio*100)}% last 30d</span>'
        )

    return f"""
    <div class="header {accent_cls}">
      <div class="header-top">
        <div>
          <h1 class="biz-name">{esc(biz)}</h1>
          <p class="biz-meta">{" · ".join(meta_bits)}</p>
          <p class="links">{"".join(link_bits)}</p>
          {chips_html}
        </div>
        <div class="badge-stack">{"".join(badges_html)}</div>
      </div>
    </div>
    """


def render_decision_maker(contact: pd.Series) -> str:
    owner = s(contact.get("primary_owner_name"))
    owner_first = s(contact.get("primary_owner_first_name"))
    source = s(contact.get("owner_name_source"))
    phone = s(contact.get("booking_phone"))
    website = s(contact.get("booking_website"))
    biz_email = s(contact.get("business_email"))
    ap_email = s(contact.get("apollo_verified_email"))
    ap_title = s(contact.get("apollo_title"))
    ap_linkedin = s(contact.get("apollo_linkedin"))

    # Validator-approved fields. Read directly from the validator cache
    # so we never render anything the LLM didn't confirm as belonging.
    place_id = s(contact.get("place_id"))
    validated = load_validator_cache(place_id)

    # Other people named in reviews — pulled from the 08b LLM review
    # analysis cache. These are techs/office staff customers mention
    # by name. Dedupe against the owner's first name so Sammy-the-owner
    # doesn't also show up as "also named in reviews."
    llm_parsed, _ = load_llm_analysis(place_id)
    referenced_people = llm_parsed.get("referenced_people") or []
    owner_first_lower = owner_first.lower().strip()
    deduped_people: list[dict] = []
    for p in referenced_people:
        name = str(p.get("name") or "").strip()
        if not name:
            continue
        first_token = name.split()[0].lower()
        if owner_first_lower and first_token == owner_first_lower:
            continue
        deduped_people.append({
            "name": name,
            "mention_count": int(p.get("mention_count") or 0),
            "sample_quote": str(p.get("sample_quote") or "").strip(),
        })
    # Sort by mention_count descending, then by name for stability
    deduped_people.sort(key=lambda p: (-p["mention_count"], p["name"].lower()))

    def _kept(category: str) -> list[str]:
        items = validated.get(category) or []
        return [
            str(it.get("value") or "")
            for it in items
            if it.get("belongs") and it.get("value")
        ]

    discovered_emails_list = _kept("emails")
    alt_phones_list = _kept("phones")  # 10-digit strings
    fb_urls = _kept("facebook_urls")
    li_urls = _kept("linkedin_company_urls")
    ig_urls = _kept("instagram_urls")

    # Normalized main phone for dedup
    main_phone_digits = re.sub(r"\D", "", phone or "")
    if len(main_phone_digits) == 11 and main_phone_digits.startswith("1"):
        main_phone_digits = main_phone_digits[1:]

    if not owner:
        owner_html = '<p class="owner-line">Owner unknown</p>'
        source_html = ""
    else:
        title_bit = f' <span style="font-weight:400;color:#666;">· {esc(ap_title)}</span>' if ap_title else ""
        owner_html = f'<p class="owner-line">{esc(owner)}{title_bit}</p>'
        if source == "az_roc_qualifying_party":
            source_html = '<p class="owner-source">Verified via AZ contractor license (public record)</p>'
        else:
            source_html = ""

    # "Also named in reviews" block — renders below the owner line with
    # one row per extracted person, sorted by mention count. Each row
    # shows the name and a short verbatim context snippet so a rep can
    # ask for anyone on this list by first name during a cold call.
    referenced_html = ""
    if deduped_people:
        rows_html = []
        for p in deduped_people:
            name = esc(p["name"])
            count = p["mention_count"]
            count_bit = f' <span class="rp-count">{count}×</span>' if count > 1 else ""
            quote = p["sample_quote"]
            quote_html = (
                f' <span class="rp-quote">— "{esc(quote)}"</span>' if quote else ""
            )
            rows_html.append(
                f'<li class="rp-row"><strong>{name}</strong>{count_bit}{quote_html}</li>'
            )
        referenced_html = (
            '<div class="referenced-people">'
            '<p class="rp-label">Also named in reviews — ask for any of these by first name:</p>'
            f'<ul class="rp-list">{"".join(rows_html)}</ul>'
            '</div>'
        )

    rows = []
    if phone:
        tel = re.sub(r"[^\d+]", "", phone)
        ask = f" — ask for {esc(owner_first)}" if owner_first else ""
        rows.append((
            "Booking phone",
            f'<a href="tel:{tel}">{esc(phone)}</a>{ask}'
            f'<span class="note">Same number their customers call to schedule.</span>'
        ))
    # Drop the main phone from the alt list (validator keeps it as one of
    # the "belongs" phones, but we render it separately above)
    alt_phones_display = [p for p in alt_phones_list if p != main_phone_digits]
    if alt_phones_display:
        def _format_phone(digits: str) -> str:
            if len(digits) == 10:
                return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
            return digits
        phone_items_html = []
        for digits in alt_phones_display:
            tel = digits
            phone_items_html.append(
                f'<div style="margin-bottom:4px;"><a href="tel:{tel}">{esc(_format_phone(digits))}</a></div>'
            )
        rows.append((
            "Additional phones",
            "".join(phone_items_html)
            + '<span class="note">May include after-hours, service-area, or additional business lines.</span>'
        ))
    if website:
        rows.append((
            "Web form",
            f'<a href="{esc(website)}" target="_blank" rel="noopener">{esc(website)}</a>'
            f'<span class="note">Same channel their customers use to book.</span>'
        ))
    if discovered_emails_list:
        email_items_html = []
        for addr in discovered_emails_list:
            email_items_html.append(
                f'<div style="margin-bottom:4px;"><a href="mailto:{esc(addr)}">{esc(addr)}</a></div>'
            )
        rows.append((
            "Discovered email",
            "".join(email_items_html)
            + '<span class="note">Confirmed as belonging to this business.</span>'
        ))
    if ap_email:
        rows.append((
            "Verified email",
            f'<a href="mailto:{esc(ap_email)}">{esc(ap_email)}</a>'
        ))
    if biz_email:
        rows.append((
            "General inbox",
            f'<a href="mailto:{esc(biz_email)}">{esc(biz_email)}</a>'
            f'<span class="note">Pulled from their website.</span>'
        ))
    # Social URLs — validator-confirmed only
    social_parts = []
    if fb_urls:
        social_parts.append(f'<a href="{esc(fb_urls[0])}" target="_blank" rel="noopener">Facebook ↗</a>')
    if li_urls:
        social_parts.append(f'<a href="{esc(li_urls[0])}" target="_blank" rel="noopener">LinkedIn company ↗</a>')
    elif ap_linkedin:
        social_parts.append(f'<a href="{esc(ap_linkedin)}" target="_blank" rel="noopener">LinkedIn ↗</a>')
    if ig_urls:
        social_parts.append(f'<a href="{esc(ig_urls[0])}" target="_blank" rel="noopener">Instagram ↗</a>')
    if social_parts:
        rows.append(("Social", " · ".join(social_parts)))

    dl_html = "".join(f"<dt>{esc(k)}</dt><dd>{v}</dd>" for k, v in rows)

    return f"""
    <div class="card">
      <p class="card-label">Decision maker</p>
      <h3 class="card-headline">Who to call</h3>
      <div class="contact-card">
        {owner_html}
        {source_html}
        {referenced_html}
        <dl>{dl_html}</dl>
      </div>
    </div>
    """


def _format_job_date(date_iso: str, source: str) -> str:
    if not date_iso:
        return ""
    label = {
        "apollo": "posted",
        "serpapi_relative": "posted",
        "first_observed": "first observed",
    }.get(source, "posted")
    return f"{label} {date_iso}"


def render_hiring_card(row: pd.Series) -> str:
    place_id = s(row.get("place_id"))
    business_name = s(row.get("business_name"))
    jobs, fetched_date = load_jobs(place_id, business_name)
    if not jobs:
        return ""

    buyer_count = sum(1 for j in jobs if fsm_buyer_role(j["title"]))
    tag_html = ""
    if buyer_count > 0:
        tag_html = f'<span class="tag tag-green">{buyer_count} FSM BUYER ROLE{"S" if buyer_count > 1 else ""}</span>'

    # Count date sources to note in subtitle
    apollo_dated = sum(1 for j in jobs if j.get("date_source") == "apollo")
    relative_dated = sum(1 for j in jobs if j.get("date_source") == "serpapi_relative")
    observed_only = sum(1 for j in jobs if j.get("date_source") == "first_observed")

    items = []
    for j in jobs[:12]:
        title = esc(j["title"])
        via = esc(j["via"])
        loc = esc(j["location"])
        link = j["link"]
        apollo_url = j.get("apollo_url") or ""
        is_buyer = fsm_buyer_role(j["title"])
        date_str = _format_job_date(j["date_iso"], j["date_source"])

        flag = '<span class="buyer-flag">FSM BUYER ROLE</span>' if is_buyer else ""
        meta_bits = []
        if date_str:
            meta_bits.append(f"<strong>{esc(date_str)}</strong>")
        if via:
            meta_bits.append(f"via {via}")
        if loc:
            meta_bits.append(loc)
        meta = " · ".join(meta_bits)

        link_parts = []
        if link:
            link_parts.append(f'<a href="{esc(link)}" target="_blank" rel="noopener">apply ↗</a>')
        if apollo_url and apollo_url != link:
            link_parts.append(f'<a href="{esc(apollo_url)}" target="_blank" rel="noopener">linkedin ↗</a>')
        link_bit = (" " + " ".join(link_parts)) if link_parts else ""

        cls = "buyer-role" if is_buyer else ""
        items.append(
            f'<li class="{cls}"><div class="title">{title}{flag}</div>'
            f'<div class="meta">{meta}{link_bit}</div></li>'
        )

    more = ""
    if len(jobs) > 12:
        more = f'<p style="font-size:12px;color:#888;margin:8px 0 0;">+ {len(jobs) - 12} more postings</p>'

    # Subtitle explains date sourcing in plain language
    source_bits = []
    exact_dated = apollo_dated + relative_dated
    if exact_dated:
        source_bits.append(f"{exact_dated} with exact posting date")
    if observed_only:
        source_bits.append(f"{observed_only} dated from first observed on {fetched_date}")
    source_note = " · ".join(source_bits)

    return f"""
    <div class="card">
      <p class="card-label">Hiring intelligence {tag_html}</p>
      <h3 class="card-headline">{len(jobs)} open job posting{'s' if len(jobs) != 1 else ''}</h3>
      <p class="card-subtitle">{source_note}</p>
      <ul class="job-list">{"".join(items)}</ul>
      {more}
    </div>
    """


SWITCH_PHRASE_RE = re.compile(
    r"\b("
    r"switch(?:ed|ing)?\s+from|"
    r"switch(?:ed|ing)?\s+to|"
    r"went\s+through\s+(?:a\s+)?(?:few|several|many)|"
    r"tried\s+(?:a\s+)?(?:few|several|many|other|another|different)|"
    r"(?:used|had)\s+to\s+(?:use|deal\s+with|call)\s+(?:another|a\s+different)|"
    r"previous\s+(?:hvac\s+)?(?:company|contractor|guys?|service)|"
    r"last\s+(?:hvac\s+)?(?:company|contractor|guys?)|"
    r"our\s+(?:old|former|previous)\s+(?:hvac\s+)?(?:company|contractor|guys?|provider)|"
    r"fired\s+(?:our|the)\s+(?:old|previous|former|last)|"
    r"left\s+(?:our|the)\s+(?:old|previous|former|last)|"
    r"after\s+(?:being\s+)?(?:burned|ripped\s+off|disappointed)\s+by|"
    r"instead\s+of\s+(?:the\s+)?(?:other|another)"
    r")",
    re.IGNORECASE,
)


def find_switch_reviews(reviews: list[dict]) -> list[dict]:
    """Return reviews whose snippet contains customer-switch phrasing.
    Mechanical, reproducible, verifiable against the cached review text."""
    if not reviews:
        return []
    out = []
    for r in reviews:
        snippet = r.get("snippet") or ""
        if SWITCH_PHRASE_RE.search(snippet):
            out.append(r)
    return out


def render_switchers_card(row: pd.Series, reviews: list[dict]) -> str:
    """
    Card for the customer-switch signal. Reads directly from the per-review
    LLM analysis cache produced by 08b_review_llm.py. Every switcher mention
    the LLM found is rendered with its dated source review — count and
    quotes can never disagree.
    """
    place_id = s(row.get("place_id"))
    parsed, indexed_reviews = load_llm_analysis(place_id)
    switchers = parsed.get("switcher_mentions") or []
    if not switchers:
        return ""

    # Resolve each mention to its dated review
    resolved = []
    for m in switchers:
        date, rating, quote = resolve_mention(m, indexed_reviews)
        if not date:
            continue
        resolved.append({
            "date": date,
            "rating": rating,
            "quote": quote,
            "prior_company_hint": s(m.get("prior_company_hint")),
        })
    if not resolved:
        return ""

    # Most recent first
    resolved.sort(key=lambda r: r["date"], reverse=True)
    n = len(resolved)

    # LLM already returns sentence-scoped verbatim quotes — no client-side trim
    quotes_html = []
    for r in resolved:
        hint = r["prior_company_hint"]
        extra = f" · prior: {hint}" if hint else ""
        quotes_html.append(f"""
        <blockquote class="evidence positive">
          {esc(r["quote"])}
          <span class="cite">{esc(make_cite(r["date"], r["rating"], "customer switched from another HVAC company" + extra))}</span>
        </blockquote>
        """)

    return f"""
    <div class="card">
      <p class="card-label">Customers who switched from a competitor <span class="tag">{n} mention{'s' if n != 1 else ''}</span></p>
      <h3 class="card-headline">{n} review{'s' if n != 1 else ''} mention switching to this shop from another HVAC company</h3>
      <p class="card-subtitle">Direct evidence their competitors just lost customers to them.</p>
      {"".join(quotes_html)}
    </div>
    """


def render_one_person_card(row: pd.Series, contact: pd.Series, reviews: list[dict]) -> str:
    """
    Only fires when the owner's first name appears in at least 4 reviews
    AND those mentions cover 20%+ of the cached reviews. 2 out of 38 is
    noise, not a one-person shop.

    Evidence is limited to positive reviews (>= 4-star) so we never pair
    a founder-praise headline with a 1-star complaint that happens to
    name the owner.
    """
    owner_first = s(contact.get("primary_owner_first_name"))
    if not owner_first or not reviews:
        return ""

    matches = count_name_mentions(reviews, owner_first)
    mention_count = len(matches)
    total_reviews = len(reviews)
    if total_reviews == 0:
        return ""
    pct = mention_count / total_reviews

    # Threshold: at least 4 absolute mentions AND 20% of the cached set
    if mention_count < 4 or pct < 0.20:
        return ""

    # Positive reviews only (>= 4 stars). A 1-star review naming the owner
    # is almost always a complaint, not praise.
    def _rating(r):
        try:
            return float(r.get("rating")) if r.get("rating") is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    positive_matches = [r for r in matches if _rating(r) >= 4.0]
    if not positive_matches:
        return ""

    # Show up to 3 most recent positive mentions
    positive_sorted = sorted(positive_matches, key=lambda r: r.get("date", ""), reverse=True)
    snippets_html = ""
    for r in positive_sorted[:3]:
        snippet = r.get("snippet") or ""
        date = r.get("date", "")
        rating_val = _rating(r)
        rating_str = f"{rating_val:.0f}★" if rating_val else ""
        trimmed = trim_to_relevant(snippet, [owner_first], max_chars=220)
        snippets_html += f"""
        <blockquote class="evidence">
          {esc(trimmed)}
          <span class="cite">{esc(make_cite(date, rating_str))}</span>
        </blockquote>
        """

    pct_str = f"{int(pct * 100)}%"

    return f"""
    <div class="card">
      <p class="card-label">Customer attention on one person</p>
      <h3 class="card-headline">{esc(owner_first)} is named by first name in {mention_count} of {total_reviews} cached reviews ({pct_str})</h3>
      <p class="card-subtitle">Customers mention a specific person often enough that the brand and the individual are tied together in public reviews. Whether that reflects a true one-person shop or a named figurehead on a bigger team is worth confirming on the call.</p>
      {snippets_html}
    </div>
    """


DISPATCH_RECENCY_CUTOFF_DAYS = 180


def _date_within_window(date_str: str, cutoff_days: int) -> bool:
    """True when date_str (YYYY-MM-DD) is within cutoff_days of today."""
    if not date_str:
        return False
    try:
        dt = datetime.fromisoformat(date_str)
    except (ValueError, TypeError):
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    cutoff = datetime.now(timezone.utc) - timedelta(days=cutoff_days)
    return dt >= cutoff


def classify_dispatch_pattern(stats: dict) -> str:
    """
    Classify a dispatch pattern from fresh recency-filtered stats.
    Returns one of:
      - 'fast'         : median <= 24h AND max <= 72h
      - 'fast_outlier' : median <= 24h, majority same-day, one slow outlier
      - 'bimodal'      : genuine split — >= 25% same-day AND >= 25% week+
      - 'strained'     : median > 48h OR max > 168h, NOT bimodal
      - 'slow'         : median > 24h with no substantial fast population
      - 'low_data'     : fewer than 2 extractions

    The 25% requirement on both populations prevents a single outlier
    from getting labeled "bimodal dispatch" when the real pattern is
    "fast dispatch with one bad job."
    """
    count = stats.get("count", 0)
    if count < 2:
        return "low_data"
    median = stats.get("median_hours", 0.0) or 0.0
    max_h = stats.get("max_hours", 0.0) or 0.0
    same_day_pct = stats.get("same_day_pct", 0.0) or 0.0
    week_plus_pct = stats.get("week_plus_pct", 0.0) or 0.0

    # Clean fast: median and max both low
    if median <= 24 and max_h <= 72:
        return "fast"

    # Genuine bimodal: substantial fast population AND substantial week+ population
    if same_day_pct >= 0.25 and week_plus_pct >= 0.25:
        return "bimodal"

    # Mostly fast with one slow outlier: majority same-day, median still low
    if same_day_pct >= 0.6 and median <= 24:
        return "fast_outlier"

    # Strained: long median or very slow tail
    if median > 48 or max_h > 168:
        return "strained"
    return "slow"


def resolve_recent_dispatch(place_id: str) -> tuple[list[dict], dict]:
    """
    Return (resolved_extractions, stats) for dispatch extractions whose
    source review is within DISPATCH_RECENCY_CUTOFF_DAYS. Stats is a dict
    with median_hours, max_hours, same_day_pct, count. Both the dispatch
    card and the "why this is a good lead" bullet use this so their
    numbers always agree.
    """
    extractions = load_dispatch_extractions(place_id)
    if not extractions:
        return [], {"count": 0}
    reviews = load_reviews(place_id)
    resolved = []
    for e in extractions:
        quote = s(e.get("verbatim_quote"))
        if not quote:
            continue
        date, rating_str, _ = find_review_date(quote, reviews)
        if not date or not _date_within_window(date, DISPATCH_RECENCY_CUTOFF_DAYS):
            continue
        resolved.append({
            "date": date,
            "rating_str": rating_str,
            "rating_raw": e.get("rating"),
            "estimated_delay_hours": e.get("estimated_delay_hours"),
            "sentiment": e.get("sentiment", ""),
            "quote": quote,
        })
    resolved.sort(key=lambda r: r["date"], reverse=True)

    delay_hours = [float(r["estimated_delay_hours"]) for r in resolved if r["estimated_delay_hours"] is not None]
    if delay_hours:
        import statistics as _stats
        stats = {
            "count": len(resolved),
            "median_hours": _stats.median(delay_hours),
            "max_hours": max(delay_hours),
            "same_day_pct": sum(1 for h in delay_hours if h <= 24) / len(delay_hours),
            # >= 168h = a week or more; used to detect genuine bimodal splits
            "week_plus_pct": sum(1 for h in delay_hours if h >= 168) / len(delay_hours),
        }
    else:
        stats = {
            "count": len(resolved),
            "median_hours": 0.0,
            "max_hours": 0.0,
            "same_day_pct": 0.0,
            "week_plus_pct": 0.0,
        }
    return resolved, stats


def render_dispatch_card(row: pd.Series) -> str:
    """
    Dispatch distribution card. Filters extractions to the last
    DISPATCH_RECENCY_CUTOFF_DAYS (default 180) so we never show dispatch
    evidence older than the LLM review analysis window.
    """
    place_id = s(row.get("place_id"))
    resolved, disp_stats = resolve_recent_dispatch(place_id)
    if not resolved:
        return ""

    median = disp_stats.get("median_hours", 0.0)
    max_h = disp_stats.get("max_hours", 0.0)
    same_day = disp_stats.get("same_day_pct", 0.0)

    # Chart uses the same structure as the dispatch_delay cache entries
    chart_input = [{
        "estimated_delay_hours": r["estimated_delay_hours"],
        "sentiment": r["sentiment"],
    } for r in resolved]
    chart = svg_dispatch_dotplot(chart_input)

    stats_html = f"""
    <div class="card-stats">
      <div class="stat"><span class="val">{median:.0f}h</span><span class="lbl">Median response</span></div>
      <div class="stat"><span class="val">{max_h:.0f}h</span><span class="lbl">Slowest job</span></div>
      <div class="stat"><span class="val">{int(same_day*100)}%</span><span class="lbl">Same-day</span></div>
      <div class="stat"><span class="val">{len(resolved)}</span><span class="lbl">Reviews with data</span></div>
    </div>
    """

    detail_rows = []
    for r in resolved:
        delay_h = r["estimated_delay_hours"]
        delay_str = f"{delay_h:.0f}h" if isinstance(delay_h, (int, float)) else "?"
        sentiment = r["sentiment"]
        rating_bit = r["rating_str"] or (f"{r['rating_raw']}★" if r["rating_raw"] is not None else "")
        sent_color = {
            "positive": "#1a8c4a",
            "negative": "#c4332b",
        }.get(sentiment, "#666")
        cls = {"positive": "positive", "negative": "negative"}.get(sentiment, "")
        cite_bits = [f"{delay_str} response"]
        if rating_bit:
            cite_bits.append(rating_bit)
        if sentiment:
            cite_bits.append(sentiment)
        cite_bits.append(r["date"])
        detail_rows.append(f"""
        <blockquote class="evidence {cls}">
          {esc(r["quote"])}
          <span class="cite" style="color:{sent_color};">
            {esc(' · '.join(cite_bits))}
          </span>
        </blockquote>
        """)

    # Classify the pattern from the fresh recency-filtered stats — never
    # the stale dispatch_category column in the CSV. If a contractor was
    # labeled dispatch_fast based on all-time data but now has only 1
    # slow review in the 180-day window, we say "slow" and mean it.
    pattern = classify_dispatch_pattern(disp_stats)
    if pattern == "fast":
        headline = "Fast dispatch on every job we could measure"
        interpretation = (
            "Fast dispatch is not a pain signal — it's what this contractor "
            "is getting right. Responsiveness like this typically caps out "
            "when volume exceeds whoever is running the schedule, which is "
            "where FSM software earns its keep."
        )
    elif pattern == "fast_outlier":
        headline = "Mostly fast dispatch with one slow outlier"
        interpretation = (
            "Most jobs in the 6-month window ran same-day. One job went "
            "long — could be parts, warranty, or complexity; the reviews "
            "don't tell us which."
        )
    elif pattern == "bimodal":
        headline = "Two-speed dispatch — substantial fast AND substantial slow"
        interpretation = (
            "At least a quarter of measured jobs went same-day and at least "
            "a quarter stretched to a week or more. We can't tell from the "
            "reviews alone whether the split is by job type, parts "
            "availability, or scheduling — worth asking how they triage."
        )
    elif pattern == "strained":
        headline = "Dispatch is strained — customers are waiting"
        interpretation = (
            "Median response times are long and the slowest jobs stretch "
            "out. Customer reviews describe jobs left pending, callbacks "
            "that never come, or rescheduling."
        )
    elif pattern == "slow":
        headline = "Dispatch response is slow"
        interpretation = ""
    else:
        headline = "Dispatch performance"
        interpretation = ""

    interp_html = f'<p class="card-body" style="margin:8px 0 12px;">{esc(interpretation)}</p>' if interpretation else ""

    return f"""
    <div class="card">
      <p class="card-label">Dispatch distribution</p>
      <h3 class="card-headline">{esc(headline)}</h3>
      {interp_html}
      <p class="card-subtitle">Every dot is a real cached review from the last 6 months. Hours estimated from review text.</p>
      {stats_html}
      <div style="margin:16px 0;">{chart}</div>
      <div style="margin-top:18px;">
        <p class="card-subtitle" style="margin-bottom:8px;">Verbatim dispatch quotes:</p>
        {"".join(detail_rows)}
      </div>
    </div>
    """


PAIN_SUBTYPE_LABELS = {
    "dispatch": "Dispatch complaint",
    "communication": "Communication complaint",
    "capacity": "Capacity complaint",
    "quality": "Quality complaint",
    "billing": "Billing complaint",
    "other": "Complaint",
}

MOMENTUM_SUBTYPE_LABELS = {
    "demand_pressure": "Demand pressure",
    "founder_owned": "Owner involved",
    "key_person": "Key person dependency",
    "long_wait": "Long wait",
    "capacity_strain": "Capacity strain",
    "other": "Momentum",
}


def _group_mentions_by_review(
    mentions: list[dict],
    indexed_reviews: list[dict],
    subtype_labels: dict[str, str],
    default_label: str,
) -> list[dict]:
    """
    Group LLM mentions by their source review_index. Returns one dict per
    unique review, in date-descending order:
      {
        "date": str, "rating": str, "observations": [
          {"quote": str, "label": str}, ...
        ]
      }
    Mentions whose review_index doesn't resolve are dropped.
    """
    by_review: dict[int, dict] = {}
    for m in mentions:
        idx = m.get("review_index")
        if not isinstance(idx, int) or idx < 1 or idx > len(indexed_reviews):
            continue
        r = indexed_reviews[idx - 1]
        date = r.get("date") or ""
        if not date:
            continue
        quote = (m.get("quote") or "").strip()
        if not quote:
            continue
        subtype = (m.get("subtype") or "other")
        label = subtype_labels.get(subtype, default_label)
        if idx not in by_review:
            rating = r.get("rating")
            rating_str = f"{float(rating):.0f}★" if rating is not None else ""
            by_review[idx] = {
                "review_index": idx,
                "date": date,
                "rating": rating_str,
                "observations": [],
            }
        by_review[idx]["observations"].append({
            "quote": quote,
            "label": label,
        })
    grouped = sorted(by_review.values(), key=lambda g: g["date"], reverse=True)
    return grouped


def _split_pain_by_burst_window(
    grouped_pain: list[dict],
    burst_start_iso: str | None,
) -> tuple[list[dict], list[dict]]:
    """
    Split grouped pain-mention reviews into (in_window, outside_window)
    relative to a burst start date. The window is a 12-day band around
    the start ([-1, +10]). Used for two decisions:

      - If any pain mentions fall inside the window, the pain card
        absorbs the crisis (badge + subtitle) and the standalone burst
        card is suppressed.
      - The pain card then renders in-window mentions in a "crisis
        burst" subsection and outside-window mentions in an "ongoing
        pain" subsection. If all pain mentions are in the window, the
        split collapses to a single section.
    """
    if not grouped_pain or not burst_start_iso:
        return [], list(grouped_pain)
    from datetime import date as _date
    try:
        start = _date.fromisoformat(burst_start_iso)
    except (ValueError, TypeError):
        return [], list(grouped_pain)
    in_window: list[dict] = []
    outside: list[dict] = []
    for g in grouped_pain:
        try:
            d = _date.fromisoformat(g["date"])
        except (ValueError, TypeError):
            outside.append(g)
            continue
        delta = (d - start).days
        if -1 <= delta <= 10:
            in_window.append(g)
        else:
            outside.append(g)
    return in_window, outside


def _pain_mentions_overlap_burst(
    grouped_pain: list[dict],
    burst_start_iso: str | None,
) -> bool:
    """
    True when at least one rendered pain review falls inside the burst
    window. Previous version required ALL reviews to be inside — too
    strict for the partial-overlap case. With the new semantics, the
    pain card will always absorb the burst framing whenever there is
    any overlap, and render in-window vs outside-window sections.
    """
    in_window, _ = _split_pain_by_burst_window(grouped_pain, burst_start_iso)
    return bool(in_window)


def render_pain_card(row: pd.Series, reviews: list[dict]) -> str:
    """
    Render verified pain mentions from the LLM analysis cache, grouped
    by source review. Headline counts distinct reviews — if one review
    contains 2 observations, that's 1 customer with 2 observations, not
    2 complaints.

    When a recent-crisis or active-crisis burst overlaps with these
    exact same reviews, we absorb the burst framing into this card as
    a badge + subtitle instead of rendering a separate burst card.
    """
    place_id = s(row.get("place_id"))
    parsed, indexed_reviews = load_llm_analysis(place_id)
    mentions = parsed.get("pain_mentions") or []
    if not mentions:
        return ""

    pain_score = i(row.get("llm_pain_score"))

    grouped = _group_mentions_by_review(
        mentions, indexed_reviews, PAIN_SUBTYPE_LABELS, "Complaint"
    )
    if not grouped:
        return ""

    # Detect whether a crisis burst overlaps any of these pain reviews.
    # With partial overlap we split into crisis-window + ongoing sections.
    burst_cat = s(row.get("burst_category"))
    burst_start = s(row.get("burst_negative_sample_date"))
    burst_baseline = f(row.get("burst_baseline_per_week"))
    is_crisis = burst_cat in ("active_crisis", "recent_crisis")
    in_window_grouped: list[dict] = []
    outside_grouped: list[dict] = list(grouped)
    if is_crisis:
        in_window_grouped, outside_grouped = _split_pain_by_burst_window(
            grouped, burst_start
        )

    absorb_burst = is_crisis and bool(in_window_grouped)

    crisis_badge = ""
    crisis_subtitle = ""
    if absorb_burst:
        crisis_label = {
            "active_crisis": "CRISIS BURST (last 30 days)",
            "recent_crisis": "CRISIS BURST (last 60 days)",
        }[burst_cat]
        crisis_badge = (
            f' <span class="tag" style="background:#4a0808;color:#ffdcdc;'
            f'border-color:#4a0808;">{esc(crisis_label)}</span>'
        )
        crisis_subtitle = (
            f'<p class="card-subtitle" style="color:#8f1a1a;font-weight:600;">'
            f'A review cluster in the last 60 days ran 3x+ this '
            f"contractor's {burst_baseline:.1f}/week baseline — complaints "
            f"marked below with the crisis badge are part of that burst.</p>"
        )

    def _render_group_block(g: dict) -> str:
        obs_html = []
        for o in g["observations"]:
            obs_html.append(
                f'<div class="obs">{esc(o["quote"])}'
                f'<span class="obs-label">{esc(o["label"])}</span></div>'
            )
        cite = make_cite(g["date"], g["rating"])
        obs_count = len(g["observations"])
        multi_note = (
            f' <span class="obs-count">{obs_count} observations</span>'
            if obs_count > 1 else ""
        )
        return f"""
        <blockquote class="evidence negative grouped">
          <div class="review-cite">{esc(cite)}{multi_note}</div>
          {"".join(obs_html)}
        </blockquote>
        """

    # Build the body. If we're absorbing a crisis AND there are reviews
    # outside the burst window, render two sections. Otherwise render a
    # single unified list.
    body_parts: list[str] = []
    if absorb_burst and outside_grouped:
        body_parts.append(
            '<p class="card-label" style="margin-top:18px;color:#8f1a1a;">'
            f"{len(in_window_grouped)} review{'s' if len(in_window_grouped) != 1 else ''} "
            "in the crisis burst window</p>"
        )
        body_parts.extend(_render_group_block(g) for g in in_window_grouped)
        body_parts.append(
            '<p class="card-label" style="margin-top:18px;">'
            f"{len(outside_grouped)} ongoing pain review{'s' if len(outside_grouped) != 1 else ''} "
            "(outside the burst window)</p>"
        )
        body_parts.extend(_render_group_block(g) for g in outside_grouped)
    else:
        # Single unified list — either no crisis, or all reviews are in
        # the window, or nothing is in the window
        body_parts.extend(_render_group_block(g) for g in grouped)

    n_reviews = len(grouped)
    n_obs = sum(len(g["observations"]) for g in grouped)
    headline_n = f"{n_reviews} recent customer{'s' if n_reviews != 1 else ''} complained"
    obs_note = ""
    if n_obs > n_reviews:
        obs_note = f" ({n_obs} distinct observations across those reviews)"

    return f"""
    <div class="card">
      <p class="card-label">Customer pain evidence <span class="tag">pain score {pain_score}/10</span>{crisis_badge}</p>
      <h3 class="card-headline">{esc(headline_n)}{esc(obs_note)}</h3>
      {crisis_subtitle}
      <p class="card-subtitle">Internal rep reference — do NOT quote back to the prospect.</p>
      {"".join(body_parts)}
    </div>
    """


def render_momentum_card(row: pd.Series, reviews: list[dict]) -> str:
    """
    Render verified momentum mentions grouped by source review.
    Headline counts distinct reviews.
    """
    place_id = s(row.get("place_id"))
    parsed, indexed_reviews = load_llm_analysis(place_id)
    mentions = parsed.get("momentum_mentions") or []
    if not mentions:
        return ""

    mom_score = i(row.get("llm_momentum_score"))

    grouped = _group_mentions_by_review(
        mentions, indexed_reviews, MOMENTUM_SUBTYPE_LABELS, "Momentum"
    )
    if not grouped:
        return ""

    blocks = []
    for g in grouped:
        obs_html = []
        for o in g["observations"]:
            # LLM already returns sentence-scoped verbatim quotes
            obs_html.append(
                f'<div class="obs">{esc(o["quote"])}'
                f'<span class="obs-label">{esc(o["label"])}</span></div>'
            )
        cite = make_cite(g["date"], g["rating"])
        obs_count = len(g["observations"])
        multi_note = (
            f' <span class="obs-count">{obs_count} observations</span>'
            if obs_count > 1 else ""
        )
        blocks.append(f"""
        <blockquote class="evidence positive grouped">
          <div class="review-cite">{esc(cite)}{multi_note}</div>
          {"".join(obs_html)}
        </blockquote>
        """)

    n_reviews = len(grouped)
    n_obs = sum(len(g["observations"]) for g in grouped)
    obs_note = ""
    if n_obs > n_reviews:
        obs_note = f" ({n_obs} distinct observations across those reviews)"
    verb = "contain" if n_reviews != 1 else "contains"
    headline = (
        f"{n_reviews} recent review{'s' if n_reviews != 1 else ''} "
        f"{verb} growth or demand-pressure language"
    )

    return f"""
    <div class="card">
      <p class="card-label">Growth momentum <span class="tag tag-amber">momentum score {mom_score}/10</span></p>
      <h3 class="card-headline">{esc(headline)}{esc(obs_note)}</h3>
      {"".join(blocks)}
    </div>
    """


def render_velocity_card(row: pd.Series) -> str:
    vel_ratio = f(row.get("velocity_ratio"))
    vel_cat = s(row.get("velocity_category"))
    recent_90 = i(row.get("recent_90d_reviews"))
    prior_90 = i(row.get("prior_90d_reviews"))

    # Fire threshold: velocity_ratio > 2 OR accelerating/hot_new OR recent_90 >= 10
    if vel_ratio < 2 and vel_cat not in ("accelerating", "hot_new") and recent_90 < 10:
        return ""

    place_id = s(row.get("place_id"))
    reviews = load_reviews(place_id)
    monthly = monthly_review_counts(reviews, months_back=12)
    chart = svg_monthly_line(monthly)

    stats = f"""
    <div class="card-stats">
      <div class="stat"><span class="val">{recent_90}</span><span class="lbl">Reviews last 90d</span></div>
      <div class="stat"><span class="val">{prior_90}</span><span class="lbl">Reviews prior 90d</span></div>
      <div class="stat"><span class="val">{vel_ratio:.1f}x</span><span class="lbl">Velocity ratio</span></div>
    </div>
    """

    headline = "Review volume is accelerating"
    if vel_cat == "hot_new":
        headline = "Hot new contractor picking up pace"

    return f"""
    <div class="card">
      <p class="card-label">Review velocity</p>
      <h3 class="card-headline">{esc(headline)}</h3>
      <p class="card-subtitle">Reviews per month over the last 12 months. Each point is the count of cached Google reviews with dates in that month.</p>
      {stats}
      <div style="margin:14px 0;">{chart}</div>
    </div>
    """


def _collect_burst_window_reviews(
    place_id: str,
    burst_start_iso: str,
    window_days: int = 10,
    max_reviews: int = 4,
) -> list[dict]:
    """
    Fallback when pain_mentions don't cover the burst window.
    Reads the raw cached SerpAPI reviews, picks the lowest-rated ones
    that fall inside a [-1, +10] day band around the burst start date,
    and returns them in the same shape as the grouped-evidence blocks
    the burst card expects: {date, rating, observations: [{quote, label}]}.
    """
    from datetime import date as _date
    try:
        start = _date.fromisoformat(burst_start_iso)
    except (ValueError, TypeError):
        return []

    raw = load_reviews(place_id)
    if not raw:
        return []

    in_window = []
    for r in raw:
        d_str = r.get("date") or ""
        snippet = (r.get("snippet") or "").strip()
        if not snippet or not d_str:
            continue
        try:
            d = _date.fromisoformat(d_str)
        except (ValueError, TypeError):
            continue
        delta = (d - start).days
        if not (-1 <= delta <= window_days):
            continue
        try:
            rating_val = float(r.get("rating")) if r.get("rating") is not None else 5.0
        except (TypeError, ValueError):
            rating_val = 5.0
        in_window.append({
            "date": d_str,
            "rating_val": rating_val,
            "snippet": snippet,
        })

    # Keep only the negative reviews (< 4 stars)
    neg = [r for r in in_window if r["rating_val"] < 4.0]
    # Sort by rating ascending (worst first), then date descending
    neg.sort(key=lambda r: (r["rating_val"], -(_date.fromisoformat(r["date"]).toordinal())))
    neg = neg[:max_reviews]

    grouped = []
    for r in neg:
        rating_str = f"{r['rating_val']:.0f}★"
        grouped.append({
            "date": r["date"],
            "rating": rating_str,
            "observations": [{
                "quote": r["snippet"][:500],  # hard cap for safety
                "label": "Raw review excerpt",
            }],
        })
    return grouped


def render_burst_card(row: pd.Series) -> str:
    """
    Only render for crisis bursts whose evidence is NOT already covered
    by the pain card. Resolution order:

      1. If pain_mentions fall inside the burst window AND the pain card
         will absorb all of them into its own badge, suppress this card.
      2. Otherwise, try to render from pain_mentions filtered to the
         burst window (partial-overlap case).
      3. If pain_mentions don't cover the window at all, fall back to
         raw cached review snippets — pick the lowest-rated reviews in
         the window so a crisis never silently disappears just because
         the LLM classified the complaints as "quality" instead of
         "operational pain".
    """
    burst_cat = s(row.get("burst_category"))
    if burst_cat not in ("active_crisis", "recent_crisis"):
        return ""

    total = i(row.get("burst_total_count"))
    negs = i(row.get("burst_negative_total"))
    baseline = f(row.get("burst_baseline_per_week"))
    neg_d = s(row.get("burst_negative_sample_date"))
    if not neg_d:
        return ""

    place_id = s(row.get("place_id"))
    parsed, indexed_reviews = load_llm_analysis(place_id)
    pain_mentions = parsed.get("pain_mentions") or []

    grouped_all_pain = _group_mentions_by_review(
        pain_mentions, indexed_reviews, PAIN_SUBTYPE_LABELS, "Complaint"
    )

    # Case 1: pain card is going to absorb this crisis — skip the card.
    if grouped_all_pain and _pain_mentions_overlap_burst(grouped_all_pain, neg_d):
        return ""

    # Case 2: try to render from pain_mentions filtered to the burst window
    from datetime import date as _date
    try:
        burst_start = _date.fromisoformat(neg_d)
    except (ValueError, TypeError):
        return ""

    grouped_from_pain = []
    for g in grouped_all_pain:
        try:
            d = _date.fromisoformat(g["date"])
        except (ValueError, TypeError):
            continue
        delta = (d - burst_start).days
        if -1 <= delta <= 10:
            grouped_from_pain.append(g)

    # Case 3: fall back to raw review snippets if nothing from pain
    # extraction lands in the burst window. This is critical for rating-
    # based bursts where the complaints are quality/billing, not the
    # operational pain categories the LLM extracts.
    if grouped_from_pain:
        grouped = grouped_from_pain
        evidence_source = "pain_mentions"
    else:
        grouped = _collect_burst_window_reviews(place_id, neg_d)
        evidence_source = "raw_reviews"

    if not grouped:
        return ""

    headline = {
        "active_crisis": "Negative review burst in the last 30 days",
        "recent_crisis": "Negative review burst in the last 60 days",
    }[burst_cat]

    blocks = []
    for g in grouped:
        obs_html = []
        for o in g["observations"]:
            obs_html.append(
                f'<div class="obs">{esc(o["quote"])}'
                f'<span class="obs-label">{esc(o["label"])}</span></div>'
            )
        cite = make_cite(g["date"], g["rating"])
        obs_count = len(g["observations"])
        multi_note = (
            f' <span class="obs-count">{obs_count} observations</span>'
            if obs_count > 1 else ""
        )
        blocks.append(f"""
        <blockquote class="evidence negative grouped">
          <div class="review-cite">{esc(cite)}{multi_note}</div>
          {"".join(obs_html)}
        </blockquote>
        """)

    fallback_note = ""
    if evidence_source == "raw_reviews":
        fallback_note = (
            '<p class="card-subtitle" style="color:#8f1a1a;">'
            'These reviews were flagged by rating-based burst detection. '
            "They did not match the operational-pain categories the review "
            "classifier extracts, so they are shown as raw excerpts."
            "</p>"
        )

    return f"""
    <div class="card">
      <p class="card-label">Review burst detected</p>
      <h3 class="card-headline">{esc(headline)}</h3>
      <p class="card-subtitle">A burst fires when a contractor receives 3x+ their baseline review velocity in a single week. Baseline: {baseline:.1f} reviews per week.</p>
      <div class="card-stats">
        <div class="stat"><span class="val">{total}</span><span class="lbl">Total bursts</span></div>
        <div class="stat"><span class="val">{negs}</span><span class="lbl">Negative bursts</span></div>
      </div>
      {fallback_note}
      {"".join(blocks)}
    </div>
    """


def render_tech_card(row: pd.Series) -> str:
    builder = s(row.get("webanalyze_site_builder"))
    cms = s(row.get("webanalyze_cms"))
    page_builder = s(row.get("webanalyze_page_builder"))
    tech = s(row.get("webanalyze_tech_summary"))
    has_booking = row.get("has_any_booking_tool")
    phone_only = row.get("phone_only")
    detect_ev = s(row.get("detection_evidence"))

    fsm_cls = "no" if not has_booking else "yes"
    fsm_txt = "Not detected" if not has_booking else "Detected"
    # `phone_only` is a misleading flag name — all it actually means is
    # "we found a phone number prominently displayed on their homepage."
    # It says nothing about whether they also take email, SMS, or contact
    # forms. The dossier used to render it as "Yes — bookings happen by
    # phone" which is a claim we can't back up. We soften to the actual
    # evidence: phone is visible, no online booking platform was detected.
    phone_cls = "yes" if phone_only else ""
    phone_txt = (
        "Likely phone or web form (no online booking platform detected)"
        if phone_only
        else "No online booking platform detected"
    )

    rows = [
        ("Field service platform", fsm_txt, fsm_cls),
        ("How customers book", phone_txt, phone_cls),
    ]
    if builder:
        rows.append(("Site builder", builder, ""))
    if cms:
        rows.append(("CMS", cms, ""))
    if page_builder:
        rows.append(("Page builder", page_builder, ""))
    if tech:
        rows.append(("All detected tech", tech, ""))

    tech_html = "".join(
        f'<div class="tech-row"><div class="k">{esc(k)}</div><div class="v {cls}">{esc(v)}</div></div>'
        for k, v, cls in rows
    )

    return f"""
    <div class="card">
      <p class="card-label">Technology stack gap</p>
      <h3 class="card-headline">What we can see about how they operate</h3>
      <p class="card-subtitle">Fingerprinted from their public website.{f' Detection evidence: {esc(detect_ev)}' if detect_ev else ''}</p>
      <div style="margin:12px 0 0;">{tech_html}</div>
    </div>
    """


# ---------- orchestration ----------

def render_dossier(row: pd.Series, contact: pd.Series) -> str:
    biz = s(row.get("business_name"))
    final_rank = i(row.get("final_rank"))
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    place_id = s(row.get("place_id"))
    reviews = load_reviews(place_id)
    jobs, _ = load_jobs(place_id, s(row.get("business_name")))

    # "Why this is a good lead" comes FIRST as the 5-second summary.
    # Then tech stack gap (loudest objective signal).
    # Then decision maker, then all the evidence cards as proof.
    sections = [
        render_header(row, jobs),
        render_why_card(row, contact, reviews, jobs),
        render_tech_card(row),
        render_decision_maker(contact),
        render_hiring_card(row),
        render_switchers_card(row, reviews),
        render_one_person_card(row, contact, reviews),
        render_dispatch_card(row),
        render_pain_card(row, reviews),
        render_momentum_card(row, reviews),
        render_velocity_card(row),
        render_burst_card(row),
    ]

    body = "".join(sec for sec in sections if sec)

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Dossier · {esc(biz)}</title>
<style>{CSS}</style>
</head>
<body>
<div class="dossier">
{body}
<div class="footer">Rank {final_rank} · Generated {generated} · HVAC Signals pipeline</div>
</div>
</body>
</html>"""


def slugify(s_in: str) -> str:
    out = re.sub(r"[^\w\s-]", "", s_in).strip()
    out = re.sub(r"[\s-]+", "_", out)
    return out[:60]


def _render_chips(chips: list[dict]) -> str:
    if not chips:
        return ""
    items = []
    for c in chips:
        val = c.get("value") or ""
        val_html = f'<span class="chip-val">{esc(val)}</span>' if val else ""
        items.append(f'<span class="chip {c["cls"]}">{esc(c["label"])}{val_html}</span>')
    return f'<div class="chips">{"".join(items)}</div>'


def render_lead_row(row: pd.Series, contact: pd.Series) -> tuple[str, dict]:
    """Render a single lead row for the index page. Returns (html, stats_dict)."""
    biz = s(row.get("business_name"))
    rank = i(row.get("final_rank"))
    city = s(row.get("city"))
    years = f(row.get("license_years"))
    review_count = i(row.get("place_review_count"))
    rating = f(row.get("place_rating"))
    score = f(row.get("score_total"))
    tier_label, tier_class = intent_tier(score)

    owner = s(contact.get("primary_owner_name"))

    place_id = s(row.get("place_id"))
    reviews = load_reviews(place_id)
    jobs, _ = load_jobs(place_id, s(row.get("business_name")))
    bullets = build_why_bullets(row, contact, reviews, jobs)
    chips = build_signal_chips(row, jobs)
    accent_cls = dominant_signal_color(row)

    display_bullets = bullets[:5]
    bullet_html = "".join(f"<li>{esc(b)}</li>" for b in display_bullets)
    more_bullets = ""
    if len(bullets) > 5:
        more_bullets = f'<li style="color:#888;font-style:italic;">+ {len(bullets) - 5} more signal{"s" if len(bullets) - 5 != 1 else ""}</li>'

    slug = slugify(biz)
    dossier_path = f"dossier_v4_{rank:02d}_{slug}.html"

    meta_bits = []
    if owner:
        meta_bits.append(esc(owner))
    if city:
        meta_bits.append(esc(city))
    if years:
        meta_bits.append(f"{years:.0f} yrs licensed")
    if review_count:
        meta_bits.append(f"{review_count} reviews · {rating:.1f}★")

    stats = {
        "has_jobs": bool(jobs),
        "has_fsm_gap": not row.get("has_any_booking_tool"),
        "tier": tier_class,
        "bullet_count": len(bullets),
        "accent": accent_cls,
    }

    html_row = f"""
    <div class="lead {accent_cls}">
      <div class="lead-rank">{rank:02d}</div>
      <div class="lead-body">
        <h3><a href="{esc(dossier_path)}">{esc(biz)}</a></h3>
        <p class="lead-meta">{" · ".join(meta_bits)}</p>
        {_render_chips(chips)}
        <ul class="lead-bullets">{bullet_html}{more_bullets}</ul>
      </div>
      <div class="lead-action">
        <div><span class="tier-badge {tier_class}">{tier_label}</span></div>
        <a class="view-link" href="{esc(dossier_path)}">View dossier →</a>
      </div>
    </div>
    """
    return html_row, stats


def render_index(scored: pd.DataFrame, contacts: pd.DataFrame) -> str:
    top = scored[scored["final_rank"] <= 25].sort_values("final_rank")

    rows = []
    all_stats = []
    for _, r in top.iterrows():
        lic = r["license_no"]
        crows = contacts[contacts["license_no"] == lic]
        if crows.empty:
            continue
        c = crows.iloc[0]
        row_html, stats = render_lead_row(r, c)
        rows.append(row_html)
        all_stats.append(stats)

    total = len(all_stats)
    high_intent = sum(1 for st in all_stats if st["tier"] == "tier-high")
    strong_intent = sum(1 for st in all_stats if st["tier"] == "tier-strong")
    with_hiring = sum(1 for st in all_stats if st["has_jobs"])
    with_fsm_gap = sum(1 for st in all_stats if st["has_fsm_gap"])

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>HVAC Phoenix — Top 25 Hidden Gems</title>
<style>{INDEX_CSS}</style>
</head>
<body>
<div class="page">

  <div class="page-header">
    <h1>Phoenix HVAC contractors · Top 25 hidden gems</h1>
    <p class="sub">
      Ranked by multi-signal scoring from AZ contractor license data, Google reviews,
      website technology fingerprinting, public job postings, and public
      company records. Click any row to open the full dossier.
    </p>
    <div class="summary-stats">
      <div class="ss"><span class="val">{total}</span><span class="lbl">Contractors</span></div>
      <div class="ss"><span class="val">{high_intent}</span><span class="lbl">High intent</span></div>
      <div class="ss"><span class="val">{strong_intent}</span><span class="lbl">Strong intent</span></div>
      <div class="ss"><span class="val">{with_hiring}</span><span class="lbl">Actively hiring</span></div>
      <div class="ss"><span class="val">{with_fsm_gap}</span><span class="lbl">No FSM platform</span></div>
    </div>
  </div>

  <div class="legend">
    <h2>How to read this page</h2>
    <div class="legend-grid">
      <div class="legend-item">
        <span class="chip chip-pain">PAIN <span class="chip-val">##</span></span>
        <div class="desc"><strong>Pain signal.</strong> Customers complaining in reviews about dispatch failures, missed callbacks, or abandoned jobs. Number shows the pain dimension score (0-40).</div>
      </div>
      <div class="legend-item">
        <span class="chip chip-growth">GROWTH <span class="chip-val">##</span></span>
        <div class="desc"><strong>Growth signal.</strong> Hiring postings, review velocity acceleration, or momentum language in reviews. Number shows the growth dimension score (0-25).</div>
      </div>
      <div class="legend-item">
        <span class="chip chip-demand">DEMAND PULL <span class="chip-val">##</span></span>
        <div class="desc"><strong>Demand pull.</strong> Signs the business is winning on reputation rather than scale: customers switching from competitors, owner named by first name in reviews, positive review bursts. Number shows the demand-pull score (0-20).</div>
      </div>
      <div class="legend-item">
        <span class="chip chip-nofsm">NO FSM</span>
        <div class="desc"><strong>No field service platform.</strong> Website fingerprinting detected no ServiceTitan, Housecall Pro, Jobber, or online booking widget. Bookings likely go through phone or web form.</div>
      </div>
      <div class="legend-item">
        <span class="chip chip-hiring">HIRING DISPATCH</span>
        <div class="desc"><strong>Actively hiring a dispatcher.</strong> Open job posting titled Dispatcher, Scheduling Coordinator, or CSR — an ops role FSM software is designed to support.</div>
      </div>
      <div class="legend-item">
        <span class="chip chip-warn">THIN SAMPLE (#)</span>
        <div class="desc"><strong>Thin sample warning.</strong> LLM review analysis ran on fewer than 10 cached reviews. Signal scores derived from review text are discounted proportionally.</div>
      </div>
    </div>

    <div class="legend-sep"></div>

    <h2>Scoring dimensions</h2>
    <div class="legend-scoring">
      <div class="line"><span class="dim">Direct pain</span><span>Review complaints, strained dispatch, crisis bursts</span><span class="cap">0–40</span></div>
      <div class="line"><span class="dim">Scaling strain</span><span>Hiring, velocity, momentum quotes</span><span class="cap">0–25</span></div>
      <div class="line"><span class="dim">Demand pull</span><span>Competitor switches, founder dependency, surge bursts</span><span class="cap">0–20</span></div>
      <div class="line"><span class="dim">Multi-signal</span><span>Bonus when multiple sources fire</span><span class="cap">0–15</span></div>
      <div class="line"><span class="dim">Operational</span><span>Website, reviews, license years</span><span class="cap">0–10</span></div>
      <div class="line"><span class="dim">Disqualifiers</span><span>Smooth-ops indicators</span><span class="cap">-15–0</span></div>
    </div>
    <p style="font-size:11px;color:#888;margin:10px 0 0;">Pain and scaling-strain scores are multiplied by a thin-sample discount when the LLM analyzed fewer than 15 cached reviews. This prevents small contractors with 5-8 reviews from maxing out a dimension on one angry customer.</p>

    <div class="legend-sep"></div>

    <h2>Intent tier badge</h2>
    <div class="legend-tiers">
      <span><span class="tier-badge tier-high">HIGH INTENT</span><span style="color:#555;">score ≥ 40</span></span>
      <span><span class="tier-badge tier-strong">STRONG INTENT</span><span style="color:#555;">25-39</span></span>
      <span><span class="tier-badge tier-emerging">EMERGING INTENT</span><span style="color:#555;">below 25</span></span>
    </div>

    <h2 style="margin-top:16px;">Left-edge accent</h2>
    <div class="legend-accent">
      <span><span class="swatch sw-pain"></span>dominant pain</span>
      <span><span class="swatch sw-growth"></span>dominant growth</span>
      <span><span class="swatch sw-demand"></span>dominant demand pull</span>
      <span><span class="swatch sw-none"></span>no dominant signal</span>
    </div>
  </div>

  {"".join(rows)}

  <div class="page-footer">
    Generated {generated} · HVAC Signals pipeline · {total} contractors
  </div>

</div>
</body>
</html>"""


def process_one(row: pd.Series, contact: pd.Series) -> Path:
    html_doc = render_dossier(row, contact)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rank = i(row.get("final_rank"))
    slug = slugify(s(row.get("business_name")))
    out_path = OUT_DIR / f"dossier_v4_{rank:02d}_{slug}.html"
    out_path.write_text(html_doc)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rank", type=int, default=1)
    parser.add_argument("--all", action="store_true", help="Generate all 25 dossiers + index")
    parser.add_argument("--index", action="store_true", help="Generate only the index page")
    args = parser.parse_args()

    if not SCORED_CSV.exists():
        sys.exit(f"missing {SCORED_CSV}")
    if not CONTACTS_CSV.exists():
        sys.exit(f"missing {CONTACTS_CSV}")

    scored = pd.read_csv(SCORED_CSV)
    contacts = pd.read_csv(CONTACTS_CSV)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.index:
        index_html = render_index(scored, contacts)
        index_path = OUT_DIR / "index.html"
        index_path.write_text(index_html)
        print(f"Index: {index_path.relative_to(ROOT)}")
        return

    if args.all:
        targets = scored[scored["final_rank"] <= 25].sort_values("final_rank")
    else:
        targets = scored[scored["final_rank"] == args.rank]
        if targets.empty:
            sys.exit(f"no contractor with final_rank={args.rank}")

    for _, row in targets.iterrows():
        lic = row["license_no"]
        crows = contacts[contacts["license_no"] == lic]
        if crows.empty:
            print(f"  skip rank {i(row['final_rank'])}: no contact row")
            continue
        contact = crows.iloc[0]
        print(f"Rank {i(row['final_rank']):>2}: {row['business_name']}")
        try:
            out = process_one(row, contact)
            print(f"         -> {out.relative_to(ROOT)}")
        except Exception as e:
            print(f"         ERROR: {type(e).__name__}: {e}")

    # If --all, also generate the index
    if args.all:
        index_html = render_index(scored, contacts)
        index_path = OUT_DIR / "index.html"
        index_path.write_text(index_html)
        print()
        print(f"Index: {index_path.relative_to(ROOT)}")

    print()
    print(f"Output dir: {OUT_DIR.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
