#!/usr/bin/env python3
"""
Evidence audit — read-only inspection of every piece of high-fidelity evidence
we already have cached for the top 25 contractors.

The dossier generator was reading the scored CSV like a dashboard, collapsing
raw evidence into aggregates. This script reads the actual primary sources:

  1. data/serpapi_raw/{place_id}.json        — raw reviews with dates
  2. data/serpapi_jobs_raw/{place_id}.json   — job postings with apply links
  3. data/review_llm_raw/{place_id}.json     — LLM analysis with verbatim quotes
  4. data/dispatch_delay_raw/{place_id}.json — per-review dispatch extractions
  5. data/contacts_raw/{place_id}.json       — website scrape + Apollo cache
  6. All *_quote / *_evidence / *_sample_title columns in the scored CSV

It prints a human-readable evidence inventory per contractor, then a coverage
summary across the top 25.

No API calls. No writes to enrichment files. Read and report only.

Usage:
  python pipeline/17_evidence_audit.py              # top 25
  python pipeline/17_evidence_audit.py --top 10
  python pipeline/17_evidence_audit.py --rank 4     # single contractor
  python pipeline/17_evidence_audit.py --save       # also save text report
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SCORED_CSV = ROOT / "data" / "03_hidden_gems" / "scored.csv"
SERPAPI_RAW_DIR = ROOT / "data" / "signals_raw" / "serpapi_reviews"
JOBS_RAW_DIR = ROOT / "data" / "signals_raw" / "serpapi_jobs"
LLM_RAW_DIR = ROOT / "data" / "signals_raw" / "review_llm"
DISPATCH_RAW_DIR = ROOT / "data" / "signals_raw" / "dispatch_delay"
CONTACTS_RAW_DIR = ROOT / "data" / "signals_raw" / "contacts"
OUT_DIR = ROOT / "outputs"


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


def wrap(text: str, width: int = 78, indent: int = 6) -> list[str]:
    """Simple word wrap for quote display."""
    if not text:
        return []
    import textwrap
    return textwrap.wrap(text, width=width - indent, break_long_words=False)


def render_contractor(row: pd.Series) -> tuple[list[str], dict]:
    """Returns (lines_to_print, coverage_flags_dict)."""
    lines: list[str] = []
    flags = {
        "has_jobs": False,
        "has_dispatcher_job": False,
        "has_dispatch_extractions": False,
        "has_dispatch_quotes": False,
        "has_burst_neg_quote": False,
        "has_burst_pos_quote": False,
        "has_llm_pain_evidence": False,
        "has_llm_momentum_evidence": False,
        "has_refugee_mentions": False,
        "has_regex_dispatch_quote": False,
        "has_regex_switch_quote": False,
        "has_regex_founder_quote": False,
        "has_regex_key_person_quote": False,
        "has_regex_growth_quote": False,
        "has_website_scrape": False,
        "has_apollo_org": False,
        "has_refugee_count": 0,
        "jobs_count": 0,
        "dispatch_extractions": 0,
        "reviews_cached": 0,
    }

    rank = i(row.get("final_rank"))
    biz = s(row.get("business_name"))
    city = s(row.get("city"))
    score = f(row.get("score_total"))
    narrative = s(row.get("primary_narrative"))
    conf = s(row.get("confidence_tier"))
    place_id = s(row.get("place_id"))
    domain = s(row.get("domain"))

    sep = "=" * 82
    lines.append("")
    lines.append(sep)
    lines.append(f"RANK {rank:>2}  {biz}  ({city})")
    lines.append(sep)
    lines.append(f"score={score:.1f}  narrative={narrative}  confidence={conf}  domain={domain or '—'}")

    # ── JOB POSTINGS ───────────────────────────────────────
    jobs_data = load_json(JOBS_RAW_DIR / f"{place_id}.json")
    resp = jobs_data.get("response") or {}
    jobs = resp.get("jobs_results") or []
    lines.append("")
    lines.append("── JOB POSTINGS ────────────────────────────────────────────────────────")
    if not jobs:
        reported_count = i(row.get("hiring_raw_count"))
        if reported_count > 0:
            lines.append(f"  (scored CSV reports {reported_count} postings but no raw cache found)")
        else:
            lines.append("  (none)")
    else:
        flags["has_jobs"] = True
        flags["jobs_count"] = len(jobs)
        lines.append(f"  {len(jobs)} cached:")
        for j in jobs:
            title = s(j.get("title"))
            via = s(j.get("via"))
            loc = s(j.get("location"))
            posted = s((j.get("detected_extensions") or {}).get("posted_at"))
            apply_opts = j.get("apply_options") or []
            link = s(apply_opts[0].get("link") if apply_opts else "")
            if any(k in title.lower() for k in ("dispatch",)):
                flags["has_dispatcher_job"] = True
                marker = "  ▶ "
            else:
                marker = "    "
            loc_bit = f" · {loc}" if loc else ""
            posted_bit = f" · posted {posted}" if posted else ""
            via_bit = f" (via {via})" if via else ""
            lines.append(f"{marker}{title}{via_bit}{loc_bit}{posted_bit}")
            if link:
                lines.append(f"      {link[:140]}")

    # Hiring sample titles from scored CSV
    ops_title = s(row.get("hiring_sample_ops_pain_title"))
    cap_title = s(row.get("hiring_sample_capacity_title"))
    hiring_sources = s(row.get("hiring_sources"))
    if ops_title or cap_title or hiring_sources:
        lines.append("")
        lines.append("  hiring classification (from scored CSV):")
        if ops_title:
            lines.append(f"    ops_pain sample title: {ops_title}")
        if cap_title:
            lines.append(f"    capacity sample title: {cap_title}")
        if hiring_sources:
            lines.append(f"    sources: {hiring_sources}")

    # ── DISPATCH EVIDENCE ──────────────────────────────────
    dispatch_raw = load_json(DISPATCH_RAW_DIR / f"{place_id}.json")
    parsed = dispatch_raw.get("parsed") or {}
    extractions = parsed.get("extractions") or []

    lines.append("")
    lines.append("── DISPATCH EVIDENCE ──────────────────────────────────────────────────")
    disp_cat = s(row.get("dispatch_category"))
    disp_median = f(row.get("dispatch_median_hours"))
    disp_max = f(row.get("dispatch_max_hours"))
    disp_same_day = f(row.get("dispatch_same_day_pct"))
    disp_neg_pct = f(row.get("dispatch_negative_sentiment_pct"))
    extractable = i(row.get("dispatch_extractable_count"))

    if extractable == 0 and not extractions:
        lines.append("  (none)")
    else:
        flags["has_dispatch_extractions"] = True
        flags["dispatch_extractions"] = len(extractions) if extractions else extractable
        lines.append(
            f"  category={disp_cat}  extractions={extractable}  "
            f"median={disp_median:.0f}h  max={disp_max:.0f}h  "
            f"same_day={disp_same_day*100:.0f}%  negative={disp_neg_pct*100:.0f}%"
        )
        if extractions:
            lines.append("")
            lines.append("  per-review extractions:")
            for e in extractions:
                idx = e.get("review_index", "?")
                rating = e.get("rating", "?")
                delay_h = e.get("estimated_delay_hours", "?")
                cat = e.get("delay_category", "?")
                sent = e.get("sentiment", "?")
                quote = s(e.get("verbatim_quote"))
                lines.append(f"    [review #{idx}] rating={rating} delay={delay_h}h cat={cat} sentiment={sent}")
                for w in wrap(f'"{quote}"', width=78, indent=8):
                    lines.append(f"       {w}")

    # Also pull dispatch_fastest/slowest/negative_quote from scored CSV
    fastest = s(row.get("dispatch_fastest_quote"))
    slowest = s(row.get("dispatch_slowest_quote"))
    neg_disp = s(row.get("dispatch_negative_quote"))
    if fastest or slowest or neg_disp:
        flags["has_dispatch_quotes"] = True
        lines.append("")
        lines.append("  dispatch quotes (from scored CSV):")
        if fastest:
            lines.append(f"    fastest: \"{fastest[:200]}\"")
        if slowest:
            lines.append(f"    slowest: \"{slowest[:200]}\"")
        if neg_disp and neg_disp != slowest:
            lines.append(f"    negative: \"{neg_disp[:200]}\"")

    # ── REVIEW LLM ANALYSIS ─────────────────────────────────
    llm_raw = load_json(LLM_RAW_DIR / f"{place_id}.json")
    parsed_llm = llm_raw.get("parsed") or {}

    lines.append("")
    lines.append("── REVIEW LLM ANALYSIS ────────────────────────────────────────────────")
    pain = i(row.get("llm_pain_score"))
    mom = i(row.get("llm_momentum_score"))
    smooth = i(row.get("llm_smooth_ops_score"))
    founder_inv = s(row.get("llm_founder_involvement"))
    key_person = s(row.get("llm_key_person_dependency"))
    refugees = i(row.get("llm_customer_refugee_mentions"))
    one_sent = s(row.get("llm_one_sentence_summary"))
    pain_ev = s(row.get("llm_pain_evidence"))
    mom_ev = s(row.get("llm_momentum_evidence"))
    smooth_ev = s(row.get("llm_smooth_ops_evidence"))
    buying_cat = s(row.get("llm_buying_category"))

    if not (pain or mom or smooth or one_sent):
        lines.append("  (no LLM analysis)")
    else:
        lines.append(
            f"  buying_category={buying_cat}  pain={pain}/10  momentum={mom}/10  "
            f"smooth_ops={smooth}/10"
        )
        lines.append(f"  founder_involvement={founder_inv}  key_person_dep={key_person}  refugees={refugees}")
        if refugees > 0:
            flags["has_refugee_mentions"] = True
            flags["has_refugee_count"] = refugees
        if one_sent:
            lines.append("")
            lines.append("  one-sentence summary:")
            for w in wrap(one_sent, width=78, indent=4):
                lines.append(f"    {w}")
        if pain_ev:
            flags["has_llm_pain_evidence"] = True
            lines.append("")
            lines.append("  pain evidence quote:")
            for w in wrap(f'"{pain_ev}"', width=78, indent=4):
                lines.append(f"    {w}")
        if mom_ev:
            flags["has_llm_momentum_evidence"] = True
            lines.append("")
            lines.append("  momentum evidence quote:")
            for w in wrap(f'"{mom_ev}"', width=78, indent=4):
                lines.append(f"    {w}")
        if smooth_ev:
            lines.append("")
            lines.append("  smooth-ops evidence quote:")
            for w in wrap(f'"{smooth_ev}"', width=78, indent=4):
                lines.append(f"    {w}")

    # ── REGEX NLP QUOTES ────────────────────────────────────
    regex_quotes = [
        ("pain_dispatch", s(row.get("pain_dispatch_sample_quote"))),
        ("pain_comms", s(row.get("pain_comms_sample_quote"))),
        ("pain_capacity", s(row.get("pain_capacity_sample_quote"))),
        ("positive_demand", s(row.get("positive_demand_sample_quote"))),
        ("positive_founder", s(row.get("positive_founder_sample_quote"))),
        ("positive_key_person", s(row.get("positive_key_person_sample_quote"))),
        ("positive_switch", s(row.get("positive_switch_sample_quote"))),
        ("positive_growth", s(row.get("positive_growth_sample_quote"))),
        ("control_positive", s(row.get("control_positive_sample_quote"))),
    ]
    regex_quotes = [(k, v) for k, v in regex_quotes if v]
    if regex_quotes:
        lines.append("")
        lines.append("── REGEX NLP SAMPLE QUOTES ─────────────────────────────────────────────")
        for k, v in regex_quotes:
            if k == "pain_dispatch":
                flags["has_regex_dispatch_quote"] = True
            if k == "positive_switch":
                flags["has_regex_switch_quote"] = True
            if k == "positive_founder":
                flags["has_regex_founder_quote"] = True
            if k == "positive_key_person":
                flags["has_regex_key_person_quote"] = True
            if k == "positive_growth":
                flags["has_regex_growth_quote"] = True
            lines.append(f"  {k}:")
            for w in wrap(f'"{v}"', width=78, indent=4):
                lines.append(f"    {w}")

    # ── REVIEW BURSTS ───────────────────────────────────────
    burst_cat = s(row.get("burst_category"))
    burst_total = i(row.get("burst_total_count"))
    burst_neg = i(row.get("burst_negative_total"))
    burst_pos = i(row.get("burst_positive_total"))
    burst_baseline = f(row.get("burst_baseline_per_week"))
    neg_q = s(row.get("burst_negative_sample_quote"))
    neg_d = s(row.get("burst_negative_sample_date"))
    pos_q = s(row.get("burst_positive_sample_quote"))
    pos_d = s(row.get("burst_positive_sample_date"))

    lines.append("")
    lines.append("── REVIEW BURSTS ──────────────────────────────────────────────────────")
    lines.append(
        f"  category={burst_cat}  total={burst_total}  "
        f"negative={burst_neg}  positive={burst_pos}  baseline={burst_baseline:.2f}/wk"
    )
    if neg_q:
        flags["has_burst_neg_quote"] = True
        lines.append("")
        lines.append(f"  negative burst sample ({neg_d}):")
        for w in wrap(f'"{neg_q}"', width=78, indent=4):
            lines.append(f"    {w}")
    if pos_q:
        flags["has_burst_pos_quote"] = True
        lines.append("")
        lines.append(f"  positive burst sample ({pos_d}):")
        for w in wrap(f'"{pos_q}"', width=78, indent=4):
            lines.append(f"    {w}")

    # ── RAW REVIEWS ─────────────────────────────────────────
    serp = load_json(SERPAPI_RAW_DIR / f"{place_id}.json")
    reviews = serp.get("reviews") or []
    flags["reviews_cached"] = len(reviews)
    lines.append("")
    lines.append("── REVIEWS CACHED ─────────────────────────────────────────────────────")
    lines.append(f"  {len(reviews)} reviews cached")
    if reviews:
        dated = []
        for r in reviews:
            iso = r.get("iso_date") or ""
            if iso:
                dated.append(iso[:10])
        if dated:
            dated.sort(reverse=True)
            lines.append(f"  date range: {dated[-1]} → {dated[0]}")

    # ── TECH STACK ──────────────────────────────────────────
    builder = s(row.get("webanalyze_site_builder"))
    cms = s(row.get("webanalyze_cms"))
    page_builder = s(row.get("webanalyze_page_builder"))
    form_builders = s(row.get("webanalyze_form_builders"))
    tech = s(row.get("webanalyze_tech_summary"))
    has_booking = row.get("has_any_booking_tool")
    detect_ev = s(row.get("detection_evidence"))

    lines.append("")
    lines.append("── TECH STACK ──────────────────────────────────────────────────────────")
    lines.append(f"  site_builder={builder or '—'}  cms={cms or '—'}  page_builder={page_builder or '—'}")
    lines.append(f"  form_builders={form_builders or '—'}")
    lines.append(f"  fsm_detected={'NO' if not has_booking else 'yes'}")
    if tech:
        lines.append(f"  all_tech: {tech}")
    if detect_ev:
        lines.append(f"  detection_evidence: {detect_ev}")

    # ── WEBSITE + APOLLO CONTACT CACHE ─────────────────────
    contacts_raw = load_json(CONTACTS_RAW_DIR / f"{place_id}.json")
    website_cache = contacts_raw.get("website") or {}
    apollo_cache = contacts_raw.get("apollo") or {}
    if website_cache or apollo_cache:
        lines.append("")
        lines.append("── WEBSITE + APOLLO CACHE ─────────────────────────────────────────────")
        if website_cache:
            flags["has_website_scrape"] = True
            pages = website_cache.get("pages_fetched") or []
            lines.append(f"  website pages fetched: {len(pages)}")
            for p in pages[:5]:
                lines.append(f"    - {p}")
            llm_result = website_cache.get("llm_result") or {}
            if llm_result:
                people = llm_result.get("people") or []
                general = llm_result.get("general") or {}
                lines.append(f"  website-extracted people: {len(people)}")
                for p in people[:5]:
                    name = s(p.get("name"))
                    title = s(p.get("title"))
                    email = s(p.get("email"))
                    bits = [b for b in [name, title, email] if b]
                    lines.append(f"    - {' | '.join(bits)}")
                if general:
                    lines.append(f"  website general: email={general.get('email','—')} phone={general.get('phone','—')}")
        if apollo_cache:
            matched = apollo_cache.get("matched_people") or []
            search = apollo_cache.get("search_people") or []
            if matched or search:
                flags["has_apollo_org"] = True
            lines.append(f"  apollo matched_people: {len(matched)}  search_people: {len(search)}")
            for p in matched[:5]:
                lines.append(f"    - {s(p.get('name'))} · {s(p.get('title'))} · {s(p.get('email'))}")

    return lines, flags


def render_coverage_summary(all_flags: list[tuple[int, str, dict]]) -> list[str]:
    lines = []
    sep = "=" * 82
    lines.append("")
    lines.append(sep)
    lines.append("COVERAGE SUMMARY across top 25")
    lines.append(sep)

    n = len(all_flags)
    def count(key: str) -> int:
        return sum(1 for _, _, fl in all_flags if fl.get(key))
    def total(key: str) -> int:
        return sum(int(fl.get(key, 0)) for _, _, fl in all_flags)

    coverage = [
        ("Job postings cached",           count("has_jobs"),             f"{total('jobs_count')} postings total"),
        ("  └ Contains 'Dispatcher' role", count("has_dispatcher_job"),  "direct FSM buyer signal"),
        ("Dispatch extractions (raw)",    count("has_dispatch_extractions"), f"{total('dispatch_extractions')} extractions total"),
        ("Dispatch quotes (scored CSV)",  count("has_dispatch_quotes"),  "fastest/slowest/negative"),
        ("LLM pain evidence quote",       count("has_llm_pain_evidence"), ""),
        ("LLM momentum evidence quote",   count("has_llm_momentum_evidence"), ""),
        ("Customer refugee mentions",     count("has_refugee_mentions"), f"{total('has_refugee_count')} total mentions"),
        ("Regex dispatch pain quote",     count("has_regex_dispatch_quote"), ""),
        ("Regex customer-switch quote",   count("has_regex_switch_quote"), ""),
        ("Regex founder-praise quote",    count("has_regex_founder_quote"), ""),
        ("Regex key-person quote",        count("has_regex_key_person_quote"), ""),
        ("Regex growth quote",            count("has_regex_growth_quote"), ""),
        ("Review burst negative quote",   count("has_burst_neg_quote"), ""),
        ("Review burst positive quote",   count("has_burst_pos_quote"), ""),
        ("Website scrape cached",         count("has_website_scrape"), ""),
        ("Apollo org/people data",        count("has_apollo_org"), ""),
    ]
    for label, c, note in coverage:
        bar = "█" * int((c / n) * 20) + "·" * (20 - int((c / n) * 20))
        note_bit = f"  ({note})" if note else ""
        lines.append(f"  {label:<34} {c:>2}/{n}  {bar}{note_bit}")

    # Undeniable signals list
    lines.append("")
    lines.append("UNDENIABLE SIGNALS (strongest specific evidence):")
    for rank, biz, fl in sorted(all_flags):
        bits = []
        if fl.get("has_dispatcher_job"):
            bits.append("DISPATCHER JOB POSTED")
        if fl.get("has_refugee_mentions"):
            bits.append(f"{fl['has_refugee_count']} refugee mentions")
        if fl.get("has_burst_neg_quote"):
            bits.append("crisis burst quote")
        if fl.get("has_regex_switch_quote"):
            bits.append("customer switch quote")
        if bits:
            lines.append(f"  rank {rank:>2}  {biz[:50]:<50}  →  {', '.join(bits)}")

    return lines


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--top", type=int, default=25)
    parser.add_argument("--rank", type=int, default=None)
    parser.add_argument("--save", action="store_true", help="Also write to outputs/ file")
    args = parser.parse_args()

    if not SCORED_CSV.exists():
        sys.exit(f"missing {SCORED_CSV}")

    df = pd.read_csv(SCORED_CSV)
    if args.rank is not None:
        df = df[df["final_rank"] == args.rank]
        if df.empty:
            sys.exit(f"no rank {args.rank}")
    else:
        df = df[df["final_rank"] <= args.top].sort_values("final_rank")

    all_lines: list[str] = []
    all_flags: list[tuple[int, str, dict]] = []
    for _, row in df.iterrows():
        lines, flags = render_contractor(row)
        all_lines.extend(lines)
        all_flags.append((i(row.get("final_rank")), s(row.get("business_name")), flags))

    if args.rank is None:
        all_lines.extend(render_coverage_summary(all_flags))

    output = "\n".join(all_lines)
    print(output)

    if args.save:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        path = OUT_DIR / f"evidence_audit_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.txt"
        path.write_text(output)
        print(f"\n\nSaved to {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
