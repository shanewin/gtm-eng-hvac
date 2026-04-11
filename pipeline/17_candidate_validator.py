#!/usr/bin/env python3
"""
Candidate validator — decides which Tavily-extracted candidates and
SerpAPI job postings actually belong to each contractor.

This is the script that replaces fuzzy string matching and regex-based
"is this the same business" judgments. It reads candidates from the
pure-extractor Tavily output and the raw SerpAPI jobs cache, sends them
to the LLM with business context (name, city, state, own domain,
Google Places phone), and gets back per-item belongs/reject decisions.

One LLM call per contractor. Cached per contractor to
`data/signals_raw/validator/{place_id}.json` so re-runs are free.

Usage:
  python pipeline/17_candidate_validator.py --limit 3     # first 3 by pre-score rank
  python pipeline/17_candidate_validator.py               # full pool (70)
  python pipeline/17_candidate_validator.py --license 123 # single contractor
  python pipeline/17_candidate_validator.py --force       # bypass cache
  python pipeline/17_candidate_validator.py --model sonnet  # promote model
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
# Validator runs BEFORE scoring now. Its hiring belongs/reject decisions
# are consumed by step 11 scoring to compute hiring_ops_pain_count and
# friends, so it must cover the full 70-contractor pool (not just top 25).
POOL_CSV = ROOT / "data" / "03_hidden_gems" / "complete.csv"
TAVILY_DIR = ROOT / "data" / "signals_raw" / "tavily_contacts"
JOBS_DIR = ROOT / "data" / "signals_raw" / "serpapi_jobs"
CACHE_DIR = ROOT / "data" / "signals_raw" / "validator"

MODEL_ALIASES = {
    "haiku": "claude-haiku-4-5-20251001",
    "sonnet": "claude-sonnet-4-6",
    "opus": "claude-opus-4-6",
}

# Haiku 4.5 pricing (default). If --model is bumped, cost estimates
# are still printed under these rates — they're a floor, not a ceiling.
INPUT_COST_PER_MTOK = 1.0
OUTPUT_COST_PER_MTOK = 5.0

REQUEST_SLEEP = 0.2


SYSTEM_PROMPT = """You are a data validator. Your job is to look at a list of \
candidate contact items and decide which ones actually belong to a specific \
business. You are given the business's name, city, state, website domain, \
and main phone number. You are given candidates drawn by a regex-based \
extractor from web search results — some will be real, many will be noise.

Your decisions go directly onto a sales rep's dossier. A wrong "belongs" \
call means the rep emails or calls the wrong business. Err toward rejection \
when you are unsure.

Reject ruthlessly:
- Emails on unrelated domains (e.g. @gmail.com when the business has its own \
domain, or @somecompetitor.com)
- **Template/placeholder emails**: addresses that look like documentation \
examples rather than real contacts — `first@`, `firstl@`, `firstname@`, \
`lastname@`, `name@`, `jane@`, `john@`, `johnd@`, `example@`, or any local \
part that reads like a placeholder for how email addresses are formatted. \
These are NOT real emails and must always be rejected.
- Phone numbers that look like they came from URL paths, image filenames, or \
fabricated digit runs (e.g. a 10-digit sequence matching a captcha filename)
- **Phone numbers that appear on aggregator/directory pages next to \
multiple different business names** — BBB profiles, Yelp review pages, \
and contractor directories sometimes show call-tracking numbers, advertising \
lines, or shared accountant/vendor lines that are not specific to the target \
business. If a phone appears only on a third-party page and its connection to \
the target business is ambiguous, reject it.
- Social URLs whose handle points to a DIFFERENT business with a similar name \
(e.g. "Cardinal Heating and Air Conditioning" in Wisconsin when the target is \
"Cardinal Heating & Cooling LLC" in Peoria, Arizona)
- Job postings whose `company_in_posting` field names a different company \
(e.g. Google Jobs "related results" from other HVAC contractors)
- Items that are vague or ambiguous and you cannot confirm

Accept with confidence:
- Emails on the business's own website domain
- Emails that appear on the business's own Facebook page or confirmed \
LinkedIn company page
- Phone numbers that appear prominently in content associated with the \
business's own domain
- Social URLs whose handle clearly corresponds to the business name (strip \
HVAC stopwords like "heating", "cooling", "air" before judging similarity)
- Job postings whose `company_in_posting` field names the business

Return ONLY valid JSON. No markdown, no code fences, no commentary."""


USER_PROMPT_TEMPLATE = """Business:
  name: {business_name}
  city: {city}, {state}
  website domain: {own_domain}
  main phone (from Google Places): {main_phone}

Evaluate each candidate below and return a JSON object with the same \
structure, adding a boolean `belongs` and a short `reason` field to every \
item.

CANDIDATES:
{candidates_json}

Return JSON in this exact shape:

{{
  "emails": [{{"value": "...", "belongs": true|false, "reason": "..."}}],
  "phones": [{{"value": "...", "belongs": true|false, "kind": "main|alt|toll_free|fax|vendor|personal|unknown", "reason": "..."}}],
  "facebook_urls": [{{"value": "...", "belongs": true|false, "reason": "..."}}],
  "linkedin_company_urls": [{{"value": "...", "belongs": true|false, "reason": "..."}}],
  "instagram_urls": [{{"value": "...", "belongs": true|false, "reason": "..."}}],
  "bbb_urls": [{{"value": "...", "belongs": true|false, "reason": "..."}}],
  "yelp_urls": [{{"value": "...", "belongs": true|false, "reason": "..."}}],
  "jobs": [{{"value": "<title>", "company_in_posting": "...", "belongs": true|false, "reason": "..."}}]
}}

Preserve every input item. Do not add new ones. The `reason` field must \
be a short specific phrase — no more than one compact sentence. The reason \
should let a debugger verify your decision without rereading the candidates. \
Good examples:

  - "own domain, on contact page"
  - "wrong business — LinkedIn shows Madison WI location"
  - "captcha filename in wp-content/uploads path"
  - "main phone matches Google Places"
  - "BBB shared line, appears under multiple contractors"

When the decision involves disambiguating between two similar business \
names, your reason MUST mention the distinguishing fact (different city, \
different state, different full legal name). Do not shortcut to "matches" \
— actually verify the match."""


def load_tavily_candidates(place_id: str) -> dict | None:
    path = TAVILY_DIR / f"{place_id}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def load_jobs_candidates(place_id: str) -> list[dict]:
    """Return every raw SerpAPI job posting the pipeline has ever fetched
    for this contractor — the legal-name query (07_serpapi_hiring.py), the
    DBA / place-name retry (07b_serpapi_hiring_retry.py), and any rejected
    postings from an older cache schema. The validator decides which belong;
    no pre-filter is applied here."""
    paths = [
        JOBS_DIR / f"{place_id}.json",
        JOBS_DIR / f"{place_id}_retry.json",
    ]
    seen: set[tuple[str, str]] = set()
    out: list[dict] = []
    for path in paths:
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        resp = data.get("response") or {}
        kept = resp.get("jobs_results") or []
        # Older caches pre-split into kept/rejected. Fold both back in —
        # the validator is the single source of truth now.
        rejected = data.get("rejected_jobs") or []
        for j in kept + rejected:
            title = j.get("title") or ""
            company = j.get("company_name") or ""
            key = (title.strip(), company.strip())
            if key in seen:
                continue
            seen.add(key)
            out.append({"title": title, "company_in_posting": company})
    return out


# Per-list cap on how many candidates get shown to the LLM. Some Tavily
# results scrape phone-directory or BBB-listing pages that dump thousands
# of unique strings — Greenwood Air LLC hit 4200 phones from one page,
# which blew past the 200K context window. 100 per list keeps the prompt
# under ~40K tokens even in the worst case while still preserving every
# candidate with meaningful cross-source confirmation.
MAX_CANDIDATES_PER_LIST = 100


def _cap(items: list[dict]) -> list[dict]:
    """Cap a candidate list at MAX_CANDIDATES_PER_LIST, preferring items
    confirmed by the most source URLs. Ties broken by insertion order
    (sorted() is stable)."""
    if len(items) <= MAX_CANDIDATES_PER_LIST:
        return items
    # Sort descending by source_urls count; stable on ties.
    ranked = sorted(
        items,
        key=lambda it: -len(it.get("source_urls") or []),
    )
    return ranked[:MAX_CANDIDATES_PER_LIST]


def build_candidates_block(
    tavily_payload: dict | None,
    jobs_candidates: list[dict],
) -> dict:
    """Collapse the Tavily + SerpAPI inputs into the shape the LLM prompt
    expects. The LLM output mirrors this structure with belongs fields.
    Each list is capped at MAX_CANDIDATES_PER_LIST, prioritizing items
    with more source-URL confirmations."""
    cand = (tavily_payload or {}).get("candidates") or {}

    def simplify(items: list[dict]) -> list[dict]:
        simplified = [
            {"value": it.get("value"), "source_urls": it.get("source_urls") or []}
            for it in items
        ]
        return _cap(simplified)

    return {
        "emails": simplify(cand.get("emails") or []),
        "phones": simplify(cand.get("phones") or []),
        "facebook_urls": simplify(cand.get("facebook_urls") or []),
        "linkedin_company_urls": simplify(cand.get("linkedin_company_urls") or []),
        "instagram_urls": simplify(cand.get("instagram_urls") or []),
        "bbb_urls": simplify(cand.get("bbb_urls") or []),
        "yelp_urls": simplify(cand.get("yelp_urls") or []),
        "jobs": [
            {"value": j["title"], "company_in_posting": j["company_in_posting"]}
            for j in jobs_candidates
        ][:MAX_CANDIDATES_PER_LIST],
    }


def is_empty_candidates(candidates: dict) -> bool:
    return not any(candidates.get(k) for k in [
        "emails", "phones", "facebook_urls", "linkedin_company_urls",
        "instagram_urls", "bbb_urls", "yelp_urls", "jobs",
    ])


def call_llm(
    client: Anthropic,
    model: str,
    row: pd.Series,
    candidates: dict,
) -> tuple[dict | None, dict]:
    """Single LLM call. Returns (parsed_result, usage)."""
    business_name = str(row.get("business_name") or "").strip()
    city = str(row.get("city") or "").strip()
    state = str(row.get("state") or "AZ").strip()
    website = str(row.get("place_website") or "").strip()
    main_phone = str(row.get("place_phone") or "").strip()

    # Derive bare domain from website URL for the prompt
    own_domain = ""
    if website:
        from urllib.parse import urlparse
        s = website.lower().strip()
        if "://" in s:
            s = urlparse(s).netloc or ""
        own_domain = s.removeprefix("www.").split("/")[0]

    user_prompt = USER_PROMPT_TEMPLATE.format(
        business_name=business_name,
        city=city,
        state=state,
        own_domain=own_domain or "(none)",
        main_phone=main_phone or "(none)",
        candidates_json=json.dumps(candidates, indent=2),
    )

    try:
        resp = client.messages.create(
            model=model,
            max_tokens=8000,
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
    except json.JSONDecodeError:
        return {"parse_error": "invalid json", "raw_text": text[:800]}, usage
    return parsed, usage


def cost_from_usage(usage: dict) -> float:
    inp = usage.get("input_tokens", 0) or 0
    out = usage.get("output_tokens", 0) or 0
    return (inp / 1_000_000) * INPUT_COST_PER_MTOK + (out / 1_000_000) * OUTPUT_COST_PER_MTOK


def print_decision_summary(parsed: dict) -> None:
    def belongs_count(key):
        items = parsed.get(key) or []
        kept = sum(1 for it in items if it.get("belongs"))
        total = len(items)
        return f"{kept}/{total}"
    print(
        f"    emails={belongs_count('emails')} "
        f"phones={belongs_count('phones')} "
        f"fb={belongs_count('facebook_urls')} "
        f"li={belongs_count('linkedin_company_urls')} "
        f"ig={belongs_count('instagram_urls')} "
        f"bbb={belongs_count('bbb_urls')} "
        f"yelp={belongs_count('yelp_urls')} "
        f"jobs={belongs_count('jobs')}"
    )


def process_contractor(
    client: Anthropic,
    model: str,
    row: pd.Series,
    force: bool = False,
) -> tuple[dict | None, dict, bool]:
    """Returns (validated_dict, usage, from_cache)."""
    place_id = str(row.get("place_id") or "")
    cache_path = CACHE_DIR / f"{place_id}.json"

    if cache_path.exists() and not force:
        try:
            cached = json.loads(cache_path.read_text())
            return cached.get("validated"), cached.get("usage") or {}, True
        except (json.JSONDecodeError, OSError):
            pass

    tavily_payload = load_tavily_candidates(place_id)
    jobs = load_jobs_candidates(place_id)
    candidates = build_candidates_block(tavily_payload, jobs)

    if is_empty_candidates(candidates):
        # Nothing to validate — write an empty-validated shell so we don't
        # re-call the LLM on next run.
        empty = {k: [] for k in candidates.keys()}
        payload = {
            "place_id": place_id,
            "business_name": str(row.get("business_name") or ""),
            "model": model,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "candidates": candidates,
            "validated": empty,
            "usage": {"input_tokens": 0, "output_tokens": 0},
        }
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(payload, indent=2))
        return empty, payload["usage"], False

    parsed, usage = call_llm(client, model, row, candidates)

    if parsed is None:
        return None, usage, False
    if "parse_error" in parsed:
        return parsed, usage, False

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps({
        "place_id": place_id,
        "business_name": str(row.get("business_name") or ""),
        "model": model,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "candidates": candidates,
        "validated": parsed,
        "usage": usage,
    }, indent=2))

    return parsed, usage, False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Smoke test on first N by pre-score rank")
    parser.add_argument("--license", type=int, default=None, help="Run for a single license_no")
    parser.add_argument("--force", action="store_true", help="Bypass cache")
    parser.add_argument("--model", default="haiku", choices=list(MODEL_ALIASES),
                        help="Model alias: haiku (default), sonnet, or opus")
    args = parser.parse_args()

    if not POOL_CSV.exists():
        sys.exit(f"missing {POOL_CSV}")

    model = MODEL_ALIASES[args.model]
    client = Anthropic()  # reads ANTHROPIC_API_KEY from env

    df = pd.read_csv(POOL_CSV)
    # complete.csv is pre-scoring — we sort by the pre-score 'rank' column
    # (from 05_rank_hidden_gems) so --limit is deterministic across runs.
    top = df.sort_values("rank") if "rank" in df.columns else df
    if args.license is not None:
        top = top[top["license_no"] == args.license]
    elif args.limit is not None:
        top = top.head(args.limit)

    print(f"Validator model: {model}")
    print(f"Processing {len(top)} contractors")
    print()

    total_cost = 0.0
    api_calls = 0
    cache_hits = 0
    parse_errors = 0
    total_candidates = 0
    total_kept = 0

    for i, (_, row) in enumerate(top.iterrows(), 1):
        rank = int(row["rank"]) if "rank" in row and pd.notna(row.get("rank")) else i
        biz = row["business_name"]
        print(f"[{i}/{len(top)}] rank {rank}: {biz}")

        parsed, usage, from_cache = process_contractor(client, model, row, force=args.force)

        if parsed is None:
            print(f"    API_ERROR: {usage.get('error','')[:120]}")
            continue
        if "parse_error" in parsed:
            parse_errors += 1
            print(f"    PARSE_ERROR: {parsed.get('parse_error')}")
            continue

        if from_cache:
            cache_hits += 1
            print(f"    (cached)", end=" ")
        else:
            api_calls += 1
            cost = cost_from_usage(usage)
            total_cost += cost
            print(f"    ${cost:.4f}", end=" ")

        print_decision_summary(parsed)

        # Rolling totals
        for key in parsed:
            items = parsed.get(key) or []
            total_candidates += len(items)
            total_kept += sum(1 for it in items if it.get("belongs"))

        if not from_cache:
            time.sleep(REQUEST_SLEEP)

    print()
    print("=" * 72)
    print("Summary")
    print("=" * 72)
    print(f"Contractors processed: {len(top)}")
    print(f"API calls:             {api_calls}")
    print(f"Cache hits:            {cache_hits}")
    print(f"Parse errors:          {parse_errors}")
    print(f"Cost this run:         ${total_cost:.4f}")
    print(f"Candidates seen:       {total_candidates}")
    print(f"Candidates kept:       {total_kept} ({total_kept/max(total_candidates,1)*100:.0f}%)")


if __name__ == "__main__":
    main()
