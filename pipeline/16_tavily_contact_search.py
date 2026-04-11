#!/usr/bin/env python3
"""
Tavily-based contact candidate discovery.

This script is a PURE EXTRACTOR. It runs one Tavily search per contractor
and dumps every email, phone number, Facebook URL, LinkedIn URL, Instagram
URL, BBB URL, and Yelp URL it can find. It makes NO judgment about whether
any candidate actually belongs to the contractor — that decision is made
downstream by pipeline/17_candidate_validator.py using an LLM call.

This separation exists because regex-based fuzzy matching of handles
against business names has repeatedly produced false positives (Cardinal
Heating & Cooling LLC matching to Cardinal Heating and Air Conditioning,
a different business in Wisconsin). Character-level similarity is the
wrong tool for "is this the same business" questions. The validator in
step 17 handles that with an LLM.

Output per contractor (cached to `data/signals_raw/tavily_contacts/{place_id}.json`):

    {
      "place_id": "...",
      "business_name": "...",
      "query": "...",
      "fetched_at": "<iso>",
      "results": [ ...raw Tavily results... ],
      "candidates": {
        "emails": [{"email": "...", "source_urls": [...]}],
        "phones": [{"phone": "...", "source_urls": [...]}],
        "facebook_urls": [{"url": "...", "source_urls": [...]}],
        "linkedin_company_urls": [...],
        "instagram_urls": [...],
        "bbb_urls": [...],
        "yelp_urls": [...]
      }
    }

Usage:
  python pipeline/16_tavily_contact_search.py --limit 3    # smoke test
  python pipeline/16_tavily_contact_search.py              # full pool (70, skips cached)
  python pipeline/16_tavily_contact_search.py --force      # bypass cache
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from dotenv import load_dotenv
from tavily import TavilyClient

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
# Tavily runs BEFORE scoring now — feeds into step 17 validator, whose
# hiring decisions feed step 11 scoring. That means we read from the
# pre-scoring pool (complete.csv, 70 rows) instead of scored.csv.
POOL_CSV = ROOT / "data" / "03_hidden_gems" / "complete.csv"
CACHE_DIR = ROOT / "data" / "signals_raw" / "tavily_contacts"

SLEEP_BETWEEN = 0.5  # be polite to Tavily

# Spec-based extractors. These find candidate strings that LOOK like the
# thing in question. They make no judgment about ownership.
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)

# Phone regex — standalone US phones only. Reject digit runs embedded in
# URLs, filenames, or other digit/letter context.
PHONE_RE = re.compile(
    r"(?<![\d.=A-Za-z/])"                            # reject if prev is digit/dot/letter/=/slash
    r"(?:\+?1[\s.-]?)?"                              # optional country code
    r"\(?([2-9]\d{2})\)?[\s.-]?"                     # area code
    r"([2-9]\d{2})[\s.-]?"                           # exchange
    r"(\d{4})"                                       # subscriber
    r"(?![\dA-Za-z])"                                # reject if next is digit/letter
)

# NANP "N11" service codes plus other unassigned area codes we've seen
# as false positives in real data.
INVALID_AREA_CODES = {
    "211", "311", "411", "511", "611", "711", "811", "911",
    "555", "899",
}

# Noise prefixes / domains that never represent a real business contact.
NOISE_EMAIL_PREFIXES = {
    "no-reply", "noreply", "do-not-reply", "donotreply",
    "privacy", "unsubscribe", "abuse", "postmaster", "webmaster",
    "dmca", "legal", "copyright",
}

NOISE_EMAIL_DOMAINS = {
    "sentry.io", "wordpress.com", "wpengine.com", "godaddy.com",
    "bluehost.com", "siteground.com", "cloudflare.com", "example.com",
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",  # personal
}

# Social URL extractors — spec-based path matching. These identify
# candidate URLs; the validator decides which actually belong.
FB_HANDLE_RE = re.compile(
    r"https?://(?:www\.|m\.|web\.)?facebook\.com/"
    r"(?!photo\.php|photos/|posts/|events/|groups/|pages/|sharer|tr\?)"
    r"([A-Za-z0-9._-]+)/?(?=$|[?#>\s\"'\)])",
    re.IGNORECASE,
)
INSTAGRAM_RE = re.compile(
    r"https?://(?:www\.)?instagram\.com/"
    r"([A-Za-z0-9._]+)/?(?=$|[?#>\s\"'\)])",
    re.IGNORECASE,
)
LINKEDIN_COMPANY_RE = re.compile(
    r"https?://(?:www\.)?linkedin\.com/company/"
    r"([A-Za-z0-9._-]+)/?(?=$|[?#>\s\"'\)])",
    re.IGNORECASE,
)
BBB_RE = re.compile(
    r"https?://(?:www\.)?bbb\.org/[A-Za-z0-9/._-]+",
    re.IGNORECASE,
)
YELP_RE = re.compile(
    r"https?://(?:www\.)?yelp\.com/biz/[A-Za-z0-9/._-]+",
    re.IGNORECASE,
)


def normalize_domain(url_or_domain: str) -> str:
    """Strip scheme, www., and path. Return bare domain (lowercased)."""
    if not url_or_domain:
        return ""
    s = url_or_domain.strip().lower()
    if "://" in s:
        s = urlparse(s).netloc or ""
    s = s.removeprefix("www.")
    s = s.split("/")[0]
    return s


def normalize_phone(s: str) -> str:
    """Return 10-digit US-normalized phone, or empty string if invalid."""
    digits = re.sub(r"\D", "", s or "")
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return ""
    return digits


def clean_email(email: str) -> str:
    """Lowercase, strip trailing punctuation, drop obvious noise."""
    e = email.lower().strip().rstrip(".,;:)")
    if "@" not in e:
        return ""
    local, _, domain = e.partition("@")
    if len(local) < 3:
        return ""
    if local in NOISE_EMAIL_PREFIXES:
        return ""
    if domain in NOISE_EMAIL_DOMAINS:
        return ""
    if not re.search(r"[a-z]", local):
        return ""
    return e


def extract_candidates_from_result(result: dict) -> dict:
    """
    Extract every candidate email, phone, and social URL from a single
    Tavily result. Returns a dict with keys matching the output schema.
    No judgment is applied — every pattern match is kept.
    """
    url = result.get("url") or ""
    title = result.get("title") or ""
    content = result.get("content") or ""
    raw_content = result.get("raw_content") or ""
    combined = "\n".join([content, raw_content, title])

    # Emails
    emails = set()
    for raw in EMAIL_RE.findall(combined):
        e = clean_email(raw)
        if e:
            emails.add(e)

    # Phones — strip URLs before scanning to avoid digit runs in paths
    phone_text = URL_RE.sub(" ", combined)
    phones = set()
    for area, exch, sub in PHONE_RE.findall(phone_text):
        if area in INVALID_AREA_CODES:
            continue
        digits = f"{area}{exch}{sub}"
        if len(digits) == 10:
            phones.add(digits)

    # Social URLs — scan URL field AND content (for embedded markdown links)
    def all_matches(pattern, text, builder):
        found = set()
        for m in pattern.finditer(text):
            handle = m.group(1) if builder else m.group(0)
            canonical = builder(handle) if builder else handle.rstrip(".,;)")
            if canonical:
                found.add(canonical)
        return found

    texts_to_scan = [url, combined]

    facebook = set()
    instagram = set()
    linkedin = set()
    bbb = set()
    yelp = set()

    for text in texts_to_scan:
        if not text:
            continue
        facebook |= all_matches(
            FB_HANDLE_RE, text,
            lambda h: f"https://www.facebook.com/{h}/",
        )
        instagram |= all_matches(
            INSTAGRAM_RE, text,
            lambda h: f"https://www.instagram.com/{h}/",
        )
        linkedin |= all_matches(
            LINKEDIN_COMPANY_RE, text,
            lambda h: f"https://www.linkedin.com/company/{h}",
        )
        for m in BBB_RE.finditer(text):
            bbb.add(m.group(0).rstrip(".,;)"))
        for m in YELP_RE.finditer(text):
            yelp.add(m.group(0).rstrip(".,;)"))

    return {
        "source_url": url,
        "emails": sorted(emails),
        "phones": sorted(phones),
        "facebook_urls": sorted(facebook),
        "linkedin_company_urls": sorted(linkedin),
        "instagram_urls": sorted(instagram),
        "bbb_urls": sorted(bbb),
        "yelp_urls": sorted(yelp),
    }


def aggregate_candidates(results: list[dict]) -> dict:
    """
    Walk every Tavily result and build the per-contractor candidate
    dict. For each candidate item, track which Tavily result URLs it
    appeared in (used as provenance by the validator).
    """
    email_sources: dict[str, set[str]] = {}
    phone_sources: dict[str, set[str]] = {}
    fb_sources: dict[str, set[str]] = {}
    li_sources: dict[str, set[str]] = {}
    ig_sources: dict[str, set[str]] = {}
    bbb_sources: dict[str, set[str]] = {}
    yelp_sources: dict[str, set[str]] = {}

    def add(d: dict, key: str, source: str):
        d.setdefault(key, set()).add(source)

    for r in results:
        ex = extract_candidates_from_result(r)
        src = ex["source_url"] or ""
        for e in ex["emails"]:
            add(email_sources, e, src)
        for p in ex["phones"]:
            add(phone_sources, p, src)
        for u in ex["facebook_urls"]:
            add(fb_sources, u, src)
        for u in ex["linkedin_company_urls"]:
            add(li_sources, u, src)
        for u in ex["instagram_urls"]:
            add(ig_sources, u, src)
        for u in ex["bbb_urls"]:
            add(bbb_sources, u, src)
        for u in ex["yelp_urls"]:
            add(yelp_sources, u, src)

    def fmt(d: dict[str, set[str]]) -> list[dict]:
        return [
            {"value": k, "source_urls": sorted(v)}
            for k, v in sorted(d.items())
        ]

    return {
        "emails": fmt(email_sources),
        "phones": fmt(phone_sources),
        "facebook_urls": fmt(fb_sources),
        "linkedin_company_urls": fmt(li_sources),
        "instagram_urls": fmt(ig_sources),
        "bbb_urls": fmt(bbb_sources),
        "yelp_urls": fmt(yelp_sources),
    }


def search_contractor(
    client: TavilyClient,
    row: pd.Series,
    force: bool = False,
) -> tuple[dict, bool]:
    """
    Run Tavily search for a single contractor, extract all candidates,
    and cache the combined payload. Returns (cache_dict, was_api_call).
    """
    place_id = str(row.get("place_id") or "")
    business_name = str(row.get("business_name") or "").strip()
    city = str(row.get("city") or "").strip()

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE_DIR / f"{place_id}.json"

    cached = None
    if cache_path.exists() and not force:
        try:
            cached = json.loads(cache_path.read_text())
        except (json.JSONDecodeError, OSError):
            cached = None

    if cached and cached.get("results") is not None and cached.get("candidates"):
        return cached, False

    # Need to fetch (or refresh) — but if we have raw results, we can
    # re-extract without hitting Tavily again.
    raw_results = cached.get("results") if cached else None
    query = ""
    api_called = False
    if raw_results is None:
        query = (
            f'"{business_name}" {city} Arizona '
            f"(contact OR email OR facebook OR linkedin)"
        )
        resp = client.search(
            query=query,
            search_depth="advanced",
            max_results=10,
            include_raw_content=True,
        )
        raw_results = resp.get("results") or []
        api_called = True
        time.sleep(SLEEP_BETWEEN)
    else:
        query = (cached or {}).get("query", "")

    candidates = aggregate_candidates(raw_results)

    payload = {
        "place_id": place_id,
        "license_no": int(row.get("license_no")) if pd.notna(row.get("license_no")) else None,
        "business_name": business_name,
        "city": city,
        "query": query,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "results": raw_results,
        "candidates": candidates,
    }
    cache_path.write_text(json.dumps(payload, indent=2))
    return payload, api_called


def print_summary(payload: dict) -> None:
    cand = payload.get("candidates") or {}
    def count(key):
        return len(cand.get(key) or [])
    print(
        f"    emails={count('emails'):<3} phones={count('phones'):<3} "
        f"fb={count('facebook_urls'):<2} li={count('linkedin_company_urls'):<2} "
        f"ig={count('instagram_urls'):<2} bbb={count('bbb_urls'):<2} "
        f"yelp={count('yelp_urls'):<2}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Smoke test on first N (by pre-score rank)")
    parser.add_argument("--license", type=int, default=None, help="Run for a single license_no")
    parser.add_argument("--force", action="store_true", help="Bypass cache and re-fetch from Tavily")
    args = parser.parse_args()

    if not POOL_CSV.exists():
        sys.exit(f"missing {POOL_CSV}")

    client = TavilyClient()  # reads TAVILY_API_KEY from env

    df = pd.read_csv(POOL_CSV)
    # complete.csv doesn't have final_rank yet (we're pre-scoring).
    # Sort by the pre-scoring 'rank' column for a stable --limit.
    top = df.sort_values("rank") if "rank" in df.columns else df
    if args.license is not None:
        top = top[top["license_no"] == args.license]
    elif args.limit:
        top = top.head(args.limit)

    print(f"Processing {len(top)} contractors")
    print(f"Cache dir: {CACHE_DIR.relative_to(ROOT)}")
    print()

    api_calls = 0
    cache_hits = 0
    total = len(top)
    totals = {
        "emails": 0, "phones": 0, "facebook_urls": 0,
        "linkedin_company_urls": 0, "instagram_urls": 0,
        "bbb_urls": 0, "yelp_urls": 0,
    }

    for i, (_, row) in enumerate(top.iterrows(), 1):
        rank = int(row["rank"]) if "rank" in row and pd.notna(row.get("rank")) else i
        biz = row["business_name"]
        print(f"[{i}/{total}] rank {rank}: {biz}")
        try:
            payload, api_called = search_contractor(client, row, force=args.force)
        except Exception as e:
            print(f"    ERROR: {type(e).__name__}: {e}")
            continue
        if api_called:
            api_calls += 1
        else:
            cache_hits += 1
        print_summary(payload)
        for key in totals:
            totals[key] += len(payload.get("candidates", {}).get(key) or [])

    print()
    print("=" * 72)
    print("Summary")
    print("=" * 72)
    print(f"Contractors processed: {total}")
    print(f"API calls:             {api_calls}")
    print(f"Cache hits:            {cache_hits}")
    print(f"Estimated cost:        ~${api_calls * 0.015:.2f}")
    print()
    print("Candidate totals (across all contractors):")
    for key, n in totals.items():
        print(f"  {key:<24} {n}")
    print()
    print(f"Next step: run pipeline/17_candidate_validator.py to decide which candidates belong")


if __name__ == "__main__":
    main()
