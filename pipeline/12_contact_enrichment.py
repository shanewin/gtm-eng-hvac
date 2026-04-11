#!/usr/bin/env python3
"""
Contact enrichment for top 25 hidden gems — finds the decision-maker's
name, title, email, and LinkedIn for each contractor.

Sources (used together, merged):
  1. Apollo People Search + Match (structured, canonical contacts)
  2. Contractor website scraping (/, /about, /contact, /team, /meet-the-team)
     with Claude Haiku-assisted extraction for messy HTML

For each contractor we output a "primary contact" (highest seniority match
we can find across all sources) plus up to two additional contacts, plus
general business contact info.

Usage:
  python pipeline/13_contact_enrichment.py --limit 3    # smoke test on top 3
  python pipeline/13_contact_enrichment.py              # full top 25 run
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
import urllib3
from anthropic import Anthropic
from dotenv import dotenv_values

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = Path(__file__).resolve().parent.parent
SCORED_CSV = ROOT / "data" / "03_hidden_gems" / "scored.csv"
OUT_CSV = ROOT / "data" / "04_contacts" / "enriched.csv"
RAW_DIR = ROOT / "data" / "signals_raw" / "contacts"
SNAPSHOT_DIR = ROOT / "data" / "snapshots" / "contacts"

ENV = dotenv_values(ROOT / ".env")
APOLLO_KEY = ENV.get("APOLLO_API_KEY")
ANTHROPIC_KEY = ENV.get("ANTHROPIC_API_KEY")
MODEL = "claude-haiku-4-5-20251001"

APOLLO_SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/api_search"
APOLLO_MATCH_URL = "https://api.apollo.io/api/v1/people/match"

# Titles to search for in Apollo — ordered by priority
APOLLO_SEARCH_TITLES = [
    "owner", "president", "ceo", "founder", "co-founder",
    "general manager", "vice president", "coo",
    "operations manager", "director of operations",
    "service manager", "managing partner", "managing member",
]

# Seniority ranking used to pick the "best" single contact per contractor.
# Lower index = higher priority.
SENIORITY_RANK = {
    "owner": 0, "founder": 0, "co-founder": 0,
    "president": 1, "ceo": 1,
    "general manager": 2, "gm": 2,
    "coo": 3, "chief operating officer": 3,
    "vp": 4, "vice president": 4,
    "managing partner": 4, "managing member": 4,
    "operations manager": 5, "director of operations": 5,
    "service manager": 6,
    "office manager": 7,
}

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
WEBSITE_CONTACT_PATHS = [
    "/", "/about", "/about-us", "/contact", "/contact-us",
    "/team", "/our-team", "/meet-the-team", "/meet-our-team",
    "/who-we-are", "/leadership",
]
MAX_PAGES_PER_SITE = 4
PAGE_SLEEP = 0.3
CONTRACTOR_SLEEP = 0.6

SYSTEM_PROMPT = """You are extracting decision-maker contact information \
from HVAC contractor website HTML.

Look for named people associated with the business (NOT customers, NOT \
reviewers). Prioritize:
- Owners, founders, co-founders
- Presidents, CEOs, COOs
- General managers, operations managers, service managers
- Family members who run the business (spouses, sons, daughters in named \
leadership roles)

For each person found, extract:
- Full name (first + last, exactly as shown)
- Title / role
- Email (only if explicitly visible and clearly tied to that person)
- LinkedIn URL (only if explicitly linked)
- Phone (only if specific to that person, not the main office line)

Also extract general business contact info:
- General business email (info@, contact@, office@)
- General business address (if shown)

CRITICAL RULES:
- Only extract information that is EXPLICITLY visible in the HTML
- Never fabricate names, emails, or titles
- Do not confuse customer names from testimonials with business leadership
- Do not confuse technician names mentioned in bios with owners
- If the same person appears multiple times, consolidate into one entry
- If no clear leadership info is present, return empty "people" array

Return ONLY valid JSON in this exact format:
{
  "people": [
    {
      "name": "Full Name",
      "title": "Owner",
      "email": "name@domain.com" or null,
      "phone": "(480) 555-1234" or null,
      "linkedin": "https://linkedin.com/in/..." or null,
      "source_note": "brief context like 'About page meet the team'"
    }
  ],
  "general": {
    "email": "info@domain.com" or null,
    "phone": "(480) 555-1234" or null,
    "address": "full address" or null
  }
}

No markdown, no commentary, no code fences. JSON only."""


def apollo_search_people(domain: str) -> tuple[list[dict], str | None]:
    if not APOLLO_KEY:
        return [], "no_apollo_key"
    headers = {
        "x-api-key": APOLLO_KEY,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    body = {
        "q_organization_domains_list": [domain],
        "person_titles": APOLLO_SEARCH_TITLES,
        "per_page": 10,
    }
    try:
        r = requests.post(APOLLO_SEARCH_URL, json=body, headers=headers, timeout=30)
    except requests.RequestException as e:
        return [], f"request_error: {str(e)[:100]}"
    if r.status_code != 200:
        return [], f"HTTP {r.status_code}"
    try:
        data = r.json()
    except json.JSONDecodeError:
        return [], "json_decode_error"
    return data.get("people") or [], None


def apollo_match_person(person_id: str) -> tuple[dict | None, str | None]:
    if not APOLLO_KEY:
        return None, "no_apollo_key"
    headers = {
        "x-api-key": APOLLO_KEY,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    body = {"id": person_id, "reveal_personal_emails": True}
    try:
        r = requests.post(APOLLO_MATCH_URL, json=body, headers=headers, timeout=30)
    except requests.RequestException as e:
        return None, f"request_error: {str(e)[:100]}"
    if r.status_code != 200:
        return None, f"HTTP {r.status_code}"
    try:
        data = r.json()
    except json.JSONDecodeError:
        return None, "json_decode_error"
    return data.get("person"), None


def seniority_score(title: str) -> int:
    if not title:
        return 999
    title_lower = title.lower()
    for keyword, rank in SENIORITY_RANK.items():
        if keyword in title_lower:
            return rank
    return 100


def fetch_website_pages(base_url: str) -> dict[str, str]:
    if not base_url:
        return {}
    parsed = urlparse(base_url)
    host = parsed.hostname
    if not host:
        return {}
    scheme = parsed.scheme or "https"
    base = f"{scheme}://{host}"

    candidates: list[str] = []
    # Always try the URL as-given first (may have franchise subpath)
    first = base_url.rstrip("/")
    candidates.append(first)
    for path in WEBSITE_CONTACT_PATHS:
        u = urljoin(base + "/", path.lstrip("/"))
        if u not in candidates:
            candidates.append(u)

    results: dict[str, str] = {}
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
        "Accept-Language": "en-US,en;q=0.9",
    })

    for url in candidates:
        if len(results) >= MAX_PAGES_PER_SITE:
            break
        html = _try_fetch(session, url, verify=True)
        if html is None:
            html = _try_fetch(session, url, verify=False)
        if html:
            results[url] = html
        time.sleep(PAGE_SLEEP)

    return results


def _try_fetch(session: requests.Session, url: str, verify: bool) -> str | None:
    try:
        r = session.get(url, timeout=8, allow_redirects=True, verify=verify)
    except requests.RequestException:
        return None
    if r.status_code == 200:
        ct = r.headers.get("Content-Type", "").lower()
        if "text/html" in ct or "application/xhtml" in ct:
            return r.text
    return None


def html_to_text_keeping_links(html: str) -> str:
    # Strip scripts/styles
    html = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"<style[^>]*>.*?</style>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    # Inline anchor hrefs as "(link)" so Claude sees LinkedIn URLs + email hrefs
    html = re.sub(
        r'<a\s+[^>]*href="([^"]*)"[^>]*>([^<]*)</a>',
        r'\2 [\1]',
        html,
        flags=re.IGNORECASE,
    )
    # Strip remaining tags
    html = re.sub(r"<[^>]+>", " ", html)
    # Decode basic entities
    html = html.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&#39;", "'").replace("&quot;", '"').replace("&nbsp;", " ")
    # Collapse whitespace
    html = re.sub(r"\s+", " ", html)
    return html.strip()


def llm_extract_contacts(
    client: Anthropic, business_name: str, pages: dict[str, str]
) -> tuple[dict | None, dict]:
    if not pages:
        return None, {"skipped": True}

    # Combine text from all fetched pages, cap to keep input tokens reasonable
    combined_parts = []
    total_chars = 0
    max_chars = 20000
    per_page_max = 6000
    for url, html in pages.items():
        text = html_to_text_keeping_links(html)
        snippet = text[:per_page_max]
        block = f"\n\n=== {url} ===\n{snippet}"
        if total_chars + len(block) > max_chars:
            break
        combined_parts.append(block)
        total_chars += len(block)

    combined = "".join(combined_parts)

    user_prompt = (
        f"Contractor name: {business_name}\n\n"
        f"Extract decision-maker contacts from the following scraped website "
        f"content. Return JSON only.\n\n"
        f"{combined}"
    )

    try:
        resp = client.messages.create(
            model=MODEL,
            max_tokens=1024,
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
        return None, {"parse_error": True, "raw_text": text[:500], **usage}

    return parsed, usage


def normalize_name(name: str) -> str:
    if not name:
        return ""
    return re.sub(r"[^a-z ]", "", name.lower()).strip()


def merge_contacts(
    apollo_people: list[dict], website_people: list[dict]
) -> list[dict]:
    """Merge Apollo and website contacts. Apollo takes precedence for fields
    it has, website fills in gaps, and any person only found in one source is
    kept as-is."""
    merged: list[dict] = []
    seen_names: dict[str, dict] = {}

    # Apollo contacts first (structured, canonical)
    for p in apollo_people:
        name = p.get("name") or ""
        key = normalize_name(name)
        if not key:
            continue
        entry = {
            "name": name,
            "title": p.get("title"),
            "email": p.get("email"),
            "linkedin": p.get("linkedin_url"),
            "phone": p.get("phone"),
            "photo": p.get("photo_url"),
            "headline": p.get("headline"),
            "city": p.get("city"),
            "state": p.get("state"),
            "source": "apollo",
            "seniority": seniority_score(p.get("title") or ""),
        }
        seen_names[key] = entry
        merged.append(entry)

    # Website contacts — merge if name overlaps, otherwise append
    for p in website_people:
        name = p.get("name") or ""
        key = normalize_name(name)
        if not key:
            continue

        # Fuzzy merge — if the website name's first+last words match an Apollo entry
        key_tokens = set(key.split())
        existing_key = None
        for ek in seen_names:
            ek_tokens = set(ek.split())
            if key_tokens == ek_tokens or (
                len(key_tokens) >= 2 and key_tokens.issubset(ek_tokens)
            ):
                existing_key = ek
                break

        if existing_key:
            # Fill gaps from website data
            entry = seen_names[existing_key]
            if not entry.get("email") and p.get("email"):
                entry["email"] = p.get("email")
                entry["source"] = entry.get("source", "") + "+website"
            if not entry.get("linkedin") and p.get("linkedin"):
                entry["linkedin"] = p.get("linkedin")
            if not entry.get("phone") and p.get("phone"):
                entry["phone"] = p.get("phone")
            if not entry.get("title") and p.get("title"):
                entry["title"] = p.get("title")
        else:
            entry = {
                "name": name,
                "title": p.get("title"),
                "email": p.get("email"),
                "linkedin": p.get("linkedin"),
                "phone": p.get("phone"),
                "source": "website",
                "seniority": seniority_score(p.get("title") or ""),
            }
            seen_names[key] = entry
            merged.append(entry)

    # Sort by seniority rank
    merged.sort(key=lambda e: (e.get("seniority", 999), e.get("name", "")))
    return merged


def enrich_contractor(
    row: pd.Series, client: Anthropic
) -> tuple[dict, dict, dict]:
    """Return (merged_row_metrics, raw_apollo_blob, raw_website_blob)."""
    biz = str(row.get("business_name") or "")
    domain = str(row.get("domain") or "")
    website = str(row.get("place_website") or "")
    place_id = str(row.get("place_id") or "")
    final_rank = row.get("final_rank")

    apollo_status = "skipped"
    apollo_people: list[dict] = []
    apollo_search_raw: list[dict] = []
    apollo_match_credits = 0

    if domain and domain != "nan":
        raw_search, err = apollo_search_people(domain)
        apollo_search_raw = raw_search
        if err:
            apollo_status = f"search_{err}"
        else:
            # Rank by title keyword match and take the TOP person
            candidates = []
            for p in raw_search:
                title = (p.get("title") or "").lower()
                score = seniority_score(title)
                candidates.append((score, p))
            candidates.sort(key=lambda x: x[0])

            # Match the top 1 person (minimize credit burn)
            for score, cand in candidates[:1]:
                pid = cand.get("id")
                if not pid:
                    continue
                matched, match_err = apollo_match_person(pid)
                apollo_match_credits += 1
                if matched:
                    apollo_people.append(matched)
            if apollo_people:
                apollo_status = f"matched_{len(apollo_people)}"
            elif candidates:
                apollo_status = "search_ok_no_match"
            else:
                apollo_status = "no_people_found"

    # Website scrape + LLM extract
    website_status = "skipped"
    llm_result: dict | None = None
    website_people: list[dict] = []
    website_general: dict = {}
    pages_fetched: dict[str, str] = {}

    if website and website != "nan":
        try:
            pages_fetched = fetch_website_pages(website)
        except Exception as e:
            website_status = f"fetch_error: {str(e)[:80]}"

        if pages_fetched:
            llm_result, usage = llm_extract_contacts(client, biz, pages_fetched)
            if llm_result is None:
                website_status = f"llm_error: {usage.get('error', usage.get('parse_error', 'unknown'))}"
            else:
                website_people = llm_result.get("people") or []
                website_general = llm_result.get("general") or {}
                website_status = f"extracted_{len(website_people)}"
        else:
            website_status = "no_pages_fetched"

    # Merge
    all_contacts = merge_contacts(apollo_people, website_people)
    primary = all_contacts[0] if all_contacts else {}
    secondary = all_contacts[1] if len(all_contacts) > 1 else {}
    tertiary = all_contacts[2] if len(all_contacts) > 2 else {}

    metrics = {
        "license_no": row.get("license_no"),
        "final_rank": final_rank,
        "business_name": biz,
        "apollo_search_count": len(apollo_search_raw),
        "apollo_match_credits": apollo_match_credits,
        "apollo_status": apollo_status,
        "website_pages_fetched": len(pages_fetched),
        "website_status": website_status,
        "contact_count": len(all_contacts),
        # Primary contact
        "primary_contact_name": primary.get("name"),
        "primary_contact_title": primary.get("title"),
        "primary_contact_email": primary.get("email"),
        "primary_contact_phone": primary.get("phone"),
        "primary_contact_linkedin": primary.get("linkedin"),
        "primary_contact_source": primary.get("source"),
        "primary_contact_headline": primary.get("headline"),
        # Secondary / tertiary
        "secondary_contact_name": secondary.get("name"),
        "secondary_contact_title": secondary.get("title"),
        "secondary_contact_email": secondary.get("email"),
        "secondary_contact_source": secondary.get("source"),
        "tertiary_contact_name": tertiary.get("name"),
        "tertiary_contact_title": tertiary.get("title"),
        "tertiary_contact_source": tertiary.get("source"),
        # General business contact from website
        "general_email": website_general.get("email"),
        "general_phone": website_general.get("phone"),
    }

    raw_apollo = {
        "search_results": apollo_search_raw,
        "matched_people": apollo_people,
        "status": apollo_status,
        "credits_used": apollo_match_credits,
    }
    raw_website = {
        "pages_fetched": list(pages_fetched.keys()),
        "llm_result": llm_result,
        "status": website_status,
    }

    return metrics, raw_apollo, raw_website


def main() -> None:
    if not APOLLO_KEY:
        sys.exit("APOLLO_API_KEY missing from .env")
    if not ANTHROPIC_KEY:
        sys.exit("ANTHROPIC_API_KEY missing from .env")
    if not SCORED_CSV.exists():
        sys.exit(f"missing {SCORED_CSV} — run 11_scoring.py first")

    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=25)
    ap.add_argument("--start", type=int, default=1, help="final_rank to start at")
    args = ap.parse_args()

    df = pd.read_csv(SCORED_CSV)
    df = df.sort_values("final_rank").reset_index(drop=True)
    target = df[
        (df["final_rank"] >= args.start)
        & (df["final_rank"] < args.start + args.limit)
    ].copy()

    print(f"Enriching contacts for {len(target)} contractors")
    print(f"Ranks: {int(target['final_rank'].min())}–{int(target['final_rank'].max())}")
    print()

    client = Anthropic(api_key=ANTHROPIC_KEY)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    results = []
    total_apollo_credits = 0

    for i, row in enumerate(target.to_dict(orient="records"), 1):
        biz = row.get("business_name", "")
        final_rank = row.get("final_rank")
        place_id = str(row.get("place_id") or "")
        print(f"[{i}/{len(target)}] rank={final_rank}  {biz[:48]:<48}", flush=True)

        metrics, raw_apollo, raw_website = enrich_contractor(row, client)
        total_apollo_credits += metrics["apollo_match_credits"]

        # Cache raw per contractor
        cache_path = RAW_DIR / f"{place_id}.json"
        cache_path.write_text(json.dumps({
            "business_name": biz,
            "license_no": int(row["license_no"]) if pd.notna(row.get("license_no")) else None,
            "place_id": place_id,
            "final_rank": int(final_rank) if pd.notna(final_rank) else None,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "apollo": raw_apollo,
            "website": raw_website,
        }, indent=2, default=str))

        results.append(metrics)

        # Report to stdout
        p_name = metrics["primary_contact_name"] or "—"
        p_title = metrics["primary_contact_title"] or "—"
        p_email = metrics["primary_contact_email"] or "—"
        p_source = metrics["primary_contact_source"] or "—"
        print(
            f"    primary: {p_name} · {p_title} · {p_email} · [{p_source}]"
        )
        print(
            f"    apollo: {metrics['apollo_status']}  |  "
            f"website: {metrics['website_status']}  |  "
            f"contacts: {metrics['contact_count']}"
        )
        print()

        time.sleep(CONTRACTOR_SLEEP)

    # Write merged CSV
    out_df = pd.DataFrame(results)
    out_df.to_csv(OUT_CSV, index=False)
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    snap = SNAPSHOT_DIR / f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.csv"
    out_df.to_csv(snap, index=False)

    # Summary
    print("=" * 74)
    print("Summary")
    print("=" * 74)
    print(f"Contractors processed:         {len(out_df)}")
    print(f"Apollo match credits used:     {total_apollo_credits}")
    print(f"Contractors with primary email: {int(out_df['primary_contact_email'].notna().sum())}")
    print(f"Contractors with primary LinkedIn: {int(out_df['primary_contact_linkedin'].notna().sum())}")
    print(f"Contractors with any contact:  {int((out_df['contact_count'] > 0).sum())}")
    print(f"Contractors with 2+ contacts:  {int((out_df['contact_count'] >= 2).sum())}")
    print()
    print("Source attribution for primary contact:")
    if "primary_contact_source" in out_df.columns:
        for src, n in out_df["primary_contact_source"].value_counts(dropna=True).items():
            print(f"  {src:<20} {n}")
    none_count = int(out_df["primary_contact_source"].isna().sum())
    if none_count:
        print(f"  (no primary found)   {none_count}")
    print()
    print("Outputs:")
    print(f"  {OUT_CSV.relative_to(ROOT)}")
    print(f"  {snap.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
