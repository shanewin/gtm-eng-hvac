#!/usr/bin/env python3
"""
Contact augmentation for top 25 hidden gems — uses EVERY source available:

  1. AZ ROC qualifying_party (ground-truth owner name, public record, free)
  2. Website scraping + Claude Haiku extraction (cached from 13_contact_enrichment)
  3. Apollo People Search + Match (cached from 13_contact_enrichment)
  4. Google Places business phone (already in data)
  5. Email pattern generation from name + domain (5-6 common formats)
  6. LinkedIn people-search URL construction (pre-built click link)

Produces data/04_contacts/augmented.csv with per-contractor:
  - primary_owner_name         (qualifying_party — the legal license holder)
  - business_phone             (from Google Places)
  - business_email             (from website extraction)
  - email_patterns             (semicolon-separated list of likely addresses)
  - linkedin_search_url        (pre-built people search URL)
  - linkedin_direct_url        (if Apollo found one)
  - website_extracted_contacts (JSON list of any additional people from website)
  - contact_confidence         (high / medium / low)
  - how_to_reach               (plain-English instructions for the sales rep)
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlparse

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SCORED_CSV = ROOT / "data" / "03_hidden_gems" / "scored.csv"
CONTACTS_RAW_DIR = ROOT / "data" / "signals_raw" / "contacts"
TAVILY_CSV = ROOT / "data" / "04_contacts" / "tavily_discovered.csv"
OUT_CSV = ROOT / "data" / "04_contacts" / "augmented.csv"
SNAPSHOT_DIR = ROOT / "data" / "snapshots" / "contacts"


# Email-pattern heuristics — ordered by likelihood for small businesses
# (founders usually use firstname@ or firstname.lastname@)
def generate_email_patterns(full_name: str, domain: str) -> list[str]:
    if not full_name or not domain:
        return []
    # Strip suffixes like Jr, Sr, III
    name = re.sub(
        r"\b(jr|sr|ii|iii|iv)\b\.?",
        "",
        full_name,
        flags=re.IGNORECASE,
    ).strip()
    parts = [p for p in re.split(r"\s+", name) if p]
    # Remove middle initials like "M." or "Dean" as standalone middle names
    if len(parts) >= 3:
        parts = [parts[0], parts[-1]]
    if not parts:
        return []

    first = parts[0].lower()
    first = re.sub(r"[^a-z]", "", first)
    last = parts[-1].lower() if len(parts) > 1 else ""
    last = re.sub(r"[^a-z]", "", last)
    f_init = first[0] if first else ""
    l_init = last[0] if last else ""

    patterns: list[str] = []

    # Owner-style (most likely for small shops)
    if first:
        patterns.append(f"{first}@{domain}")
    if first and last:
        patterns.append(f"{first}.{last}@{domain}")
        patterns.append(f"{first}{last}@{domain}")
        patterns.append(f"{f_init}{last}@{domain}")
        patterns.append(f"{f_init}.{last}@{domain}")
        patterns.append(f"{first}{l_init}@{domain}")

    # Generic business mailboxes
    patterns.extend([
        f"info@{domain}",
        f"office@{domain}",
        f"contact@{domain}",
        f"service@{domain}",
    ])

    # Dedupe preserving order
    seen = set()
    out = []
    for p in patterns:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def build_linkedin_search_url(owner_name: str, business_name: str) -> str:
    query = f"{owner_name} {business_name}".strip()
    return f"https://www.linkedin.com/search/results/people/?keywords={quote(query)}&origin=GLOBAL_SEARCH_HEADER"


def normalize_ascii(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"[^\w\s@.-]", "", s).strip()


def load_contacts_raw(place_id: str) -> dict:
    path = CONTACTS_RAW_DIR / f"{place_id}.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def derive_contact_confidence(
    owner_name: str,
    has_website_email: bool,
    has_website_name_match: bool,
    apollo_match_found: bool,
) -> str:
    """Confidence in our ability to reach the decision maker."""
    # High: we have owner name + a verified email (from Apollo or website)
    if apollo_match_found:
        return "high"
    if owner_name and has_website_email and has_website_name_match:
        return "high"
    # Medium: owner name + business email or LinkedIn search viable
    if owner_name and has_website_email:
        return "medium"
    if owner_name:
        return "medium"
    # Low: we have a business phone but no verified name
    return "low"


def build_how_to_reach(
    owner_name: str,
    owner_first_name: str,
    booking_phone: str,
    booking_website: str,
    business_email: str,
    email_patterns: list[str],
    linkedin_search_url: str,
    confidence: str,
) -> str:
    """Generate plain-English instructions for the salesperson."""
    lines = []

    # The core insight: the phone/website ARE the signal. These contractors
    # are on this list precisely because they rely on phone calls and web
    # forms for bookings — so the customer booking line is the front door.
    if booking_phone and owner_first_name:
        lines.append(
            f'Their customer booking line is {booking_phone} — this is the '
            f'number homeowners call to schedule. Ask for {owner_first_name} '
            f'(full name on the AZ contractor license: {owner_name}).'
        )
    elif booking_phone:
        lines.append(
            f'Their customer booking line is {booking_phone} — this is the '
            f'number homeowners call to schedule. Ask who handles operations.'
        )

    if booking_website:
        lines.append(
            f'Customer booking web form: {booking_website} '
            f'(same channel their customers use — good for a low-friction first touch).'
        )

    if business_email:
        lines.append(f'General inbox: {business_email} (pulled from their website).')

    if email_patterns:
        top_patterns = email_patterns[:3]
        lines.append(
            f'Likely personal email patterns: {", ".join(top_patterns)} '
            f'(NOT verified — use an email verifier or test send).'
        )

    if linkedin_search_url:
        lines.append(
            f'Find on LinkedIn: <a href="{linkedin_search_url}">click here</a> '
            f'(pre-built people-search query).'
        )

    return " ".join(lines)


def process_contractor(row: pd.Series) -> dict:
    biz = str(row.get("business_name") or "")
    domain = str(row.get("domain") or "")
    website = str(row.get("place_website") or "")
    place_phone = str(row.get("place_phone") or "")
    qualifying = str(row.get("qualifying_party") or "")
    place_id = str(row.get("place_id") or "")
    final_rank = row.get("final_rank")
    license_no = row.get("license_no")

    # Clean up qualifying_party
    if qualifying.lower().strip() in {"qp exempt", "exempt", "nan", "none", ""}:
        qualifying = ""

    owner_name = qualifying
    owner_first_name = ""
    if owner_name:
        parts = re.split(r"\s+", owner_name)
        # Strip Jr/Sr/III
        parts = [p for p in parts if not re.match(r"^(jr|sr|ii|iii|iv)\.?$", p, re.IGNORECASE)]
        if parts:
            owner_first_name = parts[0]

    # Pull cached website + Apollo data from 13_contact_enrichment run
    raw = load_contacts_raw(place_id)
    apollo_block = raw.get("apollo", {}) if raw else {}
    website_block = raw.get("website", {}) if raw else {}
    llm_result = website_block.get("llm_result") or {}

    website_people: list[dict] = llm_result.get("people") or []
    website_general: dict = llm_result.get("general") or {}

    business_email = website_general.get("email") or ""
    business_address = website_general.get("address") or ""

    # Did the website extraction find a name that matches (or contains) the owner?
    has_website_name_match = False
    if owner_name and website_people:
        owner_tokens = {t.lower() for t in re.split(r"\s+", owner_name) if len(t) > 1}
        for p in website_people:
            p_name = str(p.get("name") or "").lower()
            p_tokens = set(re.split(r"\s+", p_name))
            if owner_tokens & p_tokens:
                has_website_name_match = True
                # Also pull email/linkedin from this person if present
                if not business_email and p.get("email"):
                    business_email = p.get("email")
                break

    # Apollo matched people (from 13_contact_enrichment cache)
    apollo_matched = apollo_block.get("matched_people") or []
    apollo_primary_name = ""
    apollo_primary_email = ""
    apollo_primary_title = ""
    apollo_primary_linkedin = ""
    apollo_primary_headline = ""
    if apollo_matched:
        p = apollo_matched[0]
        apollo_primary_name = p.get("name") or ""
        apollo_primary_email = p.get("email") or ""
        apollo_primary_title = p.get("title") or ""
        apollo_primary_linkedin = p.get("linkedin_url") or ""
        apollo_primary_headline = p.get("headline") or ""
        # Prefer Apollo's owner name if we don't have a ROC one
        if not owner_name:
            owner_name = apollo_primary_name
            if apollo_primary_name:
                owner_first_name = apollo_primary_name.split()[0]

    # Email patterns
    email_patterns = generate_email_patterns(owner_name, domain) if domain else []

    # LinkedIn search URL
    linkedin_search_url = (
        build_linkedin_search_url(owner_name, biz) if owner_name else ""
    )

    # Extra website people (beyond the owner)
    extra_website_people = []
    for p in website_people:
        p_name = str(p.get("name") or "").strip()
        if not p_name:
            continue
        # Skip if this IS the owner
        if owner_name and owner_name.lower().split()[0] in p_name.lower():
            continue
        extra_website_people.append({
            "name": p_name,
            "title": p.get("title") or "",
            "email": p.get("email") or "",
        })

    confidence = derive_contact_confidence(
        owner_name,
        bool(business_email),
        has_website_name_match,
        bool(apollo_primary_email),
    )

    how_to_reach = build_how_to_reach(
        owner_name,
        owner_first_name,
        place_phone,
        website,
        business_email,
        email_patterns,
        linkedin_search_url,
        confidence,
    )

    return {
        "license_no": license_no,
        "place_id": place_id,
        "final_rank": int(final_rank) if pd.notna(final_rank) else None,
        "business_name": biz,
        "primary_owner_name": owner_name,
        "primary_owner_first_name": owner_first_name,
        "owner_name_source": "az_roc_qualifying_party" if qualifying else (
            "apollo" if apollo_primary_name else ""
        ),
        "apollo_verified_email": apollo_primary_email,
        "apollo_title": apollo_primary_title,
        "apollo_linkedin": apollo_primary_linkedin,
        "apollo_headline": apollo_primary_headline,
        "booking_phone": place_phone,
        "booking_website": website,
        "business_email": business_email,
        "business_address": business_address,
        "email_patterns": "; ".join(email_patterns),
        "email_pattern_count": len(email_patterns),
        "linkedin_search_url": linkedin_search_url,
        "extra_contacts_json": json.dumps(extra_website_people) if extra_website_people else "",
        "contact_confidence": confidence,
        "how_to_reach": how_to_reach,
    }


def main() -> None:
    if not SCORED_CSV.exists():
        sys.exit(f"missing {SCORED_CSV}")

    df = pd.read_csv(SCORED_CSV)
    top25 = df[df["final_rank"] <= 25].sort_values("final_rank").copy()
    print(f"Augmenting contacts for top {len(top25)} contractors")
    print()

    results = []
    for i, row in enumerate(top25.to_dict(orient="records"), 1):
        metrics = process_contractor(pd.Series(row))
        results.append(metrics)

        name = metrics["primary_owner_name"] or "—"
        phone = metrics["booking_phone"] or "—"
        site = metrics["booking_website"] or "—"
        biz_email = metrics["business_email"] or "—"
        conf = metrics["contact_confidence"]
        patterns_n = metrics["email_pattern_count"]

        print(f"[{i:>2}/{len(top25)}] rank={metrics['final_rank']:<2}  {row['business_name'][:36]:<36}")
        print(f"         owner: {name}")
        print(f"         booking phone: {phone}  |  email: {biz_email}")
        print(f"         booking site:  {site}")
        print(f"         pattern guesses: {patterns_n}  |  confidence: {conf}")
        print()

    out_df = pd.DataFrame(results)

    # Note: Tavily-discovered contacts are NOT merged here anymore. The
    # render layer reads validator decisions directly from
    # data/signals_raw/validator/{place_id}.json at dossier-generation
    # time. That keeps validator output as the single source of truth
    # for which candidates belong to which contractor.

    out_df.to_csv(OUT_CSV, index=False)
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    snap = SNAPSHOT_DIR / f"augmented_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.csv"
    out_df.to_csv(snap, index=False)

    # Summary
    print("=" * 74)
    print("Summary")
    print("=" * 74)
    print(f"Contractors:                           {len(out_df)}")
    print(f"With owner name (ROC qualifying_party): {int(out_df['primary_owner_name'].astype(bool).sum())}")
    print(f"With booking phone:                    {int(out_df['booking_phone'].astype(bool).sum())}")
    print(f"With booking website:                  {int(out_df['booking_website'].astype(bool).sum())}")
    print(f"With business email:                   {int(out_df['business_email'].astype(bool).sum())}")
    print(f"With Apollo-verified email:            {int(out_df['apollo_verified_email'].astype(bool).sum())}")
    print(f"With email pattern guesses:            {int((out_df['email_pattern_count'] > 0).sum())}")
    print(f"With LinkedIn search URL:              {int(out_df['linkedin_search_url'].astype(bool).sum())}")
    print()
    print("Contact confidence distribution:")
    for c, n in out_df["contact_confidence"].value_counts().items():
        print(f"  {c:<10} {n}")
    print()
    print("Outputs:")
    print(f"  {OUT_CSV.relative_to(ROOT)}")
    print(f"  {snap.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
