#!/usr/bin/env python3
"""
Validation signal #1: FSM + site tooling detection on contractor websites.

Hybrid detector:
  - Custom regex pass for the FSM vendors webanalyze's DB does not cover
    (ServiceTitan, Jobber, FieldEdge, BuildOps, Workiz, Kickserv, mHelpDesk,
    Service Autopilot, Neighborly). Housecall Pro is in both.
  - webanalyze subprocess for broader tech (CMS, site builder, page builder,
    form builders) with version info where available.

Usage:
  python pipeline/03_fsm_detection.py --limit 20   # smoke test
  python pipeline/03_fsm_detection.py              # full run (all 356)
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "data" / "01_contractors" / "tier_1_clean.csv"
ENRICH_DIR = ROOT / "data" / "02_enrichment"
OUT = ENRICH_DIR / "fsm_detection_sample.csv"
OUT_FULL = ENRICH_DIR / "fsm_detection.csv"
ERR_OUT = ENRICH_DIR / "fsm_detection_errors.csv"
SNIPPETS_OUT = ENRICH_DIR / "fsm_detection_phone_only_snippets.txt"

WEBANALYZE_BIN = ROOT / "tools" / "webanalyze" / "webanalyze"
WEBANALYZE_APPS = ROOT / "tools" / "webanalyze" / "technologies.json"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

CANDIDATE_PATHS = [
    "/",
    "/book",
    "/booking",
    "/schedule",
    "/schedule-service",
    "/book-online",
    "/contact",
    "/contact-us",
]

MAX_PAGES = 3
REQ_TIMEOUT = 8
SLEEP_BETWEEN_CONTRACTORS = 0.8
SLEEP_BETWEEN_PAGES = 0.4

SOCIAL_HOSTS = {
    "facebook.com", "m.facebook.com", "instagram.com", "twitter.com",
    "x.com", "yelp.com", "nextdoor.com", "linkedin.com",
}

# ---- Custom FSM + form-builder regex patterns ----
# Neighborly added this session: Aire Serv, One Hour, Benjamin Franklin,
# Mister Sparky etc. are all Neighborly-owned franchises routing through
# their corporate booking system.
FSM_PATTERNS: dict[str, list[str]] = {
    "ServiceTitan": [
        r"servicetitan\.com",
        r"servicetitan\.io",
        r"powered\s+by\s+servicetitan",
    ],
    "Jobber": [
        r"getjobber\.com",
        r"clienthub\.getjobber",
        r"secure\.getjobber",
    ],
    "Housecall Pro": [
        r"housecallpro\.com",
        r"housecall\.pro",
        r"book\.housecallpro",
        r"bookcall\.me",
    ],
    "BuildOps": [r"buildops\.com"],
    "FieldEdge": [r"fieldedge\.com", r"my\.fieldedge"],
    "ServiceFusion": [r"servicefusion\.com"],
    "Workiz": [r"workiz\.com", r"widget\.workiz"],
    "Kickserv": [r"kickserv\.com"],
    "mHelpDesk": [r"mhelpdesk\.com"],
    "Service Autopilot": [r"serviceautopilot\.com"],
    "Neighborly": [
        r"\bneighborly\.com",
        r"aireserv\.com",
        r"onehourheatandair\.com",
        r"benjaminfranklinplumbing\.com",
        r"mistersparky\.com",
        r"mrrooter\.com",
        r"mrelectric\.com",
        r"team-neighborly",
        r"powered\s+by\s+neighborly",
    ],
}

FORM_BUILDER_PATTERNS: dict[str, list[str]] = {
    "JotForm": [r"jotform\.com", r"jotformeu\.com", r"form\.jotform"],
    "Wufoo": [r"wufoo\.com"],
    "Typeform": [r"typeform\.com", r"embed\.typeform"],
    "Formspree": [r"formspree\.io"],
}

# ---- webanalyze category mapping ----
# webanalyze returns category_names like ["CMS", "Blogs"] or
# ["Page builders", "WordPress themes"]. We route each match to one of our
# four enrichment columns based on app_name first (site builders take
# priority over "CMS") then category membership.
SITE_BUILDER_NAMES = {
    "Wix", "GoDaddy Website Builder", "Squarespace", "Weebly",
    "Webflow", "Jimdo", "Site123", "Zyro", "Strikingly",
    "WebsiteBuilder", "Duda", "Tilda",
}
CMS_NAMES = {
    "WordPress", "Drupal", "Joomla", "Ghost", "HubSpot CMS", "Craft CMS",
    "Shopify", "Magento", "PrestaShop", "BigCommerce", "OpenCart",
    "concrete5", "TYPO3",
}
PAGE_BUILDER_NAMES = {
    "Divi", "Elementor", "WPBakery Page Builder", "Beaver Builder",
    "Oxygen", "Brizy", "Bricks", "Thrive Architect",
}
FORM_BUILDER_NAMES_WA = {
    "Gravity Forms", "Typeform", "Jotform", "Wufoo", "WPForms",
    "Ninja Forms", "Contact Form 7", "Formidable Forms", "Formstack",
    "HubSpot Form", "Marketo Forms", "JotForm",
}

PHONE_RE = re.compile(
    r"(?:\(\s*\d{3}\s*\)\s*\d{3}[-.\s]?\d{4}"
    r"|\b\d{3}[-.\s]\d{3}[-.\s]\d{4}\b"
    r"|tel:\+?1?\d{10})",
    re.IGNORECASE,
)

COMPILED_FSM = {
    v: [re.compile(p, re.IGNORECASE) for p in patterns]
    for v, patterns in FSM_PATTERNS.items()
}
COMPILED_FORM = {
    v: [re.compile(p, re.IGNORECASE) for p in patterns]
    for v, patterns in FORM_BUILDER_PATTERNS.items()
}

# Patterns that indicate the origin server is genuinely unreachable,
# not just transiently failing. Used to promote all_pages_failed -> site_dead.
DEAD_ERROR_RE = re.compile(
    r"NameResolutionError|getaddrinfo|Name or service not known"
    r"|HTTP (?:409|520|521|522|523|524)",
    re.IGNORECASE,
)


def strip_query(url: str) -> str:
    p = urlparse(url)
    return urlunparse(p._replace(query="", fragment=""))


def base_scheme_host(url: str) -> str | None:
    p = urlparse(url)
    host = (p.hostname or "").lower()
    if not host:
        return None
    scheme = p.scheme or "https"
    return f"{scheme}://{host}"


def candidate_urls(website: str) -> list[str]:
    first = strip_query(website)
    base = base_scheme_host(website)
    if not base:
        return []
    urls: list[str] = [first]
    for path in CANDIDATE_PATHS:
        u = base + path
        if u not in urls:
            urls.append(u)
    return urls


def fetch(session: requests.Session, url: str) -> dict:
    """Fetch a URL. On SSL failure, retry once with verify=False.

    Returns dict with keys: html, final_url, error, ssl_ok, status_code.
    """
    result = {
        "html": None, "final_url": url, "error": None,
        "ssl_ok": True, "status_code": None,
    }
    try:
        r = session.get(url, timeout=REQ_TIMEOUT, allow_redirects=True)
        result["final_url"] = r.url
        result["status_code"] = r.status_code
        ct = r.headers.get("Content-Type", "").lower()
        if r.status_code == 200 and "text/html" in ct:
            result["html"] = r.text
        else:
            result["error"] = f"HTTP {r.status_code}"
        return result
    except requests.exceptions.SSLError as e:
        result["ssl_ok"] = False
        try:
            r = session.get(
                url, timeout=REQ_TIMEOUT, allow_redirects=True, verify=False
            )
            result["final_url"] = r.url
            result["status_code"] = r.status_code
            ct = r.headers.get("Content-Type", "").lower()
            if r.status_code == 200 and "text/html" in ct:
                result["html"] = r.text
            else:
                result["error"] = f"HTTP {r.status_code}"
            return result
        except requests.RequestException as e2:
            result["error"] = f"ssl retry failed: {str(e2)[:160]}"
            return result
    except requests.RequestException as e:
        result["error"] = str(e)[:180]
        return result


def detect_first_match(
    html: str, compiled: dict[str, list[re.Pattern[str]]]
) -> tuple[str | None, str | None]:
    for vendor, patterns in compiled.items():
        for rx in patterns:
            m = rx.search(html)
            if m:
                return vendor, m.group(0)
    return None, None


def phone_snippet(html: str, span: int = 160) -> str | None:
    m = PHONE_RE.search(html)
    if not m:
        return None
    start = max(0, m.start() - span // 2)
    end = min(len(html), m.end() + span // 2)
    raw = html[start:end]
    clean = re.sub(r"<[^>]+>", " ", raw)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean[:300]


def classify_total_failure(first_error: str | None) -> str:
    if first_error and DEAD_ERROR_RE.search(first_error):
        return "site_dead"
    return "all_pages_failed"


# ---- webanalyze integration ----

def run_webanalyze(urls: list[str]) -> dict[str, list[dict]]:
    """Run the webanalyze binary on a list of URLs.

    Returns {original_url: [match_dict, ...]}. If the binary is missing,
    returns an empty dict (graceful degradation).
    """
    if not WEBANALYZE_BIN.exists() or not WEBANALYZE_APPS.exists():
        print(
            f"  webanalyze not found at {WEBANALYZE_BIN.relative_to(ROOT)} — "
            "skipping enrichment pass"
        )
        return {}

    unique_urls = list(dict.fromkeys(urls))  # preserve order, dedupe
    print(f"  running webanalyze on {len(unique_urls)} URL(s)...")

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", delete=False
    ) as f:
        for u in unique_urls:
            f.write(u + "\n")
        hosts_path = f.name

    try:
        proc = subprocess.run(
            [
                str(WEBANALYZE_BIN),
                "-hosts", hosts_path,
                "-apps", str(WEBANALYZE_APPS),
                "-output", "json",
                "-silent",
                "-redirect",
                "-worker", "4",
                "-search=false",
            ],
            capture_output=True,
            text=True,
            timeout=max(60, len(unique_urls) * 8),
        )
    except subprocess.TimeoutExpired:
        print("  webanalyze timed out — skipping enrichment")
        return {}
    finally:
        Path(hosts_path).unlink(missing_ok=True)

    detections: dict[str, list[dict]] = {}
    for line in (proc.stdout or "").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        host = data.get("hostname", "")
        matches = data.get("matches") or []
        detections[host] = matches

    print(f"  webanalyze returned results for {len(detections)} URL(s)")
    return detections


def categorize_webanalyze(matches: list[dict]) -> dict:
    """Flatten webanalyze matches into our enrichment columns."""
    out = {
        "webanalyze_cms": None,
        "webanalyze_site_builder": None,
        "webanalyze_page_builder": None,
        "webanalyze_form_builders": None,
        "webanalyze_tech_summary": None,
    }
    if not matches:
        return out

    form_builders: list[str] = []
    all_techs: list[str] = []

    for m in matches:
        name = m.get("app_name") or ""
        if not name:
            continue
        version = m.get("version") or ""
        display = f"{name} {version}".strip()
        all_techs.append(display)

        cats = (m.get("app") or {}).get("category_names") or []

        if name in SITE_BUILDER_NAMES:
            if not out["webanalyze_site_builder"]:
                out["webanalyze_site_builder"] = display
        elif name in CMS_NAMES or "CMS" in cats:
            if not out["webanalyze_cms"]:
                out["webanalyze_cms"] = display

        if (
            (name in PAGE_BUILDER_NAMES or "Page builders" in cats)
            and name not in SITE_BUILDER_NAMES
        ):
            if not out["webanalyze_page_builder"]:
                out["webanalyze_page_builder"] = display

        if name in FORM_BUILDER_NAMES_WA or "Form builders" in cats:
            form_builders.append(display)

    if form_builders:
        out["webanalyze_form_builders"] = "; ".join(
            sorted(set(form_builders))
        )
    if all_techs:
        # Cap the summary to avoid massive CSV cells on tech-heavy sites
        summary = sorted(set(all_techs))
        if len(summary) > 20:
            summary = summary[:20] + [f"... (+{len(all_techs) - 20} more)"]
        out["webanalyze_tech_summary"] = "; ".join(summary)

    return out


def run() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument(
        "--skip-webanalyze", action="store_true",
        help="Skip the webanalyze enrichment pass (faster, less signal)",
    )
    args = ap.parse_args()

    if not SRC.exists():
        raise SystemExit(f"missing {SRC}")

    df = pd.read_csv(SRC)

    if args.limit:
        has_site = (
            df[df["place_website"].notna()]
            .sort_values("place_review_count", ascending=False, na_position="last")
            .reset_index(drop=True)
        )
        step = max(1, len(has_site) // args.limit)
        sample = has_site.iloc[::step].head(args.limit).copy()
        out_path = OUT
        print(
            f"Smoke test: {len(sample)} contractors "
            f"(step={step} across {len(has_site)} has-website rows)"
        )
    else:
        sample = df.copy()
        out_path = OUT_FULL
        print(f"Full run: {len(sample)} contractors")

    # Pre-pass: run webanalyze once on all homepages we'll be scraping.
    # This costs one HTTP request per URL from webanalyze (in addition to
    # ours), but it's the cleanest integration — webanalyze owns its own
    # fetching + fingerprinting and we just consume its JSON output.
    webanalyze_urls: list[str] = []
    for _, row in sample.iterrows():
        w = row.get("place_website")
        if pd.isna(w) or not str(w).strip():
            continue
        host = (urlparse(str(w)).hostname or "").lower().removeprefix("www.")
        if host in SOCIAL_HOSTS:
            continue
        webanalyze_urls.append(strip_query(str(w)))

    wa_detections: dict[str, list[dict]] = {}
    if not args.skip_webanalyze and webanalyze_urls:
        wa_detections = run_webanalyze(webanalyze_urls)

    session = requests.Session()
    session.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })

    results: list[dict] = []
    errors: list[dict] = []
    phone_only_snippets: list[tuple[str, str, str]] = []

    N = len(sample)
    for i, row in enumerate(sample.itertuples(index=False), start=1):
        biz = getattr(row, "business_name", "") or ""
        website = getattr(row, "place_website", None)
        review_count = getattr(row, "place_review_count", None)
        city = getattr(row, "city", "") or ""
        license_no = getattr(row, "license_no", None)

        result = {
            "license_no": license_no,
            "business_name": biz,
            "city": city,
            "place_review_count": review_count,
            "place_website": website,
            "detected_fsm": None,
            "detected_form_builder": None,
            "has_any_booking_tool": False,
            "phone_only": False,
            "detection_evidence": None,
            "detection_source_url": None,
            "webanalyze_cms": None,
            "webanalyze_site_builder": None,
            "webanalyze_page_builder": None,
            "webanalyze_form_builders": None,
            "webanalyze_tech_summary": None,
            "pages_fetched": 0,
            "probe_404_count": 0,
            "fetch_errors": 0,
            "ssl_ok": True,
            "status": None,
        }

        if pd.isna(website) or not str(website).strip():
            result["status"] = "no_website"
            results.append(result)
            print(f"  [{i:>2}/{N}] no_website            {biz}")
            continue

        host = (urlparse(website).hostname or "").lower().removeprefix("www.")
        if host in SOCIAL_HOSTS:
            result["status"] = f"social_only:{host}"
            results.append(result)
            print(f"  [{i:>2}/{N}] social_only ({host})  {biz}")
            time.sleep(SLEEP_BETWEEN_CONTRACTORS)
            continue

        # Merge webanalyze enrichment (done in pre-pass).
        wa_lookup = strip_query(str(website))
        wa_cats = categorize_webanalyze(wa_detections.get(wa_lookup, []))
        result.update(wa_cats)

        # Custom regex detection on pages we fetch ourselves.
        urls = candidate_urls(website)
        fetched: list[tuple[str, str]] = []
        first_error: str | None = None

        for u in urls:
            if len(fetched) >= MAX_PAGES:
                break
            fr = fetch(session, u)
            if fr["html"]:
                fetched.append((fr["final_url"], fr["html"]))
            else:
                err = fr["error"] or "unknown"
                if first_error is None:
                    first_error = err
                if err == "HTTP 404":
                    result["probe_404_count"] += 1
                else:
                    result["fetch_errors"] += 1
                    errors.append({
                        "business_name": biz,
                        "url": u,
                        "error": err,
                    })
            if not fr["ssl_ok"]:
                result["ssl_ok"] = False
            time.sleep(SLEEP_BETWEEN_PAGES)

        result["pages_fetched"] = len(fetched)

        if not fetched:
            result["status"] = classify_total_failure(first_error)
            results.append(result)
            print(f"  [{i:>2}/{N}] {result['status']:<20} {biz}")
            time.sleep(SLEEP_BETWEEN_CONTRACTORS)
            continue

        # FSM detection
        for url, html in fetched:
            vendor, evidence = detect_first_match(html, COMPILED_FSM)
            if vendor:
                result["detected_fsm"] = vendor
                result["detection_evidence"] = evidence
                result["detection_source_url"] = url
                break

        # Form builder detection (regex list — webanalyze covers the rest)
        for url, html in fetched:
            vendor, evidence = detect_first_match(html, COMPILED_FORM)
            if vendor:
                result["detected_form_builder"] = vendor
                if result["detection_evidence"] is None:
                    result["detection_evidence"] = evidence
                    result["detection_source_url"] = url
                break

        # has_any_booking_tool is the UNION of regex FSM + regex form builder
        # + webanalyze-detected form builder. Site builders and CMS don't
        # count — they're enrichment, not booking tools.
        result["has_any_booking_tool"] = bool(
            result["detected_fsm"]
            or result["detected_form_builder"]
            or result["webanalyze_form_builders"]
        )

        if not result["has_any_booking_tool"]:
            for url, html in fetched:
                snip = phone_snippet(html)
                if snip:
                    result["phone_only"] = True
                    phone_only_snippets.append((biz, url, snip))
                    break

        result["status"] = "ok"
        results.append(result)

        marker_parts = []
        if result["detected_fsm"]:
            marker_parts.append(f"fsm:{result['detected_fsm']}")
        if result["detected_form_builder"] or result["webanalyze_form_builders"]:
            marker_parts.append("form")
        if result["webanalyze_site_builder"]:
            marker_parts.append(f"sb:{result['webanalyze_site_builder'].split()[0]}")
        if not marker_parts:
            marker_parts.append("phone_only" if result["phone_only"] else "no_tool")
        marker = ",".join(marker_parts)

        ssl_flag = "" if result["ssl_ok"] else " ssl-fallback"
        print(
            f"  [{i:>2}/{N}] {marker:<26} {biz}  "
            f"(p={result['pages_fetched']}, 404={result['probe_404_count']}, "
            f"err={result['fetch_errors']}{ssl_flag})"
        )

        time.sleep(SLEEP_BETWEEN_CONTRACTORS)

    out_df = pd.DataFrame(results)
    out_df.to_csv(out_path, index=False)

    if errors:
        pd.DataFrame(errors).to_csv(ERR_OUT, index=False)

    if phone_only_snippets:
        with SNIPPETS_OUT.open("w") as f:
            for biz, url, snip in phone_only_snippets:
                f.write(f"=== {biz}\n{url}\n{snip}\n\n")

    # ---- Summary ----
    print()
    print("=" * 72)
    print("Summary")
    print("=" * 72)
    print(f"  total:                 {len(out_df)}")

    print()
    print("Status breakdown:")
    for s, c in out_df["status"].value_counts(dropna=False).items():
        print(f"  {str(s):<22} {c}")

    print()
    print("Regex FSM detection:")
    fsm_hits = out_df["detected_fsm"].notna().sum()
    print(f"  detected_fsm (any):    {fsm_hits}")
    if fsm_hits:
        for v, c in out_df["detected_fsm"].value_counts().items():
            print(f"    {v:<22} {c}")

    print()
    print("Form builder (regex + webanalyze union):")
    regex_form = out_df["detected_form_builder"].notna().sum()
    wa_form = out_df["webanalyze_form_builders"].notna().sum()
    print(f"  regex hits:            {regex_form}")
    print(f"  webanalyze hits:       {wa_form}")
    if wa_form:
        for v, c in out_df["webanalyze_form_builders"].value_counts().items():
            print(f"    {v[:50]:<50} {c}")

    print()
    print("webanalyze enrichment (site tooling):")
    sb = out_df["webanalyze_site_builder"].notna().sum()
    cms = out_df["webanalyze_cms"].notna().sum()
    pb = out_df["webanalyze_page_builder"].notna().sum()
    print(f"  site builder:          {sb}")
    if sb:
        for v, c in out_df["webanalyze_site_builder"].value_counts().items():
            print(f"    {v[:40]:<40} {c}")
    print(f"  CMS:                   {cms}")
    if cms:
        for v, c in out_df["webanalyze_cms"].value_counts().items():
            print(f"    {v[:40]:<40} {c}")
    print(f"  page builder:          {pb}")
    if pb:
        for v, c in out_df["webanalyze_page_builder"].value_counts().items():
            print(f"    {v[:40]:<40} {c}")

    print()
    print("Classification outcomes:")
    any_tool = int(out_df["has_any_booking_tool"].sum())
    phone_only = int(out_df["phone_only"].sum())
    no_tool = int(((out_df["status"] == "ok")
                   & (~out_df["has_any_booking_tool"])
                   & (~out_df["phone_only"])).sum())
    print(f"  has_any_booking_tool:  {any_tool}")
    print(f"  phone_only:            {phone_only}")
    print(f"  ok but no tool/phone:  {no_tool}")

    print()
    print("Fetch health:")
    print(f"  real fetch errors:     {len(errors)}")
    print(f"  probe 404s (expected): {int(out_df['probe_404_count'].sum())}")
    print(f"  ssl fallback used:     {int((~out_df['ssl_ok']).sum())}")

    print()
    print(f"  output:   {out_path.relative_to(ROOT)}")
    if errors:
        print(f"  errors:   {ERR_OUT.relative_to(ROOT)}")
    if phone_only_snippets:
        print(f"  snippets: {SNIPPETS_OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    run()
