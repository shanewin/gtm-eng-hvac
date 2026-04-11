#!/usr/bin/env python3
"""
Load the AZ ROC residential and dual contractor CSVs, filter down to
Maricopa County HVAC contractors with 5-25 years of active license history,
dedupe, and drop obvious sole-proprietor name patterns.

Writes:
  data/contractors_filtered.csv              (keepers)
  data/contractors_filtered_soleprop_drops.csv  (dropped for manual review)
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
RESIDENTIAL_CSV = ROOT / "ROC_Posting-List_Residential_2026-04-10.csv"
DUAL_CSV = ROOT / "ROC_Posting-List_Dual_2026-04-10.csv"

DATA_DIR = ROOT / "data" / "01_contractors"
OUT_FILTERED = DATA_DIR / "filtered.csv"
OUT_SOLEPROP = DATA_DIR / "dropped" / "soleprop.csv"

# AZ ROC HVAC classifications (AZ uses R-/CR- prefixes, not CA's C-39).
HVAC_CLASSES = {"R-39", "R-39R", "CR-39"}

MARICOPA_CITIES = {
    c.lower()
    for c in [
        "Phoenix", "Mesa", "Tempe", "Scottsdale", "Glendale", "Chandler",
        "Gilbert", "Peoria", "Surprise", "Avondale", "Goodyear", "Buckeye",
        "Sun City", "Queen Creek", "Fountain Hills", "Paradise Valley",
        "Cave Creek", "Carefree", "Litchfield Park", "Tolleson", "El Mirage",
        "Youngtown", "Guadalupe", "Wickenburg",
    ]
}

SNAPSHOT_DATE = date(2026, 4, 10)
MIN_YEARS = 5
MAX_YEARS = 25

# Real business-entity suffixes. Trade words (HVAC, Air, Heating) are
# deliberately excluded — "Bob Smith HVAC" still looks like a sole prop.
CORP_SUFFIX_RE = re.compile(
    r"\b(inc|incorporated|llc|corp|corporation|co|company|companies|"
    r"ltd|limited|pllc|lp|llp|plc)\b\.?",
    re.IGNORECASE,
)

WORD_RE = re.compile(r"[A-Za-z0-9&]+")

# HVAC trade words used by Rule B of the sole-prop heuristic.
TRADE_WORDS = {
    "hvac", "air", "heating", "cooling", "refrigeration", "mechanical", "ac",
}


def load_roc_csv(path: Path) -> pd.DataFrame:
    """The ROC export has a banner on row 1; real headers are on row 2.
    At least one row has a malformed quote/comma — the python engine plus
    on_bad_lines='skip' handles it cleanly."""
    df = pd.read_csv(
        path, skiprows=1, dtype=str, engine="python", on_bad_lines="skip"
    )
    df = df.rename(columns=lambda c: c.strip().lower().replace(" ", "_"))
    if "#" in df.columns:
        df = df.drop(columns="#")
    for col in df.columns:
        df[col] = df[col].astype("string").str.strip()
    return df


def has_corp_suffix(name: str) -> bool:
    if not isinstance(name, str) or not name:
        return False
    return bool(CORP_SUFFIX_RE.search(name))


def qp_last_name(qp: str | None) -> str | None:
    if not isinstance(qp, str) or not qp:
        return None
    if qp.strip().lower() in {"qp exempt", "exempt"}:
        return None
    parts = [p for p in WORD_RE.findall(qp) if len(p) > 1]
    return parts[-1].lower() if parts else None


def looks_like_sole_prop(business_name: str, qualifying_party: str) -> bool:
    """Combined heuristic:
      Rule A: no corp suffix AND business name contains QP's last name as a word
      Rule B: no corp suffix AND 2-3 words AND last word is a trade word
              (catches 'Bob Smith HVAC' even when QP is exempt)
    """
    if not isinstance(business_name, str) or not business_name:
        return False
    if has_corp_suffix(business_name):
        return False

    words = [w.lower() for w in WORD_RE.findall(business_name)]
    if not words:
        return False

    last = qp_last_name(qualifying_party)
    if last and last in set(words):
        return True

    if 2 <= len(words) <= 3 and words[-1] in TRADE_WORDS:
        return True

    return False


def summary_line(label: str, count: int, prev: int | None = None) -> str:
    if prev is None:
        return f"  {label:<46} {count:>7,}"
    delta = count - prev
    return f"  {label:<46} {count:>7,}  ({delta:+,})"


def main() -> None:
    assert RESIDENTIAL_CSV.exists(), f"missing {RESIDENTIAL_CSV}"
    assert DUAL_CSV.exists(), f"missing {DUAL_CSV}"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUT_SOLEPROP.parent.mkdir(parents=True, exist_ok=True)

    print("Loading ROC exports")
    resid = load_roc_csv(RESIDENTIAL_CSV)
    dual = load_roc_csv(DUAL_CSV)
    print(summary_line("residential rows", len(resid)))
    print(summary_line("dual rows", len(dual)))

    combined = pd.concat([resid, dual], ignore_index=True)
    start = len(combined)
    print()
    print("Filter funnel")
    print(summary_line("combined (resid + dual)", start))

    # Drop exact cross-file duplicates — CR-39 rows appear in both exports.
    prev = len(combined)
    combined = combined.drop_duplicates(
        subset=["license_no", "class"], keep="first"
    )
    print(summary_line("after cross-file dedup (license+class)", len(combined), prev))

    # HVAC classes.
    prev = len(combined)
    hvac = combined[combined["class"].isin(HVAC_CLASSES)].copy()
    print(summary_line(f"HVAC classes {sorted(HVAC_CLASSES)}", len(hvac), prev))

    # AZ state only.
    prev = len(hvac)
    hvac = hvac[hvac["state"].fillna("").str.upper() == "AZ"]
    print(summary_line("AZ mailing address", len(hvac), prev))

    # Maricopa city allowlist (case-insensitive).
    hvac["__city_norm"] = hvac["city"].fillna("").str.strip().str.lower()
    prev = len(hvac)
    hvac = hvac[hvac["__city_norm"].isin(MARICOPA_CITIES)]
    print(summary_line("Maricopa city allowlist", len(hvac), prev))

    # License age window.
    hvac["issued_date_parsed"] = pd.to_datetime(
        hvac["issued_date"], errors="coerce", format="%Y-%m-%d"
    )
    hvac["license_years"] = (
        (pd.Timestamp(SNAPSHOT_DATE) - hvac["issued_date_parsed"]).dt.days
        / 365.25
    )
    prev = len(hvac)
    hvac = hvac[hvac["license_years"].between(MIN_YEARS, MAX_YEARS)]
    print(summary_line(
        f"license age {MIN_YEARS}-{MAX_YEARS} years", len(hvac), prev
    ))

    # Sole-prop drop.
    sole_mask = hvac.apply(
        lambda r: looks_like_sole_prop(
            r.get("business_name") or "", r.get("qualifying_party") or ""
        ),
        axis=1,
    )
    drops = hvac[sole_mask].copy()
    prev = len(hvac)
    hvac = hvac[~sole_mask]
    print(summary_line("sole-prop name drop", len(hvac), prev))

    # Dedup by license number (keep first).
    prev = len(hvac)
    hvac = hvac.drop_duplicates(subset=["license_no"], keep="first")
    print(summary_line("dedup by license_no", len(hvac), prev))

    # Tidy for output.
    hvac["city"] = hvac["city"].str.title()
    hvac = hvac.drop(columns="__city_norm")
    hvac["license_years"] = hvac["license_years"].round(1)

    if "__city_norm" in drops.columns:
        drops = drops.drop(columns="__city_norm")
    drops["city"] = drops["city"].str.title()
    if "license_years" in drops.columns:
        drops["license_years"] = drops["license_years"].round(1)

    hvac.to_csv(OUT_FILTERED, index=False)
    drops.to_csv(OUT_SOLEPROP, index=False)

    print()
    print("Outputs")
    print(f"  {len(hvac):,} rows  -> {OUT_FILTERED.relative_to(ROOT)}")
    print(f"  {len(drops):,} rows  -> {OUT_SOLEPROP.relative_to(ROOT)}  (for review)")


if __name__ == "__main__":
    main()
