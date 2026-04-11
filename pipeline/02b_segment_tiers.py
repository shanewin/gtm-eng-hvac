#!/usr/bin/env python3
"""
One-time segmentation: split data/contractors_enriched.csv into four tier
CSVs based on place_match_confidence and place_business_status.

Does not overwrite contractors_enriched.csv. Makes no API calls.

Tiers:
  1 - Enterprise prospects       (conf >= 85 AND OPERATIONAL)
  2 - Borderline, review needed  (70 <= conf < 85 AND OPERATIONAL)
  3 - Small shops / no GBP       (unmatched — Google returned nothing)
  4 - Closed / clearly wrong     (conf < 70 OR CLOSED_PERMANENTLY)

Any row that does not fall into one of the four tiers is printed as an edge
case and written to data/contractors_tier_unclassified.csv for review.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
CONTRACTORS_DIR = ROOT / "data" / "01_contractors"
ENRICHED_CSV = CONTRACTORS_DIR / "enriched.csv"

TIER_FILES = {
    1: CONTRACTORS_DIR / "tier_1_enterprise.csv",
    2: CONTRACTORS_DIR / "tier_2_borderline.csv",
    3: CONTRACTORS_DIR / "tier_3_small_shops.csv",
    4: CONTRACTORS_DIR / "tier_4_rejects.csv",
}
UNCLASSIFIED_CSV = CONTRACTORS_DIR / "tier_unclassified.csv"

TIER_LABELS = {
    1: "Enterprise prospects",
    2: "Borderline, review needed",
    3: "Small shops / no GBP",
    4: "Closed or clearly wrong match",
}

CONFIDENCE_T1 = 85
CONFIDENCE_T2_LOW = 70


def coerce_bool(x) -> bool:
    """CSV roundtrip turns True/False into strings. Handle both."""
    if isinstance(x, bool):
        return x
    if isinstance(x, str):
        return x.strip().lower() == "true"
    return False


def assign_tier(row: pd.Series) -> int | None:
    matched = coerce_bool(row.get("place_match"))
    conf = row.get("place_match_confidence")
    status = row.get("place_business_status")

    # Tier 3: Google returned nothing at all.
    if not matched:
        return 3

    # Matched rows should always have a confidence score. If one doesn't,
    # that's an edge case — fall through to unclassified.
    if pd.isna(conf):
        return None

    # Tier 4: bad match OR permanently closed — rejects regardless of the
    # other field. Checked before T1/T2 so closed-but-high-confidence rows
    # (e.g. Morales Air at 100) don't end up as "enterprise prospects".
    if conf < CONFIDENCE_T2_LOW or status == "CLOSED_PERMANENTLY":
        return 4

    # T1 and T2 both require OPERATIONAL. Anything else (CLOSED_TEMPORARILY,
    # BUSINESS_STATUS_UNSPECIFIED, null) is an edge case we want to surface,
    # not silently bucket.
    if status != "OPERATIONAL":
        return None

    if conf >= CONFIDENCE_T1:
        return 1
    return 2  # 70 <= conf < 85


def main() -> None:
    if not ENRICHED_CSV.exists():
        raise SystemExit(
            f"missing {ENRICHED_CSV} — run 02_enrich_places.py first"
        )

    df = pd.read_csv(ENRICHED_CSV)
    print(f"Loaded {len(df):,} rows from {ENRICHED_CSV.relative_to(ROOT)}")
    print()

    df["__tier"] = df.apply(assign_tier, axis=1)

    tier_counts: dict[int, int] = {}
    for tier, path in TIER_FILES.items():
        subset = df[df["__tier"] == tier].drop(columns="__tier")
        subset.to_csv(path, index=False)
        tier_counts[tier] = len(subset)

    unclassified = df[df["__tier"].isna()].drop(columns="__tier")

    # ---- Summary ----
    total_classified = sum(tier_counts.values())
    print("Tier counts")
    print(f"{'Tier':<6}{'Rows':>7}   Description")
    for tier in sorted(TIER_FILES):
        print(
            f"  {tier:<4}{tier_counts[tier]:>7}   {TIER_LABELS[tier]}"
        )
    print(f"  {'sum':<4}{total_classified:>7}")
    print(f"  {'src':<4}{len(df):>7}   (contractors_enriched.csv)")

    if total_classified == len(df) and len(unclassified) == 0:
        print("  ✓ all rows classified; sum matches source")
    else:
        print(f"  !! {len(unclassified)} row(s) unclassified — written to "
              f"{UNCLASSIFIED_CSV.relative_to(ROOT)}")
        unclassified.to_csv(UNCLASSIFIED_CSV, index=False)
        print()
        print("Edge-case rows:")
        cols = [
            "license_no", "business_name", "city",
            "place_match", "place_match_confidence", "place_business_status",
        ]
        available = [c for c in cols if c in unclassified.columns]
        print(unclassified[available].to_string(index=False))

    print()
    print("Files written")
    for tier, path in TIER_FILES.items():
        print(f"  tier {tier}: {path.relative_to(ROOT)}")

    # ---- Top 5 sanity check for each tier ----
    preview_cols = [
        "business_name", "doing_business_as", "city",
        "place_name", "place_match_confidence",
        "place_review_count", "place_business_status",
    ]
    for tier in sorted(TIER_FILES):
        path = TIER_FILES[tier]
        print()
        print("=" * 78)
        print(f"TIER {tier} — {TIER_LABELS[tier]}  ({tier_counts[tier]} rows)")
        print("=" * 78)
        if tier_counts[tier] == 0:
            print("  (empty)")
            continue
        sub = pd.read_csv(path)
        available = [c for c in preview_cols if c in sub.columns]
        with pd.option_context(
            "display.max_colwidth", 42, "display.width", 220
        ):
            print(sub[available].head(5).to_string(index=False))


if __name__ == "__main__":
    main()
