#!/usr/bin/env python3
"""
Rank Phoenix HVAC contractors as "hidden gems" — too small to be in
ServiceTitan-class SDRs' CRM yet, but big enough to be real businesses
worth outreach.

Strict exclusions (never eligible):
  - Apollo revenue >= $10M                 (known account)
  - Apollo-tracked PE/parent ownership     (centralized procurement)
  - place_review_count > 1000              (too visible)
  - detected_fsm is not null               (already on a competing FSM,
    including Apollo tech-stack confirmations)

Inclusion criteria (strict version):
  - phone_only OR form-builder-only (no FSM)
  - 50 <= review_count <= 500
  - 5 <= license_years <= 20
  - apollo_has_parent_company == False (covered by exclusion above,
    belt-and-suspenders)

If the strict pool has fewer than 25 rows, loosen filters one at a time
in a defined order until the pool contains ~30-40 candidates.

Final rank: review_count descending within the filtered pool.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
TIER1 = ROOT / "data" / "01_contractors" / "tier_1_clean.csv"
FSM = ROOT / "data" / "02_enrichment" / "fsm_detection.csv"
APOLLO = ROOT / "data" / "02_enrichment" / "apollo_signals.csv"
OUT = ROOT / "data" / "03_hidden_gems" / "top_25.csv"
OUT_FULL_POOL = ROOT / "data" / "03_hidden_gems" / "filtered_pool.csv"

TARGET_POOL_MIN = 30
TARGET_POOL_MAX = 40
TOP_N = 25


def coerce_bool(x) -> bool:
    if isinstance(x, bool):
        return x
    if pd.isna(x):
        return False
    if isinstance(x, str):
        return x.strip().lower() in {"true", "1", "yes"}
    return bool(x)


def load_merged() -> pd.DataFrame:
    tier1 = pd.read_csv(TIER1)
    fsm = pd.read_csv(FSM)
    apollo = pd.read_csv(APOLLO)

    # De-duplicate join cols: fsm and apollo both carry business_name, city,
    # review_count, website copies. Keep only license_no + new columns.
    fsm_keep = [c for c in fsm.columns if c == "license_no" or c not in tier1.columns]
    apollo_keep = [c for c in apollo.columns if c == "license_no" or c not in tier1.columns]

    merged = tier1.merge(fsm[fsm_keep], on="license_no", how="left")
    merged = merged.merge(apollo[apollo_keep], on="license_no", how="left")

    # Normalize bool-ish cols
    for c in ["phone_only", "has_any_booking_tool", "apollo_found",
              "apollo_has_parent_company", "apollo_hiring_ops_pain",
              "apollo_hiring_capacity", "apollo_any_hiring_signal"]:
        if c in merged.columns:
            merged[c] = merged[c].map(coerce_bool)

    return merged


def apply_exclusions(df: pd.DataFrame) -> tuple[pd.DataFrame, list[tuple[str, int]]]:
    """Drop rows that are never eligible. Return (survivors, drop_log)."""
    log = []
    n0 = len(df)

    # 1. Apollo revenue >= 10M
    mask_big_rev = df["apollo_revenue_usd"].fillna(0) >= 10_000_000
    log.append(("Apollo revenue >= $10M", int(mask_big_rev.sum())))
    df = df[~mask_big_rev]

    # 2. PE-owned per Apollo
    mask_pe = df["apollo_has_parent_company"] == True
    log.append(("Apollo PE / parent-owned", int(mask_pe.sum())))
    df = df[~mask_pe]

    # 3. > 1000 reviews
    mask_huge = pd.to_numeric(df["place_review_count"], errors="coerce").fillna(0) > 1000
    log.append(("> 1000 reviews", int(mask_huge.sum())))
    df = df[~mask_huge]

    # 4. Already on competing FSM (regex-detected OR Apollo tech stack)
    regex_fsm = df["detected_fsm"].notna()
    apollo_fsm = df["apollo_fsm_in_stack"].notna() if "apollo_fsm_in_stack" in df.columns else pd.Series(False, index=df.index)
    mask_fsm = regex_fsm | apollo_fsm
    log.append(("already on competing FSM", int(mask_fsm.sum())))
    df = df[~mask_fsm]

    return df, log


def apply_inclusions(
    df: pd.DataFrame,
    review_min: int,
    review_max: int,
    years_min: float,
    years_max: float,
) -> tuple[pd.DataFrame, list[tuple[str, int]]]:
    """Apply the positive filters that define the ICP."""
    log = []
    n = len(df)
    log.append(("start (post-exclusions)", n))

    # phone_only OR form-builder-only
    form_only = df["detected_form_builder"].notna()
    mask_tool = (df["phone_only"] == True) | form_only
    df = df[mask_tool]
    log.append((f"phone_only OR form_builder (after)", len(df)))

    # review count window
    rc = pd.to_numeric(df["place_review_count"], errors="coerce")
    df = df[rc.between(review_min, review_max)]
    log.append((f"reviews in [{review_min}, {review_max}]", len(df)))

    # license age window
    ly = pd.to_numeric(df["license_years"], errors="coerce")
    df = df[ly.between(years_min, years_max)]
    log.append((f"license years in [{years_min}, {years_max}]", len(df)))

    return df, log


def main() -> None:
    merged = load_merged()
    print(f"Merged input rows: {len(merged)}")
    print()

    survivors, drop_log = apply_exclusions(merged)
    print("Exclusion funnel:")
    print(f"  start:                                 {len(merged)}")
    for label, dropped in drop_log:
        print(f"  -- {label}: -{dropped}")
    print(f"  after exclusions:                      {len(survivors)}")
    print()

    # Strict inclusion parameters
    configs = [
        ("STRICT",       50, 500,  5.0, 20.0),
        ("LOOSEN 1: reviews 40-600",   40, 600,  5.0, 20.0),
        ("LOOSEN 2: + years 4-22",     40, 600,  4.0, 22.0),
        ("LOOSEN 3: + reviews 30-700", 30, 700,  4.0, 22.0),
        ("LOOSEN 4: + reviews 20-900", 20, 900,  4.0, 22.0),
    ]

    final_df = None
    final_label = None
    final_log = None
    for label, rmin, rmax, ymin, ymax in configs:
        filtered, log = apply_inclusions(survivors, rmin, rmax, ymin, ymax)
        n = len(filtered)
        print(f"{label}:")
        for step_label, step_n in log:
            print(f"  {step_label:<45} {step_n}")
        print(f"  -> pool size: {n}")
        print()

        if n >= TARGET_POOL_MIN and n <= TARGET_POOL_MAX * 2:
            final_df = filtered
            final_label = label
            final_log = log
            break
        if n >= TARGET_POOL_MIN:
            # Between MIN and 2*MAX — accept but keep label
            final_df = filtered
            final_label = label
            final_log = log
            break
        if label == "STRICT" and n >= TOP_N:
            # Strict already has enough for top 25 — accept
            final_df = filtered
            final_label = label
            final_log = log
            break

    if final_df is None:
        print("!! No config produced >= TOP_N candidates. Using most-loosened config.")
        final_df, log = apply_inclusions(survivors, 20, 900, 4.0, 22.0)
        final_label = "MAX-LOOSENED"

    # Rank by review count desc within the filtered pool
    ranked = final_df.copy()
    ranked["place_review_count_num"] = pd.to_numeric(
        ranked["place_review_count"], errors="coerce"
    )
    ranked = ranked.sort_values(
        "place_review_count_num", ascending=False, na_position="last"
    ).reset_index(drop=True)
    ranked["rank"] = ranked.index + 1

    # Save full filtered pool AND top-25 slice
    ranked.to_csv(OUT_FULL_POOL, index=False)
    top25 = ranked.head(TOP_N)
    top25.to_csv(OUT, index=False)

    print("=" * 72)
    print(f"Final filter config: {final_label}")
    print(f"Filtered pool:       {len(ranked)}")
    print(f"Top {TOP_N} saved:        {len(top25)}")
    print("=" * 72)
    print()
    print("Top 25 hidden gems (by review count desc):")
    cols = [
        "rank", "business_name", "city", "place_review_count_num",
        "place_rating", "license_years", "detected_form_builder",
        "webanalyze_site_builder", "webanalyze_cms",
    ]
    available = [c for c in cols if c in top25.columns]
    with pd.option_context("display.max_colwidth", 40, "display.width", 200):
        print(top25[available].to_string(index=False))

    print()
    print(f"Outputs:")
    print(f"  top 25:        {OUT.relative_to(ROOT)}")
    print(f"  full pool:     {OUT_FULL_POOL.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
