#!/usr/bin/env python3
"""
One-time cleanup of data/contractors_tier_1_enterprise.csv:
  1. Bounding box filter (drop rows whose Google coords are outside Maricopa).
  2. Dedup by place_id (multiple ROC licenses → same Google listing).
  3. Drop the all-null place_error column.
  4. Add manual_review_flag column; flag Pro Tech HVAC LLC if present.

Writes:
  data/contractors_tier_1_clean.csv             (new downstream input)
  data/contractors_tier_1_geo_dropped.csv       (for audit)
  data/contractors_tier_1_dedup_dropped.csv     (for audit)

Does NOT modify contractors_tier_1_enterprise.csv.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
CONTRACTORS_DIR = ROOT / "data" / "01_contractors"
SRC = CONTRACTORS_DIR / "tier_1_enterprise.csv"
OUT_CLEAN = CONTRACTORS_DIR / "tier_1_clean.csv"
OUT_GEO_DROPPED = CONTRACTORS_DIR / "dropped" / "geo.csv"
OUT_DEDUP_DROPPED = CONTRACTORS_DIR / "dropped" / "dedup.csv"

# Maricopa County bounding box. Slightly generous superset of the actual
# county bounds (~32.50–34.05 lat, -113.35 to -111.03 lon). All 45 outliers
# observed in the enrichment data are clearly out-of-state, so a loose box
# is fine.
LAT_LO, LAT_HI = 32.5, 34.1
LON_LO, LON_HI = -113.4, -111.0

PRO_TECH_NAME = "Pro Tech HVAC LLC"
PRO_TECH_FLAG = "outlier: high review count + low rating"


def main() -> None:
    if not SRC.exists():
        raise SystemExit(f"missing {SRC}")

    df = pd.read_csv(SRC)
    n_start = len(df)
    print(f"Loaded {n_start} rows from {SRC.relative_to(ROOT)}")

    # ---- Step 1: Bounding box filter ----
    lat = pd.to_numeric(df["place_latitude"], errors="coerce")
    lon = pd.to_numeric(df["place_longitude"], errors="coerce")
    in_box = lat.between(LAT_LO, LAT_HI) & lon.between(LON_LO, LON_HI)

    geo_dropped = df[~in_box].copy()
    df = df[in_box].copy()
    geo_dropped.to_csv(OUT_GEO_DROPPED, index=False)
    print(
        f"Bounding box filter: dropped {len(geo_dropped)}  ->  "
        f"{len(df)} remaining"
    )

    # ---- Step 2: Dedup by place_id with 3-step tiebreaker ----
    # (1) higher place_match_confidence
    # (2) higher place_review_count
    # (3) older issued_date  (structurally-tied conf+reviews are expected
    #     for place_id duplicates — same Google listing, same review count)
    df["_issued_ts"] = pd.to_datetime(df["issued_date"], errors="coerce")
    df_sorted = df.sort_values(
        by=["place_match_confidence", "place_review_count", "_issued_ts"],
        ascending=[False, False, True],
        kind="mergesort",  # stable
        na_position="last",
    )

    dup_mask = df_sorted["place_id"].duplicated(keep="first")
    dedup_dropped = df_sorted[dup_mask].copy()
    df = df_sorted[~dup_mask].copy()

    df = df.drop(columns="_issued_ts")
    if "_issued_ts" in dedup_dropped.columns:
        dedup_dropped = dedup_dropped.drop(columns="_issued_ts")

    dedup_dropped.to_csv(OUT_DEDUP_DROPPED, index=False)
    print(
        f"place_id dedup:     dropped {len(dedup_dropped):>3}  ->  "
        f"{len(df)} remaining"
    )

    if len(dedup_dropped):
        print()
        print("Dedup decisions:")
        for pid in dedup_dropped["place_id"].unique():
            kept_row = df[df["place_id"] == pid]
            dropped_rows = dedup_dropped[dedup_dropped["place_id"] == pid]
            place_name = (
                kept_row["place_name"].iloc[0]
                if len(kept_row) else "(none)"
            )
            print(f"  place_id={pid}  ({place_name})")
            for _, r in kept_row.iterrows():
                print(
                    f"    KEEP  license={r['license_no']:<7}  "
                    f"{str(r['business_name'])[:44]:<44}  "
                    f"conf={r['place_match_confidence']:>5.0f}  "
                    f"reviews={r['place_review_count']}  "
                    f"issued={r['issued_date']}"
                )
            for _, r in dropped_rows.iterrows():
                print(
                    f"    drop  license={r['license_no']:<7}  "
                    f"{str(r['business_name'])[:44]:<44}  "
                    f"conf={r['place_match_confidence']:>5.0f}  "
                    f"reviews={r['place_review_count']}  "
                    f"issued={r['issued_date']}"
                )
        print()

    # ---- Step 3: Drop all-null place_error column ----
    if "place_error" in df.columns:
        null_pct = df["place_error"].isna().mean() * 100
        if null_pct == 100.0:
            df = df.drop(columns="place_error")
            print("Dropped column: place_error (100% null)")
        else:
            print(
                f"NOT dropping place_error — it's only {100 - null_pct:.1f}% "
                f"populated now, no longer fully null"
            )

    # ---- Step 4: manual_review_flag ----
    df["manual_review_flag"] = ""

    pro_tech_mask = df["business_name"] == PRO_TECH_NAME
    if pro_tech_mask.any():
        df.loc[pro_tech_mask, "manual_review_flag"] = PRO_TECH_FLAG
        print(f"Flagged: {PRO_TECH_NAME} -> '{PRO_TECH_FLAG}'")
    else:
        in_geo = (geo_dropped["business_name"] == PRO_TECH_NAME).any()
        in_dedup = (dedup_dropped["business_name"] == PRO_TECH_NAME).any()
        reason = (
            "bounding box filter" if in_geo
            else "place_id dedup" if in_dedup
            else "not found in source"
        )
        print()
        print(f"!! WARNING: {PRO_TECH_NAME} was not in the cleaned pool.")
        print(f"   Reason: removed by {reason}.")
        if in_geo:
            print(
                f"   Its Google coordinates placed it outside Maricopa "
                f"(Santa Rosa, CA area)."
            )
            print(
                f"   See {OUT_GEO_DROPPED.relative_to(ROOT)} to review "
                f"and recover manually if needed."
            )
        print(f"   manual_review_flag was NOT applied.")

    # ---- Write output ----
    df.to_csv(OUT_CLEAN, index=False)

    # ---- Summary ----
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  starting rows:        {n_start}")
    print(f"  dropped (geo):        {len(geo_dropped)}")
    print(f"  dropped (dedup):      {len(dedup_dropped)}")
    print(f"  final rows:           {len(df)}")
    print()
    print("Outputs")
    print(f"  clean:         {OUT_CLEAN.relative_to(ROOT)}")
    print(f"  geo dropped:   {OUT_GEO_DROPPED.relative_to(ROOT)}")
    print(f"  dedup dropped: {OUT_DEDUP_DROPPED.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
