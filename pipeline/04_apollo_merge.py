#!/usr/bin/env python3
"""
Merge Apollo enrichment + hiring-signal data into contractors_tier_1_clean.csv.
One-shot: reads data/apollo_raw/apollo_results_2026-04-10.json and writes
the combined output to data/apollo_signals.csv +
data/apollo_snapshots/2026-04-10.csv.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "data" / "01_contractors" / "tier_1_clean.csv"
APOLLO_JSON = ROOT / "data" / "signals_raw" / "apollo" / "apollo_results_2026-04-10.json"
OUT_CURRENT = ROOT / "data" / "02_enrichment" / "apollo_signals.csv"
OUT_SNAPSHOT_DIR = ROOT / "data" / "snapshots" / "apollo"


def domain_from_url(url) -> str:
    if url is None or (isinstance(url, float) and pd.isna(url)):
        return ""
    return (urlparse(str(url)).hostname or "").lower().removeprefix("www.")


def main() -> None:
    df = pd.read_csv(SRC)
    df["domain"] = df["place_website"].map(domain_from_url)

    data = json.loads(APOLLO_JSON.read_text())
    orgs = data["organizations"]
    ops_pain_set = set(data["hiring_ops_pain_domains"])
    cap_set = set(data["hiring_capacity_domains"])

    rows = []
    for _, r in df.iterrows():
        dom = r["domain"]
        org = orgs.get(dom)

        out = {
            "license_no": r["license_no"],
            "business_name": r["business_name"],
            "city": r["city"],
            "place_review_count": r.get("place_review_count"),
            "place_website": r.get("place_website"),
            "domain": dom,
            "apollo_found": False,
            "apollo_data_quality": "absent",
            "apollo_id": None,
            "apollo_name": None,
            "apollo_revenue_usd": None,
            "apollo_revenue_printed": None,
            "apollo_employees": None,
            "apollo_founded_year": None,
            "apollo_linkedin_url": None,
            "apollo_parent_name": None,
            "apollo_parent_website": None,
            "apollo_has_parent_company": False,
            "apollo_recent_acquisition_date": None,
            "apollo_hc_growth_6mo": None,
            "apollo_hc_growth_12mo": None,
            "apollo_hc_growth_24mo": None,
            "apollo_fsm_in_stack": None,
            "apollo_has_gravity_forms": False,
            "apollo_has_callrail": False,
            "apollo_hiring_ops_pain": dom in ops_pain_set,
            "apollo_hiring_capacity": dom in cap_set,
            "apollo_any_hiring_signal": (dom in ops_pain_set) or (dom in cap_set),
        }

        if org:
            out["apollo_found"] = True
            out["apollo_id"] = org.get("id")
            out["apollo_name"] = org.get("name")
            rev = org.get("revenue")
            out["apollo_revenue_usd"] = rev if rev else None
            if rev:
                out["apollo_revenue_printed"] = f"${rev/1_000_000:.1f}M"
            out["apollo_employees"] = org.get("employees")
            out["apollo_founded_year"] = org.get("founded")
            out["apollo_linkedin_url"] = org.get("linkedin")
            parent = org.get("parent")
            if parent:
                out["apollo_parent_name"] = parent
                out["apollo_parent_website"] = org.get("parent_website")
                out["apollo_has_parent_company"] = True
            out["apollo_recent_acquisition_date"] = org.get("recent_acquisition")
            out["apollo_hc_growth_6mo"] = org.get("hc_6mo")
            out["apollo_hc_growth_12mo"] = org.get("hc_12mo")
            out["apollo_hc_growth_24mo"] = org.get("hc_24mo")
            out["apollo_fsm_in_stack"] = org.get("fsm_in_stack")
            out["apollo_has_gravity_forms"] = bool(org.get("has_gravity_forms"))
            out["apollo_has_callrail"] = bool(org.get("has_callrail"))

            # Data quality classification
            has_rich_data = (
                org.get("revenue") or org.get("employees") or org.get("hc_6mo") is not None
            )
            out["apollo_data_quality"] = "rich" if has_rich_data else "skeleton"

        rows.append(out)

    merged = pd.DataFrame(rows)

    OUT_SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    snapshot_path = OUT_SNAPSHOT_DIR / f"{date.today().isoformat()}.csv"
    merged.to_csv(snapshot_path, index=False)
    merged.to_csv(OUT_CURRENT, index=False)

    # ---- Summary ----
    n = len(merged)
    print(f"Total rows: {n}")
    print()
    print("Apollo coverage:")
    print(f"  apollo_found:            {int(merged['apollo_found'].sum())}")
    print(f"  apollo_data_quality:")
    for k, v in merged["apollo_data_quality"].value_counts().items():
        print(f"    {k:<12} {v}")
    print()
    print("Ownership (top signal):")
    pe = merged[merged["apollo_has_parent_company"] == True]
    print(f"  parent-owned:            {len(pe)}")
    if len(pe):
        for _, r in pe.iterrows():
            print(f"    {r['business_name']:<42} -> {r['apollo_parent_name']}")
    print()
    print("Hiring signals:")
    print(f"  ops_pain:                {int(merged['apollo_hiring_ops_pain'].sum())}")
    print(f"  capacity:                {int(merged['apollo_hiring_capacity'].sum())}")
    print(f"  any hiring signal:       {int(merged['apollo_any_hiring_signal'].sum())}")
    print()
    print("FSM confirmations via Apollo tech stack:")
    fsm = merged[merged["apollo_fsm_in_stack"].notna()]
    for _, r in fsm.iterrows():
        print(f"  {r['business_name']:<42} -> {r['apollo_fsm_in_stack']}")
    print()
    print("Headcount growth (rich-data rows only):")
    rich = merged[merged["apollo_data_quality"] == "rich"].copy()
    rich = rich[rich["apollo_hc_growth_12mo"].notna()]
    rich = rich.sort_values("apollo_hc_growth_12mo", ascending=False)
    print("  growing (12mo > +5%):")
    for _, r in rich[rich["apollo_hc_growth_12mo"] > 0.05].iterrows():
        print(f"    +{r['apollo_hc_growth_12mo']*100:>5.1f}%  {r['business_name']}")
    print("  shrinking (12mo < -5%):")
    for _, r in rich[rich["apollo_hc_growth_12mo"] < -0.05].iterrows():
        print(f"    {r['apollo_hc_growth_12mo']*100:>6.1f}%  {r['business_name']}")
    print()
    print(f"Outputs:")
    print(f"  {OUT_CURRENT.relative_to(ROOT)}")
    print(f"  {snapshot_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
