# HVAC Signals

A buying-signal pipeline for field service management (FSM) software vendors selling into Phoenix-area HVAC contractors. Starts with two raw Arizona Registrar of Contractors license exports (82,610 rows) and produces a ranked list of 25 contractors most likely in a buying state, with a per-contractor HTML dossier showing exactly which signals fired and what evidence supports each claim.

## The output

- **[deliverables/index.html](deliverables/index.html)** — one-page ranked list of the top 25 contractors. Each row shows signal chips (PAIN / GROWTH / DEMAND PULL / NO FSM / HIRING DISPATCH / THIN SAMPLE), a colored accent indicating the dominant signal, the intent tier badge, and the top bullet points for why this contractor is a lead.
- **[deliverables/dossier_v4_*.html](deliverables/)** — 25 individual dossiers, one per contractor. Each dossier is a stack of cards, one per fired signal, with every claim backed by a dated citation to the source evidence. Headers show intent tier, estimated annual revenue (from AZ ROC bond amounts), and a signal freshness badge when 25%+ of dated signals are in the last 30 days.
- **[SCORING.md](SCORING.md)** — full scoring methodology: 7 dimensions, 3 non-scoring display fields, FSM-vendor hard disqualifier, and explicit disclaimers about what the model can and can't see.

## The funnel

| Stage | Rows |
|---|---:|
| Raw AZ ROC license exports (residential + dual) | 82,610 |
| After Step 01 filter (HVAC class, Maricopa city, license age, sole-prop drop) | 963 |
| After Step 02c cleanup (dedup, geo bounding box, closed-business filter) | 356 |
| After Step 05 hidden gems filter | 70 |
| **After Step 11 scoring and ranking** | **25 surfaced** |

## The pipeline

18 numbered Python scripts under [pipeline/](pipeline/). Run in order:

1. **[01_load_and_filter](pipeline/01_load_and_filter.py)** — filter ROC exports to Phoenix HVAC contractors with 5-25 year license age, drop sole-proprietor-pattern names
2. **[02_enrich_places](pipeline/02_enrich_places.py)** — Google Places API enrichment with DBA routing
3. **[02b_segment_tiers](pipeline/02b_segment_tiers.py)** / **[02c_clean_tier_1](pipeline/02c_clean_tier_1.py)** — tier segmentation, dedup, geo + closed-business filters
4. **[03_fsm_detection](pipeline/03_fsm_detection.py)** — webanalyze + regex fingerprinting for field service platform presence
5. **[04_apollo_merge](pipeline/04_apollo_merge.py)** — Apollo organization enrichment
6. **[05_rank_hidden_gems](pipeline/05_rank_hidden_gems.py)** — narrow 356 → 70 (hidden gems = no FSM detected, 50-500 reviews, 4.0+ rating)
7. **[06_serpapi_velocity](pipeline/06_serpapi_velocity.py)** — Google Maps reviews via SerpAPI, review velocity computation
8. **[07_serpapi_hiring](pipeline/07_serpapi_hiring.py)** / **[07b_serpapi_hiring_retry](pipeline/07b_serpapi_hiring_retry.py)** — Google Jobs hiring signal (raw postings cached; the LLM validator in step 17 decides which belong to each contractor)
9. **[08_review_nlp](pipeline/08_review_nlp.py)** / **[08b_review_llm](pipeline/08b_review_llm.py)** — regex and LLM review analysis in parallel, 6-month recency window, returns per-mention arrays with review_index references
10. **[09_dispatch_delay](pipeline/09_dispatch_delay.py)** — per-review dispatch time extraction, 6-month recency window, classifier shared with the render layer
11. **[10_review_burst_detection](pipeline/10_review_burst_detection.py)** — detect review volume spikes (crisis bursts are the signal; positive surges are deprioritized in favor of the velocity card)
12. **[18_roc_bond_scraper](pipeline/18_roc_bond_scraper.py)** — scrapes bond amounts from the AZ ROC public website (Playwright headless browser). Bond amounts are mandated by law to scale with annual gross volume — maps to revenue bands ($150K–$500K, $500K–$1.5M, etc.). 100% coverage on the pool, $0 cost
13. **[11_scoring](pipeline/11_scoring.py)** — 7-dimension additive scoring with ICP fit, FSM-vendor disqualifier, revenue band, size tier, and signal freshness display fields
14. **[12_contact_enrichment](pipeline/12_contact_enrichment.py)** / **[13_contact_augment](pipeline/13_contact_augment.py)** — website scrape, Apollo people, AZ ROC qualifying party merge
14. **[16_tavily_contact_search](pipeline/16_tavily_contact_search.py)** — pure extractor: Tavily search, dumps every email/phone/social-URL candidate with no ownership judgment
15. **[17_candidate_validator](pipeline/17_candidate_validator.py)** — one Claude Haiku call per contractor decides which Tavily + SerpAPI-jobs candidates actually belong, with a written reason per decision. Single source of truth for the dossier's contact and hiring cards
16. **[14_dossier_cards](pipeline/14_dossier_cards.py)** — card-based HTML dossiers + index page
17. **[15_evidence_audit](pipeline/15_evidence_audit.py)** — diagnostic script for inspecting raw cached evidence

For the full narrative — what each stage does, why it exists, and the data shape at each step — read **[PIPELINE.md](PIPELINE.md)**.

## Project rules

Standing rules live in **[CLAUDE.md](CLAUDE.md)**. Highlights:

- **Read from the source cache, not rolled-up columns.** The render layer reads raw cached data directly. Aggregate columns are for scoring only, never for rendering evidence.
- **Counts shown in headlines must match the evidence rendered beneath them.** Compute the list first, render every item, use its length for the headline.
- **Reviews older than 6 months do not contribute to signals.** A 180-day filter is applied at every stage that touches review content.
- **Signal-classification definitions live in one place per signal.** When two scripts classify the same signal, their thresholds must be identical.
- **Never hardcode cultural-assumption lists.** No first-name lists, no keyword bags that encode regional or linguistic bias. Use the LLM for semantic judgment.
- **Extract then validate, never fuzzy-match.** Regex and search extractors pull candidates exhaustively; a single LLM validator decides which belong to which contractor. Character-level name similarity is the wrong tool for "is this the same business" questions.
- **Smoke test before scale.** Every API-hitting script supports `--limit N` and requires smoke-test approval before running at scale.
- **Snapshot, don't overwrite.** Every enrichment step writes a timestamped snapshot plus a current-state file. Dropped rows go to a sidecar file, never deleted.
- **DBA over legal name.** When querying external APIs, prefer the doing-business-as field. 12% of contractors need it, and those 12% include most of the flagship brands.
- **The hottest signal is software absence.** A contractor with no detectable FSM platform and phone-only bookings is, by definition, a prospect.

## Running the pipeline

Each script is runnable independently. The typical path from scratch:

```bash
# Install
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt  # pandas, anthropic, requests, python-dotenv, beautifulsoup4

# Set API keys in .env
# GOOGLE_PLACES_API_KEY, SERPAPI_API_KEY, ANTHROPIC_API_KEY, APOLLO_API_KEY

# Run the pipeline
python pipeline/01_load_and_filter.py
python pipeline/02_enrich_places.py --limit 20    # smoke test first
python pipeline/02_enrich_places.py               # full run
python pipeline/02b_segment_tiers.py
python pipeline/02c_clean_tier_1.py
python pipeline/03_fsm_detection.py
python pipeline/04_apollo_merge.py
python pipeline/05_rank_hidden_gems.py
python pipeline/06_serpapi_velocity.py
python pipeline/07_serpapi_hiring.py
python pipeline/07b_serpapi_hiring_retry.py
python pipeline/08_review_nlp.py
python pipeline/08b_review_llm.py
python pipeline/09_dispatch_delay.py
python pipeline/10_review_burst_detection.py
python pipeline/12_contact_enrichment.py          # website scrape + Apollo people
python pipeline/13_contact_augment.py             # merge ROC owner + Places + website
python pipeline/16_tavily_contact_search.py       # pure extractor: every candidate contact (all 70)
python pipeline/17_candidate_validator.py         # LLM validator: belongs/reject per candidate (all 70)
python pipeline/18_roc_bond_scraper.py            # scrape bond amounts → revenue bands (all 70)
python pipeline/11_scoring.py                     # reads validator + bond caches for scoring
python pipeline/14_dossier_cards.py --all         # generates 25 dossiers + index
```

**API cost warning.** Running the pipeline end-to-end from scratch hits Google Places (~$5 for 356 contractors), SerpAPI Google Maps reviews (~$1.50), SerpAPI Google Jobs (~$1.50), Tavily Advanced (~$1.05 on the full 70-contractor pool), and Claude Haiku 4.5 (~$0.75 for review analysis + validator on all 70). Expect roughly $10-15 total on a fresh full pool run. All API responses are cached per contractor — reruns after the first pass are free.

Regenerate just the dossiers and index after a scoring change:

```bash
python pipeline/17_candidate_validator.py         # refresh validator on all 70
python pipeline/11_scoring.py                     # re-score using validator hiring
python pipeline/13_contact_augment.py             # refresh contact augmented csv
python pipeline/14_dossier_cards.py --all         # regenerate dossiers + index
```

Inspect the raw cached evidence for any contractor:

```bash
python pipeline/15_evidence_audit.py --rank 13
```

## File layout

```
hvac-signals/
├── README.md                          # this file
├── PIPELINE.md                        # full pipeline narrative
├── CLAUDE.md                          # standing rules and lessons
│
├── pipeline/                          # 18 numbered scripts (01 through 18)
├── data/
│   ├── 01_contractors/                # Step 01–02c outputs (filtered, enriched, tiered)
│   ├── 02_enrichment/                 # Step 03–04 outputs (FSM detection, Apollo signals)
│   ├── 03_hidden_gems/                # Step 05–11 outputs (pool → scored)
│   ├── 04_contacts/                   # Step 12–13 outputs
│   ├── signals_raw/                   # cached API responses (never re-fetched)
│   └── snapshots/                     # timestamped audit copies, one subdir per step
│
├── deliverables/                      # index.html + 25 dossier HTML files
├── outputs/                           # diagnostic output (evidence audit)
├── tools/                             # webanalyze binary + fingerprints
│
├── ROC_Posting-List_Dual_2026-04-10.csv
└── ROC_Posting-List_Residential_2026-04-10.csv
```

The `data/` sub-folders mirror the pipeline stages. See [PIPELINE.md](PIPELINE.md) Appendix A for the full layout with per-file descriptions.
