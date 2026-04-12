# HVAC Signals Pipeline

This document walks through how the pipeline built a ranked list of 25 Phoenix HVAC contractors most likely in a buying state for field service management (FSM) software, starting from two raw Arizona state contractor license exports.

Every number in this document is pulled from the real output files. Every lesson called out is a lesson the pipeline actually encountered during development, recorded in [CLAUDE.md](CLAUDE.md) at the time it was discovered.

## The funnel at a glance

| Stage | Rows | File |
|---|---:|---|
| AZ ROC Dual license CSV | 35,892 | `ROC_Posting-List_Dual_2026-04-10.csv` |
| AZ ROC Residential license CSV | 46,718 | `ROC_Posting-List_Residential_2026-04-10.csv` |
| **Combined + cross-file dedup** | ~82,610 | (in-memory) |
| **After Step 01 filter** (HVAC class + Maricopa city + license-age window + sole-prop heuristic) | **963** | `data/01_contractors/filtered.csv` |
| Step 02b tier segmentation — Tier 1 enterprise | 405 | `data/01_contractors/tier_1_enterprise.csv` |
| Step 02b Tier 2 borderline | 74 | `data/01_contractors/tier_2_borderline.csv` |
| Step 02b Tier 3 small shops | 185 | `data/01_contractors/tier_3_small_shops.csv` |
| Step 02b Tier 4 rejects | 299 | `data/01_contractors/tier_4_rejects.csv` |
| **After Step 02c cleanup** (dedup tiebreaker + geo bounding box + closed-business filter) | **356** | `data/01_contractors/tier_1_clean.csv` |
| Step 05 hidden gems filtered pool | 70 | `data/03_hidden_gems/filtered_pool.csv` |
| After all signal enrichments | 70 | `data/03_hidden_gems/complete.csv` |
| **After Step 11 scoring** | **70 ranked** | `data/03_hidden_gems/scored.csv` |
| **Top 25** | **25** | `deliverables/index.html` |

The top 25 narrative mix: 16 light_signal, 4 scaling_strain, 3 demand_pull, 1 mixed, 1 unclear. Confidence tiers: 3 high, 7 medium, 15 low.

---

## Chapter 1 — Data sources

Two CSVs exported from the Arizona Registrar of Contractors (ROC) on 2026-04-10:

- **`ROC_Posting-List_Residential_2026-04-10.csv`** — R-prefix residential licenses (R-39 and R-39R are the HVAC classes)
- **`ROC_Posting-List_Dual_2026-04-10.csv`** — CR-prefix dual-use licenses (CR-39 is HVAC)

HVAC classes span both files, so a contractor can appear in both under the same `(license_no, class)` — we dedupe on that. Each row has license number, business name, doing-business-as, **qualifying party** (the licensed individual legally responsible for the work), mailing address, issued date, status, and class.

The `qualifying_party` field is the project's ground-truth owner name. For 24 of the current top 25, that field names a real person who runs the shop; [pipeline/13_contact_augment.py](pipeline/13_contact_augment.py) uses it verbatim on every dossier's "Who to call" card.

Both files ship with a banner row and a few malformed quoted lines — `pipeline/01_load_and_filter.py` handles them by skipping the banner and running the Python CSV engine with `on_bad_lines='skip'`.

---

## Chapter 2 — Step 01: filter 82,610 raw rows → 963

`pipeline/01_load_and_filter.py`

### Filter chain

1. Combine residential + dual → 82,610 rows
2. Cross-file dedup on `(license_no, class)`
3. HVAC class allowlist: `{R-39, R-39R, CR-39}`
4. `state == "AZ"` on the mailing address
5. Maricopa city allowlist (24 cities covering Phoenix, Mesa, Tempe, Scottsdale, Glendale, Chandler, Gilbert, Peoria, and surrounding municipalities)
6. License age 5–25 years as of the snapshot date (under 5 = too new to have operational pain, over 25 = probably already bought FSM)
7. Sole-prop heuristic (see below)
8. Dedup by `license_no`

**Result:** 963 keepers, 40 sole-prop drops written to `data/01_contractors/dropped/soleprop.csv`.

### Sole-prop heuristic

FSM vendors don't pursue one-person owner-operators — nothing to automate. Two rules, either disqualifies:

- **Rule A** — no corporate suffix AND the business name contains the qualifying party's last name as a word
- **Rule B** — no corporate suffix AND 2-3 words AND the last word is a trade word (`hvac, air, heating, cooling, refrigeration, mechanical, ac`)

Trade words are excluded from the corporate-suffix list so "Bob Smith Air" still looks like a sole prop.

### Note: Step 01 is not the only geographic filter

Step 01 filters on the ROC mailing address city. It doesn't catch contractors with a real Phoenix mailing address whose **Google Places listing** resolves to a multi-state HQ. That case is handled by Step 02c's bounding box (Chapter 4).

---

## Chapter 3 — Step 02: Google Places enrichment (963 rows)

`pipeline/02_enrich_places.py`

Queries the Google Places API (New) for each contractor using a tight field mask. Captures:

- `place_id` — the join key for every downstream script
- `place_address`, `place_latitude`, `place_longitude` — used by Step 02c's bounding box
- `place_phone`, `place_website` — the "booking phone" and "web form" on the dossier
- `place_rating`, `place_review_count` — feed signal enrichment
- `place_business_status` — distinguishes OPERATIONAL from CLOSED_PERMANENTLY
- `place_match_confidence` — a 0–100 score

### Query normalization

Business names are stripped of punctuation, collapsed to single spaces, and trimmed. **The doing-business-as field is preferred over legal name.** About 12% of contractors have a DBA, and those 12% include flagship brands like Collins Comfort Masters and Rainforest Plumbing and Air — without DBA routing they miss or mismatch.

### Match confidence

`place_match_confidence` strips HVAC stopwords (`inc, llc, corp, co, company, air, hvac, heating, cooling, refrigeration, mechanical, services, the, and, &`) before running a character-level similarity between query name and returned name. Anything below 85 is flagged for review. Without stopword stripping, "Phoenix Air Department LLC" and "Phoenix Air Company LLC" score as near-identical.

---

## Chapter 4 — Step 02b + 02c: segmentation and cleanup

### Step 02b: `pipeline/02b_segment_tiers.py` — 963 → 4 tiers

| Tier | Count | Definition |
|---|---:|---|
| 1 — enterprise | 405 | Real operational business, confident match, enough reviews. Downstream pipeline scores these. |
| 2 — borderline | 74 | Low confidence match or partial data |
| 3 — small shops | 185 | Legit but under the review-count threshold |
| 4 — rejects | 299 | No match, closed, or out-of-region |

### Step 02c: `pipeline/02c_clean_tier_1.py` — 405 → 356

Three passes on Tier 1.

**Dedup by `place_id`** using the tiebreaker chain (documented in [CLAUDE.md](CLAUDE.md)):
1. Higher `place_match_confidence` wins
2. Higher `place_review_count` wins
3. Older `issued_date` wins

The older-license tiebreaker matters because same-Google-listing duplicates always tie on review count (the reviews come from the same listing). When a contractor holds multiple ROC licenses mapping to one listing, the older license is almost always the original operating entity; newer ones are reorganizations. 4 duplicates dropped.

**Geographic bounding box** — lat 32.5–34.1, lon −113.4 to −111.0 (a loose superset of the real Maricopa bounds). 45 rows dropped where the Google Places match resolved to an out-of-state HQ (Dubai, NY, FL, CA, UT all appeared in the drop file).

**Closed-business filter** — drops `place_business_status == 'CLOSED_PERMANENTLY'`. Catches 100-confidence name matches to dead businesses (Morales Air was the incident).

**Result:** 356 clean Tier 1 contractors.

---

## Chapter 5 — Step 03: FSM platform detection

`pipeline/03_fsm_detection.py`

For each contractor with a website, detect whether they run a field service management platform or operate on phones + spreadsheets. Two passes:

1. **Regex fingerprinting** — HTTP-fetches the homepage and scans for known booking-widget signatures (ServiceTitan, Housecall Pro, Jobber, online-scheduler widgets, form builders that imply online booking).
2. **webanalyze** — the [webanalyze](tools/webanalyze/) binary runs 7,517 technology fingerprints against the homepage, catching site builders, CMS, page builders, CDNs, and anything else useful for operational context.

### Outputs per contractor

- `has_any_booking_tool` — boolean; the primary hidden-gems qualifier
- `phone_only` — true when no online booking detected and a phone number is prominent
- `webanalyze_site_builder`, `webanalyze_cms`, `webanalyze_page_builder`, `webanalyze_tech_summary` — detected stack
- `detection_evidence` — which patterns fired on which URL

All 25 contractors in the current top 25 have `has_any_booking_tool == false`. No-FSM-detected is the primary hidden-gems filter and the loudest signal in the whole pipeline.

---

## Chapter 6 — Steps 04 + 05: Apollo merge + hidden gems filter

### Step 04 — Apollo organization merge (`04_apollo_merge.py`)

Runs the 356 domains through Apollo's Organization Enrich API. When available, pulls LinkedIn URL, employee count, founded year, industry tags, parent-company relationships. Apollo covers about 20% of small Phoenix HVAC contractors; the other 80% aren't in Apollo's org database.

### Step 05 — Hidden gems filter (`05_rank_hidden_gems.py`)

Narrows 356 → 70 using four rules:

- `has_any_booking_tool == false` (the FSM gap is the primary qualifier)
- 50–500 Google reviews (under 50 = can't signal-score; over 500 = already a brand)
- Rating ≥ 4.0 (filters noise, not a quality ranking)
- Not matched to a national franchise

The pool is deliberately kept at 70 so expensive signal enrichment (SerpAPI, LLM review analysis, dispatch extraction) can run within budget. Every downstream script operates on these 70 only.

---

## Chapter 7 — Signal enrichment (Steps 06 through 10)

Five enrichment steps, each operating on the 70 hidden gems. All use cached raw data so re-runs are free after the first pass.

### Step 06: Review velocity (`06_serpapi_velocity.py`)

Pulls the first 1-3 pages of Google reviews via SerpAPI per contractor. Caches the raw response to `data/signals_raw/serpapi_reviews/{place_id}.json`. Computes:

- `recent_90d_reviews` / `prior_90d_reviews` — review counts in consecutive 90-day windows
- `velocity_ratio` — recent/prior
- `velocity_category` — `accelerating`, `hot_new`, `steady`, `low_volume`

90-day windows are wide enough to filter seasonality but short enough to catch real momentum changes.

### Step 07: Hiring signal (`07_serpapi_hiring.py`, `07b_serpapi_hiring_retry.py`)

Queries SerpAPI's Google Jobs engine with `"<business name>"` and caches the raw response to `data/signals_raw/serpapi_jobs/{place_id}.json`. **Google Jobs often returns "related" postings from unrelated HVAC companies**, so the script applies a strict `company_name` slug-substring filter before writing to cache — unrelated rows go into a `rejected_jobs` sidecar field for audit.

Classification of matching postings:

- **`ops_pain`** — dispatcher, scheduling coordinator, customer service rep (roles FSM replaces)
- **`capacity_growth`** — technician, installer, apprentice (growing headcount)
- **`other`** — marketing, finance, back-office

The `07b_` retry script handles contractors where the first pass returned nothing by retrying under the Google Places name (for casing/spacing mismatches against the legal name).

**Dossier surfacing.** The hiring card shows every cached posting with its real date and flags `ops_pain` titles with a green "FSM BUYER ROLE" badge (green because these are positive buying signals, not warnings). Date sources, in priority order:
1. Apollo's `organization_job_postings` endpoint (exact ISO timestamps, LinkedIn URLs, available when the org is in Apollo's database)
2. SerpAPI's `"X days ago"` field computed against the cache's `fetched_at` timestamp
3. `fetched_at` itself, labeled as "first observed on \<date\>" — honest lower bound when neither source gives an absolute date

### Step 08: Review NLP (`08_review_nlp.py`, `08b_review_llm.py`)

Two parallel approaches on the same cached reviews. Both apply a strict **6-month recency filter** — reviews older than 180 days never reach either classifier.

**08 (regex NLP)** — keyword-match review text for dispatch complaints, communication issues, capacity complaints, growth mentions, founder praise, key-person mentions, customer-switch mentions ("I switched from X to this shop"), smooth-ops positive indicators. Produces counts and sample quotes per category.

**08b (LLM)** — Claude Haiku 4.5 reads the reviews (sorted most-recent-first, numbered in the prompt) and returns a structured JSON blob with both aggregate scores AND per-mention arrays:

- **Aggregate scores:** `pain_score` (0–10), `momentum_score` (0–10), `smooth_ops_score` (0–10), `buying_category`, `founder_involvement` (none/moderate/heavy), `key_person_dependency` (none/moderate/heavy), `one_sentence_summary`
- **Per-mention arrays** (capped at 8 entries each): `pain_mentions`, `momentum_mentions`, `switcher_mentions`, `smooth_ops_mentions`
- **Referenced people** (`referenced_people` array): first names of employees customers mention in reviews — technicians, dispatchers, office staff. One entry per distinct person, with a `mention_count` and a short `sample_quote` (verbatim, under 80 chars). The LLM is instructed to exclude the reviewer's own family, prior contractors being badmouthed, and generic references ("the tech"). Rendered inline on the decision-maker card as "Also named in reviews — ask for any of these by first name." Owner's first name is deduped at render time so the one-person card and the referenced-people list don't double-count.

Each mention is an object: `{review_index, quote, subtype}`. The `review_index` is 1-based and points into the `indexed_reviews` list cached alongside the parsed response — so any downstream reader can resolve a mention back to its dated source review. Subtypes are specific per category (pain subtypes: dispatch / communication / capacity / quality / billing / other; momentum subtypes: demand_pressure / founder_owned / key_person / long_wait / capacity_strain / other).

The LLM prompt explicitly instructs it to return verbatim quote sentences (not paraphrases) and to keep the most recent mentions when capacity exceeds the cap of 8 per category.

The LLM approach is ~$0.003–0.006 per contractor at current Haiku pricing. 70-contractor full runs cost about $0.20. The regex approach is free and cross-validates the LLM's claims.

**The thin-sample problem.** About half of the 70 hidden gems have 5–10 cached reviews. The LLM can produce a confident `pain_score=6` from 8 reviews if one customer was particularly angry. This is handled by the thin-sample discount in Step 11 scoring — see Chapter 8.

### Step 09: Dispatch delay extraction (`09_dispatch_delay.py`)

Another LLM pass over the cached reviews, but narrower. For each review (filtered to the 6-month window, sorted most-recent-first), extract any mention of response time — "same day," "two hours," "6 days to come back" — and convert it to a structured extraction: `estimated_delay_hours`, `delay_category` (emergency / same_day / next_day / same_week / week_plus), `sentiment`, and a `verbatim_quote`.

Aggregated into per-contractor statistics: `dispatch_median_hours`, `dispatch_max_hours`, `dispatch_stdev_hours`, `dispatch_same_day_pct`, `dispatch_week_plus_pct`, `dispatch_negative_sentiment_pct`, and `dispatch_category`.

**Classification rules** (shared between `09_dispatch_delay.py` and the render layer in `14_dossier_cards.py` — the threshold definitions are identical in both places):

- **`dispatch_fast`** — median ≤ 24h AND max ≤ 72h
- **`dispatch_bimodal`** — ≥ 25% of measured jobs same-day AND ≥ 25% are week-plus (genuine split, not a single outlier)
- **`dispatch_fast_outlier`** — median ≤ 24h AND ≥ 60% same-day, but one slow outlier drags the max above 72h (mostly fast dispatch with an isolated incident)
- **`dispatch_strained`** — median > 48h OR max > 168h, without the bimodal split
- **`dispatch_slow`** — median > 24h with no substantial fast population
- **`dispatch_low_data`** — fewer than 2 extractions

The dispatch card in [pipeline/14_dossier_cards.py](pipeline/14_dossier_cards.py) renders these extractions as a dot plot on a 0-to-168-hour axis, colored by sentiment, so the dispatch pattern is visible at a glance.

### Step 10: Review burst detection (`10_review_burst_detection.py`)

For each contractor, slide a 7-day window through their cached reviews. Flag a burst when `count_in_window >= 3 AND count >= 3x the contractor's own baseline velocity`. Classify bursts by average rating:

- **`active_crisis`** — negative burst in the last 30 days
- **`recent_crisis`** — negative burst in the last 60 days
- **`scaling_surge`** — positive burst in the last 60 days (see note below)
- **`historical_burst`** — older burst
- **`steady`** — no bursts

Zero API cost — this is pure local computation over the already-cached SerpAPI reviews.

**`scaling_surge` is deliberately de-emphasized.** The burst detector fires on tiny baselines — a contractor with 0.17 reviews/week baseline and 3 positive reviews in a week is mathematically "17x the baseline" but says nothing meaningful about real growth. Scoring only rewards a `scaling_surge` when `burst_baseline_per_week >= 0.5` (at least one review every two weeks of baseline velocity), and the dossier never renders a standalone card for `scaling_surge` at all — the review velocity card already shows momentum with a dated 12-month bar chart, which is the rigorous view.

Crisis bursts (`active_crisis` and `recent_crisis`) are the real value of this step.

---

## Chapter 8 — Step 11: Scoring (70 hidden gems → final ranking)

`pipeline/11_scoring.py`

### Seven scoring dimensions

| # | Dimension | Cap | What it rewards |
|---|---|---|---|
| 1 | Direct pain | 0–40 | Customer complaints, strained dispatch, crisis bursts, negative sentiment |
| 2 | Scaling strain | 0–25 | Hiring, review velocity acceleration, momentum quotes |
| 3 | Multi-signal convergence | 0–15 | Bonus when multiple independent sources fire |
| 4 | Operational readiness | 0–10 | Has website, enough reviews, 10+ years licensed |
| 5 | Demand pull | 0–20 | Customer-switch mentions, heavy founder/key-person dependency, qualifying scaling_surge bursts, fast dispatch combined with any of the above |
| 6 | ICP fit | 0–5 | License scope: Dual (CR-39, commercial + residential) = +5, Residential-only (R-39/R-39R) = +2 |
| 7 | Disqualifiers | −30–0 | Smooth-ops indicators, FSM-vendor job postings (hard exclusion) |

Three non-scoring display fields also appear on every dossier:

- **Revenue band** — from AZ ROC bond amounts (step 18), maps to $150K–$500K / $500K–$1.5M / $1.5M–$5M / $5M+ tiers. 100% coverage, government-mandated, updates at license renewal.
- **Size tier** — secondary proxy from license tenure + review count (S/M/L/XL). Fallback when bond data is unavailable.
- **Signal freshness** — FRESH (50%+ of dated signals in last 30 days) or RECENT (25%+). Tells a rep which prospects are in pain *right now*.

### The thin-sample discount

Dimensions 1 and 2 are multiplied by `min(1, llm_review_count_analyzed / 15)`. A contractor scored on 5 reviews gets only 33% credit on those two dimensions. A contractor scored on 15+ reviews gets full credit. This prevents small-sample contractors from maxing out the pain dimension on a single angry customer.

### The demand-pull dimension

The demand-pull dimension captures a specific buying pattern the other dimensions don't: **success dependent on a single person, with growing demand and no system to scale**. Classic example: a one-person shop with heavy founder involvement, customers switching from competitors, fast dispatch (because the owner is the one dispatching), and no FSM platform. The contractor is winning right now, but they're at a capacity ceiling.

The dimension rewards:
- Customer-switch mentions (refugees) — +3 per mention, cap at +12
- `founder_involvement == "heavy"` — +4
- `key_person_dependency == "heavy"` — +4
- `scaling_surge` burst with real baseline (`burst_baseline_per_week >= 0.5`) — +4
- `dispatch_fast` combined with any of the above (the "capacity ceiling" compounding signal) — +3
- Cap: 20 points total

### The smooth-ops vs demand-pull disambiguation

These two concepts sound similar but mean opposite things:

- **`smooth_ops`** — customers explicitly praise automated workflows (online booking, text reminders, technician tracking, arrival ETAs). This signals the contractor has already bought FSM software. It's a disqualifier — −10 points.
- **`demand_pull`** — customers praise a specific person (the owner, a named tech) and the contractor executes fast. This signals they're at a capacity ceiling without automation. It's a buying signal — up to +20 points.

The disqualifier's `dispatch_fast + no pain` penalty only fires when there's ALSO no demand-pull signal. A contractor with fast dispatch who also has heavy founder involvement or refugee mentions is NOT penalized — their fast dispatch is a demand-pull signal, not a smooth-ops signal.

### Confidence tier

Confidence is capped at `low` when the LLM review sample is below 10. This is a companion to the thin-sample discount — it tells the reader how much to trust the scores, not just what the scores are.

### Result: 70 contractors ranked, top 25 surfaced

Top 5 in the current state:

1. **Grand Canyon Home Services LLC** (Peoria) — 57.0 — scaling_strain, high confidence, 36 reviews analyzed, active hiring of **HVAC Dispatcher** role (confirmed via Apollo org job postings)
2. **Comfort Experts LLC** (Mesa) — 53.0 — scaling_strain, high confidence, 27 reviews analyzed, 2 customer-switch mentions, Ryan Cronstrom / Ryan Mikita co-founders
3. **D W Plumbing Heating & Cooling Inc** (Phoenix) — 46.5 — scaling_strain, high confidence, 40 reviews analyzed, dispatch_fast + qualifying scaling_surge
4. **Air & Water Mechanical Services LLC** (Mesa) — 45.0 — scaling_strain, medium confidence
5. **Total Care Heating & Cooling LLC** (Peoria) — 44.0 — demand_pull, medium confidence, 18 reviews, 3 customer-switch mentions, heavy founder + heavy key-person (Sammy Sayegh named in 16 of 18 cached reviews)

---

## Chapter 9 — Steps 12 through 17: Contact enrichment

Contact enrichment is an **extract then validate** pipeline. Extractors are dumb and exhaustive — they pull every candidate email, phone, and social URL they can find. A single LLM validator then decides which candidates actually belong to each contractor. No fuzzy string matching. No regex-based "is this the same business" judgments.

### Step 12: `pipeline/12_contact_enrichment.py`

For each top-25 contractor, attempts website scraping (fetches `/about`, `/contact`, `/team`, `/leadership` etc. and LLM-extracts people, emails, phones) and an Apollo People Search + Match pass. Cached to `data/signals_raw/contacts/{place_id}.json`.

Apollo's people-database coverage of small Phoenix HVAC contractors is near zero, so this step's practical yield is a handful of scraped business emails.

### Step 13: `pipeline/13_contact_augment.py`

Merges the AZ ROC `qualifying_party` (authoritative owner name — coverage 24/25), Google Places phone/website, Apollo-verified emails and titles (when present), and the Step-12 website-scraped emails. Writes `data/04_contacts/augmented.csv`. The validator cache (Step 17) is read directly by the render layer, not merged here.

### Step 16: `pipeline/16_tavily_contact_search.py` — pure extractor

One Tavily advanced search per contractor (all 70 — runs before scoring), query `"<business>" <city> Arizona (contact OR email OR facebook OR linkedin)`. Regex-extracts every email, phone, Facebook URL, LinkedIn company URL, Instagram URL, BBB URL, and Yelp URL it can find, and records the source URL(s) each candidate came from. **Makes no judgment about ownership** — character-level similarity is the wrong tool for "is this the same business" and has repeatedly produced false positives (e.g. `linkedin.com/company/cardinal-heating-and-air-conditioning`, a Wisconsin business, matching Cardinal Heating & Cooling LLC in Peoria, Arizona).

Cost: ~$0.015 per contractor. A full 70-contractor pass runs about $1.05. Cached to `data/signals_raw/tavily_contacts/{place_id}.json`.

### Step 17: `pipeline/17_candidate_validator.py` — LLM validator

One Claude Haiku 4.5 call per contractor (all 70). Takes the Tavily candidates plus the raw SerpAPI job postings from both the legal-name cache (step 07) and the DBA / place-name retry cache (step 07b), and decides belongs/reject for every item with a one-sentence reason. The model gets business name, city, state, own domain, and Google Places main phone as context. Each candidate list is capped at 100 items before the call, prioritizing items confirmed by the most source URLs — this avoids the prompt blowing past the context window on pathological Tavily dumps (phone directories, BBB aggregator pages) while still surfacing every candidate that has real cross-source confirmation. Output mirrors the input shape with added `belongs` and `reason` fields. Cached to `data/signals_raw/validator/{place_id}.json`.

Cost: ~$0.005 per contractor on Haiku average, occasional outliers up to $0.04 on contractors with heavy Tavily dumps; a full 70-contractor pass runs about $0.50. Promotable to `--model sonnet` or `--model opus` when a case needs more reasoning.

**The validator is the single source of truth for which contacts AND jobs belong to which contractor.** Step 11 scoring reads the validator's kept jobs (classified into ops_pain / capacity / other at read time) to compute `hiring_ops_pain_count`, `hiring_capacity_count`, and `hiring_signal`. The dossier render layer reads from the same cache for contacts and hiring cards. There is no second fuzzy check anywhere downstream — if the validator kept it, it renders; if the validator rejected it, it doesn't exist. Every claim on every dossier traces back to a validator decision with a written reason.

**Pipeline ordering consequence.** Because scoring depends on the validator for hiring counts and the bond scraper for revenue bands, the run order is now: 06 velocity → 07 hiring extractor → 07b retry → 08 review NLP → 08b review LLM → 09 dispatch → 10 burst → 12/13 contact augment → **16 Tavily → 17 validator → 18 bond scraper → 11 scoring** → 14 dossier. The file numbers reflect the order the scripts were written in, not the order they run in — see the README for the correct execution sequence.

### Step 18: `pipeline/18_roc_bond_scraper.py` — revenue bands from public record

Arizona law ([ARS 32-1152](https://www.azleg.gov/ars/32/01152.htm)) requires every contractor to post a surety bond sized to their annual gross volume tier. The bond amount is public record on every contractor's ROC detail page. This step scrapes it for all 70 contractors using Playwright (headless Chromium, because the ROC site is a Salesforce Aura app that renders client-side).

The scraper searches by license number, extracts the Salesforce `licenseId` from the search results, loads the detail page, and reads the bond amount. When a contractor holds separate R-39 (residential) and C-39 (commercial) licenses instead of a single CR-39 dual, both bonds are scraped and combined. Cached to `data/signals_raw/roc_bonds/{place_id}.json`.

Coverage: 70/70 (100%). Cost: $0 — public government website. Runtime: ~12 minutes for the full pool.

The bond amount maps to a revenue band: $4K–$9K → $150K–$500K, $9K–$20K → $500K–$1.5M, $20K–$50K → $1.5M–$5M, $50K+ → $5M+. This is not an estimate from a third-party database — it's a government-mandated disclosure tied to reported volume. Apollo has 0/70 coverage on this pool; the ROC bond has 100%.

---

## Chapter 10 — Step 14: The dossier

`pipeline/14_dossier_cards.py`

### The core design rule

Every claim in the dossier is backed by a specific piece of data in a specific cached file, and the dossier renders the citation alongside the claim. No LLM interpretation. No synthesis paragraphs. No "sales angles" generated by a model. If the claim can't be cited, the card doesn't render.

More strictly: **the render layer reads from raw cached data, never from rolled-up aggregate columns**. Dispatch extractions come from `data/signals_raw/dispatch_delay/{place_id}.json`. Review mentions come from `data/signals_raw/review_llm/{place_id}.json`. Contact candidates and job postings come from `data/signals_raw/validator/{place_id}.json` — the validator cache is the single source of truth for which emails, phones, social URLs, and job postings belong to each contractor. Aggregates in the scored CSV are used for scoring and sorting only.

### Card structure

Each dossier is a stack of cards. Cards only render when their underlying signal actually fires for that contractor. The order:

1. **Header** — business name, years licensed, reviews, rating, website + Google Business Profile links, intent tier badge (HIGH / STRONG / EMERGING), signal chips (PAIN / GROWTH / DEMAND PULL / NO FSM / HIRING DISPATCH / THIN SAMPLE), colored left-edge accent bar (red = pain dominant, amber = growth dominant, purple = demand pull dominant)
2. **Why this is a good lead** — a yellow bullet-list panel with one bullet per fired signal. The five-second scan.
3. **Technology stack gap** — placed first in the evidence section because no-FSM is the loudest objective signal
4. **Decision maker** — owner name with green ✓ "verified via AZ contractor license," booking phone, booking web form, discovered emails (from Tavily with source attribution), alternate phones (labeled AZ / toll-free / out-of-state), social URLs (Facebook, LinkedIn, Instagram)
5. **Hiring intelligence** — renders when the validator kept any job postings for the contractor. Lists real postings with real dates (Apollo ISO timestamps where available, relative-date computed from SerpAPI otherwise, or `fetched_at` as a last-resort "first observed on" label). FSM buyer roles (dispatcher, scheduling coordinator, CSR) flagged with a green badge.
6. **Customers who switched from a competitor** — renders every verified switcher mention from `switcher_mentions` in the LLM cache. Count is `len(switcher_mentions)` so the headline always matches the evidence.
7. **One-person operation** — fires only when the owner's first name is mentioned in ≥4 cached reviews AND ≥20% of cached reviews (mechanical regex match against the AZ-verified owner first name). Evidence restricted to ≥4-star reviews so the card never pairs "founder praise" framing with a complaint.
8. **Dispatch distribution** — renders when ≥2 extractions exist in the 6-month window. Dot plot on a 0-to-168-hour axis, colored by sentiment. Headline derived from fresh stats via `classify_dispatch_pattern` — `fast`, `fast_outlier`, `bimodal`, `strained`, or `slow`. Each pattern has distinct headline text and interpretation.
9. **Customer pain evidence** — renders every pain mention from the LLM cache, grouped by source review. Headline counts distinct reviews, not observations (so "2 customers complained (4 distinct observations)" when one review contains two observations). When a crisis burst overlaps these pain reviews, the burst framing gets absorbed into this card as a dark-red CRISIS BURST badge + a subtitle — the separate burst card is suppressed.
10. **Growth momentum** — same grouping and count logic as the pain card, but for `momentum_mentions`.
11. **Review velocity** — renders when velocity signal is above threshold. Shows 12-month bar chart of reviews per month.
12. **Review burst** — renders only for crisis bursts (`active_crisis` or `recent_crisis`) whose evidence isn't already absorbed by the pain card. Falls back to raw cached review snippets if the LLM's pain extraction didn't cover the burst window (prevents rating-based crises from silently disappearing when the LLM classified the complaints as "quality" rather than "operational pain").

### Burst-to-pain absorption logic

When a crisis burst fires and the pain card already covers some or all of the reviews in the burst window, the pain card absorbs the burst:

- A dark-red `CRISIS BURST (last 30 days)` or `CRISIS BURST (last 60 days)` badge appears next to the pain-score tag
- A dark-red subtitle explains that a review cluster ran 3x+ the contractor's baseline velocity
- If pain reviews split across the window (some inside, some outside), the card renders two subsections: "N reviews in the crisis burst window" and "N ongoing pain reviews (outside the burst window)"
- The standalone burst card is suppressed
- The "why this is a good lead" bullet list does not duplicate the crisis bullet

If the LLM's pain extraction missed the burst entirely (e.g., the burst contains 1-star quality complaints that don't match operational-pain subtypes), the burst card falls back to rendering raw review snippets from the cached SerpAPI data — the lowest-rated reviews inside the burst window, labeled as raw excerpts.

### The "why this is a good lead" summary

The yellow panel at the top is the executive summary. It reads the same data as the cards below and produces a scannable bullet list. A rep can look at it and know in 5 seconds what's firing. Every bullet is mechanically derived from the same data the evidence cards use — if the dispatch card shows "fast dispatch," the bullet says "fast dispatch" with the same numbers. Bullets and cards always agree because they share the underlying resolver functions.

### The index page

`deliverables/index.html` is a one-page list of all 25 ranked dossiers. Each row shows:

- Large rank number
- Business name (clickable)
- Meta line: owner · city · years licensed · reviews · rating
- Signal chips (same chips as on the individual dossier header)
- Top 5 bullets from the "why this is a good lead" logic
- Intent tier badge
- "View dossier →" button

The index header has a legend panel explaining all six chip types, the seven scoring dimensions, the three intent tiers, and the four left-accent colors. It's dense but it makes the page self-explaining.

---

## Chapter 11 — Utility: Step 15 evidence audit

`pipeline/15_evidence_audit.py`

Not part of the main scoring or dossier flow. This is a diagnostic script that walks every cached source file for the top 25 (or any slice) and prints a full inventory: job postings with links, dispatch extractions with verbatim quotes, LLM pain/momentum evidence, regex sample quotes, review bursts, website scrape status, Apollo cache status.

This script exists because I spent most of the project shipping the scored CSV to the dossier and ignoring the raw caches. The evidence audit was the moment of realizing that the dossier had been rendering dashboard summaries while dozens of verbatim quotes and real job titles with apply links were sitting in `data/*_raw/` directories completely untouched.

Run it when you suspect the dossier is hiding something:

```bash
python pipeline/15_evidence_audit.py              # top 25 full inventory
python pipeline/15_evidence_audit.py --rank 13    # single contractor
python pipeline/15_evidence_audit.py --save       # save to outputs/
```

Output goes to stdout and optionally to `outputs/evidence_audit_YYYY-MM-DD.txt`.

---

## Appendix A — File layout

```
hvac-signals/
├── pipeline/                           # 18 numbered pipeline scripts
│   ├── 01_load_and_filter.py           # 82,610 → 963
│   ├── 02_enrich_places.py             # Google Places join
│   ├── 02b_segment_tiers.py            # → 4 tiers
│   ├── 02c_clean_tier_1.py             # dedup + geo + closed filter → 356
│   ├── 03_fsm_detection.py             # webanalyze + regex
│   ├── 04_apollo_merge.py              # Apollo org data
│   ├── 05_rank_hidden_gems.py          # 356 → 70
│   ├── 06_serpapi_velocity.py          # Google Maps reviews
│   ├── 07_serpapi_hiring.py            # Google Jobs (with company_name filter)
│   ├── 07b_serpapi_hiring_retry.py     # retry under alt name
│   ├── 08_review_nlp.py                # regex review analysis
│   ├── 08b_review_llm.py               # LLM review analysis (per-mention arrays, referenced_people, 6-month filter)
│   ├── 09_dispatch_delay.py            # per-review dispatch extraction (6-month filter)
│   ├── 10_review_burst_detection.py    # burst detection
│   ├── 11_scoring.py                   # 7-dimension scoring + display fields
│   ├── 12_contact_enrichment.py        # website + Apollo contacts
│   ├── 13_contact_augment.py           # ROC qualifying_party merge + Tavily merge
│   ├── 14_dossier_cards.py             # HTML dossiers + index
│   ├── 15_evidence_audit.py            # diagnostic: dump all cached evidence per contractor
│   ├── 16_tavily_contact_search.py     # Tavily-based contact discovery (pure extractor)
│   ├── 17_candidate_validator.py       # LLM validator: belongs/reject per candidate
│   └── 18_roc_bond_scraper.py          # ROC bond scraper: revenue bands from public record
│
├── data/
│   ├── 01_contractors/                 # Steps 01, 02, 02b, 02c outputs
│   │   ├── filtered.csv                # after 01
│   │   ├── enriched.csv                # after 02
│   │   ├── tier_1_enterprise.csv       # after 02b
│   │   ├── tier_1_clean.csv            # after 02c — the working pool
│   │   ├── tier_2_borderline.csv
│   │   ├── tier_3_small_shops.csv
│   │   ├── tier_4_rejects.csv
│   │   └── dropped/                    # sidecar audit files
│   │       ├── soleprop.csv            # sole-prop name drops
│   │       ├── dedup.csv               # place_id tiebreaker losers
│   │       └── geo.csv                 # out-of-bbox matches
│   │
│   ├── 02_enrichment/                  # Steps 03, 04 outputs
│   │   ├── fsm_detection.csv
│   │   ├── fsm_detection_sample.csv
│   │   ├── fsm_detection_errors.csv
│   │   ├── fsm_detection_phone_only_snippets.txt
│   │   └── apollo_signals.csv
│   │
│   ├── 03_hidden_gems/                 # Steps 05 through 11 outputs
│   │   ├── filtered_pool.csv           # after 05 — entry point for enrichment
│   │   ├── complete.csv                # after 06-10 — working state
│   │   ├── scored.csv                  # after 11 — dossier reads this
│   │   └── top_25.csv                  # convenience slice
│   │
│   ├── 04_contacts/                    # Steps 12, 13, 16 outputs
│   │   ├── enriched.csv                # 12 output
│   │   ├── augmented.csv               # 13 output (dossier reads this)
│   │   └── tavily_discovered.csv       # 16 output (merged into augmented)
│   │
│   ├── signals_raw/                    # all cached API responses
│   │   ├── apollo/                     # Apollo org enrich raw
│   │   ├── apollo_org/                 # per-domain Apollo org cache
│   │   ├── apollo_jobs/                # Apollo organization job postings
│   │   ├── serpapi_reviews/            # Google Maps review cache
│   │   ├── serpapi_jobs/               # Google Jobs cache (company-filtered)
│   │   ├── review_llm/                 # LLM review analysis cache (per-mention arrays + indexed_reviews)
│   │   ├── dispatch_delay/             # per-review dispatch extractions
│   │   ├── contacts/                   # website scrape + Apollo people
│   │   └── tavily_contacts/            # Tavily search cache (step 16)
│   │
│   └── snapshots/                      # all timestamped audit snapshots
│       ├── places/                     # Step 02
│       ├── velocity/                   # Step 06
│       ├── hiring/                     # Step 07
│       ├── review_nlp/                 # Step 08
│       ├── review_llm/                 # Step 08b
│       ├── dispatch_delay/             # Step 09
│       ├── burst/                      # Step 10
│       ├── scored/                     # Step 11
│       ├── contacts/                   # Steps 12, 13
│       └── apollo/                     # Step 04
│
├── deliverables/
│   ├── index.html                      # top 25 ranked overview
│   └── dossier_v4_*.html               # 25 individual dossiers
│
├── outputs/
│   └── evidence_audit_*.txt            # diagnostic output
│
├── tools/
│   └── webanalyze/                     # Go binary + fingerprints
│
├── ROC_Posting-List_Dual_2026-04-10.csv
├── ROC_Posting-List_Residential_2026-04-10.csv
├── CLAUDE.md                           # project rules and lessons
├── PIPELINE.md                         # this file
└── README.md                           # one-page summary
```

The `data/` layout mirrors the pipeline stages: numbered subdirs (`01_contractors/`, `02_enrichment/`, `03_hidden_gems/`, `04_contacts/`) hold CSV outputs that flow forward through the pipeline. `signals_raw/` holds the pristine cached API responses that any step can re-read without hitting the network. `snapshots/` holds timestamped audit copies of every enrichment step's output, never overwritten, used to diff state week-over-week.

## Appendix B — Key standing rules from CLAUDE.md

These rules govern all pipeline work on the project. Each exists because of a specific mistake or discovery made while building.

### Workflow rules

1. **Inspect before you build.** Load the input file and report schema, dtypes, null counts, and data quality issues before writing any processing script.
2. **Smoke test before scale.** Every API-hitting script supports `--limit N`. Run on 10–15 rows first, wait for approval before full-pool.
3. **Estimate cost before running.** For any paid API, calculate and print estimated cost before the run.
4. **Snapshot, don't overwrite.** Every enrichment step writes a timestamped snapshot plus a current-state file.
5. **Preserve everything.** Filters write dropped rows to sidecar files — never discard.

### Evidence handling rules

6. **Read from the source cache, not from rolled-up columns.** The render layer reads raw cached data directly. Aggregate columns in the scored CSV are for scoring and sorting only.
7. **Counts shown in headlines must match the evidence rendered beneath them.** Compute the list first, render every item, use `len(list)` for the headline.
8. **Reviews older than 6 months do not contribute to signals.** Apply the 180-day filter at LLM input, aggregation, and render layers.
9. **Signal-classification definitions live in one place per signal.** When two scripts classify the same signal, the thresholds must be identical.
10. **Never hardcode cultural-assumption lists.** No first-name lists, no keyword bags that encode regional/linguistic bias. Use the LLM for semantic judgment, or don't do the task.

### Data quality rules

11. **DBA over legal name.** Prefer the doing-business-as field when querying external APIs. 12% of contractors need this.
12. **Strip HVAC stopwords before string comparison.** `inc, llc, corp, air, hvac, heating, cooling, mechanical, services, the, and` all get stripped before computing match confidence.
13. **Closed-business filter is mandatory.** `place_business_status == 'CLOSED_PERMANENTLY'` drops before any expensive operation.
14. **The hottest signal is software absence.** Binary "no FSM detected" beats any proxy pain signal.
15. **Rating is useless as a scoring signal.** 74% of contractors are 4.5+. Use review count and velocity instead.
16. **Dedup tiebreaker chain.** Confidence → review count → older issue date.
17. **Maricopa bounding box**: lat 32.5–34.1, lon −113.4 to −111.0. Loose is safe.
