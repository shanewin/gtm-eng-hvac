# HVAC Signals — Project Rules

This file contains standing instructions for all pipeline work on this project. Read it at the start of every session and follow it without being reminded.

## Project context

Building a buying signal pipeline for Phoenix HVAC contractors. Working pool lives in `data/01_contractors/tier_1_clean.csv` (356 rows as of 2026-04-10). Downstream scripts read from this file and write new columns or produce new outputs. Never overwrite source data — always write new files.

The goal: a ranked top-25 list of Phoenix HVAC contractors most likely to be in a buying state for field service management software, with dossiers and opening lines for a sales rep. Used as a portfolio piece to land interviews at ServiceTitan-class vendors.

## Core working principles

### Inspect before you build
Before writing any script that processes data, load the input file and report exact columns, dtypes, row count, null counts, and any obvious data quality issues. Do this even for small scripts. Never assume a schema.

### Smoke test before scale
Every script that hits an API or scrapes the web must support a `--limit N` flag. Run it on 10–15 rows first, show the results, and wait for approval before running at scale. Never run an expensive operation against the full pool on the first execution.

### Estimate cost before running
For any script that hits a paid API, calculate and print the estimated cost before the run. Wait for approval before running against more than the smoke test set.

### Fail loudly, recover gracefully
Never let one bad row crash the pipeline. Catch exceptions per-row, log them to a sidecar error file, and continue. Print error counts in the end-of-run summary.

### Snapshot, don't overwrite
Every enrichment step writes two outputs:
- A timestamped snapshot: `data/[step]_snapshots/YYYY-MM-DD.csv` (never overwritten)
- A current-state file that downstream scripts read from (can be overwritten)

This lets future runs diff against past snapshots for week-over-week state change detection.

### Preserve everything
Never delete rows from source files. If a filter drops rows, write the dropped rows to a sidecar file (`*_dropped.csv`) so they can be reviewed or recovered later. We've already caught legitimate data in "rejected" buckets once.

### Think before you code
For any non-trivial script, explain the approach in plain English first. Flag anything suspicious in the data before writing code to process it.

### Flag surprises
If you find something unexpected (outliers, weird distributions, unexpected values), stop and report it before continuing. Don't silently work around it.

### No silent assumptions
If the plan references something that isn't in the data, ask instead of guessing. If a column doesn't exist, say so — don't make up a substitute.

## Evidence handling (standing rules)

These rules govern how dossiers, scorers, and any render-layer code reads data. Every one of them came from a specific bug where aggregate data and source data disagreed.

### Read from the source cache, not from rolled-up columns
When raw cached data is available, the render layer must read from the cache, not from an aggregate column in the scored CSV. Dispatch extractions are read from `data/signals_raw/dispatch_delay/{place_id}.json`, not `dispatch_category`. Review mention arrays are read from `data/signals_raw/review_llm/{place_id}.json`, not `llm_pain_evidence` string columns. Aggregates are for scoring and sorting only — never for rendering evidence.

**Why:** rolled-up columns drift. A pipeline step computes them once and they stick around even after the underlying data changes. Every dossier bug that surfaced wrong evidence traces back to reading a stale aggregate.

### Counts shown in headlines must match the evidence rendered beneath them
If a card says "4 complaints", there must be exactly 4 distinct pieces of evidence shown below it. If a card says "2 reviews mention switching", there must be exactly 2 dated quotes. The count and the evidence come from the same computation, not two different ones.

**Why:** multiple bugs shipped with headlines that inflated or deflated the real count. The fix is always the same shape: compute the list first, render every item, use `len(list)` for the headline.

### Reviews older than 6 months do not contribute to signals
Any signal derived from review content — pain scores, momentum, dispatch patterns, switcher mentions, one-person name counts — must filter input to the last 180 days before processing. This applies at the LLM input stage (scripts that send reviews to Claude), at the aggregation stage (dispatch stats), and at the render stage (as a defensive filter when reading caches).

**Why:** a pain complaint from 2023 says nothing about the contractor's current operations. Recency-filtered signals align with velocity metrics, which already use 90-day windows.

### Signal-classification definitions live in one place
When two scripts need to classify the same signal (example: `classify_dispatch_pattern` used by both `09_dispatch_delay.py` and `14_dossier_cards.py`), the classification thresholds must be identical across both scripts. Prefer a shared function. If that's infeasible, duplicate the function with a comment in both scripts pointing to the other, and test that they agree on every contractor before merging.

**Why:** two copies of a classifier will drift silently. A dispatch category labeled `dispatch_fast` in the CSV but rendered as `bimodal` on the dossier is a contradiction the reader can't reconcile.

### Never hardcode cultural-assumption lists
Do not hardcode lists of first names, last names, nicknames, gendered words, or language-specific keyword bags to drive semantic classification. Phoenix-area contractors include Hispanic, South Asian, Middle Eastern, and Anglo owners — a "common first names" list will misclassify most of them. If a task needs semantic judgment ("is this person a real owner?", "is this sentence a complaint?"), use the LLM (which already understands context) or do not do the task. Keyword matching is acceptable only when the keywords are operational jargon that has no cultural dimension (e.g., "dispatcher", "scheduling coordinator").

**Why:** a 300-name list was proposed and correctly rejected. It would have rejected Jose, Safwat, Sammy, Ahsan, Gerardo, and Jeramy — all real owners in the current top 25.

### Extract then validate, never fuzzy-match
Regex and search-based extractors should pull candidates exhaustively with no ownership judgment. A single LLM validator then decides which candidates belong to which contractor, and that decision is the single source of truth downstream. Do not run a second fuzzy check at render time — if the validator kept it, render it; if the validator rejected it, drop it. When a rendered claim turns out to be wrong, the fix is in the validator prompt, not in a new regex filter.

**Why:** character-level similarity was repeatedly matching "Cardinal Heating and Air Conditioning" (Wisconsin) to "Cardinal Heating & Cooling LLC" (Arizona) and pasting the wrong LinkedIn onto the dossier. Splitting extraction from validation made that class of error structurally impossible and gave every contact a written reason that can be audited later. Implemented in `pipeline/07_serpapi_hiring.py` (pure extractor for jobs), `pipeline/16_tavily_contact_search.py` (pure extractor for contacts), and `pipeline/17_candidate_validator.py` (single LLM validator). The validator's kept/rejected decisions are the single source of truth for both step 11 scoring (hiring counts) and step 14 dossier rendering (contacts and job postings).

**Exception:** exact brand-name matching (e.g. scanning job descriptions for "ServiceTitan" or "Housecall Pro" as an FSM-vendor disqualifier) is not fuzzy matching and is safe. The no-fuzzy-match rule applies to ownership judgments ("is this the same business?"), not to exact-token brand detection.

## Data quality checks required after every enrichment

These are lessons learned from the Tier 1 cleanup. Every new enrichment script must include these in its end-of-run summary:

### Geographic sanity
If the script pulls or modifies location data, verify lat/long falls within the Maricopa County bounding box (see constants below). Flag any rows outside. Never silently keep them.

### Match confidence
If the script matches records across sources (Google Places, BBB, Craigslist, etc.), compute a confidence score using normalized string comparison. Strip common stopwords before comparing: `inc`, `llc`, `corp`, `co`, `company`, `ltd`, `pllc`, `air`, `hvac`, `heating`, `cooling`, `refrigeration`, `mechanical`, `services`, `the`, `and`, `&`. Flag anything below 85 confidence.

### Duplicate detection
After any join or dedup, check for duplicate IDs (license_no, place_id, phone_normalized) and report any found. Use the tiebreaker chain below for resolution.

### Business status filter
Always filter out contractors with `place_business_status == 'CLOSED_PERMANENTLY'` before running expensive operations on them. Morales Air was a 100-confidence name match that was actually a closed business.

### Null handling
Scoring or filtering logic must handle null values explicitly. ~5% of rows have null ratings and review counts. Scripts must never crash on null.

## Dedup tiebreaker chain (standing rule)

When deduping contractor rows that share a `place_id` (multiple AZ ROC licenses that Google Places matched to the same business listing), apply this tiebreaker chain in order:

1. Higher `place_match_confidence` wins.
2. Higher `place_review_count` wins.
3. Older `issued_date` wins (earlier date = lower `license_no`).

**Why the third step exists.** Structurally-tied `place_match_confidence` + `place_review_count` is *expected* for `place_id` duplicates, not a rare edge case. Two ROC rows pointing to the same Google listing always share the same review count (because the reviews come from the same listing), so the review-count tiebreaker can never resolve anything on its own. The `issued_date` fallback is what actually breaks those ties.

**Why "older license wins" is the right fallback.** When a contractor holds multiple AZ ROC licenses that map to the same Google listing, the older license is almost always the original operating entity. Newer licenses on the same business are typically reorganizations, subsidiaries, or holding LLCs. Keeping the older row preserves the longer license history, which matters for downstream "years active" scoring.

**Precedent.** This rule was decided during the Tier 1 cleanup step after finding 4 `place_id` duplicate pairs in the 405-row working pool, 2 of which had tied confidence (100) and tied review counts. Implemented in `pipeline/02c_clean_tier_1.py`.

## Maricopa County bounding box

For geographic filtering of Google Places coordinates, use:

- Latitude: `32.5` to `34.1`
- Longitude: `-113.4` to `-111.0`

This is a slightly generous superset of the actual county bounds (~32.50–34.05 lat, -113.35 to -111.03 lon). All geographic outliers observed in production data have been clearly out-of-state (Dubai, NY, FL, CA, UT), so a loose box is safe and avoids borderline false drops.

## API call discipline

### Before calling any external API
- Confirm the API key is loaded from `.env`
- Respect rate limits (default to 0.5s sleep between calls unless the API docs say faster is fine)
- Use proper User-Agent headers on web scrapes
- Set timeouts (8 seconds for web fetches, 30 seconds for API calls)
- Retry once with exponential backoff on transient errors, then skip and log

### Google Places API (New)
Always specify a field mask. Don't pull fields we don't need — costs more and adds noise.

### Query normalization
When querying external APIs by name, first normalize:
- Strip punctuation: `,` `.` `/` `&`
- Collapse multiple spaces to single space
- Trim leading/trailing whitespace
- **Prefer `doing_business_as` over legal name when both exist.** DBA routing fires on ~12% of rows and catches several of the biggest brands in the pool (Collins Comfort Masters, American Home Water & Air, Rainforest Plumbing and Air). Without DBA routing, those big brands would either miss or bad-match.

## File organization

- `pipeline/` — numbered scripts that build the main pipeline (`01_`, `02_`, `02b_`, `02c_`, `03_`, etc.)
- `data/` — all CSVs, inputs and outputs
- `data/*_snapshots/` — timestamped snapshot files, never overwritten
- `data/*_dropped.csv` — sidecar files for audit, contain rows dropped by filters
- `docs/` — final outputs: HTML dossiers + index, served via GitHub Pages
- `outputs/` — intermediate outputs and diagnostics

## Standing lessons from past steps

### Signal quality lessons
1. **Rating is useless as a scoring signal.** 74% of contractors are 4.5+, median is 4.9. Use review *count* and *velocity*, not raw rating.
2. **Review count alone is weak.** It measures how long they've been collecting reviews, not whether they're in motion now. Review velocity (acceleration over 90 vs 90 days) is what matters.
3. **Qualification filters run before scoring.** Don't score contractors that will be disqualified anyway.
4. **The hottest signal is software absence.** A contractor with no detectable FSM platform and no online booking is by definition a prospect. Binary clarity beats proxy signals.

### Data lessons
1. **DBA field is critical.** Always prefer DBA over legal name in queries. 12% of rows need it; those 12% include flagship brands.
2. **Confidence scores fooled by shared suffixes.** Strip stopwords before string comparison.
3. **Google Places returns wrong matches silently.** Name match + geographic sanity check + business status check are all required to trust a match. The confidence score catches name mismatches. The bounding box catches wrong-location matches. The business_status filter catches closed businesses.
4. **Multi-state companies match to HQ.** A Phoenix contractor whose corporate HQ is in another state will return that HQ's Google listing. Bounding box filter is the fix.
5. **Keep rejected rows for audit.** Today's reject pool included legitimate contractors who just had bad Google matches. Sidecar files let us recover them.

### Pipeline lessons
1. **Smoke tests catch what summaries miss.** Early smoke tests showed 9/10 matched but actually only 6/10 were correct. The summary stat was a lie until we eyeballed the rows.
2. **Data quality issues compound.** Each enrichment step adds noise. Clean between steps, not at the end.
3. **CLAUDE.md is a living document.** Add new rules to this file as lessons emerge. Every rule here came from a specific mistake or discovery.