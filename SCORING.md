# Scoring Methodology

How a contractor gets ranked in the HVAC Signals top 25.

## The short version

Every contractor in the qualified pool gets a score across seven independent dimensions. The score is additive: positive signals add points, negative signals (evidence they've already solved the problem this software sells) subtract points. The dimensions are designed so that a contractor can't reach the top of the list on a single data point — they need multiple independent sources of evidence to agree.

Ranking is a sort by `score_total`, descending, with one hard exclusion: contractors whose job postings explicitly require experience with a named FSM platform (ServiceTitan, Jobber, Housecall Pro, etc.) are suppressed from the top 25 regardless of their score. They already bought — we don't pitch our own customers.

Two non-scoring display fields also appear on every dossier: a **size tier** (S/M/L/XL) computed from license tenure and review volume, and a **signal freshness** badge when 25%+ of the contractor's dated signals come from the last 30 days.

Every claim inside a score traces back to a specific cached piece of evidence — a review quote with a date, a job posting URL, a public-record license, a dispatch time extracted from review text. There are no opinion-based adjustments and no "gut feel" overrides.

## The seven dimensions

### 1. Direct pain · 0 to 40 points

Evidence that customers are actively complaining about operational failures — the conditions that push a contractor to buy field service management software because the status quo is visibly breaking.

| Signal | Points |
|---|---|
| LLM review analysis classified contractor as `active_pain` | +25 |
| LLM pain score (0–10 from review content) | up to +10 |
| Regex pain hits from review text | up to +5 |
| Strained dispatch pattern (median response time > 48h) | +10 |
| Two-speed dispatch pattern (≥25% same-day AND ≥25% week+) | +5 |
| Active crisis burst in last 30 days (3×+ baseline negative review volume) | +15 |
| Recent crisis burst in last 60 days | +10 |
| ≥25% of dispatch mentions are negative sentiment | +5 |

Hard cap: 40 points. Thin-sample discount applies (see below).

### 2. Scaling strain · 0 to 25 points

Evidence the business is growing faster than its infrastructure can handle — hiring, review velocity acceleration, and customer language about capacity pressure.

| Signal | Points |
|---|---|
| LLM classified as `scaling_strain` | +15 |
| Regex classified as `scaling_strain` (second classifier, independent) | +10 |
| LLM momentum score | up to +5 |
| Regex momentum hits | up to +5 |
| Hiring an ops role FSM software supports (dispatcher, scheduler, CSR) | up to +20 |
| Hiring a technician or installer | up to +6 |
| Review velocity category: `accelerating` | +10 |
| Review velocity category: `hot_new` | +8 |
| 90-day review volume more than 3× the prior 90 days | +3 |

Hard cap: 25 points. Thin-sample discount applies.

Hiring counts come from the LLM validator cache, not from fuzzy-matched job titles. A posting counts only if the validator confirmed it belongs to this contractor, with a written reason.

### 3. Demand pull · 0 to 20 points

A different shape of signal: contractors who are winning on reputation rather than scaling strain. Customers describe switching from other providers, the owner personally handles jobs, a single named person appears in most reviews. These contractors aren't in pain — they're at a capacity ceiling that only systems can break through.

| Signal | Points |
|---|---|
| Customer-switch mentions ("we left our old guy and came here") | up to +12 |
| Heavy founder involvement (owner named in review text) | +4 |
| Heavy key-person dependency (one named individual dominates reviews) | +4 |
| Scaling surge burst with a real baseline (≥0.5 reviews/week) | +4 |
| Fast dispatch *combined with* any of the above (capacity-ceiling signature) | +3 |

Hard cap: 20 points.

This dimension exists because early versions of the model ranked small one-person shops with excellent execution *below* dysfunctional larger contractors. That's backwards — the one-person shops are the ones whose next hire forces a software decision.

### 4. Multi-signal convergence bonus · 0 to 15 points

A bonus for contractors where multiple *independent* sources of evidence point the same direction. A contractor flagged by the LLM review analysis, the regex classifier, the hiring signal, *and* the dispatch signal is much more likely to be a real buyer than a contractor whose score came entirely from one source.

| Independent sources firing | Bonus |
|---|---|
| 4 or more | +15 |
| 3 | +10 |
| 2 | +5 |
| 0–1 | 0 |

Sources counted: LLM NLP classification, regex NLP classification, hiring signal, dispatch classification, burst category, review velocity. Six possible sources total.

### 5. Operational readiness · 0 to 10 points

Baseline indicators that this is a real, established business we can actually sell to. Not a buying-intent signal — a prerequisite signal. A contractor can have perfect pain evidence but if they have no website, no reviews, and one year in business, they're not a closeable prospect.

| Signal | Points |
|---|---|
| Has a website | +3 |
| Found in Apollo's business database | +2 |
| 100+ Google reviews | +2 |
| 10+ years on the AZ contractor license | +2 |
| Google rating 4.7 or higher | +1 |

Hard cap: 10 points.

### 6. ICP fit · 0 to 5 points

License scope from the AZ Registrar of Contractors, which encodes whether a contractor can do commercial work or is restricted to residential only. This is a direct fit signal for enterprise FSM buyers whose commercial-grade products (ServiceTitan, bigger Jobber tiers) target multi-truck shops serving commercial customers.

| Signal | Points |
|---|---|
| Dual-scope license (class code `CR-39`): commercial + residential capable | +5 |
| Residential-only license (`R-39` or `R-39R`) | +2 |
| Unknown / missing class code | 0 |

Hard cap: 5 points. Deliberately small — ICP fit is a modifier, not a headline. A Residential-only contractor with strong intent signals still outranks a Dual-scope contractor with nothing firing.

The distribution in the current top 25 is roughly 22 Dual / 3 Residential-only, which matches the broader pool split of ~90% Dual / 10% Residential in the 5–25 year HVAC license window.

### 7. Disqualifiers · −30 to 0 points

Evidence the contractor has *already* bought field service management software. These contractors aren't prospects — they're someone else's customer.

| Signal | Points |
|---|---|
| **Job posting requires a named FSM platform** (ServiceTitan, Jobber, Housecall Pro, FieldEdge, Workiz, etc.) | **−15 + hard exclusion from top 25** |
| LLM classified as `smooth_ops` (customers praising automated operations) | −10 |
| Regex classified as `smooth_ops` | −5 |
| LLM smooth-ops score ≥6 (text reminders, online booking, arrival ETAs mentioned) | −5 |
| Fast dispatch with no pain AND no demand-pull signals (they've already solved ops) | −5 |

Floor: −30 points (expanded from −15 in V2 so the FSM-customer flag can stack on top of other disqualifiers).

**The FSM-vendor disqualifier is the strongest signal in this category** because it's explicit proof, not inference. When a contractor's own job description says *"must have 2+ years ServiceTitan experience,"* they are definitively a ServiceTitan customer. The pipeline scans every cached SerpAPI job posting (title, description, and job highlights) for a list of FSM platform brand names and flags any match. Flagged contractors are suppressed from the top 25 entirely regardless of their positive-signal score. The -15 point deduction is kept on their row for audit purposes, but the hard sort order is what actually prevents them from appearing in a list a rep would act on.

The audit sidecar at [data/03_hidden_gems/already_fsm_dropped.csv](data/03_hidden_gems/already_fsm_dropped.csv) logs every contractor flagged this way, including the matched vendor name and a verbatim context window around the match, so the regex can be verified manually.

Brand names currently matched (exact-token, case-insensitive, word-boundary anchored):

> ServiceTitan, Housecall Pro, Jobber, FieldEdge, ServiceBridge, Workiz, FieldPulse, Service Fusion, Tradify, Kickserv, mHelpDesk, Synchroteam, GorillaDesk, WorkWave

This is exact-token matching, not fuzzy — brand names don't have cultural-assumption issues, so the no-fuzzy-match rule in `CLAUDE.md` doesn't apply here.

The "fast dispatch with no pain and no demand-pull" rule is surgically specific. A contractor executing fast dispatch with *demand-pull* signals (customer switches, owner mentions) is not disqualified — their fast dispatch is evidence the founder is carrying the business personally, which is the exact opposite conclusion.

## The thin-sample discount

When the LLM review analysis runs on fewer than 15 reviews, the direct-pain and scaling-strain scores are multiplied by a discount factor of `reviews_analyzed / 15`. A contractor with 6 reviews analyzed gets 40% credit on those two dimensions.

This exists because a single angry review can max out pain signals on a sparse sample. Without the discount, contractors with 5 reviews would outrank contractors with 40 reviews just because they had one bad week.

Demand pull, multi-signal convergence, operational readiness, and disqualifiers are *not* discounted — those dimensions draw on evidence other than review text volume.

## Confidence tier

Every contractor gets one of three confidence tiers, displayed alongside the score:

- **High confidence:** 3 or more signal sources firing AND at least 15 reviews analyzed by the LLM.
- **Medium confidence:** 2 or more signal sources firing AND at least 10 reviews analyzed.
- **Low confidence:** everything else, including any contractor with fewer than 10 reviews analyzed (hard cap — a thin sample can never be high-confidence no matter how many signals fire).

A high-score contractor with low confidence is interesting but not actionable. A medium-score contractor with high confidence is more reliable than either.

## Primary narrative

The scoring model also assigns each contractor a one-word story that describes *why* the score is what it is. This is what gets shown on the dossier header as a badge.

| Narrative | Fires when |
|---|---|
| `demand_pull` | Demand-pull dimension ≥12 AND dominates the other two |
| `active_pain` | Direct-pain dimension ≥20 AND exceeds scaling strain |
| `scaling_strain` | Scaling-strain dimension ≥15 AND exceeds direct pain |
| `mixed` | Both direct pain ≥10 AND scaling ≥10 |
| `light_signal` | Any of the three dimensions ≥5 but none cross the thresholds above |
| `unclear` | Nothing fires — low-signal contractor |

Two contractors can share the same total score and have completely different narratives. A rep pitching a `demand_pull` contractor runs a different call than a rep pitching an `active_pain` contractor.

## Non-scoring display fields

Two pieces of information appear on every dossier header but are deliberately kept out of the `score_total` because they're reference data, not buying-intent signals.

### Revenue band (from AZ ROC bond amount)

Arizona law ([ARS 32-1152](https://www.azleg.gov/ars/32/01152.htm)) requires every contractor to post a surety bond sized to their anticipated annual gross volume. The bond amount is public record, visible on every contractor's ROC detail page, and directly tied to reported revenue — not an estimate from a third-party database.

The pipeline scrapes the bond amount from the ROC website for every contractor in the pool (100% coverage) and maps it to a revenue band:

| Combined Bond Amount | Revenue Band |
|---|---|
| $50,000+ | $5M+ |
| $20,000–$49,999 | $1.5M–$5M |
| $9,000–$19,999 | $500K–$1.5M |
| $4,000–$8,999 | $150K–$500K |
| Under $4,000 | Under $150K |

For dual-scope (CR-39) contractors, the bond amount shown on the ROC page is the sum of the commercial bond and the residential bond. The mapping above accounts for that combined amount.

For contractors holding separate R-39 (residential) and C-39 (commercial) licenses instead of a single CR-39 dual, both bonds are scraped and combined. The scraper detects multi-license contractors by searching by license number and matching the correct Salesforce `licenseId` when multiple results appear.

This is **not a scoring signal** — it's a display field. A $500K contractor with multiple intent signals should still outrank a $5M contractor with none. But two contractors with similar scores may represent very different deals, and the revenue band lets a buyer route them to the right sales motion (enterprise vs SMB).

**How it's collected.** `pipeline/18_roc_bond_scraper.py` uses Playwright (headless Chromium) to render the ROC's Salesforce-hosted detail pages — there is no API. The scraper searches by license number, extracts the Salesforce `licenseId` from the search results, loads the detail page, and reads the bond amount. Cached per contractor at `data/signals_raw/roc_bonds/{place_id}.json`. Full pool scrape: ~12 minutes, $0 cost.

**Why this is better than Apollo or ZoomInfo revenue estimates:** those databases have 0/70 coverage for our hidden-gems pool. The ROC bond amount has 70/70 coverage, is government-mandated, and is updated when the contractor renews their license. It's not a model or an estimate — it's a legal disclosure.

### Size tier (`size_tier`)

A secondary size proxy computed from license tenure and Google review volume. Used as a fallback when bond data is unavailable (e.g. if the pipeline is ported to a state without volume-based bonds). Segments contractors into S / M / L / XL.

| Tier | Rule |
|---|---|
| **XL** | License ≥20 years AND ≥400 Google reviews |
| **L** | License ≥15 years OR ≥300 Google reviews |
| **M** | License ≥10 years OR ≥150 Google reviews |
| **S** | Everything else |

When the revenue band is available (which it is for 100% of the current pool), the revenue band takes priority on the dossier header. Size tier remains in the scored CSV as a secondary reference.

### Signal freshness (`signal_freshness`)

A render-time badge that fires when a meaningful share of a contractor's dated signal mentions (pain, momentum, switcher) come from the *last 30 days* rather than being spread evenly across the 180-day window.

| Badge | Fires when |
|---|---|
| **FRESH** (orange) | 50%+ of dated signals are from the last 30 days AND total ≥ 3 |
| **RECENT** (yellow) | 25%+ of dated signals are from the last 30 days AND total ≥ 3 |
| (none) | Everything else — the 180-day window is already recent enough |

Also not a scoring signal. The point is to let a rep sort the top 25 by "which of these are in pain *right now* versus in pain at some point in the last six months?" A contractor with 15 pain mentions concentrated in the last 30 days is a much hotter prospect than a contractor with 15 pain mentions spread across six months, and those two contractors can have identical `score_total` values today.

## Final rank

Sort descending by `score_total`, with one hard pre-filter: contractors flagged as `already_fsm_customer` (see the FSM-vendor disqualifier above) are pushed to the bottom of the list regardless of their positive-signal score. The top 25 become the delivered list; the other 45 qualified contractors remain in the pool for future ranking but don't get dossiers generated.

The rank is not stable across runs. When new reviews arrive or a new hiring signal fires, ranks shift. A fresh run can promote or demote contractors meaningfully — that's a feature, not a bug. The list reflects current state, not historical state.

## What the score does NOT try to do

Being honest about the edges:

1. **The score is not a prediction of deal size or close probability.** It predicts *how loud the buying signal is right now*. A high score says "this contractor has multiple active indicators of FSM-software purchase intent." It does not say "this contractor will sign in 90 days" — close probability depends on your sales motion, pricing, and the contractor's own timeline, none of which we can see.

2. **The score does not account for existing vendor relationships.** If a contractor just signed a three-year deal with a competing FSM platform and their website hasn't been updated yet, we can't see that. FSM detection runs against their public site; it can't see what they've bought behind the login.

3. **The score ignores deal fit beyond software absence.** We look for evidence they need FSM, not evidence they can afford it, will choose your product specifically, or are culturally ready for a digital tool. Those are human-in-the-loop questions.

4. **Review-based signals have a 180-day recency window.** Any pain, momentum, switcher, or dispatch evidence is computed from reviews in the last 6 months only. A pain complaint from 2023 doesn't say anything useful about the contractor's current operations, and the model refuses to count it.

5. **The model is calibrated to Phoenix-area HVAC, 2026.** Scores are comparable within the pool. Porting the thresholds to a different metro or a different trade would require re-calibration — the thin-sample cutoffs, velocity ratios, and hiring weights were all tuned on this specific dataset.

6. **Third-party firmographic databases don't cover these contractors.** Apollo's organization database returns 0/70 revenue or employee records for our hidden-gems pool. This is expected — these are sub-$10M private HVAC shops that commodity lead databases miss. Instead of estimated revenue from a third party, we use the **AZ ROC bond amount** — a government-mandated, volume-tiered surety bond that maps directly to the contractor's reported annual gross volume. Coverage: 70/70 (100%). See the "Revenue band" section above.

## How to read a score in practice

- **Score ≥ 40**, high confidence, `scaling_strain` or `active_pain`: top-of-list prospect. Multiple independent signals, real review volume, clear narrative. These are the calls you make first.
- **Score 25–40**, any narrative: worth the time. Some dimension is clearly firing.
- **Score 15–25**, `demand_pull`: don't dismiss. One-person shops executing fast. Smaller deal sizes on average but faster cycles.
- **Score 15–25**, `light_signal`: thin data. Worth a look if there's capacity, skip if there isn't.
- **Score < 15**: the score model can't make a confident statement. These don't appear in the top 25.
- **Any contractor with a FRESH badge**: call them first regardless of rank. The badge means their pain is *now*, not six months ago.
- **`size_tier = L` or `XL`**: route to the enterprise sales motion. `S` or `M`: route to SMB.
- **`score_icp_fit = +5` (Dual-scope)**: commercial customer segment is available to them, which matters for enterprise FSM pitches. `+2` (Residential-only) is a Jobber / Housecall Pro shape.

**Also on every dossier (not part of the scoring model):**

- **Referenced people.** First names of technicians, dispatchers, and office staff that customers mention in reviews (from `pipeline/08b_review_llm.py`). Rendered inline on the decision-maker card as "Also named in reviews — ask for any of these by first name." The owner is deduped so the same person doesn't appear twice. These are not a scoring signal — they're a sales-call aid.
- **Validated contacts.** Emails, phones, social URLs, and job postings, each with a written reason from the LLM validator explaining why it belongs to this contractor. No fuzzy matching — every contact on every dossier traces back to a specific validator decision.

The scoring model fits in one Python file ([pipeline/11_scoring.py](pipeline/11_scoring.py)), under 800 lines, with no opinion-based weights that can't be traced back to specific cached evidence. Every number in every dossier header can be reconstructed from the raw signal caches by re-running the scorer. That reproducibility is the point.
