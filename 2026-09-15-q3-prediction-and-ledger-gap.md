# Exit note — 15 September 2026

Pricing study state, one open defect, and a pre-registered prediction.
Written before Q3 results exist. Nothing below may be edited after this file is committed.

---

## 1. Open defect — rate ledger gap

**RESOLVED 18 September 2026 — see Appendix D.2.** `history/combined/rate_changes-2026-09.csv` now spans
2026-09-02 to 2026-09-17, 418,784 rows. The section below is left unedited as the record of the defect as
it stood at writing.

`history/rate_log_runs.csv` **ends at 2026-09-09.** Eleven rows, last entry 09-09.

Daily snapshots exist for 09-10 through 09-15 — `history/publicstorage/` is continuous — so no observation is lost and every event is recomputable. But the ledger has a hole across exactly the window containing the 14 September reset, which is the largest finding in the study.

**Action before any figure from the ledger is quoted anywhere:** rebuild across the full quarter, then re-derive the September event counts from the rebuilt ledger rather than from the ad-hoc analysis scripts.

Two related items:

- Local `publicstorage-*.log` files exist for 09-13 through 09-15. Earlier days came from Actions. First local run was 09-13.
- `history/rate_changes.csv` stops on 09-09 as well, not just the run ledger. Both need rebuilding.
- `pyarrow` / `fastparquet` are not installed in the local Python 3.14 environment, so `history/parquet/` cannot be read on this machine. The CSVs under `history/combined/` are the working path locally.

---

## 2. Coverage

Q3 2026 is 1 July – 30 September, 92 days.

The record opens **11 July**. April files are a pilot, not part of the series.

**CORRECTED 18 September 2026 — this section originally read 82 of 92 days. It is 77.** See Appendix D.

Two windows are unobserved, not one: 1–10 July, before collection began, and **26–30 August**, a five-day pause taken when the source's robots.txt and terms were found to have changed mid-project.

That gives **77 of 92 days** once collection runs to 30 September — 21 in July, 26 of 31 in August, 30 in September. Any claim about Q3 must say so. Collection must continue to the 30th; stopping early turns 77 into a smaller number still.

---

## 3. What is being predicted

Public Storage reports Q3 2026 in late October. Two reported line items are within reach of this dataset. Two are not, and are excluded deliberately.

**In scope**

- *Promotional discounts given* — the dataset observes promotion text and duration on every advertised unit daily.
- *New-customer contract rates* — contract rent excludes promotional discount, which makes it the nearest reported analogue to the advertised reference rate.

**Out of scope, and why**

- *Realized rent per occupied square foot* — dominated by existing tenants at in-place rates. Not observable here.
- *Same-store NOI* — contains costs. No visibility.
- *Occupancy* — `units_avail` is advertised website availability, not audited physical vacancy. The 12→13 September jump was subsequently shown not to be a local-enrichment artifact (Appendix A), but its count semantics and relationship to actual occupancy remain unverified. It cannot support an occupancy prediction.

---

## 4. The predictions

### P1 — Q3 promotional discounts given (primary)

Q2 2026 reported promotional discounts given **up 16.1% year over year.**

Observed: promotions were rich through July and August (30–50% off for four months). On 14 September, 35,235 promotions changed and multi-month percentage offers were largely replaced with $1 first month, first month 50% off, second month free, or no promotion. Modeled promotional value supplied fell ~49.7% on that day.

If its terms persisted unchanged through quarter-end, the September 14 reset would cover 17 of 92 quarter days. Persistence is not assumed: only 14–15 September were observed when this note was written, and this operator reprices frequently. The prediction rests on two promotion-heavy prior months plus the direction of the reset, not on a claim that the reset will remain unchanged through the 30th.

**P1: Q3 promotional discounts given will still be up year over year, but the year-over-year growth rate will be below Q2's +16.1%.**

Falsified if Q3 growth comes in at or above +16.1%, or if the line item declines outright.

### P2 — Q3 new-customer contract rates (primary)

Q2 reported new-customer contract rates **up 1.6% year over year** — the first positive reading since 2021.

Observed: rates rose sharply on 1 August (median +$34 across the wave; +$45 within the promotion cohort), partially reversed on 20 August, then fell to approximately 75% of prior level on 14 September across 31,109 matched SKUs.

The completed-quarter design would split **60** observed pre-reset days (11 July – 13 September, less the 26–30 August pause) and at most 17 post-reset days (14–30 September). **At writing, only 14–15 September are actually observed after the reset.** The unobserved windows — 1–10 July and 26–30 August — are in neither figure.

*Corrected 18 September 2026: this originally read 65 and assumed an unbroken August. See Appendix D.*

**P2: Q3 new-customer contract rate growth will come in below +1.6% year over year.**

Falsified if it meets or exceeds +1.6%.

### P3 — Q4 new-customer contract rates (the real bet)

The 14 September reset establishes the rate regime entering Q4, seventeen days before the quarter opens. It does not lock it — this operator reprices daily and has run coordinated events roughly every three to four weeks.

Management guided move-in rents to **positive low single digits**, against a prior assumption of negative mid-single digits.

*(An earlier draft of this file said "low double digits." That figure came from a search-result summary rather than the transcript and was wrong. Corrected before sealing. Mode D — a secondary source treated as authority.)*

**P3: Q4 2026 new-customer contract rates will be flat or negative year over year.**

This contradicts guidance, though by less than the earlier draft implied.

Falsified if Q4 new-customer contract rate growth is positive.

**What a failure would and would not establish.** If P3 fails, P3 is wrong. That is all it establishes on its own. Move-in mix, conversion from advertised to transacted, geographic weighting, and repricing between now and December are each sufficient to explain a miss without any defect in the dataset. Determining which requires the analysis that follows the result, not the result itself.

---

## 5. Measurement caveats, stated before the answer is known

**Contract rent is not the reference price.** Contract rent is the rate a renter actually agreed to. The reference price here is the figure displayed alongside a promotion on a public page. They are related and they are not the same measure. If P2 or P3 fail, this is the first place to look, and failing for this reason is a finding rather than an error.

**Advertised inventory is not rented inventory.** Every model here weights one hypothetical rental per advertised SKU. Reported metrics weight actual move-ins. A quarter whose move-ins skew toward small units will not match an equal-weighted advertised model.

**The offer engine does not control the reported metrics it will be graded against.** Same-store revenue and NOI are driven by existing tenants and length of stay. Advertised move-in pricing is customer acquisition. These predictions are deliberately confined to the two line items the dataset can actually see.

---

## 6. Publication state

**Updated 18 September 2026 with the actual sequence.**

The August event surfaced while the directory was running. A write-up was prepared. Before publishing
anything, robots.txt and the source's terms of use were re-read — and the terms had been **updated in May
2026, after this project began.** Collection stopped the same day. The last published snapshot is
**25 August**; the record then shows a five-day pause, 26–30 August, while the position was reviewed and
discussed.

What resumed on 31 August was not the same thing. **FindStorage — the directory, the product, the name —
is retired.** `findstorage.pages.dev` stands as a dated archive frozen at 25 August, the public post saying
collection stopped is accurate about the directory, and the Upwork history entry is accurate. None of that
is being walked back.

What continued is the research: a pricing study that observes advertised offers over time. It carries no
product name, sells nothing, and exists to answer a question rather than to list stores. That distinction is
the whole of the decision — the product died, the measurement did not.

**Current position, stated plainly rather than implied:**

- The two earlier constraints — operator unnamed, nothing dated after 25 August — are **retired**, and
  deliberately so. `/storage` and `/permits` on the portfolio name Public Storage and carry September
  figures because a study that hides its subject cannot be audited.
- Those pages are built and published. This is not a pending decision.
- Anything describing the *directory* still says it ended on 25 August, because it did.
- Collection method is unchanged from what the project always did: published pages only, robots.txt
  re-read each run, a request floor held, no account, nothing circumvented.

No legal conclusion is asserted here about the terms, before or after May. What is recorded is conduct:
what was found, when it was found, what stopped, and what was decided afterwards.

*Written 2026-09-15, before Q3 results exist. Sealed on commit — until this file is tracked it is a draft, not a pre-registration. Corrections to method may be appended below with their own date; the predictions themselves are not to be revised.*

---

## Appendix A — 15 September 2026, same day, before sealing

### A.1 A misread number, and how it was caught

The 12→13 September advertised-availability jump had been carried through the analysis as **"+27,528 units"** and read as if it were 27,528 new SKUs. It is not. It is the **sum of the `count` field** across advertised listings.

Decomposition:

| Component | Units |
|---|---:|
| SKU records added (5,116 records) | +14,191 |
| — of which returning SKUs (4,830 records, 94.4%) | +13,731 |
| — of which never previously observed (286 records, 5.6%) | +460 |
| SKU records removed (2,574 records) | −4,981 |
| Count increases on SKUs present both days | +18,318 |
| **Net** | **+27,528** |

Genuinely first-observed SKUs account for **460 units — 1.7%** of the apparent jump. Unique store/SKU records went 58,232 → 60,774, a net +2,542.

The 3,275-store figure was likewise real and misread: 3,275 stores had a higher *summed count*, but only 1,857 had more *SKU records*.

**How it was caught:** by asking whether new units had been created in order to price them differently, and testing SKU identity against every prior snapshot rather than trusting the aggregate. The aggregate had been correct arithmetic on the wrong unit of measurement for most of a day, and was one revision away from being sealed into this file.

Audit: `analysis/september13_sku_jump.py`.

### A.2 A hypothesis, killed from the code

It was proposed that the 13 September increase reflected the collection move to local execution — that fuller store-page enrichment surfaced units GitHub Actions had been missing.

**Refuted, twice.** Phase 6's pricing API creates unit records; Phase 7's store-page enrichment can only attach `attrs`, `price_min` and `price_max` to SKUs that already exist. It cannot surface or create units. Independently, Actions had already collected 61,400 records on 1 September and 61,731 on 2 September — both above the 13th's 60,774. There was no unreached universe.

A time- or environment-dependent difference in the pricing API's own response remains technically possible. That would be a Phase 6 phenomenon and is not what was proposed.

### A.3 13 and 14 September are one event

Testing the count-expanded cohort against the next day's reset:

| | Count-expanded | Portfolio-wide | Ratio |
|---|---:|---:|---:|
| Received a price cut | 70.5% | 52.3% | 1.35× |
| Changed promotion | 76.0% | 59.2% | 1.28× |
| Changed both | 70.2% | — | — |

Median headline price on that cohort fell to **75.1%** of its 13 September level. Newly appearing SKUs behaved almost identically: 69.9% cut, 77.3% promotion change, median to 75.2%.

Of 3,018 stores that expanded counts on retained SKUs, **82.0% cut at least one price the following day** and **90.9% changed at least one promotion**.

The exposure persisted through the reset — that cohort carried 59,526 advertised units on the 12th, 83,055 on the 13th, and 82,704 on the 14th. **98.5% of the increase survived the repricing**, so this was not a scrape hollowing and refilling.

### A.4 The control that makes A.3 meaningful

Modeled four-month customer cost, 13 → 14 September:

| Cohort | Change |
|---|---:|
| Newly appearing SKUs | **+5.1%** |
| Count-expanded SKUs | **+3.3%** |
| Flat-count control | **+0.3%** |

The portfolio-wide **+1.1%** stated in §4 and elsewhere is an average across these populations and conceals both. The units whose availability was expanded are the units whose four-month economics worsened most, by roughly a factor of ten against the flat-count control.

Newly appearing SKUs carried no pricing premium against retained units of the same size at the same store: median ratio 1.013, 52.5% priced higher, 43.4% lower.

Audit: `analysis/september13_14_sequence.py`.

### A.5 Two things not yet checked

**Selection effect — RUN, and it changed the finding. See A.7.**

**Count semantics — open.** A.1 establishes that the jump is a count-field increase, not inventory creation. It does not establish that the count field still means what it meant on the 12th. Compare the *distribution* of `count` values across the two days rather than the sum: a real availability release shifts the same shape, a semantics change alters it. Not run.

Until the remaining count-semantics test is run, the language in A.3 should read *coordinated portfolio execution* and not *planned campaign*. The selection caveat was tested and resolved at the store level in Appendices B and C.

### A.6 Effect on the sealed predictions

**None. P1, P2 and P3 stand exactly as written in §4.**

A.3 and A.4 bear on P1: more advertised units carrying weaker promotional terms means that, if they convert, promotional discounts given falls faster than the raw promotion-change count implied. That marginally strengthens P1 — which is precisely the kind of adjustment a pre-registration exists to forbid. Recorded here, dated, and not applied.

---

## Appendix B — 15 September 2026, stratified test

### B.1 The selection test, run

A.5 flagged that the count-expanded and flat-count cohorts were not randomly assigned, and that A.4's +3.3% might describe *which units were expanded* rather than a targeting decision. The test was to stratify.

**Within market and size — the gap survives.** Using the project's ZIP3 market definition, across 304 markets containing both cohorts' 10x10s (1,816 expanded observations, 6,581 flat):

| | Value |
|---|---:|
| Composition-controlled four-month cost gap | **+3.59%** |
| Bootstrap 95% interval | +2.93% to +4.19% |
| Additional probability of a price cut | +17.7 pts |
| Additional probability of a promotion change | +16.7 pts |

City/state definitions give the same answer: +3.06% (interval +2.38% to +3.78%), +13.6 pts, +12.4 pts.

So weaker markets and a different size mix do **not** explain A.4.

**Within store and size — the gap vanishes.**

| Comparison | Four-month gap | Price-cut gap | Promo-change gap |
|---|---:|---:|---:|
| All sizes | −0.75% | +0.4 pts | +0.7 pts |
| 10x10 only | −0.27% | 0.0 pts | +0.8 pts |

The 10x10 interval is −0.85% to +0.31% — effectively zero.

Audit: `analysis/september13_14_stratified.py`.

### B.2 What that means, and the revision it forces

The variance is **between** stores, not inside them. Public Storage did not selectively reprice the particular SKUs whose counts rose while sparing flat-count SKUs standing beside them in the same store. Once a store was selected, its whole offer set received substantially the same reset.

**A.3 and A.4 are hereby revised.** The two-stage sequence is real and the customer-cost gap is real, but the coordination is at the **store** level, not the SKU level:

> Public Storage expanded advertised availability at a selected group of stores on 13 September, then repriced the broader offer set at those stores the following day. The relationship survives comparison among identical unit sizes within the same markets and disappears between SKUs inside the same store. The store, not the individual listing, is the level at which the two actions were coordinated.

This is a more precise finding than A.3's, and a slightly smaller one. A.3's language implying SKU-level targeting should not be quoted.

### B.3 Effect on planned-versus-reactive

Still not distinguished, but the shape has tilted. A human campaign would more plausibly be organised geographically or brand-wide. Store as the decision unit, with everything inside it swept together, is the shape a centralised system reading per-store signals produces.

That is a less dramatic reading than a planned campaign and a more credible one. "Reacted independently and locally" remains dead — the synchronisation across 42–43 states on consecutive days rules it out.

### B.4 What is now open

**Store selection is the object of study.** If stores were chosen, the question is which and on what basis, and the data to answer it is already on disk. Compare the 3,018 expanded stores against the rest across the fortnight *preceding* 13 September:

- advertised availability trend (`units_avail`, `listings`, daily, per store)
- price level relative to their own market's median
- promotional richness going in
- participation in the 1 August event

A shared observable characteristic in the run-up is the closest available approach to the trigger condition.

**Count semantics — still open**, unchanged from A.5. Compare the *distribution* of `count` values on the 12th against the 13th, not the sum.

**Power on the null.** B.1's within-store result is a null, and a null is a claim. It should be published with the number of contributing stores and the median paired observations per store. The interval is tight enough that this looks adequate, but it is not yet stated.

### B.5 Effect on the sealed predictions

**None. P1, P2 and P3 stand as written in §4.**

---

## Appendix C — 15 September 2026, store selection and the negative control

### C.1 The risk this appendix exists to address

B.4 opened the question of *which* stores were selected. Answering it introduces a defect the answer cannot fix on its own: the cohort is **defined by 13 September behaviour and then examined backwards**. Any characteristic correlated with being treated will appear to predict treatment, because the cohort was drawn from the treated.

If this operator's revenue management generally works on expensive, promotion-rich stores, then every wave-defined cohort would carry that profile and the finding would collapse to *stores that get treated tend to be stores that get treated*.

So the profile is recorded below only because a counter-cohort was run first.

### C.2 What distinguished the 3,018 selected stores

Measured on information available **before** 13 September:

| | Selected | Other |
|---|---:|---:|
| Median offer vs ZIP3-and-size peer median | **+1.4% above** | 5.1% below |
| Mean market-adjusted price gap | **+10.0%** (95% CI +8.5% to +11.7%) | — |
| Mean modeled four-month discount | **34.4%** | 24.6% |
| Within-ZIP3-and-size promotional gap | **+7.2 pts** | — |
| Median advertised availability, 12 Sept | **39** | 48 |
| Availability trend, 1–12 Sept | **−4.35%** | −2.00% |
| Participated in the 1 August event | **21.5%** | 7.1% |

Displayed availability at the selected stores was **tightening faster** than elsewhere, not expanding. The obvious trigger — *the system found stores where availability was already climbing* — is rejected by its own direction.

Not merely August carryover: among stores that sat out August, the September-selected ones were still 9.9% more expensive relative to market, carried 6.5 points more promotional value, and showed a steeper pre-event availability decline.

Audit: `analysis/september13_store_selection.py`.

### C.3 The segment has a six-week history

| | 31 July | 25 Aug | 12 Sept |
|---|---:|---:|---:|
| Price premium vs other stores, market-adjusted | +4.5% | +10.0% | +10.0% |
| Four-month promotional-value advantage | **−3.7 pts** | +2.7 pts | **+7.3 pts** |
| Median advertised availability, selected | 38 | 34 | 39 |
| Median advertised availability, other | 45 | 45 | 48 |

The promotional line is the one that matters. In July these stores were more expensive *and* had **weaker** promotions. By 12 September they were more expensive *and* had substantially **richer** ones. That is the price-into-promotion movement documented elsewhere in this study, running over six weeks rather than two days.

Availability ran in three stages: up 3.6% at selected stores over 23–31 July while others fell 0.9%; down 13.8% over 1–25 August against 7.1%; down 4.4% over 1–12 September against 2.0%. Then abruptly expanded on the 13th.

Audit: `analysis/september13_historical_cohort.py`.

### C.4 The negative control — inverted

A cohort was defined from the **24 July** wave and run through the identical backward analysis.

| Cohort, immediately before its own event | Market-relative price | Within-market promo gap | Advertised availability |
|---|---:|---:|---:|
| 13 September selected | **+10.0%** | **+7.2 pts** | **Lower** — 39 vs 48 |
| 24 July treated | **−8.6%** | **−1.7 pts** | **Higher** — 46 vs 26 |

The pre-event trends invert as well: September's cohort was shedding availability *faster* than its complement (−4.35% vs −2.00%); July 24's was shedding it *more slowly* (−5.72% vs −9.84%).

**This falsifies the artifact explanation.** Wave-defined cohorts are not uniformly expensive, promotion-rich and tight. One is the opposite on every measured axis.

Audit: `analysis/july24_counter_cohort.py`, including the July coverage correction for the smaller early catalog.

### C.5 Two lanes, alternating

The clearest single observation in this appendix, and the most novel claim the study supports.

On **23 July**, 97.2% of the stores that would be the *control* lane the next day were touched, against only 65.7% of the stores that would be *treated*. On **24 July**, treatment flipped to the other lane.

Forward: 24 July treated stores entered the September cohort at 60.2%; 24 July control stores entered it at 75.1%.

The July 24 treated lane then stayed roughly 8–9% below market through September with consistently greater displayed availability, while its promotional treatment rotated: 1.7 points poorer before treatment, 4.0 points richer by 31 July, roughly neutral by 25 August, 2.2 points poorer again by 12 September.

Across the broader waves, selected stores were touched by 87.5% of eligible July/August waves at the median against 75.0% for others — but the spread is not uniform. July 23 (+26.0 pts), July 28 (+14.2), August 1 (+24.8), August 16 (+34.8), August 22 (+37.5) and August 25 (+26.9) favoured the September cohort. July 24 and 25 favoured the other lane.

**Two commercially distinct populations receiving alternating treatments on consecutive days over six weeks, with diverging price and promotion trajectories.**

### C.6 What C.5 is not

It is not established to be an A/B test. A phased rollout, a regional scheduling rule, or segmentation by store characteristics all produce the same visible pattern. The segments were also **already unbalanced at 31 July** (+4.5% price gap), which argues against clean randomisation and toward segmentation that predates the observation window entirely — the record opens 11 July and the divergence is visible by 23 July, so the question of when these lanes were formed cannot be answered from this data.

The selection rule itself remains unknown. Demand, occupancy, conversion, campaign assignment or another private signal are all consistent with what is observed.

### C.7 Language discipline for anything quoted from A–C

- **"Displayed availability tightened."** Not "occupancy held." Physical occupancy is not observed.
- **"Consistent with testing whether a cheaper-looking monthly rate converts better."** Not "they were testing." The data establish treatment structure and modeled customer cost. They do not establish objective or conversion outcome.
- **"Coordinated portfolio execution."** Not "planned campaign." See B.3.
- **"Store-level, not SKU-level."** See B.2. A.3's phrasing should not be quoted.

### C.8 Still open

- **Count semantics**, unchanged since A.5. Compare the distribution of `count` values on 12 vs 13 September, not the sum.
- **Power on B.1's null.** Number of contributing stores and median paired observations per store, stated.
- **A second counter-cohort.** C.4 rests on one inverted control. One is decisively better than none; a third cohort — the 20 August wave is available — would establish whether inversion or similarity is the norm.
- **When the lanes were formed.** Not answerable from a record opening 11 July.

### C.9 Effect on the sealed predictions

**None. P1, P2 and P3 stand as written in §4.**

Appendices A, B and C describe the mechanism. They do not touch what is predicted about the reported Q3 line items, and the fact that the mechanism now looks considerably more deliberate than it did this morning is not a reason to revise a forecast that was made before it was understood.

---

## Appendix D — 16 September 2026, advertised savings and the reference-price problem

### D.1 What the 1 August event did — and did not do

The exact event cohort contains **4,904 offers** that both moved onto “40% off For 4 Month” and received a higher headline/reference rate on 1 August.

| Four-month accounting | Before | After | Change |
|---|---:|---:|---:|
| Headline value before promotions | $2,934,376 | $3,969,380 | **+35.3%** |
| Modeled promotional discount supplied | $501,867 | $1,587,752 | **+216.4%** |
| Modeled customer cost | $2,432,509 | $2,381,628 | **−2.1%** |

The new discounted monthly payment, `0.6 × new reference rate`, was lower than the old **undiscounted** rate on all 4,904 offers: median ratio 0.858 and aggregate ratio 0.812. It would be wrong to say that the new sale price exceeded the old sticker price.

That is not the relevant customer comparison for most offers. **4,491 of 4,904 — 91.6% — already carried a promotion.** Against the promotional offer it replaced:

- 73.7% cost more in month one; median difference **+$37**.
- 55.8% cost more over four months; median difference **+$2**.
- 96% cost more by month eight, after the temporary four-month discount expired into the higher full rate.

The aggregate four-month reduction and the majority-offer increase coexist because they are different statistics: large reductions on a smaller set of expensive offers outweigh smaller increases spread across more offers.

The accurate August description is:

> Public Storage raised underlying reference rates by 35.3% and replaced existing promotions with a more visually prominent 40%-off offer. The discounted monthly amount remained below the former undiscounted rate, but most affected customers would initially pay more than under the promotion it replaced, and almost all would pay more after the temporary discount expired.

Audit: `analysis/promo_cost_model.py`.

### D.2 One exact advertisement and its adjacent history

Public Storage store **235**, 2065 Placentia Avenue, Costa Mesa, California. Exact SKU **V_1455679** (normalized as 5×15; presented on the live page as 7.5×10):

| Snapshot | Headline/reference rate | Promotion |
|---|---:|---|
| 20 August | $161 | $1 first month rent |
| 21 August | $161 | $1 first month rent |
| 22 August | $237 | $1 first month rent |
| 23 August | $237 | $1 first month rent |
| 24 August | $237 | 40% off for four months |
| 25 August | $161 | second month free |

On 24 August, the live page displayed:

- “Month 1–4 40% OFF”
- the $237 full monthly rate struck through
- approximately $142 for months 1–4
- “Months 5–12 In-Store Rent” at the full rate
- “Total Estimated 12-Month Savings” of **$380**

The operator's arithmetic was internally consistent with its chosen reference rate:

`4 × 40% × $237 = $379.20`, displayed as approximately **$380**.

But against the $161 rate observed immediately before and after the elevated window, the promotional payment represented only:

`4 × ($161 − $142.20) = $75.20`.

Approximately **$304 — 80% of the displayed $380 savings — depended on the difference between the temporary $237 reference rate and the adjacent $161 rate.**

The customer-cost comparison is sharper still:

| Offer | Modeled four-month rent |
|---|---:|
| 20 August: $1 first month, then $161 | $484.00 |
| 24 August: four months at 60% of $237 | $568.80 |
| 25 August: second month free at $161 | $483.00 |

The offer presented as providing **$380 in estimated savings cost approximately $85 more over four months than either adjacent offer on the exact same SKU.**

### D.3 What is preserved, and what is not

The Git snapshots independently preserve the store, exact SKU, rate and promotion sequence in D.2. The “Total Estimated 12-Month Savings” label and the $110/$66/$176, $165/$99/$264 and $237/$142/$380 examples were transcribed during a live-page inspection on 24 August and recorded contemporaneously in `analysis/promo_cost_model.py`, `private/aug1-review.html` and the dashboard.

The scraper schema did **not** retain the savings field, raw HTML or a screenshot of that particular Public Storage page. The PNG currently in `Claude outputs/` is a Storage Pricer dashboard screenshot and is not evidence of the advertisement. Therefore:

- the price and promotion history is independently reproducible from the repository;
- the savings arithmetic and live-page language are contemporaneously documented;
- the precise advertisement is not supported by a preserved first-party page capture and should not be described as though it were.

Any later publication should state this evidence hierarchy. Recovering an archived first-party capture would materially strengthen the advertising analysis; silently treating the transcription as a screenshot would weaken it.

### D.4 What the evidence supports

Strongest factual statement:

> Public Storage displayed $380 in estimated savings against a $237 reference rate. The exact unit was offered at $161 immediately before the elevated-rate window and returned to $161 the following day. Relative to that adjacent rate, approximately $75 represented a lower promotional payment and approximately $304 of the displayed savings depended on the temporary comparison-price increase.

Strongest portfolio statement:

> In August, Public Storage increased the event cohort's headline/reference value by 35.3% while increasing displayed promotional value by 216.4%. In September it moved the other direction, cutting headline rates while withdrawing enough promotional value that modeled four-month customer cost rose. Across both events, the number made visually salient to the customer moved independently of the modeled introductory economics.

What is **not** established:

- Intent — the sequence is consistent with manufacturing a larger-looking savings claim, but the data do not establish why the reference rate was raised.
- A legal conclusion about deceptive advertising — the displayed arithmetic is correct against the contemporaneous $237 reference rate; the unresolved question is whether that temporary reference rate was a meaningful comparison basis.
- Shareholder deception — no materially false investor statement or required omission has been identified. The data expose customer-facing mechanics behind the centralized revenue-management system, not securities fraud.

Language permitted in the portfolio:

- **“A temporarily elevated reference rate produced a much larger advertised savings figure.”**
- **“The offer advertised as saving $380 cost about $85 more over four months than the adjacent offers on the same unit.”**
- **“The evidence raises a reference-price question.”**

Language not supported:

- “The discounted price exceeded the old undiscounted price.”
- “Public Storage raised the rate in order to deceive customers.”
- “Public Storage misled shareholders.”

### D.5 Effect on the sealed predictions

**None. P1, P2 and P3 stand as written in §4.**

Appendix D concerns presentation, reference-price construction and modeled customer cost. It does not add a new reported Q3 metric or justify changing a prediction after registration.

---

## Appendix D — 18 September 2026, corrections and the volume/direction distinction

### D.1 Coverage was overstated by five days

§2 originally read **82 of 92** observed days of Q3. It is **77**.

The error: July 11 → September 30 was computed as a continuous range. August is not continuous. `history/parquet/stores_daily/` runs 2026-08-01 through 2026-08-25, then jumps to 2026-08-31 — a five-day pause taken when the source's robots.txt and terms were found to have been rewritten mid-project.

Two unobserved windows, not one:

| Window | Days | Reason |
|---|---:|---|
| 1–10 July | 10 | Before collection began |
| 26–30 August | 5 | Deliberate pause during terms review |

21 July days + 26 August days + 30 September days = **77 of 92**.

The pre-reset split in §4 was wrong the same way: **60**, not 65.

**How it was caught:** not internally. The gap was visible in a directory listing that had been read days earlier and never counted. A range assumed to be continuous is the same failure as a number assumed to be current.

*The pause itself is not a defect and should be stated plainly in any write-up. Finding that terms had changed mid-project, stopping the same day, and checking before resuming is the strongest paragraph available about this dataset's provenance.*

### D.2 The ledger gap is closed

A.1 and §1 recorded `rate_changes.csv` and `rate_log_runs.csv` stopping at 2026-09-09. **Resolved.** `history/combined/rate_changes-2026-09.csv` now spans 2026-09-02 to 2026-09-17, 418,784 rows. September event counts are now natively derivable and no longer need reconstruction from the analysis scripts.

### D.3 The biggest day was not the event

**16 September produced more price changes than 14 September — and is not a repricing event.**

| | 14 Sept | 16 Sept |
|---|---:|---:|
| Price events | 31,244 | **50,740** |
| Share of matched inventory | ~55% | **89.6%** |
| Moved **down** | **99.6%** | 45.6% |
| Moved up | 0.4% | 54.4% |
| Median change | **−$32.00** | **+$1.00** |
| Median new ÷ old | **0.7500** | 1.0192 |
| Promotion changes | **35,235** | 1,670 |

*(89.6% = 50,740 price events ÷ 56,620 SKUs present in both the 15 and 16 September snapshots.)*

**Why this matters, stated plainly.**

An event count answers one question: *how many things moved?* It cannot answer *did anything get decided?*

On 14 September, 99.6% of changes went the same direction, the median landed on exactly 75% of the prior price, and 35,235 promotions were rewritten alongside. Thirty-one thousand units did not independently arrive at three-quarters of their previous price on the same Monday. That is a decision, executed.

On 16 September, nearly twice as many prices moved — 89.6% of everything tracked — and they moved both ways, by a median of one dollar, with almost no promotional activity. That is a revenue-management system doing its ordinary work at high volume. Lots of motion, no direction.

Had the study been built on event counts alone, 16 September would have been the headline and the finding would have been wrong. The three measures that separate them are **direction share, median magnitude, and whether promotions moved too** — and none of them is visible in a count.

This also sharpens the bimodality established in §3. Quiet days and wave days were distinguished by *share of inventory touched*. 16 September has a wave day's share and a quiet day's character, which means share alone is not sufficient either. The classifier for a coordinated event needs direction and promotion coupling, not volume.

### D.4 The Costa Mesa unit is a 5x15

The published case study's advertisement table labels the $237 unit **7.5×10**. The record says **5x15**.

`history/parquet/store-sizes/2026-08-24.parquet`, store 235:

| size | price |
|---|---:|
| 5x5 | $110 |
| 5x10 | $165 |
| **5x15** | **$237** |

The first two rows match the published table on both size and price. The third matches on price only.

This is not a normalisation artifact. The string `7.5` appears in none of the ten size labels used across 30,000+ rows that day, and nowhere in `daily_scraper.py` or `storage_pipeline.py`. Size is passed through as returned by the pricing API; no mapping exists that could produce 5x15 from 7.5×10.

**Use 5x15** unless an original screenshot shows the page displayed otherwise. It is the value that is re-derivable, and that is the standard this study is held to.

### D.5 Two figures circulating in draft outlines are wrong

From `history/combined/daily-2026-09.csv`, median `cheapest_10x10` on 2026-09-17:

| Brand | Circulating | Actual |
|---|---:|---:|
| U-Haul | $159.95 | **$149.95** |
| Public Storage | $100 | **$95** |

The U-Haul premium is **57.8%**, not 60%. Others that date: CubeSmart $86, StorageMart $89, Storage Sense $89, SmartStop $76, independents $85.

Confirmed exactly as circulated: SmartStop carries **no promotional text at all** — 2,512 units on 17 September, every `promo` field empty.

Not verifiable: SKU `V_1455679`. `history/publicstorage/` contains 17 files, all September; no August raw snapshots exist. The Costa Mesa sequence in D.4 came from parquet instead. Absence here is a limit of what can be asked, not evidence.

### D.6 Effect on the sealed predictions

**None. P1, P2 and P3 stand as written in §4.**

---

## Appendix E — 18 September 2026, the reset is walked back

### E.1 An event, four days after the event

| | 14 Sept | 16 Sept | 17 Sept | **18 Sept** |
|---|---:|---:|---:|---:|
| Price events | 31,244 | 50,740 | **0** | 22,772 |
| Moved down | 99.6% | 45.6% | — | **17.3%** |
| Moved up | 0.4% | 54.4% | — | **82.7%** |
| Median change | −$32.00 | +$1.00 | — | **+$12.00** |
| Median new ÷ old | **0.7500** | 1.0192 | — | **1.1176** |
| Promotion changes | 35,235 | 1,670 | 1,719 | **17,935** |

Direction, magnitude and promotion coupling all fire on 18 September. By the classifier in D.3 it is a coordinated event — the same signature as 1 August and 14 September, pointed upward.

**17 September is its own small finding.** Zero price changes, 1,719 promotion changes. The fields moved independently the day before moving together, which is the July 28 control repeating itself unprompted eleven days after it was first cited.

### E.2 Matched panel — what actually happened to the listings

Restricted to the **52,577 SKUs present on 13, 14 and 18 September**:

| Span | Median ratio |
|---|---:|
| 13 → 14 September | 0.7667 |
| 14 → 18 September | 1.0435 |
| **13 → 18 September, net** | **0.9800** |

Two percent below the pre-reset level, not twenty-five.

*Panel note: this is the three-day intersection — SKUs present on 13, 14 and 18 September, n = 52,577. The
four-day intersection, adding 15 September, is a slightly smaller panel (n = 51,921) and gives 1.0000 →
0.7667 → 0.7667 → **0.9797**. Both are correct; any published figure must state which panel it came from.
The portfolio `/storage` timeline uses the four-day panel because it displays four dates.*

**But the net is a cancellation, not a return.** Against their 13 September price on 18 September:

| | share |
|---|---:|
| Same price | 8.5% |
| **Lower** | **54.0%** |
| **Higher** | **37.5%** |

And the cut was not reversed. Of the **27,525 SKUs cut on 14 September**, only **2.2%** are back at their pre-cut price, with a median at **0.9103** — still 9% down. The listings that rose on the 18th are substantially *different listings* from those that fell on the 14th.

This is a reshuffle. It is the mechanism already recorded in the published case study — *"individual stores move decisively and in opposite directions within the same wave, so the national median can sit flat while most of the book moves under it"* — observed across five days instead of one.

### E.3 Why the medians must not be compounded

Naive compounding of the two headline ratios gives **0.7500 × 1.1176 = 0.838**, or 16% down.

The matched-panel answer is **0.9800**, or 2% down.

**An eighteen-point error**, produced by multiplying two medians computed over different populations on different days. This is the same defect as the 12→13 September availability figure in A.1 — arithmetic that reconciles and describes nothing. Any figure spanning more than one day must come from a matched panel or not be quoted.

### E.4 Effect on the sealed predictions

**P1, P2 and P3 stand exactly as written in §4. Nothing here revises them.**

P3's stated premise is weakened, and the seal is what makes that interesting rather than awkward. §4 said, before any of this was observable:

> *"The 14 September reset establishes the rate regime entering Q4, seventeen days before the quarter opens. It does not lock it — this operator reprices daily and has run coordinated events roughly every three to four weeks."*

That hedge was written on 15 September and tested on 18 September. The regime entering Q4 is now the 18 September level, not the 14th's, and on matched listings that is roughly two percent below 13 September rather than twenty-five.

Whether P3 survives is now a question about the rest of the quarter, not about the reset. That is the correct position for a prediction to be in three days after sealing, and it is recorded here rather than acted on.

### E.5 For anything published

The 18 September walkback belongs in any public write-up of the 14 September reset. A page describing a 25% cut as the standing regime would describe a state that lasted four days.

The honest version is shorter and better: the reset was walked back, the prediction was sealed before it happened and stands unchanged, and the matched panel says the two moves largely cancelled while more than half the book still finished lower.
