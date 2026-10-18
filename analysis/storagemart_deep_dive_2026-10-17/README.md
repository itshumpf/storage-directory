# StorageMart fall pricing deep dive — October 17, 2026

StorageMart has expanded a real introductory discount while often raising the recurring web rate attached to it. The dashboard's fairly flat national line misses the introductory discount entirely and compresses substantial offer-level repricing into a median.

## What changed and when

Available unit-group offers in our U.S. snapshots:

| Date | Tracked stores | Offers at 75% off 3 months | Stores with at least one such offer | Median cheapest available 10×10 web rate | Median of store median web rates |
|---|---:|---:|---:|---:|---:|
| Sep 28 | 197 | 0 | 0 | $88 | $104 |
| Sep 29 | 197 | 403 | 53 | $88 | $109 |
| Oct 13 | 197 | 398 | 53 | $87 | $109 |
| Oct 14 | 197 | 1,059 | 190 | $89 | $110.50 |
| Oct 17 | 197 | 1,005 | 187 | $90 | $110.50 |

Two distinct events emerge. September 29 introduced the deeper offer at 53 stores. October 14 expanded it across almost the entire tracked footprint. These are first observed snapshot changes, not verified advertising campaign launch dates. The research did not capture the particular fall advertisement the user saw, so attribution to that specific creative remains unverified.

As of October 17, 39.4% of 2,549 available offer rows carry 75% off; 32.6% carry 50% off, 22.5% carry 25% off, and 5.5% carry no promotion. Thus wide store coverage does not mean every unit qualifies. Daily descriptive totals retain the same rows as the production dashboard; duplicate keys are excluded from longitudinal comparisons below.

## Why our line barely reacts

`storage_pipeline.py` builds each store's cheapest available 10×10 and median available offer price from `price`. The national chart takes a median across stores. `price` maps to the source's `discountedPrice` (fallback `price`); `promo_price` and the month-by-month `promo_terms` are separate. The production trend values were independently reproduced from the raw snapshots for the five dates in the table.

Consequently:

1. A move from 50% off to 75% off does not itself reduce the recurring web-rate series.
2. A national median can barely move even while many individual offers change.
3. The cheapest-10×10 series observes one minimum per store and ignores repricing of other sizes or more expensive 10×10 variants.
4. Availability and the offered size/amenity mix can change the daily median. It is not a fixed-offer price index.

Across October 13–17, 2,394 eligible offers available at both endpoints include 1,066 rate increases, 207 decreases, and 1,121 unchanged rates. The median individual percentage change is zero, while the sum of their recurring rates rises 3.93%. The latter weights expensive offers more; it is not revenue or an occupancy-weighted price index.

## The two promotion events behave differently

September 28–29: 394 eligible, continuously available offers moved from 50% to 75% off across 53 stores. Of these, 386 kept the same recurring price, seven cut it, and one raised it. Modeled first-three-month rent fell 50.08% in aggregate, and twelve-month rent fell 7.28%. This was predominantly a promotion-only change.

October 13–14: 652 eligible, continuously available offers switched from 50% to 75% off across 139 stores and 18 states. Of these, 622 (95.4%) raised the recurring rate, 11 cut it, and 19 left it unchanged. The median increase among increases was 10.03%. Recurring rates for the entire switching cohort rose 9.49% in aggregate.

Largest October 14 switching cohorts: Missouri 133 offers/32 stores; Iowa 86/21; Illinois 77/14; Kansas 57/14; Wisconsin 50/10; Nebraska 41/8. This was geographically broad rather than isolated to one market. State counts describe our sample, not every StorageMart property.

For the same October 14 switching cohort, modeled cumulative rent changes are:

| Stay | Change in aggregate rent | Offers costing more than before |
|---|---:|---:|
| 3 months | −45.26% | 0 of 652 |
| 6 months | −8.76% | 0 of 652 |
| 9 months | −1.46% | 19 of 652 |
| 12 months | +1.67% | 601 of 652 (92.2%) |

This is a sum of one hypothetical rental per eligible offer, not tenant spending. Holding other terms fixed, a 10% recurring increase combined with a move from 50% to 75% off three months crosses over at month 9.75. Actual offer-specific crossover varies.

## Concrete example

StorageMart 0106, Columbia, Missouri, Paris Road: 5×10 Drive-Up, Non-CC, SKU `smart_acb27f90-db0a-4f34-af8b-564669bd3749`.

| Snapshot | Recurring web rate | Promotion | Promotional monthly rent | 3-month rent | 12-month rent |
|---|---:|---|---:|---:|---:|
| Oct 13 | $49 | 50% off 3 months | $24.50 | $73.50 | $514.50 |
| Oct 14–17 | $54 | 75% off 3 months | $13.50 | $40.50 | $526.50 |

The recurring rate rises 10.2%, introductory rent falls 44.9%, three-month savings are $33, and twelve-month cost rises $12. Both effects can be true simultaneously. Do not apply an additional discount to `promo_price`.

Source facility: https://www.storage-mart.com/columbia-missouri/65202-paris-rd . Historical amounts above come from dated local snapshots; the text-only live webpage does not expose the full unit list and cannot independently confirm the historical prices.

## Broader business context

StorageMart and Manhattan Mini Storage's August 12 company release reported Q2 same-store revenue growth of 4.2%, expense growth of 2.2%, and NOI growth of 5.0%. Management described the Midwest and Northeast as stronger and the Sunbelt as lagging, and stated a continued focus on increasing rental rates. This supports an interpretation of introductory acquisition incentives alongside recurring-rate growth, but does not prove the October campaign's intent, financial outcome, or occupancy pressure. These are company-reported combined-brand results, not a financial disclosure for our 197-store sample.

Company release: https://www.globenewswire.com/news-release/2026/08/12/3343715/0/en/storagemart-and-manhattan-mini-storage-report-strong-second-quarter-2026-results.html

The April 14 company release reported 20 facility additions in Q1, including 15 New York facilities under Manhattan Mini Storage. Our StorageMart-only U.S. sample therefore should not be treated as the entire corporate portfolio.

Company release: https://www.fidelity.com/news/article/mergers-and-acquisitions/202604140936PRIMZONEFULLFEED9687566

## Interpretation and useful tracking changes

The strongest supported interpretation is a broader introductory offer paired with recurring-rate increases, rather than a broad recurring-rate price cut. Short-stay renters receive material rent savings; longer stays can erase them. A flat dashboard line is not evidence that nothing changed or that the deals are fictitious.

For future tracking, retain the recurring-rate chart and add promotion coverage, a fixed-offer recurring-rate index, and modeled 3/6/12-month effective monthly rent. Separate changing availability from fixed-offer prices. These are proposed metrics; no production dashboard or collector changes were made.

## Evidence, reproducibility and limitations

Run from repository root: `node analysis/storagemart_deep_dive_2026-10-17.cjs`.

`results.json` includes input SHA-256 hashes, every daily descriptive summary, daily 50%-to-75% transitions, cohort statistics, state counts, and example records. Dated comparison JSON files contain eligible offer identities, source URLs, old/new rates and promotions.

Match on store ID + SKU; exclude every duplicated key on either date; require unchanged width, depth, height and attrs; require positive finite rates and availability at both endpoints. The current complete collector report has 197 U.S. stores, 4,124 total unit-group rows, no skips and no warnings. The October 13–14 comparison excludes 23 shared keys with changed descriptions before availability filtering. The older October 15 audit used a broader cohort including unavailable offers and did not check height; its 659 switches should not be confused with this analysis's 652 continuously available switches.

Modeled costs use each snapshot's recurring rate and structured month-specific discount terms, with no future rate changes. They exclude taxes, administrative/lock charges, coverage, payment fees and move-in proration. Two matched available offers with promo text but no structured terms are omitted from broad cost calculations; all 652 October switching offers have usable terms. As a consistency check, all 2,380 currently available offers with structured terms and numeric `promo_price` agree with their calculated first-month percent discount to within one cent.

Advertised available counts do not establish occupancy, leasing velocity, realized tenant rents, or revenue. The snapshots establish advertised terms and changes, not intent or deceptive conduct. Marketing creative and actual checkout totals were not captured in this investigation.
