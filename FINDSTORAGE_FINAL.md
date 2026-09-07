# FindStorage — numbers of record

**Frozen. Re-derived from source 2026-09-06.** This file exists so nobody has to
compute these again.

FindStorage is a **closed** project. Collection ended **25 August 2026**. Every
figure below is filtered to `date <= 2026-08-25` and cannot move.

---

## 0. The boundary

Braeden, 2026-09-06: *"Any numbers for FindStorage should be considered as
something different after August twenty-fifth — that's when it became our
private data."*

So this is **two datasets, not one dataset with a pause.** Everything after 25
August belongs to a separate private collection with no public name and no
public numbers. **A figure that spans the boundary describes neither dataset**
and must not be quoted as a FindStorage number.

The trap is concrete: counting the whole of `rate_changes.csv` today gives
430,482 price changes. The FindStorage number is **396,402**. The difference is
the private set.

---

## 1. Numbers of record

Every row re-derived on 2026-09-06 from the files named in §5. Nothing here is
carried forward from an earlier document.

| figure | value |
|---|---|
| Collection window | **2026-04-29 → 2026-08-25** |
| Snapshot days | **48** |
| **Longest consecutive run** | **46 days, 2026-07-11 → 2026-08-25, zero gaps** |
| Stores, final day | **4,664** |
| Distinct stores ever seen | 4,666 |
| Coverage | **43 jurisdictions — 41 states + DC + Puerto Rico** |
| Advertised listings, final day | **55,328** |
| Units available, final day | **282,861** |
| Price changes logged | **396,402** |
| Promotion changes logged | **148,870** |
| Total events | **545,272** |
| Rate-log span | 2026-07-11 → 2026-08-25 — 46 calendar days, **43 carrying events** |
| Repricing waves | **13** |
| Largest single-day reprice | **88.7%** of advertised inventory, 2026-08-20 |

**The merger, absorbed with no manual intervention.** Between 2026-07-22 and
2026-07-23 the store count went **3,541 → 4,637** (+1,096) and listings
**40,391 → 52,768**. The pipeline's safety rails permit an abort on a >10%
run-over-run swing; this one was +31% and was allowed through as a real event
rather than a collection failure.

---

## 2. The wave series — all 13

A wave is a day on which Public Storage repriced a large share of advertised
inventory at once.

| date | day | price events | listings | share |
|---|---|---|---|---|
| 2026-07-15 | Wed | 33,661 | 41,896 | 80.3% |
| 2026-07-18 | Sat | 22,221 | 41,196 | 53.9% |
| 2026-07-23 | Thu | 33,504 | 52,768 | 63.5% |
| 2026-07-24 | Fri | 13,979 | 52,294 | 26.7% |
| 2026-07-25 | Sat | 13,961 | 51,817 | 26.9% |
| 2026-07-29 | Wed | 42,640 | 52,940 | 80.5% |
| 2026-08-01 | Sat | 12,792 | 57,244 | 22.3% |
| 2026-08-05 | Wed | 48,037 | 58,219 | 82.5% |
| 2026-08-12 | Wed | 46,122 | 56,599 | 81.5% |
| 2026-08-16 | Sun | 15,942 | 56,398 | 28.3% |
| 2026-08-20 | Thu | 50,052 | 56,458 | **88.7%** |
| 2026-08-22 | Sat | 33,897 | 56,279 | 60.2% |
| 2026-08-25 | Tue | 23,563 | 55,328 | 42.6% |

**The separation is total.** Quiet days span **0.00%–5.69%** of inventory.
Wave days span **22.3%–88.7%**. Nothing has ever landed between 5.69% and
22.3%. That reproduces `ERROR_LOG.md` [E43] exactly.

**Do not publish a day-of-week pattern.** Through 2026-08-23 all twelve waves
fell Wednesday–Sunday, p ≈ 0.011 under a uniform null — found by inspection
after the fact. **Wave 13 fell on Tuesday 25 August and broke it.** The
`ERROR_LOG.md` open item asking for a September test is answered: refuted, and
refuted by the last day of the project itself.

---

## 3. Corrections this pass produced

**"48 consecutive daily snapshots with zero gaps" is wrong**, and it is on the
resume and the portfolio right now. 48 is the count of snapshot days across a
119-day calendar span. The record is: one probe day 2026-04-29, a 71-day gap,
one day 2026-07-09, a two-day gap, then **46 consecutive days from 2026-07-11
to 2026-08-25 with no gaps.** Both numbers are good — *48 snapshot days* and
*a 46-day unbroken daily run* — but they are different claims and must not be
welded into one.

**"43 states" is loose.** The 43 distinct values include **DC and Puerto Rico**.
Accurate: *43 jurisdictions — 41 states, DC and Puerto Rico.*

**"Advertised price points: 55,332" does not reproduce.** The listings count on
2026-08-25 is **55,328**, four apart. The earlier figure may use a different
denominator. Unresolved; quote 55,328 as *advertised listings on the final day*
and drop the other unless someone re-derives it.

**A claim I made on 2026-09-06 and now retract.** I reported that 75,437 of
647,313 rows in `rate_changes.csv` carried a trailing CR in the brand field and
that merging brands would scatter 12% of history into a phantom sixth brand.
**That is wrong.** The file does have mixed line endings — 75,437 `\r\n` against
571,877 bare `\n` — but a conforming CSV reader handles it, and zero rows parse
with a stray CR. The corruption was in my reader (`awk`, splitting on `\n`), not
in the data. Naive line-splitting on this file will still mis-parse; the file
itself is sound.

---

## 4. Not re-derived — do not quote without re-checking

These appear in the 25 August exit report and were **not** reproduced today,
because each needs its own analysis script rather than a query over the history
files:

- **ZIP3 markets computed: 414**
- **Climate-control premium: ~18–22%** (matched pairs, same store, same size)
- **Store-size listings 34,416, of which 9,164 quarantined** — the direct count
  of `store-sizes-2026-08.csv` at 2026-08-25 is **29,038 rows across 4,648
  stores**, which does not reconcile with either figure. Something differs in
  the definition. Leave all three alone until someone runs the original script.

---

## 5. How every number above was produced

Source files, all under `C:\dev\storageDir\history\`:

| file | supplies |
|---|---|
| `2026-04.csv`, `2026-07.csv`, `2026-08.csv` | `date,store_id,units_avail,cheapest_10x10,median_price,listings` — snapshot days, store counts, listings, units |
| `rate_changes.csv` | `date,store_id,site_number,size,sku,field,old,new,brand` — price and promo events. **647,313 rows, brand is `publicstorage` for every one** |
| `sizes-2026-07.csv`, `sizes-2026-08.csv` | `date,state,size,listings,units_avail,median_price` — jurisdiction coverage |
| `store-sizes-2026-08.csv` | `date,store_id,size,price,available` — per-store size grid |

Method: read each with a conforming CSV parser (**not** `awk` — see §3), filter
`date <= "2026-08-25"`, and count. Waves are days with more than 5,000 price
events; the threshold sits inside an empty band running from 5,690 to roughly
12,800 events, so no day in the series is near it.

---

## 6. Standing rules

1. **Quote the 25 August set or quote nothing.** Do not mix in later data.
2. **Nothing sourced after 25 August goes on a public surface** — site, resume,
   proposals, attachments. The later waves are unpublishable for this reason.
3. **This file supersedes memory.** If an agent's recollection disagrees with
   the table in §1, the table wins.
4. Still wrong in places nobody has fixed: the **HN profile** ("4,600+ sites,
   245k price updates"), the **Upwork profile** ("430,482 price changes"), and
   `storageDir/README.md` ("~3,500 facilities", pre-merger).
