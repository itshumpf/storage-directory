# Run baseline — what a normal day looks like

**Internal. Names brands directly; do not publish or attach as-is.**

Built 2026-09-06 from `history/rate_changes.csv` (647,313 rows) and
`history/rate_log_runs.csv`. Covers **Public Storage only**, 2026-07-11 to
2026-09-05, 49 days carrying rows.

---

## 0. What this file is, and what it is not

This is **not a pre-registration.** A pre-registration is written before the
data is seen. This baseline was computed from data already in hand, after an
anomaly investigation. It borrows the *form* of the CSI pre-registrations —
frozen numbers, named risks, a verdict vocabulary — but not their epistemic
standing.

What that means in practice: these bands describe fifty-seven days of one
brand's behaviour. They are a **description**, and the first genuinely new
failure mode will fall outside them. The pre-registered part is everything
from here forward — once frozen, a band does not get widened to make a
failing day pass. It gets an entry in the run ledger explaining why it
failed, and the band changes only in a dated revision that says what changed
and why.

**Freeze date: _______ (unfrozen until Braeden signs off).**

---

## 1. The correction this file exists because of

On 2026-09-06 an agent (me) flagged 2026-08-31 and 2026-09-02 as pipeline
artifacts — 55,227 and 42,176 change events against a ~1,000/day baseline —
and proposed a guard that would quarantine any day above ~0.05 events per
matched SKU.

**That was wrong, and the proposed guard would have deleted the dataset's
primary signal.** Pulling price events across the full history rather than
the last week showed fifteen mass-price days across fifty-seven, recurring
every three to four days. Public Storage reprices on a schedule. The guard
would have discarded all fifteen.

Failure mode E in `.astory/ERROR_LOG.md`: *anomaly assumed to be a defect;
record not opened before blaming code.*

Consequence for the design below: **the day is classified first, then checked
against its own class.** A single threshold across a bimodal population
cannot be right for both halves.

---

## 2. Day classification — computed before any check

| class | rule |
|---|---|
| `REPRICE` | `price_events >= 5000` |
| `PROMO_SWEEP` | `promo_events >= 5000` |
| `QUIET` | neither |
| `UNCLASSIFIED` | `2444 <= price_events <= 12791` — see §3 |

A day may be both `REPRICE` and `PROMO_SWEEP`. Five have been: 07-24, 08-01,
08-05, 08-25, 08-31.

**Observed classes:** 15 `REPRICE`, 7 `PROMO_SWEEP`, 34 `QUIET`.

---

## 3. The separation these thresholds rest on

**Price events are cleanly bimodal.**

```
REPRICE days (n=15)     12,792 ... 50,052
all other days (n=34)         0 ...  2,443
                        -------------------
empty band              2,444 ... 12,791   (5.2x, nothing has ever landed here)
```

The 5,000 cut sits inside a wide empty gap, so it is not a tuned number —
anything from 3,000 to 12,000 gives the same 15/34 split.

**Promo events are not cleanly bimodal, and this cut is provisional.**

```
SWEEP days (n=7)        5,640  7,841  9,723  12,385  21,147  26,767  (+5,750)
all other days          701 ... 4,195   median 1,942   p90 2,851
                        gap of only 1.34x
```

A promo day landing between 4,195 and 5,640 is a coin flip. Treated as a
weaker signal accordingly.

---

## 4. Frozen bands

### QUIET days (n=34)

| quantity | observed | gate |
|---|---|---|
| price events | 0 – 2,443 | `> 2,443` → alarm |
| promo events | 701 – 4,195 (median 1,942) | `> 5,640` → reclassify, not alarm |
| stores touched | 428 – 2,903 | `> 3,000` → alarm |

### REPRICE days (n=15)

| quantity | observed | gate |
|---|---|---|
| price events | 12,792 – 50,052 | outside `10,000 – 60,000` → alarm |
| stores touched | 2,901 – 4,633 | `< 2,500` → partial run, quarantine |
| catalog share touched | 62% – 99% | `< 55%` → partial run |
| median % move | −7.1% – +35.9% | outside `−15% – +45%` → review |
| p05 % move | −43.8% – −10.8% | `< −55%` → review |
| p95 % move | +15.6% – +87.8% | `> +100%` → review |
| extreme-move share (\|Δ\|>50%) | see below | `> 2%` → **review, do not publish** |

**Extreme-move share is the sharpest discriminator inside the REPRICE class.**
Thirteen of fifteen days sit at or below 1.21%. Two do not:

```
07-15  0.49%    07-29  0.14%    08-16  0.56%    08-31  0.71%
07-18  0.06%    08-01  0.30%    08-20  0.50%    09-02  0.06%
07-23  0.23%    08-05  1.21%    08-22 18.44%  <---
07-24 12.10% <  08-12  0.67%    08-25  0.03%
07-25  0.72%
```

**07-24 and 08-22 are unexplained.** They are inside the REPRICE class on
every other measure. They are included in the bands above, which is what
widens p95 to +87.8%. **Decide before freezing whether to exclude them** — if
they turn out to be defects, the p95 and extreme-share bands both tighten
considerably.

### Run health (all classes)

Available only for 08-31 onward; `rate_log_runs.csv` starts there.

| quantity | observed | gate |
|---|---|---|
| match rate (`matched/old`) | 0.819 – 0.955 | `< 0.90` → catalog-churn flag |
| snapshot age | — | `> 36 h` → stale |
| day missing entirely | 8 occurrences | → gap entry, not silent |

Match rate has exactly one excursion: **08-31 at 0.819**, against 0.943–0.955
on the other five days, with the catalog jumping 55,328 → 60,132 SKUs. That
is a real catalog-growth event that happened to land on a repricing day —
which is why that day looked so extreme, and why one number could not explain
it.

---

## 5. Verdict vocabulary

Every run emits a verdict alongside its numbers. Raw events are never
deleted; a quarantined day stays in the file and is excluded from charts.

| verdict | meaning |
|---|---|
| `NORMAL` | classified, every gate for its class passed |
| `REVIEW` | passed structural gates, failed a distribution gate |
| `QUARANTINED` | failed a structural gate; not plotted as market activity |
| `UNCLASSIFIED` | fell in the empty band; never observed, treat as a bug in classification |
| `GAP` | no run; recorded explicitly rather than interpolated |

Mark at write time, filter at read time. The scraper records the judgement
because it knows the most; the dashboard honours it and does not re-derive.

---

## 6. Named risks — written before these gates are used

1. **These bands are Public Storage's.** U-Haul has one day of change
   history, Storage Sense four. **The bands do not transfer**, and applying
   them to another brand is a defect, not a shortcut. Each brand earns its
   own baseline or runs ungated.
2. **The empty band is empirical.** Nothing has landed in 2,444–12,791 across
   49 days. A genuinely medium repricing day would be quarantined wrongly.
   That is the intended failure direction — quarantine and look — but it will
   happen eventually and should not be read as a bug.
3. **The promo cut is weak** (§3). Expect misclassification there first.
4. **07-24 and 08-22 are unexplained and inside the bands.** They inflate
   p95 and the extreme-share ceiling. Every gate involving those two numbers
   is looser than it should be until they are diagnosed.
5. **Match-rate history is six days long.** The 0.90 floor is one excursion
   and five clean days. It is the least-supported number here.
6. **Seasonality is unmeasured.** Fifty-seven days spanning mid-July to early
   September. Storage demand is seasonal; a Q4 pattern is outside everything
   above.
7. **`rate_changes.csv` contains only Public Storage** — 647,313 rows, one
   brand. 75,437 of them carry a trailing CR in the brand field, so a
   group-by sees `publicstorage` and `publicstorage\r` as two brands. Merging
   the other four brands in before fixing that will scatter 12% of the
   history into a phantom sixth brand.

---

## 7. Open items this baseline does not cover

- **U-Haul's 4,342 change events never reach the dashboard.** The per-brand
  files are not merged into the master. Unrelated to anything above; still
  wrong.
- **Coverage gaps:** no rows on 07-22, 08-08, 08-15, and 08-26 → 08-30.
  The five-day run is the only one long enough to matter.
- **Only Public Storage has `rate_log_runs.csv`.** Storage Sense has a
  `last_run.json`, U-Haul has a partial one, the rest have nothing.
- **The repricing calendar itself is a finding, not a defect.** Fifteen days
  across fifty-seven, every three to four days, clustering Wednesday (5) and
  Saturday (4). Eight weeks of a national operator's repricing cadence.
