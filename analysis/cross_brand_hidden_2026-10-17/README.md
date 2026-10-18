# What the storage price lines hide — October 17, 2026

## Clarification after discussion

All promotion transitions in this report describe successive advertised offers for prospective new renters. They do not describe an existing renter switching promotions or having their signed promotional contract rewritten. The Public Storage wording should be read as: an offer advertised with $1 first-month rent on the earlier observation was advertised with the Halloween Sale label on the later observation, with a different captured rate.

For U-Haul's October 1–17 endpoint panel, 332 offers raised their advertised rate. Of these, 31 also gained the recorded Free Lock text and 10 lost it; 291 had unchanged recorded promotion text. This establishes no broad paired rental-promotion/rate-change event in the captured fields. It does not establish whether qualifying free-month eligibility or price-lock terms changed: those benefits are not historically captured by the unit parser. Current offer pages cannot answer a historical-change question.

A meaningful fall comparison should compare changes in new-customer advertised offers within each operator, and clearly distinguish online rent, comparison price, introductory terms and capture gaps. A universal cross-operator total-cost ranking or a claim that every operator offsets deeper discounts with higher prices would exceed this evidence. October findings should be labeled October 1–17 or given exact event dates, rather than presented as a complete fall-season study.

## Finding

There is substantial October activity beyond StorageMart, but the operators are not all doing the same thing. CubeSmart's larger advertised percentage discount persisted after a brief web-price cut largely reversed. Public Storage's Halloween label combined opposing offer-level rate movements. SmartStop and Storage Sense show actual advertised web-rate cuts. U-Haul's matched offer rates are largely stable. Independent coverage and Extra Space history cannot support an equivalent current national conclusion.

## CubeSmart: October 15 discount expansion, October 16 rebound

Available offer rows advertising 50% off rose from 423 on October 14 to 7,851 on October 15, and remained at 7,912 on October 17. The national median cheapest 10×10 moved $82 → $77 → $82 → $82 over October 14–17.

The strict four-day cohort contains 6,250 offers across 637 stores. Each has the same store ID, SKU, size and attributes across all four snapshots, is available and positively priced throughout, and changes from 40% off + First Month Free on October 14 to 50% off + First Month Free on October 15–17. Duplicate keys are excluded.

| Measure | Median individual change |
|---|---:|
| Online price, Oct 14 → 15 | −16.39% |
| Online price, Oct 15 → 16 | +19.81% |
| Online price, Oct 14 → 16 | 0% |
| In-store comparison price, Oct 14 → 16 | +19.91% |

6,169/6,250 (98.7%) raised the in-store comparison price between October 14 and 16. 3,798/6,250 (60.8%) returned to exactly the same online dollar rate at those endpoints. These percentages use the four-day surviving cohort, not all CubeSmart offers. Medians of individual ratios are not additive across days.

Example: Green Valley AZ, 630 W Camino Casa Verde, 5×5 Drive-up Access, SKU `cube_64440003-9779-4fd4-8fcc-b10e5d32d53f`:

| Date | Online price | In-store comparison | Discount | Additional offer |
|---|---:|---:|---|---|
| Oct 14 | $30 | $50 | 40% off | First Month Free |
| Oct 15 | $25 | $50 | 50% off | First Month Free |
| Oct 16 | $30 | $60 | 50% off | First Month Free |
| Oct 17 | $30 | $60 | 50% off | First Month Free |

The current public facility page independently shows $30 online and $60 in-store, and advertises up to 50% off and First Month Free. Historical sequence comes from dated snapshots. The parser rounds source prices to whole dollars, so these are rounded observations.

Source: https://www.cubesmart.com/arizona-self-storage/green-valley-self-storage/1.html

CubeSmart's `price` is already the online price after the percentage comparison discount. Do not discount it again by 50%. The free-month offer is separately recorded as `promo2`. Its in-store comparison price is not established as the tenant's automatic post-promotion rate. The offer terms allow rate adjustment with advance notice, and neither the percentage banner nor our snapshots guarantees a fixed future rate.

Terms: https://www.cubesmart.com/deals/terms/

## Public Storage: opposing movements inside the Halloween label

The October 11 → 12 matched available cohort includes:

| Previous promotion → Halloween Sale | Offers | Higher captured rate | Lower captured rate | Median individual rate change |
|---|---:|---:|---:|---:|
| $1 first month rent | 9,380 | 9,365 | 3 | +33.33% |
| 2nd Month Free | 3,187 | 3,179 | 1 | +33.33% |
| First month 50% off | 2,225 | 2,224 | 0 | +33.33% |
| 50% off First 4 Months | 6,400 | 1 | 6,399 | −16.28% |
| 40% off For 4 Month | 2,936 | 0 | 2,936 | −16.43% |
| 30% off For 4 Month | 1,023 | 0 | 1,023 | −16.37% |

The national median cheapest 10×10 rises $90 → $96 on October 12, but cannot show these different paths. By October 17, 26,621 available offers carry Halloween Sale. October 1–17 overall matched captured rates fall 7.58% in aggregate, so it would be wrong to characterize every October offer as a rate hike.

Important semantics: the collector maps `price` from the pricing API's `saleprice`. Source webpages can distinguish an introductory Web Only Offer from a later In-Store Rent; consequently this captured field is not established as a universal recurring/post-promotion base across every promotion. Halloween Sale itself does not encode a percentage, duration or complete payment schedule. This audit does not infer its all-in or 12-month cost, and does not apply a guessed additional discount.

Current official deals page confirms the Halloween campaign and seven-day move-in requirement, but does not supply one universal payment schedule:
https://www.publicstorage.com/storage-solutions/storage-deals

An accessible Burbank page, crawled earlier, separately shows month 1–4 online offers and months 5–12 in-store rent. Treat this as source-field context, not a live verification of October 12 historical terms:
https://www.publicstorage.com/self-storage-ca-burbank/1.html

## SmartStop: broad cuts in the captured web rate

October 7 → 8: 1,887/2,607 matched available offers cut their web rate; 141 raised it and 579 stayed unchanged. Median individual change is −13.64%, and the sum of captured rates falls 12.11%. National median cheapest 10×10 falls $74 → $64.

October 1 → 17: 1,733/2,271 comparable available offers are cheaper, versus 251 more expensive and 287 unchanged. Median individual change is −14.17%. This is materially different from the StorageMart event.

Promotion coverage remains a blind spot: `smartstop_parser.py` explicitly writes empty `promo` and `promo2` fields. The absence of recorded promotions is not evidence of no promotions. An official Apopka page advertises a First Month Rent Free special:
https://smartstopselfstorage.com/find-storage/fl/apopka/2200-coral-hills-rd

## Storage Sense: targeted cuts, less evidence of a broad paired event

October 1 → 17: 1,951 eligible available offers, excluding ambiguous names, include 386 rate cuts, 75 increases and 1,490 unchanged. Sum of rates falls 3.98%; median individual change is zero. National median cheapest 10×10 falls $89 → $80, while median of store median prices falls $99 → $96.50.

Only 75 eligible offers change promo text between those endpoints. Of 38 gaining 1st Month Free from no promotion, one raises its web rate, 11 cut it and 26 keep it unchanged. No comparable broad StorageMart-style event emerges in the tested October window. Endpoint survivor comparisons miss offers that enter, leave or change identity; they are not exhaustive counts of every campaign exposure. Large 70%-off banners are present, but their existence alone does not establish a new October campaign or accompanying rate increase.

## U-Haul: stable rate panel, conditional benefits outside our unit fields

October 1 → 17: 23,731 matched available offers include 332 increases, one decrease and 23,398 unchanged (98.6%). Sum of rates rises 0.092%. National median cheapest 10×10 nonetheless rises $149.95 → $154.95. The median-of-store-medians remains $134.95. Changing availability/composition and where prices sit within the distribution matter; the chart is not a fixed-offer index.

Our unit-level promotion parser records Free Lock, not the full conditional reservation offer. U-Haul currently advertises one month free at participating locations with qualifying equipment rental, and a one-year base-price lock at participating locations. These benefits are not represented by a national unit-price line and should not be treated as unconditional for every offer:
https://www.uhaul.com/Discounts/

## Where the data cannot answer

Independents: latest combined snapshot is October 13, not October 17. Available promo-bearing rows present October 1 disappear from the October 4 sample; they were associated with Right Move Storage. There are zero matched promotion changes, despite the changing aggregate promotion distribution. This is a coverage/offer-composition warning, not evidence that independent operators all ended promotions. Independents are also heterogeneous, not one pricing operator.

Extra Space: no complete comparable dated series in the production history. The existing offline audit describes a limited pilot and an access refusal. Excluded from national claims; no new bulk collection attempted.

## Meaning for the charts and a potential post

One line cannot represent all four of: current online price, comparison/in-store price, introductory benefits and later payment schedule. These fields differ by operator. A larger discount can coincide with a lower price, an unchanged price, or a higher recurring base.

The strongest new follow-up is the CubeSmart three-step sequence, supported by the four-day matched cohort and an independently checked current example. The defensible wider claim is that national price lines conceal different promotion and pricing changes, not that everyone is running the same scheme or coordinating with one another.

Useful additions: comparison-price trends for CubeSmart; percentage and free-month coverage as separate metrics; explicit promotion-data coverage labels; fixed-offer online-rate indices beside the changing-inventory median; cost horizons only where a source supplies reliable payment terms.

## Method and evidence

Reproduce from repository root:

`node --max-old-space-size=4096 analysis/cross_brand_hidden_2026-10-17.cjs`

`node analysis/cross_brand_hidden_2026-10-17/events.cjs`

The main scan reads September 28–October 17 snapshots and compares successive observed collection dates, which may span missing calendar days. October endpoint comparisons use first observed date on/after October 1 and last available date. `results.json` records hashes, source dates, duplicate exclusions, daily summaries, adjacent comparisons and endpoints. `events.json` includes all 6,250 CubeSmart cohort rows with their four daily rate pairs and source URLs.

Main comparisons match store ID + SKU, exclude every duplicate key on either date, require unchanged size/width/depth/height/attributes, exclude ambiguous names, and require positive finite prices and advertised availability at both endpoints. CubeSmart also has a unique-description endpoint sensitivity comparison because offer IDs can turn over; it is an offer-class comparison, not proof of the same physical unit. Results directionally agree but denominators differ.

Daily descriptive figures preserve production-row conventions, so they include rows later excluded from matched comparisons. No physical-unit or leasing-volume weighting is used. Aggregate rate changes mean the ratio of sums of one advertised rate per matched offer, not realized revenue, tenant costs, occupancy or financial performance. The snapshot date does not establish an exact simultaneous campaign launch. No intent, deceptive conduct, causal coordination or actual tenant rent increase is established. Production dashboards and collectors were not changed.
