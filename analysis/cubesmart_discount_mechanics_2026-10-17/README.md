# CubeSmart discount mechanics — October 17, 2026

## What the offer contains

CubeSmart's public unit cards distinguish an online monthly price from a crossed-out in-store price. The percentage-off banner describes the relationship between these two contemporaneously advertised prices. First Month Free is an additional move-in rent concession in the advertised package. The online price already incorporates the percentage discount; applying it again would double count it.

User-provided Topeka screenshot: 5×5 Climate Controlled / 1st Floor Access, 240 SE 29th St, $43 online versus $86 in-store, with First Month Free & 50% Off. 5×10 shows $88.50 online versus $177 in-store with the same advertised package. Both percentage calculations are exact in that screenshot.

The official Green Valley facility page independently labels the two fields online price and in-store price and shows $30 versus $60 for the 5×5. Source: https://www.cubesmart.com/arizona-self-storage/green-valley-self-storage/1.html

## Duration and future rent

The standard percentage-off text reviewed does not specify a fixed number of months. That differs from StorageMart's explicitly three-month discount schedule. Do not infer that CubeSmart's percentage discount ends after month one, that it lasts permanently, or that the crossed-out in-store figure is a contracted automatic future rent.

CubeSmart's public terms allow changes to the initial monthly rate with 30 days' advance notice. This is notice for a potential change, not a promise of a rate increase after 30 days, a guaranteed 30-day-only rate, or a disclosed future rate schedule. Its FAQ also describes advance notification for rent adjustments.

First Month Free applies at move-in to rent, while locks, insurance/protection plans, admin fees and taxes remain outside the rent discount. Thus free rent does not establish a zero-dollar move-in bill. The generic terms restrict additional offer combinations even though the unit itself advertises a percentage-plus-free-month package; that is not evidence that the two displayed benefits must be mutually exclusive. A checkout/lease is needed for exact offer-specific invoicing and any proration.

The terms separately describe commitment promotions, including a 35%-off minimum-four-month-stay offer requiring automatic payments. A bare 35% label in our normalized snapshots is not enough to assign those commitment terms to every such offer.

Sources accessed October 17:
- https://www.cubesmart.com/deals/terms/
- https://www.cubesmart.com/about-us/frequently-asked-questions/

## What happened October 14–16

Strict three-day panel: 6,480 comparable available offers at 637 stores, with the same store ID/SKU, unchanged size/attributes/dimensions, and positive finite online and comparison prices throughout. Every cohort offer advertises 40% Off + First Month Free on October 14, then 50% Off + First Month Free on October 15 and 16. Duplicate keys on any date are excluded.

| Measure | Result |
|---|---:|
| Median individual online-price change, Oct 14 → 15 | −16.39% |
| Median individual online-price change, Oct 15 → 16 | +19.80% |
| Median individual online-price change, Oct 14 → 16 | 0% |
| Median individual comparison-price change, Oct 14 → 16 | +19.91% |
| Comparison price higher on Oct 16 than Oct 14 | 6,392 / 6,480 (98.6%) |
| Online price unchanged at whole-dollar precision | 3,951 / 6,480 (61.0%) |
| Online price lower at whole-dollar precision | 1,135 / 6,480 (17.5%) |
| Online price higher at whole-dollar precision | 1,394 / 6,480 (21.5%) |

All 6,480 offers' daily online/comparison relationships agree with the labeled percentage within $1 on each of the three days. This tolerance accounts for normalization rounding; it is not independent checkout validation.

The earlier investigation's 6,250 denominator additionally required availability and the same promotion on October 17. This 6,480 panel ends October 16 to align with the ten-day post window and avoid requiring a subsequent day's survival. Neither is a random sample or a physical-unit/tenant count.

## Two examples of different timing

| Facility / date | Online price | In-store comparison | Promotion |
|---|---:|---:|---|
| Green Valley 5×5, Oct 14 | $30 | $50 | 40% + first month free |
| Green Valley 5×5, Oct 15 | $25 | $50 | 50% + first month free |
| Green Valley 5×5, Oct 16 | $30 | $60 | 50% + first month free |
| Topeka 5×5, Oct 14 | $43 | $71 | 40% + first month free |
| Topeka 5×5, Oct 15 | $43 | $86 | 50% + first month free |
| Topeka 5×5, Oct 16 | $43 | $86 | 50% + first month free |

Historical dollar amounts in this table are normalized, rounded snapshots. For Topeka, $71 × 60% is $42.60, which rounds to $43; therefore the table cannot establish exact cent-level equality across dates. The current screenshot establishes $43.00 / $86.00 but not historical cents. Avoid calling every whole-dollar unchanged offer exactly unchanged at cent precision.

The Green Valley sequence separates the discount expansion from the comparison-price increase across two observed collection dates. Topeka's comparison-price change is already visible with the deeper discount on October 15. Thus one universal rollout timing would be inaccurate.

Historical source identities and URLs are preserved in `results.json`. The public Topeka page returned older-looking 40%-off values during web extraction, whereas the user's screenshot shows 50% off. That mismatch could involve crawl timing or page state; it does not establish personalized pricing or simultaneous discriminatory offers. Current web search extracts cannot validate historical amounts.

## Interpretation

The observable mechanism is a changed comparison-price/discount pair: moving a 40%-off $50 comparison to a 50%-off $60 comparison leaves the online price at $30. With a constant online price W, 40% off implies comparison W/0.6 and 50% off implies W/0.5; that is a 20% higher comparison. This arithmetic describes the published prices, not which field the internal system chose first.

The defensible finding is that a larger advertised savings percentage often did not mean a lower online price than two days earlier. The snapshots cannot show whether customers paid the in-store comparison price, whether the company raised it specifically to enlarge the discount, what internal calculation generated it, or what existing tenants later paid. No fake-sale, intent or legal conclusion is established.

For the graphic, label CubeSmart's percentage as a comparison against the in-store price, show First Month Free separately, and avoid adding an invented three-month or lifetime term. Rate-lock duration remains unestablished without the offer-specific agreement.

## Reproduce

`node analysis/cubesmart_discount_mechanics_2026-10-17.cjs`

The output records SHA-256 hashes and all cohort rows. `cubesmart_parser.py` maps `price` from online `data-unitprice` (fallback discounted-price span), `street_price` from the original-price span/strikeout, and percentage/free-month text separately. `_clean_price` rounds to integers. These are advertised offer histories for prospective renters, not changing contracts for existing renters. No reservations, customer messages, or payments were submitted.
