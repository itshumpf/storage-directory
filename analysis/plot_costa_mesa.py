#!/usr/bin/env python3
"""Plot the Costa Mesa advertised-savings reconstruction straight from the record.

    python analysis/plot_costa_mesa.py            # -> media_exports/costa_mesa_reference_rate.png

Every value on the chart is read out of `history/rate_changes.csv` at run time
and carried forward between changes. Nothing is typed in, with two exceptions
that are labelled on the figure itself:

  * the string "Total Estimated 12-Month Savings: $380" was transcribed from the
    live page on 24 August 2026 and recorded in analysis/promo_cost_model.py.
    The scraper schema never retained the savings field, the raw HTML, or a
    screenshot. It is drawn as a transcription and captioned as one.
  * the promotion cost model (which month each promotion discounts, and by how
    much) is the verified model in analysis/promo_cost_model.py.

See 2026-09-15-q3-prediction-and-ledger-gap.md Appendix D.2-D.4 for the evidence
hierarchy and for the language this figure is allowed to use. No intent is
attributed anywhere on it: the chart shows a sequence and a cost, not a motive.
"""
from __future__ import annotations

import csv
import os
from datetime import date, timedelta

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

matplotlib.rcParams["mathtext.default"] = "regular"
matplotlib.rcParams["axes.unicode_minus"] = False

def dollar(v, dp=0):
    """Dollar string with the sign escaped so matplotlib never reads it as math."""
    return "\\$" + format(v, f",.{dp}f")
from matplotlib.ticker import FuncFormatter

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
LOG = os.path.join(ROOT, "history", "rate_changes.csv")
OUT_DIR = os.path.join(ROOT, "media_exports")

SKU = "V_1455679"
STORE = "235"
PAGE_SIZE = "7.5x10"          # as the live page presented it; the feed normalizes to 5x15
WINDOW = (date(2026, 8, 20), date(2026, 8, 26))

# Rent due in month m at list price P. Identical to promo_cost_model.py.
PROMO_MODEL = {
    "$1 first month rent": lambda P, m: 1.0 if m == 1 else P,
    "40% off For 4 Month": lambda P, m: 0.6 * P if m <= 4 else P,
    "2nd Month Free":      lambda P, m: 0.0 if m == 2 else P,
    "":                    lambda P, m: P,
}

BG, CARD, FG, MUTED = "#0b0f17", "#111827", "#e2e8f0", "#94a3b8"
RED, TEAL, AMBER, GRID = "#f87171", "#2dd4bf", "#fbbf24", "#1f2937"


def series():
    """Carry price and promotion forward across the window from the change log."""
    rows = []
    with open(LOG, newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            if r["sku"] == SKU and r["store_id"] == STORE:
                rows.append(r)
    rows.sort(key=lambda r: r["date"])

    price, promo = None, None
    for r in rows:                                  # state immediately before the window
        if date.fromisoformat(r["date"]) > WINDOW[0]:
            break
        price = float(r["new"]) if r["field"] == "price" else price
        promo = r["new"] if r["field"] == "promo" else promo
    if price is None:                               # fall back to the first row's `old`
        first = rows[0]
        price = float(first["old"]) if first["field"] == "price" else price
    if promo is None:
        promo = next((r["old"] for r in rows if r["field"] == "promo"), "")

    out, d = [], WINDOW[0]
    while d <= WINDOW[1]:
        for r in rows:
            if date.fromisoformat(r["date"]) == d:
                if r["field"] == "price":
                    price = float(r["new"])
                else:
                    promo = r["new"]
        out.append((d, price, promo))
        d += timedelta(days=1)
    return out


def four_month_cost(P: float, promo: str) -> float:
    fn = PROMO_MODEL.get(promo)
    return None if fn is None else sum(fn(P, m) for m in range(1, 5))


def main() -> None:
    data = series()
    days = [d for d, _, _ in data]
    prices = [p for _, p, _ in data]
    costs = [four_month_cost(p, pr) for _, p, pr in data]

    base = min(prices)
    peak = max(prices)
    ad_day = next(d for d, _, pr in data if pr == "40% off For 4 Month")
    ad_price = dict((d, p) for d, p, _ in data)[ad_day]
    ad_i = days.index(ad_day)
    promo_rate = round(0.6 * ad_price, 2)
    advertised = round(4 * (ad_price - promo_rate), 2)       # the operator's own figure
    real = round(4 * (base - promo_rate), 2)                 # against the adjacent rate
    gap = round(4 * (ad_price - base), 2)
    share = gap / advertised

    money = FuncFormatter(lambda v, _: dollar(v))
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(12, 12.6), dpi=150, facecolor=BG,
        gridspec_kw={"height_ratios": [1.1, 1], "hspace": 0.34})

    fig.suptitle("Anatomy of an Advertised “Savings” Figure", x=0.062, y=0.972,
                 ha="left", color=FG, fontsize=23, fontweight="600")
    fig.text(0.062, 0.940,
             f"Public Storage store {STORE}, Costa Mesa CA · unit {PAGE_SIZE} · SKU {SKU}"
             "\nEvery value on this figure is read from history/rate_changes.csv at run time.",
             ha="left", va="top", color=MUTED, fontsize=11.5, linespacing=1.5)

    # --- panel 1: the advertised reference rate -----------------------------
    ax1.set_facecolor(CARD)
    ax1.plot(days, prices, color=RED, lw=2.6, marker="o", ms=7, zorder=3)
    ax1.axhline(base, color=TEAL, lw=1.1, ls=(0, (4, 4)), alpha=.75, zorder=1)
    ax1.annotate(f"{dollar(base)} — the rate on either side of the window",
                 xy=(days[0], base), xytext=(0, -24), textcoords="offset points",
                 color=TEAL, fontsize=11)
    ax1.annotate(f"{dollar(peak)}   +{peak / base - 1:.0%} reference rate",
                 xy=(days[3], peak), xytext=(0, 14), textcoords="offset points",
                 ha="center", color=RED, fontsize=12, fontweight="600")
    ax1.axvline(ad_day, color=AMBER, lw=1.3, ls=":", zorder=2)
    ax1.annotate("24 Aug — the 40%-off\noffer appears",
                 xy=(ad_day, base), xytext=(8, 6), textcoords="offset points",
                 color=AMBER, fontsize=11, fontweight="600")
    ax1.set_title("Advertised reference rate (the struck-through figure)",
                  color=FG, fontsize=13.5, loc="left", pad=12)
    ax1.set_ylim(base - 45, peak + 38)

    # --- panel 2: what four months actually cost ----------------------------
    ax2.set_facecolor(CARD)
    bars = ax2.bar(days, costs, width=.6,
                   color=[AMBER if d == ad_day else "#334155" for d in days], zorder=3)
    cheapest = min(c for c in costs if c)
    for day, c, b in zip(days, costs, bars):
        ax2.annotate(dollar(c, 2), xy=(day, c), xytext=(0, 6), textcoords="offset points",
                     ha="center", color=FG if day == ad_day else MUTED,
                     fontsize=10.5, fontweight="600" if day == ad_day else "normal")
    ax2.axhline(cheapest, color=TEAL, lw=1.1, ls=(0, (4, 4)), alpha=.75, zorder=1)
    ax2.set_title("Four months of rent under the offer on the page that day "
                  "(promotion model: analysis/promo_cost_model.py)",
                  color=FG, fontsize=13.5, loc="left", pad=12)
    ax2.set_ylim(0, max(c for c in costs if c) * 1.22)

    for ax in (ax1, ax2):
        ax.yaxis.set_major_formatter(money)
        ax.grid(axis="y", color=GRID, lw=.8)
        ax.set_axisbelow(True)
        ax.tick_params(colors=MUTED, labelsize=10.5)
        for side, sp in ax.spines.items():
            sp.set_visible(side == "bottom")
            sp.set_color(GRID)
        ax.set_xticks(days)
        ax.set_xticklabels([d.strftime("%-d %b") for d in days])

    adj = min(c for c in costs if c)
    excess = round(costs[ad_i] - adj, 2)
    fig.text(
        0.062, 0.292,
        f"On 24 August the page displayed \u201cTotal Estimated 12-Month Savings\u201d of {dollar(380)}. Four\n"
        f"months at 40% off the {dollar(ad_price)} reference rate is {dollar(advertised, 2)}; the page truncates it.\n"
        f"Measured against the {dollar(base)} rate on either side, {dollar(real, 2)} of that was a lower monthly\n"
        f"payment and {dollar(gap, 2)} \u2014 {share:.2%} \u2014 was the difference between the two reference rates.\n\n"
        f"The offer advertised as saving {dollar(380)} cost {dollar(excess, 2)} more over four months than the\n"
        f"adjacent offers on the same unit \u2014 {dollar(costs[ad_i], 2)} against {dollar(adj, 2)}.",
        ha="left", va="top", color=FG, fontsize=13, linespacing=1.65)

    fig.text(
        0.062, 0.022,
        "Prices and promotions: reproducible from history/rate_changes.csv in this repository.\n"
        "Four-month costs: modeled \u2014 analysis/promo_cost_model.py.\n"
        "The \u201cTotal Estimated 12-Month Savings\u201d wording and the \\$380 figure were transcribed from the\n"
        "live page on 24 August 2026; no first-party page capture was preserved. No intent is attributed.",
        ha="left", va="bottom", color=MUTED, fontsize=10, linespacing=1.55)

    fig.subplots_adjust(left=.085, right=.965, top=.868, bottom=.345)
    os.makedirs(OUT_DIR, exist_ok=True)
    out = os.path.join(OUT_DIR, "costa_mesa_reference_rate.png")
    fig.savefig(out, facecolor=BG)
    print(f"wrote {out}")
    print(f"  baseline ${base:,.0f}  peak ${peak:,.0f}  promo rate ${promo_rate:,.2f}")
    print(f"  advertised ${advertised:,.2f}  real ${real:,.2f}  gap ${gap:,.2f}  share {share:.2%}")
    for d, p, pr in data:
        print(f"  {d}  ${p:>7,.2f}  {pr or '(none)':<22} 4-month ${four_month_cost(p, pr):,.2f}")


if __name__ == "__main__":
    main()
