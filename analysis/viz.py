"""
viz.py — Shared SVG chart library for the FindStorage report pages.

Pure stdlib. Every function returns an HTML string embedding a responsive
<svg> (viewBox, width:100%). Visual spec (validated against the site's dark
surface #161c22 with the dataviz palette checker):

    series slots  #3987e5 blue · #199e70 green · #c98500 gold ·
                  #9085e9 violet · #e66767 red    (max 5; never cycled)
    diverging     blue #3987e5 (down/below) <-> red #e66767 (up/above)
    marks         2px lines, r>=4 markers with a 2px surface ring,
                  bars <=22px with a 4px rounded data-end (square baseline)
    chrome        hairline solid gridlines #232c35, muted ink #8fa0af,
                  text in text tokens, never in series color

Native <title> elements provide per-mark hover tooltips; the accompanying
tables carry every value, so nothing is gated behind hover or color.
"""
import html
import math

SURFACE = "#161c22"   # card / chart surface
GRID    = "#232c35"   # hairline grid
INK     = "#e8edf2"   # primary text
DIM     = "#8fa0af"   # secondary / axis text
SERIES  = ["#3987e5", "#199e70", "#c98500", "#9085e9", "#e66767"]
POS     = "#e66767"   # diverging: above baseline (price up)
NEG     = "#3987e5"   # diverging: below baseline (price down)

def esc(s):
    return html.escape(str(s))

def _nice_ticks(lo, hi, n=4):
    """Round tick values covering [lo, hi]."""
    if hi <= lo:
        hi = lo + 1
    raw = (hi - lo) / n
    mag = 10 ** math.floor(math.log10(raw))
    step = min(s for s in (1, 2, 2.5, 5, 10) if s * mag >= raw) * mag
    t = math.floor(lo / step) * step
    ticks = []
    while True:                     # last tick must reach/exceed hi, never clip it
        ticks.append(round(t, 6))
        if t >= hi - step * 0.01:
            break
        t += step
    return ticks

def legend(names):
    """Legend chip row for >=2 series (a single series needs none)."""
    if len(names) < 2:
        return ""
    chips = "".join(
        f"<span class='lg'><i style='background:{SERIES[i]}'></i>{esc(n)}</span>"
        for i, n in enumerate(names))
    return f"<div class='legend'>{chips}</div>"

def multiline(series, fmt="{:,.0f}", prefix="", W=760, H=230):
    """series: [(name, [(x_label, value), ...]), ...] sharing an x domain.

    2px lines, ringed markers, endpoint direct labels, one y axis.
    """
    series = [(n, [(x, v) for x, v in pts if v is not None]) for n, pts in series]
    series = [(n, pts) for n, pts in series if pts]
    if not series:
        return "<p class='empty'>Not enough history yet — accumulates with each daily run.</p>"
    xs = sorted({x for _, pts in series for x, _ in pts})
    xi = {x: i for i, x in enumerate(xs)}
    vals = [v for _, pts in series for _, v in pts]
    ticks = _nice_ticks(min(vals), max(vals))
    lo, hi = ticks[0], ticks[-1]
    PL, PR, PT, PB = 64, 116, 14, 30
    def X(x): return PL + (W - PL - PR) * (xi[x] / max(len(xs) - 1, 1))
    def Y(v): return PT + (H - PT - PB) * (1 - (v - lo) / (hi - lo))
    g = "".join(
        f"<line x1='{PL}' y1='{Y(t):.1f}' x2='{W-PR}' y2='{Y(t):.1f}' stroke='{GRID}' stroke-width='1'/>"
        f"<text x='{PL-8}' y='{Y(t)+4:.1f}' fill='{DIM}' font-size='11' text-anchor='end'>{prefix}{fmt.format(t)}</text>"
        for t in ticks)
    body = ""
    end_labels = []
    for i, (name, pts) in enumerate(series):
        c = SERIES[i % len(SERIES)]
        path = " ".join(f"{X(x):.1f},{Y(v):.1f}" for x, v in pts)
        body += (f"<polyline points='{path}' fill='none' stroke='{c}' "
                 f"stroke-width='2' stroke-linejoin='round' stroke-linecap='round'/>")
        for x, v in pts:
            body += (f"<circle cx='{X(x):.1f}' cy='{Y(v):.1f}' r='4' fill='{c}' "
                     f"stroke='{SURFACE}' stroke-width='2'>"
                     f"<title>{esc(x)} · {esc(name)}: {prefix}{fmt.format(v)}</title></circle>")
        ex, ev = pts[-1]
        ly = Y(ev) + 4
        while any(abs(ly - u) < 30 for u in end_labels):   # nudge colliding end-labels apart
            ly += 30
        end_labels.append(ly)
        body += (f"<text x='{X(ex)+9:.1f}' y='{ly:.1f}' fill='{INK}' font-size='12'>"
                 f"{prefix}{fmt.format(ev)}</text>"
                 f"<text x='{X(ex)+9:.1f}' y='{ly+13:.1f}' fill='{DIM}' font-size='10.5'>{esc(name)}</text>")
    xt = f"<text x='{PL}' y='{H-8}' fill='{DIM}' font-size='11'>{esc(xs[0])}</text>"
    if len(xs) > 1:
        xt += f"<text x='{W-PR}' y='{H-8}' fill='{DIM}' font-size='11' text-anchor='end'>{esc(xs[-1])}</text>"
    return (legend([n for n, _ in series]) +
            f"<svg viewBox='0 0 {W} {H}' role='img' style='width:100%;height:auto'>{g}{body}{xt}</svg>")

def histogram(values, bin_w, fmt="${:,.0f}", markers=None, W=760, H=210, clip_pct=99):
    """Column histogram, sequential single hue, rounded caps, percentile lines."""
    if not values:
        return ""
    values = sorted(values)
    cap = values[min(len(values) - 1, int(len(values) * clip_pct / 100))]
    cap = math.ceil(cap / bin_w) * bin_w
    nb = int(cap / bin_w)
    counts = [0] * (nb + 1)                      # last bin: overflow
    for v in values:
        counts[min(int(v // bin_w), nb)] += 1
    mx = max(counts)
    PL, PR, PT, PB = 14, 14, 30, 30
    bw = (W - PL - PR) / (nb + 1)
    def X(i): return PL + i * bw
    def HGT(c): return (H - PT - PB) * c / mx
    bars = ""
    for i, c in enumerate(counts):
        if not c:
            continue
        h = max(HGT(c), 2)
        x, y = X(i) + 1, H - PB - h
        lab = (f"${i*bin_w:,.0f}–{(i+1)*bin_w:,.0f}" if i < nb else f"${cap:,.0f}+")
        r = min(4, bw / 2 - 1, h)
        bars += (f"<path d='M{x:.1f},{H-PB} v{-(h-r):.1f} q0,{-r} {r},{-r} h{bw-2-2*r:.1f} "
                 f"q{r},0 {r},{r} v{h-r:.1f} z' fill='#3987e5'>"
                 f"<title>{lab}: {c:,} listings</title></path>")
    marks = ""
    for label, mv in (markers or {}).items():
        if mv is None or mv > cap + bin_w:
            continue
        x = PL + (W - PL - PR) * min(mv / (cap + bin_w), 1.0)
        marks += (f"<line x1='{x:.1f}' y1='{PT}' x2='{x:.1f}' y2='{H-PB}' stroke='{DIM}' stroke-width='1'/>"
                  f"<text x='{x:.1f}' y='{PT-6}' fill='{DIM}' font-size='11' text-anchor='middle'>"
                  f"{esc(label)} {fmt.format(mv)}</text>")
    xt = (f"<text x='{PL}' y='{H-8}' fill='{DIM}' font-size='11'>{fmt.format(0)}</text>"
          f"<text x='{W-PR}' y='{H-8}' fill='{DIM}' font-size='11' text-anchor='end'>{fmt.format(cap)}+</text>")
    return f"<svg viewBox='0 0 {W} {H}' role='img' style='width:100%;height:auto'>{bars}{marks}{xt}</svg>"

def diverging(rows, fmt="{:+,.0f}", suffix="", W=760, label_w=170, row_h=26):
    """rows: [(label, value)] — horizontal bars around a centered zero baseline.
    Positive = red (up), negative = blue (down). Rounded at the data end only.
    """
    rows = [(l, v) for l, v in rows if v is not None]
    if not rows:
        return ""
    mx = max(abs(v) for _, v in rows) or 1
    H = len(rows) * row_h + 16
    cx = label_w + (W - label_w - 70) / 2
    half = (W - label_w - 70) / 2
    out = (f"<svg viewBox='0 0 {W} {H}' role='img' style='width:100%;height:auto'>"
           f"<line x1='{cx}' y1='4' x2='{cx}' y2='{H-8}' stroke='{GRID}' stroke-width='1'/>")
    for i, (label, v) in enumerate(rows):
        y = i * row_h + 8
        bh = row_h - 10
        w = half * abs(v) / mx
        c = POS if v > 0 else NEG
        r = min(4, bh / 2, w)
        if v >= 0:
            path = f"M{cx},{y} h{w-r:.1f} q{r},0 {r},{r/1:.1f} v{bh-2*r:.1f} q0,{r} {-r},{r} h{-(w-r):.1f} z"
            tx, anch = cx + w + 6, "start"
            if tx > W - 8:                       # no room outside — tuck inside the bar end
                tx, anch = cx + w - 6, "end"
        else:
            path = f"M{cx},{y} h{-(w-r):.1f} q{-r},0 {-r},{r} v{bh-2*r:.1f} q0,{r} {r},{r} h{w-r:.1f} z"
            tx, anch = cx - w - 6, "end"
            if tx < label_w + 46:                # would collide with the label column
                tx, anch = cx - w + 6, "start"
        out += (f"<text x='{label_w-8}' y='{y+bh-3}' fill='{DIM}' font-size='12' text-anchor='end'>{esc(label)}</text>"
                f"<path d='{path}' fill='{c}'><title>{esc(label)}: {fmt.format(v)}{suffix}</title></path>"
                f"<text x='{tx:.1f}' y='{y+bh-3}' fill='{INK}' font-size='12' text-anchor='{anch}'>"
                f"{fmt.format(v)}{suffix}</text>")
    return out + "</svg>"

def scatter(points, fit=None, fit_pts=None, x_fmt="{:,.0f}", y_fmt="${:,.0f}", annot="", W=760, H=290):
    """points: [(x, y, tooltip)]; fit: (slope, intercept) straight line, or
    fit_pts: [(x, y), ...] drawn as a polyline (for fits computed in log space)."""
    if not points:
        return ""
    xs, ys = [p[0] for p in points], [p[1] for p in points]
    xt, yt = _nice_ticks(min(xs), max(xs)), _nice_ticks(min(ys), max(ys))
    xlo, xhi, ylo, yhi = xt[0], xt[-1], yt[0], yt[-1]
    PL, PR, PT, PB = 70, 18, 16, 40
    def X(v): return PL + (W - PL - PR) * (v - xlo) / (xhi - xlo)
    def Y(v): return PT + (H - PT - PB) * (1 - (v - ylo) / (yhi - ylo))
    g = "".join(
        f"<line x1='{PL}' y1='{Y(t):.1f}' x2='{W-PR}' y2='{Y(t):.1f}' stroke='{GRID}' stroke-width='1'/>"
        f"<text x='{PL-8}' y='{Y(t)+4:.1f}' fill='{DIM}' font-size='11' text-anchor='end'>{y_fmt.format(t)}</text>"
        for t in yt)
    g += "".join(
        f"<text x='{X(t):.1f}' y='{H-14}' fill='{DIM}' font-size='11' text-anchor='middle'>{x_fmt.format(t)}</text>"
        for t in xt[::max(1, len(xt)//5)])
    dots = "".join(
        f"<circle cx='{X(x):.1f}' cy='{Y(y):.1f}' r='4.5' fill='#3987e5' fill-opacity='0.75' "
        f"stroke='{SURFACE}' stroke-width='2'><title>{esc(t)}</title></circle>"
        for x, y, t in points)
    fitline = ""
    if fit:
        b, a = fit
        y1, y2 = a + b * xlo, a + b * xhi
        fitline = (f"<line x1='{X(xlo):.1f}' y1='{Y(y1):.1f}' x2='{X(xhi):.1f}' y2='{Y(y2):.1f}' "
                   f"stroke='#e66767' stroke-width='2' stroke-linecap='round'/>")
    elif fit_pts:
        pts = " ".join(f"{X(x):.1f},{Y(y):.1f}" for x, y in fit_pts
                       if xlo <= x <= xhi and ylo <= y <= yhi)
        fitline = (f"<polyline points='{pts}' fill='none' stroke='#e66767' "
                   f"stroke-width='2' stroke-linecap='round' stroke-linejoin='round'/>")
    an = (f"<text x='{W-PR}' y='{PT+6}' fill='{INK}' font-size='12.5' text-anchor='end'>{esc(annot)}</text>"
          if annot else "")
    return f"<svg viewBox='0 0 {W} {H}' role='img' style='width:100%;height:auto'>{g}{dots}{fitline}{an}</svg>"

CSS = """
.legend{display:flex;flex-wrap:wrap;gap:14px;margin:6px 0 4px;font-size:.8rem;color:var(--dim)}
.legend .lg{display:inline-flex;align-items:center;gap:6px}
.legend .lg i{width:10px;height:10px;border-radius:2px;display:inline-block}
.fig{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:14px 16px;margin:14px 0}
.kpi .d{font-size:.8rem;margin-top:2px}
.kpi .d.up{color:#e66767}.kpi .d.down{color:#3987e5}
.unlock{border:1px dashed var(--line);border-radius:8px;padding:14px 16px;color:var(--dim);font-size:.9rem}
"""

# ---------------------------------------------------------------- stats
def ols(pairs):
    """[(x, y)] -> (slope, intercept, r2, n) — plain least squares."""
    n = len(pairs)
    if n < 3:
        return None
    sx = sum(p[0] for p in pairs); sy = sum(p[1] for p in pairs)
    mx, my = sx / n, sy / n
    sxx = sum((p[0] - mx) ** 2 for p in pairs)
    syy = sum((p[1] - my) ** 2 for p in pairs)
    sxy = sum((p[0] - mx) * (p[1] - my) for p in pairs)
    if sxx == 0 or syy == 0:
        return None
    b = sxy / sxx
    a = my - b * mx
    r2 = sxy * sxy / (sxx * syy)
    return b, a, r2, n

def spearman(pairs):
    """[(x, y)] -> (rho, n) via Pearson on average ranks."""
    def ranks(vals):
        order = sorted(range(len(vals)), key=lambda i: vals[i])
        rk = [0.0] * len(vals)
        i = 0
        while i < len(order):
            j = i
            while j + 1 < len(order) and vals[order[j + 1]] == vals[order[i]]:
                j += 1
            r = (i + j) / 2 + 1
            for k in range(i, j + 1):
                rk[order[k]] = r
            i = j + 1
        return rk
    n = len(pairs)
    if n < 10:
        return None
    rx, ry = ranks([p[0] for p in pairs]), ranks([p[1] for p in pairs])
    fit = ols(list(zip(rx, ry)))
    if not fit:
        return None
    rho = math.copysign(math.sqrt(fit[2]), fit[0])
    return rho, n
