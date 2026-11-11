#!/usr/bin/env python3
"""
PART 4 - Visualisations.

    python src/viz/make_figures.py

Reads the JSON written by the MongoDB, Spark and integration jobs and produces
static PNGs for the report. Nothing is recomputed here: every number on every
chart came out of one of the analytical jobs, so the figures and the tables in
the report cannot disagree.
"""
import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

os.makedirs(config.FIGURES_DIR, exist_ok=True)

plt.rcParams.update({
    "figure.dpi": 150, "savefig.dpi": 150, "font.size": 9,
    "axes.grid": True, "grid.alpha": .25, "axes.axisbelow": True,
    "axes.spines.top": False, "axes.spines.right": False,
})
NAVY, BLUE, GREEN, AMBER, GREY = "#0B2D5C", "#1F5C97", "#1B7A3E", "#E08A1E", "#8A96A5"


def num(v, default=0.0):
    """
    Coerce to float.

    Spark SQL returns DECIMAL for rounded expressions, and json.dump writes a
    DECIMAL as a quoted string. Reading it back gives "20.29" rather than
    20.29, which breaks every arithmetic operation downstream. Coercing once
    here is simpler than casting inside each query.
    """
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def load(name):
    path = os.path.join(config.RESULTS_DIR, f"{name}.json")
    if not os.path.exists(path):
        print(f"   skipped {name} (not found - run the job that writes it)")
        return None
    with open(path) as fh:
        return json.load(fh)


def save(fig, name):
    path = os.path.join(config.FIGURES_DIR, f"{name}.png")
    fig.savefig(path, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"   -> figures/{name}.png")


# ------------------------------------------------------------------ figure 1
def fig_revenue_by_category():
    rows = load("sql_revenue_by_category") or load("mongo_revenue_by_category")
    if not rows:
        return
    rows = sorted(rows, key=lambda r: -num(r["revenue"]))[:12]
    names = [(r.get("category_name") or r.get("category_id") or "?")[:28] for r in rows]
    rev = [num(r["revenue"]) / 1e6 for r in rows]
    pos = range(len(names))

    fig, ax = plt.subplots(figsize=(8, 4.5))
    bars = ax.barh(list(pos), rev, color=BLUE, height=.72)
    bars[0].set_color(NAVY)
    ax.set_yticks(list(pos)); ax.set_yticklabels(names)
    ax.invert_yaxis(); ax.margins(x=.16)
    ax.set_xlabel("Revenue (millions)")
    ax.set_title("Figure 1   Revenue by product category", loc="left", fontweight="bold")
    for p, v in zip(pos, rev):
        ax.text(v, p, f"  {v:,.2f}M", va="center", fontsize=8)
    save(fig, "fig1_revenue_by_category")


# ------------------------------------------------------------------ figure 2
def fig_funnel():
    rows = load("sql_conversion_funnel")
    if not rows:
        return
    rows = sorted(rows, key=lambda r: r["stage"])
    labels = [r["stage"].split(". ", 1)[-1] for r in rows]
    counts = [num(r["n"]) for r in rows]
    pct = [num(r["pct_of_sessions"]) for r in rows]
    pos = range(len(labels))

    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.bar(list(pos), counts, color=[NAVY, BLUE, AMBER, GREEN], width=.6)
    ax.set_xticks(list(pos)); ax.set_xticklabels(labels)
    ax.set_ylabel("Sessions")
    ax.set_title("Figure 2   Conversion funnel", loc="left", fontweight="bold")
    ax.set_ylim(0, max(counts) * 1.18)
    for p, (c, q) in enumerate(zip(counts, pct)):
        ax.text(p, c, f"{c:,.0f}\n{q:.1f}%", ha="center", va="bottom", fontsize=8)
    # drop-off annotation between stages
    for i in range(len(counts) - 1):
        drop = (counts[i] - counts[i + 1]) / counts[i] * 100 if counts[i] else 0
        ax.annotate(f"-{drop:.0f}%", xy=(i + .5, (counts[i] + counts[i + 1]) / 2),
                    ha="center", fontsize=8, color="#b02418", fontweight="bold")
    save(fig, "fig2_conversion_funnel")


# ------------------------------------------------------------------ figure 3
def fig_cohort():
    rows = load("spark_cohort_analysis")
    if not rows:
        return
    cohorts = sorted({r["cohort_month"] for r in rows})
    months = sorted({r["months_since"] for r in rows})
    months = [m for m in months if m <= 11]
    grid = np.full((len(cohorts), len(months)), np.nan)
    ci = {c: i for i, c in enumerate(cohorts)}
    mi = {m: i for i, m in enumerate(months)}
    for r in rows:
        if r["months_since"] in mi:
            grid[ci[r["cohort_month"]], mi[r["months_since"]]] = num(r["retention_pct"])

    fig, ax = plt.subplots(figsize=(9, max(3, len(cohorts) * .42)))
    im = ax.imshow(grid, aspect="auto", cmap="Blues", vmin=0,
                   vmax=np.nanmax(grid) if np.isfinite(grid).any() else 1)
    ax.set_xticks(range(len(months))); ax.set_xticklabels(months)
    ax.set_yticks(range(len(cohorts))); ax.set_yticklabels(cohorts)
    ax.set_xlabel("Months since registration"); ax.set_ylabel("Registration cohort")
    ax.set_title("Figure 3   Cohort retention — % of each cohort still purchasing",
                 loc="left", fontweight="bold")
    ax.grid(False)
    for i in range(len(cohorts)):
        for j in range(len(months)):
            if np.isfinite(grid[i, j]):
                ax.text(j, i, f"{grid[i, j]:.0f}", ha="center", va="center",
                        fontsize=7,
                        color="white" if grid[i, j] > np.nanmax(grid) * .55 else NAVY)
    fig.colorbar(im, ax=ax, label="retention %", shrink=.8)
    save(fig, "fig3_cohort_retention")


# ------------------------------------------------------------------ figure 4
def fig_hourly():
    rows = load("sql_hourly_demand")
    if not rows:
        return
    rows = sorted(rows, key=lambda r: num(r["hour_of_day"]))
    hours = [int(num(r["hour_of_day"])) for r in rows]
    sess = [num(r["sessions"]) for r in rows]
    conv = [num(r["conversion_rate_pct"]) for r in rows]

    fig, ax = plt.subplots(figsize=(9, 3.8))
    bars = ax.bar(hours, sess, color=BLUE, width=.72, label="sessions")
    bars[sess.index(max(sess))].set_color(NAVY)
    ax.set_xticks(range(24)); ax.set_xlabel("Hour of day")
    ax.set_ylabel("Sessions", color=BLUE)
    ax.set_title("Figure 4   Demand and conversion rate by hour of day",
                 loc="left", fontweight="bold")

    ax2 = ax.twinx(); ax2.grid(False)
    ax2.plot(hours, conv, color=AMBER, marker="o", ms=3.5, lw=2,
             label="conversion rate")
    ax2.set_ylabel("Conversion rate (%)", color=AMBER)
    lo, hi = min(conv), max(conv)
    ax2.set_ylim(lo - (hi - lo) * .4, hi + (hi - lo) * .4)
    save(fig, "fig4_hourly_demand")


# ------------------------------------------------------------------ figure 5
def fig_device():
    rows = load("sql_device_performance")
    if not rows:
        return
    rows = sorted(rows, key=lambda r: -num(r["sessions"]))
    labels = [r["device_type"] for r in rows]
    sess = [num(r["sessions"]) for r in rows]
    conv = [num(r["conversion_rate_pct"]) for r in rows]
    pos = np.arange(len(labels))

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(9, 3.6))
    a1.bar(pos, sess, color=BLUE, width=.6)
    a1.set_xticks(pos); a1.set_xticklabels(labels)
    a1.set_ylabel("Sessions"); a1.set_title("Traffic by device")
    for p, v in zip(pos, sess):
        a1.text(p, v, f"{v:,.0f}", ha="center", va="bottom", fontsize=8)

    b = a2.bar(pos, conv, color=GREEN, width=.6)
    b[conv.index(max(conv))].set_color(AMBER)
    a2.set_xticks(pos); a2.set_xticklabels(labels)
    a2.set_ylabel("Conversion rate (%)"); a2.set_title("Conversion by device")
    a2.set_ylim(0, max(conv) * 1.25)
    for p, v in zip(pos, conv):
        a2.text(p, v, f"{v:.2f}%", ha="center", va="bottom", fontsize=8)
    fig.suptitle("Figure 5   Device drives traffic volume and conversion differently",
                 fontweight="bold", x=.02, ha="left")
    fig.tight_layout()
    save(fig, "fig5_device_performance")


# ------------------------------------------------------------------ figure 6
def fig_clv():
    rows = load("clv_top200")
    if not rows:
        return
    clv = [num(r["predicted_clv"]) for r in rows]
    spend = [num(r["lifetime_spend"]) for r in rows]
    sessions = [num(r["sessions"]) for r in rows]

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(9.5, 3.8))
    a1.hist(clv, bins=30, color=BLUE, edgecolor="white")
    a1.set_xlabel("Predicted CLV"); a1.set_ylabel("Customers")
    a1.set_title("Distribution of predicted CLV (top 200)")

    sc = a2.scatter(spend, clv, c=sessions, cmap="viridis", s=18, alpha=.85)
    a2.set_xlabel("Actual lifetime spend to date")
    a2.set_ylabel("Predicted CLV")
    a2.set_title("Predicted value vs spend, coloured by engagement")
    fig.colorbar(sc, ax=a2, label="sessions (HBase)", shrink=.85)
    fig.suptitle("Figure 6   Customer lifetime value — MongoDB spend x HBase engagement",
                 fontweight="bold", x=.02, ha="left")
    fig.tight_layout()
    save(fig, "fig6_clv")


# ------------------------------------------------------------------ figure 7
def fig_top_products():
    rows = load("mongo_top_products")
    if not rows:
        return
    rows = rows[:12]
    names = [f"{r['name'][:26]}" for r in rows]
    rev = [num(r["revenue"]) / 1000 for r in rows]
    pos = range(len(names))

    fig, ax = plt.subplots(figsize=(8, 4.2))
    bars = ax.barh(list(pos), rev, color=GREEN, height=.72)
    bars[0].set_color(NAVY)
    ax.set_yticks(list(pos)); ax.set_yticklabels(names)
    ax.invert_yaxis(); ax.margins(x=.18)
    ax.set_xlabel("Revenue (thousands)")
    ax.set_title("Figure 7   Top products by revenue (MongoDB aggregation)",
                 loc="left", fontweight="bold")
    for p, v in zip(pos, rev):
        ax.text(v, p, f"  {v:,.0f}K", va="center", fontsize=8)
    save(fig, "fig7_top_products")


def main():
    print("Building figures from results/\n")
    for fn in (fig_revenue_by_category, fig_funnel, fig_cohort, fig_hourly,
               fig_device, fig_clv, fig_top_products):
        try:
            fn()
        except Exception as e:
            print(f"   {fn.__name__} failed: {type(e).__name__}: {e}")
    print(f"\nFigures in {config.FIGURES_DIR}/")


if __name__ == "__main__":
    main()
