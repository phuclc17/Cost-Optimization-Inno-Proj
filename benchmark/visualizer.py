"""
Visualizer — Generate charts for report
Dung: python3 benchmark/visualizer.py
"""
import sys, os
sys.path.insert(0, ".")

try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np
except ImportError:
    os.system("pip3 install matplotlib numpy --break-system-packages")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np

from pipeline.storage.db_client import DatabaseClient

COLORS = {
    "static_conservative": "#6C757D",
    "static_aggressive":   "#FF6B35",
    "reactive":            "#4361EE",
    "cost_aware":          "#2DC653",
}
LABELS = {
    "static_conservative": "S1 Static Low",
    "static_aggressive":   "S2 Static High",
    "reactive":            "S3 Reactive",
    "cost_aware":          "S4 Cost-Aware",
}
PATTERNS_ORDER = ["normal", "peak", "mixed"]
STRATEGY_ORDER = ["static_conservative",
                  "static_aggressive",
                  "reactive",
                  "cost_aware"]

os.makedirs("docs/figures", exist_ok=True)

def load_data():
    db = DatabaseClient()
    results = db.get_all_benchmark_results()
    data = {}
    for r in results:
        s = r["strategy"]
        p = r["traffic_pattern"]
        key = (s, p)
        if key not in data:
            data[key] = []
        data[key].append({
            "rps":   float(r["avg_throughput_rps"]),
            "p99":   float(r["p99_latency_ms"]),
            "sla":   float(r["sla_compliance_pct"]),
            "cost":  float(r["total_cost_usd"]),
            "fi":    float(r["finops_score"]),
            "waste": float(r.get("avg_workers", 1)),
        })
    return data

def mean(vals, key):
    v = [x[key] for x in vals]
    return sum(v)/len(v) if v else 0

def std(vals, key):
    import math
    v = [x[key] for x in vals]
    if len(v) < 2: return 0
    m = sum(v)/len(v)
    return math.sqrt(sum((x-m)**2 for x in v)/(len(v)-1))

def fig_throughput(data):
    """Figure 5.1 — Throughput comparison"""
    fig, ax = plt.subplots(figsize=(10, 6))
    x = np.arange(len(PATTERNS_ORDER))
    width = 0.2
    for i, strat in enumerate(STRATEGY_ORDER):
        means = [mean(data.get((strat,p),[{"rps":0}]),"rps")
                 for p in PATTERNS_ORDER]
        errs  = [std(data.get((strat,p),[{"rps":0}]),"rps")
                 for p in PATTERNS_ORDER]
        ax.bar(x + i*width, means, width,
               label=LABELS[strat],
               color=COLORS[strat],
               yerr=errs, capsize=3, alpha=0.85)
    ax.set_xlabel("Traffic Pattern")
    ax.set_ylabel("Throughput (rps)")
    ax.set_title("Figure 5.1: Throughput Comparison Across Strategies")
    ax.set_xticks(x + width*1.5)
    ax.set_xticklabels(["T1 Normal","T2 Flash Sale","T3 Mixed"])
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig("docs/figures/fig5_1_throughput.png", dpi=150)
    plt.close()
    print("Saved: fig5_1_throughput.png")

def fig_cost_comparison(data):
    """Figure 5.6 — Cost comparison"""
    fig, ax = plt.subplots(figsize=(10, 6))
    x = np.arange(len(PATTERNS_ORDER))
    width = 0.2
    for i, strat in enumerate(STRATEGY_ORDER):
        means = [mean(data.get((strat,p),[{"cost":0}]),"cost")
                 for p in PATTERNS_ORDER]
        ax.bar(x + i*width, means, width,
               label=LABELS[strat],
               color=COLORS[strat], alpha=0.85)
    ax.set_xlabel("Traffic Pattern")
    ax.set_ylabel("Cost per 30-min Experiment ($)")
    ax.set_title("Figure 5.6: Cost Comparison Across Strategies")
    ax.set_xticks(x + width*1.5)
    ax.set_xticklabels(["T1 Normal","T2 Flash Sale","T3 Mixed"])
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig("docs/figures/fig5_6_cost.png", dpi=150)
    plt.close()
    print("Saved: fig5_6_cost.png")

def fig_finops_index(data):
    """Figure 5.9 — FinOps Index"""
    fig, ax = plt.subplots(figsize=(10, 6))
    x = np.arange(len(STRATEGY_ORDER))
    for j, pat in enumerate(PATTERNS_ORDER):
        means = [mean(data.get((s,pat),[{"fi":0}]),"fi")
                 for s in STRATEGY_ORDER]
        offset = (j - 1) * 0.25
        bars = ax.bar(x + offset, means, 0.25,
                      label=f"T{j+1} {'Normal' if j==0 else 'Flash' if j==1 else 'Mixed'}",
                      alpha=0.85)
    ax.axhline(y=0.9, color="green", linestyle="--",
               alpha=0.5, label="FI=0.90 target")
    ax.set_xlabel("Strategy")
    ax.set_ylabel("FinOps Index (FI)")
    ax.set_title("Figure 5.9: FinOps Index by Strategy and Traffic Pattern")
    ax.set_xticks(x)
    ax.set_xticklabels([LABELS[s] for s in STRATEGY_ORDER],
                       rotation=15)
    ax.set_ylim(0, 1.1)
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig("docs/figures/fig5_9_finops_index.png", dpi=150)
    plt.close()
    print("Saved: fig5_9_finops_index.png")

def fig_pareto(data):
    """Figure 5.12 — Cost vs SLA Pareto"""
    fig, ax = plt.subplots(figsize=(9, 7))
    for strat in STRATEGY_ORDER:
        costs, slas = [], []
        for pat in PATTERNS_ORDER:
            vals = data.get((strat, pat), [])
            if vals:
                costs.append(mean(vals, "cost"))
                slas.append(mean(vals, "sla"))
        if costs:
            ax.scatter(costs, slas,
                       color=COLORS[strat],
                       s=200, zorder=5,
                       label=LABELS[strat])
            ax.annotate(
                LABELS[strat],
                (sum(costs)/len(costs), sum(slas)/len(slas)),
                textcoords="offset points",
                xytext=(10, 5), fontsize=9
            )
    ax.axhline(y=99, color="red", linestyle="--",
               alpha=0.5, label="99% SLA target")
    ax.set_xlabel("Total Cost per 30-min Experiment ($)")
    ax.set_ylabel("SLA Compliance (%)")
    ax.set_title("Figure 5.12: Cost vs SLA Compliance — Pareto Analysis")
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig("docs/figures/fig5_12_pareto.png", dpi=150)
    plt.close()
    print("Saved: fig5_12_pareto.png")

def fig_scaling_law():
    """Figure 5.11 — Scaling extrapolation"""
    import sys; sys.path.insert(0,".")
    from pipeline.analytical_model import (
        find_optimal_workers, mmc_latency, compute_cost
    )
    lambdas = [50,100,250,500,1000,2500,5000,10000,25000]
    savings = []
    for lam in lambdas:
        w_opt  = find_optimal_workers(lam, 200, 1000)
        w_sta  = max(int(w_opt * 1.5), w_opt+1)
        c_dyn  = compute_cost(w_opt, 1.0)
        c_sta  = compute_cost(w_sta, 1.0)
        savings.append((c_sta - c_dyn)/c_sta*100)

    fig, ax = plt.subplots(figsize=(10, 5))
    scale = [l/50 for l in lambdas]
    ax.plot(scale, savings, "o-", color="#2DC653",
            linewidth=2, markersize=8, label="Cost Savings %")
    ax.axhline(y=33.3, color="gray", linestyle="--",
               alpha=0.7, label="33.3% constant")
    ax.set_xscale("log")
    ax.set_xlabel("Traffic Scale Factor (×baseline)")
    ax.set_ylabel("Cost Savings vs Static (%)")
    ax.set_title("Figure 5.11: Cost Savings Consistent Across All Scales")
    ax.set_ylim(0, 50)
    ax.legend()
    ax.grid(alpha=0.3)
    for i, (s, sv) in enumerate(zip(scale, savings)):
        ax.annotate(f"{sv:.1f}%", (s, sv),
                    textcoords="offset points",
                    xytext=(0,8), ha="center", fontsize=8)
    plt.tight_layout()
    plt.savefig("docs/figures/fig5_11_scaling.png", dpi=150)
    plt.close()
    print("Saved: fig5_11_scaling.png")

def main():
    data = load_data()
    if not data:
        print("No data yet!")
        return
    print(f"Loaded {sum(len(v) for v in data.values())} results")
    fig_throughput(data)
    fig_cost_comparison(data)
    fig_finops_index(data)
    fig_pareto(data)
    fig_scaling_law()
    print(f"\nAll figures saved to docs/figures/")

if __name__ == "__main__":
    main()