"""
Statistical Analysis of Benchmark Results
Dung: python3 benchmark/analysis.py
"""
import sys
sys.path.insert(0, ".")

from pipeline.storage.db_client import DatabaseClient
from pipeline.analytical_model import compute_finops_index
import statistics
import math

def load_results():
    db = DatabaseClient()
    return db.get_all_benchmark_results()

def group_by(results, key):
    groups = {}
    for r in results:
        k = r[key]
        groups.setdefault(k, []).append(r)
    return groups

def mean(values):
    return sum(values) / len(values) if values else 0

def std(values):
    if len(values) < 2:
        return 0
    m = mean(values)
    return math.sqrt(sum((x-m)**2 for x in values)/(len(values)-1))

def ci95(values):
    if len(values) < 2:
        return 0
    return 1.96 * std(values) / math.sqrt(len(values))

def welch_t(g1, g2):
    """Welch's t-test, returns (t_stat, reject_H0)"""
    if not g1 or not g2:
        return 0, False
    m1, m2 = mean(g1), mean(g2)
    s1, s2 = std(g1), std(g2)
    n1, n2 = len(g1), len(g2)
    if s1 == 0 and s2 == 0:
        return 0, False
    se = math.sqrt((s1**2/n1 + s2**2/n2) + 1e-10)
    t  = abs(m1 - m2) / se
    return round(t, 3), t > 2.776  # df≈2, α=0.05

def cohens_d(g1, g2):
    if not g1 or not g2:
        return 0
    pooled = math.sqrt((std(g1)**2 + std(g2)**2) / 2 + 1e-10)
    return round(abs(mean(g1) - mean(g2)) / pooled, 3)

def print_summary(results):
    print("\n" + "=" * 70)
    print("BENCHMARK RESULTS SUMMARY")
    print("=" * 70)

    by_strategy = group_by(results, "strategy")

    metrics = [
        ("avg_throughput_rps", "Throughput (rps)"),
        ("p99_latency_ms",     "p99 Latency (ms)"),
        ("sla_compliance_pct", "SLA Compliance (%)"),
        ("total_cost_usd",     "Total Cost ($)"),
        ("finops_score",       "FinOps Index"),
    ]

    for metric_key, metric_name in metrics:
        print(f"\n--- {metric_name} ---")
        print(f"{'Strategy':<25} {'Mean':>8} {'±Std':>8} "
              f"{'95%CI':>8} {'n':>4}")
        print("-" * 60)

        for strategy in ["static_conservative",
                         "static_aggressive",
                         "reactive",
                         "cost_aware"]:
            if strategy not in by_strategy:
                continue
            vals = [
                float(r[metric_key])
                for r in by_strategy[strategy]
                if r.get(metric_key) is not None
            ]
            if not vals:
                continue
            m  = mean(vals)
            s  = std(vals)
            ci = ci95(vals)
            print(f"{strategy:<25} {m:>8.3f} "
                  f"{s:>8.3f} {ci:>8.3f} {len(vals):>4}")

def print_hypothesis_tests(results):
    print("\n" + "=" * 70)
    print("HYPOTHESIS TESTS (Welch's t-test, α=0.05)")
    print("=" * 70)

    by_strategy = group_by(results, "strategy")

    def get_vals(strategy, metric):
        return [
            float(r[metric])
            for r in by_strategy.get(strategy, [])
            if r.get(metric) is not None
        ]

    tests = [
        ("H1 Cost: S4 vs S2",
         get_vals("cost_aware",         "total_cost_usd"),
         get_vals("static_aggressive",  "total_cost_usd"),
         "lower"),
        ("H2 SLA T2: S4 vs S3",
         [float(r["sla_compliance_pct"])
          for r in results
          if r["strategy"] == "cost_aware"
          and r["traffic_pattern"] == "peak"],
         [float(r["sla_compliance_pct"])
          for r in results
          if r["strategy"] == "reactive"
          and r["traffic_pattern"] == "peak"],
         "higher"),
        ("H3 FI: S4 vs S3",
         get_vals("cost_aware", "finops_score"),
         get_vals("reactive",   "finops_score"),
         "higher"),
    ]

    for name, g1, g2, direction in tests:
        t, sig = welch_t(g1, g2)
        d = cohens_d(g1, g2)
        m1, m2 = mean(g1), mean(g2)
        result = (
            "CONFIRMED ✅" if (
                sig and (
                    (direction == "lower"  and m1 < m2) or
                    (direction == "higher" and m1 > m2)
                )
            ) else "NOT CONFIRMED ❌"
        )
        print(f"\n{name}:")
        print(f"  Group1 mean={m1:.4f} | Group2 mean={m2:.4f}")
        print(f"  t={t} | significant={'Yes' if sig else 'No'}")
        print(f"  Cohen's d={d} | Result: {result}")

def print_finops_ranking(results):
    print("\n" + "=" * 70)
    print("FINOPS INDEX RANKING BY STRATEGY × PATTERN")
    print("=" * 70)

    by_strat_pat = {}
    for r in results:
        k = (r["strategy"], r["traffic_pattern"])
        by_strat_pat.setdefault(k, []).append(
            float(r["finops_score"])
        )

    print(f"{'Strategy':<25} {'Pattern':<8} "
          f"{'Mean FI':>8} {'Std':>6} {'n':>4}")
    print("-" * 60)

    rows = []
    for (strat, pat), vals in by_strat_pat.items():
        rows.append((mean(vals), strat, pat, std(vals), len(vals)))
    rows.sort(reverse=True)

    for m, strat, pat, s, n in rows:
        print(f"{strat:<25} {pat:<8} {m:>8.4f} {s:>6.4f} {n:>4}")

def print_cost_savings(results):
    print("\n" + "=" * 70)
    print("COST SAVINGS vs STATIC AGGRESSIVE")
    print("=" * 70)

    by_strategy = group_by(results, "strategy")

    s2_cost = mean([
        float(r["total_cost_usd"])
        for r in by_strategy.get("static_aggressive", [])
    ])

    if s2_cost == 0:
        print("No S2 data yet")
        return

    for strat in ["static_conservative", "reactive", "cost_aware"]:
        vals = [
            float(r["total_cost_usd"])
            for r in by_strategy.get(strat, [])
        ]
        if not vals:
            continue
        m = mean(vals)
        savings = (s2_cost - m) / s2_cost * 100
        print(f"{strat:<25} avg=${m:.4f} "
              f"saves {savings:+.1f}% vs S2")

def main():
    db_env_set = all([
        __import__("os").getenv("POSTGRES_HOST"),
        __import__("os").getenv("POSTGRES_USER"),
    ])
    if not db_env_set:
        print("Set DB env vars first!")
        print("POSTGRES_HOST=localhost POSTGRES_USER=phuclc17 ...")
        return

    results = load_results()
    if not results:
        print("No results yet. Run experiments first.")
        return

    print(f"Loaded {len(results)} experiment results")

    print_summary(results)
    print_hypothesis_tests(results)
    print_finops_ranking(results)
    print_cost_savings(results)

if __name__ == "__main__":
    main()