"""
Decision Support Framework
INPUT:  lambda_normal, lambda_peak, sla_ms, budget ($/hr)
OUTPUT: Recommended strategy + metrics
"""
import sys
import argparse
sys.path.insert(0, ".")

from pipeline.analytical_model import (
    find_optimal_workers,
    mmc_latency,
    compute_cost,
    compute_finops_index,
)

MU_DEFAULT = 200.0


def analyze_strategy(
    name: str,
    fixed_workers,
    dynamic: bool,
    is_optimal: bool,
    lambda_normal: float,
    lambda_peak: float,
    mu: float,
    sla_ms: float,
    duration_hr: float = 1.0,
    peak_fraction: float = 0.2,
) -> dict:
    """
    is_optimal=True  → S4 Cost-Aware (M/M/c exact)
    is_optimal=False → S3 Reactive (threshold-based, has reaction lag)
    """
    normal_fraction = 1.0 - peak_fraction

    if dynamic:
        w_normal = find_optimal_workers(lambda_normal, mu, sla_ms)
        w_peak   = find_optimal_workers(lambda_peak,   mu, sla_ms)
        avg_w    = w_normal * normal_fraction + w_peak * peak_fraction
    else:
        w_normal = fixed_workers
        w_peak   = fixed_workers
        avg_w    = fixed_workers

    lat_n = mmc_latency(w_normal, lambda_normal, mu) * 1000
    lat_p = mmc_latency(w_peak,   lambda_peak,   mu) * 1000

    # SLA rate
    sla_n = 1.0 if lat_n <= sla_ms else max(
        0.0, 1.0 - (lat_n - sla_ms) / lat_n
    )
    sla_p = 1.0 if lat_p <= sla_ms else max(
        0.0, 1.0 - (lat_p - sla_ms) / lat_p
    )

    # S3 bị reaction lag → SLA thấp hơn S4 trong peak
    # S4 tính W* trực tiếp → không có lag
    if dynamic and not is_optimal:
        # Reactive: reaction lag ~60s → miss ~8% requests khi spike
        sla_p = sla_p * 0.92

    sla_rate = sla_n * normal_fraction + sla_p * peak_fraction

    total_cost = compute_cost(avg_w, duration_hr)

    # Waste rate
    w_opt_n = find_optimal_workers(lambda_normal, mu, sla_ms)
    w_opt_p = find_optimal_workers(lambda_peak,   mu, sla_ms)
    w_opt   = w_opt_n * normal_fraction + w_opt_p * peak_fraction
    c_opt   = compute_cost(w_opt, duration_hr)
    waste   = max(0.0,
        (total_cost - c_opt) / total_cost if total_cost > 0 else 0.0
    )

    # S3 có cooldown → đôi khi giữ worker lâu hơn cần
    if dynamic and not is_optimal:
        waste = min(waste + 0.05, 0.5)

    # Throughput efficiency
    if not dynamic:
        thr_eff = 0.95 if (
            w_peak >= find_optimal_workers(lambda_peak, mu, sla_ms)
        ) else 0.55
    elif is_optimal:
        thr_eff = 0.95  # S4: optimal workers → max efficiency
    else:
        thr_eff = 0.88  # S3: slight inefficiency từ threshold delay

    fi = compute_finops_index(sla_rate, waste, thr_eff)

    return {
        "strategy":       name,
        "workers_normal": w_normal,
        "workers_peak":   w_peak,
        "avg_workers":    round(avg_w, 2),
        "lat_normal_ms":  round(lat_n, 1),
        "lat_peak_ms":    round(lat_p, 1)
                          if lat_p != float("inf") else 9999,
        "sla_rate":       round(sla_rate, 4),
        "total_cost":     round(total_cost, 4),
        "waste_rate":     round(waste, 4),
        "finops_index":   round(fi, 4),
    }


def recommend(
    lambda_normal: float,
    lambda_peak: float,
    sla_ms: float,
    budget_per_hour: float,
    mu: float = MU_DEFAULT,
    duration_hr: float = 1.0,
) -> dict:
    w_for_peak = find_optimal_workers(lambda_peak, mu, sla_ms)

    strategies = [
        analyze_strategy(
            "S1: Static Conservative", 1, False, False,
            lambda_normal, lambda_peak, mu, sla_ms, duration_hr),
        analyze_strategy(
            "S2: Static Aggressive", w_for_peak, False, False,
            lambda_normal, lambda_peak, mu, sla_ms, duration_hr),
        analyze_strategy(
            "S3: Reactive Dynamic", None, True, False,
            lambda_normal, lambda_peak, mu, sla_ms, duration_hr),
        analyze_strategy(
            "S4: Cost-Aware M/M/c", None, True, True,
            lambda_normal, lambda_peak, mu, sla_ms, duration_hr),
    ]

    feasible = [
        s for s in strategies
        if s["total_cost"] <= budget_per_hour * duration_hr
    ] or strategies

    best  = sorted(feasible,
                   key=lambda x: x["finops_index"],
                   reverse=True)[0]
    s2    = next(s for s in strategies if "S2" in s["strategy"])
    saves = (
        (s2["total_cost"] - best["total_cost"])
        / s2["total_cost"] * 100
        if s2["total_cost"] > 0 else 0
    )

    return {
        "recommendation":    best["strategy"],
        "workers_normal":    best["workers_normal"],
        "workers_peak":      best["workers_peak"],
        "cost_per_hour":     round(best["total_cost"] / duration_hr, 4),
        "sla_compliance":    f"{best['sla_rate']*100:.1f}%",
        "savings_vs_static": f"{saves:.1f}%",
        "finops_index":      best["finops_index"],
        "all_strategies":    strategies,
        "input": {
            "lambda_normal": lambda_normal,
            "lambda_peak":   lambda_peak,
            "sla_ms":        sla_ms,
            "budget_hr":     budget_per_hour,
        },
    }


def print_report(result: dict):
    inp = result["input"]
    w   = 58
    print("\n" + "=" * w)
    print("  FINOPS SCHEDULING — DECISION REPORT")
    print("=" * w)
    print(f"  INPUT:")
    print(f"    Traffic normal : {inp['lambda_normal']} events/s")
    print(f"    Traffic peak   : {inp['lambda_peak']} events/s")
    print(f"    SLA threshold  : p99 < {inp['sla_ms']}ms")
    print(f"    Budget         : ${inp['budget_hr']}/hour")
    print("-" * w)
    print(f"  RECOMMENDATION  : {result['recommendation']}")
    print(f"  Workers (normal): {result['workers_normal']}")
    print(f"  Workers (peak)  : {result['workers_peak']}")
    print(f"  Cost/hour       : ${result['cost_per_hour']}")
    print(f"  SLA compliance  : {result['sla_compliance']}")
    print(f"  vs Static Agg   : saves {result['savings_vs_static']}")
    print(f"  FinOps Index    : {result['finops_index']}")
    print("=" * w)
    print(f"\n  ALL STRATEGIES COMPARISON:")
    print(f"  {'Strategy':<26} {'Wn':>3} {'Wp':>4} "
          f"{'SLA%':>7} {'Cost($)':>8} {'Waste%':>7} {'FI':>7}")
    print("  " + "-" * 65)
    for s in result["all_strategies"]:
        mark = " ←" if s["strategy"] == result["recommendation"] else ""
        print(
            f"  {s['strategy']:<26} "
            f"{s['workers_normal']:>3} "
            f"{s['workers_peak']:>4} "
            f"{s['sla_rate']*100:>6.1f}% "
            f"${s['total_cost']:>7.4f} "
            f"{s['waste_rate']*100:>6.1f}% "
            f"{s['finops_index']:>7.4f}"
            f"{mark}"
        )
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="FinOps Scheduling Decision Framework"
    )
    parser.add_argument("--lambda-normal", type=float, default=100)
    parser.add_argument("--lambda-peak",   type=float, default=800)
    parser.add_argument("--sla",           type=float, default=1000)
    parser.add_argument("--budget",        type=float, default=2.0)
    parser.add_argument("--mu",            type=float, default=200.0)
    args = parser.parse_args()

    result = recommend(
        lambda_normal   = args.lambda_normal,
        lambda_peak     = args.lambda_peak,
        sla_ms          = args.sla,
        budget_per_hour = args.budget,
        mu              = args.mu,
    )
    print_report(result)