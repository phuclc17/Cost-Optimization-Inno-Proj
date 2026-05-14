"""
Analytical Model — M/M/c Queueing Theory
==========================================
Dùng cho:
  1. S4 Scheduler tính W* real-time
  2. Decision Framework recommend strategy
  3. Scaling analysis extrapolate to large scale
  4. Model validation (theory vs measured)
"""
import math
from typing import Dict, List, Tuple


# ─── CORE M/M/c FORMULAS ─────────────────────────────────

def erlang_c(c: int, rho: float) -> float:
    """
    Erlang-C: xác suất request phải chờ trong hàng.
    c   = số workers
    rho = traffic intensity = λ/(c×μ)
    """
    if rho >= 1.0:
        return 1.0  # Hệ thống quá tải

    try:
        crho  = c * rho
        # (cρ)^c / c!
        term  = (crho ** c) / math.factorial(c)
        denom = term / (1.0 - rho)
        # Σ(k=0..c-1) (cρ)^k/k!
        sigma = sum(
            (crho ** k) / math.factorial(k)
            for k in range(c)
        )
        total = sigma + denom
        return denom / total if total > 0 else 1.0
    except (OverflowError, ZeroDivisionError):
        return 1.0


def mmc_latency(
    c: int,
    lambda_: float,
    mu: float
) -> float:
    """
    End-to-end latency từ M/M/c (seconds).
    Return float('inf') nếu hệ thống không ổn định.
    """
    if c <= 0 or mu <= 0 or lambda_ <= 0:
        return float("inf")

    rho = lambda_ / (c * mu)
    if rho >= 1.0:
        return float("inf")

    ec      = erlang_c(c, rho)
    w_queue = ec / (c * mu - lambda_)  # thời gian chờ
    latency = w_queue + 1.0 / mu       # + service time
    return latency


def find_optimal_workers(
    lambda_: float,
    mu: float,
    sla_ms: float,
    min_w: int = 1,
    max_w: int = 200
) -> int:
    """
    W* = số workers nhỏ nhất đảm bảo SLA.
    W* = argmin{c ∈ ℤ⁺ : L(c,λ,μ)×1000 ≤ sla_ms}
    """
    sla_s = sla_ms / 1000.0

    for c in range(min_w, max_w + 1):
        lat = mmc_latency(c, lambda_, mu)
        if lat != float("inf") and lat <= sla_s:
            return c

    return max_w  # fallback


# ─── FINOPS CALCULATIONS ─────────────────────────────────

COST_PER_WORKER_HOUR = 0.048  # USD (AWS t3.small equiv)


def compute_cost(
    workers: float,
    duration_hours: float
) -> float:
    """Chi phí = workers × $0.048 × hours."""
    return workers * COST_PER_WORKER_HOUR * duration_hours


def compute_finops_index(
    sla_rate: float,
    waste_rate: float,
    throughput_efficiency: float,
    alpha: float = 0.4,
    beta: float = 0.4,
    gamma: float = 0.2
) -> float:
    """
    FI = α×SLA_rate + β×(1-Waste_rate) + γ×Throughput_eff
    FI ∈ [0, 1], cao hơn = tốt hơn.
    """
    return (
        alpha * sla_rate
        + beta * (1.0 - waste_rate)
        + gamma * throughput_efficiency
    )


def compute_waste_rate(
    actual_workers: float,
    lambda_: float,
    mu: float,
    sla_ms: float
) -> float:
    """
    Waste = (W_actual - W*) / W_actual
    """
    w_optimal = find_optimal_workers(lambda_, mu, sla_ms)
    if actual_workers <= 0:
        return 0.0
    waste = (actual_workers - w_optimal) / actual_workers
    return max(0.0, min(1.0, waste))


# ─── SCALING ANALYSIS ────────────────────────────────────

def theoretical_scaling_table(
    mu: float = 200.0,
    sla_ms: float = 1000.0,
    lambda_values: List[float] = None
) -> List[Dict]:
    """
    Tạo bảng scaling analysis.
    Với mỗi λ: tính W*, cost dynamic vs static, savings%.

    Dùng cho: Report Table 5.11
    """
    if lambda_values is None:
        lambda_values = [50, 100, 250, 500,
                         1000, 2500, 5000, 10000, 25000]

    baseline_lambda = lambda_values[0]
    rows = []

    for lam in lambda_values:
        w_optimal = find_optimal_workers(lam, mu, sla_ms)
        # Static aggressive: đủ workers cho peak
        w_static  = find_optimal_workers(lam, mu, sla_ms)

        lat_theory = mmc_latency(w_optimal, lam, mu) * 1000

        cost_dynamic = compute_cost(w_optimal, 1.0)
        # Static cần W* cho peak, giữ cả lúc thấp tải
        cost_static  = compute_cost(w_static * 1.5, 1.0)

        savings_pct = (
            (cost_static - cost_dynamic) / cost_static * 100
            if cost_static > 0 else 0
        )

        rows.append({
            "lambda_rps":        lam,
            "scale_factor":      lam / baseline_lambda,
            "w_optimal":         w_optimal,
            "w_static_equiv":    int(w_static * 1.5),
            "latency_theory_ms": round(lat_theory, 2),
            "cost_dynamic_hr":   round(cost_dynamic, 4),
            "cost_static_hr":    round(cost_static, 4),
            "savings_pct":       round(savings_pct, 1),
        })

    return rows


def print_scaling_table(rows: List[Dict]):
    """Print bảng đẹp ra terminal."""
    print("\n" + "=" * 75)
    print("THEORETICAL SCALING ANALYSIS (M/M/c Model)")
    print(f"μ = 200 events/s/worker | SLA = 1000ms")
    print("=" * 75)
    print(f"{'λ (rps)':>10} {'Scale':>6} {'W*':>4} "
          f"{'Lat(ms)':>10} {'Cost Dyn':>10} "
          f"{'Cost Stat':>10} {'Savings':>8}")
    print("-" * 75)
    for r in rows:
        print(
            f"{r['lambda_rps']:>10} "
            f"{r['scale_factor']:>5.0f}× "
            f"{r['w_optimal']:>4} "
            f"{r['latency_theory_ms']:>10.1f} "
            f"${r['cost_dynamic_hr']:>9.4f} "
            f"${r['cost_static_hr']:>9.4f} "
            f"{r['savings_pct']:>7.1f}%"
        )
    print("=" * 75)


# ─── MODEL VALIDATION ────────────────────────────────────

def validate_model(
    measured_data: List[Dict]
) -> Dict:
    """
    So sánh theoretical latency vs measured latency.

    measured_data format:
    [{"workers": 1, "lambda_rps": 50, "mu": 200,
      "measured_p99_ms": 5.2}, ...]

    Returns: {"mean_error_pct": X, "rows": [...]}
    """
    results = []

    for d in measured_data:
        c       = d["workers"]
        lam     = d["lambda_rps"]
        mu      = d.get("mu", 200.0)
        p99_ms  = d["measured_p99_ms"]

        lat_theory = mmc_latency(c, lam, mu) * 1000

        if lat_theory != float("inf") and p99_ms > 0:
            error_pct = abs(lat_theory - p99_ms) / p99_ms * 100
        else:
            error_pct = None

        results.append({
            "workers":          c,
            "lambda_rps":       lam,
            "latency_theory":   round(lat_theory, 2),
            "latency_measured": p99_ms,
            "error_pct":        round(error_pct, 1) if error_pct else None,
        })

    valid_errors = [
        r["error_pct"] for r in results
        if r["error_pct"] is not None
    ]
    mean_error = (
        sum(valid_errors) / len(valid_errors)
        if valid_errors else None
    )

    return {
        "mean_error_pct": round(mean_error, 2) if mean_error else None,
        "rows": results,
        "model_acceptable": mean_error < 20 if mean_error else False
    }


# ─── AMDAHL'S LAW ────────────────────────────────────────

def amdahls_speedup(w: int, serial_fraction: float) -> float:
    """
    Speedup(W) = 1 / (S + (1-S)/W)
    S = serial fraction
    """
    p = 1.0 - serial_fraction
    return 1.0 / (serial_fraction + p / w)


def amdahls_efficiency(w: int, serial_fraction: float) -> float:
    """E(W) = Speedup(W) / W"""
    return amdahls_speedup(w, serial_fraction) / w


def estimate_serial_fraction(
    measured_throughputs: Dict[int, float]
) -> float:
    """
    Ước tính serial fraction S từ measured data.
    measured_throughputs = {1: 53.6, 2: 98.0, 3: 132.0, ...}
    """
    if 1 not in measured_throughputs:
        return 0.1  # default

    base = measured_throughputs[1]
    best_fit_error = float("inf")
    best_s = 0.1

    for s_candidate in [i / 100 for i in range(1, 50)]:
        total_error = 0.0
        count = 0
        for w, thr in measured_throughputs.items():
            if w == 1:
                continue
            predicted_speedup = amdahls_speedup(w, s_candidate)
            actual_speedup    = thr / base
            total_error += (predicted_speedup - actual_speedup) ** 2
            count += 1
        if count > 0:
            mse = total_error / count
            if mse < best_fit_error:
                best_fit_error = mse
                best_s = s_candidate

    return best_s


# ─── MAIN (test khi chạy trực tiếp) ─────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("M/M/c Analytical Model — Quick Test")
    print("=" * 50)

    mu  = 200.0   # events/s per worker
    sla = 1000.0  # ms

    print("\n--- Optimal Workers W* ---")
    print(f"{'λ (rps)':>10} {'W*':>5} {'Latency(ms)':>12} {'Cost/hr':>10}")
    print("-" * 45)
    for lam in [50, 100, 200, 300, 500, 800, 1000]:
        w   = find_optimal_workers(lam, mu, sla)
        lat = mmc_latency(w, lam, mu) * 1000
        cost = compute_cost(w, 1.0)
        print(f"{lam:>10} {w:>5} {lat:>12.1f} ${cost:>9.4f}")

    print("\n--- Scaling Table ---")
    rows = theoretical_scaling_table(mu=200, sla_ms=1000)
    print_scaling_table(rows)

    print("\n--- FinOps Index Examples ---")
    scenarios = [
        ("S1 Static Low",  0.65, 0.05, 0.45),
        ("S2 Static High", 0.99, 0.67, 1.00),
        ("S3 Reactive",    0.94, 0.35, 0.88),
        ("S4 Cost-Aware",  0.97, 0.22, 0.90),
    ]
    for name, sla_r, waste, thr in scenarios:
        fi = compute_finops_index(sla_r, waste, thr)
        print(f"  {name:<20} FI = {fi:.3f}")

    print("\n✅ Analytical model OK")