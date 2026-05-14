"""
Cost-Aware Scheduler (S4) — M/M/c Based
─────────────────────────────────────────
Dùng M/M/c Queueing Theory để tìm W* tối ưu:
W* = workers nhỏ nhất đảm bảo SLA

Khác S3: S4 dùng CÔNG THỨC TOÁN HỌC
         không chỉ dựa vào threshold
"""
import time
import math
import logging
import requests
from typing import Dict, Optional
from collections import deque
from scheduler.base import BaseScheduler

logger = logging.getLogger(__name__)


def erlang_c(c: int, rho: float) -> float:
    """
    Erlang-C formula: P(waiting > 0)
    c   = số servers (workers)
    rho = traffic intensity = λ/(c×μ)
    """
    if rho >= 1.0:
        return 1.0  # Hệ thống không ổn định

    # Tính (c*rho)^c / c!
    crho    = c * rho
    term    = (crho ** c) / math.factorial(c)
    denom   = term / (1 - rho)

    # Tính Σ(k=0..c-1) (c*rho)^k/k!
    sigma = sum(
        (crho ** k) / math.factorial(k)
        for k in range(c)
    )

    total = sigma + denom
    if total == 0:
        return 1.0
    return denom / total


def mmс_latency(
    c: int,
    lambda_: float,
    mu: float
) -> float:
    """
    Tính end-to-end latency từ M/M/c model (giây).

    c       = số workers
    lambda_ = arrival rate (events/s)
    mu      = service rate per worker (events/s)
    """
    if c <= 0 or mu <= 0:
        return float("inf")

    rho = lambda_ / (c * mu)

    if rho >= 1.0:
        return float("inf")   # Hệ thống quá tải

    ec      = erlang_c(c, rho)
    w_queue = ec / (c * mu - lambda_)   # Thời gian chờ
    latency = w_queue + 1.0 / mu        # + service time
    return latency


def find_optimal_workers(
    lambda_: float,
    mu: float,
    sla_ms: float,
    min_w: int = 1,
    max_w: int = 10
) -> int:
    """
    Tìm W* = workers nhỏ nhất đảm bảo SLA.

    W* = argmin{c} subject to:
         mmс_latency(c, λ, μ) × 1000 ≤ sla_ms
    """
    sla_s = sla_ms / 1000.0  # Đổi ms → giây

    for c in range(min_w, max_w + 1):
        lat = mmс_latency(c, lambda_, mu)
        if lat <= sla_s:
            return c

    return max_w  # Fallback: dùng max


class CostAwareScheduler(BaseScheduler):
    """
    S4: Cost-Aware scheduling dựa trên M/M/c.
    Tự đo λ và μ, tính W* tối ưu mỗi 15 giây.
    """

    SLA_MS        = 1000.0  # SLA threshold
    MU_DEFAULT    = 200.0   # Default service rate
    WINDOW        = 60      # Giây để đo λ

    def __init__(self, experiment_id: str):
        super().__init__(
            strategy_name    = "cost_aware",
            experiment_id    = experiment_id,
            min_workers      = 1,
            max_workers      = 5,
            interval_seconds = 15
        )
        self.event_times  = deque()   # Timestamps events
        self.mu_measured  = self.MU_DEFAULT
        self.last_events  = 0

    def measure_lambda(self) -> float:
        """Đo arrival rate λ từ Prometheus."""
        try:
            r = requests.get(
                "http://localhost:8000/metrics",
                timeout=3
            )
            # Đếm tổng events
            total = 0.0
            for line in r.text.split("\n"):
                if (line.startswith("worker_events_total{")
                        and 'status="success"' in line):
                    try:
                        total += float(line.split()[-1])
                    except Exception:
                        pass

            # Tính rate từ delta
            delta   = total - self.last_events
            self.last_events = total
            lambda_ = delta / self.interval

            return max(lambda_, 1.0)
        except Exception:
            return 50.0  # Fallback

    def measure_mu(self) -> float:
        """Đo service rate μ từ worker stats."""
        try:
            r     = requests.get(
                "http://localhost:8000/stats",
                timeout=3
            )
            channels = r.json().get("channels", [])
            if not channels:
                return self.MU_DEFAULT

            # μ = 1000 / avg_latency_ms
            lats = [
                ch.get("avg_latency", 5)
                for ch in channels
                if ch.get("avg_latency", 0) > 0
            ]
            if lats:
                avg_lat_ms = sum(lats) / len(lats)
                mu = 1000.0 / avg_lat_ms
                self.mu_measured = mu
                return mu

        except Exception:
            pass
        return self.mu_measured

    def decide(self, metrics: Dict) -> int:
        """
        Tính W* tối ưu bằng M/M/c formula.
        """
        lambda_ = self.measure_lambda()
        mu      = self.measure_mu()
        current = self.get_current_workers()

        w_star = find_optimal_workers(
            lambda_  = lambda_,
            mu       = mu,
            sla_ms   = self.SLA_MS,
            min_w    = self.min_workers,
            max_w    = self.max_workers
        )

        # Tính latency lý thuyết tại W*
        lat_theory = mmс_latency(w_star, lambda_, mu) * 1000

        logger.info(
            f"M/M/c | λ={lambda_:.1f} μ={mu:.1f} "
            f"W*={w_star} lat={lat_theory:.1f}ms"
        )

        if w_star != current:
            action = (
                "scale_out"
                if w_star > current
                else "scale_in"
            )
            reason = (
                f"MMC_OPTIMAL λ={lambda_:.1f} "
                f"μ={mu:.1f} W*={w_star} "
                f"lat={lat_theory:.1f}ms"
            )
            self._log_decision(
                action, current, w_star,
                reason, metrics.get("cpu_pct", 0)
            )

        return w_star
# Latin aliases (fix Cyrillic character issue)
mmc_latency = mmс_latency
