"""
Reactive Dynamic Scheduler (S3)
────────────────────────────────
Scale OUT khi CPU cao hoặc latency cao.
Scale IN  khi CPU thấp và latency thấp.
Có cooldown để tránh oscillation.
"""
import time
import logging
import requests
from typing import Dict
from scheduler.base import BaseScheduler

logger = logging.getLogger(__name__)


class ReactiveScheduler(BaseScheduler):
    """
    S3: Threshold-based reactive scheduling.
    Hành động DỰA TRÊN metrics hiện tại.
    """

    # Thresholds
    CPU_HIGH    = 70.0   # % → scale out
    CPU_LOW     = 25.0   # % → scale in
    LAT_HIGH_MS = 500    # ms → scale out
    COOLDOWN    = 60     # giây giữa 2 lần scale

    def __init__(self, experiment_id: str):
        super().__init__(
            strategy_name    = "reactive",
            experiment_id    = experiment_id,
            min_workers      = 1,
            max_workers      = 5,
            interval_seconds = 15
        )
        self.last_action_time = 0
        self.latency_window   = []

    def get_p99_latency(self) -> float:
        """Lấy p99 latency từ worker stats."""
        try:
            r = requests.get(
                "http://localhost:8000/stats",
                timeout=3
            )
            channels = r.json().get("channels", [])
            if not channels:
                return 0.0
            p99s = [
                ch.get("p99_latency", 0)
                for ch in channels
            ]
            return max(p99s) if p99s else 0.0
        except Exception:
            return 0.0

    def decide(self, metrics: Dict) -> int:
        now     = time.time()
        current = self.get_current_workers()
        cpu     = metrics.get("cpu_pct", 0)
        p99     = self.get_p99_latency()

        # Cooldown check
        if now - self.last_action_time < self.COOLDOWN:
            return current

        # Scale OUT: tải cao
        if (cpu > self.CPU_HIGH or p99 > self.LAT_HIGH_MS):
            if current < self.max_workers:
                target = min(current + 1, self.max_workers)
                reason = (
                    f"HIGH_LOAD cpu={cpu:.1f}% "
                    f"p99={p99:.0f}ms"
                )
                logger.info(f"SCALE_OUT: {reason}")
                self._log_decision(
                    "scale_out", current, target,
                    reason, cpu
                )
                self.last_action_time = now
                return target

        # Scale IN: tải thấp
        elif (cpu < self.CPU_LOW
              and p99 < self.LAT_HIGH_MS / 2
              and current > self.min_workers):
            target = max(current - 1, self.min_workers)
            reason = (
                f"LOW_LOAD cpu={cpu:.1f}% "
                f"p99={p99:.0f}ms"
            )
            logger.info(f"SCALE_IN: {reason}")
            self._log_decision(
                "scale_in", current, target,
                reason, cpu
            )
            self.last_action_time = now
            return target

        return current