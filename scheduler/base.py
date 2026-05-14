"""
Base Scheduler — Abstract class
Tất cả schedulers kế thừa từ đây.
"""
import time
import logging
import subprocess
from abc import ABC, abstractmethod
from typing import Dict
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Cost simulation (USD per worker per hour)
COST_PER_WORKER_HOUR = 0.048

class BaseScheduler(ABC):

    def __init__(
        self,
        strategy_name: str,
        experiment_id: str,
        min_workers: int = 1,
        max_workers: int = 5,
        interval_seconds: int = 30
    ):
        self.strategy      = strategy_name
        self.experiment_id = experiment_id
        self.min_workers   = min_workers
        self.max_workers   = max_workers
        self.interval      = interval_seconds
        self.running       = False
        self.decisions     = []

        # Import DB
        import sys
        sys.path.insert(0, ".")
        from pipeline.storage.db_client import DatabaseClient
        self.db = DatabaseClient()

        logger.info(
            f"Scheduler init | strategy={strategy_name} | "
            f"experiment={experiment_id}"
        )

    @abstractmethod
    def decide(self, metrics: Dict) -> int:
        """
        Quyết định số workers cần có.
        Trả về: target worker count
        """
        pass

    def get_current_workers(self) -> int:
        """Đếm số worker containers đang chạy."""
        try:
            result = subprocess.run(
                ["docker", "compose", "ps",
                 "--format", "{{.Name}}", "worker"],
                capture_output=True, text=True
            )
            lines = [
                l for l in result.stdout.strip().split("\n")
                if l and "worker" in l
            ]
            return max(len(lines), 1)
        except Exception:
            return 1

    def scale_to(self, target: int):
        """Scale workers tới target số lượng."""
        target  = max(self.min_workers,
                      min(self.max_workers, target))
        current = self.get_current_workers()

        if target == current:
            return

        action = "scale_out" if target > current else "scale_in"
        logger.info(
            f"Scaling {current} → {target} workers "
            f"({action})"
        )

        subprocess.run(
            ["docker", "compose", "up", "-d",
             "--scale", f"worker={target}",
             "--no-recreate"],
            capture_output=True
        )

        # Log decision
        self._log_decision(action, current, target)

    def _log_decision(
        self,
        action: str,
        before: int,
        after: int,
        reason: str = "",
        cpu: float = 0.0
    ):
        cost = after * COST_PER_WORKER_HOUR
        decision = {
            "strategy":        self.strategy,
            "experiment_id":   self.experiment_id,
            "action":          action,
            "workers_before":  before,
            "workers_after":   after,
            "trigger_reason":  reason,
            "cpu_at_decision": cpu,
            "cost_per_hour":   cost,
        }
        self.decisions.append(decision)
        try:
            self.db.log_scaling_decision(decision)
        except Exception as e:
            logger.warning(f"Could not log decision: {e}")

    def get_metrics(self) -> Dict:
        """
        Thu thập metrics từ worker.
        Trả về cpu_pct, latency, throughput.
        """
        import requests
        try:
            r = requests.get(
                "http://localhost:8000/metrics",
                timeout=3
            )
            text = r.text

            # Parse Prometheus text format
            cpu     = self._parse_metric(
                text, "process_cpu_seconds_total"
            )
            events  = self._parse_metric(
                text, "worker_events_total"
            )
            return {
                "cpu_pct":   min(cpu * 100, 100),
                "events":    events,
                "timestamp": datetime.now(
                    timezone.utc
                ).isoformat()
            }
        except Exception:
            return {"cpu_pct": 0, "events": 0}

    def _parse_metric(self, text: str, name: str) -> float:
        """Parse 1 metric từ Prometheus text."""
        for line in text.split("\n"):
            if line.startswith(name) and not line.startswith("#"):
                try:
                    return float(line.split()[-1])
                except Exception:
                    pass
        return 0.0

    def run(self, duration_seconds: int = 1800):
        """Main loop: chạy scheduler trong duration giây."""
        self.running   = True
        start_time     = time.time()
        cycle          = 0

        logger.info(
            f"Scheduler started | "
            f"strategy={self.strategy} | "
            f"duration={duration_seconds}s"
        )

        while self.running:
            elapsed = time.time() - start_time
            if elapsed >= duration_seconds:
                break

            cycle  += 1
            metrics = self.get_metrics()
            target  = self.decide(metrics)
            self.scale_to(target)

            logger.info(
                f"Cycle {cycle:3d} | "
                f"t={elapsed:5.0f}s | "
                f"workers={self.get_current_workers()} | "
                f"cpu={metrics.get('cpu_pct', 0):.1f}%"
            )

            time.sleep(self.interval)

        self.running = False
        logger.info(
            f"Scheduler done | "
            f"cycles={cycle} | "
            f"decisions={len(self.decisions)}"
        )