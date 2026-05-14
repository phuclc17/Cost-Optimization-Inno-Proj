"""
Static Schedulers
─────────────────
S1: Conservative — luôn giữ MIN workers
S2: Aggressive   — luôn giữ MAX workers
"""
import logging
from typing import Dict
from scheduler.base import BaseScheduler

logger = logging.getLogger(__name__)


class StaticConservativeScheduler(BaseScheduler):
    """
    S1: Luôn dùng 1 worker.
    Chi phí thấp nhất, hiệu năng thấp nhất.
    """
    def __init__(self, experiment_id: str):
        super().__init__(
            strategy_name  = "static_conservative",
            experiment_id  = experiment_id,
            min_workers    = 1,
            max_workers    = 1,
            interval_seconds = 30
        )

    def decide(self, metrics: Dict) -> int:
        return 1  # Luôn 1 worker


class StaticAggressiveScheduler(BaseScheduler):
    """
    S2: Luôn dùng MAX workers.
    Chi phí cao nhất, hiệu năng cao nhất.
    Dùng làm upper bound benchmark.
    """
    def __init__(
        self,
        experiment_id: str,
        workers: int = 3
    ):
        super().__init__(
            strategy_name  = "static_aggressive",
            experiment_id  = experiment_id,
            min_workers    = workers,
            max_workers    = workers,
            interval_seconds = 30
        )
        self.fixed_workers = workers

    def decide(self, metrics: Dict) -> int:
        return self.fixed_workers  # Luôn cố định