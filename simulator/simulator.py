import os
import sys
import time
import uuid
import random
import logging
import threading
from datetime import datetime, timezone
from typing import Optional
import requests
from traffic import get_pattern

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

WORKER_URL = os.getenv("WORKER_URL", "http://localhost:8000")

# ─── CHANNEL CONFIG ──────────────────────────────────────
CHANNELS = {
    "web": {
        "weight":      0.50,
        "event_types": ["page_view", "add_to_cart",
                        "purchase", "cart_abandon"],
        "amount_range": (50_000, 2_000_000),
        "devices":     ["mobile", "desktop", "tablet"],
        "regions":     ["HCM", "HN", "DN", "CT", "BD"],
    },
    "pos": {
        "weight":      0.30,
        "event_types": ["transaction", "return", "exchange"],
        "amount_range": (30_000, 500_000),
        "devices":     ["pos_terminal"],
        "regions":     ["HCM", "HN", "DN", "CT"],
    },
    "marketplace": {
        "weight":      0.20,
        "event_types": ["order_placed", "order_shipped",
                        "order_delivered", "order_cancelled"],
        "amount_range": (20_000, 1_500_000),
        "devices":     ["mobile", "desktop"],
        "regions":     ["HCM", "HN", "DN", "CT", "HP"],
    },
}

CATEGORIES = [
    "electronics", "fashion", "food_beverage",
    "home_living", "beauty", "sports", "books",
]


def pick_channel() -> str:
    """Chọn channel theo tỉ lệ weight."""
    r = random.random()
    cumulative = 0.0
    for name, info in CHANNELS.items():
        cumulative += info["weight"]
        if r <= cumulative:
            return name
    return "web"


def generate_event(channel: str) -> dict:
    """Tạo 1 retail event ngẫu nhiên."""
    ch = CHANNELS[channel]
    return {
        "event_id":   str(uuid.uuid4()),
        "channel":    channel,
        "event_type": random.choice(ch["event_types"]),
        "user_id":    f"user_{random.randint(1, 100_000)}",
        "product_id": f"prod_{random.randint(1, 10_000)}",
        "category":   random.choice(CATEGORIES),
        "amount":     round(
                          random.uniform(*ch["amount_range"]), -3
                      ),
        "region":     random.choice(ch["regions"]),
        "device":     random.choice(ch["devices"]),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def send_event(event: dict) -> Optional[float]:
    """
    Gửi event tới worker.
    Trả về latency_ms nếu thành công, None nếu lỗi.
    """
    try:
        start = time.perf_counter()
        resp = requests.post(
            f"{WORKER_URL}/process",
            json=event,
            timeout=5.0
        )
        latency_ms = (time.perf_counter() - start) * 1000
        if resp.status_code == 200:
            return latency_ms
        return None
    except requests.exceptions.RequestException:
        return None


class RetailSimulator:
    """Simulator chính: tạo và gửi events theo traffic pattern."""

    def __init__(self, pattern_name: str = "normal"):
        self.pattern  = get_pattern(pattern_name)
        self.running  = False
        self.stats    = {
            "sent": 0, "success": 0,
            "error": 0, "total_latency_ms": 0.0
        }
        logger.info(
            f"Simulator ready | pattern={pattern_name} | "
            f"base_rate={self.pattern.base_rate} rps"
        )

    def run(self, duration_seconds: int = 1800):
        """Chạy simulator trong duration_seconds giây."""
        self.running   = True
        start_time     = time.time()
        last_log_time  = start_time

        logger.info(
            f"Simulator started | duration={duration_seconds}s"
        )

        while self.running:
            elapsed = time.time() - start_time

            # Dừng khi hết thời gian
            if elapsed >= duration_seconds:
                break

            # Tính rate hiện tại
            current_rate = self.pattern.get_rate_at(elapsed)
            interval     = 1.0 / max(current_rate, 0.1)

            # Tạo và gửi event (async, không block loop)
            channel = pick_channel()
            event   = generate_event(channel)
            self.stats["sent"] += 1

            t = threading.Thread(
                target=self._send_async,
                args=(event,),
                daemon=True
            )
            t.start()

            time.sleep(interval)

            # Log mỗi 10 giây
            now = time.time()
            if now - last_log_time >= 10:
                self._log_stats(elapsed, current_rate)
                last_log_time = now

        self.running = False
        logger.info(
            f"Simulator done | "
            f"sent={self.stats['sent']} | "
            f"success={self.stats['success']} | "
            f"error={self.stats['error']}"
        )

    def _send_async(self, event: dict):
        latency = send_event(event)
        if latency is not None:
            self.stats["success"]          += 1
            self.stats["total_latency_ms"] += latency
        else:
            self.stats["error"] += 1

    def _log_stats(self, elapsed: float, rate: float):
        s       = self.stats["success"]
        avg_lat = (
            self.stats["total_latency_ms"] / s if s > 0 else 0
        )
        logger.info(
            f"t={elapsed:5.0f}s | "
            f"rate={rate:6.1f} rps | "
            f"sent={self.stats['sent']:6d} | "
            f"ok={s:6d} | "
            f"err={self.stats['error']:4d} | "
            f"avg_lat={avg_lat:6.1f}ms"
        )


# ─── ENTRY POINT ─────────────────────────────────────────
if __name__ == "__main__":
    pattern  = sys.argv[1] if len(sys.argv) > 1 else "normal"
    duration = int(sys.argv[2]) if len(sys.argv) > 2 else 300

    sim = RetailSimulator(pattern)
    sim.run(duration)