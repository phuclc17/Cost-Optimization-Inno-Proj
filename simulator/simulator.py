"""
Retail Event Simulator
Ghi lại cả successful và failed (timeout) requests
để đo latency chính xác
"""
import os, sys, time, uuid, random, logging, threading
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
TIMEOUT_MS = 5000  # 5 giây timeout = SLA violation

CHANNELS = {
    "web": {
        "weight":       0.50,
        "event_types":  ["page_view","add_to_cart",
                         "purchase","cart_abandon"],
        "amount_range": (50_000, 2_000_000),
        "devices":      ["mobile","desktop","tablet"],
        "regions":      ["HCM","HN","DN","CT","BD"],
    },
    "pos": {
        "weight":       0.30,
        "event_types":  ["transaction","return","exchange"],
        "amount_range": (30_000, 500_000),
        "devices":      ["pos_terminal"],
        "regions":      ["HCM","HN","DN","CT"],
    },
    "marketplace": {
        "weight":       0.20,
        "event_types":  ["order_placed","order_shipped",
                         "order_delivered","order_cancelled"],
        "amount_range": (20_000, 1_500_000),
        "devices":      ["mobile","desktop"],
        "regions":      ["HCM","HN","DN","CT","HP"],
    },
}
CATEGORIES = ["electronics","fashion","food_beverage",
              "home_living","beauty","sports","books"]


def pick_channel() -> str:
    r = random.random()
    cum = 0.0
    for name, info in CHANNELS.items():
        cum += info["weight"]
        if r <= cum:
            return name
    return "web"


def generate_event(channel: str) -> dict:
    ch = CHANNELS[channel]
    return {
        "event_id":   str(uuid.uuid4()),
        "channel":    channel,
        "event_type": random.choice(ch["event_types"]),
        "user_id":    f"user_{random.randint(1,100_000)}",
        "product_id": f"prod_{random.randint(1,10_000)}",
        "category":   random.choice(CATEGORIES),
        "amount":     round(random.uniform(*ch["amount_range"]),-3),
        "region":     random.choice(ch["regions"]),
        "device":     random.choice(ch["devices"]),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def send_event(event: dict) -> dict:
    """
    Gửi event, trả về result với latency thật.
    Nếu timeout/error → latency = TIMEOUT_MS (SLA violation)
    """
    start = time.perf_counter()
    try:
        resp = requests.post(
            f"{WORKER_URL}/process",
            json=event,
            timeout=TIMEOUT_MS / 1000
        )
        latency_ms = (time.perf_counter() - start) * 1000
        if resp.status_code == 200:
            return {"success": True,  "latency_ms": latency_ms}
        return {"success": False, "latency_ms": latency_ms}
    except requests.exceptions.Timeout:
        latency_ms = (time.perf_counter() - start) * 1000
        return {"success": False, "latency_ms": latency_ms,
                "error": "timeout"}
    except Exception as e:
        latency_ms = (time.perf_counter() - start) * 1000
        return {"success": False, "latency_ms": latency_ms,
                "error": str(e)}


class RetailSimulator:
    def __init__(self, pattern_name: str = "normal"):
        self.pattern = get_pattern(pattern_name)
        self.running = False
        self.stats = {
            "sent": 0, "success": 0, "error": 0,
            "timeout": 0,
            "total_latency": 0.0,
            "latencies": [],  # để tính percentiles
        }

    def run(self, duration_seconds: int = 1800):
        self.running  = True
        start_time    = time.time()
        last_log      = start_time

        logger.info(
            f"Simulator started | pattern={self.pattern.name} "
            f"| duration={duration_seconds}s"
        )

        while self.running:
            elapsed = time.time() - start_time
            if elapsed >= duration_seconds:
                break

            rate     = self.pattern.get_rate_at(elapsed)
            interval = 1.0 / max(rate, 0.1)

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

            now = time.time()
            if now - last_log >= 10:
                self._log_stats(elapsed, rate)
                last_log = now

        self.running = False
        self._final_stats()

    def _send_async(self, event):
        result = send_event(event)
        lat    = result["latency_ms"]

        self.stats["total_latency"] += lat
        self.stats["latencies"].append(lat)

        if result["success"]:
            self.stats["success"] += 1
        else:
            self.stats["error"] += 1
            if result.get("error") == "timeout":
                self.stats["timeout"] += 1

    def _log_stats(self, elapsed, rate):
        s   = self.stats
        avg = s["total_latency"] / s["sent"] if s["sent"] else 0

        # p99 từ latencies đã thu thập
        lats = sorted(s["latencies"])
        p99  = lats[int(len(lats)*0.99)] if lats else 0

        logger.info(
            f"t={elapsed:5.0f}s | rate={rate:6.1f}rps | "
            f"sent={s['sent']:6d} | ok={s['success']:6d} | "
            f"err={s['error']:4d} | timeout={s['timeout']:4d} | "
            f"avg={avg:6.1f}ms | p99={p99:6.1f}ms"
        )

    def _final_stats(self):
        s    = self.stats
        lats = sorted(s["latencies"])
        p50  = lats[int(len(lats)*0.50)] if lats else 0
        p95  = lats[int(len(lats)*0.95)] if lats else 0
        p99  = lats[int(len(lats)*0.99)] if lats else 0
        sla  = sum(1 for l in lats if l <= 1000) / len(lats) * 100 if lats else 0

        logger.info("=" * 55)
        logger.info(f"FINAL STATS | sent={s['sent']}")
        logger.info(f"  Success:   {s['success']} ({s['success']/s['sent']*100:.1f}%)")
        logger.info(f"  Error:     {s['error']}")
        logger.info(f"  Timeout:   {s['timeout']}")
        logger.info(f"  p50:       {p50:.1f}ms")
        logger.info(f"  p95:       {p95:.1f}ms")
        logger.info(f"  p99:       {p99:.1f}ms")
        logger.info(f"  SLA (≤1s): {sla:.1f}%")
        logger.info("=" * 55)

        return {
            "p50_ms": p50, "p95_ms": p95, "p99_ms": p99,
            "sla_pct": sla,
            "sent": s["sent"], "success": s["success"],
            "timeout": s["timeout"],
        }


if __name__ == "__main__":
    pattern  = sys.argv[1] if len(sys.argv) > 1 else "normal"
    duration = int(sys.argv[2]) if len(sys.argv) > 2 else 300
    sim = RetailSimulator(pattern)
    sim.run(duration)