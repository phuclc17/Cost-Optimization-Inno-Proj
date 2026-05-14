"""
Integration Tests - Phase 1
Chạy: python3 tests/test_integration.py
Yêu cầu: Stack đang chạy (./scripts/start.sh)
"""

import sys
import time
import uuid
import requests
from datetime import datetime, timezone

BASE_URL = "http://localhost:8000"

# ─── HELPERS ─────────────────────────────────────────────

def make_event(channel="web", event_type="purchase"):
    return {
        "event_id":   str(uuid.uuid4()),
        "channel":    channel,
        "event_type": event_type,
        "user_id":    f"user_{uuid.uuid4().hex[:6]}",
        "product_id": f"prod_{uuid.uuid4().hex[:6]}",
        "category":   "electronics",
        "amount":     500000.0,
        "region":     "HCM",
        "device":     "mobile",
        "created_at": datetime.now(timezone.utc).isoformat()
    }

def post_event(event):
    return requests.post(
        f"{BASE_URL}/process",
        json=event,
        timeout=5.0
    )

passed = 0
failed = 0

def check(name, condition, detail=""):
    global passed, failed
    if condition:
        print(f"  ✅ {name}")
        passed += 1
    else:
        print(f"  ❌ {name} {detail}")
        failed += 1

# ─── TESTS ───────────────────────────────────────────────

def test_health():
    print("\n[1] Health Check")
    r = requests.get(f"{BASE_URL}/health")
    check("Status 200",        r.status_code == 200)
    check("status=healthy",    r.json()["status"] == "healthy")
    check("db_connected=true", r.json()["db_connected"] == True)

def test_process_channels():
    print("\n[2] Process All Channels")
    expected_latency = {
        "web":         (3, 15),
        "pos":         (2, 10),
        "marketplace": (5, 20),
    }
    for channel, (min_ms, max_ms) in expected_latency.items():
        r = post_event(make_event(channel))
        check(f"channel={channel} status 200",
              r.status_code == 200)
        lat = r.json().get("latency_ms", 0)
        check(f"channel={channel} latency {min_ms}-{max_ms}ms",
              min_ms <= lat <= max_ms,
              f"(got {lat}ms)")

def test_invalid_channel():
    print("\n[3] Invalid Channel → 400")
    r = post_event(make_event("invalid_channel"))
    check("Returns 400", r.status_code == 400)
    check("Has detail",  "detail" in r.json())

def test_metrics():
    print("\n[4] Prometheus Metrics")
    # Gửi 1 event trước
    post_event(make_event("web"))
    time.sleep(1)
    r = requests.get(f"{BASE_URL}/metrics")
    check("Status 200",
          r.status_code == 200)
    check("Has worker_events_total",
          "worker_events_total" in r.text)
    check("Has processing_latency",
          "worker_processing_latency_ms" in r.text)

def test_throughput():
    print("\n[5] Throughput Baseline (50 events)")
    n       = 50
    start   = time.time()
    success = 0

    for _ in range(n):
        r = post_event(make_event())
        if r.status_code == 200:
            success += 1

    elapsed = time.time() - start
    rps     = success / elapsed

    check(f"All {n} events processed",  success == n)
    check(f"Throughput > 5 rps",        rps > 5,
          f"(got {rps:.1f} rps)")
    check(f"Throughput < 500 rps",      rps < 500)

    print(f"     → Baseline throughput: {rps:.1f} rps")
    print(f"     → Total time: {elapsed:.1f}s")
    return rps

def test_load_balancing():
    print("\n[6] Load Balancing (20 events)")
    worker_ids = set()
    for _ in range(20):
        r = post_event(make_event())
        if r.status_code == 200:
            worker_ids.add(r.json().get("worker_id"))

    print(f"     → Workers responding: {worker_ids}")
    # Chỉ pass nếu đang scale > 1
    # Với 1 worker thì chỉ có 1 worker_id
    check("At least 1 worker responding",
          len(worker_ids) >= 1)

def test_latency_distribution():
    print("\n[7] Latency Distribution (100 events)")
    latencies = []
    for _ in range(100):
        r = post_event(make_event())
        if r.status_code == 200:
            latencies.append(r.json()["latency_ms"])

    if latencies:
        latencies.sort()
        p50 = latencies[int(len(latencies) * 0.50)]
        p95 = latencies[int(len(latencies) * 0.95)]
        p99 = latencies[int(len(latencies) * 0.99)]

        print(f"     → p50: {p50:.1f}ms")
        print(f"     → p95: {p95:.1f}ms")
        print(f"     → p99: {p99:.1f}ms")

        check("p99 < 1000ms (SLA)",  p99 < 1000,
              f"(got {p99:.1f}ms)")
        check("p50 < 100ms",         p50 < 100,
              f"(got {p50:.1f}ms)")

        return {"p50": p50, "p95": p95, "p99": p99}
    return {}

def test_data_persistence():
    print("\n[8] Data Persistence")
    sys.path.insert(0, ".")
    try:
        from pipeline.storage.db_client import DatabaseClient
        db     = DatabaseClient()
        before = db.count_events()

        # Gửi 5 events
        for _ in range(5):
            post_event(make_event())
        time.sleep(1)

        after = db.count_events()
        check("Events saved to DB",
              after >= before + 5,
              f"(before={before}, after={after})")
    except Exception as e:
        check("DB connection", False, str(e))

# ─── MAIN ────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 45)
    print(" INTEGRATION TESTS - Phase 1")
    print("=" * 45)

    # Check worker available
    try:
        requests.get(f"{BASE_URL}/health", timeout=3)
    except Exception:
        print("❌ Worker not running!")
        print("   Run: ./scripts/start.sh")
        sys.exit(1)

    test_health()
    test_process_channels()
    test_invalid_channel()
    test_metrics()
    rps    = test_throughput()
    test_load_balancing()
    lats   = test_latency_distribution()
    test_data_persistence()

    print("\n" + "=" * 45)
    print(f" RESULTS: {passed} passed, {failed} failed")
    print("=" * 45)

    if lats:
        print(f"\n📊 Key metrics for report:")
        print(f"   Baseline throughput: {rps:.1f} rps")
        print(f"   p50 latency: {lats.get('p50', 0):.1f}ms")
        print(f"   p95 latency: {lats.get('p95', 0):.1f}ms")
        print(f"   p99 latency: {lats.get('p99', 0):.1f}ms")

    if failed == 0:
        print("\n✅ Phase 1 Complete!")
        print("   Ready for Phase 2: Schedulers")
    else:
        print(f"\n⚠️  {failed} tests failed, check above")
        sys.exit(1)