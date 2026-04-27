from prometheus_client import Counter, Histogram, Gauge

# Đếm tổng events theo channel/type/status
events_total = Counter(
    "worker_events_total",
    "Total events processed",
    ["channel", "event_type", "status"]
)

# Đo phân phối latency (ms)
processing_latency = Histogram(
    "worker_processing_latency_ms",
    "Event processing latency in milliseconds",
    ["channel"],
    buckets=[5, 10, 25, 50, 100, 250,
             500, 1000, 2500, 5000]
)

# Số connections đang active
active_connections = Gauge(
    "worker_active_connections",
    "Current active connections"
)

# Thông tin worker (để biết có bao nhiêu workers đang chạy)
worker_info = Gauge(
    "worker_info",
    "Worker metadata",
    ["worker_id"]
)