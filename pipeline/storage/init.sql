CREATE TABLE IF NOT EXISTS retail_events (
id           BIGSERIAL PRIMARY KEY,
event_id     VARCHAR(50)   NOT NULL,
channel      VARCHAR(20)   NOT NULL,
event_type   VARCHAR(30)   NOT NULL,
user_id      VARCHAR(50),
product_id   VARCHAR(50),
category     VARCHAR(50),
amount       DECIMAL(12,2) DEFAULT 0,
region       VARCHAR(20),
device       VARCHAR(20),
created_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
processed_at TIMESTAMPTZ,
worker_id    VARCHAR(50),
latency_ms   INTEGER);
CREATE TABLE IF NOT EXISTS scaling_decisions (
id              BIGSERIAL PRIMARY KEY,
strategy        VARCHAR(30)  NOT NULL,
experiment_id   VARCHAR(50),
action          VARCHAR(20)  NOT NULL,
workers_before  INTEGER,
workers_after   INTEGER,
trigger_reason  TEXT,
cpu_at_decision FLOAT,
cost_per_hour   FLOAT,
decided_at      TIMESTAMPTZ  DEFAULT NOW());
CREATE TABLE IF NOT EXISTS cost_tracking (
id                  BIGSERIAL PRIMARY KEY,
experiment_id       VARCHAR(50),
strategy            VARCHAR(30) NOT NULL,
period_start        TIMESTAMPTZ NOT NULL,
period_end          TIMESTAMPTZ NOT NULL,
active_workers      FLOAT,
cost_usd            FLOAT,
events_processed    BIGINT,
sla_compliance_pct  FLOAT,
created_at          TIMESTAMPTZ DEFAULT NOW());
CREATE TABLE IF NOT EXISTS benchmark_results (
id                  BIGSERIAL PRIMARY KEY,
experiment_id       VARCHAR(50) UNIQUE,
strategy            VARCHAR(30) NOT NULL,
traffic_pattern     VARCHAR(20) NOT NULL,
duration_seconds    INTEGER,
total_events        BIGINT,
avg_throughput_rps  FLOAT,
p50_latency_ms      FLOAT,
p95_latency_ms      FLOAT,
p99_latency_ms      FLOAT,
error_rate_pct      FLOAT,
avg_workers         FLOAT,
avg_cpu_pct         FLOAT,
total_cost_usd      FLOAT,
sla_compliance_pct  FLOAT,
finops_score        FLOAT,
run_at              TIMESTAMPTZ DEFAULT NOW());
CREATE INDEX IF NOT EXISTS idx_events_channel
ON retail_events(channel);
CREATE INDEX IF NOT EXISTS idx_events_created
ON retail_events(created_at);
CREATE INDEX IF NOT EXISTS idx_events_worker
ON retail_events(worker_id);
CREATE INDEX IF NOT EXISTS idx_decisions_strategy
ON scaling_decisions(strategy, decided_at);
CREATE INDEX IF NOT EXISTS idx_benchmark_strategy
ON benchmark_results(strategy, traffic_pattern);
