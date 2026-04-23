import os
import logging
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class DatabaseClient:
    """
    PostgreSQL client dùng chung cho toàn project.
    Tự đọc config từ environment variables (.env).
    """

    def __init__(self):
        self.config = {
            "host":     os.getenv("POSTGRES_HOST", "localhost"),
            "port":     int(os.getenv("POSTGRES_PORT", "5432")),
            "dbname":   os.getenv("POSTGRES_DB", "retail_db"),
            "user":     os.getenv("POSTGRES_USER", "admin"),
            "password": os.getenv("POSTGRES_PASSWORD", "secret123"),
        }

    @contextmanager
    def get_conn(self):
        """
        Mở kết nối DB, tự động commit/rollback/close.
        Dùng: with db.get_conn() as conn:
        """
        conn = psycopg2.connect(**self.config)
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"DB error: {e}")
            raise
        finally:
            conn.close()

    # ─── WRITE METHODS ───────────────────────────────

    def insert_event(self, event: Dict) -> bool:
        """Lưu 1 retail event vào DB."""
        sql = """
            INSERT INTO retail_events
            (event_id, channel, event_type, user_id,
             product_id, category, amount, region,
             device, created_at, processed_at,
             worker_id, latency_ms)
            VALUES
            (%(event_id)s, %(channel)s, %(event_type)s,
             %(user_id)s, %(product_id)s, %(category)s,
             %(amount)s, %(region)s, %(device)s,
             %(created_at)s, %(processed_at)s,
             %(worker_id)s, %(latency_ms)s)
        """
        with self.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, event)
        return True

    def insert_events_batch(self, events: List[Dict]) -> int:
        """Bulk insert nhiều events — nhanh hơn insert từng cái."""
        if not events:
            return 0
        sql = """
            INSERT INTO retail_events
            (event_id, channel, event_type, user_id,
             product_id, category, amount, region,
             device, created_at, processed_at,
             worker_id, latency_ms)
            VALUES
            (%(event_id)s, %(channel)s, %(event_type)s,
             %(user_id)s, %(product_id)s, %(category)s,
             %(amount)s, %(region)s, %(device)s,
             %(created_at)s, %(processed_at)s,
             %(worker_id)s, %(latency_ms)s)
        """
        with self.get_conn() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, sql, events)
        return len(events)

    def log_scaling_decision(self, decision: Dict) -> bool:
        """Ghi lại quyết định scale của scheduler."""
        sql = """
            INSERT INTO scaling_decisions
            (strategy, experiment_id, action,
             workers_before, workers_after,
             trigger_reason, cpu_at_decision, cost_per_hour)
            VALUES
            (%(strategy)s, %(experiment_id)s, %(action)s,
             %(workers_before)s, %(workers_after)s,
             %(trigger_reason)s, %(cpu_at_decision)s,
             %(cost_per_hour)s)
        """
        with self.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, decision)
        return True

    def save_benchmark_result(self, result: Dict) -> bool:
        """Lưu kết quả tổng hợp của 1 experiment."""
        sql = """
            INSERT INTO benchmark_results
            (experiment_id, strategy, traffic_pattern,
             duration_seconds, total_events,
             avg_throughput_rps, p50_latency_ms,
             p95_latency_ms, p99_latency_ms,
             error_rate_pct, avg_workers, avg_cpu_pct,
             total_cost_usd, sla_compliance_pct, finops_score)
            VALUES
            (%(experiment_id)s, %(strategy)s,
             %(traffic_pattern)s, %(duration_seconds)s,
             %(total_events)s, %(avg_throughput_rps)s,
             %(p50_latency_ms)s, %(p95_latency_ms)s,
             %(p99_latency_ms)s, %(error_rate_pct)s,
             %(avg_workers)s, %(avg_cpu_pct)s,
             %(total_cost_usd)s, %(sla_compliance_pct)s,
             %(finops_score)s)
            ON CONFLICT (experiment_id)
            DO UPDATE SET finops_score = EXCLUDED.finops_score
        """
        with self.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, result)
        return True

    # ─── READ METHODS ────────────────────────────────

    def get_latency_stats(self, worker_id_pattern: str) -> Dict:
        """Tính p50/p95/p99 latency cho 1 experiment."""
        sql = """
            SELECT
                PERCENTILE_CONT(0.50) WITHIN GROUP
                    (ORDER BY latency_ms) AS p50,
                PERCENTILE_CONT(0.95) WITHIN GROUP
                    (ORDER BY latency_ms) AS p95,
                PERCENTILE_CONT(0.99) WITHIN GROUP
                    (ORDER BY latency_ms) AS p99,
                AVG(latency_ms)             AS avg_ms,
                MAX(latency_ms)             AS max_ms,
                COUNT(*)                    AS total_events
            FROM retail_events
            WHERE worker_id LIKE %s
        """
        with self.get_conn() as conn:
            with conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cur:
                cur.execute(sql, (f"%{worker_id_pattern}%",))
                row = cur.fetchone()
                return dict(row) if row else {}

    def count_events(self) -> int:
        """Đếm tổng số events trong DB."""
        with self.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM retail_events")
                return cur.fetchone()[0]

    def get_all_benchmark_results(self) -> List[Dict]:
        """Lấy tất cả kết quả để vẽ biểu đồ so sánh."""
        sql = """
            SELECT * FROM benchmark_results
            ORDER BY strategy, traffic_pattern
        """
        with self.get_conn() as conn:
            with conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cur:
                cur.execute(sql)
                return [dict(r) for r in cur.fetchall()]

    # ─── HEALTH CHECK ────────────────────────────────

    def health_check(self) -> bool:
        """Kiểm tra có kết nối được DB không."""
        try:
            with self.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False


# Singleton — import và dùng luôn
db = DatabaseClient()