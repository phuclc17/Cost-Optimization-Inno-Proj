"""
Run Experiments — 12 scenarios x 3 repeats
Dung:
  python3 benchmark/run_experiments.py --dry-run
  python3 benchmark/run_experiments.py --exp E01 --repeats 1
  python3 benchmark/run_experiments.py
"""
import os, sys, time, uuid, logging, argparse, subprocess
from datetime import datetime, timezone

sys.path.insert(0, ".")
from pipeline.storage.db_client import DatabaseClient
from pipeline.analytical_model  import compute_finops_index

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ─── CONFIG ──────────────────────────────────────────────
EXPERIMENTS = [
    {"id":"E01","strategy":"static_conservative","pattern":"normal", "workers":1},
    {"id":"E02","strategy":"static_conservative","pattern":"peak",   "workers":1},
    {"id":"E03","strategy":"static_conservative","pattern":"mixed",  "workers":1},
    {"id":"E04","strategy":"static_aggressive",  "pattern":"normal", "workers":3},
    {"id":"E05","strategy":"static_aggressive",  "pattern":"peak",   "workers":3},
    {"id":"E06","strategy":"static_aggressive",  "pattern":"mixed",  "workers":3},
    {"id":"E07","strategy":"reactive",           "pattern":"normal", "workers":None},
    {"id":"E08","strategy":"reactive",           "pattern":"peak",   "workers":None},
    {"id":"E09","strategy":"reactive",           "pattern":"mixed",  "workers":None},
    {"id":"E10","strategy":"cost_aware",         "pattern":"normal", "workers":None},
    {"id":"E11","strategy":"cost_aware",         "pattern":"peak",   "workers":None},
    {"id":"E12","strategy":"cost_aware",         "pattern":"mixed",  "workers":None},
]

DURATION   = 1800   # 30 phút
WARMUP     = 60     # 1 phút warmup
REPEATS    = 3
COOLDOWN   = 30     # 30s giữa runs
WORKER_URL = "http://localhost:8000"
DB_ENV = {
    "POSTGRES_HOST":     "localhost",
    "POSTGRES_USER":     "phuclc17",
    "POSTGRES_PASSWORD": "172005",
    "POSTGRES_DB":       "inno_db",
}

# ─── HELPERS ─────────────────────────────────────────────

def run_cmd(cmd, background=False, env_extra=None):
    env = {**os.environ, **(env_extra or DB_ENV)}
    if background:
        return subprocess.Popen(
            cmd, shell=True, env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    return subprocess.run(
        cmd, shell=True, env=env,
        capture_output=True, text=True
    )

def scale_workers(n):
    log.info(f"Scaling to {n} workers...")
    run_cmd(
        f"docker compose up -d --scale worker={n} --no-recreate"
    )
    time.sleep(8)

def get_worker_count():
    r = run_cmd(
        "docker compose ps --format '{{.Name}}' 2>/dev/null"
        " | grep worker | wc -l"
    )
    try:
        return max(int(r.stdout.strip()), 1)
    except Exception:
        return 1

def reset_db():
    log.info("Resetting DB...")
    db = DatabaseClient()
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            for t in ["retail_events",
                      "scaling_decisions",
                      "cost_tracking"]:
                cur.execute(
                    f"TRUNCATE {t} RESTART IDENTITY CASCADE"
                )
    log.info("DB reset done")

def collect_metrics():
    db = DatabaseClient()
    with db.get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                  COUNT(*)                                       AS total,
                  AVG(latency_ms)                                AS avg_ms,
                  PERCENTILE_CONT(0.50) WITHIN GROUP
                      (ORDER BY latency_ms)                      AS p50,
                  PERCENTILE_CONT(0.95) WITHIN GROUP
                      (ORDER BY latency_ms)                      AS p95,
                  PERCENTILE_CONT(0.99) WITHIN GROUP
                      (ORDER BY latency_ms)                      AS p99,
                  MAX(latency_ms)                                AS max_ms,
                  COUNT(*) FILTER (WHERE latency_ms > 1000)      AS violations,
                  EXTRACT(EPOCH FROM
                      (MAX(processed_at) - MIN(processed_at)))   AS duration_s
                FROM retail_events
            """)
            row = cur.fetchone()
            (total, avg, p50, p95, p99,
             max_ms, violations, duration_s) = row
            # Convert Decimal to float
            total      = float(total or 0)
            p50        = float(p50 or 0)
            p95        = float(p95 or 0)
            p99        = float(p99 or 0)
            max_ms     = float(max_ms or 0)
            violations = float(violations or 0)
            duration_s = float(duration_s or 1)

            cur.execute("""
                SELECT COUNT(*), AVG(workers_after)
                FROM scaling_decisions
            """)
            sc_row = cur.fetchone()
            scale_events = sc_row[0] or 0
            avg_workers  = float(sc_row[1] or get_worker_count())

    total      = total or 0
    duration_s = duration_s or 1
    thr        = total / duration_s
    sla_rate   = 1 - (violations / total) if total > 0 else 1.0
    cost       = avg_workers * 0.048 * (duration_s / 3600)
    waste      = max(0.0,
                     1 - (thr / (avg_workers * 200 + 1)))
    thr_eff    = min(1.0, thr / (avg_workers * 53.6 + 1))
    fi         = compute_finops_index(sla_rate, waste, thr_eff)

    return {
        "total_events":       int(total),
        "duration_seconds":   int(duration_s),
        "avg_throughput_rps": round(thr, 2),
        "p50_latency_ms":     round(p50 or 0, 2),
        "p95_latency_ms":     round(p95 or 0, 2),
        "p99_latency_ms":     round(p99 or 0, 2),
        "max_latency_ms":     round(max_ms or 0, 2),
        "error_rate_pct":     0.0,
        "avg_workers":        round(avg_workers, 2),
        "total_cost_usd":     round(cost, 4),
        "sla_compliance_pct": round(sla_rate * 100, 2),
        "finops_score":       round(fi, 4),
        "scale_events":       int(scale_events),
    }

# ─── SINGLE EXPERIMENT ───────────────────────────────────

def run_single(exp, repeat, dry_run=False, duration=DURATION):
    exp_id = f"{exp['id']}_r{repeat}_{uuid.uuid4().hex[:4]}"
    log.info("=" * 55)
    log.info(
        f"START {exp['id']} | strategy={exp['strategy']} "
        f"| pattern={exp['pattern']} | repeat={repeat}"
    )

    if dry_run:
        log.info("[DRY RUN] skipped")
        return {"experiment_id": exp_id,
                "strategy": exp["strategy"],
                "traffic_pattern": exp["pattern"],
                "dry_run": True}

    # 1. Reset DB
    reset_db()

    # 2. Scale workers
    scale_workers(exp["workers"] or 1)

    # 3. Warmup (không tính metrics)
    log.info(f"Warmup {WARMUP}s...")
    wup = run_cmd(
        f"WORKER_URL={WORKER_URL} "
        f"python3 simulator/simulator.py normal {WARMUP}",
        background=True
    )
    wup.wait()
    reset_db()

    # 4. Start scheduler (dynamic only)
    sched_proc = None
    if exp["strategy"] in ("reactive", "cost_aware"):
        cls_map = {
            "reactive":
                "from scheduler.reactive import ReactiveScheduler; "
                f"ReactiveScheduler('{exp_id}').run({duration + 30})",
            "cost_aware":
                "from scheduler.cost_aware import CostAwareScheduler; "
                f"CostAwareScheduler('{exp_id}').run({duration + 30})",
        }
        sched_proc = run_cmd(
            f"python3 -c \"{cls_map[exp['strategy']]}\"",
            background=True,
            env_extra={**os.environ, **DB_ENV}
        )
        log.info(f"Scheduler started pid={sched_proc.pid}")
        time.sleep(3)

    # 5. Run simulator
    log.info(f"Simulator running {duration}s "
             f"pattern={exp['pattern']}...")
    # Run simulator and capture output
    sim_result = run_cmd(
        f"WORKER_URL={WORKER_URL} "
        f"python3 simulator/simulator.py "
        f"{exp['pattern']} {duration}"
    )
    log.info("Simulator done")

    # Parse p99 from simulator output
    sim_p99 = None
    for line in sim_result.stdout.split("\n"):
        if "p99:" in line:
            try:
                sim_p99 = float(line.split("p99:")[-1].strip().replace("ms",""))
            except Exception:
                pass
    if sim_p99:
        log.info(f"Simulator p99 from output: {sim_p99}ms")

    # 6. Stop scheduler
    if sched_proc:
        sched_proc.terminate()

    # 7. Collect + save
    time.sleep(3)
    m = collect_metrics()

    result = {
        "experiment_id":      exp_id,
        "strategy":           exp["strategy"],
        "traffic_pattern":    exp["pattern"],
        "duration_seconds":   m["duration_seconds"],
        "total_events":       m["total_events"],
        "avg_throughput_rps": m["avg_throughput_rps"],
        "p50_latency_ms":     m["p50_latency_ms"],
        "p95_latency_ms":     m["p95_latency_ms"],
        "p99_latency_ms":     m["p99_latency_ms"],
        "error_rate_pct":     m["error_rate_pct"],
        "avg_workers":        m["avg_workers"],
        "avg_cpu_pct":        0.0,
        "total_cost_usd":     m["total_cost_usd"],
        "sla_compliance_pct": m["sla_compliance_pct"],
        "finops_score":       m["finops_score"],
    }

    db = DatabaseClient()
    db.save_benchmark_result(result)

    log.info(
        f"RESULT | rps={m['avg_throughput_rps']} "
        f"p99={m['p99_latency_ms']}ms "
        f"SLA={m['sla_compliance_pct']}% "
        f"FI={m['finops_score']} "
        f"cost=${m['total_cost_usd']}"
    )
    return result

# ─── MAIN ────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--exp",      help="Run one experiment e.g. E01")
    parser.add_argument("--dry-run",  action="store_true")
    parser.add_argument("--repeats",  type=int, default=REPEATS)
    parser.add_argument("--duration", type=int, default=DURATION)
    args = parser.parse_args()

    exps = (
        [e for e in EXPERIMENTS if e["id"] == args.exp.upper()]
        if args.exp else EXPERIMENTS
    )
    if not exps:
        print(f"Unknown: {args.exp}")
        sys.exit(1)

    total = len(exps) * args.repeats
    est_h = total * (args.duration + WARMUP + COOLDOWN) / 3600
    log.info(
        f"Plan: {len(exps)} exp × {args.repeats} = "
        f"{total} runs | ~{est_h:.1f} hours"
    )

    results, done = [], 0

    for exp in exps:
        for r in range(1, args.repeats + 1):
            try:
                res = run_single(
                    exp, r,
                    dry_run=args.dry_run,
                    duration=args.duration
                )
                results.append(res)
                done += 1
                log.info(f"Progress: {done}/{total}")
            except Exception as e:
                log.error(f"FAILED {exp['id']} r{r}: {e}")

            if done < total:
                log.info(f"Cooldown {COOLDOWN}s...")
                time.sleep(COOLDOWN)

    # Print summary
    log.info("=" * 60)
    log.info("SUMMARY")
    log.info(f"{'ID':<8} {'Strategy':<22} {'Pattern':<8} "
             f"{'RPS':>6} {'p99':>7} {'SLA%':>6} {'FI':>6}")
    log.info("-" * 70)
    for r in results:
        if "dry_run" not in r:
            log.info(
                f"{r['experiment_id'][:8]:<8} "
                f"{r['strategy']:<22} "
                f"{r['traffic_pattern']:<8} "
                f"{r.get('avg_throughput_rps',0):>6.1f} "
                f"{r.get('p99_latency_ms',0):>6.1f}ms "
                f"{r.get('sla_compliance_pct',0):>5.1f}% "
                f"{r.get('finops_score',0):>6.4f}"
            )
    log.info(f"Done: {done}/{total}")

if __name__ == "__main__":
    main()
