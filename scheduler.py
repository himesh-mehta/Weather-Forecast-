"""
Runner for the tiered ingestion schedule. Windows Task Scheduler starts it (see
setup_scheduler.ps1); you can also run it yourself:

    python scheduler.py                 # full morning run (all providers + daily archive snapshot)
    python scheduler.py --tier A        # live readings only      (task: every 15 min)
    python scheduler.py --tier B        # 2-hourly providers      (task: every 2 h)
    python scheduler.py --tier C        # strict-quota providers  (task: 18:00; also part of 06:00)
    python scheduler.py --plan --tier B # show what a tier would call + today's quota use, fetch nothing
    python scheduler.py --check         # check network + keys only
    python scheduler.py --force         # redo today's full run even if it already succeeded

Exit codes: 0 ok/partial/skipped, 1 failed, 2 no network, 3 another run held the lock too long.
"""
import argparse
import asyncio
import contextlib
import io
import json
import logging
import os
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOCK_PATH = BASE_DIR / ".cache" / "scheduler.lock"
LAST_RUN_PATH = LOG_DIR / "last_run.json"
LOG_KEEP_DAYS = 30
STALE_LOCK_S = 30 * 60
NETWORK_WAIT_S = {"full": 300, "C": 120, "D": 120, "B": 60, "A": 20}  # how long to wait for Wi-Fi
LOCK_WAIT_S = {"full": 600, "C": 300, "D": 300, "B": 180, "A": 45}    # how long to queue behind another run

os.chdir(BASE_DIR)
sys.path.insert(0, str(BASE_DIR))

import history  # noqa: E402
import ingest   # noqa: E402

log = logging.getLogger("scheduler")


def setup_logging(tier: str) -> Path:
    LOG_DIR.mkdir(exist_ok=True)
    # live (A) runs every 15 min -> separate file so the main log stays readable
    name = "live" if tier == "A" else "ingestion"
    path = LOG_DIR / f"{name}_{datetime.now(ingest.LOCAL_TZ):%Y-%m-%d}.log"
    fmt = logging.Formatter("%(asctime)s  %(levelname)-7s %(message)s", "%H:%M:%S")
    handlers = [logging.FileHandler(path, encoding="utf-8")]
    if sys.stdout is not None:  # pythonw (background task) has no console
        handlers.append(logging.StreamHandler(sys.stdout))
    for h in handlers:
        h.setFormatter(fmt)
        log.addHandler(h)
    log.setLevel(logging.INFO)
    cutoff = time.time() - LOG_KEEP_DAYS * 86400
    for old in LOG_DIR.glob("*.log"):
        if old.stat().st_mtime < cutoff:
            old.unlink(missing_ok=True)
    return path


@contextlib.contextmanager
def run_lock(wait_s: int):
    """Only one run at a time. A run that finds the lock taken waits its turn (up to wait_s)."""
    LOCK_PATH.parent.mkdir(exist_ok=True)
    deadline, announced = time.time() + wait_s, False
    while True:
        if LOCK_PATH.exists() and time.time() - LOCK_PATH.stat().st_mtime > STALE_LOCK_S:
            LOCK_PATH.unlink(missing_ok=True)  # left behind by a crash
        try:
            fd = os.open(LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode())
            os.close(fd)
            break
        except FileExistsError:
            if time.time() >= deadline:
                yield False
                return
            if not announced:
                log.info("Another run is in progress - waiting for it to finish...")
                announced = True
            time.sleep(3)
    try:
        yield True
    finally:
        LOCK_PATH.unlink(missing_ok=True)


def wait_for_network(timeout_s: int) -> bool:
    deadline = time.time() + timeout_s
    while True:
        try:
            socket.create_connection(("api.open-meteo.com", 443), timeout=5).close()
            return True
        except OSError:
            if time.time() >= deadline:
                return False
            log.info("No network yet - retrying in 10 s")
            time.sleep(10)


def key_report(names) -> tuple[list[str], list[str]]:
    active, missing = [], []
    for name, env_var, _ in ingest.TIER2:
        if names is not None and name not in names:
            continue
        envs = env_var if isinstance(env_var, tuple) else (env_var,)
        (active if all(os.getenv(v, "").strip() for v in envs) else missing).append(name)
    return active, missing


def print_plan(tier: str, names) -> None:
    jobs = names or [n for n, _ in ingest.TIER1] + [n for n, _, _ in ingest.TIER2]
    _, missing = key_report(None)
    n_cities = len(ingest.LOCATIONS)
    log.info(f"Plan for tier {tier}: {len(jobs)} providers x up to {n_cities} cities")
    for j in jobs:
        lim, cost = ingest.QUOTAS.get(j, (None, 1))
        used = history.usage_today(j)
        if j in missing:
            note = "no API key - will be skipped"
        elif lim:
            room = int(lim * ingest.QUOTA_SAFETY) - used
            note = f"quota {used}/{lim} used today, room for {max(room, 0) // cost} more city calls"
        else:
            note = "keyless / no daily quota"
        log.info(f"  {j:22} {note}")


class _LogStream(io.TextIOBase):
    """Routes ingest.py's print() output into the log."""
    def write(self, s):
        for line in s.rstrip().splitlines():
            if line.strip():
                (log.warning if "[WARN]" in line else log.info)(line.replace("[WARN] ", ""))
        return len(s)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tier", default="full", choices=["full", "A", "B", "C", "D"])
    ap.add_argument("--force", action="store_true", help="redo today's full run even if it succeeded")
    ap.add_argument("--check", action="store_true", help="only check network and API keys")
    ap.add_argument("--plan", action="store_true", help="show what would be fetched + quota use; fetch nothing")
    ap.add_argument("--cities", nargs="+", help="limit to these cities")
    ap.add_argument("--trigger", default="manual", choices=["manual", "scheduled"])
    a = ap.parse_args()

    tier = a.tier
    names = ingest.providers_for_tier(tier)
    log_path = setup_logging(tier)
    now_local = datetime.now(ingest.LOCAL_TZ)
    today = now_local.date().isoformat()
    log.info(f"=== Tier {tier} ({a.trigger}) - {now_local:%a %d %b %Y %H:%M} IST ===")

    if a.plan:
        print_plan(tier, names)
        return 0
    if ingest.fetching_paused():
        log.info("Data fetching is paused (FETCH_PAUSED=1 in .env) - nothing fetched.")
        return 0

    active, missing = key_report(names)
    if tier != "A":
        log.info(f"Keyed providers active: {len(active)} | no key: {', '.join(missing) or 'none'}")

    with run_lock(LOCK_WAIT_S[tier]) as got_lock:
        if not got_lock:
            log.warning("Another run held the lock too long - skipping this slot.")
            return 3
        if tier == "full" and not (a.force or a.check or a.cities) and history.has_successful_run(today):
            log.info(f"Today's full snapshot ({today}) already exists - skipping. Use --force to rerun.")
            history.log_run({"started_at": datetime.now(timezone.utc).isoformat(), "snapshot_date": today,
                             "trigger": a.trigger, "status": "skipped", "tier": tier})
            return 0

        if not wait_for_network(5 if a.check else NETWORK_WAIT_S[tier]):
            log.error("No internet connection - giving up for this slot.")
            if not a.check:
                history.log_run({"started_at": datetime.now(timezone.utc).isoformat(), "snapshot_date": today,
                                 "trigger": a.trigger, "status": "failed", "errors": ["no network"], "tier": tier})
            return 2
        if a.check:
            log.info("Network OK. Check only - nothing fetched.")
            return 0

        started, t0 = datetime.now(timezone.utc), time.time()
        try:
            with contextlib.redirect_stdout(_LogStream()):
                result = asyncio.run(ingest.run_daily_ingestion(a.cities, providers=names))
        except Exception as exc:  # noqa: BLE001
            log.exception(f"Ingestion crashed: {exc}")
            history.log_run({"started_at": started.isoformat(), "finished_at": datetime.now(timezone.utc).isoformat(),
                             "snapshot_date": today, "trigger": a.trigger, "status": "failed", "tier": tier,
                             "errors": [ingest.redact(str(exc))[:300]], "duration_s": round(time.time() - t0, 1)})
            return 1

        refreshed = set(result["cities"])
        cache_rows = [r for r in ingest.load_cache()["records"] if r.get("city") in refreshed]
        fresh = [r for r in cache_rows if r.get("fetched_at", "") >= started.isoformat()]

        # Live readings go to the observations table on every run that fetched them.
        obs = history.archive_observations(fresh)
        # Tier A / full also refresh the "right now" layer: airport METARs, air quality, 2-h rain verdict.
        if tier in ("A", "full"):
            try:
                import live
                live_res = asyncio.run(live.refresh_cities(a.cities))
                obs += history.archive_metars(live_res)
                problems = [f"{c}: {k}" for c, v in live_res.items()
                            for k in ("air_quality_error", "nowcast_error", "metar_error") if v.get(k)]
                log.info(f"Live layer: {len(live_res)} cities, airports matched: "
                         f"{sum(1 for v in live_res.values() if v.get('metar'))}"
                         + (f" | issues: {'; '.join(problems)}" if problems else ""))
            except Exception as exc:  # noqa: BLE001 - live layer must never fail the forecast run
                log.warning(f"Live layer failed: {ingest.redact(str(exc))[:200]}")
        # Only the full 06:00 run writes the daily forecast snapshot (one clean snapshot per day).
        archived = history.archive_snapshot(fresh, result["fetched_at"], today) if tier == "full" else 0

        st = result["status"]
        errors = [f"{p} / {c}: {s[7:]}" for c, s_ in st.items() for p, s in s_.items() if s.startswith("error")]
        guarded = sorted({p for s_ in st.values() for p, s in s_.items() if s.startswith("skipped: daily quota")})
        ok = sum(s.startswith("ok") for s_ in st.values() for s in s_.values())
        status = "ok" if not errors else ("partial" if result["new_records"] else "failed")
        run = {"started_at": started.isoformat(), "finished_at": datetime.now(timezone.utc).isoformat(),
               "snapshot_date": today, "trigger": a.trigger, "status": status, "tier": tier,
               "cities": len(refreshed), "records": result["new_records"], "archived": archived,
               "providers_ok": ok, "providers_failed": len(errors), "errors": errors + [f"quota guard: {g}" for g in guarded],
               "duration_s": round(time.time() - t0, 1)}
        history.log_run(run)
        if tier != "A":
            LAST_RUN_PATH.write_text(json.dumps(run, indent=2), encoding="utf-8")

        log.info(f"{run['records']} records, {run['cities']} cities, {run['duration_s']} s | "
                 f"snapshot rows: {archived} | live readings: {obs} | calls ok: {ok}, failed: {len(errors)}")
        for e in errors:
            log.warning(f"  {e}")
        if guarded:
            log.warning(f"  Quota guard held back: {', '.join(guarded)}")
        if tier == "full":
            s = history.summary()
            log.info(f"Archive: {s['snapshots']} daily snapshots, {s['rows']} rows, {s['size_mb']} MB")
        log.info(f"Status: {status.upper()}  (log: {log_path.name})")
        return 1 if status == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
