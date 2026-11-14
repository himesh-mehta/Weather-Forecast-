"""
Permanent forecast archive (SQLite, built into Python - no extra installs).

latest_forecasts.json only ever holds the *current* 7-day window. This database keeps
every morning's snapshot, so you can see:
  * how the forecast for one day changed as that day got closer, and
  * how accurate each provider was (scored against NASA POWER, which publishes
    measured/reanalysis values ~3 days after the fact).
"""
import contextlib
import json
import sqlite3
import statistics
import zlib
from datetime import date
from pathlib import Path

import os as _os

# WEATHER_DB_PATH lets tests (and CI) point the archive somewhere else.
DB_PATH = Path(_os.getenv("WEATHER_DB_PATH") or Path(__file__).resolve().parent / "forecast_history.db")

FIELDS = [
    "temp_min_c", "temp_max_c", "feels_like_max_c", "dew_point_c", "temp_now_c",
    "precip_mm", "precip_prob_pct", "precip_hours", "precip_type",
    "wind_speed_kph", "wind_basis", "wind_gust_kph", "wind_dir_deg",
    "uv_index", "sunshine_hours", "solar_radiation_wm2",
    "cloud_cover_pct", "humidity_pct", "pressure_hpa", "visibility_km",
    "wave_height_m", "swell_height_m", "sea_surface_temp_c",
    "sunrise", "sunset", "moon_phase", "moon_illumination_pct",
]
TEXT_FIELDS = {"precip_type", "wind_basis", "sunrise", "sunset", "moon_phase"}
TRUTH_PROVIDER = "NASA POWER"  # reanalysis = what actually happened (approx., ~50 km grid)

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS daily_forecasts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_at   TEXT NOT NULL,      -- when the morning run happened (UTC ISO)
    snapshot_date TEXT NOT NULL,      -- local (IST) date of that run
    lead_days     INTEGER,            -- forecast_date - snapshot_date (0 = same-day forecast)
    provider      TEXT NOT NULL,
    model_source  TEXT,
    city          TEXT NOT NULL,
    latitude      REAL,
    longitude     REAL,
    forecast_date TEXT NOT NULL,
    {", ".join(f"{f} {'TEXT' if f in TEXT_FIELDS else 'REAL'}" for f in FIELDS)},
    coverage_hours INTEGER,
    flags         TEXT,               -- JSON list, e.g. ["trial"]
    fetched_at    TEXT,
    raw_payload   BLOB,               -- zlib-compressed JSON of the provider response
    UNIQUE (snapshot_at, provider, model_source, city, forecast_date)
);
CREATE INDEX IF NOT EXISTS ix_df_city_date ON daily_forecasts (city, forecast_date);
CREATE INDEX IF NOT EXISTS ix_df_snapshot ON daily_forecasts (snapshot_date);

CREATE TABLE IF NOT EXISTS ingestion_runs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at     TEXT NOT NULL,
    finished_at    TEXT,
    snapshot_date  TEXT,
    trigger        TEXT,              -- 'scheduled' | 'manual'
    status         TEXT,              -- 'ok' | 'partial' | 'failed' | 'skipped'
    cities         INTEGER,
    records        INTEGER,
    archived       INTEGER,
    providers_ok   INTEGER,
    providers_failed INTEGER,
    errors         TEXT,              -- JSON list of "provider / city: message"
    duration_s     REAL,
    tier           TEXT               -- 'full' | 'A' | 'B' | 'C' | 'D'
);

-- Calls made per provider per UTC day (free-tier quotas reset at 00:00 UTC = 05:30 IST)
CREATE TABLE IF NOT EXISTS api_usage (
    day      TEXT NOT NULL,
    provider TEXT NOT NULL,
    calls    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day, provider)
);

-- Live readings captured every 15 min (Weather Union stations, Open-Meteo current analysis).
-- This is the "what actually happened" record that forecasts can later be checked against.
CREATE TABLE IF NOT EXISTS observations (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at  TEXT NOT NULL,       -- UTC time of the fetch
    provider     TEXT NOT NULL,
    kind         TEXT NOT NULL,       -- 'station' (measured) | 'model' (analysis)
    city         TEXT NOT NULL,
    temp_c       REAL, humidity_pct REAL, wind_kph REAL, wind_dir_deg REAL,
    rain_today_mm REAL, cloud_cover_pct REAL, pressure_hpa REAL,
    UNIQUE (captured_at, provider, city)
);
CREATE INDEX IF NOT EXISTS ix_obs_city_time ON observations (city, captured_at);

-- "Report your sky" button presses (what people actually see outside)
CREATE TABLE IF NOT EXISTS sky_reports (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    reported_at TEXT NOT NULL,        -- UTC
    place       TEXT NOT NULL,        -- city name, or "lat,lon" rounded to 0.01 for custom points
    sky         TEXT NOT NULL         -- clear | cloudy | drizzle | heavy
);
CREATE INDEX IF NOT EXISTS ix_sky_place_time ON sky_reports (place, reported_at);
"""


@contextlib.contextmanager
def connect(path: Path = DB_PATH):
    """`with connect() as con:` -> commits on success, rolls back on error, and always CLOSES the
    connection (sqlite3's own context manager only commits, leaving the file handle open)."""
    con = sqlite3.connect(path, timeout=30)
    try:
        con.row_factory = sqlite3.Row
        con.executescript(SCHEMA)
        # migrate databases created before the 'tier' column existed
        if "tier" not in {r["name"] for r in con.execute("PRAGMA table_info(ingestion_runs)")}:
            con.execute("ALTER TABLE ingestion_runs ADD COLUMN tier TEXT")
        with con:
            yield con
    finally:
        con.close()


# ------------------------------------------------------------------ quota tracking

def _utc_day() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).date().isoformat()


def usage_today(provider: str, path: Path = DB_PATH) -> int:
    with connect(path) as con:
        r = con.execute("SELECT calls FROM api_usage WHERE day = ? AND provider = ?", (_utc_day(), provider)).fetchone()
        return r["calls"] if r else 0


def add_usage(provider: str, calls: int = 1, path: Path = DB_PATH) -> None:
    with connect(path) as con:
        con.execute("INSERT INTO api_usage (day, provider, calls) VALUES (?, ?, ?) "
                    "ON CONFLICT(day, provider) DO UPDATE SET calls = calls + excluded.calls",
                    (_utc_day(), provider, calls))


def usage_report(path: Path = DB_PATH) -> dict:
    with connect(path) as con:
        return {r["provider"]: r["calls"] for r in
                con.execute("SELECT provider, calls FROM api_usage WHERE day = ? ORDER BY provider", (_utc_day(),))}


# ------------------------------------------------------------------ live observations

def archive_observations(records: list[dict], path: Path = DB_PATH) -> int:
    rows = []
    for r in records:
        flags = r.get("flags") or []
        if not ({"observation", "current"} & set(flags)):
            continue
        rows.append([r.get("fetched_at"), r.get("provider"), "station" if "observation" in flags else "model",
                     r.get("city"), r.get("temp_now_c"), r.get("humidity_pct"), r.get("wind_speed_kph"),
                     r.get("wind_dir_deg"), r.get("precip_mm") if "observation" in flags else None,
                     r.get("cloud_cover_pct"), r.get("pressure_hpa")])
    if not rows:
        return 0
    with connect(path) as con:
        before = con.total_changes
        con.executemany("INSERT OR IGNORE INTO observations (captured_at, provider, kind, city, temp_c, humidity_pct, "
                        "wind_kph, wind_dir_deg, rain_today_mm, cloud_cover_pct, pressure_hpa) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
        return con.total_changes - before


def archive_metars(live_by_city: dict, path: Path = DB_PATH) -> int:
    """Store each city's nearest-airport METAR as an 'airport' observation (deduplicated by report time)."""
    rows = []
    for city, live in live_by_city.items():
        m = (live or {}).get("metar")
        if not m or not m.get("observed_at"):
            continue
        rows.append([m["observed_at"], f"METAR {m['icao']}", "airport", city, m.get("temp_c"), None,
                     m.get("wind_kph"), m.get("wind_dir_deg"), None, None, m.get("qnh_hpa")])
    if not rows:
        return 0
    with connect(path) as con:
        before = con.total_changes
        con.executemany("INSERT OR IGNORE INTO observations (captured_at, provider, kind, city, temp_c, humidity_pct, "
                        "wind_kph, wind_dir_deg, rain_today_mm, cloud_cover_pct, pressure_hpa) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
        return con.total_changes - before


SKY_CHOICES = ("clear", "cloudy", "drizzle", "heavy")


def add_sky_report(place: str, sky: str, path: Path = DB_PATH) -> None:
    from datetime import datetime, timezone
    if sky not in SKY_CHOICES:
        raise ValueError(f"sky must be one of {SKY_CHOICES}")
    with connect(path) as con:
        con.execute("INSERT INTO sky_reports (reported_at, place, sky) VALUES (?, ?, ?)",
                    (datetime.now(timezone.utc).isoformat(), place, sky))


def recent_sky_reports(place: str, hours: int = 2, path: Path = DB_PATH) -> dict:
    from datetime import datetime, timedelta, timezone
    if not path.exists():
        return {"hours": hours, "total": 0, "counts": {}, "latest": None}
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with connect(path) as con:
        rows = con.execute("SELECT sky, reported_at FROM sky_reports WHERE place = ? AND reported_at >= ? "
                           "ORDER BY reported_at DESC", (place, since)).fetchall()
    counts = {}
    for r in rows:
        counts[r["sky"]] = counts.get(r["sky"], 0) + 1
    return {"hours": hours, "total": len(rows), "counts": counts,
            "latest": {"sky": rows[0]["sky"], "at": rows[0]["reported_at"]} if rows else None}


def recent_observations(city: str, hours: int = 24, path: Path = DB_PATH) -> list[dict]:
    if not path.exists():
        return []
    from datetime import datetime, timedelta, timezone
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with connect(path) as con:
        return [dict(r) for r in con.execute(
            "SELECT * FROM observations WHERE city = ? COLLATE NOCASE AND captured_at >= ? ORDER BY captured_at",
            (city, since))]


# ------------------------------------------------------------------ writing

def archive_snapshot(records: list[dict], snapshot_at: str, snapshot_date: str,
                     path: Path = DB_PATH) -> int:
    """Append one run's records. Earlier snapshots are never modified. Returns rows inserted."""
    snap = date.fromisoformat(snapshot_date)
    cols = ["snapshot_at", "snapshot_date", "lead_days", "provider", "model_source", "city",
            "latitude", "longitude", "forecast_date", *FIELDS, "coverage_hours", "flags",
            "fetched_at", "raw_payload"]
    rows = []
    for r in records:
        try:
            lead = (date.fromisoformat(r["forecast_date"]) - snap).days
        except (KeyError, TypeError, ValueError):
            lead = None
        raw = r.get("raw_payload")
        blob = zlib.compress(json.dumps(raw, ensure_ascii=False).encode("utf-8"), 9) if raw else None
        rows.append([snapshot_at, snapshot_date, lead, r.get("provider"), r.get("model_source"),
                     r.get("city"), r.get("latitude"), r.get("longitude"), r.get("forecast_date"),
                     *[r.get(f) for f in FIELDS], r.get("coverage_hours"),
                     json.dumps(r.get("flags") or []), r.get("fetched_at"), blob])
    with connect(path) as con:
        before = con.total_changes
        con.executemany(f"INSERT OR IGNORE INTO daily_forecasts ({', '.join(cols)}) "
                        f"VALUES ({', '.join('?' * len(cols))})", rows)
        return con.total_changes - before


def log_run(run: dict, path: Path = DB_PATH) -> None:
    cols = ["started_at", "finished_at", "snapshot_date", "trigger", "status", "cities", "records",
            "archived", "providers_ok", "providers_failed", "errors", "duration_s", "tier"]
    vals = [json.dumps(run.get(c)) if c == "errors" else run.get(c) for c in cols]
    with connect(path) as con:
        con.execute(f"INSERT INTO ingestion_runs ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})", vals)


# ------------------------------------------------------------------ reading

def has_successful_run(snapshot_date: str, path: Path = DB_PATH) -> bool:
    if not path.exists():
        return False
    with connect(path) as con:
        return con.execute("SELECT 1 FROM ingestion_runs WHERE snapshot_date = ? AND status IN ('ok','partial') "
                           "AND archived > 0 AND COALESCE(tier, 'full') = 'full' LIMIT 1",
                           (snapshot_date,)).fetchone() is not None


def recent_runs(limit: int = 14, path: Path = DB_PATH) -> list[dict]:
    if not path.exists():
        return []
    with connect(path) as con:
        out = [dict(r) for r in con.execute("SELECT * FROM ingestion_runs ORDER BY id DESC LIMIT ?", (limit,))]
    for r in out:
        r["errors"] = json.loads(r["errors"] or "[]")
    return out


def summary(path: Path = DB_PATH) -> dict:
    if not path.exists():
        return {"snapshots": 0, "rows": 0, "first": None, "last": None, "size_mb": 0}
    with connect(path) as con:
        r = con.execute("SELECT COUNT(DISTINCT snapshot_at) s, COUNT(*) n, MIN(snapshot_date) a, "
                        "MAX(snapshot_date) b FROM daily_forecasts").fetchone()
    return {"snapshots": r["s"], "rows": r["n"], "first": r["a"], "last": r["b"],
            "size_mb": round(path.stat().st_size / 1e6, 2)}


def _excluded(row) -> bool:
    flags = json.loads(row["flags"] or "[]")
    return (bool({"trial", "reanalysis", "observation", "current"} & set(flags))
            or (row["coverage_hours"] if row["coverage_hours"] is not None else 24) < 20)


def forecast_evolution(city: str, forecast_date: str, path: Path = DB_PATH) -> dict:
    """For one city + day: the ensemble median at each morning snapshot, plus every provider's
    temp_max per snapshot. Reported values only - nothing is interpolated between snapshots."""
    if not path.exists():
        return {"snapshots": [], "providers": {}}
    with connect(path) as con:
        rows = con.execute(
            "SELECT snapshot_at, snapshot_date, lead_days, provider, model_source, temp_max_c, temp_min_c, "
            "precip_mm, precip_prob_pct, flags, coverage_hours FROM daily_forecasts "
            "WHERE city = ? COLLATE NOCASE AND forecast_date = ? ORDER BY snapshot_at", (city, forecast_date)).fetchall()
    by_snap, providers = {}, {}
    for r in rows:
        by_snap.setdefault(r["snapshot_at"], []).append(r)
        if r["temp_max_c"] is not None:
            providers.setdefault(f"{r['provider']} · {r['model_source']}", []).append(
                {"snapshot_date": r["snapshot_date"], "lead_days": r["lead_days"], "temp_max_c": r["temp_max_c"]})

    def med(rs, f):
        v = [x[f] for x in rs if x[f] is not None and not _excluded(x)]
        return (round(statistics.median(v), 2), len(v)) if v else (None, 0)

    snaps = []
    for snap_at, rs in by_snap.items():
        hi, n = med(rs, "temp_max_c")
        snaps.append({"snapshot_at": snap_at, "snapshot_date": rs[0]["snapshot_date"],
                      "lead_days": rs[0]["lead_days"], "models": n, "temp_max_median": hi,
                      "temp_min_median": med(rs, "temp_min_c")[0], "precip_mm_median": med(rs, "precip_mm")[0],
                      "precip_prob_median": med(rs, "precip_prob_pct")[0]})
    return {"snapshots": snaps, "providers": providers}


def accuracy(city: str, field: str = "temp_max_c", path: Path = DB_PATH) -> dict:
    """Mean absolute error of each provider vs. NASA POWER for days NASA has published.
    Returns an empty table until ~4 days of snapshots exist."""
    if field not in ("temp_max_c", "temp_min_c", "precip_mm", "humidity_pct", "wind_speed_kph"):
        raise ValueError("field must be one of temp_max_c, temp_min_c, precip_mm, humidity_pct, wind_speed_kph")
    if not path.exists():
        return {"truth": TRUTH_PROVIDER, "days_scored": 0, "providers": []}
    with connect(path) as con:
        # SQLite returns the bare column from the row holding MAX() -> latest NASA value per day
        truth = {r["forecast_date"]: r["v"] for r in con.execute(
            f"SELECT forecast_date, {field} v, MAX(snapshot_at) FROM daily_forecasts WHERE city = ? COLLATE NOCASE "
            f"AND provider = ? AND {field} IS NOT NULL GROUP BY forecast_date", (city, TRUTH_PROVIDER))}
        if not truth:
            return {"truth": TRUTH_PROVIDER, "days_scored": 0, "providers": []}
        marks = ",".join("?" * len(truth))
        rows = con.execute(
            f"SELECT provider, model_source, lead_days, forecast_date, {field} v, flags, coverage_hours "
            f"FROM daily_forecasts WHERE city = ? COLLATE NOCASE AND provider != ? AND {field} IS NOT NULL "
            f"AND lead_days >= 0 AND forecast_date IN ({marks})", (city, TRUTH_PROVIDER, *truth)).fetchall()
    errs = {}
    for r in rows:
        if _excluded(r):
            continue
        errs.setdefault(f"{r['provider']} · {r['model_source']}", []).append(abs(r["v"] - truth[r["forecast_date"]]))
    table = sorted(({"model": k, "mae": round(statistics.fmean(v), 2), "samples": len(v)} for k, v in errs.items()),
                   key=lambda x: x["mae"])
    return {"truth": TRUTH_PROVIDER, "field": field, "days_scored": len(truth), "providers": table}
