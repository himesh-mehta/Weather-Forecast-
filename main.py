"""
Weather consensus API + dashboard.

    uvicorn main:app --reload        ->  http://127.0.0.1:8000
"""
import asyncio
import statistics
from pathlib import Path
from datetime import date as date_cls, datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

from ingest import CACHE_PATH, LOCAL_TZ, LOCATIONS, FIELDS, load_cache, fetching_paused

BASE_DIR = Path(__file__).resolve().parent
FULL_DAY_HOURS = 20  # below this, a provider only saw part of the local day

from security import ALLOWED_ORIGINS, ENABLE_API_DOCS, SecurityMiddleware, is_admin  # noqa: E402 (after .env load)

app = FastAPI(title="Maharashtra Weather Consensus",
              # interactive API docs are off unless ENABLE_API_DOCS=1 (less surface when exposed)
              docs_url="/docs" if ENABLE_API_DOCS else None, redoc_url=None,
              openapi_url="/openapi.json" if ENABLE_API_DOCS else None)
app.add_middleware(SecurityMiddleware)
if ALLOWED_ORIGINS:  # default: no CORS at all -> browsers only allow same-origin calls
    from fastapi.middleware.cors import CORSMiddleware
    app.add_middleware(CORSMiddleware, allow_origins=ALLOWED_ORIGINS, allow_methods=["GET", "POST"],
                       allow_headers=["Content-Type"], allow_credentials=False)


def check_date(s: str | None) -> str | None:
    """YYYY-MM-DD that is also a real calendar date (the regex alone accepts 2026-13-45)."""
    if s is None:
        return None
    try:
        return date_cls.fromisoformat(s).isoformat()
    except ValueError:
        raise HTTPException(422, "date must be a real date in YYYY-MM-DD format")

# ------------------------------------------------------------------------------
# Domain / metric definitions (shared with the frontend via /api/today)
# ------------------------------------------------------------------------------

DOMAINS = [
    {"id": "thermal", "label": "Temperature", "icon": "🌡️", "columns": [
        {"key": "temp_min_c", "label": "Min", "unit": "°C", "dp": 1},
        {"key": "temp_max_c", "label": "Max", "unit": "°C", "dp": 1},
        {"key": "feels_like_max_c", "label": "Feels-like max", "unit": "°C", "dp": 1},
        {"key": "dew_point_c", "label": "Dew point", "unit": "°C", "dp": 1},
        {"key": "temp_now_c", "label": "Now (live)", "unit": "°C", "dp": 1}]},
    {"id": "precip", "label": "Rain & Precip", "icon": "🌧️", "columns": [
        {"key": "precip_prob_pct", "label": "Probability", "unit": "%", "dp": 0},
        {"key": "precip_mm", "label": "Amount", "unit": "mm", "dp": 1},
        {"key": "precip_type", "label": "Type", "kind": "ptype"},
        {"key": "precip_hours", "label": "Wet hours", "unit": "h", "dp": 1}]},
    {"id": "wind", "label": "Wind", "icon": "💨", "columns": [
        {"key": "wind_speed_kph", "label": "Wind speed", "unit": "km/h", "dp": 1},
        {"key": "wind_gust_kph", "label": "Max gust", "unit": "km/h", "dp": 1},
        {"key": "wind_dir_deg", "label": "Direction", "kind": "dir"}]},
    {"id": "solar", "label": "Solar & UV", "icon": "☀️", "columns": [
        {"key": "uv_index", "label": "UV index", "unit": "", "dp": 1},
        {"key": "sunshine_hours", "label": "Sunshine", "unit": "h", "dp": 1},
        {"key": "solar_radiation_wm2", "label": "Solar rad. (day mean)", "unit": "W/m²", "dp": 0}]},
    {"id": "sky", "label": "Sky & Air", "icon": "☁️", "columns": [
        {"key": "cloud_cover_pct", "label": "Cloud cover", "unit": "%", "dp": 0},
        {"key": "humidity_pct", "label": "Humidity", "unit": "%", "dp": 0},
        {"key": "pressure_hpa", "label": "Pressure (MSL)", "unit": "hPa", "dp": 0},
        {"key": "visibility_km", "label": "Visibility", "unit": "km", "dp": 1}]},
    {"id": "marine", "label": "Marine", "icon": "🌊", "columns": [
        {"key": "wave_height_m", "label": "Wave height (max)", "unit": "m", "dp": 2},
        {"key": "swell_height_m", "label": "Swell height (max)", "unit": "m", "dp": 2},
        {"key": "sea_surface_temp_c", "label": "Sea surface temp", "unit": "°C", "dp": 1}]},
    {"id": "astro", "label": "Sun/Moon", "icon": "🌓", "columns": [
        {"key": "sunrise", "label": "Sunrise", "kind": "text"},
        {"key": "sunset", "label": "Sunset", "kind": "text"},
        {"key": "moon_phase", "label": "Moon phase", "kind": "text"},
        {"key": "moon_illumination_pct", "label": "Illumination", "unit": "%", "dp": 0}]},
    {"id": "consensus", "label": "All Models", "icon": "📊", "columns": []},
]

# key -> agreement tolerance (a model "agrees" if within ±tol of the ensemble median)
METRICS = {
    "temp_min_c": 1.5, "temp_max_c": 1.5, "feels_like_max_c": 2.0, "dew_point_c": 1.5,
    "precip_mm": None,  # categorical agreement: rain vs dry
    "precip_prob_pct": 15, "precip_hours": 2.0,
    "wind_speed_kph": 5.0, "wind_gust_kph": 8.0,
    "uv_index": 1.5, "sunshine_hours": 1.5, "solar_radiation_wm2": 30,
    "cloud_cover_pct": 20, "humidity_pct": 10, "pressure_hpa": 2, "visibility_km": 5,
    "wave_height_m": 0.2, "swell_height_m": 0.2, "sea_surface_temp_c": 1.0,
    "moon_illumination_pct": 5,
}
# Strict display order. Each slot: (label, record matcher, status key in the ingest log or None
# if no adapter exists). A slot with no record for the day is still shown, as an all-"—" row.
PRIORITY = [
    ("ECMWF", lambda r: r["provider"] == "ECMWF", "Open-Meteo / ECMWF"),
    ("IMD / Mausam", lambda r: r["provider"].startswith("IMD"), None),
    ("AccuWeather", lambda r: r["provider"] == "AccuWeather", "AccuWeather"),
    ("The Weather Channel", lambda r: r["provider"] == "The Weather Channel", None),
    ("Windy", lambda r: r["provider"] == "Windy", "Windy"),
    ("NOAA / GFS", lambda r: r["provider"] == "NOAA / NWS" or r["model_source"] == "GFS (NOAA)", "Open-Meteo / ECMWF"),
    ("MET Norway", lambda r: r["provider"] == "MET Norway", "MET Norway"),
    ("NASA POWER", lambda r: r["provider"] == "NASA POWER", "NASA POWER"),
    ("OpenWeather", lambda r: r["provider"] == "OpenWeather", "OpenWeather"),
    ("Tomorrow.io", lambda r: r["provider"] == "Tomorrow.io", "Tomorrow.io"),
    ("Visual Crossing", lambda r: r["provider"] == "Visual Crossing", "Visual Crossing"),
    ("WeatherAPI.com", lambda r: r["provider"] == "WeatherAPI.com", "WeatherAPI.com"),
    ("Meteoblue", lambda r: r["provider"] == "Meteoblue", "Meteoblue"),
    ("Weatherbit", lambda r: r["provider"] == "Weatherbit", "Weatherbit"),
    ("Meteosource", lambda r: r["provider"] == "Meteosource", "Meteosource"),
    ("Stormglass", lambda r: r["provider"] == "Stormglass", "Stormglass"),
    # The marine feed the original pipeline called "INCOIS" is really the Open-Meteo Marine API.
    ("INCOIS", lambda r: r["provider"] in ("INCOIS", "Open-Meteo Marine"), "Open-Meteo Marine"),
    ("DWD Bright Sky", lambda r: r["provider"] == "DWD Bright Sky", "DWD Bright Sky"),
    # added after the original 18
    ("Met Office", lambda r: r["provider"] == "Met Office", "Met Office"),
    ("Xweather", lambda r: r["provider"] == "Xweather", "Xweather"),
    ("Weather Union", lambda r: r["provider"] == "Weather Union", "Weather Union"),
    ("Foreca", lambda r: r["provider"] == "Foreca", "Foreca"),
    ("Ambee", lambda r: r["provider"] == "Ambee", "Ambee"),
    ("Google Weather", lambda r: r["provider"] == "Google Weather", "Google Weather"),
]


def ordered_models(rows: list[dict], provider_status: dict) -> list[dict]:
    """All 18 priority slots in order (placeholder rows when empty), then any unlisted models."""
    used, out = set(), []

    def emit(r, rank, slot):
        used.add(id(r))
        out.append({"id": model_id(r), "rank": rank, "slot": slot, "placeholder": False,
                    **{k: v for k, v in r.items() if k != "raw_payload"}})

    for rank, (slot, match, status_key) in enumerate(PRIORITY, 1):
        hits = sorted((r for r in rows if id(r) not in used and match(r)), key=lambda r: r["model_source"])
        for r in hits:
            emit(r, rank, slot)
        if hits:
            continue
        status = provider_status.get(status_key, "") if status_key else ""
        if status_key is None:
            why = "no data source connected"
        elif status == "no API key":
            why = "no API key in .env"
        elif status == "skipped: custom point":
            why = ("reanalysis, ~3 days behind: no data for this date" if slot == "NASA POWER"
                   else "not used for custom locations (saves its small daily quota)")
        elif status.startswith("skipped: daily quota"):
            why = "held back by the daily quota guard"
        elif slot == "Weather Union":
            why = "live readings only (today)" if status.startswith("ok") else "no station near this location"
        elif slot == "NASA POWER":
            why = "reanalysis, ~3 days behind: no data for this date"
        elif status.startswith("error"):
            why = "fetch failed"
        elif "n/a" in status:
            why = "not available for this location"
        elif not status:
            why = "not fetched"
        else:
            why = "no data for this date"
        out.append({"id": slot, "rank": rank, "slot": slot, "placeholder": True, "placeholder_reason": why,
                    "provider": slot, "model_source": "", "flags": [], "coverage_hours": None,
                    **{f: None for f in FIELDS}})

    extra = sorted((r for r in rows if id(r) not in used), key=lambda r: (r["provider"], r["model_source"]))
    for r in extra:
        emit(r, len(PRIORITY) + 1, "Other")
    return out


PARTIAL_SENSITIVE ={"temp_min_c", "temp_max_c", "feels_like_max_c", "precip_mm", "precip_hours"}
HEADLINE = ["temp_max_c", "temp_min_c", "precip_mm", "wind_speed_kph"]
WET_MM = 0.2

# ------------------------------------------------------------------------------
# Cache access (reloads automatically when ingest rewrites the file)
# ------------------------------------------------------------------------------

_cache = {"mtime": None, "data": None}


def cache() -> dict:
    try:
        mtime = CACHE_PATH.stat().st_mtime
    except OSError:
        return {"generated_at": None, "cities": {}, "records": []}
    if _cache["mtime"] != mtime:
        _cache["data"], _cache["mtime"] = load_cache(), mtime
    return _cache["data"]


def find_location(city: str) -> dict:
    loc = next((l for l in LOCATIONS if l["city"].lower() == city.lower()), None)
    if not loc:
        raise HTTPException(404, f"Unknown city '{city}'. See /api/locations.")
    return loc


def today_local() -> str:
    return datetime.now(LOCAL_TZ).date().isoformat()


def city_records(city: str) -> list[dict]:
    return [r for r in cache().get("records", []) if r.get("city", "").lower() == city.lower()]


def pick_date(records: list[dict], requested) -> tuple[str, list[str]]:
    dates = sorted({r["forecast_date"] for r in records})
    if requested:
        return check_date(requested), dates
    t = today_local()
    if t in dates or not dates:
        return t, dates
    future = [d for d in dates if d >= t]
    return (future[0] if future else dates[-1]), dates


def model_id(r: dict) -> str:
    return f"{r['provider']} · {r['model_source']}"


def exclusion_reason(r: dict, key: str):
    flags = r.get("flags") or []
    if "trial" in flags:
        return "trial API key (shuffled data)"
    if "reanalysis" in flags:
        return "reanalysis, not a forecast"
    if "observation" in flags:
        return "live station reading, not a forecast"
    if "current" in flags:
        return "current-conditions analysis, not a daily forecast"
    cov = r.get("coverage_hours")
    if key in PARTIAL_SENSITIVE and cov is not None and cov < FULL_DAY_HOURS:
        return f"partial day ({cov}h of 24h)"
    if key == "wind_speed_kph" and r.get("wind_basis") == "avg":
        return "daily mean, not max"
    if key == "wind_speed_kph" and r.get("wind_basis") == "spot":
        return "midday/midnight spot values, not max"
    return None


# ------------------------------------------------------------------------------
# Ensemble statistics
# ------------------------------------------------------------------------------

def metric_stats(rows: list[dict], key: str) -> dict:
    included, excluded = [], []
    for r in rows:
        v = r.get(key)
        if not isinstance(v, (int, float)):
            continue
        why = exclusion_reason(r, key)
        (excluded if why else included).append((model_id(r), float(v), why))

    out = {"key": key, "n": len(included),
           "excluded": [{"model": m, "value": v, "reason": w} for m, v, w in excluded]}
    if not included:
        return out
    vals = [v for _, v, _ in included]
    med = statistics.median(vals)
    out.update({
        "median": med, "mean": statistics.fmean(vals), "min": min(vals), "max": max(vals),
        "spread": max(vals) - min(vals), "stdev": statistics.pstdev(vals) if len(vals) > 1 else 0.0,
    })
    tol = METRICS.get(key)
    if key == "precip_mm":
        wet = sum(v >= WET_MM for v in vals)
        majority_wet = wet * 2 > len(vals)
        agree = wet if majority_wet else len(vals) - wet
        out["agreement_pct"] = round(100 * agree / len(vals))
        out["consensus_label"] = "rain" if majority_wet else f"dry (< {WET_MM} mm)"
        out["outliers"] = [m for m, v, _ in included if (v >= WET_MM) != majority_wet]
    elif tol is not None:
        agree = sum(abs(v - med) <= tol for v in vals)
        out["agreement_pct"] = round(100 * agree / len(vals))
        out["tolerance"] = tol
        out["outliers"] = [m for m, v, _ in included if abs(v - med) > 2 * tol]
    return out


def consensus_for(rows: list[dict]) -> dict:
    metrics = {k: metric_stats(rows, k) for k in METRICS}
    head = [metrics[k]["agreement_pct"] for k in HEADLINE
            if metrics[k].get("n", 0) >= 3 and "agreement_pct" in metrics[k]]
    score = round(statistics.fmean(head)) if head else None
    level = None if score is None else ("high" if score >= 80 else "moderate" if score >= 60 else "low")
    return {"agreement_score": score, "agreement_level": level, "headline_metrics": HEADLINE,
            "metrics": metrics}


# ------------------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------------------

_page = {"mtime": None, "html": ""}


@app.get("/")
def dashboard(request: Request):
    """Dashboard page. Its inline <script> gets this request's CSP nonce, so the browser runs
    only that script - an injected <script> (XSS) has no nonce and is blocked."""
    path = BASE_DIR / "templates" / "index.html"
    mtime = path.stat().st_mtime
    if _page["mtime"] != mtime:
        _page.update(mtime=mtime, html=path.read_text(encoding="utf-8"))
    # Replace the inline script tag with the nonce
    html = _page["html"].replace("<script>", f'<script nonce="{request.state.csp_nonce}">', 1)
    return HTMLResponse(html, headers={"Cache-Control": "no-cache"})


@app.get("/api/locations")
def locations():
    meta = cache().get("cities", {})
    return [{"city": l["city"], "lat": l["lat"], "lon": l["lon"], "coastal": l.get("coastal", False),
             "region": l.get("region"), "fetched_at": meta.get(l["city"], {}).get("fetched_at")}
            for l in LOCATIONS]


def _day_rows(city: str, day):
    find_location(city)
    recs = city_records(city)
    d, dates = pick_date(recs, day)
    return [r for r in recs if r["forecast_date"] == d], d, dates


def day_payload(loc: dict, recs: list[dict], status: dict, date, fetched_at, updated_at, kind="city") -> dict:
    """Everything the dashboard needs for one place + day (shared by cities and custom points)."""
    d, dates = pick_date(recs, date)
    rows = [r for r in recs if r["forecast_date"] == d]
    return {
        "kind": kind, "city": loc["city"], "lat": loc["lat"], "lon": loc["lon"], "region": loc.get("region"),
        "coastal": loc.get("coastal", False), "date": d, "today": today_local(),
        "available_dates": dates, "fetched_at": fetched_at, "updated_at": updated_at or fetched_at,
        "provider_status": status, "domains": DOMAINS,
        "models": ordered_models(rows, status), "consensus": consensus_for(rows),
        "fetch_paused": fetching_paused(),
    }


def week_summary(recs: list[dict]) -> list[dict]:
    t = today_local()
    days = []
    for d in sorted({r["forecast_date"] for r in recs if r["forecast_date"] >= t}):
        c = consensus_for([r for r in recs if r["forecast_date"] == d])
        m = c["metrics"]
        g = lambda k, f="median": m[k].get(f)
        days.append({
            "date": d, "high": g("temp_max_c"), "low": g("temp_min_c"),
            "rain_prob": g("precip_prob_pct"), "rain_mm": g("precip_mm"),
            "rain_label": g("precip_mm", "consensus_label"), "cloud": g("cloud_cover_pct"),
            "uv": g("uv_index"), "wind": g("wind_speed_kph"), "models": m["temp_max_c"].get("n", 0),
            "agreement_score": c["agreement_score"], "agreement_level": c["agreement_level"],
        })
    return days


@app.get("/api/today")
def today(city: str = "Mumbai", date: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$")):
    loc = find_location(city)
    meta = cache().get("cities", {}).get(loc["city"], {})
    return day_payload(loc, city_records(loc["city"]), meta.get("providers", {}), date,
                       meta.get("fetched_at"), meta.get("updated_at"))


@app.get("/api/consensus")
def consensus(city: str = "Mumbai", date: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$")):
    rows, d, _ = _day_rows(city, date)
    return {"city": find_location(city)["city"], "date": d, "model_count": len(rows), **consensus_for(rows)}


@app.get("/api/timeseries")
def timeseries(city: str = "Mumbai", metric: str = "temp_max_c",
               lat: float | None = Query(None, ge=-90, le=90), lon: float | None = Query(None, ge=-180, le=180)):
    """7-day curve per provider/model for one metric, plus the ensemble median and the range of
    counted forecasts. Works for a city, or for a lat/lon already opened via /api/point."""
    if metric not in METRICS:
        raise HTTPException(422, f"metric must be one of {', '.join(METRICS)}")
    if lat is not None and lon is not None:
        hit = _points.get((round(lat, 2), round(lon, 2)))
        if not hit:
            raise HTTPException(404, "Open this location first (GET /api/point), then ask for its time series.")
        recs, place = hit["records"], hit["loc"]["city"]
    else:
        loc = find_location(city)
        recs, place = city_records(loc["city"]), loc["city"]
    return {"place": place, **timeseries_from(recs, metric)}


def _rank(r: dict) -> int:
    return next((i for i, (_, match, _) in enumerate(PRIORITY, 1) if match(r)), len(PRIORITY) + 1)


def timeseries_from(recs: list[dict], metric: str) -> dict:
    """Values are exactly what each provider reported; a day it didn't report is null (a gap)."""
    col = next((c for dm in DOMAINS for c in dm["columns"] if c["key"] == metric), {})
    dates = sorted({r["forecast_date"] for r in recs if r["forecast_date"] >= today_local()})[:7]
    series, ensemble = {}, []
    for i, d in enumerate(dates):
        rows = [r for r in recs if r["forecast_date"] == d]
        st = metric_stats(rows, metric)
        ensemble.append({k: st.get(k) for k in ("median", "min", "max", "n", "agreement_pct")} | {"date": d})
        excluded = {e["model"]: e["reason"] for e in st["excluded"]}
        for r in rows:
            v = r.get(metric)
            if not isinstance(v, (int, float)):
                continue
            mid = model_id(r)
            s_ = series.setdefault(mid, {
                "id": mid, "provider": r["provider"], "model_source": r.get("model_source"), "rank": _rank(r),
                "flags": r.get("flags") or [], "values": [None] * len(dates), "excluded": [None] * len(dates)})
            s_["values"][i] = v
            s_["excluded"][i] = excluded.get(mid)
    out = sorted(series.values(), key=lambda x: (x["rank"], x["provider"], x["model_source"] or ""))
    for s_ in out:  # a line that is never counted (e.g. Windy trial data) is drawn dashed
        reasons = [e for v, e in zip(s_["values"], s_["excluded"]) if v is not None]
        s_["never_counted"] = bool(reasons) and all(reasons)
        s_["data"] = s_["values"]  # backwards-compatible alias (the Chart.js timeline reads `data`)
    return {"metric": metric, "label": col.get("label", metric), "unit": col.get("unit", ""), "dp": col.get("dp", 1),
            "dates": dates, "ensemble": ensemble, "series": out}


@app.get("/api/timeseries")
def timeseries(city: str = "Mumbai", metric: str = "temp_max_c"):
    """Multi-model 7-day time series timeline with per-provider curves and ensemble corridor."""
    loc = find_location(city)
    recs = city_records(loc["city"])
    t = today_local()
    dates = sorted({r["forecast_date"] for r in recs if r["forecast_date"] >= t})[:7]
    
    # Calculate ensemble stats (median, 25th percentile, 75th percentile, min, max) per day
    ensemble = []
    providers_data = {}
    
    for d in dates:
        day_rows = [r for r in recs if r["forecast_date"] == d]
        c = consensus_for(day_rows)
        m_stat = c["metrics"].get(metric, {})
        ensemble.append({
            "date": d,
            "median": m_stat.get("median"),
            "mean": m_stat.get("mean"),
            "min": m_stat.get("min"),
            "max": m_stat.get("max"),
            "spread": m_stat.get("spread"),
            "agreement_pct": m_stat.get("agreement_pct"),
            "n": m_stat.get("n", 0),
        })
        
        # Populate provider trajectories
        for r in day_rows:
            p_name = r["provider"]
            if r.get("flags") and ("trial" in r["flags"] or "reanalysis" in r["flags"]):
                continue
            v = r.get(metric)
            if p_name not in providers_data:
                providers_data[p_name] = {
                    "provider": p_name,
                    "model_source": r.get("model_source"),
                    "values": {dt: None for dt in dates}
                }
            providers_data[p_name]["values"][d] = v
            
    # Convert providers_data to sorted list
    series = []
    for p_name, p_info in sorted(providers_data.items()):
        vals = [p_info["values"][dt] for dt in dates]
        if any(v is not None for v in vals):
            series.append({
                "provider": p_name,
                "model_source": p_info["model_source"],
                "data": vals,
            })
            
    return {
        "city": loc["city"],
        "metric": metric,
        "dates": dates,
        "ensemble": ensemble,
        "series": series
    }


@app.get("/api/week")
def week(city: str = "Mumbai"):
    """One ensemble summary per forecast day (today onward) for the 7-day strip."""
    loc = find_location(city)
    return {"city": loc["city"], "today": today_local(), "days": week_summary(city_records(loc["city"]))}


@app.get("/api/divergence")
def divergence(date: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$")):
    """Cross-city comparison of how much the models disagree (e.g. coastal Mumbai vs Vidarbha)."""
    d = date or today_local()
    out = []
    for loc in LOCATIONS:
        rows = [r for r in city_records(loc["city"]) if r["forecast_date"] == d]
        if not rows:
            continue
        c = consensus_for(rows)
        m = c["metrics"]
        pick = lambda k, f: m[k].get(f)
        out.append({"city": loc["city"], "region": loc.get("region"), "coastal": loc.get("coastal", False),
                    "models": len(rows), "temp_max_median": pick("temp_max_c", "median"),
                    "temp_max_spread": pick("temp_max_c", "spread"), "temp_max_stdev": pick("temp_max_c", "stdev"),
                    "temp_min_median": pick("temp_min_c", "median"),
                    "temp_min_spread": pick("temp_min_c", "spread"),
                    "humidity_median": pick("humidity_pct", "median"),
                    "wind_spread": pick("wind_speed_kph", "spread"),
                    "agreement_score": c["agreement_score"], "agreement_level": c["agreement_level"]})
    return {"date": d, "cities": out}


# --- forecast for any latitude / longitude (on demand, cached) -------------------
# Data for the configured cities is refreshed automatically by the scheduler. A custom point is
# fetched the first time it's asked for, then served from memory for POINT_TTL.

POINT_TTL = timedelta(hours=2)
POINT_NEW_LOOKUPS_PER_HOUR = 20     # protects the keyless services from runaway use
_points: dict[tuple, dict] = {}     # (lat, lon) rounded to 0.01° (~1 km) -> fetch result
_point_locks: dict[tuple, asyncio.Lock] = {}
_lookup_times: list[datetime] = []


@app.get("/api/point")
async def point(lat: float = Query(..., ge=-90, le=90), lon: float = Query(..., ge=-180, le=180),
                date: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$")):
    """All-in-one payload (day + 7-day strip) for any coordinate. Uses keyless and generous-quota
    providers only; strict-quota ones appear as '—' rows with the reason."""
    from ingest import fetch_point
    key = (round(lat, 2), round(lon, 2))
    lock = _point_locks.setdefault(key, asyncio.Lock())
    async with lock:  # two tabs asking for the same point -> one fetch
        hit = _points.get(key)
        now = datetime.now(timezone.utc)
        if not hit or now - datetime.fromisoformat(hit["fetched_at"]) > POINT_TTL:
            recent = [t for t in _lookup_times if now - t < timedelta(hours=1)]
            _lookup_times[:] = recent
            if len(recent) >= POINT_NEW_LOOKUPS_PER_HOUR:
                raise HTTPException(429, f"Limit of {POINT_NEW_LOOKUPS_PER_HOUR} new locations per hour reached "
                                         "(protects the free weather APIs). Try again later.")
            from ingest import FetchPaused
            try:
                hit = await fetch_point(*key)
            except FetchPaused as exc:
                raise HTTPException(503, str(exc))
            _lookup_times.append(now)
            _points[key] = hit
    loc = hit["loc"]
    payload = day_payload(loc, hit["records"], hit["status"], date, hit["fetched_at"], hit["fetched_at"], kind="point")
    payload["week"] = {"city": loc["city"], "today": today_local(), "days": week_summary(hit["records"])}
    payload["cache_expires_at"] = (datetime.fromisoformat(hit["fetched_at"]) + POINT_TTL).isoformat()
    return payload


# --- forecast archive (written by the 06:00 scheduler) ---------------------------

@app.get("/api/history")
def forecast_history(city: str = "Mumbai", date: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$")):
    """How the forecast for one day changed over the mornings leading up to it."""
    import history
    date = check_date(date)
    loc = find_location(city)
    return {"city": loc["city"], "date": date, "archive": history.summary(),
            **history.forecast_evolution(loc["city"], date)}


@app.get("/api/accuracy")
def forecast_accuracy(city: str = "Mumbai", field: str = "temp_max_c"):
    """Mean absolute error per provider vs NASA POWER (fills in once ~4 days are archived)."""
    import history
    loc = find_location(city)
    try:
        return {"city": loc["city"], **history.accuracy(loc["city"], field)}
    except ValueError as exc:
        raise HTTPException(422, str(exc))


@app.get("/api/runs")
def ingestion_runs(request: Request, limit: int = Query(14, ge=1, le=200), tier: str | None = None):
    """Recent scheduler runs plus the archive size. Admins get full detail (errors, counts);
    everyone else gets only time + status (enough for the dashboard's 'last update' line)."""
    import history
    runs = history.recent_runs(200 if tier else limit)
    if tier:
        runs = [r for r in runs if (r.get("tier") or "full") == tier][:limit]
    if not is_admin(request):
        runs = [{k: r.get(k) for k in ("started_at", "finished_at", "status", "tier")} for r in runs[:5]]
    return {"archive": history.summary(), "runs": runs}


@app.get("/api/observations")
def observations(city: str = "Mumbai", hours: int = Query(24, ge=1, le=24 * 31)):
    """Live readings captured every 15 min (station = measured, model = current analysis)."""
    import history
    loc = find_location(city)
    return {"city": loc["city"], "hours": hours, "readings": history.recent_observations(loc["city"], hours)}


@app.get("/api/usage")
def api_usage(request: Request):
    """Today's API calls per provider vs. its free-tier daily limit (resets 00:00 UTC = 05:30 IST).
    Admin only: reveals which upstream accounts exist and how close they are to their limits."""
    import history
    if not is_admin(request):
        raise HTTPException(403, "Admin only: send the X-Admin-Key header (or call from this PC).")
    from ingest import QUOTAS, QUOTA_SAFETY, TIERS
    used = history.usage_report()
    tier_of = {p: t for t, ps in TIERS.items() for p in ps}
    return {"guard_at_pct": int(QUOTA_SAFETY * 100), "providers": [
        {"provider": p, "tier": tier_of.get(p), "used": used.get(p, 0), "limit": lim,
         "cost_per_city_call": cost, "pct": round(100 * used.get(p, 0) / lim, 1)}
        for p, (lim, cost) in sorted(QUOTAS.items())]}



# --- "right now" layer: airport METAR, air quality, 2-hour rain verdict, radar, sky reports ---

LIVE_STALE = timedelta(minutes=30)   # city data older than this is fetched on demand
POINT_LIVE_TTL = timedelta(minutes=15)
_live_points: dict[tuple, dict] = {}
_live_city_locks: dict[str, asyncio.Lock] = {}
_sky_last: dict[tuple, datetime] = {}


def _age_ok(item, ttl) -> bool:
    return bool(item) and datetime.now(timezone.utc) - datetime.fromisoformat(item["fetched_at"]) < ttl


@app.get("/api/live")
async def live_now(city: str | None = None, lat: float | None = Query(None, ge=-90, le=90),
                   lon: float | None = Query(None, ge=-180, le=180)):
    """Nearest airport METAR, air quality and the 2-hour rain verdict for a city or a lat/lon."""
    import history
    import live
    from ingest import FetchPaused, fetching_paused
    if lat is not None and lon is not None:
        key = (round(lat, 2), round(lon, 2))
        item = _live_points.get(key)
        if not _age_ok(item, POINT_LIVE_TTL):
            try:
                item = await live.live_for_custom(*key)
                _live_points[key] = item
            except FetchPaused as exc:
                if not item:
                    raise HTTPException(503, str(exc))
        place = f"{key[0]:.2f},{key[1]:.2f}"
    else:
        loc = find_location(city or "Mumbai")
        place = loc["city"]
        item = live.load_live().get(place)
        if not _age_ok(item, LIVE_STALE):  # scheduler hasn't refreshed it recently -> fetch now
            lock = _live_city_locks.setdefault(place, asyncio.Lock())
            async with lock:
                item = live.load_live().get(place)
                if not _age_ok(item, LIVE_STALE):
                    try:
                        item = (await live.refresh_cities([place]))[place]
                    except FetchPaused:
                        pass  # paused: serve the last saved live data (may be old or missing)
    return {"place": place, **(item or {}), "fetch_paused": fetching_paused(),
            "sky_reports": history.recent_sky_reports(place)}


@app.get("/api/radar")
async def radar():
    """RainViewer radar frames (past ~2 h) for the map."""
    import live
    try:
        return await live.radar_frames()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(502, f"Radar service unavailable: {type(exc).__name__}: {exc}")


@app.post("/api/report-sky")
async def report_sky(request: Request):
    """Store one 'what does your sky look like' report. Body: {"sky": ..., "city": ...} or
    {"sky": ..., "lat": .., "lon": ..}. One report per place per 10 minutes from the same client."""
    import history
    try:
        body = await request.json()
    except ValueError:
        raise HTTPException(422, "Send JSON like {\"sky\": \"clear\", \"city\": \"Mumbai\"}.")
    sky = str(body.get("sky", "")).lower()
    if sky not in history.SKY_CHOICES:
        raise HTTPException(422, f"sky must be one of {', '.join(history.SKY_CHOICES)}")
    if body.get("lat") is not None and body.get("lon") is not None:
        try:
            lat, lon = float(body["lat"]), float(body["lon"])
        except (TypeError, ValueError):
            raise HTTPException(422, "lat/lon must be numbers")
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            raise HTTPException(422, "lat/lon out of range")
        place = f"{round(lat, 2):.2f},{round(lon, 2):.2f}"
    else:
        place = find_location(str(body.get("city") or "Mumbai"))["city"]
    client = request.client.host if request.client else "?"
    now = datetime.now(timezone.utc)
    last = _sky_last.get((client, place))
    if last and now - last < timedelta(minutes=10):
        raise HTTPException(429, "Thanks! You already reported this place in the last 10 minutes.")
    history.add_sky_report(place, sky)
    _sky_last[(client, place)] = now
    return {"ok": True, "place": place, "sky_reports": history.recent_sky_reports(place)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
