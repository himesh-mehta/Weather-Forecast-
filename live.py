"""
"Right now" layer: things that change minute to minute and sit beside the daily forecasts.

  * Airport weather (METAR) - official observer-verified reports, NOAA Aviation Weather Center
  * Air quality             - Open-Meteo Air Quality (CAMS model), US + European AQI and pollutants
  * 2-hour rain verdict     - next 2 h of 15-minute rain from 5 models -> how many expect rain
  * Rain radar frames       - RainViewer (past ~2 h, zoom <= 7 on the free API)

All keyless. City results are refreshed by the 15-minute scheduler task (tier A) and saved to
live_conditions.json; custom lat/lon points are fetched on demand.
"""
import asyncio
import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

from ingest import BASE_DIR, LOCATIONS, LOCAL_TZ, TZ_NAME, ensure_fetching_allowed, redact
from http_client import USER_AGENT, make_client

LIVE_PATH = BASE_DIR / "live_conditions.json"
UA = {"User-Agent": USER_AGENT}
AIRPORT_MAX_KM = 60          # farther than this, an airport doesn't represent the place
MAHARASHTRA_BBOX = "15.5,72.5,22.2,80.9"
NOWCAST_MODELS = ["best_match", "ecmwf_ifs025", "gfs_seamless", "icon_seamless", "jma_seamless"]
MODEL_NAMES = {"best_match": "Open-Meteo Best Match", "ecmwf_ifs025": "ECMWF", "gfs_seamless": "GFS (NOAA)",
               "icon_seamless": "ICON (DWD)", "jma_seamless": "JMA"}
WET_MM = 0.1                 # per 15 min

WX_CODES = {"RA": "rain", "DZ": "drizzle", "TS": "thunderstorm", "SH": "showers", "FG": "fog", "BR": "mist",
            "HZ": "haze", "FU": "smoke", "DU": "dust", "SA": "sand", "GR": "hail", "SQ": "squall", "VC": "nearby"}
COVER = {"FEW": "few clouds", "SCT": "scattered clouds", "BKN": "broken clouds", "OVC": "overcast",
         "NSC": "no significant cloud", "SKC": "clear sky", "CLR": "clear sky", "CAVOK": "clear, visibility OK"}


def km_between(lat1, lon1, lat2, lon2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 6371 * 2 * math.asin(math.sqrt(a))


def _decode_wx(wx: str | None) -> str | None:
    if not wx:
        return None
    words = []
    for token in wx.split():
        prefix = "light " if token.startswith("-") else "heavy " if token.startswith("+") else ""
        t = token.lstrip("+-")
        parts = [WX_CODES[t[i:i + 2]] for i in range(0, len(t), 2) if t[i:i + 2] in WX_CODES]
        if parts:
            words.append(prefix + " ".join(parts))
    return ", ".join(words) or wx


# ------------------------------------------------------------------ METAR

async def fetch_metars(client: httpx.AsyncClient, bbox: str) -> list[dict]:
    r = await client.get("https://aviationweather.gov/api/data/metar", params={"bbox": bbox, "format": "json"},
                         headers=UA)
    if r.status_code == 204:
        return []
    r.raise_for_status()
    return r.json()


def metar_for(lat: float, lon: float, metars: list[dict]) -> dict | None:
    """Nearest airport report within AIRPORT_MAX_KM, normalised. Values are as reported;
    only unit conversions (knots -> km/h, statute miles -> km) are applied."""
    best = min(((km_between(lat, lon, m["lat"], m["lon"]), m) for m in metars if m.get("lat") is not None),
               default=(None, None), key=lambda x: x[0])
    dist, m = best
    if m is None or dist > AIRPORT_MAX_KM:
        return None
    vis = m.get("visib")
    vis_plus = isinstance(vis, str) and vis.endswith("+")
    try:
        vis_km = round(float(str(vis).rstrip("+")) * 1.609, 1) if vis is not None else None
    except ValueError:
        vis_km = None
    clouds = [f"{COVER.get(c.get('cover'), c.get('cover'))}" + (f" at {c['base']} ft" if c.get("base") else "")
              for c in (m.get("clouds") or [])]
    raw = m.get("rawOb", "")
    if not clouds:
        for code in ("NSC", "SKC", "CLR", "CAVOK"):
            if f" {code}" in raw:
                clouds = [COVER[code]]
                break
    wspd, wgst = m.get("wspd"), m.get("wgst")
    return {
        "icao": m.get("icaoId"), "name": (m.get("name") or "").split(",")[0], "distance_km": round(dist, 1),
        "elevation_m": m.get("elev"),
        "observed_at": m.get("reportTime"), "temp_c": m.get("temp"), "dew_point_c": m.get("dewp"),
        "wind_dir_deg": m.get("wdir") if isinstance(m.get("wdir"), (int, float)) else None,
        "wind_variable": m.get("wdir") == "VRB",
        "wind_kph": round(wspd * 1.852, 1) if isinstance(wspd, (int, float)) else None,
        "gust_kph": round(wgst * 1.852, 1) if isinstance(wgst, (int, float)) else None,
        "visibility_km": vis_km, "visibility_plus": vis_plus, "qnh_hpa": m.get("altim"),
        "weather": _decode_wx(m.get("wxString")), "clouds": clouds, "flight_category": m.get("fltCat"),
        "raw": raw,
    }


# ------------------------------------------------------------------ air quality

AQ_VARS = "us_aqi,european_aqi,pm2_5,pm10,nitrogen_dioxide,ozone,sulphur_dioxide,carbon_monoxide"


async def fetch_air_quality(client: httpx.AsyncClient, lat: float, lon: float) -> dict | None:
    r = await client.get("https://air-quality-api.open-meteo.com/v1/air-quality", params={
        "latitude": lat, "longitude": lon, "timezone": TZ_NAME, "current": AQ_VARS,
        "hourly": "us_aqi", "forecast_hours": 24})
    r.raise_for_status()
    j = r.json()
    cur = j.get("current") or {}
    if cur.get("us_aqi") is None and cur.get("pm2_5") is None:
        return None
    hourly = j.get("hourly") or {}
    peak = max(((v, t) for t, v in zip(hourly.get("time", []), hourly.get("us_aqi", [])) if v is not None),
               default=(None, None))
    return {"time": cur.get("time"), "us_aqi": cur.get("us_aqi"), "european_aqi": cur.get("european_aqi"),
            "pm2_5": cur.get("pm2_5"), "pm10": cur.get("pm10"), "no2": cur.get("nitrogen_dioxide"),
            "o3": cur.get("ozone"), "so2": cur.get("sulphur_dioxide"), "co": cur.get("carbon_monoxide"),
            "units": "µg/m³", "peak_24h_us_aqi": peak[0], "peak_24h_at": peak[1],
            "source": "Open-Meteo Air Quality (CAMS global model)"}


# ------------------------------------------------------------------ 2-hour rain verdict

async def fetch_nowcast(client: httpx.AsyncClient, lat: float, lon: float) -> dict | None:
    r = await client.get("https://api.open-meteo.com/v1/forecast", params={
        "latitude": lat, "longitude": lon, "timezone": TZ_NAME, "models": ",".join(NOWCAST_MODELS),
        "minutely_15": "precipitation", "forecast_minutely_15": 8,
        "hourly": "precipitation_probability", "forecast_hours": 2})
    r.raise_for_status()
    j = r.json()
    m15, hourly = j.get("minutely_15") or {}, j.get("hourly") or {}
    times = m15.get("time", [])
    models = []
    for m in NOWCAST_MODELS:
        series = m15.get(f"precipitation_{m}") or []
        vals = [v for v in series if v is not None]
        if not vals:
            continue
        first_wet = next((t for t, v in zip(times, series) if v is not None and v >= WET_MM), None)
        probs = [p for p in (hourly.get(f"precipitation_probability_{m}") or []) if p is not None]
        models.append({"model": MODEL_NAMES[m], "wet": first_wet is not None, "total_mm": round(sum(vals), 1),
                       "starts_at": first_wet, "max_probability_pct": max(probs) if probs else None})
    if not models:
        return None
    wet = [m for m in models if m["wet"]]
    rain = len(wet) * 2 > len(models)
    agree = len(wet) if rain else len(models) - len(wet)
    starts = sorted(m["starts_at"] for m in wet)
    return {
        "elevation_m": j.get("elevation"),  # terrain height of the requested point
        "window": {"from": times[0] if times else None, "to": times[-1] if times else None},
        "verdict": "rain" if rain else "dry", "models_total": len(models), "models_wet": len(wet),
        "agreement_pct": round(100 * agree / len(models)),
        "earliest_start": starts[0] if starts else None,
        "max_total_mm": max((m["total_mm"] for m in models), default=None),
        "models": models,
    }


# ------------------------------------------------------------------ radar

_radar_cache = {"at": None, "data": None}


async def radar_frames() -> dict:
    # RainViewer public tile service is free, keyless, and does not consume API quotas
    now = datetime.now(timezone.utc)
    if _radar_cache["at"] and now - _radar_cache["at"] < timedelta(minutes=5):
        return _radar_cache["data"]
    async with make_client(timeout=20) as client:
        r = await client.get("https://api.rainviewer.com/public/weather-maps.json")
        r.raise_for_status()
        wm = r.json()
    data = {
        "host": wm.get("host"),
        "max_zoom": 7,
        "frames": [{"time": f["time"], "path": f["path"]} for f in (wm.get("radar") or {}).get("past", [])],
        "satellite": [{"time": f["time"], "path": f["path"]} for f in (wm.get("satellite") or {}).get("infrared", [])],
        "note": "Past ~2 hours of radar and satellite from RainViewer. "
                "Isolated speckles on dry days are usually ground clutter, not rain."
    }
    _radar_cache.update(at=now, data=data)
    return data


# ------------------------------------------------------------------ assembling

async def live_for_point(client, lat, lon, metars) -> dict:
    aq, nc = await asyncio.gather(fetch_air_quality(client, lat, lon), fetch_nowcast(client, lat, lon),
                                  return_exceptions=True)
    err = lambda x: redact(f"{type(x).__name__}: {x}")[:160]
    metar = metar_for(lat, lon, metars) if metars is not None else None
    if metar and not isinstance(nc, Exception) and nc and nc.get("elevation_m") is not None             and metar.get("elevation_m") is not None:
        metar["elevation_diff_m"] = round(nc["elevation_m"] - metar["elevation_m"])
    return {"fetched_at": datetime.now(timezone.utc).isoformat(),
            "metar": metar,
            "metar_error": None if metars is not None else "airport feed unavailable",
            "air_quality": None if isinstance(aq, Exception) else aq,
            "air_quality_error": err(aq) if isinstance(aq, Exception) else None,
            "nowcast": None if isinstance(nc, Exception) else nc,
            "nowcast_error": err(nc) if isinstance(nc, Exception) else None}


async def refresh_cities(cities=None) -> dict:
    """Fetch live conditions for the configured cities and save them (called by scheduler tier A)."""
    ensure_fetching_allowed()
    locs = [l for l in LOCATIONS if not cities or l["city"] in cities]
    async with make_client() as client:
        try:
            metars = await fetch_metars(client, MAHARASHTRA_BBOX)
        except Exception:  # noqa: BLE001 - airport feed down: still save AQ + nowcast
            metars = None
        results = {}
        for loc in locs:
            results[loc["city"]] = await live_for_point(client, loc["lat"], loc["lon"], metars)
            await asyncio.sleep(0.2)
    data = load_live()
    data.update(results)
    tmp = LIVE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=1, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, LIVE_PATH)
    return results


def load_live() -> dict:
    try:
        return json.loads(LIVE_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


async def live_for_custom(lat: float, lon: float) -> dict:
    ensure_fetching_allowed()
    bbox = f"{lat - 0.6:.2f},{lon - 0.6:.2f},{lat + 0.6:.2f},{lon + 0.6:.2f}"
    async with make_client() as client:
        try:
            metars = await fetch_metars(client, bbox)
        except Exception:  # noqa: BLE001
            metars = None
        return await live_for_point(client, lat, lon, metars)
