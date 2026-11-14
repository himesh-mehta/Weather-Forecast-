"""
Multi-provider daily weather ingestion for Maharashtra.

Every adapter returns records in one normalised schema (see FIELDS). Days are
bucketed in local time (IST, UTC+05:30) so every provider describes the same
24-hour window. Run directly to refresh every configured city:

    python ingest.py               # all cities
    python ingest.py Mumbai Pune   # selected cities
"""
import os
import re
import sys
import json
import math
import time
import asyncio
import contextlib
from pathlib import Path
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import httpx
from dotenv import dotenv_values

BASE_DIR = Path(__file__).resolve().parent
CACHE_PATH = BASE_DIR / "latest_forecasts.json"
ACCU_KEY_CACHE = BASE_DIR / ".cache" / "accuweather_location_keys.json"


def load_env_file(path: Path) -> list[str]:
    """Load .env without overriding real environment values, but DO fill variables that exist
    and are empty. In GitHub Actions every `KEY: ${{ secrets.KEY }}` mapping creates the variable
    even when that secret isn't set (as ""), which must not hide the key from a DOTENV-written .env.
    Non-empty environment values always win (e.g. the workflow's FETCH_PAUSED=0)."""
    filled = []
    for k, v in dotenv_values(path).items():   # a later duplicate in .env wins, like python-dotenv
        if v not in (None, "") and not os.environ.get(k):
            os.environ[k] = v
            filled.append(k)
    return filled


load_env_file(BASE_DIR / ".env")

from http_client import make_client, pace  # noqa: E402 (needs .env loaded first)

# All configured cities are in India -> one fixed offset, no tz database needed
# (Windows ships without IANA tz data, so zoneinfo would need the tzdata package).
TZ_NAME = "Asia/Kolkata"
LOCAL_TZ = timezone(timedelta(hours=5, minutes=30))

LOCATIONS = [
    {"city": "Mumbai", "lat": 19.0760, "lon": 72.8777, "country": "IN", "coastal": True,
     "region": "Konkan (coastal)"},
    {"city": "Ratnagiri", "lat": 16.9902, "lon": 73.3120, "country": "IN", "coastal": True,
     "region": "Konkan (coastal)", "marine": (16.99, 73.25)},
    {"city": "Pune", "lat": 18.5204, "lon": 73.8567, "country": "IN", "coastal": False,
     "region": "Western Maharashtra"},
    {"city": "Kolhapur", "lat": 16.7050, "lon": 74.2433, "country": "IN", "coastal": False,
     "region": "Western Maharashtra"},
    {"city": "Nashik", "lat": 19.9975, "lon": 73.7898, "country": "IN", "coastal": False,
     "region": "North Maharashtra"},
    {"city": "Chhatrapati Sambhajinagar", "lat": 19.8762, "lon": 75.3433, "country": "IN",
     "coastal": False, "region": "Marathwada"},
    {"city": "Nagpur", "lat": 21.1458, "lon": 79.0882, "country": "IN", "coastal": False,
     "region": "Vidarbha"},
]

# Maharashtra (Sindhudurg -> Nandurbar, Arabian Sea coast -> Gadchiroli)
MAHARASHTRA_BBOX = {"lat_min": 15.60, "lat_max": 22.03, "lon_min": 72.65, "lon_max": 80.89}

# Normalised schema shared by every provider. Missing = None (rendered as "—").
FIELDS = [
    # thermal
    "temp_min_c", "temp_max_c", "feels_like_max_c", "dew_point_c",
    "temp_now_c",  # live station reading (observation providers only, e.g. Weather Union)
    # precipitation
    "precip_mm", "precip_prob_pct", "precip_hours", "precip_type",
    # wind  (wind_basis: "max" = daily max sustained, "avg" = daily mean)
    "wind_speed_kph", "wind_basis", "wind_gust_kph", "wind_dir_deg",
    # solar
    "uv_index", "sunshine_hours", "solar_radiation_wm2",
    # sky & atmosphere
    "cloud_cover_pct", "humidity_pct", "pressure_hpa", "visibility_km",
    # marine
    "wave_height_m", "swell_height_m", "sea_surface_temp_c",
    # astronomy
    "sunrise", "sunset", "moon_phase", "moon_illumination_pct",
]


# ==============================================================================
# HELPERS
# ==============================================================================

def make_record(provider, model_source, loc, forecast_date, raw_payload=None,
                coverage_hours=None, flags=None, **fields) -> dict:
    unknown = set(fields) - set(FIELDS)
    if unknown:
        raise ValueError(f"Unknown fields for {provider}: {unknown}")
    rec = {
        "provider": provider,
        "model_source": model_source,
        "city": loc["city"],
        "latitude": loc["lat"],
        "longitude": loc["lon"],
        "forecast_date": forecast_date,
    }
    for f in FIELDS:
        v = fields.get(f)
        if isinstance(v, float):
            v = round(v, 2)
        rec[f] = v
    # No derived values: a field the provider does not supply stays None (rendered "—").
    if rec["wind_speed_kph"] is not None and rec["wind_basis"] is None:
        rec["wind_basis"] = "max"
    rec["coverage_hours"] = coverage_hours  # hours of the local day actually covered
    rec["flags"] = flags or []
    rec["fetched_at"] = datetime.now(timezone.utc).isoformat()
    rec["raw_payload"] = raw_payload or {}
    return rec


def ms_to_kph(v):
    return v * 3.6 if v is not None else None


def num(v):
    """Coerce to float, treating NASA-style fill values as missing."""
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if f <= -999 or math.isnan(f) else f


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def vmax(xs):
    xs = [x for x in xs if x is not None]
    return max(xs) if xs else None


def vmin(xs):
    xs = [x for x in xs if x is not None]
    return min(xs) if xs else None


def vsum(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) if xs else None


def dir_mean(degs):
    """Circular mean of wind directions (degrees)."""
    degs = [d for d in degs if d is not None]
    if not degs:
        return None
    s = sum(math.sin(math.radians(d)) for d in degs)
    c = sum(math.cos(math.radians(d)) for d in degs)
    return (math.degrees(math.atan2(s, c)) + 360) % 360


def uv_to_speed_dir(u, v):
    """Wind u/v components (m/s) -> speed (km/h), meteorological 'from' direction."""
    if u is None or v is None:
        return None, None
    return math.hypot(u, v) * 3.6, (math.degrees(math.atan2(-u, -v)) + 360) % 360


def to_local(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(LOCAL_TZ)


def parse_iso(s: str) -> datetime:
    s = re.sub(r"(\.\d{6})\d+", r"\1", s)  # trim nanoseconds, e.g. Google: 01:14:36.938427269Z
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def local_date_from_epoch(ts) -> str:
    return to_local(datetime.fromtimestamp(ts, tz=timezone.utc)).date().isoformat()


def hhmm(value):
    """Normalise many time formats to local 'HH:MM'."""
    if value in (None, "", 0):
        return None
    if isinstance(value, (int, float)):
        return to_local(datetime.fromtimestamp(value, tz=timezone.utc)).strftime("%H:%M")
    s = str(value).strip()
    for fmt in ("%I:%M %p", "%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(s, fmt).strftime("%H:%M")
        except ValueError:
            pass
    try:
        dt = parse_iso(s)
        return (to_local(dt) if dt.tzinfo else dt).strftime("%H:%M")
    except ValueError:
        return None


MOON_NAMES = ["New Moon", "Waxing Crescent", "First Quarter", "Waxing Gibbous",
              "Full Moon", "Waning Gibbous", "Last Quarter", "Waning Crescent"]


def moon_from_lunation(frac):
    """Lunation fraction (0=new, 0.5=full) -> (phase name, illumination %)."""
    if frac is None:
        return None, None
    frac = frac % 1.0
    name = MOON_NAMES[int((frac * 8) + 0.5) % 8]
    illum = (1 - math.cos(2 * math.pi * frac)) / 2 * 100
    return name, round(illum)


def split_camel(s):
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s) if s else s


_SECRET_RE = re.compile(r"((?:key|apikey|appid|token|client_id|client_secret|secret)=)[^&\s'\"]+", re.I)


def redact(msg: str) -> str:
    """Strip API keys out of URLs that httpx puts into error messages."""
    return _SECRET_RE.sub(r"\1***", str(msg))


class HourlyBuckets:
    """Collects hourly/3-hourly samples per local date, then reduces them."""

    def __init__(self):
        self.days = defaultdict(lambda: defaultdict(list))

    def add(self, local_dt: datetime, step_hours: float, **values):
        b = self.days[local_dt.date().isoformat()]
        b["_hours"].append(step_hours)
        for k, v in values.items():
            b[k].append(v)

    def items(self):
        return sorted(self.days.items())


def coverage(b):
    return min(24, round(sum(b["_hours"])))


# ==============================================================================
# TIER 1: KEYLESS / OPEN ENDPOINTS
# ==============================================================================

OPEN_METEO_MODELS = {
    "best_match": ("Open-Meteo", "Best Match"),
    "ecmwf_ifs025": ("ECMWF", "IFS 0.25°"),
    "gfs_seamless": ("Open-Meteo", "GFS (NOAA)"),
    "icon_seamless": ("Open-Meteo", "ICON (DWD)"),
    "jma_seamless": ("Open-Meteo", "JMA"),
    "gem_seamless": ("Open-Meteo", "GEM (Canada)"),
}


async def fetch_open_meteo_and_ecmwf(client: httpx.AsyncClient, loc: dict) -> list[dict]:
    daily_vars = [
        "temperature_2m_max", "temperature_2m_min", "apparent_temperature_max",
        "precipitation_sum", "precipitation_hours", "precipitation_probability_max",
        "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant",
        "uv_index_max", "sunshine_duration", "shortwave_radiation_sum", "sunrise", "sunset",
    ]
    hourly_vars = ["relative_humidity_2m", "dew_point_2m", "cloud_cover", "pressure_msl", "visibility"]
    params = {
        "latitude": loc["lat"], "longitude": loc["lon"],
        "daily": ",".join(daily_vars), "hourly": ",".join(hourly_vars),
        "models": ",".join(OPEN_METEO_MODELS), "timezone": TZ_NAME, "forecast_days": 7,
    }
    resp = await client.get("https://api.open-meteo.com/v1/forecast", params=params)
    resp.raise_for_status()
    data = resp.json()
    daily, hourly = data.get("daily", {}), data.get("hourly", {})
    dates, hours = daily.get("time", []), hourly.get("time", [])

    def series(block, var, model, n):
        # With several models requested, EVERY model (best_match included) gets a suffix.
        return block.get(f"{var}_{model}") or block.get(var) or [None] * n

    records = []
    for model, (provider, label) in OPEN_METEO_MODELS.items():
        d = {v: series(daily, v, model, len(dates)) for v in daily_vars}
        h = {v: series(hourly, v, model, len(hours)) for v in hourly_vars}
        by_day = defaultdict(lambda: defaultdict(list))
        for i, t in enumerate(hours):
            for v in hourly_vars:
                by_day[t[:10]][v].append(h[v][i])
        for i, date in enumerate(dates):
            if d["temperature_2m_max"][i] is None:
                continue
            hb = by_day[date]
            sw = d["shortwave_radiation_sum"][i]
            sun_s = d["sunshine_duration"][i]
            vis = mean(hb["visibility"])
            records.append(make_record(
                provider, label, loc, date,
                raw_payload={"model": model},
                coverage_hours=24,
                temp_min_c=d["temperature_2m_min"][i],
                temp_max_c=d["temperature_2m_max"][i],
                feels_like_max_c=d["apparent_temperature_max"][i],
                dew_point_c=mean(hb["dew_point_2m"]),
                precip_mm=d["precipitation_sum"][i],
                precip_prob_pct=d["precipitation_probability_max"][i],
                precip_hours=d["precipitation_hours"][i],
                wind_speed_kph=d["wind_speed_10m_max"][i],
                wind_gust_kph=d["wind_gusts_10m_max"][i],
                wind_dir_deg=d["wind_direction_10m_dominant"][i],
                uv_index=d["uv_index_max"][i],
                sunshine_hours=sun_s / 3600 if sun_s is not None else None,
                solar_radiation_wm2=sw * 1e6 / 86400 if sw is not None else None,  # MJ/m²/day -> mean W/m²
                cloud_cover_pct=mean(hb["cloud_cover"]),
                humidity_pct=mean(hb["relative_humidity_2m"]),
                pressure_hpa=mean(hb["pressure_msl"]),
                visibility_km=vis / 1000 if vis is not None else None,
                sunrise=hhmm(d["sunrise"][i]),
                sunset=hhmm(d["sunset"][i]),
            ))
    return records


async def fetch_met_norway(client: httpx.AsyncClient, loc: dict) -> list[dict]:
    url = "https://api.met.no/weatherapi/locationforecast/2.0/complete"
    params = {"lat": round(loc["lat"], 4), "lon": round(loc["lon"], 4)}
    resp = await client.get(url, params=params)  # shared client sends the User-Agent + contact MET requires
    resp.raise_for_status()

    bk = HourlyBuckets()
    for entry in resp.json().get("properties", {}).get("timeseries", []):
        inst = entry["data"]["instant"]["details"]
        n1 = entry["data"].get("next_1_hours", {}).get("details")
        n6 = entry["data"].get("next_6_hours", {}).get("details")
        nxt, step = (n1, 1) if n1 is not None else (n6 or {}, 6)
        precip = nxt.get("precipitation_amount")
        bk.add(
            to_local(parse_iso(entry["time"])), step,
            temp=inst.get("air_temperature"), dew=inst.get("dew_point_temperature"),
            rh=inst.get("relative_humidity"), cloud=inst.get("cloud_area_fraction"),
            pres=inst.get("air_pressure_at_sea_level"), wdir=inst.get("wind_from_direction"),
            wind=ms_to_kph(inst.get("wind_speed")), gust=ms_to_kph(inst.get("wind_speed_of_gust")),
            uv=inst.get("ultraviolet_index_clear_sky"), precip=precip,
            pop=nxt.get("probability_of_precipitation"),
            wet_hours=step if (precip or 0) >= 0.1 else 0,
        )

    records = []
    for d, b in bk.items()[:7]:
        if not any(t is not None for t in b["temp"]):
            continue
        records.append(make_record(
            "MET Norway", "Locationforecast 2.0", loc, d,
            raw_payload={"samples": len(b["temp"])}, coverage_hours=coverage(b),
            temp_min_c=vmin(b["temp"]), temp_max_c=vmax(b["temp"]),
            dew_point_c=mean(b["dew"]), precip_mm=vsum(b["precip"]),
            precip_prob_pct=vmax(b["pop"]), precip_hours=vsum(b["wet_hours"]),
            wind_speed_kph=vmax(b["wind"]), wind_gust_kph=vmax(b["gust"]),
            wind_dir_deg=dir_mean(b["wdir"]), uv_index=vmax(b["uv"]),
            cloud_cover_pct=mean(b["cloud"]), humidity_pct=mean(b["rh"]), pressure_hpa=mean(b["pres"]),
        ))
    return records


async def fetch_dwd_brightsky(client: httpx.AsyncClient, loc: dict) -> list[dict]:
    today = datetime.now(LOCAL_TZ).date()
    params = {
        "lat": loc["lat"], "lon": loc["lon"], "tz": TZ_NAME,
        "date": today.isoformat(), "last_date": (today + timedelta(days=6)).isoformat(),
    }
    resp = await client.get("https://api.brightsky.dev/weather", params=params)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()

    bk = HourlyBuckets()
    for r in resp.json().get("weather", []):
        p = r.get("precipitation")
        bk.add(
            to_local(parse_iso(r["timestamp"])), 1,
            temp=r.get("temperature"), dew=r.get("dew_point"), rh=r.get("relative_humidity"),
            precip=p, pop=r.get("precipitation_probability"), wet=1 if (p or 0) >= 0.1 else 0,
            wind=r.get("wind_speed"), gust=r.get("wind_gust_speed"), wdir=r.get("wind_direction"),
            cloud=r.get("cloud_cover"), pres=r.get("pressure_msl"), vis=r.get("visibility"),
            sun=r.get("sunshine"), solar=r.get("solar"),
        )

    records = []
    for d, b in bk.items():
        if not any(t is not None for t in b["temp"]):
            continue
        sun_min, solar_kwh, vis = vsum(b["sun"]), vsum(b["solar"]), mean(b["vis"])
        records.append(make_record(
            "DWD Bright Sky", "MOSMIX", loc, d,
            raw_payload={"samples": len(b["temp"])}, coverage_hours=coverage(b),
            temp_min_c=vmin(b["temp"]), temp_max_c=vmax(b["temp"]), dew_point_c=mean(b["dew"]),
            precip_mm=vsum(b["precip"]), precip_prob_pct=vmax(b["pop"]),
            precip_hours=vsum(b["wet"]) if any(p is not None for p in b["precip"]) else None,
            wind_speed_kph=vmax(b["wind"]), wind_gust_kph=vmax(b["gust"]), wind_dir_deg=dir_mean(b["wdir"]),
            sunshine_hours=sun_min / 60 if sun_min is not None else None,
            solar_radiation_wm2=solar_kwh * 1000 / 24 if solar_kwh is not None else None,
            cloud_cover_pct=mean(b["cloud"]), humidity_pct=mean(b["rh"]), pressure_hpa=mean(b["pres"]),
            visibility_km=vis / 1000 if vis is not None else None,
        ))
    return records


async def fetch_nasa_power(client: httpx.AsyncClient, loc: dict) -> list[dict]:
    """Satellite/reanalysis, lags ~3+ days: covers recent past, never 'today'."""
    end_dt = datetime.now(timezone.utc).date() - timedelta(days=3)
    start_dt = end_dt - timedelta(days=4)
    params = {
        "parameters": "T2M_MIN,T2M_MAX,PRECTOTCORR,WS10M_MAX,RH2M,T2MDEW,ALLSKY_SFC_SW_DWN,CLOUD_AMT",
        "community": "AG", "longitude": loc["lon"], "latitude": loc["lat"],
        "start": start_dt.strftime("%Y%m%d"), "end": end_dt.strftime("%Y%m%d"), "format": "JSON",
    }
    resp = await client.get("https://power.larc.nasa.gov/api/temporal/daily/point", params=params)
    resp.raise_for_status()
    p = resp.json().get("properties", {}).get("parameter", {})
    g = lambda var, day: num(p.get(var, {}).get(day))

    records = []
    for day in p.get("T2M_MIN", {}):
        if g("T2M_MIN", day) is None:
            continue
        sw = g("ALLSKY_SFC_SW_DWN", day)
        records.append(make_record(
            "NASA POWER", "GEOS / CERES (reanalysis)", loc, f"{day[:4]}-{day[4:6]}-{day[6:]}",
            raw_payload={"yyyymmdd": day}, coverage_hours=24, flags=["reanalysis"],
            temp_min_c=g("T2M_MIN", day), temp_max_c=g("T2M_MAX", day), dew_point_c=g("T2MDEW", day),
            precip_mm=g("PRECTOTCORR", day), wind_speed_kph=ms_to_kph(g("WS10M_MAX", day)),
            humidity_pct=g("RH2M", day), cloud_cover_pct=g("CLOUD_AMT", day),
            solar_radiation_wm2=sw * 1000 / 24 if sw is not None else None,  # kWh/m²/day -> mean W/m²
        ))
    return records


async def fetch_noaa_nws(client: httpx.AsyncClient, loc: dict) -> list[dict]:
    if loc.get("country") != "US":
        return []
    headers = {}
    p_resp = await client.get(
        f"https://api.weather.gov/points/{round(loc['lat'], 4)},{round(loc['lon'], 4)}", headers=headers)
    p_resp.raise_for_status()
    f_resp = await client.get(p_resp.json()["properties"]["forecast"], headers=headers)
    f_resp.raise_for_status()
    records = []
    for p in f_resp.json().get("properties", {}).get("periods", []):
        if not p.get("isDaytime"):
            continue
        temp_f = p.get("temperature")
        records.append(make_record(
            "NOAA / NWS", "NDFD / NCEP", loc, p["startTime"][:10], raw_payload=p,
            temp_max_c=(temp_f - 32) * 5.0 / 9.0 if temp_f is not None else None,
        ))
    return records


async def fetch_eccc_canada(client: httpx.AsyncClient, loc: dict) -> list[dict]:
    if loc.get("country") != "CA":
        return []
    url = "https://api.weather.gc.ca/collections/citypageweather-realtime/items"
    params = {"f": "json", "limit": 1,
              "bbox": f"{loc['lon']-0.5},{loc['lat']-0.5},{loc['lon']+0.5},{loc['lat']+0.5}"}
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    features = resp.json().get("features", [])
    if not features:
        return []
    props = features[0].get("properties", {})
    curr = props.get("currentConditions", {})
    return [make_record(
        "ECCC (Canada)", "MSC GeoMet", loc, datetime.now(timezone.utc).date().isoformat(),
        raw_payload={"station": props.get("name", {}).get("en")},
        temp_max_c=curr.get("temperature", {}).get("value", {}).get("en"),
        humidity_pct=curr.get("relativeHumidity", {}).get("value", {}).get("en"),
    )]


async def fetch_marine(client: httpx.AsyncClient, loc: dict) -> list[dict]:
    """Open-Meteo Marine API (wave models). Previously mislabelled 'INCOIS'."""
    if not loc.get("coastal"):
        return []
    lat, lon = loc.get("marine", (loc["lat"], loc["lon"]))
    params = {
        "latitude": lat, "longitude": lon, "timezone": TZ_NAME, "forecast_days": 7,
        "daily": "wave_height_max,swell_wave_height_max", "hourly": "sea_surface_temperature",
    }
    resp = await client.get("https://marine-api.open-meteo.com/v1/marine", params=params)
    resp.raise_for_status()
    data = resp.json()
    daily, hourly = data.get("daily", {}), data.get("hourly", {})
    sst = defaultdict(list)
    for t, v in zip(hourly.get("time", []), hourly.get("sea_surface_temperature", [])):
        sst[t[:10]].append(v)
    dates = daily.get("time", [])
    waves = daily.get("wave_height_max", [None] * len(dates))
    swell = daily.get("swell_wave_height_max", [None] * len(dates))
    return [
        make_record("Open-Meteo Marine", "Wave & Swell Model", loc, d, coverage_hours=24,
                    wave_height_m=waves[i], swell_height_m=swell[i], sea_surface_temp_c=mean(sst[d]))
        for i, d in enumerate(dates)
        if waves[i] is not None or swell[i] is not None
    ]


# ==============================================================================
# TIER 2: COMMERCIAL / KEYED APIs
# ==============================================================================

async def fetch_weatherapi(client: httpx.AsyncClient, loc: dict, key: str) -> list[dict]:
    params = {"key": key, "q": f"{loc['lat']},{loc['lon']}", "days": 7, "aqi": "no", "alerts": "no"}
    resp = await client.get("https://api.weatherapi.com/v1/forecast.json", params=params)
    resp.raise_for_status()
    records = []
    for fd in resp.json().get("forecast", {}).get("forecastday", []):
        d, astro, hrs = fd["day"], fd.get("astro", {}), fd.get("hour", [])
        col = lambda k: [h.get(k) for h in hrs]
        records.append(make_record(
            "WeatherAPI.com", "WeatherAPI Blend", loc, fd["date"],
            raw_payload={"day": d, "astro": astro}, coverage_hours=24,
            temp_min_c=d.get("mintemp_c"), temp_max_c=d.get("maxtemp_c"),
            feels_like_max_c=vmax(col("feelslike_c")), dew_point_c=mean(col("dewpoint_c")),
            precip_mm=d.get("totalprecip_mm"), precip_prob_pct=d.get("daily_chance_of_rain"),
            # provider's own yes/no flags
            precip_type=("Snow" if d.get("daily_will_it_snow") else "Rain" if d.get("daily_will_it_rain")
                         else "Dry") if "daily_will_it_rain" in d else None,
            precip_hours=sum(1 for h in hrs if (h.get("precip_mm") or 0) >= 0.1) if hrs else None,
            wind_speed_kph=d.get("maxwind_kph"), wind_gust_kph=vmax(col("gust_kph")),
            wind_dir_deg=dir_mean(col("wind_degree")), uv_index=d.get("uv"),
            solar_radiation_wm2=mean(col("short_rad")), cloud_cover_pct=mean(col("cloud")),
            humidity_pct=d.get("avghumidity"), pressure_hpa=mean(col("pressure_mb")),
            visibility_km=d.get("avgvis_km"), sunrise=hhmm(astro.get("sunrise")),
            sunset=hhmm(astro.get("sunset")), moon_phase=astro.get("moon_phase"),
            moon_illumination_pct=num(astro.get("moon_illumination")),
        ))
    return records


async def fetch_openweather(client: httpx.AsyncClient, loc: dict, key: str) -> list[dict]:
    params = {"lat": loc["lat"], "lon": loc["lon"], "appid": key, "units": "metric"}
    resp = await client.get("https://api.openweathermap.org/data/2.5/forecast", params=params)
    resp.raise_for_status()

    bk = HourlyBuckets()
    for it in resp.json().get("list", []):
        m, w = it.get("main", {}), it.get("wind", {})
        rain = it.get("rain", {}).get("3h", 0.0) + it.get("snow", {}).get("3h", 0.0)
        pop = it.get("pop")
        bk.add(
            to_local(datetime.fromtimestamp(it["dt"], tz=timezone.utc)), 3,
            tmin=m.get("temp_min"), tmax=m.get("temp_max"), feels=m.get("feels_like"),
            rh=m.get("humidity"), pres=m.get("sea_level") or m.get("pressure"),
            cloud=it.get("clouds", {}).get("all"), vis=it.get("visibility"),
            wind=ms_to_kph(w.get("speed")), gust=ms_to_kph(w.get("gust")), wdir=w.get("deg"),
            precip=rain, wet=3 if rain >= 0.1 else 0, pop=pop * 100 if pop is not None else None,
        )

    records = []
    for d, b in bk.items():
        vis = mean(b["vis"])
        records.append(make_record(
            "OpenWeather", "5-Day / 3-Hour (v2.5)", loc, d,
            raw_payload={"blocks": len(b["tmin"])}, coverage_hours=coverage(b),
            temp_min_c=vmin(b["tmin"]), temp_max_c=vmax(b["tmax"]), feels_like_max_c=vmax(b["feels"]),
            precip_mm=vsum(b["precip"]), precip_prob_pct=vmax(b["pop"]), precip_hours=vsum(b["wet"]),
            wind_speed_kph=vmax(b["wind"]), wind_gust_kph=vmax(b["gust"]), wind_dir_deg=dir_mean(b["wdir"]),
            cloud_cover_pct=mean(b["cloud"]), humidity_pct=mean(b["rh"]), pressure_hpa=mean(b["pres"]),
            visibility_km=vis / 1000 if vis is not None else None,
        ))
    return records


async def fetch_visual_crossing(client: httpx.AsyncClient, loc: dict, key: str) -> list[dict]:
    # Visual Crossing bills per DAY of data returned (default = 15 days), so ask for exactly 7.
    start = datetime.now(LOCAL_TZ).date()
    url = ("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
           f"{loc['lat']},{loc['lon']}/{start.isoformat()}/{(start + timedelta(days=6)).isoformat()}")
    params = {"unitGroup": "metric", "include": "days", "key": key, "contentType": "json"}
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    records = []
    for d in resp.json().get("days", [])[:7]:
        phase, _ = moon_from_lunation(d.get("moonphase"))  # name only; VC gives no illumination
        cover = d.get("precipcover")
        ptype = d.get("preciptype")
        records.append(make_record(
            "Visual Crossing", "Timeline API", loc, d["datetime"], raw_payload=d, coverage_hours=24,
            temp_min_c=d.get("tempmin"), temp_max_c=d.get("tempmax"), feels_like_max_c=d.get("feelslikemax"),
            dew_point_c=d.get("dew"), precip_mm=d.get("precip"), precip_prob_pct=d.get("precipprob"),
            precip_hours=cover * 24 / 100 if cover is not None else None,
            precip_type=", ".join(p.title() for p in ptype) if ptype else None,
            wind_speed_kph=d.get("windspeed"), wind_gust_kph=d.get("windgust"), wind_dir_deg=d.get("winddir"),
            uv_index=d.get("uvindex"), solar_radiation_wm2=d.get("solarradiation"),
            cloud_cover_pct=d.get("cloudcover"), humidity_pct=d.get("humidity"),
            pressure_hpa=d.get("pressure"), visibility_km=d.get("visibility"),
            sunrise=hhmm(d.get("sunrise")), sunset=hhmm(d.get("sunset")),
            moon_phase=phase,
        ))
    return records


async def fetch_tomorrow_io(client: httpx.AsyncClient, loc: dict, key: str) -> list[dict]:
    params = {"location": f"{loc['lat']},{loc['lon']}", "timesteps": "1d", "units": "metric", "apikey": key}
    resp = await client.get("https://api.tomorrow.io/v4/weather/forecast", params=params)
    resp.raise_for_status()
    records = []
    for item in resp.json().get("timelines", {}).get("daily", []):
        v = item.get("values", {})
        records.append(make_record(
            "Tomorrow.io", "Tomorrow NWP", loc, to_local(parse_iso(item["time"])).date().isoformat(),
            raw_payload=item, coverage_hours=24,
            temp_min_c=v.get("temperatureMin"), temp_max_c=v.get("temperatureMax"),
            feels_like_max_c=v.get("temperatureApparentMax"), dew_point_c=v.get("dewPointAvg"),
            # rainAccumulationSum is the daily total; precipitationIntensityAvg is a rate (mm/h).
            precip_mm=v.get("rainAccumulationSum"), precip_prob_pct=v.get("precipitationProbabilityMax"),
            wind_speed_kph=ms_to_kph(v.get("windSpeedMax")), wind_gust_kph=ms_to_kph(v.get("windGustMax")),
            # windDirectionAvg is an arithmetic mean of angles (350° & 10° -> 180°), so it is
            # not used; a circular mean would need the hourly timeline (extra quota).
            uv_index=v.get("uvIndexMax"),
            cloud_cover_pct=v.get("cloudCoverAvg"), humidity_pct=v.get("humidityAvg"),
            pressure_hpa=v.get("pressureSeaLevelAvg"), visibility_km=v.get("visibilityAvg"),
            sunrise=hhmm(v.get("sunriseTime")), sunset=hhmm(v.get("sunsetTime")),
        ))
    return records


async def fetch_weatherbit(client: httpx.AsyncClient, loc: dict, key: str) -> list[dict]:
    params = {"lat": loc["lat"], "lon": loc["lon"], "days": 7, "key": key}
    resp = await client.get("https://api.weatherbit.io/v2.0/forecast/daily", params=params)
    resp.raise_for_status()
    records = []
    for d in resp.json().get("data", []):
        phase, _ = moon_from_lunation(d.get("moon_phase_lunation"))
        mp = d.get("moon_phase")
        records.append(make_record(
            "Weatherbit", "Weatherbit Multi-Model", loc, d["valid_date"], raw_payload=d, coverage_hours=24,
            temp_min_c=d.get("min_temp"), temp_max_c=d.get("max_temp"), feels_like_max_c=d.get("app_max_temp"),
            dew_point_c=d.get("dewpt"), precip_mm=d.get("precip"), precip_prob_pct=d.get("pop"),
            wind_speed_kph=ms_to_kph(d.get("wind_spd")), wind_basis="avg",
            wind_gust_kph=ms_to_kph(d.get("wind_gust_spd")), wind_dir_deg=d.get("wind_dir"),
            uv_index=d.get("uv"), solar_radiation_wm2=d.get("solar_rad"),
            cloud_cover_pct=d.get("clouds"), humidity_pct=d.get("rh"),
            pressure_hpa=d.get("slp") or d.get("pres"), visibility_km=d.get("vis"),
            sunrise=hhmm(d.get("sunrise_ts")), sunset=hhmm(d.get("sunset_ts")),
            moon_phase=phase, moon_illumination_pct=round(mp * 100) if mp is not None else None,
        ))
    return records


async def fetch_meteosource(client: httpx.AsyncClient, loc: dict, key: str) -> list[dict]:
    params = {"lat": loc["lat"], "lon": loc["lon"], "sections": "daily", "units": "metric", "key": key}
    resp = await client.get("https://www.meteosource.com/api/v1/free/point", params=params)
    resp.raise_for_status()
    records = []
    for d in resp.json().get("daily", {}).get("data", []):
        a = d.get("all_day", {})
        ptype = (a.get("precipitation") or {}).get("type")
        records.append(make_record(
            "Meteosource", "Meteosource AI/NWP", loc, d["day"], raw_payload=d, coverage_hours=24,
            temp_min_c=a.get("temperature_min"), temp_max_c=a.get("temperature_max"),
            precip_mm=(a.get("precipitation") or {}).get("total"),
            precip_type=("Dry" if ptype == "none" else ptype.title()) if ptype else None,
            wind_speed_kph=ms_to_kph((a.get("wind") or {}).get("speed")), wind_basis="avg",
            wind_dir_deg=(a.get("wind") or {}).get("angle"),
            cloud_cover_pct=(a.get("cloud_cover") or {}).get("total"),
        ))
    return records


# Known AccuWeather location keys for our configured city centers so we match the website
ACCU_CITY_KEYS = {
    "Mumbai": "204842",
    "Pune": "204848",
    "Nagpur": "204844",
    "Nashik": "189304",
    "Ratnagiri": "189289",
    "Kolhapur": "189317",
    "Chhatrapati Sambhajinagar": "189320",
}


def _load_accu_keys() -> dict:
    try:
        return json.loads(ACCU_KEY_CACHE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


async def fetch_accuweather(client: httpx.AsyncClient, loc: dict, key: str) -> list[dict]:
    # 1. Use explicit city key first (guarantees matching the website URL), else cached key, else reverse-geocode
    loc_key = ACCU_CITY_KEYS.get(loc.get("city"))
    cache = _load_accu_keys()
    ck = f"{loc['lat']},{loc['lon']}"
    if not loc_key:
        loc_key = cache.get(ck)
    if not loc_key:
        geo = await client.get("https://dataservice.accuweather.com/locations/v1/cities/geoposition/search",
                               params={"apikey": key, "q": ck})
        geo.raise_for_status()
        loc_key = geo.json().get("Key")
        if not loc_key:
            return []
        cache[ck] = loc_key
        ACCU_KEY_CACHE.parent.mkdir(exist_ok=True)
        ACCU_KEY_CACHE.write_text(json.dumps(cache, indent=2), encoding="utf-8")

    fc = await client.get(f"https://dataservice.accuweather.com/forecasts/v1/daily/5day/{loc_key}",
                          params={"apikey": key, "metric": "true", "details": "true"})
    fc.raise_for_status()
    records = []
    for d in fc.json().get("DailyForecasts", []):
        day, night = d.get("Day", {}), d.get("Night", {})
        both = lambda f: [f(day), f(night)]
        uv = next((x.get("Value") for x in d.get("AirAndPollen", []) if x.get("Name") == "UVIndex"), None)
        moon = d.get("Moon", {})
        records.append(make_record(
            "AccuWeather", "AccuWeather Daily", loc, d["Date"][:10], raw_payload=d, coverage_hours=24,
            temp_min_c=d.get("Temperature", {}).get("Minimum", {}).get("Value"),
            temp_max_c=d.get("Temperature", {}).get("Maximum", {}).get("Value"),
            feels_like_max_c=d.get("RealFeelTemperature", {}).get("Maximum", {}).get("Value"),
            precip_mm=vsum(both(lambda p: p.get("TotalLiquid", {}).get("Value"))),
            precip_prob_pct=vmax(both(lambda p: p.get("PrecipitationProbability"))),
            precip_hours=vsum(both(lambda p: p.get("HoursOfPrecipitation"))),
            precip_type=day.get("PrecipitationType") if day.get("HasPrecipitation") else None,
            wind_speed_kph=vmax(both(lambda p: p.get("Wind", {}).get("Speed", {}).get("Value"))),
            wind_gust_kph=vmax(both(lambda p: p.get("WindGust", {}).get("Speed", {}).get("Value"))),
            wind_dir_deg=day.get("Wind", {}).get("Direction", {}).get("Degrees"),
            uv_index=uv if uv is not None else day.get("UVIndexFloat"),
            sunshine_hours=d.get("HoursOfSun"),
            cloud_cover_pct=mean(both(lambda p: p.get("CloudCover"))),
            humidity_pct=day.get("RelativeHumidity", {}).get("Average"),
            sunrise=hhmm(d.get("Sun", {}).get("Rise")), sunset=hhmm(d.get("Sun", {}).get("Set")),
            moon_phase=split_camel(moon.get("Phase")),
        ))
    return records


async def fetch_meteoblue(client: httpx.AsyncClient, loc: dict, key: str) -> list[dict]:
    params = {"lat": loc["lat"], "lon": loc["lon"], "apikey": key, "format": "json", "tz": TZ_NAME}
    resp = await client.get("https://my.meteoblue.com/packages/basic-day", params=params)
    resp.raise_for_status()
    dd = resp.json().get("data_day", {})
    dates = dd.get("time", [])
    col = lambda k, i: (dd.get(k) or [None] * len(dates))[i]
    records = []
    for i, d in enumerate(dates):
        records.append(make_record(
            "Meteoblue", "mLM Multi-Model", loc, d,
            raw_payload={"predictability_pct": col("predictability", i)}, coverage_hours=24,
            temp_min_c=col("temperature_min", i), temp_max_c=col("temperature_max", i),
            feels_like_max_c=col("felttemperature_max", i), precip_mm=col("precipitation", i),
            precip_prob_pct=col("precipitation_probability", i), precip_hours=col("precipitation_hours", i),
            wind_speed_kph=ms_to_kph(col("windspeed_max", i)), wind_dir_deg=col("winddirection", i),
            uv_index=col("uvindex", i), humidity_pct=col("relativehumidity_mean", i),
            pressure_hpa=col("sealevelpressure_mean", i),
        ))
    return records


async def fetch_windy(client: httpx.AsyncClient, loc: dict, key: str) -> list[dict]:
    payload = {
        "lat": loc["lat"], "lon": loc["lon"], "model": "gfs", "levels": ["surface"], "key": key,
        "parameters": ["temp", "dewpoint", "precip", "wind", "windGust", "rh", "lclouds", "mclouds", "hclouds"],
    }
    resp = await client.post("https://api.windy.com/api/point-forecast/v2", json=payload)
    resp.raise_for_status()
    data = resp.json()
    s = lambda k, i: (data.get(k) or [])[i] if i < len(data.get(k) or []) else None
    k2c = lambda v: v - 273.15 if v is not None else None

    bk = HourlyBuckets()
    for i, ts in enumerate(data.get("ts", [])):
        speed, wdir = uv_to_speed_dir(s("wind_u-surface", i), s("wind_v-surface", i))
        precip = s("past3hprecip-surface", i)
        clouds = [s(f"{c}-surface", i) for c in ("lclouds", "mclouds", "hclouds")]
        bk.add(
            to_local(datetime.fromtimestamp(ts / 1000, tz=timezone.utc)), 3,
            temp=k2c(s("temp-surface", i)), dew=k2c(s("dewpoint-surface", i)), rh=s("rh-surface", i),
            precip=precip * 1000 if precip is not None else None,  # m -> mm
            wind=speed, wdir=wdir, gust=ms_to_kph(s("gust-surface", i)), cloud=vmax(clouds),
        )

    # Windy's free "trial" keys return deliberately shuffled data -> flagged, excluded from consensus.
    trial = os.getenv("WINDY_TRIAL", "1").strip() != "0"
    records = []
    for d, b in bk.items()[:7]:
        if not any(t is not None for t in b["temp"]):
            continue
        records.append(make_record(
            "Windy", "Point Forecast (GFS)", loc, d, raw_payload={"steps": len(b["temp"])},
            coverage_hours=coverage(b), flags=["trial"] if trial else [],
            temp_min_c=vmin(b["temp"]), temp_max_c=vmax(b["temp"]), dew_point_c=mean(b["dew"]),
            precip_mm=vsum(b["precip"]), wind_speed_kph=vmax(b["wind"]), wind_gust_kph=vmax(b["gust"]),
            wind_dir_deg=dir_mean(b["wdir"]), cloud_cover_pct=mean(b["cloud"]), humidity_pct=mean(b["rh"]),
        ))
    return records


async def fetch_stormglass(client: httpx.AsyncClient, loc: dict, key: str) -> list[dict]:
    if not loc.get("coastal"):
        return []  # free tier is 10 requests/day -> spend them on coastal cities only
    start = datetime.now(LOCAL_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    params = {
        "lat": loc["lat"], "lng": loc["lon"],
        "start": int(start.timestamp()), "end": int((start + timedelta(days=7)).timestamp()),
        "params": "airTemperature,precipitation,windSpeed,windDirection,gust,humidity,pressure,"
                  "cloudCover,visibility,waveHeight,swellHeight,waterTemperature",
    }
    resp = await client.get("https://api.stormglass.io/v2/weather/point", params=params,
                            headers={"Authorization": key})
    resp.raise_for_status()

    def pick(h, k):
        v = h.get(k) or {}
        return v.get("sg", next(iter(v.values()), None)) if v else None

    bk = HourlyBuckets()
    for h in resp.json().get("hours", []):
        p = pick(h, "precipitation")
        bk.add(
            to_local(parse_iso(h["time"])), 1,
            temp=pick(h, "airTemperature"), precip=p, wet=1 if (p or 0) >= 0.1 else 0,
            wind=ms_to_kph(pick(h, "windSpeed")), gust=ms_to_kph(pick(h, "gust")),
            wdir=pick(h, "windDirection"), rh=pick(h, "humidity"), pres=pick(h, "pressure"),
            cloud=pick(h, "cloudCover"), vis=pick(h, "visibility"), wave=pick(h, "waveHeight"),
            swell=pick(h, "swellHeight"), sst=pick(h, "waterTemperature"),
        )

    records = []
    for d, b in bk.items()[:7]:
        if not any(t is not None for t in b["temp"]):
            continue
        records.append(make_record(
            "Stormglass", "Stormglass Blend (sg)", loc, d, raw_payload={"hours": len(b["temp"])},
            coverage_hours=coverage(b),
            temp_min_c=vmin(b["temp"]), temp_max_c=vmax(b["temp"]), precip_mm=vsum(b["precip"]),
            precip_hours=vsum(b["wet"]), wind_speed_kph=vmax(b["wind"]), wind_gust_kph=vmax(b["gust"]),
            wind_dir_deg=dir_mean(b["wdir"]), cloud_cover_pct=mean(b["cloud"]), humidity_pct=mean(b["rh"]),
            pressure_hpa=mean(b["pres"]), visibility_km=mean(b["vis"]),
            wave_height_m=vmax(b["wave"]), swell_height_m=vmax(b["swell"]), sea_surface_temp_c=mean(b["sst"]),
        ))
    return records


async def fetch_ambee(client: httpx.AsyncClient, loc: dict, key: str) -> list[dict]:
    """Ambee forecast. Despite filter=daily it returns ~48 HOURLY steps, so they are bucketed
    into local days (today and the last day are partial -> flagged via coverage_hours).
    units=si: °C, m/s, km, mm/h, hPa; humidity is already 0-100."""
    params = {"lat": loc["lat"], "lng": loc["lon"], "filter": "daily", "units": "si"}
    resp = await client.get("https://api.ambeedata.com/weather/forecast/by-lat-lng",
                            params=params, headers={"x-api-key": key})
    resp.raise_for_status()
    hours = (resp.json().get("data") or {}).get("forecast") or []
    pct = lambda p: None if p is None else (p * 100 if p <= 1 else p)  # 0-1 fraction or already %

    bk = HourlyBuckets()
    for h in hours:
        if "time" not in h:
            continue
        rate = h.get("precipIntensity")  # mm/h over a 1-hour step -> mm
        bk.add(to_local(datetime.fromtimestamp(h["time"], tz=timezone.utc)), 1,
               temp=h.get("temperature"), feels=h.get("apparentTemperature"), dew=h.get("dewPoint"),
               rh=h.get("humidity"), pres=h.get("pressure"), cloud=h.get("cloudCover"), vis=h.get("visibility"),
               wind=ms_to_kph(h.get("windSpeed")), gust=ms_to_kph(h.get("windGust")), wdir=h.get("windBearing"),
               uv=h.get("uvIndex"), precip=rate, wet=1 if (rate or 0) >= 0.1 else 0,
               pop=pct(h.get("precipProbability")))
    records = []
    for d, b in bk.items():
        if not any(t is not None for t in b["temp"]):
            continue
        cloud = mean(b["cloud"])
        records.append(make_record(
            "Ambee", "Ambee Forecast (hourly)", loc, d, raw_payload={"hours": len(b["temp"])},
            coverage_hours=coverage(b),
            temp_min_c=vmin(b["temp"]), temp_max_c=vmax(b["temp"]), feels_like_max_c=vmax(b["feels"]),
            dew_point_c=mean(b["dew"]), precip_mm=vsum(b["precip"]), precip_prob_pct=vmax(b["pop"]),
            precip_hours=vsum(b["wet"]), wind_speed_kph=vmax(b["wind"]), wind_gust_kph=vmax(b["gust"]),
            wind_dir_deg=dir_mean(b["wdir"]), uv_index=vmax(b["uv"]),
            # cloudCover may come as 0-1 (Dark Sky style) or 0-100
            cloud_cover_pct=None if cloud is None else (cloud * 100 if max(x for x in b["cloud"] if x is not None) <= 1 else cloud),
            humidity_pct=mean(b["rh"]), pressure_hpa=mean(b["pres"]), visibility_km=mean(b["vis"]),
        ))
    return records


async def fetch_google_weather(client: httpx.AsyncClient, loc: dict, key: str) -> list[dict]:
    """Google Maps Platform Weather API, daily forecast (metric). Each Google 'day' runs 07:00 ->
    07:00 next morning, split into daytime/nighttime halves."""
    params = {"key": key, "location.latitude": loc["lat"], "location.longitude": loc["lon"],
              "days": 7, "pageSize": 7, "unitsSystem": "METRIC"}
    resp = await client.get("https://weather.googleapis.com/v1/forecast/days:lookup", params=params)
    resp.raise_for_status()
    records, prev_min = [], None
    for d in resp.json().get("forecastDays", []):
        disp = d.get("displayDate", {})
        day, night = d.get("daytimeForecast") or {}, d.get("nighttimeForecast") or {}
        both = lambda *path: [_dig(day, *path), _dig(night, *path)]
        # Google's low for this 'day' happens at the END of its window (early next morning), so the
        # low on the morning of THIS date is the previous day's minTemperature (same rule as Met Office).
        temp_min, prev_min = prev_min, _dig(d, "minTemperature", "degrees")
        phase = _dig(d, "moonEvents", "moonPhase")
        records.append(make_record(
            "Google Weather", "Google Weather API", loc,
            f"{disp.get('year')}-{disp.get('month'):02d}-{disp.get('day'):02d}", raw_payload=d, coverage_hours=24,
            temp_min_c=temp_min, temp_max_c=_dig(d, "maxTemperature", "degrees"),
            feels_like_max_c=_dig(d, "feelsLikeMaxTemperature", "degrees"),
            precip_mm=vsum(both("precipitation", "qpf", "quantity")),
            precip_prob_pct=vmax(both("precipitation", "probability", "percent")),
            wind_speed_kph=vmax(both("wind", "speed", "value")), wind_gust_kph=vmax(both("wind", "gust", "value")),
            wind_dir_deg=_dig(day, "wind", "direction", "degrees"), uv_index=_dig(day, "uvIndex"),
            cloud_cover_pct=mean(both("cloudCover")), humidity_pct=mean(both("relativeHumidity")),
            sunrise=hhmm(_dig(d, "sunEvents", "sunriseTime")), sunset=hhmm(_dig(d, "sunEvents", "sunsetTime")),
            moon_phase=phase.replace("_", " ").title() if phase else None,
        ))
    return records


def _dig(obj, *path):
    """Safe nested get: _dig(d, "a", "b") -> d["a"]["b"] or None."""
    for p in path:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(p)
    return obj


async def fetch_apple_weatherkit(client: httpx.AsyncClient, loc: dict, jwt_token: str) -> list[dict]:
    url = f"https://weatherkit.apple.com/api/v1/weather/en/{loc['lat']}/{loc['lon']}"
    resp = await client.get(url, params={"dataSets": "forecastDaily"},
                            headers={"Authorization": f"Bearer {jwt_token}"})
    resp.raise_for_status()
    return [
        make_record(
            "Apple WeatherKit", "Apple Weather", loc, to_local(parse_iso(d["forecastStart"])).date().isoformat(),
            raw_payload=d, temp_min_c=d.get("temperatureMin"), temp_max_c=d.get("temperatureMax"),
            precip_mm=d.get("precipitationAmount"), wind_speed_kph=d.get("maxWindSpeed"),
        )
        for d in resp.json().get("forecastDaily", {}).get("days", [])[:7]
    ]


async def fetch_xweather(client: httpx.AsyncClient, loc: dict, creds: tuple) -> list[dict]:
    """Xweather (formerly AerisWeather) daily forecast. Needs client_id + client_secret."""
    client_id, client_secret = creds
    resp = await client.get(
        f"https://data.api.xweather.com/forecasts/{loc['lat']},{loc['lon']}",
        params={"filter": "day", "limit": 7, "client_id": client_id, "client_secret": client_secret})
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        err = data.get("error") or {}
        raise RuntimeError(f"Xweather {err.get('code')}: {err.get('description')}")
    records = []
    for p in (data.get("response") or [{}])[0].get("periods", []):
        sol = p.get("solradWM2")  # daily total (Wh/m²) -> /24 = mean W/m², same basis as other providers
        records.append(make_record(
            "Xweather", "Xweather Forecast", loc, to_local(parse_iso(p["dateTimeISO"])).date().isoformat(),
            raw_payload=p, coverage_hours=24,
            temp_min_c=p.get("minTempC"), temp_max_c=p.get("maxTempC"), feels_like_max_c=p.get("maxFeelslikeC"),
            dew_point_c=p.get("avgDewpointC"), precip_mm=p.get("precipMM"), precip_prob_pct=p.get("pop"),
            wind_speed_kph=p.get("windSpeedMaxKPH"), wind_gust_kph=p.get("windGustKPH"),
            wind_dir_deg=p.get("windDirDEG"), uv_index=p.get("uvi"),
            solar_radiation_wm2=sol / 24 if sol is not None else None,
            cloud_cover_pct=p.get("sky"), humidity_pct=p.get("humidity"), pressure_hpa=p.get("pressureMB"),
            visibility_km=p.get("visibilityKM"),
            sunrise=hhmm(p.get("sunriseISO")), sunset=hhmm(p.get("sunsetISO")),
        ))
    return records


async def fetch_metoffice(client: httpx.AsyncClient, loc: dict, key: str) -> list[dict]:
    """UK Met Office DataHub 'Global Spot' site-specific daily forecast (worldwide coverage).
    Auth is the subscription API key in an `apikey` header (not a query parameter)."""
    resp = await client.get(
        "https://data.hub.api.metoffice.gov.uk/sitespecific/v0/point/daily",
        params={"latitude": loc["lat"], "longitude": loc["lon"], "excludeParameterMetadata": "true"},
        headers={"apikey": key, "accept": "application/json"})
    resp.raise_for_status()
    feats = resp.json().get("features") or []
    if not feats:
        return []
    records, prev_night_min = [], None
    for t in feats[0].get("properties", {}).get("timeSeries", []):
        # Met Office "night" = 18:00 this day -> 06:00 next day, so the minimum on the
        # morning of THIS day is the previous entry's night value (not this entry's).
        temp_min, prev_night_min = prev_night_min, t.get("nightMinScreenTemperature")
        mslp, vis = t.get("middayMslp"), t.get("middayVisibility")
        wind = vmax([t.get("midday10MWindSpeed"), t.get("midnight10MWindSpeed")])
        records.append(make_record(
            "Met Office", "Global Spot (DataHub)", loc, t["time"][:10], raw_payload=t, coverage_hours=24,
            temp_min_c=temp_min, temp_max_c=t.get("dayMaxScreenTemperature"),
            feels_like_max_c=t.get("dayMaxFeelsLikeTemp"),
            precip_prob_pct=vmax([t.get("dayProbabilityOfPrecipitation"), t.get("nightProbabilityOfPrecipitation")]),
            # only midday + midnight spot winds are published -> not a daily max, excluded from consensus
            wind_speed_kph=ms_to_kph(wind), wind_basis="spot",
            wind_dir_deg=t.get("midday10MWindDirection"), uv_index=t.get("maxUvIndex"),
            pressure_hpa=mslp / 100 if mslp is not None else None,
            visibility_km=vis / 1000 if vis is not None else None,
        ))
    return records


async def fetch_foreca(client: httpx.AsyncClient, loc: dict, token: str) -> list[dict]:
    """Foreca Weather API daily forecast (worldwide). Auth: Bearer token."""
    url = f"https://weatherapi.foreca.net/api/v1/forecast/daily/{loc['lon']},{loc['lat']}"
    resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    records = []
    for d in resp.json().get("forecast", []):
        records.append(make_record(
            "Foreca", "Foreca Daily", loc, d["date"], raw_payload=d, coverage_hours=24,
            temp_min_c=d.get("minTemp"), temp_max_c=d.get("maxTemp"),
            precip_mm=d.get("precipAccum"),
            wind_speed_kph=round(d["maxWindSpeed"] * 3.6, 1) if d.get("maxWindSpeed") is not None else None,
            wind_dir_deg=d.get("windDir"),
        ))
    return records


async def fetch_weather_union(client: httpx.AsyncClient, loc: dict, key: str) -> list[dict]:
    """Weather Union (Zomato) - LIVE readings from local ground stations, not a forecast.
    Stored for today only, flagged 'observation', and kept out of the forecast consensus."""
    resp = await client.get(
        "https://www.weatherunion.com/gw/weather/external/v0/get_weather_data",
        params={"latitude": loc["lat"], "longitude": loc["lon"]}, headers={"x-zomato-api-key": key})
    if resp.status_code == 400 and "not supported" in resp.text:
        return []  # Weather Union has no stations here (e.g. Ratnagiri, Kolhapur)
    resp.raise_for_status()
    data = resp.json()
    if str(data.get("status")) != "200":
        raise RuntimeError(f"Weather Union: {data.get('message') or data.get('status')}")
    w = data.get("locality_weather_data") or {}
    if all(v is None for v in w.values()):
        return []  # no station near this city
    return [make_record(
        "Weather Union", "Zomato ground station (live)", loc, datetime.now(LOCAL_TZ).date().isoformat(),
        raw_payload=data, flags=["observation"],
        temp_now_c=w.get("temperature"), humidity_pct=w.get("humidity"),
        wind_speed_kph=ms_to_kph(w.get("wind_speed")),  # documented in m/s
        wind_basis="now", wind_dir_deg=w.get("wind_direction"),
        precip_mm=w.get("rain_accumulation"),  # rain so far today
    )]


async def fetch_open_meteo_current(client: httpx.AsyncClient, loc: dict) -> list[dict]:
    """Open-Meteo 'current' conditions: the model's analysis of right now (updated every 15 min).
    NOT a ground measurement - flagged 'current' and kept out of the forecast consensus."""
    params = {
        "latitude": loc["lat"], "longitude": loc["lon"], "timezone": TZ_NAME,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,cloud_cover,pressure_msl",
    }
    resp = await client.get("https://api.open-meteo.com/v1/forecast", params=params)
    resp.raise_for_status()
    cur = resp.json().get("current") or {}
    if cur.get("temperature_2m") is None:
        return []
    return [make_record(
        "Open-Meteo Current", f"Model analysis at {cur.get('time', '')[11:16]}", loc, cur["time"][:10],
        raw_payload=cur, flags=["current"],
        temp_now_c=cur.get("temperature_2m"), humidity_pct=cur.get("relative_humidity_2m"),
        wind_speed_kph=cur.get("wind_speed_10m"), wind_basis="now", wind_dir_deg=cur.get("wind_direction_10m"),
        cloud_cover_pct=cur.get("cloud_cover"), pressure_hpa=cur.get("pressure_msl"),
    )]


# ==============================================================================
# CADENCE TIERS & QUOTA GUARD
# ==============================================================================

# How often each provider is fetched (scheduler.py --tier X). "full" (06:00) runs everything.
TIERS = {
    # A - every 15 min: live readings
    "A": ["Weather Union", "Open-Meteo Current"],
    # B - every 2 h: generous quotas / frequently updated models
    "B": ["WeatherAPI.com", "OpenWeather", "Visual Crossing", "Tomorrow.io", "Meteosource", "Xweather",
          "Foreca", "Met Office", "MET Norway", "DWD Bright Sky", "Open-Meteo Marine"],
    # C - 06:00 + 18:00: strict quotas, 6-hourly global model runs, trial data
    "C": ["Open-Meteo / ECMWF", "AccuWeather", "Meteoblue", "Weatherbit", "Windy",
          "Ambee", "Google Weather", "Apple WeatherKit"],
    # D - 06:00 only: 10 calls/day, or data that changes once a day
    "D": ["Stormglass", "NASA POWER", "NOAA / NWS", "ECCC (Canada)"],
}

# Free-tier daily limits (calls/day, cost per city call). The guard stops a provider at 90%.
QUOTAS = {
    "Stormglass": (10, 1), "AccuWeather": (50, 1), "Weatherbit": (50, 1), "Meteoblue": (100, 1),
    "Tomorrow.io": (500, 1), "Meteosource": (400, 1), "OpenWeather": (1000, 1), "Windy": (500, 1),
    "Visual Crossing": (1000, 7),  # billed per day of data; we request 7 days
    "WeatherAPI.com": (30000, 1), "Xweather": (1000, 1), "Met Office": (360, 1), "Weather Union": (1000, 1),
    "Ambee": (100, 1),            # free plan limit not published in responses -> conservative
    "Google Weather": (300, 1),   # ~9k/month: stays under Google's 10k/month free usage (billing account!)
}
QUOTA_SAFETY = 0.9


def providers_for_tier(tier: str) -> list[str] | None:
    """Job names for a tier; None = everything ('full')."""
    if tier == "full":
        return None
    if tier not in TIERS:
        raise ValueError(f"Unknown tier '{tier}' (use A, B, C, D or full)")
    return list(TIERS[tier])


# ==============================================================================
# ORCHESTRATOR
# ==============================================================================

TIER1 = [
    ("Open-Meteo / ECMWF", fetch_open_meteo_and_ecmwf),
    ("Open-Meteo Current", fetch_open_meteo_current),
    ("MET Norway", fetch_met_norway),
    ("DWD Bright Sky", fetch_dwd_brightsky),
    ("NASA POWER", fetch_nasa_power),
    ("NOAA / NWS", fetch_noaa_nws),
    ("ECCC (Canada)", fetch_eccc_canada),
    ("Open-Meteo Marine", fetch_marine),
]

TIER2 = [
    ("WeatherAPI.com", "WEATHERAPI_KEY", fetch_weatherapi),
    ("OpenWeather", "OPENWEATHER_KEY", fetch_openweather),
    ("Visual Crossing", "VISUAL_CROSSING_KEY", fetch_visual_crossing),
    ("Tomorrow.io", "TOMORROW_IO_KEY", fetch_tomorrow_io),
    ("Weatherbit", "WEATHERBIT_KEY", fetch_weatherbit),
    ("Meteosource", "METEOSOURCE_KEY", fetch_meteosource),
    ("AccuWeather", "ACCUWEATHER_KEY", fetch_accuweather),
    ("Meteoblue", "METEOBLUE_KEY", fetch_meteoblue),
    ("Windy", "WINDY_KEY", fetch_windy),
    ("Stormglass", "STORMGLASS_KEY", fetch_stormglass),
    ("Ambee", "AMBEE_KEY", fetch_ambee),
    ("Google Weather", "GOOGLE_WEATHER_KEY", fetch_google_weather),
    ("Apple WeatherKit", "APPLE_WEATHERKIT_JWT", fetch_apple_weatherkit),
    ("Met Office", "METOFFICE_DATAHUB_KEY", fetch_metoffice),
    ("Xweather", ("XWEATHER_CLIENT_ID", "XWEATHER_CLIENT_SECRET"), fetch_xweather),
    ("Weather Union", "WEATHER_UNION_KEY", fetch_weather_union),
    ("Foreca", "FORECA_KEY", fetch_foreca),
]
# A tuple of env vars means the provider needs all of them (passed to the adapter as a tuple).


def load_cache(path: Path = CACHE_PATH) -> dict:
    """Read the cache; accepts the legacy bare-list format too."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"generated_at": None, "cities": {}, "records": []}
    if isinstance(data, list):
        return {"generated_at": None, "cities": {}, "records": data, "legacy": True}
    return data


async def _run_provider(name, fn, client, locs, key, status):
    """One provider walks the cities sequentially: no request bursts against rate limits.
    Each city call is checked against (and counted toward) the provider's daily quota."""
    import history  # local import: history.py has no dependency on this module
    limit, cost = QUOTAS.get(name, (None, 1))
    rows = []
    for loc in locs:
        if limit is not None:
            used = history.usage_today(name)
            if used + cost > limit * QUOTA_SAFETY:
                status[loc["city"]][name] = f"skipped: daily quota guard ({used}/{limit} used)"
                print(f"[WARN] {name} / {loc['city']}: skipped, {used}/{limit} calls used today")
                continue
            history.add_usage(name, cost)
        try:
            got = await (fn(client, loc, key) if key is not None else fn(client, loc))
            rows.extend(got)
            status[loc["city"]][name] = f"ok ({len(got)} days)" if got else "n/a for location"
        except httpx.HTTPStatusError as exc:
            msg = redact(f"HTTP {exc.response.status_code}: {exc}")[:200]
            status[loc["city"]][name] = f"error: {msg}"
            print(f"[WARN] {name} / {loc['city']}: {msg}")
            if exc.response.status_code == 429:
                # Rate limited: stop calling this provider for the rest of the run (no retries,
                # no key rotation). Remaining cities keep their last good data.
                for rest in locs[locs.index(loc) + 1:]:
                    status[rest["city"]][name] = "error: rate limited (HTTP 429) - not called"
                print(f"[WARN] {name}: rate limited, skipping its remaining cities this run")
                break
        except Exception as exc:  # noqa: BLE001 - one bad provider must not sink the run
            msg = redact(f"{type(exc).__name__}: {exc}")[:200]
            status[loc["city"]][name] = f"error: {msg}"
            print(f"[WARN] {name} / {loc['city']}: {msg}")
        await pace()
    return rows


def _build_jobs(providers, locs, status) -> list[tuple]:
    """(name, adapter, key) for every selected provider that has its key(s). Missing keys are
    recorded in `status` as 'no API key'."""
    if providers:
        known = {n for n, _ in TIER1} | {n for n, _, _ in TIER2}
        unknown = {p for p in providers if p not in known}
        if unknown:
            raise ValueError(f"Unknown provider(s) {sorted(unknown)}. Known: {sorted(known)}")
    pick = (lambda n: n in providers) if providers else (lambda n: True)
    jobs = [(name, fn, None) for name, fn in TIER1 if pick(name)]
    for name, env_var, fn in TIER2:
        if not pick(name):
            continue
        names = env_var if isinstance(env_var, tuple) else (env_var,)
        vals = tuple(os.getenv(v, "").strip() for v in names)
        if all(vals):
            jobs.append((name, fn, vals if isinstance(env_var, tuple) else vals[0]))
        else:
            for l in locs:
                status[l["city"]][name] = "no API key"
    return jobs


# Providers used for an on-demand custom lat/lon lookup: keyless + generous quotas only.
# Strict-quota providers (AccuWeather 50/day, Stormglass 10/day, Weatherbit 50/day, ...) are
# reserved for the scheduled city runs; Visual Crossing is left out because it bills 7 per call.
CUSTOM_POINT_PROVIDERS = [
    "Open-Meteo / ECMWF", "Open-Meteo Current", "MET Norway", "DWD Bright Sky", "Open-Meteo Marine",
    "WeatherAPI.com", "OpenWeather", "Tomorrow.io", "Meteosource", "Xweather", "Met Office", "Weather Union",
]


class FetchPaused(RuntimeError):
    """Raised instead of calling any upstream API while FETCH_PAUSED=1."""


def fetching_paused() -> bool:
    """Global kill switch: FETCH_PAUSED=1 in .env stops every outbound API call (scheduler,
    dashboard on-demand fetches, CLI). Saved data keeps being served."""
    return os.getenv("FETCH_PAUSED", "0").strip().lower() in ("1", "true", "yes", "on")


def ensure_fetching_allowed() -> None:
    if fetching_paused():
        raise FetchPaused("Data fetching is paused (FETCH_PAUSED=1 in .env). Set it to 0 to resume.")


async def fetch_point(lat: float, lon: float) -> dict:
    """Forecasts for any coordinate, fetched on demand. Nothing is written to latest_forecasts.json
    or the archive; API calls still count toward (and are limited by) the quota guard."""
    ensure_fetching_allowed()
    label = f"{lat:.4f}, {lon:.4f}"
    loc = {"city": label, "lat": lat, "lon": lon, "country": None,
           "coastal": True, "region": "Custom location"}  # coastal=True lets the marine feed try
    status = defaultdict(dict)
    jobs = _build_jobs(CUSTOM_POINT_PROVIDERS, [loc], status)
    for name in [n for n, _ in TIER1] + [n for n, _, _ in TIER2]:
        if name not in CUSTOM_POINT_PROVIDERS:
            status[label][name] = "skipped: custom point"
    async with make_client() as client:
        results = await asyncio.gather(*[_run_provider(n, fn, client, [loc], key, status) for n, fn, key in jobs])
    return {"loc": loc, "records": [r for rows in results for r in rows], "status": dict(status[label]),
            "fetched_at": datetime.now(timezone.utc).isoformat()}


# Job name -> provider labels its records carry (when they differ from the job name).
JOB_LABELS = {"Open-Meteo / ECMWF": {"Open-Meteo", "ECMWF"}}


@contextlib.contextmanager
def _cache_write_lock(timeout_s: float = 60):
    """Serialises read-merge-write of latest_forecasts.json between processes
    (e.g. a 15-minute live update finishing while the dashboard's Update button runs)."""
    lock = BASE_DIR / ".cache" / "cache_write.lock"
    lock.parent.mkdir(exist_ok=True)
    deadline = time.time() + timeout_s
    while True:
        try:
            fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            break
        except FileExistsError:
            if time.time() - lock.stat().st_mtime > 120:  # left over from a crash
                lock.unlink(missing_ok=True)
            elif time.time() > deadline:
                raise TimeoutError("latest_forecasts.json is locked by another writer")
            time.sleep(0.25)
    try:
        yield
    finally:
        lock.unlink(missing_ok=True)


async def run_daily_ingestion(cities=None, out_path: Path = CACHE_PATH, providers=None) -> dict:
    """Fetch and merge into the cache. `providers` (job names) limits a run to those providers;
    rows from all other providers are kept untouched."""
    ensure_fetching_allowed()
    wanted = {c.lower() for c in cities} if cities else None
    locs = [l for l in LOCATIONS if wanted is None or l["city"].lower() in wanted]
    if not locs:
        raise ValueError(f"No configured location matches {cities}")

    status = defaultdict(dict)
    jobs = _build_jobs(providers, locs, status)
    print(f"Fetching {len(jobs)} providers for {[l['city'] for l in locs]}")

    async with make_client() as client:
        results = await asyncio.gather(*[
            _run_provider(name, fn, client, locs, key, status) for name, fn, key in jobs
        ])
    new_rows = [r for rows in results for r in rows]

    # Merge. For each refreshed city, a provider's old rows are replaced only if that provider
    # answered this time (ok / n-a). If it failed or was quota-skipped, its last good rows stay.
    label_to_job = {lab: job for job, labs in JOB_LABELS.items() for lab in labs}
    answered = {c: {j for j, s in st.items() if s.startswith(("ok", "n/a"))} for c, st in status.items()}

    def stale(r):
        job = label_to_job.get(r.get("provider"), r.get("provider"))
        return r.get("city") in answered and job in answered[r["city"]]

    now = datetime.now(timezone.utc).isoformat()
    with _cache_write_lock():
        cache = load_cache(out_path)
        refreshed = {l["city"] for l in locs}
        keep = [r for r in cache.get("records", []) if not stale(r) and "coverage_hours" in r]
        dedup = {(r["provider"], r["model_source"], r["city"], r["forecast_date"]): r for r in keep + new_rows}
        city_meta = {k: v for k, v in cache.get("cities", {}).items() if not cache.get("legacy")}
        for l in locs:
            old = city_meta.get(l["city"], {})
            merged = {**old.get("providers", {}), **status[l["city"]]}
            if providers:  # partial run: keep the full-refresh time, record the partial update time
                city_meta[l["city"]] = {**old, "providers": merged, "updated_at": now}
            else:
                city_meta[l["city"]] = {"fetched_at": now, "updated_at": now, "providers": merged}
        out = {"generated_at": now, "timezone": TZ_NAME, "cities": city_meta, "records": list(dedup.values())}
        tmp = out_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(out, indent=1, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, out_path)
    print(f"Saved {len(new_rows)} new records ({len(out['records'])} total) -> {out_path.name}")
    return {"fetched_at": now, "cities": sorted(refreshed), "new_records": len(new_rows),
            "partial": bool(providers), "status": {c: status[c] for c in refreshed}}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Fetch forecasts into latest_forecasts.json")
    ap.add_argument("cities", nargs="*", help="cities to refresh (default: all)")
    ap.add_argument("--providers", nargs="+", metavar="NAME",
                    help='only these providers, e.g. --providers "Weather Union" Xweather')
    ap.add_argument("--tier", choices=["A", "B", "C", "D", "full"], help="only one cadence tier")
    args = ap.parse_args()
    provs = args.providers or (providers_for_tier(args.tier) if args.tier else None)
    asyncio.run(run_daily_ingestion(args.cities or None, providers=provs))
