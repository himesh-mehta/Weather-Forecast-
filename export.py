"""
Public, timestamped export of the forecasts (schema 2.0) - the file other apps and the GitHub
`data` branch serve, e.g. https://raw.githubusercontent.com/<owner>/<repo>/data/latest_maharashtra.json

    python export.py                                   # writes latest_maharashtra.json
    python export.py --out X.json --archive history    # also writes history/YYYY/MM/<UTC time>.json.gz

Contents per city: location, per-day ensemble consensus (median / range / agreement), and every
provider's own daily values with the time they were fetched. Raw provider payloads are left out
(they stay in latest_forecasts.json), which keeps a gzipped snapshot around 30-40 KB.
Values are exactly what providers reported; nothing is filled in (see README "Data integrity").
"""
import argparse
import gzip
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import ingest
import main

SCHEMA_VERSION = "2.0"
CONSENSUS_FIELDS = ["temp_max_c", "temp_min_c", "feels_like_max_c", "precip_mm", "precip_prob_pct",
                    "wind_speed_kph", "wind_gust_kph", "humidity_pct", "cloud_cover_pct", "uv_index"]
UNITS = {c["key"]: c.get("unit", "") for dm in main.DOMAINS for c in dm["columns"] if c.get("unit") is not None}
MAHARASHTRA_BBOX = ingest.MAHARASHTRA_BBOX


def _round(v):
    return round(v, 2) if isinstance(v, float) else v


def _consensus_day(rows: list[dict]) -> dict:
    c = main.consensus_for(rows)
    out = {"overall_agreement_pct": c["agreement_score"], "overall_agreement_level": c["agreement_level"]}
    for f in CONSENSUS_FIELDS:
        s = c["metrics"].get(f) or {}
        if not s.get("n"):
            continue
        entry = {"median": _round(s.get("median")), "min": _round(s.get("min")), "max": _round(s.get("max")),
                 "models_counted": s["n"], "agreement_pct": s.get("agreement_pct")}
        if f == "precip_mm":
            entry["verdict"] = s.get("consensus_label")
        out[f] = {k: v for k, v in entry.items() if v is not None}
    return out


def _provider_blocks(rows: list[dict], dates: list[str]) -> list[dict]:
    blocks = {}
    for d in dates:
        day_rows = [r for r in rows if r["forecast_date"] == d]
        excluded = {f: {e["model"]: e["reason"] for e in main.metric_stats(day_rows, f)["excluded"]}
                    for f in CONSENSUS_FIELDS}
        for r in day_rows:
            mid = main.model_id(r)
            b = blocks.setdefault(mid, {"provider": r["provider"], "model": r.get("model_source"),
                                        "display_rank": main._rank(r), "flags": r.get("flags") or [],
                                        "fetched_at_utc": r.get("fetched_at"), "daily": []})
            b["fetched_at_utc"] = max(b["fetched_at_utc"] or "", r.get("fetched_at") or "") or None
            day = {"date": d, **{f: _round(r[f]) for f in ingest.FIELDS if r.get(f) not in (None, "")}}
            if r.get("coverage_hours") is not None and r["coverage_hours"] < 24:
                day["coverage_hours"] = r["coverage_hours"]
            not_counted = {f: why[mid] for f, why in excluded.items() if mid in why}
            if not_counted:
                day["not_counted_in_consensus"] = not_counted
            b["daily"].append(day)
    return sorted(blocks.values(), key=lambda b: (b["display_rank"], b["provider"], b["model"] or ""))


def build_export(now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    cache = ingest.load_cache()
    live = {}
    try:
        import live as live_mod
        live = live_mod.load_live()
    except Exception:  # noqa: BLE001 - live layer is optional in the export
        pass
    today = now.astimezone(ingest.LOCAL_TZ).date().isoformat()
    cities, model_ids = {}, set()
    for loc in ingest.LOCATIONS:
        rows = [r for r in cache.get("records", []) if r.get("city") == loc["city"]]
        dates = sorted({r["forecast_date"] for r in rows if r["forecast_date"] >= today})[:7]
        meta = cache.get("cities", {}).get(loc["city"], {})
        nowcast = (live.get(loc["city"]) or {}).get("nowcast") or {}
        location = {"city": loc["city"], "lat": loc["lat"], "lon": loc["lon"], "region": loc.get("region"),
                    "coastal": bool(loc.get("coastal")), "elevation_m": nowcast.get("elevation_m")}
        if loc.get("marine"):
            location["marine_point"] = {"lat": loc["marine"][0], "lon": loc["marine"][1]}
        providers = _provider_blocks([r for r in rows if r["forecast_date"] in dates], dates)
        model_ids.update((p["provider"], p["model"]) for p in providers)
        cities[loc["city"]] = {
            "location": location,
            "last_full_refresh_utc": meta.get("fetched_at"),
            "last_update_utc": meta.get("updated_at") or meta.get("fetched_at"),
            "daily_consensus": [{"date": d, **_consensus_day([r for r in rows if r["forecast_date"] == d])}
                                for d in dates],
            "provider_forecasts": providers,
            "provider_status": meta.get("providers", {}),
        }
    return {
        "meta": {
            "schema_version": SCHEMA_VERSION,
            "region": "Maharashtra, India",
            "generated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "generated_at_ist": now.astimezone(ingest.LOCAL_TZ).isoformat(timespec="seconds"),
            "data_updated_at_utc": cache.get("generated_at"),
            "timezone_for_dates": ingest.TZ_NAME,
            "total_cities": len(cities),
            "provider_models_reporting": len(model_ids),
            "bounding_box": MAHARASHTRA_BBOX,
            "units": UNITS,
            "source": os.getenv("CONTACT_URL") or None,
            "notes": [
                "Dates are IST calendar days. 'median/min/max' count only forecasts used in the consensus.",
                "A provider's value is exactly what it reported; absent fields were not provided.",
                "Upstream APIs do not report which model run (00z/06z/12z/18z) they served, so only fetch times are given.",
            ],
        },
        "cities": cities,
    }


def _block_taken(archive: Path, now: datetime, every_h: int) -> bool:
    """True if a snapshot already exists in this UTC block (e.g. 06:00-11:59 for every_h=6)."""
    start = now.hour // every_h * every_h
    folder = archive / now.strftime("%Y") / now.strftime("%m")
    prefix = now.strftime("%Y-%m-%dT")
    for f in folder.glob(f"{prefix}*.json.gz") if folder.exists() else []:
        hh = int(f.name[len(prefix):len(prefix) + 2])
        if start <= hh < start + every_h:
            return True
    return False


def write(out: Path, archive: Path | None, now: datetime | None = None, every_h: int = 6) -> dict:
    now = now or datetime.now(timezone.utc)
    data = build_export(now)
    body = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(out.suffix + ".tmp")
    tmp.write_bytes(body)
    os.replace(tmp, out)
    result = {"latest": str(out), "bytes": len(body)}
    if archive is not None and every_h and _block_taken(archive, now, every_h):
        result["snapshot"] = None  # this 6-hour block is already archived
    elif archive is not None:
        snap = archive / now.strftime("%Y") / now.strftime("%m") / f"{now.strftime('%Y-%m-%dT%H%MZ')}.json.gz"
        snap.parent.mkdir(parents=True, exist_ok=True)
        snap.write_bytes(gzip.compress(body, 9))
        result.update(snapshot=str(snap), snapshot_bytes=snap.stat().st_size)
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=ingest.BASE_DIR / "latest_maharashtra.json")
    ap.add_argument("--archive", type=Path, help="folder for timestamped .json.gz snapshots")
    ap.add_argument("--every-hours", type=int, default=6,
                    help="at most one snapshot per UTC block of this many hours (0 = every run)")
    a = ap.parse_args()
    r = write(a.out, a.archive, every_h=a.every_hours)
    msg = f"Wrote {r['latest']} ({r['bytes'] / 1e3:.0f} KB)"
    if r.get("snapshot"):
        msg += f" and {r['snapshot']} ({r['snapshot_bytes'] / 1e3:.0f} KB gz)"
    elif "snapshot" in r:
        msg += f"; snapshot skipped (this {a.every_hours}-hour block is already archived)"
    print(msg)
