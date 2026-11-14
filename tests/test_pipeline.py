"""
Offline test suite - never calls an external API (FETCH_PAUSED is forced on) and never writes to
the real database (temp files / mocks). Run from the project folder:

    python -m unittest discover -s tests -v
"""
import asyncio
import gzip
import json
import os
import sys
import tempfile
import unittest
from collections import defaultdict
from pathlib import Path
from unittest import mock

# must be set BEFORE importing the app (security.py reads them at import; .env never overrides them)
os.environ["FETCH_PAUSED"] = "1"
os.environ["ALLOWED_HOSTS"] = "testserver"
os.environ["ADMIN_KEY"] = "test-admin-key"
_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False); _TMP_DB.close()
os.environ["WEATHER_DB_PATH"] = _TMP_DB.name   # never touch the real archive
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx    # noqa: E402
import history  # noqa: E402
import ingest   # noqa: E402
import main     # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

LOC = {"city": "Testville", "lat": 19.0, "lon": 73.0}


def rec(provider, model="M", **fields):
    r = ingest.make_record(provider, model, LOC, "2026-11-13", coverage_hours=24, **fields)
    return r


class Helpers(unittest.TestCase):
    def test_parse_iso_handles_nanoseconds_and_z(self):
        self.assertEqual(ingest.parse_iso("2026-11-13T01:14:36.938427269Z").isoformat(),
                         "2026-11-13T01:14:36.938427+00:00")
        self.assertEqual(ingest.parse_iso("2026-11-13T09:00:00Z").isoformat(), "2026-11-13T09:00:00+00:00")

    def test_hhmm_formats_to_local_time(self):
        self.assertEqual(ingest.hhmm("06:44 AM"), "06:44")
        self.assertEqual(ingest.hhmm("18:00:38"), "18:00")
        self.assertEqual(ingest.hhmm("2026-11-13T01:14:36Z"), "06:44")  # UTC -> IST
        self.assertIsNone(ingest.hhmm(None))

    def test_redact_hides_secrets_in_urls(self):
        msg = "GET https://x.io/f?key=ABC123&client_id=ID9&client_secret=SEC&apikey=Z&lat=1"
        out = ingest.redact(msg)
        for secret in ("ABC123", "ID9", "SEC", "=Z&"):
            self.assertNotIn(secret, out)
        self.assertIn("lat=1", out)

    def test_wind_direction_circular_mean(self):
        self.assertAlmostEqual(ingest.dir_mean([350, 10]) % 360, 0, delta=0.01)
        speed, d = ingest.uv_to_speed_dir(0, -5)  # wind blowing toward south = from north
        self.assertAlmostEqual(speed, 18.0, places=1)
        self.assertAlmostEqual(d, 0, delta=0.01)

    def test_moon_from_lunation(self):
        self.assertEqual(ingest.moon_from_lunation(0.5), ("Full Moon", 100))
        self.assertEqual(ingest.moon_from_lunation(0.0)[0], "New Moon")


class NormalisedRecord(unittest.TestCase):
    def test_unknown_field_is_rejected(self):
        with self.assertRaises(ValueError):
            rec("X", not_a_field=1)

    def test_missing_values_stay_none(self):
        r = rec("X", precip_mm=0.0)
        self.assertIsNone(r["precip_type"])   # never derived from precip_mm
        self.assertIsNone(r["temp_max_c"])

    def test_wind_basis_defaults_to_max(self):
        self.assertEqual(rec("X", wind_speed_kph=10)["wind_basis"], "max")
        self.assertEqual(rec("X", wind_speed_kph=10, wind_basis="avg")["wind_basis"], "avg")

    def test_hourly_buckets_report_coverage(self):
        bk = ingest.HourlyBuckets()
        start = ingest.parse_iso("2026-11-13T00:00:00+05:30")
        for h in range(10):
            bk.add(start.replace(hour=h), 1, temp=20 + h)
        (day, b), = bk.items()
        self.assertEqual(day, "2026-11-13")
        self.assertEqual(ingest.coverage(b), 10)


class Consensus(unittest.TestCase):
    def test_median_and_exclusions(self):
        rows = [rec("A", temp_max_c=30), rec("B", temp_max_c=31), rec("C", temp_max_c=32),
                rec("W", temp_max_c=20, )]
        rows[3]["flags"] = ["trial"]                      # Windy-style trial data
        partial = rec("P", temp_max_c=40); partial["coverage_hours"] = 10
        s = main.metric_stats(rows + [partial], "temp_max_c")
        self.assertEqual(s["n"], 3)
        self.assertEqual(s["median"], 31)
        reasons = {e["model"].split(" · ")[0]: e["reason"] for e in s["excluded"]}
        self.assertIn("trial", reasons["W"])
        self.assertIn("partial day", reasons["P"])

    def test_average_wind_not_counted_as_max(self):
        rows = [rec("A", wind_speed_kph=20), rec("B", wind_speed_kph=5, wind_basis="avg")]
        s = main.metric_stats(rows, "wind_speed_kph")
        self.assertEqual(s["n"], 1)

    def test_rain_agreement_is_categorical(self):
        rows = [rec(p, precip_mm=v) for p, v in [("A", 0), ("B", 0), ("C", 0), ("D", 5)]]
        s = main.metric_stats(rows, "precip_mm")
        self.assertEqual(s["agreement_pct"], 75)
        self.assertTrue(s["consensus_label"].startswith("dry"))

    def test_every_priority_slot_is_always_listed(self):
        out = main.ordered_models([rec("ECMWF", "IFS", temp_max_c=30)], {})
        slots = [m["slot"] for m in out if m["slot"] != "Other"]
        self.assertEqual(slots, [s for s, _, _ in main.PRIORITY])
        self.assertFalse(out[0]["placeholder"])
        self.assertTrue(all(m["placeholder"] for m in out[1:]))


class QuotaGuard(unittest.TestCase):
    def test_provider_stops_at_90_percent(self):
        used = defaultdict(int)
        calls = []

        async def fake_adapter(client, loc, key=None):
            calls.append(loc["city"])
            return []

        with mock.patch.dict(ingest.QUOTAS, {"TEST": (5, 1)}), \
             mock.patch.object(history, "usage_today", lambda p: used[p]), \
             mock.patch.object(history, "add_usage", lambda p, n=1: used.__setitem__(p, used[p] + n)), \
             mock.patch.object(ingest.asyncio, "sleep", new=mock.AsyncMock()):
            status = defaultdict(dict)
            asyncio.run(ingest._run_provider("TEST", fake_adapter, None, ingest.LOCATIONS, None, status))
        self.assertEqual(len(calls), 4)  # 90% of 5 = 4.5 -> 4 calls allowed
        self.assertTrue(any(v.startswith("skipped: daily quota") for c in status.values() for v in c.values()))


class PoliteHttp(unittest.TestCase):
    """http_client: retries transient errors, never hammers a 429, sends a User-Agent."""

    def _run(self, responses):
        import http_client
        seen = []

        def handler(request):
            seen.append(request)
            status, headers = responses[min(len(seen) - 1, len(responses) - 1)]
            return httpx.Response(status, headers=headers, json={})

        transport = http_client.PoliteTransport(httpx.MockTransport(handler), sleep=mock.AsyncMock())

        async def go():
            async with httpx.AsyncClient(transport=transport, headers={"User-Agent": http_client.USER_AGENT}) as c:
                return await c.get("https://example.test/x")

        return asyncio.run(go()), seen

    def test_retries_503_then_succeeds(self):
        resp, seen = self._run([(503, {}), (503, {}), (200, {})])
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(seen), 3)

    def test_gives_up_after_max_retries(self):
        resp, seen = self._run([(503, {})])
        self.assertEqual(resp.status_code, 503)
        self.assertEqual(len(seen), 3)  # 1 try + 2 retries, then stop

    def test_429_with_long_retry_after_is_not_retried(self):
        resp, seen = self._run([(429, {"Retry-After": "3600"})])
        self.assertEqual(resp.status_code, 429)
        self.assertEqual(len(seen), 1)

    def test_429_short_retry_after_is_honoured_once(self):
        resp, seen = self._run([(429, {"Retry-After": "2"}), (429, {"Retry-After": "2"})])
        self.assertEqual(len(seen), 2)  # one polite retry, never a loop

    def test_user_agent_is_sent(self):
        _, seen = self._run([(200, {})])
        self.assertTrue(seen[0].headers["user-agent"].startswith("WeatherConsensus/1.0"))

    def test_provider_stops_after_429(self):
        calls = []

        async def limited(client, loc, key=None):
            calls.append(loc["city"])
            raise httpx.HTTPStatusError("429", request=httpx.Request("GET", "https://x"),
                                        response=httpx.Response(429))

        with mock.patch.object(ingest, "pace", new=mock.AsyncMock()):
            status = defaultdict(dict)
            asyncio.run(ingest._run_provider("Keyless", limited, None, ingest.LOCATIONS, None, status))
        self.assertEqual(len(calls), 1)  # first city only; the other 6 are not called
        self.assertTrue(all("429" in v["Keyless"] for v in status.values()))


class PublicExport(unittest.TestCase):
    def setUp(self):
        import export
        self.export = export
        a = rec("ECMWF", "IFS", temp_max_c=34.0, precip_mm=0.0); a["city"] = "Mumbai"
        b = rec("AccuWeather", "Daily", temp_max_c=36.0, precip_mm=0.0); b["city"] = "Mumbai"
        w = rec("Windy", "GFS", temp_max_c=29.0); w["city"] = "Mumbai"; w["flags"] = ["trial"]
        a["raw_payload"] = {"secret_looking": "should never be exported"}
        self.cache = {"generated_at": "2026-11-13T00:30:00+00:00", "records": [a, b, w],
                      "cities": {"Mumbai": {"fetched_at": "2026-11-13T00:30:00+00:00", "providers": {}}}}
        self.now = ingest.parse_iso("2026-11-13T06:15:00Z")

    def test_schema_timestamps_and_content(self):
        with mock.patch.object(ingest, "load_cache", return_value=self.cache):
            d = self.export.build_export(self.now)
        m = d["meta"]
        self.assertEqual(m["schema_version"], "2.0")
        self.assertEqual(m["generated_at_utc"], "2026-11-13T06:15:00Z")
        self.assertEqual(m["generated_at_ist"], "2026-11-13T11:45:00+05:30")
        city = d["cities"]["Mumbai"]
        day = city["daily_consensus"][0]
        self.assertEqual(day["temp_max_c"]["median"], 35.0)          # Windy trial value not counted
        self.assertEqual(day["temp_max_c"]["models_counted"], 2)
        windy = next(p for p in city["provider_forecasts"] if p["provider"] == "Windy")
        self.assertIn("trial", windy["daily"][0]["not_counted_in_consensus"]["temp_max_c"])
        self.assertNotIn("should never be exported", json.dumps(d))   # raw payloads stay internal
        self.assertNotIn("model_run_cycle", json.dumps(d))            # not reported upstream -> not invented

    def test_snapshot_once_per_six_hour_block(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(ingest, "load_cache", return_value=self.cache):
            out, arc = Path(tmp) / "latest.json", Path(tmp) / "history"
            r1 = self.export.write(out, arc, now=self.now)
            r2 = self.export.write(out, arc, now=ingest.parse_iso("2026-11-13T11:59:00Z"))
            r3 = self.export.write(out, arc, now=ingest.parse_iso("2026-11-13T12:05:00Z"))
            self.assertTrue(r1["snapshot"].endswith("2026-11-13T0615Z.json.gz"))
            self.assertIsNone(r2["snapshot"])                          # same 06-12 UTC block
            self.assertTrue(r3["snapshot"].endswith("2026-11-13T1205Z.json.gz"))
            self.assertEqual(json.loads(gzip.decompress(Path(r1["snapshot"]).read_bytes()))["meta"]["schema_version"], "2.0")


class HistoryArchive(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "h.db"

    def tearDown(self):
        self.tmp.cleanup()

    def test_snapshot_roundtrip(self):
        rows = [rec("A", temp_max_c=30), rec("B", temp_max_c=32)]
        n = history.archive_snapshot(rows, "2026-11-13T00:30:00+00:00", "2026-11-11", path=self.db)
        self.assertEqual(n, 2)
        self.assertEqual(history.archive_snapshot(rows, "2026-11-13T00:30:00+00:00", "2026-11-11", path=self.db), 0)
        evo = history.forecast_evolution("Testville", "2026-11-13", path=self.db)
        self.assertEqual(evo["snapshots"][0]["lead_days"], 2)
        self.assertEqual(evo["snapshots"][0]["temp_max_median"], 31)

    def test_sky_reports(self):
        history.add_sky_report("Testville", "cloudy", path=self.db)
        with self.assertRaises(ValueError):
            history.add_sky_report("Testville", "tornado", path=self.db)
        r = history.recent_sky_reports("Testville", path=self.db)
        self.assertEqual(r["counts"], {"cloudy": 1})


class Api(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.c = TestClient(main.app)

    def test_core_endpoints_respond(self):
        for path in ("/", "/api/locations", "/api/today?city=Mumbai", "/api/week?city=Pune",
                     "/api/consensus?city=Nagpur", "/api/divergence", "/api/runs"):
            self.assertEqual(self.c.get(path).status_code, 200, path)

    def test_paused_fetching_refuses_upstream_calls(self):
        self.assertEqual(self.c.get("/api/point?lat=12.97&lon=77.59").status_code, 503)
        # /api/radar is intentionally exempt (RainViewer is keyless and uses no quota) - not asserted here
        # because it would make a real network call.
        self.assertTrue(self.c.get("/api/today?city=Mumbai").json()["fetch_paused"])

    def test_input_validation(self):
        self.assertEqual(self.c.get("/api/today?city=Pune&date=2026-13-45").status_code, 422)
        self.assertEqual(self.c.get("/api/point?lat=91&lon=0").status_code, 422)
        self.assertEqual(self.c.get("/api/today?city=Atlantis").status_code, 404)
        self.assertEqual(self.c.get("/api/accuracy?field=x;DROP").status_code, 422)

    def test_security_layer(self):
        self.assertEqual(self.c.get("/api/today", headers={"Host": "evil.example"}).status_code, 400)
        r = self.c.post("/api/report-sky", json={"sky": "clear", "city": "Pune"}, headers={"Origin": "https://evil.example"})
        self.assertEqual(r.status_code, 403)
        r = self.c.post("/api/report-sky", content='{"sky":"clear"}', headers={"Content-Type": "text/plain"})
        self.assertEqual(r.status_code, 415)
        self.assertEqual(self.c.get("/docs").status_code, 404)
        page = self.c.get("/")
        nonce = page.headers["content-security-policy"].split("'nonce-")[1].split("'")[0]
        self.assertIn(f'<script nonce="{nonce}">', page.text)
        self.assertEqual(page.headers["x-frame-options"], "DENY")

    def test_timeseries(self):
        r = self.c.get("/api/timeseries?city=Mumbai&metric=temp_max_c")
        self.assertEqual(r.status_code, 200)
        ids = [s["id"] for s in r.json()["series"]]
        self.assertEqual(len(ids), len(set(ids)))  # each model is its own line (no Open-Meteo collisions)
        self.assertEqual(self.c.get("/api/timeseries?metric=bogus").status_code, 422)
        self.assertEqual(self.c.get("/api/timeseries?lat=10&lon=10").status_code, 404)  # point not opened yet

    def test_timeseries_keeps_models_apart(self):
        recs = [rec("Open-Meteo", "GFS (NOAA)", temp_max_c=30), rec("Open-Meteo", "ICON (DWD)", temp_max_c=32)]
        with mock.patch.object(main, "today_local", return_value="2026-11-13"):
            out = main.timeseries_from(recs, "temp_max_c")
        self.assertEqual(sorted(s["values"][0] for s in out["series"]), [30, 32])
        self.assertEqual(out["ensemble"][0]["n"], 2)

    def test_admin_endpoints(self):
        self.assertEqual(self.c.get("/api/usage").status_code, 403)
        self.assertEqual(self.c.get("/api/usage", headers={"X-Admin-Key": "test-admin-key"}).status_code, 200)


if __name__ == "__main__":
    unittest.main()
