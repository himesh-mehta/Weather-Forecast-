"""
HTTP security layer for the dashboard/API (pure Starlette, no extra dependencies).

What it does, in request order:
  1. Trusted hosts      - only answer requests addressed to allowed host names. Blocks DNS-rebinding,
                          where a website you visit points its domain at 127.0.0.1 to reach this API.
  2. Body size limit    - POST bodies over 4 KB are rejected (413).
  3. Cross-site writes  - POSTs from another website's pages are refused (Origin check, 403) and
                          must be JSON (415), so a page can't submit hidden forms to this API.
  4. Rate limiting      - per client IP, sliding window; tighter for endpoints that call
                          upstream APIs (custom lat/lon) or write data. Returns 429 + Retry-After.
  5. Security headers   - nosniff, no framing, CSP (per-request nonce for the dashboard script),
                          permissions policy, HSTS when served over HTTPS.

Configuration (.env, all optional):
  ALLOWED_HOSTS    extra host names, comma-separated      (default: 127.0.0.1,localhost)
  ALLOWED_ORIGINS  other sites allowed to call the API    (default: none = same-origin only)
  ADMIN_KEY        secret for admin endpoints via the X-Admin-Key header
                   (if unset, admin endpoints answer only to requests from this PC)
  TRUST_PROXY=1    behind a reverse proxy: take client IP / scheme from X-Forwarded-* headers
  ENABLE_API_DOCS=1  expose /docs (Swagger UI); off by default
"""
import hmac
import os
import secrets
import time
from collections import deque

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

_split = lambda v: [x.strip() for x in v.split(",") if x.strip()]
ALLOWED_HOSTS = ["127.0.0.1", "localhost"] + _split(os.getenv("ALLOWED_HOSTS", ""))
ALLOWED_ORIGINS = _split(os.getenv("ALLOWED_ORIGINS", ""))
ADMIN_KEY = os.getenv("ADMIN_KEY", "").strip()
TRUST_PROXY = os.getenv("TRUST_PROXY", "0").strip() == "1"
ENABLE_API_DOCS = os.getenv("ENABLE_API_DOCS", "0").strip() == "1"
MAX_BODY_BYTES = 4096
LOOPBACK = {"127.0.0.1", "::1", "localhost"}

# (name, matcher, max requests, window seconds) - first matching rule applies
RATE_RULES = [
    ("upstream", lambda r: r.url.path == "/api/point"
     or (r.url.path == "/api/live" and "lat" in r.query_params), 12, 60),
    ("write", lambda r: r.method == "POST", 6, 60),
    ("api", lambda r: r.url.path.startswith("/api/"), 180, 60),
    ("page", lambda r: True, 120, 60),
]


def client_ip(request: Request) -> str:
    if TRUST_PROXY:
        fwd = request.headers.get("x-forwarded-for", "")
        if fwd:
            return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def is_https(request: Request) -> bool:
    if TRUST_PROXY and request.headers.get("x-forwarded-proto", "").lower() == "https":
        return True
    return request.url.scheme == "https"


def is_admin(request: Request) -> bool:
    """Admin = correct X-Admin-Key (constant-time compare), or - when no ADMIN_KEY is configured -
    a request coming from this PC itself."""
    if ADMIN_KEY:
        return hmac.compare_digest(request.headers.get("x-admin-key", ""), ADMIN_KEY)
    return client_ip(request) in LOOPBACK


class RateLimiter:
    def __init__(self):
        self.hits: dict[tuple, deque] = {}
        self.last_sweep = time.monotonic()

    def check(self, key: tuple, limit: int, window: int) -> int:
        """0 if allowed, else seconds until the next request would be allowed."""
        now = time.monotonic()
        q = self.hits.setdefault(key, deque())
        while q and now - q[0] >= window:
            q.popleft()
        if len(q) >= limit:
            return max(1, int(window - (now - q[0])) + 1)
        q.append(now)
        if now - self.last_sweep > 300:  # forget idle clients so memory stays bounded
            self.hits = {k: v for k, v in self.hits.items() if v and now - v[-1] < 600}
            self.last_sweep = now
        return 0


_limiter = RateLimiter()


def _same_site(origin: str, request: Request) -> bool:
    host = request.headers.get("host", "")
    return origin in ALLOWED_ORIGINS or origin.split("://", 1)[-1] == host


def csp_for_dashboard(nonce: str) -> str:
    return "; ".join([
        "default-src 'self'",
        f"script-src 'self' 'nonce-{nonce}' https://unpkg.com https://cdn.jsdelivr.net",  # Leaflet + Chart.js
        "style-src 'self' 'unsafe-inline' https://unpkg.com",          # inline style="" attributes
        "img-src 'self' data: https://tile.openstreetmap.org https://tilecache.rainviewer.com https://unpkg.com",
        "connect-src 'self'",
        "font-src 'self'",
        "object-src 'none'",
        "base-uri 'none'",
        "form-action 'self'",
        "frame-ancestors 'none'",
    ])


class SecurityMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # 1. trusted host (DNS-rebinding protection)
        host = request.headers.get("host", "").rsplit(":", 1)[0].strip("[]").lower()
        if host not in ALLOWED_HOSTS:
            return JSONResponse({"detail": "Host not allowed"}, status_code=400)

        # 2 + 3. writes: size, origin, content type
        if request.method not in ("GET", "HEAD", "OPTIONS"):
            if int(request.headers.get("content-length") or 0) > MAX_BODY_BYTES:
                return JSONResponse({"detail": "Request body too large"}, status_code=413)
            origin = request.headers.get("origin")
            if origin and not _same_site(origin, request):
                return JSONResponse({"detail": "Cross-site request refused"}, status_code=403)
            if not request.headers.get("content-type", "").startswith("application/json"):
                return JSONResponse({"detail": "Content-Type must be application/json"}, status_code=415)

        # 4. rate limit
        name, _, limit, window = next(r for r in RATE_RULES if r[1](request))
        wait = _limiter.check((client_ip(request), name), limit, window)
        if wait:
            return JSONResponse({"detail": f"Too many requests - try again in {wait} s."},
                                status_code=429, headers={"Retry-After": str(wait)})

        # 5. security headers
        request.state.csp_nonce = secrets.token_urlsafe(16)
        response = await call_next(request)
        h = response.headers
        h["X-Content-Type-Options"] = "nosniff"
        h["X-Frame-Options"] = "DENY"
        h["Referrer-Policy"] = "strict-origin-when-cross-origin"   # map tiles (OSM) need the origin
        h["Permissions-Policy"] = "geolocation=(self), camera=(), microphone=(), payment=(), usb=()"
        h["Cross-Origin-Opener-Policy"] = "same-origin"
        if request.url.path in ("/docs", "/redoc") or request.url.path.startswith("/docs/"):
            pass  # Swagger UI loads its own CDN assets; only reachable when ENABLE_API_DOCS=1
        elif response.headers.get("content-type", "").startswith("text/html"):
            h["Content-Security-Policy"] = csp_for_dashboard(request.state.csp_nonce)
        else:
            h["Content-Security-Policy"] = "default-src 'none'; frame-ancestors 'none'"
        if is_https(request):
            h["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        return response
