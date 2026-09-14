# Security notes

FAILFLUME is designed as a **single-machine local application**.

## Expected trust boundary

- Run the scanner from the provided launcher or `python app.py`.
- Use `http://127.0.0.1:8765` (or localhost equivalent).
- Do not expose the scanner to a LAN, public interface, reverse proxy, or the internet. The application refuses non-loopback binds.
- Do not open `static/index.html` directly with `file://`; the API is intentionally same-origin-only.

## Browser/API protections retained in v1.7.2.7

- no wildcard CORS;
- local Host validation;
- foreign Origin rejection for API requests;
- JSON-only POST API with a 64 KiB request cap;
- CSP, anti-framing, no-sniff, referrer, opener/resource and permissions headers;
- no absolute database path in `/api/health`;
- no Python version in the HTTP `Server` header.

## Local data

Outbound current-slate requests are limited to fixed public providers used by the application (MLB StatsAPI, Baseball Savant, and the RotoWire daily-lineups page); user input is not used to choose arbitrary remote hosts.

Runtime data is written under `failflume_batter_scanner/data/`. The repository `.gitignore` excludes that directory so local SQLite databases, downloaded snapshots and state caches are not committed accidentally.

## Reporting

If testing reveals a security issue, report the exact application version, request/response or reproduction steps, and whether the scanner was running on its default loopback address. Avoid publishing local database contents or personal filesystem paths in public issues.
