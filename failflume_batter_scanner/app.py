#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import io
import json
import math
import os
import sqlite3
import threading
import time
import webbrowser
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent
STATIC_DIR = ROOT / "static"
DATA_DIR = ROOT / "data"
CACHE_DIR = DATA_DIR / "http_cache"
DB_PATH = DATA_DIR / "failflume.sqlite3"
LEGACY_STATE_CACHE_PATH = DATA_DIR / "last_state.json"
STATE_CACHE_DIR = DATA_DIR / "state_cache"
RAW_STATCAST_DIR = DATA_DIR / "statcast_raw"

MLB_BASE = "https://statsapi.mlb.com/api/v1"
MLB_LIVE_BASE = "https://statsapi.mlb.com/api/v1.1"
SAVANT_BASE = "https://baseballsavant.mlb.com/statcast_search/csv"
USER_AGENT = "FailflumeBatterScanner/1.7.2.4 (local analytics app)"
APP_VERSION = "1.7.2.4"
SCORING_VERSION = "1.7.1-generative-simplicity-freeze-v1"
DEEP_MODEL_VERSION = "1.7.1-pa-dirichlet-simplicity-freeze-v1"
# Network politeness is enforced globally per host, not merely by UI convention.
# Savant is the expensive historical source: one date/request, sequential, >= 3 s apart.
# MLB StatsAPI request starts are globally spaced and cached; two workers only hide latency.
SAVANT_DELAY = max(3.0, float(os.getenv("FAILFLUME_SAVANT_DELAY", "3.0")))
MLB_MIN_INTERVAL = max(0.50, float(os.getenv("FAILFLUME_MLB_MIN_INTERVAL", "0.50")))
MLB_WORKERS = max(1, min(int(os.getenv("FAILFLUME_MLB_WORKERS", "2")), 2))
FORCE_CACHE_FLOOR_SECONDS = max(15, int(os.getenv("FAILFLUME_FORCE_CACHE_FLOOR", "30")))
STATE_TTL_SECONDS = int(os.getenv("FAILFLUME_STATE_TTL", "300"))
AUTO_BACKFILL_DAYS = max(0, min(int(os.getenv("FAILFLUME_AUTO_BACKFILL_DAYS", "90")), 370))
PROVISIONAL_LOOKBACK_DAYS = max(1, min(int(os.getenv("FAILFLUME_PROVISIONAL_DAYS", "2")), 7))
DEFAULT_DECAY = float(os.getenv("FAILFLUME_RECENCY_DECAY", "0.82"))
DEFAULT_MIN_PA = int(os.getenv("FAILFLUME_MIN_PA", "12"))

HIT_EVENTS = {"single": 1, "double": 2, "triple": 3, "home_run": 4}
NON_AB_EVENTS = {
    "walk", "intent_walk", "hit_by_pitch", "sac_fly", "sac_bunt",
    "catcher_interf", "catcher_interference", "sac_fly_double_play",
}
STRIKEOUT_EVENTS = {"strikeout", "strikeout_double_play"}
SWING_DESCRIPTIONS = {
    "hit_into_play", "hit_into_play_no_out", "hit_into_play_score",
    "foul", "foul_tip", "swinging_strike", "swinging_strike_blocked",
    "missed_bunt", "foul_bunt",
}
WHIFF_DESCRIPTIONS = {"swinging_strike", "swinging_strike_blocked", "missed_bunt"}

CONTACT_WEIGHTS = {
    "Recent H/AB": 0.22,
    "Hit-game rate": 0.15,
    "Local baseline": 0.10,
    "Handedness split": 0.14,
    "xBA": 0.12,
    "Avoids strikeouts": 0.11,
    "Hard-hit rate": 0.04,
    "Starter matchup": 0.09,
    "Lineup slot": 0.03,
}
HR_WEIGHTS = {
    "Recent HR/PA": 0.24,
    "Recent extra TB/AB": 0.12,
    "Local HR baseline": 0.07,
    "Handedness HR split": 0.11,
    "Barrel rate": 0.16,
    "Hard-hit rate": 0.07,
    "Sweet-spot rate": 0.06,
    "Starter matchup": 0.15,
    "Lineup slot": 0.02,
}

# GOAL scoring is target-specific. Bounds are deliberately broad baseball ranges used
# only to map unlike raw statistics onto a common 0-100 ranking scale; they are not
# presented as calibrated probabilities. Multi-hit / multi-base targets use their own
# attainment, target-volume and upper-tail signals rather than inheriting the 1+ model.
GOAL_RATE_BOUNDS = {
    "hits": {1: (0.30, 0.85), 2: (0.03, 0.45), 3: (0.00, 0.20), 4: (0.00, 0.08), 5: (0.00, 0.04)},
    "total_bases": {1: (0.30, 0.85), 2: (0.08, 0.58), 3: (0.03, 0.42), 4: (0.02, 0.32), 5: (0.01, 0.24)},
    "home_runs": {1: (0.015, 0.25), 2: (0.00, 0.075), 3: (0.00, 0.025), 4: (0.00, 0.010), 5: (0.00, 0.006)},
    "extra_base_hits": {1: (0.04, 0.38), 2: (0.00, 0.14), 3: (0.00, 0.055), 4: (0.00, 0.025), 5: (0.00, 0.012)},
}
GOAL_UNIT_BOUNDS = {
    "hits": {1: (0.20, 1.80), 2: (0.00, 0.90), 3: (0.00, 0.38), 4: (0.00, 0.16)},
    "total_bases": {1: (0.25, 3.20), 2: (0.00, 2.40), 3: (0.00, 1.80), 4: (0.00, 1.40)},
    "home_runs": {1: (0.00, 0.32), 2: (0.00, 0.10), 3: (0.00, 0.04), 4: (0.00, 0.02)},
    "extra_base_hits": {1: (0.02, 0.52), 2: (0.00, 0.20), 3: (0.00, 0.08), 4: (0.00, 0.04)},
}

_backfill_lock = threading.Lock()
_backfill_status: dict[str, Any] = {
    "running": False,
    "mode": None,
    "done": 0,
    "total": 0,
    "current_date": None,
    "message": "Idle",
    "error": None,
    "started_at": None,
    "finished_at": None,
    "failed": 0,
    "errors": [],
}
_state_lock = threading.Lock()
_fit_lock = threading.Lock()
DEEP_TUNING_VERSION = "1.7.1-dual-rolling-simplicity-gate-v1"
BACKTEST_VERSION = "1.7.1-development-holdout-simplicity-freeze-v1"
_backtest_lock = threading.Lock()
_backtest_status: dict[str, Any] = {
    "running": False, "message": "Idle", "done": 0, "total": 0,
    "current_date": None, "error": None, "run_id": None, "run_key": None,
    "started_at": None, "finished_at": None, "predictions": 0,
}
AUDIT_VERSION = "1.7.1-historical-audit-simplicity-freeze-v1"
PROSPECTIVE_START = "2026-09-14"
DATA_BRIDGE_PROSPECTIVE_START = "2026-09-15"
_audit_lock = threading.Lock()
_audit_status: dict[str, Any] = {
    "running": False, "message": "Idle", "done": 0, "total": 0,
    "current_window": None, "current_date": None, "error": None, "run_id": None,
    "started_at": None, "finished_at": None, "predictions": 0,
}



def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    RAW_STATCAST_DIR.mkdir(parents=True, exist_ok=True)


def db_connect() -> sqlite3.Connection:
    ensure_dirs()
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


def init_db() -> None:
    with db_connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS pitches (
                game_date TEXT NOT NULL,
                game_pk INTEGER NOT NULL,
                at_bat_number INTEGER NOT NULL,
                pitch_number INTEGER NOT NULL,
                batter INTEGER,
                pitcher INTEGER,
                events TEXT,
                description TEXT,
                stand TEXT,
                p_throws TEXT,
                home_team TEXT,
                away_team TEXT,
                inning_topbot TEXT,
                launch_speed REAL,
                launch_angle REAL,
                estimated_ba REAL,
                estimated_woba REAL,
                barrel INTEGER,
                bb_type TEXT,
                hit_distance REAL,
                source TEXT NOT NULL DEFAULT 'savant',
                PRIMARY KEY (game_pk, at_bat_number, pitch_number)
            );
            CREATE TABLE IF NOT EXISTS fetched_dates (
                game_date TEXT PRIMARY KEY,
                row_count INTEGER NOT NULL,
                fetched_at TEXT NOT NULL,
                status TEXT NOT NULL,
                note TEXT
            );
            CREATE TABLE IF NOT EXISTS fetch_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_date TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                fetched_at TEXT NOT NULL,
                status TEXT NOT NULL,
                note TEXT,
                raw_snapshot TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_fetch_history_date ON fetch_history(game_date, status);
            CREATE INDEX IF NOT EXISTS idx_pitches_batter_date ON pitches(batter, game_date);
            CREATE INDEX IF NOT EXISTS idx_pitches_pitcher_date ON pitches(pitcher, game_date);
            CREATE INDEX IF NOT EXISTS idx_pitches_events ON pitches(events);
            CREATE INDEX IF NOT EXISTS idx_pitches_game_date ON pitches(game_date);
            CREATE TABLE IF NOT EXISTS model_fits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tuning_version TEXT NOT NULL,
                coverage_signature TEXT NOT NULL,
                before_day TEXT NOT NULL,
                fitted_at TEXT NOT NULL,
                params_json TEXT NOT NULL,
                metrics_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_model_fits_lookup
                ON model_fits(tuning_version, coverage_signature, before_day, id);
            CREATE TABLE IF NOT EXISTS backtest_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                backtest_version TEXT NOT NULL,
                model_version TEXT NOT NULL,
                tuning_version TEXT NOT NULL,
                coverage_signature TEXT NOT NULL,
                holdout_start TEXT NOT NULL,
                holdout_end TEXT NOT NULL,
                holdout_days INTEGER NOT NULL,
                min_prior_pa INTEGER NOT NULL,
                context_mode TEXT NOT NULL,
                created_at TEXT NOT NULL,
                tuning_json TEXT NOT NULL,
                context_json TEXT NOT NULL,
                metrics_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_backtest_runs_created ON backtest_runs(id, created_at);
            CREATE TABLE IF NOT EXISTS backtest_predictions (
                run_id INTEGER NOT NULL,
                game_date TEXT NOT NULL,
                game_pk INTEGER NOT NULL,
                batter INTEGER NOT NULL,
                pitcher INTEGER,
                team TEXT,
                opponent TEXT,
                lineup_order INTEGER,
                pitcher_hand TEXT,
                prior_pa INTEGER NOT NULL,
                actual_pa INTEGER NOT NULL,
                actual_h INTEGER NOT NULL,
                actual_tb INTEGER NOT NULL,
                actual_hr INTEGER NOT NULL,
                actual_xbh INTEGER NOT NULL,
                probabilities_json TEXT NOT NULL,
                PRIMARY KEY (run_id, game_date, game_pk, batter)
            );
            CREATE INDEX IF NOT EXISTS idx_backtest_predictions_run ON backtest_predictions(run_id, game_date);
            CREATE TABLE IF NOT EXISTS historical_audit_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                audit_version TEXT NOT NULL,
                model_version TEXT NOT NULL,
                tuning_version TEXT NOT NULL,
                coverage_signature TEXT NOT NULL,
                windows INTEGER NOT NULL,
                window_days INTEGER NOT NULL,
                min_prior_pa INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                config_json TEXT NOT NULL,
                result_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_historical_audit_runs_created ON historical_audit_runs(id, created_at);
            """
        )
        # Append-only schema extension: preserve slice/context diagnostics in future
        # raw exports without rewriting any existing backtest prediction row.
        existing_cols={str(r[1]) for r in conn.execute("PRAGMA table_info(backtest_predictions)").fetchall()}
        for name,decl in {
            "hand_pa":"INTEGER", "pitcher_pa":"INTEGER", "expected_pa":"REAL",
            "recent_hit_delta":"REAL", "recent_form_band":"TEXT", "prior_band":"TEXT", "rho":"REAL",
        }.items():
            if name not in existing_cols:
                conn.execute(f"ALTER TABLE backtest_predictions ADD COLUMN {name} {decl}")

        # v1.7.2 source provenance. Existing rows are canonical Savant history.
        # Only the short-lived StatsAPI D-1 overlay is replaceable; canonical
        # Savant rows remain the durable historical source.
        pitch_cols={str(r[1]) for r in conn.execute("PRAGMA table_info(pitches)").fetchall()}
        if "source" not in pitch_cols:
            conn.execute("ALTER TABLE pitches ADD COLUMN source TEXT NOT NULL DEFAULT 'savant'")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pitches_source_date ON pitches(source, game_date)")


def clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def scale(v: float | None, lo: float, hi: float, default: float = 50.0) -> float:
    if v is None or not math.isfinite(v):
        return default
    if hi == lo:
        return default
    return clamp((v - lo) / (hi - lo) * 100.0)


def safe_float(v: Any) -> float | None:
    try:
        if v is None:
            return None
        s = str(v).strip()
        if not s or s.lower() in {"nan", "null", "none"}:
            return None
        x = float(s)
        return x if math.isfinite(x) else None
    except (ValueError, TypeError):
        return None


def safe_int(v: Any) -> int | None:
    try:
        if v is None or str(v).strip() == "":
            return None
        return int(float(v))
    except (ValueError, TypeError):
        return None


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


_rate_lock = threading.Lock()
_host_next_allowed: dict[str, float] = {}
_host_block_until: dict[str, float] = {}

def _host_interval(host: str) -> float:
    host = host.lower()
    if host == "baseballsavant.mlb.com":
        return SAVANT_DELAY
    if host == "statsapi.mlb.com":
        return MLB_MIN_INTERVAL
    return 0.0

def _wait_for_host_slot(host: str) -> None:
    """Globally space request *starts* per host, including concurrent worker calls."""
    interval = _host_interval(host)
    if interval <= 0:
        return
    while True:
        with _rate_lock:
            now = time.monotonic()
            ready = max(_host_next_allowed.get(host, 0.0), _host_block_until.get(host, 0.0))
            if now >= ready:
                _host_next_allowed[host] = now + interval
                return
            wait = ready - now
        time.sleep(min(wait, 5.0))

def _defer_host(host: str, seconds: float) -> None:
    """Apply a shared cooldown after a server-side throttle response."""
    with _rate_lock:
        until = time.monotonic() + max(0.0, seconds)
        _host_block_until[host] = max(_host_block_until.get(host, 0.0), until)
        _host_next_allowed[host] = max(_host_next_allowed.get(host, 0.0), until)

def http_get_bytes(url: str, timeout: int = 60, retries: int = 3) -> bytes:
    last_error: Exception | None = None
    host = (urlparse(url).netloc or url).lower()
    for attempt in range(retries):
        _wait_for_host_slot(host)
        req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
        try:
            with urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except HTTPError as exc:
            last_error = exc
            if exc.code == 429:
                # Respect Retry-After when supplied; otherwise back off hard rather than
                # repeatedly testing the provider's limit. Cooldown is shared by all workers.
                retry_after = safe_int(exc.headers.get("Retry-After")) or min(120, 15 * (2 ** attempt))
                _defer_host(host, retry_after)
                if attempt + 1 < retries:
                    continue
                raise RuntimeError(f"HTTP 429 from {host}; paused requests for {retry_after}s") from exc
            if 500 <= exc.code < 600 and attempt + 1 < retries:
                cooldown = min(30, 2 ** attempt)
                _defer_host(host, cooldown)
                continue
            raise RuntimeError(f"HTTP {exc.code} from {host}: {exc.reason}") from exc
        except URLError as exc:
            last_error = exc
            if attempt + 1 < retries:
                _defer_host(host, min(15, 2 ** attempt))
                continue
            reason = getattr(exc, "reason", exc)
            raise RuntimeError(f"Network error contacting {host}: {reason}") from exc
        except (TimeoutError, OSError) as exc:
            last_error = exc
            if attempt + 1 < retries:
                _defer_host(host, min(15, 2 ** attempt))
                continue
            raise RuntimeError(f"Network error contacting {host}: {exc}") from exc
    if last_error:
        raise RuntimeError(f"Network error contacting {host}: {last_error}") from last_error
    raise RuntimeError(f"HTTP request to {host} failed")


def cache_key_for(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def legacy_cache_path_for(url: str) -> Path:
    return CACHE_DIR / f"{cache_key_for(url)}.json"


def cache_snapshot_dir_for(url: str) -> Path:
    return CACHE_DIR / cache_key_for(url)


def newest_json_cache(url: str) -> Path | None:
    candidates: list[Path] = []
    legacy = legacy_cache_path_for(url)
    if legacy.exists():
        candidates.append(legacy)
    d = cache_snapshot_dir_for(url)
    if d.exists():
        candidates.extend(x for x in d.glob("*.json") if x.is_file())
    return max(candidates, key=lambda x: x.stat().st_mtime) if candidates else None


def append_json_cache_snapshot(url: str, raw: bytes) -> Path:
    d = cache_snapshot_dir_for(url)
    d.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(raw).hexdigest()[:16]
    # Timestamp + content digest: every refresh is append-only; no previous response is replaced.
    stamp = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%f%z")
    p = d / f"{stamp}-{digest}.json"
    with p.open("xb") as fh:
        fh.write(raw)
    return p


def fetch_json(url: str, ttl: int = 3600, force: bool = False) -> Any:
    ensure_dirs()
    p = newest_json_cache(url)
    age = (time.time() - p.stat().st_mtime) if p else None
    # Even a manual FORCE cannot hammer the same endpoint repeatedly. A very fresh
    # immutable snapshot is reused for a short floor window; after that, force works.
    cache_limit = FORCE_CACHE_FLOOR_SECONDS if force else ttl
    if p and age is not None and age < cache_limit:
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    try:
        raw = http_get_bytes(url, timeout=45)
    except Exception:
        # Immutable cache fallback: if MLB is temporarily unreachable, reuse the newest
        # previously saved response and recompute all derived scoring locally. Never return
        # an old derived score table merely because the network is down.
        if p:
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                pass
        raise
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        host = urlparse(url).netloc or url
        raise RuntimeError(f"Invalid JSON returned by {host}") from exc
    # Validate before preserving the immutable response snapshot.
    append_json_cache_snapshot(url, raw)
    return obj


def mlb_url(path: str, **params: Any) -> str:
    clean = {k: v for k, v in params.items() if v is not None}
    return f"{MLB_BASE}{path}?{urlencode(clean, doseq=True)}"


def mlb_live_url(path: str, **params: Any) -> str:
    """StatsAPI live game feeds are served from the v1.1 API surface."""
    clean = {k: v for k, v in params.items() if v is not None}
    suffix = f"?{urlencode(clean, doseq=True)}" if clean else ""
    return f"{MLB_LIVE_BASE}{path}{suffix}"


def get_teams(season: int, force: bool = False) -> dict[int, dict[str, Any]]:
    data = fetch_json(mlb_url("/teams", sportId=1, season=season), ttl=86400, force=force)
    result = {}
    for t in data.get("teams", []):
        result[int(t["id"])] = {
            "id": int(t["id"]),
            "name": t.get("name", ""),
            "abbreviation": t.get("abbreviation") or t.get("teamCode", "???").upper(),
        }
    return result


def get_schedule(day: str, force: bool = False) -> list[dict[str, Any]]:
    url = mlb_url("/schedule", sportId=1, date=day, hydrate="team,probablePitcher")
    data = fetch_json(url, ttl=600, force=force)
    games = []
    for d in data.get("dates", []):
        games.extend(d.get("games", []))
    return games


def get_boxscore(game_pk: int, force: bool = False) -> dict[str, Any]:
    return fetch_json(mlb_url(f"/game/{game_pk}/boxscore"), ttl=300, force=force)


def get_roster(team_id: int, day: str, force: bool = False) -> dict[str, Any]:
    return fetch_json(mlb_url(f"/teams/{team_id}/roster", rosterType="active", date=day), ttl=21600, force=force)


def get_pitcher_profile(player_id: int, season: int, force: bool = False) -> dict[str, Any]:
    hydrate = f"stats(group=[pitching],type=[season],season={season})"
    data = fetch_json(mlb_url(f"/people/{player_id}", hydrate=hydrate), ttl=21600, force=force)
    people = data.get("people", [])
    if not people:
        return {"id": player_id, "name": "Unknown", "hand": "?", "era": None}
    p = people[0]
    era = None
    for block in p.get("stats", []):
        for split in block.get("splits", []):
            stat = split.get("stat", {})
            if stat.get("era") is not None:
                era = safe_float(stat.get("era"))
                break
    return {
        "id": player_id,
        "name": p.get("fullName", "Unknown"),
        "hand": (p.get("pitchHand") or {}).get("code", "?"),
        "era": era,
    }


def lineup_from_boxscore(box: dict[str, Any], side: str) -> list[dict[str, Any]]:
    team = (box.get("teams") or {}).get(side) or {}
    players = team.get("players") or {}
    batting_order = team.get("battingOrder") or []
    result = []
    for idx, pid in enumerate(batting_order[:9], start=1):
        entry = players.get(f"ID{pid}", {})
        person = entry.get("person") or {}
        pos = entry.get("position") or {}
        result.append({
            "id": int(pid),
            "name": person.get("fullName", f"Player {pid}"),
            "position": pos.get("abbreviation", ""),
            "lineup_order": idx,
            "confirmed": True,
        })
    return result


def hitters_from_roster(roster: dict[str, Any]) -> list[dict[str, Any]]:
    result = []
    for row in roster.get("roster", []):
        person = row.get("person") or {}
        pos = row.get("position") or {}
        abbr = pos.get("abbreviation", "")
        typ = pos.get("type", "")
        if typ == "Pitcher" or abbr == "P":
            continue
        pid = person.get("id")
        if not pid:
            continue
        result.append({
            "id": int(pid),
            "name": person.get("fullName", f"Player {pid}"),
            "position": abbr,
            "lineup_order": None,
            "confirmed": False,
        })
    return result


def build_daily_candidates(day: str, force: bool = False) -> tuple[list[dict[str, Any]], dict[int, dict[str, Any]], list[dict[str, Any]]]:
    season = int(day[:4])
    teams = get_teams(season, force=force)
    games = get_schedule(day, force=force)
    if not games:
        return [], {}, []

    boxes: dict[int, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=MLB_WORKERS) as pool:
        futs = {pool.submit(get_boxscore, int(g["gamePk"]), force): int(g["gamePk"]) for g in games}
        for fut in as_completed(futs):
            try:
                boxes[futs[fut]] = fut.result()
            except Exception:
                boxes[futs[fut]] = {}

    roster_needs: set[int] = set()
    game_rows: list[dict[str, Any]] = []
    preliminary: list[dict[str, Any]] = []
    pitcher_ids: set[int] = set()

    for g in games:
        game_pk = int(g["gamePk"])
        box = boxes.get(game_pk, {})
        game_teams = g.get("teams") or {}
        home_obj = (game_teams.get("home") or {}).get("team") or {}
        away_obj = (game_teams.get("away") or {}).get("team") or {}
        home_id = int(home_obj.get("id"))
        away_id = int(away_obj.get("id"))
        home_prob = (game_teams.get("home") or {}).get("probablePitcher") or {}
        away_prob = (game_teams.get("away") or {}).get("probablePitcher") or {}
        home_pitcher_id = safe_int(home_prob.get("id"))
        away_pitcher_id = safe_int(away_prob.get("id"))
        if home_pitcher_id:
            pitcher_ids.add(home_pitcher_id)
        if away_pitcher_id:
            pitcher_ids.add(away_pitcher_id)

        home_lineup = lineup_from_boxscore(box, "home")
        away_lineup = lineup_from_boxscore(box, "away")
        if not home_lineup:
            roster_needs.add(home_id)
        if not away_lineup:
            roster_needs.add(away_id)

        row = {
            "game_pk": game_pk,
            "status": ((g.get("status") or {}).get("detailedState") or ""),
            "game_date": g.get("gameDate"),
            "home_team_id": home_id,
            "away_team_id": away_id,
            "home_pitcher_id": home_pitcher_id,
            "away_pitcher_id": away_pitcher_id,
            "home_pitcher_name": home_prob.get("fullName"),
            "away_pitcher_name": away_prob.get("fullName"),
        }
        game_rows.append(row)

        for player in home_lineup:
            preliminary.append({**player, "team_id": home_id, "opp_team_id": away_id, "pitcher_id": away_pitcher_id, "game_pk": game_pk, "park_team_id": home_id})
        for player in away_lineup:
            preliminary.append({**player, "team_id": away_id, "opp_team_id": home_id, "pitcher_id": home_pitcher_id, "game_pk": game_pk, "park_team_id": home_id})

    rosters: dict[int, dict[str, Any]] = {}
    if roster_needs:
        with ThreadPoolExecutor(max_workers=MLB_WORKERS) as pool:
            futs = {pool.submit(get_roster, tid, day, force): tid for tid in roster_needs}
            for fut in as_completed(futs):
                try:
                    rosters[futs[fut]] = fut.result()
                except Exception:
                    rosters[futs[fut]] = {}

    already = {(r["team_id"], r["game_pk"]) for r in preliminary}
    for game in game_rows:
        pairs = [
            (game["home_team_id"], game["away_team_id"], game["away_pitcher_id"]),
            (game["away_team_id"], game["home_team_id"], game["home_pitcher_id"]),
        ]
        for team_id, opp_id, pitcher_id in pairs:
            if (team_id, game["game_pk"]) in already:
                continue
            for player in hitters_from_roster(rosters.get(team_id, {})):
                preliminary.append({**player, "team_id": team_id, "opp_team_id": opp_id, "pitcher_id": pitcher_id, "game_pk": game["game_pk"], "park_team_id": game["home_team_id"]})

    profiles: dict[int, dict[str, Any]] = {}
    if pitcher_ids:
        with ThreadPoolExecutor(max_workers=MLB_WORKERS) as pool:
            futs = {pool.submit(get_pitcher_profile, pid, season, force): pid for pid in pitcher_ids}
            for fut in as_completed(futs):
                try:
                    profiles[futs[fut]] = fut.result()
                except Exception:
                    profiles[futs[fut]] = {"id": futs[fut], "name": "Unknown", "hand": "?", "era": None}

    for p in preliminary:
        team = teams.get(p["team_id"], {})
        opp = teams.get(p["opp_team_id"], {})
        pitcher = profiles.get(p["pitcher_id"] or -1, {})
        p["team"] = team.get("abbreviation", "???")
        p["team_name"] = team.get("name", "")
        p["opp_team"] = opp.get("abbreviation", "???")
        park_team = teams.get(p.get("park_team_id") or -1, {})
        p["park_team"] = park_team.get("abbreviation", "")
        p["pitcher"] = pitcher or {"id": p["pitcher_id"], "name": "TBD", "hand": "?", "era": None}

    return preliminary, profiles, game_rows


def placeholders(ids: list[int]) -> str:
    return ",".join("?" for _ in ids)


def terminal_pa_rows(conn: sqlite3.Connection, ids: list[int], before_day: str, by: str = "batter") -> dict[int, list[dict[str, Any]]]:
    if not ids:
        return {}
    q = f"""
        SELECT game_date, game_pk, batter, pitcher, events, stand, p_throws,
               home_team, away_team, inning_topbot, source,
               estimated_ba, estimated_woba, launch_speed, launch_angle, barrel, bb_type
        FROM pitches
        WHERE {by} IN ({placeholders(ids)})
          AND game_date < ?
          AND events IS NOT NULL AND events <> ''
        ORDER BY game_date, game_pk, at_bat_number
    """
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in conn.execute(q, [*ids, before_day]):
        grouped[int(row[by])].append(dict(row))
    return grouped


def pitch_rows(conn: sqlite3.Connection, ids: list[int], before_day: str, by: str = "batter") -> dict[int, list[dict[str, Any]]]:
    if not ids:
        return {}
    q = f"""
        SELECT game_date, game_pk, batter, pitcher, description, p_throws, source,
               launch_speed, launch_angle, estimated_ba, estimated_woba, barrel, bb_type
        FROM pitches
        WHERE {by} IN ({placeholders(ids)}) AND game_date < ?
        ORDER BY game_date, game_pk, at_bat_number, pitch_number
    """
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in conn.execute(q, [*ids, before_day]):
        grouped[int(row[by])].append(dict(row))
    return grouped


def is_ab(event: str) -> bool:
    return bool(event) and event not in NON_AB_EVENTS


def summarise_games(pa_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    games: dict[tuple[str, int], dict[str, Any]] = {}
    for r in pa_rows:
        key = (r["game_date"], int(r["game_pk"]))
        g = games.setdefault(key, {
            "date": r["game_date"], "game_pk": int(r["game_pk"]),
            "pa": 0, "ab": 0, "h": 0, "tb": 0, "hr": 0, "xbh": 0, "k": 0,
            "bb": 0, "ibb": 0, "hbp": 0,
            "opp": "", "home_away": "", "start_hand": None,
        })
        event = r.get("events") or ""
        if g["start_hand"] is None and r.get("p_throws") in {"L", "R"}:
            # Rows are PA-ordered; the first pitcher faced is a good local proxy for starter hand.
            g["start_hand"] = r.get("p_throws")
        g["pa"] += 1
        if is_ab(event):
            g["ab"] += 1
        if event in HIT_EVENTS:
            g["h"] += 1
            g["tb"] += HIT_EVENTS[event]
        if event == "home_run":
            g["hr"] += 1
        if event in {"double", "triple", "home_run"}:
            g["xbh"] += 1
        if event in STRIKEOUT_EVENTS:
            g["k"] += 1
        if event in {"walk", "intent_walk"}:
            g["bb"] += 1
        if event == "intent_walk":
            g["ibb"] += 1
        if event == "hit_by_pitch":
            g["hbp"] += 1
        top = (r.get("inning_topbot") or "").lower() == "top"
        g["opp"] = r.get("home_team") if top else r.get("away_team")
        g["home_away"] = "@" if top else "vs"
    return sorted(games.values(), key=lambda x: (x["date"], x["game_pk"]))


def weighted_rate(games: list[dict[str, Any]], numerator: str, denominator: str, decay: float, n: int = 10) -> float | None:
    subset = list(reversed(games[-n:]))
    num = 0.0
    den = 0.0
    for i, g in enumerate(subset):
        w = decay ** i
        num += w * g.get(numerator, 0)
        den += w * g.get(denominator, 0)
    return (num / den) if den else None


def weighted_extra_tb_ab(games: list[dict[str, Any]], decay: float, n: int = 10) -> float | None:
    subset = list(reversed(games[-n:]))
    num = den = 0.0
    for i, g in enumerate(subset):
        w = decay ** i
        # TB - H removes the one base supplied by each hit: singles become zero;
        # doubles/triples/HR contribute only their extra-base damage.
        num += w * max(int(g.get("tb", 0) or 0) - int(g.get("h", 0) or 0), 0)
        den += w * int(g.get("ab", 0) or 0)
    return (num / den) if den else None


def weighted_binary_hit_rate(games: list[dict[str, Any]], decay: float, n: int = 10) -> float | None:
    subset = list(reversed(games[-n:]))
    num = 0.0
    den = 0.0
    for i, g in enumerate(subset):
        w = decay ** i
        num += w * (1 if g.get("h", 0) > 0 else 0)
        den += w
    return (num / den) if den else None


def weighted_goal_rate(
    games: list[dict[str, Any]], field: str, target: int, decay: float, hand: str | None = None, n: int = 20
) -> tuple[float | None, int]:
    selected = [g for g in games if hand is None or g.get("start_hand") == hand][-n:]
    if not selected:
        return None, 0
    num = 0.0
    den = 0.0
    for i, g in enumerate(reversed(selected)):
        w = decay ** i
        num += w * (1.0 if int(g.get(field, 0) or 0) >= target else 0.0)
        den += w
    return ((num / den) if den else None), len(selected)


def goal_rate_table(games: list[dict[str, Any]], decay: float) -> dict[str, Any]:
    fields = {
        "hits": "h",
        "total_bases": "tb",
        "home_runs": "hr",
        "extra_base_hits": "xbh",
    }
    out: dict[str, Any] = {}
    for metric, field in fields.items():
        out[metric] = {}
        for target in range(1, 5):
            all_rate, all_n = weighted_goal_rate(games, field, target, decay, None)
            prior = all_rate if all_rate is not None else 0.0
            row: dict[str, Any] = {
                "ALL": {"rate": round(prior, 4), "games": all_n},
            }
            for hand in ("L", "R"):
                raw, n_hand = weighted_goal_rate(games, field, target, decay, hand)
                if raw is None:
                    shrunk = prior
                else:
                    # Six-game prior keeps tiny handedness samples from dominating the goal list.
                    shrunk = (raw * n_hand + prior * 6.0) / (n_hand + 6.0)
                row[hand] = {"rate": round(shrunk, 4), "raw_rate": round(raw or 0.0, 4), "games": n_hand}
            out[metric][str(target)] = row
    return out


def _goal_opportunity_weight(game: dict[str, Any]) -> float:
    """Down-weight pinch-hit/very-short appearances when modelling full-game batter props."""
    pa = int(game.get("pa", 0) or 0)
    if pa <= 0:
        return 0.0
    return min(1.0, max(0.25, pa / 4.0))


def _weighted_game_value(
    games: list[dict[str, Any]], field: str, decay: float | None, n: int | None,
    hand: str | None, transform
) -> tuple[float, int]:
    selected = [g for g in games if hand is None or g.get("start_hand") == hand]
    if n is not None:
        selected = selected[-n:]
    if not selected:
        return 0.0, 0
    num = den = 0.0
    for i, g in enumerate(reversed(selected)):
        recency_w = (decay ** i) if decay is not None else 1.0
        w = recency_w * _goal_opportunity_weight(g)
        num += w * float(transform(int(g.get(field, 0) or 0)))
        den += w
    return ((num / den) if den else 0.0), len(selected)


def _plain_goal_rate(games: list[dict[str, Any]], field: str, target: int, hand: str | None) -> tuple[float, int]:
    selected = [g for g in games if hand is None or g.get("start_hand") == hand]
    if not selected:
        return 0.0, 0
    hits = sum(1 for g in selected if int(g.get(field, 0) or 0) >= target)
    return hits / len(selected), len(selected)


def weighted_goal_features(
    games: list[dict[str, Any]], field: str, target: int, decay: float, hand: str | None = None
) -> dict[str, float | int]:
    # v0.8 deliberately separates three questions:
    #   1) is he doing THIS target lately?         -> recent_rate
    #   2) has he historically done THIS target?  -> long_rate over ALL cached games
    #   3) when he gets there, does he clear it hard? -> target_units / tail_rate
    # Short appearances are down-weighted because a one-PA pinch hit is not comparable to
    # tonight's projected full game, especially for 2+/3+ hit or base targets.
    recent_rate, recent_n = _weighted_game_value(
        games, field, decay, 10, hand, lambda v: 1.0 if v >= target else 0.0
    )
    long_rate, long_n = _weighted_game_value(
        games, field, None, None, hand, lambda v: 1.0 if v >= target else 0.0
    )
    raw_long_rate, _ = _plain_goal_rate(games, field, target, hand)

    # ZERO credit below the selected threshold.  For 2+ hits:
    # 1 H -> 0, 2 H -> 1, 3 H -> 2, 4 H -> 3.
    target_units, units_n = _weighted_game_value(
        games, field, decay, 12, hand, lambda v: float(max(v - target + 1, 0))
    )
    tail_rate, tail_n = _weighted_game_value(
        games, field, decay, 15, hand, lambda v: 1.0 if v >= target + 1 else 0.0
    )
    near_target = max(1, target - 1)
    near_rate, near_n = _weighted_game_value(
        games, field, decay, 15, hand, lambda v: 1.0 if v >= near_target else 0.0
    )
    known_hand_games = sum(1 for g in games if g.get("start_hand") in {"L", "R"})
    return {
        "recent_rate": recent_rate,
        "long_rate": long_rate,
        "raw_long_rate": raw_long_rate,
        "target_units": target_units,
        "tail_rate": tail_rate,
        "near_rate": near_rate,
        "recent_games": recent_n,
        "games": long_n,
        "units_games": units_n,
        "tail_games": tail_n,
        "near_games": near_n,
        "known_hand_games": known_hand_games,
    }


GOAL_SPLIT_PRIOR_N = {
    # Rarer outcomes need more evidence before a handedness split is allowed to move far
    # from the player's all-hand target history.
    "hits": {1: 12.0, 2: 18.0, 3: 26.0, 4: 34.0},
    "total_bases": {1: 12.0, 2: 18.0, 3: 24.0, 4: 30.0},
    "home_runs": {1: 30.0, 2: 45.0, 3: 60.0, 4: 75.0},
    "extra_base_hits": {1: 22.0, 2: 32.0, 3: 44.0, 4: 56.0},
}


def shrink_goal_features(
    raw: dict[str, float | int], prior: dict[str, float | int], prior_n: float
) -> dict[str, float | int]:
    if int(raw.get("games", 0) or 0) <= 0:
        out = dict(prior)
        out["games"] = 0
        out["raw_long_rate"] = 0.0
        out["split_games"] = 0
        return out

    out = dict(raw)
    sample_keys = {
        "recent_rate": "recent_games",
        "long_rate": "games",
        "target_units": "units_games",
        "tail_rate": "tail_games",
        "near_rate": "near_games",
    }
    for key, n_key in sample_keys.items():
        n = float(raw.get(n_key, 0) or 0)
        rv = float(raw.get(key, 0.0) or 0.0)
        pv = float(prior.get(key, 0.0) or 0.0)
        out[key] = (rv * n + pv * prior_n) / (n + prior_n) if n > 0 else pv
    out["raw_long_rate"] = float(raw.get("raw_long_rate", 0.0) or 0.0)
    out["split_games"] = int(raw.get("games", 0) or 0)
    return out


def goal_rate_score(metric: str, target: int, value: float) -> float:
    bounds = GOAL_RATE_BOUNDS[metric]
    lo, hi = bounds.get(target, bounds[max(bounds)])
    return scale(value, lo, hi, default=0.0)


def goal_unit_score(metric: str, target: int, value: float) -> float:
    bounds = GOAL_UNIT_BOUNDS[metric]
    lo, hi = bounds.get(target, bounds[max(bounds)])
    return scale(value, lo, hi, default=0.0)


def goal_component_weights(metric: str, target: int) -> dict[str, float]:
    # All rows sum to 1.0. Higher thresholds explicitly shift weight away from generic
    # 1+ contact/power and toward actual target attainment, excess production and tail.
    if metric == "hits":
        if target == 1:
            return {"Recent target rate": .27, "All-history target rate": .20, "Target-volume": .06, "Upper tail": .04, "Market skill": .20, "Starter matchup": .13, "Lineup opportunity": .10}
        if target == 2:
            return {"Recent target rate": .32, "All-history target rate": .16, "Target-volume": .22, "Upper tail": .12, "Market skill": .05, "Starter matchup": .04, "Lineup opportunity": .09}
        return {"Recent target rate": .30, "All-history target rate": .13, "Target-volume": .24, "Upper tail": .14, "Near-target rate": .07, "Market skill": .02, "Starter matchup": .02, "Lineup opportunity": .08}
    if metric == "total_bases":
        if target == 1:
            return {"Recent target rate": .25, "All-history target rate": .18, "Target-volume": .07, "Upper tail": .05, "Market skill": .22, "Starter matchup": .13, "Lineup opportunity": .10}
        if target == 2:
            return {"Recent target rate": .30, "All-history target rate": .16, "Target-volume": .22, "Upper tail": .13, "Market skill": .08, "Starter matchup": .05, "Lineup opportunity": .06}
        return {"Recent target rate": .28, "All-history target rate": .13, "Target-volume": .25, "Upper tail": .14, "Near-target rate": .07, "Market skill": .05, "Starter matchup": .03, "Lineup opportunity": .05}
    if metric == "home_runs":
        if target == 1:
            return {"Recent target rate": .23, "All-history target rate": .17, "Target-volume": .10, "Upper tail": .03, "Market skill": .30, "Starter matchup": .13, "Lineup opportunity": .04}
        return {"Recent target rate": .20, "All-history target rate": .12, "Target-volume": .18, "Upper tail": .08, "Near-target rate": .12, "Market skill": .22, "Starter matchup": .06, "Lineup opportunity": .02}
    # Extra-base hits: actual XBH target shape dominates; generic singles barely matter.
    if target == 1:
        return {"Recent target rate": .27, "All-history target rate": .17, "Target-volume": .12, "Upper tail": .05, "Market skill": .24, "Starter matchup": .09, "Lineup opportunity": .06}
    return {"Recent target rate": .29, "All-history target rate": .14, "Target-volume": .23, "Upper tail": .13, "Near-target rate": .07, "Market skill": .07, "Starter matchup": .03, "Lineup opportunity": .04}


def goal_market_context(
    metric: str, target: int, contact_score: float, hr_score: float, pitcher_contact: float, pitcher_hr: float
) -> tuple[float, float]:
    if metric == "hits":
        return contact_score, pitcher_contact
    if metric == "home_runs":
        return hr_score, pitcher_hr
    if metric == "total_bases":
        if target == 1:
            power_share = 0.30
        elif target == 2:
            power_share = 0.65
        else:
            power_share = 0.82
        return ((1-power_share)*contact_score + power_share*hr_score, (1-power_share)*pitcher_contact + power_share*pitcher_hr)
    # Extra-base hits should be power-led even at 1+.
    power_share = 0.78 if target == 1 else 0.90
    return ((1-power_share)*contact_score + power_share*hr_score, (1-power_share)*pitcher_contact + power_share*pitcher_hr)


def goal_score_table(
    games: list[dict[str, Any]], decay: float, contact_score: float, hr_score: float,
    pitcher_contact: float, pitcher_hr: float, lineup: float
) -> dict[str, Any]:
    fields = {"hits": "h", "total_bases": "tb", "home_runs": "hr", "extra_base_hits": "xbh"}
    out: dict[str, Any] = {}
    for metric, field in fields.items():
        out[metric] = {}
        for target in range(1, 5):
            all_features = weighted_goal_features(games, field, target, decay, None)
            by_hand: dict[str, Any] = {}
            for hand in ("ALL", "L", "R"):
                if hand == "ALL":
                    f = dict(all_features)
                    f["split_games"] = int(f.get("games", 0) or 0)
                else:
                    prior_n = GOAL_SPLIT_PRIOR_N[metric].get(target, 24.0)
                    f = shrink_goal_features(
                        weighted_goal_features(games, field, target, decay, hand),
                        all_features,
                        prior_n,
                    )
                market_skill, matchup = goal_market_context(metric, target, contact_score, hr_score, pitcher_contact, pitcher_hr)
                vals = {
                    "Recent target rate": goal_rate_score(metric, target, float(f["recent_rate"])),
                    "All-history target rate": goal_rate_score(metric, target, float(f["long_rate"])),
                    "Target-volume": goal_unit_score(metric, target, float(f["target_units"])),
                    "Upper tail": goal_rate_score(metric, target + 1, float(f["tail_rate"])),
                    "Near-target rate": goal_rate_score(metric, max(1, target - 1), float(f["near_rate"])),
                    "Market skill": market_skill,
                    "Starter matchup": matchup,
                    "Lineup opportunity": lineup,
                }
                score, components = make_components(vals, goal_component_weights(metric, target))
                by_hand[hand] = {
                    "quality_score": score,
                    "rate": round(float(f["long_rate"]), 4),
                    "recent_rate": round(float(f["recent_rate"]), 4),
                    "target_units": round(float(f["target_units"]), 4),
                    "tail_rate": round(float(f["tail_rate"]), 4),
                    "near_rate": round(float(f["near_rate"]), 4),
                    "raw_rate": round(float(f.get("raw_long_rate", f["long_rate"])), 4),
                    "recent_games": int(f.get("recent_games", 0) or 0),
                    "games": int(f.get("games", 0) or 0),
                    "split_games": int(f.get("split_games", f.get("games", 0)) or 0),
                    "known_hand_games": int(f.get("known_hand_games", 0) or 0),
                    "components": components,
                }
            out[metric][str(target)] = by_hand
    return out


OUTCOME_KEYS = ("out", "bb_hbp", "1b", "2b", "3b", "hr")
OUTCOME_BASES = {"out": 0, "bb_hbp": 0, "1b": 1, "2b": 2, "3b": 3, "hr": 4}
HIT_OUTCOMES = {"1b", "2b", "3b", "hr"}
XBH_OUTCOMES = {"2b", "3b", "hr"}
# Generic MLB-ish fallback used only if the local table is too small to estimate a prior.
# Deep mode does not use these as fitted values; they are emergency defaults only when
# local history is too short to support rolling out-of-sample tuning.
FALLBACK_OUTCOME_PRIOR = {"out": 0.676, "bb_hbp": 0.088, "1b": 0.151, "2b": 0.047, "3b": 0.004, "hr": 0.034}
DEEP_FALLBACK_PARAMS = {
    "player_prior_pa": 120.0,
    # v1.7.1 freezes the v1.7 PA-reliability experiment OFF after it contributed
    # effectively zero development LL and re-selected k=0 in all three historical
    # recheck windows. The key remains for backward-compatible scoring of old fits.
    "batter_reliability_prior_pa": 0.0,
    "hand_prior_pa": 90.0,
    "recent_prior_pa": 48.0,
    "recent_pa_decay": 0.955,
    "recent_half_life_pa": math.log(0.5) / math.log(0.955),
    "recent_half_life_games": (math.log(0.5) / math.log(0.955)) / 4.3,
    "pitcher_prior_pa": 220.0,
    # No modelling contribution below is sacred. Every new v1.5 feature has a
    # zero-effect candidate in the same nested-blind search used by the old terms.
    "player_effect": 1.0,
    "hand_effect": 1.0,
    "recent_effect": 1.0,
    "pitcher_effect": 1.0,
    "contact_quality_prior_pa": 80.0,
    "contact_quality_effect": 0.0,
    "bullpen_prior_pa": 500.0,
    "bullpen_effect": 0.0,
    "park_prior_pa": 1200.0,
    "park_effect": 0.0,
    "starter_share": 0.55,
    "starter_share_scale": 1.0,
    "lineup_slot_strength": 1.0,
    "rho_scale": 1.0,
    "calibration_models": {},
    "calibration_meta": {},
}
LINEUP_PA_MEAN = {1: 4.72, 2: 4.63, 3: 4.54, 4: 4.46, 5: 4.37, 6: 4.28, 7: 4.18, 8: 4.08, 9: 3.98}


def pa_outcome(event: str | None) -> str:
    e = event or ""
    if e == "single":
        return "1b"
    if e == "double":
        return "2b"
    if e == "triple":
        return "3b"
    if e == "home_run":
        return "hr"
    if e in {"walk", "intent_walk", "hit_by_pitch", "catcher_interf", "catcher_interference"}:
        return "bb_hbp"
    return "out"


def normalise_probs(values: dict[str, float]) -> dict[str, float]:
    total = sum(max(0.0, float(values.get(k, 0.0))) for k in OUTCOME_KEYS)
    if total <= 0:
        return dict(FALLBACK_OUTCOME_PRIOR)
    return {k: max(0.0, float(values.get(k, 0.0))) / total for k in OUTCOME_KEYS}


def outcome_counts(pa_rows: list[dict[str, Any]], hand: str | None = None) -> tuple[dict[str, float], float]:
    counts = {k: 0.0 for k in OUTCOME_KEYS}
    n = 0.0
    for r in pa_rows:
        if hand and r.get("p_throws") != hand:
            continue
        counts[pa_outcome(r.get("events"))] += 1.0
        n += 1.0
    return counts, n


def weighted_recent_outcome_counts(pa_rows: list[dict[str, Any]], game_decay: float) -> tuple[dict[str, float], float, float]:
    # Preserve the UI's familiar game-level decay semantics while operating per PA.
    # 4.3 PA approximates a full starting hitter game, so decay=0.82 means roughly
    # the same half-life as the old game EWMA without forcing fixed recent/all windows.
    pa_decay = max(0.70, min(0.999, float(game_decay) ** (1.0 / 4.3)))
    counts = {k: 0.0 for k in OUTCOME_KEYS}
    eff_n = 0.0
    for i, r in enumerate(reversed(pa_rows)):
        w = pa_decay ** i
        if w < 0.0005:
            break
        counts[pa_outcome(r.get("events"))] += w
        eff_n += w
    return counts, eff_n, pa_decay


def posterior_probs(counts: dict[str, float], prior: dict[str, float], prior_strength: float) -> dict[str, float]:
    n = sum(float(counts.get(k, 0.0)) for k in OUTCOME_KEYS)
    denom = n + max(0.0, prior_strength)
    if denom <= 0:
        return normalise_probs(prior)
    return normalise_probs({
        k: (float(counts.get(k, 0.0)) + float(prior.get(k, 0.0)) * prior_strength) / denom
        for k in OUTCOME_KEYS
    })


def league_outcome_priors(conn: sqlite3.Connection, before_day: str) -> dict[str, dict[str, float]]:
    buckets: dict[str, dict[str, float]] = {h: {k: 0.0 for k in OUTCOME_KEYS} for h in ("ALL", "L", "R")}
    rows = conn.execute(
        """
        SELECT p_throws, events, COUNT(*) AS n
        FROM pitches
        WHERE game_date < ? AND events IS NOT NULL AND events <> ''
        GROUP BY p_throws, events
        """,
        [before_day],
    ).fetchall()
    for row in rows:
        o = pa_outcome(row["events"])
        n = float(row["n"] or 0)
        buckets["ALL"][o] += n
        hand = row["p_throws"]
        if hand in {"L", "R"}:
            buckets[str(hand)][o] += n

    out: dict[str, dict[str, float]] = {}
    for hand in ("ALL", "L", "R"):
        # A tiny fallback pseudo-sample prevents impossible zero cells if the user has only
        # backfilled a very short range. Once local history grows, it becomes negligible.
        out[hand] = posterior_probs(buckets[hand], FALLBACK_OUTCOME_PRIOR, 60.0)
    return out


def relative_adjust(base: dict[str, float], reference: dict[str, float], comparison: dict[str, float], exponent: float) -> dict[str, float]:
    """Apply a multiplicative distributional adjustment in log-ratio space.

    All inputs are posterior distributions with non-zero prior mass, so arbitrary
    hand-picked ratio clamps are unnecessary. The only floor is numerical, not a
    baseball judgement.
    """
    raw: dict[str, float] = {}
    for k in OUTCOME_KEYS:
        ref = max(float(reference.get(k, 0.0)), 1e-12)
        comp = max(float(comparison.get(k, ref)), 1e-12)
        raw[k] = float(base.get(k, 0.0)) * ((comp / ref) ** exponent)
    return normalise_probs(raw)


def discrete_normal_pa_dist(mean: float, sd: float = 0.78) -> dict[int, float]:
    vals = {n: math.exp(-0.5 * ((n - mean) / sd) ** 2) for n in range(1, 10)}
    z = sum(vals.values()) or 1.0
    return {n: v / z for n, v in vals.items()}


def pa_distribution(games: list[dict[str, Any]], lineup_order: int | None, tuning: dict[str, Any] | None = None) -> dict[int, float]:
    # v1.3: lineup opportunity itself is tunable. Historical slot and all-slot
    # distributions are measured from pregame-eligible history, then combined in
    # log-ratio space. strength=0 means "ignore batting slot"; 1 means use the
    # empirical slot distribution; >1 may strengthen a repeatedly validated effect.
    if tuning and tuning.get("pa_distribution_by_slot"):
        dists = tuning.get("pa_distribution_by_slot") or {}
        all_raw = dists.get("ALL") or {}
        slot_key = str(int(lineup_order)) if lineup_order else "ALL"
        slot_raw = dists.get(slot_key) or all_raw
        all_dist = {int(k): max(1e-12, float(v)) for k, v in all_raw.items() if float(v) > 0}
        slot_dist = {int(k): max(1e-12, float(v)) for k, v in slot_raw.items() if float(v) > 0}
        keys = sorted(set(all_dist) | set(slot_dist))
        if keys:
            za = sum(all_dist.get(k, 0.0) for k in keys) or 1.0
            zs = sum(slot_dist.get(k, 0.0) for k in keys) or 1.0
            a = {k: max(1e-12, all_dist.get(k, 0.0) / za) for k in keys}
            b = {k: max(1e-12, slot_dist.get(k, 0.0) / zs) for k in keys}
            strength = max(0.0, min(1.75, float(tuning.get("lineup_slot_strength", 1.0))))
            raw = {k: a[k] * ((b[k] / a[k]) ** strength) for k in keys}
            z = sum(raw.values()) or 1.0
            return {k: raw[k] / z for k in keys}

    mean = LINEUP_PA_MEAN.get(int(lineup_order or 0), 4.25)
    prior = discrete_normal_pa_dist(mean)
    full = [g for g in games if int(g.get("pa", 0) or 0) >= 3]
    if not full:
        return prior
    hist_counts = {n: 0.0 for n in range(1, 10)}
    for g in full[-120:]:
        n = max(1, min(9, int(g.get("pa", 0) or 0)))
        hist_counts[n] += 1.0
    htotal = sum(hist_counts.values()) or 1.0
    hist = {n: hist_counts[n] / htotal for n in hist_counts}
    hist_share = min(0.42, 0.42 * len(full) / 60.0)
    return {n: (1.0 - hist_share) * prior[n] + hist_share * hist[n] for n in prior}


def binomial_tail(n: int, p: float, target: int) -> float:
    if target <= 0:
        return 1.0
    if target > n:
        return 0.0
    return sum(math.comb(n, k) * (p ** k) * ((1.0 - p) ** (n - k)) for k in range(target, n + 1))


def estimate_hit_overdispersion(games: list[dict[str, Any]], p_hit: float, rho_scale: float = 1.0) -> tuple[float, float, int]:
    """Estimate within-game hit clustering beyond iid PA draws.

    v1.3 makes the *influence* of this estimate tunable. The raw beta-binomial ICC is
    still shrunk for finite player-game samples; rho_scale=0 makes the game model iid,
    while values above 1 strengthen a clustering effect only if blind validation earns it.
    """
    full = [g for g in games if int(g.get("pa", 0) or 0) >= 3]
    if len(full) < 18 or p_hit <= 0.01 or p_hit >= 0.70:
        return 0.0, 0.0, len(full)
    v = p_hit * (1.0 - p_hit)
    num = 0.0
    den = 0.0
    for g in full:
        n = int(g.get("pa", 0) or 0)
        h = int(g.get("h", 0) or 0)
        num += (h - n * p_hit) ** 2 - n * v
        den += n * max(0, n - 1) * v
    raw = (num / den) if den > 0 else 0.0
    raw = max(0.0, min(0.35, raw))
    shrink = len(full) / (len(full) + 60.0)
    rho = raw * shrink * max(0.0, min(2.5, float(rho_scale)))
    return max(0.0, min(0.35, rho)), raw, len(full)


def beta_binomial_pmf(n: int, p: float, k: int, rho: float) -> float:
    if k < 0 or k > n:
        return 0.0
    if rho <= 1e-6:
        return math.comb(n, k) * (p ** k) * ((1.0 - p) ** (n - k))
    p = max(1e-9, min(1.0 - 1e-9, p))
    rho = max(1e-9, min(0.20, rho))
    concentration = 1.0 / rho - 1.0
    a = p * concentration
    b = (1.0 - p) * concentration
    log_choose = math.lgamma(n + 1) - math.lgamma(k + 1) - math.lgamma(n - k + 1)
    log_beta = math.lgamma(k + a) + math.lgamma(n - k + b) - math.lgamma(n + a + b)
    log_beta_ab = math.lgamma(a) + math.lgamma(b) - math.lgamma(a + b)
    return math.exp(log_choose + log_beta - log_beta_ab)


def beta_binomial_tail(n: int, p: float, target: int, rho: float) -> float:
    if target <= 0:
        return 1.0
    if target > n:
        return 0.0
    return max(0.0, min(1.0, sum(beta_binomial_pmf(n, p, k, rho) for k in range(target, n + 1))))


def conditional_tb_tail(k_hits: int, hit_mix: dict[int, float], target: int) -> float:
    if target <= 0:
        return 1.0
    if k_hits <= 0:
        return 0.0
    dist = {0: 1.0}
    for _ in range(k_hits):
        nxt: dict[int, float] = defaultdict(float)
        for cur, cp in dist.items():
            for bases, op in hit_mix.items():
                nxt[cur + bases] += cp * op
        dist = dict(nxt)
    return sum(v for bases, v in dist.items() if bases >= target)


def market_probabilities(probs: dict[str, float], pa_dist: dict[int, float], hit_rho: float = 0.0) -> dict[str, dict[str, float]]:
    """Game-market tails from one coherent per-PA distribution.

    The player-level hit overdispersion is reused as a shared within-game production
    state: first draw how many hits occur via beta-binomial, then draw hit type
    conditional on a hit. This gives HR/XBH/TB the same empirically estimated bursty
    game state without trying to fit impossibly sparse market-specific rho values.
    """
    p_hit = sum(probs[k] for k in HIT_OUTCOMES)
    if p_hit <= 1e-12:
        hit_mix = {1: 1.0, 2: 0.0, 3: 0.0, 4: 0.0}
    else:
        hit_mix = {1: probs["1b"] / p_hit, 2: probs["2b"] / p_hit, 3: probs["3b"] / p_hit, 4: probs["hr"] / p_hit}
    q_hr = hit_mix[4]
    q_xbh = hit_mix[2] + hit_mix[3] + hit_mix[4]
    out: dict[str, dict[str, float]] = {m: {} for m in ("hits", "total_bases", "home_runs", "extra_base_hits")}
    for target in range(1, 5):
        h = hr = xbh = tb = 0.0
        for n, wp in pa_dist.items():
            h_n = hr_n = xbh_n = tb_n = 0.0
            for k in range(0, n + 1):
                pk = beta_binomial_pmf(n, p_hit, k, hit_rho)
                if k >= target:
                    h_n += pk
                hr_n += pk * binomial_tail(k, q_hr, target)
                xbh_n += pk * binomial_tail(k, q_xbh, target)
                tb_n += pk * conditional_tb_tail(k, hit_mix, target)
            h += wp * h_n
            hr += wp * hr_n
            xbh += wp * xbh_n
            tb += wp * tb_n
        out["hits"][str(target)] = max(0.0, min(1.0, h))
        out["home_runs"][str(target)] = max(0.0, min(1.0, hr))
        out["extra_base_hits"][str(target)] = max(0.0, min(1.0, xbh))
        out["total_bases"][str(target)] = max(0.0, min(1.0, tb))
    return out


def observed_game_shape(games: list[dict[str, Any]], field: str) -> dict[str, float]:
    full = [g for g in games if int(g.get("pa", 0) or 0) >= 3]
    if not full:
        return {"0": 0.0, "1": 0.0, "2": 0.0, "3+": 0.0}
    n = len(full)
    return {
        "0": sum(1 for g in full if int(g.get(field, 0) or 0) == 0) / n,
        "1": sum(1 for g in full if int(g.get(field, 0) or 0) == 1) / n,
        "2": sum(1 for g in full if int(g.get(field, 0) or 0) == 2) / n,
        "3+": sum(1 for g in full if int(g.get(field, 0) or 0) >= 3) / n,
    }


def pearson_lag1(values: list[float]) -> float | None:
    if len(values) < 25:
        return None
    a, b = values[:-1], values[1:]
    ma, mb = sum(a) / len(a), sum(b) / len(b)
    va = sum((x - ma) ** 2 for x in a)
    vb = sum((x - mb) ** 2 for x in b)
    if va <= 1e-12 or vb <= 1e-12:
        return 0.0
    return sum((x - ma) * (y - mb) for x, y in zip(a, b)) / math.sqrt(va * vb)


def streak_diagnostics(games: list[dict[str, Any]], p_hit: float, hit_rho: float = 0.0) -> dict[str, Any]:
    full = [g for g in games if int(g.get("pa", 0) or 0) >= 3]
    residuals: list[float] = []
    cleared: list[bool] = []
    for g in full:
        pa = int(g.get("pa", 0) or 0)
        exp2 = beta_binomial_tail(pa, p_hit, 2, hit_rho)
        actual = 1.0 if int(g.get("h", 0) or 0) >= 2 else 0.0
        residuals.append(actual - exp2)
        cleared.append(bool(actual))
    corr = pearson_lag1(residuals)
    after_clear = []
    after_miss = []
    for i in range(1, len(cleared)):
        (after_clear if cleared[i - 1] else after_miss).append(1.0 if cleared[i] else 0.0)
    ac = (sum(after_clear) / len(after_clear)) if after_clear else None
    am = (sum(after_miss) / len(after_miss)) if after_miss else None
    if corr is None:
        label = "INSUFFICIENT"
    elif abs(corr) < 0.08:
        label = "NEGLIGIBLE"
    elif abs(corr) < 0.16:
        label = "WEAK"
    else:
        label = "POSSIBLE"
    return {
        "lag1_residual_corr": None if corr is None else round(corr, 4),
        "classification": label,
        "games": len(full),
        "next_2plus_after_2plus": None if ac is None else round(ac, 4),
        "next_2plus_after_miss": None if am is None else round(am, 4),
        "used_as_adjustment": False,
    }




def _zero_arr() -> list[float]:
    return [0.0] * len(OUTCOME_KEYS)


def _arr_norm(a: list[float]) -> list[float]:
    z = sum(max(0.0, x) for x in a)
    if z <= 0:
        return [FALLBACK_OUTCOME_PRIOR[k] for k in OUTCOME_KEYS]
    return [max(0.0, x) / z for x in a]


def _arr_posterior(counts: list[float], prior: list[float], strength: float) -> list[float]:
    n = sum(counts)
    d = n + max(0.0, strength)
    if d <= 0:
        return _arr_norm(prior)
    return _arr_norm([(counts[i] + prior[i] * strength) / d for i in range(len(OUTCOME_KEYS))])


def _arr_relative(base: list[float], reference: list[float], comparison: list[float], exponent: float = 1.0) -> list[float]:
    raw = []
    for i in range(len(OUTCOME_KEYS)):
        ref = max(reference[i], 1e-12)
        comp = max(comparison[i], 1e-12)
        raw.append(base[i] * ((comp / ref) ** exponent))
    return _arr_norm(raw)


def _batter_reliability(prior_pa: float, reliability_prior_pa: float) -> float:
    """Continuous total-history reliability gate for batter-derived signals.

    reliability_prior_pa=0 is an exact OFF/identity candidate. Positive values
    attenuate player-specific log-ratio adjustments most strongly for tiny samples
    and asymptotically approach 1 as prior PA accumulates. The scale is selected by
    the same chronological dual-panel tuner as the other model terms; audit residual
    magnitudes are never hard-coded into the function.
    """
    n=max(0.0,float(prior_pa)); k=max(0.0,float(reliability_prior_pa))
    if k<=1e-12:
        return 1.0
    return n/(n+k)


def _quality_zero() -> dict[str, float]:
    return {"pa": 0.0, "xhits": 0.0, "bbe": 0.0}


def _quality_update(q: dict[str, float], row: dict[str, Any], weight: float = 1.0) -> None:
    # StatsAPI D-1 rows do not carry Savant xBA. Hold the xBA-quality state
    # unchanged until canonical Savant data arrives rather than treating missing
    # expected BA as a zero-quality plate appearance.
    if row.get("source") == "statsapi_provisional" and row.get("estimated_ba") is None:
        return
    w=max(0.0,float(weight)); q["pa"]=float(q.get("pa",0.0))+w
    xba=row.get("estimated_ba")
    if xba is not None:
        try:
            xv=max(0.0,min(1.0,float(xba)))
        except Exception:
            xv=0.0
        q["xhits"]=float(q.get("xhits",0.0))+w*xv
        q["bbe"]=float(q.get("bbe",0.0))+w


def _quality_decay(q: dict[str, float], decay: float) -> None:
    d=max(0.0,min(1.0,float(decay)))
    for k in ("pa","xhits","bbe"):
        q[k]=float(q.get(k,0.0))*d


def _quality_hit_posterior(q: dict[str, float], anchor_hit: float, prior_pa: float) -> tuple[float, float]:
    pa=max(0.0,float(q.get("pa",0.0))); prior=max(0.0,float(prior_pa))
    anchor=max(1e-6,min(1.0-1e-6,float(anchor_hit)))
    if pa+prior<=0:
        return anchor, pa
    xhits=max(0.0,min(pa,float(q.get("xhits",0.0))))
    return max(1e-6,min(1.0-1e-6,(xhits+anchor*prior)/(pa+prior))),pa


def _arr_adjust_group_target(base: list[float], keys: set[str], target_prob: float, effect: float) -> list[float]:
    """Move one outcome group's odds toward a target probability, preserving within-group shape.

    effect=0 is exact identity; effect=1 reaches the target group probability. This is
    useful for xBA-style expected-hit corrections without inventing a hit-type mapping.
    """
    e=max(0.0,float(effect))
    if e<=1e-12:
        return _arr_norm(base)
    idx=[i for i,k in enumerate(OUTCOME_KEYS) if k in keys]
    p=sum(base[i] for i in idx)
    if p<=1e-9 or p>=1.0-1e-9:
        return _arr_norm(base)
    q=max(1e-6,min(1.0-1e-6,float(target_prob)))
    odds_ratio=(q/(1.0-q))/(p/(1.0-p))
    factor=max(1e-6,min(1e6,odds_ratio**e))
    raw=[float(x) for x in base]
    for i in idx:
        raw[i]*=factor
    return _arr_norm(raw)


def _fielding_team(row: dict[str, Any]) -> str:
    top=str(row.get("inning_topbot") or "").lower()=="top"
    return str((row.get("home_team") if top else row.get("away_team")) or "")


def _batting_side_key(row: dict[str, Any]) -> tuple[int, str]:
    side="away" if str(row.get("inning_topbot") or "").lower()=="top" else "home"
    return int(row.get("game_pk") or 0),side


# Historical role inference for completed games.  The first pitcher is usually the
# starter, but opener games invert that assumption: a short first stint is followed
# by a bulk arm who functions as the starter for most batter PAs.  We infer that
# role from terminal-PA counts only after the completed prior game, so it cannot leak
# the game being predicted.  In an opener pattern the opener is treated as bullpen
# history and the bulk follower is the functional starter.
OPENER_MAX_BF = 9
BULK_MIN_BF = 12


def _functional_starter_info(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ordered=sorted(rows,key=lambda x:int(x.get("at_bat_number") or 0))
    order: list[int]=[]
    counts: dict[int,int]=defaultdict(int)
    hand: dict[int,str]={}
    for r in ordered:
        p=safe_int(r.get("pitcher"))
        if p is None:
            continue
        if p not in counts:
            order.append(p)
            h=str(r.get("p_throws") or "?").upper()
            hand[p]=h if h in {"L","R"} else "?"
        counts[p]+=1
    if not order:
        return {"pitcher":None,"hand":"?","first_pitcher":None,"first_bf":0,"starter_bf":0,"opener_proxy":False}
    first=order[0]
    starter=first
    opener_proxy=False
    if counts[first] <= OPENER_MAX_BF:
        for p in order[1:]:
            if counts[p] >= BULK_MIN_BF:
                starter=p
                opener_proxy=True
                break
    return {
        "pitcher":starter,
        "hand":hand.get(starter,"?"),
        "first_pitcher":first,
        "first_bf":int(counts[first]),
        "starter_bf":int(counts[starter]),
        "opener_proxy":opener_proxy,
    }


def _functional_starter_by_side(rows: list[dict[str, Any]]) -> dict[tuple[int,str], dict[str, Any]]:
    groups: dict[tuple[int,str],list[dict[str,Any]]]=defaultdict(list)
    for r in rows:
        groups[_batting_side_key(r)].append(r)
    return {k:_functional_starter_info(rs) for k,rs in groups.items()}


def _environment_zero() -> dict[str, Any]:
    return {
        "park_home": defaultdict(_zero_arr),
        "park_road": defaultdict(_zero_arr),
        "bullpen": defaultdict(_zero_arr),
    }


def _environment_update(env: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    ordered=sorted(rows,key=lambda x:(int(x.get("game_pk") or 0),int(x.get("at_bat_number") or 0)))
    starter_info=_functional_starter_by_side(ordered)
    for r in ordered:
        idx=_outcome_idx(r.get("events")); home=str(r.get("home_team") or ""); away=str(r.get("away_team") or "")
        if home:
            _bt_add(env["park_home"][home],idx)
        if away:
            _bt_add(env["park_road"][away],idx)
        p=safe_int(r.get("pitcher")); info=starter_info.get(_batting_side_key(r)) or {}; starter=info.get("pitcher"); fld=_fielding_team(r)
        if fld and p is not None and starter is not None and p!=starter:
            _bt_add(env["bullpen"][fld],idx)


def _environment_from_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    env=_environment_zero()
    by_day: dict[str,list[dict[str,Any]]]=defaultdict(list)
    for r in rows:
        by_day[str(r.get("game_date") or "")].append(r)
    for day in sorted(by_day):
        _environment_update(env,by_day[day])
    return env


def _outcome_idx(event: str | None) -> int:
    return OUTCOME_KEYS.index(pa_outcome(event))


def tuning_rows(conn: sqlite3.Connection, before_day: str, canonical_only: bool = False) -> list[dict[str, Any]]:
    source_clause = " AND source <> 'statsapi_provisional'" if canonical_only else ""
    rows = conn.execute(
        f"""
        SELECT game_date, game_pk, at_bat_number, batter, pitcher, p_throws, events,
               home_team, away_team, inning_topbot, source,
               estimated_ba, estimated_woba, launch_speed, launch_angle, barrel, bb_type
        FROM pitches
        WHERE game_date < ? AND events IS NOT NULL AND events <> ''
              AND batter IS NOT NULL AND pitcher IS NOT NULL{source_clause}
        ORDER BY game_date, game_pk, at_bat_number
        """,
        [before_day],
    ).fetchall()
    return [dict(r) for r in rows]


def tuning_coverage_signature(conn: sqlite3.Connection, before_day: str) -> tuple[str, dict[str, Any]]:
    # Fit/reuse hyperparameters from canonical history only. The D-1 overlay may
    # update live sufficient statistics, but it must not retune a model from rows
    # missing Savant-only quality fields.
    r = conn.execute(
        """
        SELECT MIN(game_date) AS first_date, MAX(game_date) AS last_date,
               COUNT(*) AS pa_count, COUNT(DISTINCT game_date) AS day_count
        FROM pitches
        WHERE game_date < ? AND events IS NOT NULL AND events <> ''
              AND source <> 'statsapi_provisional'
        """,
        [before_day],
    ).fetchone()
    meta = {
        "first_date": r["first_date"], "last_date": r["last_date"],
        "pa_count": int(r["pa_count"] or 0), "day_count": int(r["day_count"] or 0),
    }
    raw = f'{meta["first_date"]}|{meta["last_date"]}|{meta["pa_count"]}|{meta["day_count"]}'
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20], meta


def derive_game_environment(rows: list[dict[str, Any]]) -> tuple[float, dict[str, dict[str, float]], int]:
    groups: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        side = "away" if str(r.get("inning_topbot") or "").lower() == "top" else "home"
        groups[(int(r["game_pk"]), side)].append(r)
    starter_pa = total_pa = 0
    slot_counts = {s: defaultdict(float) for s in range(1, 10)}
    team_games = 0
    for rs in groups.values():
        if not rs:
            continue
        rs.sort(key=lambda x: int(x.get("at_bat_number") or 0))
        starter = _functional_starter_info(rs).get("pitcher")
        t = len(rs)
        if t < 9:
            continue
        team_games += 1
        total_pa += t
        if starter is not None:
            starter_pa += sum(1 for r in rs if safe_int(r.get("pitcher")) == starter)
        # Batting-order slots consume team PAs cyclically. This gives opportunity by slot
        # without needing historical lineup metadata or guessing from player names.
        for slot in range(1, 10):
            n = 0 if t < slot else 1 + (t - slot) // 9
            slot_counts[slot][max(1, min(9, n))] += 1.0
    starter_share = (starter_pa / total_pa) if total_pa else DEEP_FALLBACK_PARAMS["starter_share"]
    dists: dict[str, dict[str, float]] = {}
    all_slots: dict[int, float] = defaultdict(float)
    for slot in range(1, 10):
        z = sum(slot_counts[slot].values())
        if z:
            dists[str(slot)] = {str(n): c / z for n, c in sorted(slot_counts[slot].items())}
            for n, c in slot_counts[slot].items():
                all_slots[n] += c
    zall = sum(all_slots.values())
    if zall:
        dists["ALL"] = {str(n): c / zall for n, c in sorted(all_slots.items())}
    return starter_share, dists, team_games


def _league_arrays(rows: list[dict[str, Any]]) -> dict[str, list[float]]:
    out = {"ALL": _zero_arr(), "L": _zero_arr(), "R": _zero_arr()}
    for r in rows:
        idx = _outcome_idx(r.get("events"))
        out["ALL"][idx] += 1.0
        h = str(r.get("p_throws") or "")
        if h in {"L", "R"}:
            out[h][idx] += 1.0
    fb = [FALLBACK_OUTCOME_PRIOR[k] for k in OUTCOME_KEYS]
    for h in out:
        if sum(out[h]) < 100:
            out[h] = fb[:]
        else:
            out[h] = _arr_norm(out[h])
    return out


def _eval_tuning(rows: list[dict[str, Any]], split_date: str, params: dict[str, float], score_limit: int = 16000) -> dict[str, float]:
    """Legacy per-PA diagnostic retained for comparison; v1.4 selects on dual-panel game-level CV."""
    train = [r for r in rows if str(r["game_date"]) < split_date]
    valid = [r for r in rows if str(r["game_date"]) >= split_date]
    if not train or not valid:
        return {"log_loss": 99.0, "brier": 99.0, "scored": 0}
    league = _league_arrays(train)
    player: dict[int, list[float]] = defaultdict(_zero_arr)
    handc: dict[tuple[int, str], list[float]] = defaultdict(_zero_arr)
    pitcher: dict[int, list[float]] = defaultdict(_zero_arr)
    recent: dict[int, list[float]] = defaultdict(_zero_arr)
    d = float(params["recent_pa_decay"])
    for r in train:
        b, p = int(r["batter"]), int(r["pitcher"])
        h = str(r.get("p_throws") or "")
        idx = _outcome_idx(r.get("events"))
        player[b][idx] += 1.0; pitcher[p][idx] += 1.0
        if h in {"L", "R"}: handc[(b, h)][idx] += 1.0
        rr = recent[b]
        for i in range(len(rr)): rr[i] *= d
        rr[idx] += 1.0
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in valid: by_day[str(r["game_date"])].append(r)
    total_valid = len(valid); stride = max(1, total_valid // max(1, score_limit))
    ll = br = 0.0; scored = seen = 0
    for day in sorted(by_day):
        rs = by_day[day]
        for r in rs:
            b, p = int(r["batter"]), int(r["pitcher"]); h = str(r.get("p_throws") or "")
            prior_pa = int(sum(player[b]))
            batter_reliability = _batter_reliability(prior_pa, float(params.get("batter_reliability_prior_pa", 0.0)))
            stable_raw = _arr_posterior(player[b], league["ALL"], params["player_prior_pa"])
            stable = _arr_relative(league["ALL"], league["ALL"], stable_raw, float(params.get("player_effect", 1.0))*batter_reliability)
            if h in {"L", "R"}:
                hand_anchor = _arr_relative(stable, league["ALL"], league[h], 1.0)
                hand_raw = _arr_posterior(handc[(b, h)], hand_anchor, params["hand_prior_pa"])
                hp = _arr_relative(stable, stable, hand_raw, float(params.get("hand_effect", 1.0))*batter_reliability); lh = league[h]
            else: hp, lh = stable, league["ALL"]
            rp = _arr_posterior(recent[b], stable, params["recent_prior_pa"])
            pre = _arr_relative(hp, stable, rp, float(params.get("recent_effect", 1.0))*batter_reliability)
            pp = _arr_posterior(pitcher[p], lh, params["pitcher_prior_pa"])
            adj = _arr_relative(pre, lh, pp, float(params.get("pitcher_effect", 1.0)))
            share = max(0.0, min(1.0, float(params.get("starter_share", .55)) * float(params.get("starter_share_scale", 1.0))))
            final = _arr_norm([(1-share)*pre[i] + share*adj[i] for i in range(len(OUTCOME_KEYS))])
            if seen % stride == 0:
                y = _outcome_idx(r.get("events")); py=max(final[y],1e-12)
                ll -= math.log(py); br += sum((final[i]-(1.0 if i==y else 0.0))**2 for i in range(len(final))); scored += 1
            seen += 1
        for r in rs:
            b,p=int(r["batter"]),int(r["pitcher"]); h=str(r.get("p_throws") or ""); idx=_outcome_idx(r.get("events"))
            player[b][idx]+=1.0; pitcher[p][idx]+=1.0
            if h in {"L","R"}: handc[(b,h)][idx]+=1.0
            rr=recent[b]
            for i in range(len(rr)): rr[i]*=d
            rr[idx]+=1.0
    return {"log_loss": ll/max(1,scored), "brier": br/max(1,scored), "scored": scored}


def _quantile(values: list[float], q: float) -> float:
    xs=sorted(float(x) for x in values if math.isfinite(float(x)))
    if not xs: return 0.0
    if len(xs)==1: return xs[0]
    pos=max(0.0,min(1.0,q))*(len(xs)-1); lo=int(math.floor(pos)); hi=int(math.ceil(pos))
    if lo==hi: return xs[lo]
    f=pos-lo; return xs[lo]*(1-f)+xs[hi]*f


def _solve_linear(a: list[list[float]], b: list[float]) -> list[float] | None:
    n=len(b); m=[list(map(float,a[i]))+[float(b[i])] for i in range(n)]
    for col in range(n):
        piv=max(range(col,n), key=lambda r: abs(m[r][col]))
        if abs(m[piv][col]) < 1e-11: return None
        if piv!=col: m[col],m[piv]=m[piv],m[col]
        d=m[col][col]
        for j in range(col,n+1): m[col][j]/=d
        for r in range(n):
            if r==col: continue
            f=m[r][col]
            if abs(f)<1e-18: continue
            for j in range(col,n+1): m[r][j]-=f*m[col][j]
    return [m[i][n] for i in range(n)]


def _calibration_meta_from_predictions(preds: list[dict[str, Any]]) -> dict[str, float]:
    prior=[float(r.get("prior_pa") or 0) for r in preds]
    recent=[float(r.get("recent_hit_delta") or 0.0) for r in preds]
    return {
        "prior_q1": _quantile(prior, 1/3), "prior_q2": _quantile(prior, 2/3),
        "recent_q1": _quantile(recent, 1/3), "recent_q2": _quantile(recent, 2/3),
    }


def _cal_feature_vector(raw_p: float, r: dict[str, Any], meta: dict[str, float]) -> list[float]:
    hand=str(r.get("pitcher_hand") or "?").upper(); slot=int(r.get("lineup_order") or 9)
    prior=float(r.get("prior_pa") or 0); rd=float(r.get("recent_hit_delta") or 0.0)
    raw=_safe_logit(raw_p)
    return [
        1.0, raw,
        1.0 if hand=="L" else 0.0,
        raw if hand=="R" else 0.0,
        1.0 if slot<=3 else 0.0,
        1.0 if slot>=7 else 0.0,
        1.0 if prior < float(meta.get("prior_q1",100.0)) else 0.0,
        1.0 if prior > float(meta.get("prior_q2",300.0)) else 0.0,
        1.0 if rd < float(meta.get("recent_q1",-0.005)) else 0.0,
        1.0 if rd > float(meta.get("recent_q2",0.005)) else 0.0,
    ]


CAL_FEATURE_NAMES=("intercept","raw_logit","hand_L","rhp_raw_logit","lineup_1_3","lineup_7_9","prior_low","prior_high","recent_low","recent_high")
RHP_SLOPE_FEATURE_INDEX=CAL_FEATURE_NAMES.index("rhp_raw_logit")


def _fit_logit_main_effects(items: list[dict[str, Any]], ridge: float, meta: dict[str,float], allow_rhp_slope: bool=False) -> dict[str, Any]:
    """Fit global logit recalibration + additive slice offsets with L2 shrinkage.

    This is deliberately main-effects-only. Intercept and raw-logit slope are free;
    hand/lineup/prior/recent slice offsets shrink toward zero. Coordinate Newton
    updates reuse the current linear predictor, making repeated blind-CV fits cheap
    enough to run locally without numpy/scipy.
    """
    if not items:
        return {"kind":"identity"}
    ys=[int(r["y"]) for r in items]
    pos=sum(ys); neg=len(ys)-pos
    if pos < 5 or neg < 5:
        return {"kind":"identity","n":len(items),"positives":pos,"reason":"too few class events"}

    xrows=[_cal_feature_vector(float(r["p"]),r,meta) for r in items]
    full=pos>=20 and neg>=20
    if full:
        active=[j for j in range(len(CAL_FEATURE_NAMES)) if allow_rhp_slope or j!=RHP_SLOPE_FEATURE_INDEX]
    else:
        active=[0,1]
    beta=[0.0]*len(CAL_FEATURE_NAMES)
    beta[1]=1.0  # identity calibration is the neutral starting point
    z=[x[1] for x in xrows]
    cols={j:[(i,x[j]) for i,x in enumerate(xrows) if abs(x[j])>1e-15] for j in active}
    lam=max(0.0,float(ridge))

    for _sweep in range(14):
        max_step=0.0
        for j in active:
            col=cols[j]
            if j>=2 and len(col)<15:
                continue
            penalty=lam if j>=2 else 0.0
            center=0.0
            g=-penalty*(beta[j]-center)
            h=penalty+1e-8
            for i,xij in col:
                zz=max(-30.0,min(30.0,z[i]))
                q=1.0/(1.0+math.exp(-zz))
                g+=xij*(ys[i]-q)
                h+=xij*xij*max(1e-8,q*(1.0-q))
            step=g/h
            # Separation in a tiny slice should not throw a huge coefficient into the
            # next coordinate. Repeated sweeps can still move farther when data earn it.
            step=max(-1.5,min(1.5,step))
            if abs(step)<1e-10:
                continue
            beta[j]+=step
            for i,xij in col:
                z[i]+=step*xij
            max_step=max(max_step,abs(step))
        if max_step<1e-6:
            break

    coefs={name:float(beta[i]) for i,name in enumerate(CAL_FEATURE_NAMES)}
    if not full:
        for i in range(2,len(CAL_FEATURE_NAMES)):
            coefs[CAL_FEATURE_NAMES[i]]=0.0
    elif not allow_rhp_slope:
        coefs["rhp_raw_logit"]=0.0
    return {
        "kind":("main_effect_logit_rhp_slope" if full and allow_rhp_slope else ("main_effect_logit" if full else "global_logit")),
        "coefficients":coefs,"ridge":lam,"n":len(items),"positives":pos,"rhp_slope":bool(full and allow_rhp_slope),
    }

def _apply_cal_model(p: float, row: dict[str,Any], model: dict[str,Any] | None, meta: dict[str,float]) -> float:
    if not model or model.get("kind")=="identity": return max(1e-7,min(1-1e-7,float(p)))
    co=model.get("coefficients") or {}; x=_cal_feature_vector(float(p),row,meta)
    z=sum(float(co.get(name, 0.0 if name!="raw_logit" else 1.0))*x[i] for i,name in enumerate(CAL_FEATURE_NAMES))
    z=max(-30.0,min(30.0,z)); return 1/(1+math.exp(-z))


def _binary_ll(items: list[dict[str,Any]]) -> float:
    if not items: return 99.0
    return -sum(int(r["y"])*math.log(max(1e-9,min(1-1e-9,float(r["p"])))) + (1-int(r["y"]))*math.log(max(1e-9,1-min(1-1e-9,float(r["p"])))) for r in items)/len(items)


def _fit_calibration_models(preds: list[dict[str,Any]]) -> tuple[dict[str,Any], dict[str,float], dict[str,Any]]:
    final_meta=_calibration_meta_from_predictions(preds); models={}; report={}
    dates=sorted({str(r["game_date"]) for r in preds}); n=len(dates)
    # Earliest block is calibration training only; later blocks are scored strictly
    # out-of-fold. Slice cut-points are computed from the training block available at
    # that moment, not from future feature distributions.
    cut1=max(1,n//3); cut2=max(cut1+1,(2*n)//3)
    blocks=[dates[:cut1],dates[cut1:cut2],dates[cut2:]]
    ridges=[4.0,32.0,128.0]
    for market in BACKTEST_MARKETS:
        models[market]={}; report[market]={}
        actual_field=BACKTEST_ACTUAL_FIELD[market]
        for target in range(1,5):
            base=[]
            for r in preds:
                p=float((r["probabilities"].get(market) or {}).get(str(target),0.0))
                y=1 if int(r[actual_field])>=target else 0
                base.append({**r,"p":p,"y":y})
            identity_all=_binary_ll(base)
            pos_all=sum(int(r["y"]) for r in base); neg_all=len(base)-pos_all
            if pos_all < 20 or neg_all < 20 or len(blocks)<3:
                final={"kind":"identity","ridge":None,"n":len(base),"positives":pos_all,
                       "cv_log_loss":identity_all,"identity_log_loss":identity_all,
                       "identity_cv_log_loss":identity_all,"cv_n":len(base)}
                models[market][str(target)]=final
                report[market][str(target)]={"ridge":None,"cv_log_loss":identity_all,
                    "identity_log_loss":identity_all,"identity_cv_log_loss":identity_all,
                    "improvement":0.0,"kind":"identity","n":len(base),"positives":pos_all}
                continue

            eval_dates=set(d for block in blocks[1:] for d in block)
            identity_eval=[r for r in base if str(r["game_date"]) in eval_dates]
            identity_cv=_binary_ll(identity_eval) if identity_eval else identity_all
            best_ridge=None; best_rhp=False; best=identity_cv; scored=len(identity_eval)
            # v1.7+ retires the RHP-specific slope after the v1.6 replication audit
            # selected it in 0/3 windows. Keep only the simpler main-effects family.
            for allow_rhp in (False,):
                for ridge in ridges:
                    losses=[]; weights=[]; prior_dates=[]
                    for bi,block in enumerate(blocks):
                        if bi==0:
                            prior_dates.extend(block); continue
                        prior_set=set(prior_dates); block_set=set(block)
                        train=[r for r in base if str(r["game_date"]) in prior_set]
                        valid=[r for r in base if str(r["game_date"]) in block_set]
                        if not train or not valid:
                            prior_dates.extend(block); continue
                        fold_meta=_calibration_meta_from_predictions(train)
                        mdl=_fit_logit_main_effects(train,ridge,fold_meta,allow_rhp_slope=allow_rhp)
                        vv=[{**r,"p":_apply_cal_model(float(r["p"]),r,mdl,fold_meta)} for r in valid]
                        losses.append(_binary_ll(vv)); weights.append(len(vv)); prior_dates.extend(block)
                    cv=sum(l*w for l,w in zip(losses,weights))/sum(weights) if weights else 99.0
                    if cv < best-1e-8:
                        best,best_ridge,best_rhp,scored=cv,ridge,allow_rhp,sum(weights)

            if best_ridge is None:
                final={"kind":"identity","ridge":None,"n":len(base),"positives":pos_all,"rhp_slope":False}
            else:
                final=_fit_logit_main_effects(base,best_ridge,final_meta,allow_rhp_slope=best_rhp)
            final.update({"cv_log_loss":best,"identity_log_loss":identity_all,
                          "identity_cv_log_loss":identity_cv,"cv_n":scored})
            models[market][str(target)]=final
            report[market][str(target)]={"ridge":best_ridge,"rhp_slope":bool(best_rhp),"cv_log_loss":best,
                "identity_log_loss":identity_all,"identity_cv_log_loss":identity_cv,
                "improvement":identity_cv-best,"kind":final.get("kind"),
                "n":len(base),"positives":pos_all}
    return models,final_meta,report

def _project_nonincreasing(values: dict[str,float]) -> dict[str,float]:
    """PAVA projection enforcing P(1+) >= P(2+) >= P(3+) >= P(4+)."""
    keys=sorted(values,key=lambda x:int(x))
    blocks=[]
    for k in keys:
        blocks.append({"keys":[k],"sum":float(values[k]),"w":1.0})
        # For a decreasing sequence, previous mean may not be below next mean.
        while len(blocks)>=2:
            a,b=blocks[-2],blocks[-1]
            ma=a["sum"]/a["w"]; mb=b["sum"]/b["w"]
            if ma+1e-15 >= mb:
                break
            blocks[-2:] = [{"keys":a["keys"]+b["keys"],"sum":a["sum"]+b["sum"],"w":a["w"]+b["w"]}]
    out={}
    for b in blocks:
        m=max(1e-7,min(1-1e-7,b["sum"]/b["w"]))
        for k in b["keys"]:
            out[k]=m
    return out


def _apply_calibration_matrix(markets: dict[str,dict[str,float]], row: dict[str,Any], params: dict[str,Any]) -> dict[str,dict[str,float]]:
    models=params.get("calibration_models") or {}; meta=params.get("calibration_meta") or {}
    if not models:
        return {m:_project_nonincreasing({str(t):float(p) for t,p in targets.items()}) for m,targets in markets.items()}
    out={}
    for m,targets in markets.items():
        calibrated={str(t):_apply_cal_model(float(p),row,(models.get(m) or {}).get(str(t)),meta) for t,p in targets.items()}
        out[m]=_project_nonincreasing(calibrated)
    return out


def _build_dual_cv_specs(rows: list[dict[str,Any]], dates: list[str]) -> tuple[list[dict[str,Any]], dict[str,list[dict[str,Any]]], dict[str,Any]]:
    """Build deterministic expanding + fixed-width chronological development folds.

    The two panels score the same test blocks. Expanding folds use all prior dates;
    fixed-width folds use the same number of immediately preceding dates as the first
    expanding fold. This separates calendar/regime effects from training-sample-size
    effects without introducing a random resampling knob.
    """
    if len(dates) < 45:
        return [], {}, {"reason":"too few dates for dual rolling CV","dates":len(dates)}
    rows_by_day: dict[str,list[dict[str,Any]]] = defaultdict(list)
    for r in rows:
        rows_by_day[str(r["game_date"])].append(r)

    min_train=max(28,min(60,len(dates)//3))
    test_days=4
    available=len(dates)-min_train
    n_folds=max(4,min(5,available//test_days))
    if n_folds < 4 or available < test_days*4:
        return [], rows_by_day, {"reason":"insufficient post-warmup span","dates":len(dates),"min_train_dates":min_train}
    last_start=len(dates)-test_days
    if n_folds==1:
        starts=[min_train]
    else:
        raw=[min_train + round(i*(last_start-min_train)/(n_folds-1)) for i in range(n_folds)]
        starts=[]
        for x in raw:
            x=max(min_train,min(last_start,int(x)))
            if not starts or x>=starts[-1]+test_days:
                starts.append(x)
        # If rounding/short history collapsed a fold, greedily fill any missing slots.
        x=min_train
        while len(starts)<n_folds and x<=last_start:
            if all(abs(x-y)>=test_days for y in starts): starts.append(x)
            x+=test_days
        starts=sorted(starts)
    specs=[]
    for fi,start_idx in enumerate(starts, start=1):
        test_dates=dates[start_idx:start_idx+test_days]
        if len(test_dates)<test_days: continue
        for panel in ("expanding","fixed"):
            train_start_idx=0 if panel=="expanding" else max(0,start_idx-min_train)
            train_dates=dates[train_start_idx:start_idx]
            train_rows=[r for d in train_dates for r in rows_by_day.get(d,[])]
            starter_share,pa_dist,team_games=derive_game_environment(train_rows)
            specs.append({
                "panel":panel,"fold":fi,"test_dates":list(test_dates),
                "train_start":train_dates[0] if train_dates else None,
                "train_end":train_dates[-1] if train_dates else None,
                "train_dates":len(train_dates),"train_rows":train_rows,
                "starter_share":starter_share,"pa_distribution_by_slot":pa_dist,
                "team_games":team_games,
            })
    meta={
        "folds":len({int(x["fold"]) for x in specs}),"test_days_per_fold":test_days,
        "fixed_train_dates":min_train,"first_test_date":specs[0]["test_dates"][0] if specs else None,
        "last_test_date":specs[-1]["test_dates"][-1] if specs else None,
        "panels":["expanding","fixed"],
    }
    return specs,rows_by_day,meta


def _weighted_mean(values: list[tuple[float,float]]) -> float:
    z=sum(w for _,w in values)
    return sum(v*w for v,w in values)/z if z>0 else 99.0


def _panel_metric_summary(panel: str, panel_obs: dict[tuple[str,int],list[tuple[str,float,int]]],
                          panel_date_acc: dict[str,dict[tuple[str,int],list[float]]],
                          pred_count_by_date: dict[str,int], fold_dates: dict[int,list[str]]) -> dict[str,Any]:
    supported=[]
    for cell,obs in panel_obs.items():
        n=len(obs); pos=sum(y for _,_,y in obs); neg=n-pos
        if n>=100 and pos>=8 and neg>=8:
            supported.append(cell)
    if not supported:
        return {"panel":panel,"objective_log_loss":99.0,"objective_brier":99.0,
                "tail_log_loss":99.0,"tail_brier":99.0,"tail_calibration_mae":99.0,
                "date_scores":[],"folds":[],"included_cells":0,"tail_cells":[]}

    date_scores=[]
    for day in sorted(panel_date_acc):
        lls=[]; brs=[]
        for cell in supported:
            n,lls_sum,brs_sum=panel_date_acc[day].get(cell,(0.0,0.0,0.0))
            if n>0:
                lls.append(lls_sum/n); brs.append(brs_sum/n)
        if not lls: continue
        n_pred=max(1,int(pred_count_by_date.get(day,0)))
        date_scores.append({"date":day,"log_loss":sum(lls)/len(lls),"brier":sum(brs)/len(brs),
                            "n":n_pred,"weight":math.sqrt(n_pred)})
    objective_ll=_weighted_mean([(d["log_loss"],d["weight"]) for d in date_scores])
    objective_br=_weighted_mean([(d["brier"],d["weight"]) for d in date_scores])

    tail_cells=[]
    for market,target in supported:
        obs=panel_obs[(market,target)]
        # Tail selection is slate-relative: take each date's own highest-probability
        # decile before pooling. This targets the top-of-board failure mode directly
        # instead of letting generally high-offense dates monopolise the tail sample.
        by_day_tail: dict[str,list[tuple[str,float,int]]]=defaultdict(list)
        for item in obs: by_day_tail[item[0]].append(item)
        top=[]
        for day,day_obs in by_day_tail.items():
            ranked=sorted(day_obs,key=lambda x:x[1],reverse=True)
            k=max(1,int(math.ceil(0.10*len(ranked))))
            top.extend(ranked[:k])
        k=len(top)
        if not k: continue
        ll=-sum(y*math.log(max(1e-9,min(1-1e-9,p)))+(1-y)*math.log(max(1e-9,1-min(1-1e-9,p))) for _,p,y in top)/k
        br=sum((p-y)**2 for _,p,y in top)/k
        mp=sum(p for _,p,_ in top)/k; ar=sum(y for _,_,y in top)/k
        tail_cells.append({"market":market,"target":target,"n":k,"log_loss":ll,"brier":br,
                           "mean_p":mp,"actual_rate":ar,"calibration_error":abs(mp-ar),"selection":"top_decile_each_date"})
    tail_ll=sum(x["log_loss"] for x in tail_cells)/len(tail_cells) if tail_cells else 99.0
    tail_br=sum(x["brier"] for x in tail_cells)/len(tail_cells) if tail_cells else 99.0
    tail_ce=sum(x["calibration_error"] for x in tail_cells)/len(tail_cells) if tail_cells else 99.0

    fold_reports=[]
    by_date={d["date"]:d for d in date_scores}
    for fi,ds in sorted(fold_dates.items()):
        dd=[by_date[d] for d in ds if d in by_date]
        if not dd: continue
        fold_reports.append({"fold":fi,"dates":len(dd),
            "log_loss":_weighted_mean([(x["log_loss"],x["weight"]) for x in dd]),
            "brier":_weighted_mean([(x["brier"],x["weight"]) for x in dd]),
            "n_predictions":sum(x["n"] for x in dd),"start":dd[0]["date"],"end":dd[-1]["date"]})
    return {"panel":panel,"objective_log_loss":objective_ll,"objective_brier":objective_br,
            "tail_log_loss":tail_ll,"tail_brier":tail_br,"tail_calibration_mae":tail_ce,
            "date_scores":date_scores,"folds":fold_reports,"included_cells":len(supported),"tail_cells":tail_cells}


def _evaluate_dual_panels(contexts_by_day: dict[str,list[dict[str,Any]]], rows_by_day: dict[str,list[dict[str,Any]]],
                          specs: list[dict[str,Any]], params: dict[str,Any], min_prior_pa: int,
                          state_cache: dict[tuple[str,int,float],dict[str,Any]]) -> dict[str,Any]:
    panel_obs: dict[str,dict[tuple[str,int],list[tuple[str,float,int]]]]={"expanding":defaultdict(list),"fixed":defaultdict(list)}
    panel_date_acc: dict[str,dict[str,dict[tuple[str,int],list[float]]]]={"expanding":defaultdict(lambda:defaultdict(lambda:[0.0,0.0,0.0])),"fixed":defaultdict(lambda:defaultdict(lambda:[0.0,0.0,0.0]))}
    pred_counts: dict[str,dict[str,int]]={"expanding":defaultdict(int),"fixed":defaultdict(int)}
    fold_dates: dict[str,dict[int,list[str]]]={"expanding":{},"fixed":{}}
    decay=round(float(params.get("recent_pa_decay",DEEP_FALLBACK_PARAMS["recent_pa_decay"])),6)
    for spec in specs:
        panel=str(spec["panel"]); fi=int(spec["fold"]); fold_dates[panel][fi]=list(spec["test_dates"])
        key=(panel,fi,decay)
        if key not in state_cache:
            state_cache[key]=_bt_init_states(spec["train_rows"],{**params,"recent_pa_decay":decay})
        state=copy.deepcopy(state_cache[key])
        fp=dict(params); fp["starter_share"]=float(spec["starter_share"]); fp["pa_distribution_by_slot"]=spec["pa_distribution_by_slot"]
        for day in spec["test_dates"]:
            for ctx in contexts_by_day.get(day,[]):
                pred=_bt_predict_candidate_raw(ctx,state,fp)
                if pred is None: continue
                markets,meta=pred
                if int(meta["prior_pa"])<min_prior_pa: continue
                pred_counts[panel][day]+=1
                for market in BACKTEST_MARKETS:
                    actual=int(ctx[BACKTEST_ACTUAL_FIELD[market]])
                    for target in range(1,5):
                        p=max(1e-9,min(1-1e-9,float((markets.get(market) or {}).get(str(target),0.0))))
                        y=1 if actual>=target else 0
                        cell=(market,target)
                        panel_obs[panel][cell].append((day,p,y))
                        a=panel_date_acc[panel][day][cell]; a[0]+=1; a[1]-=y*math.log(p)+(1-y)*math.log(1-p); a[2]+=(p-y)**2
            _bt_update_states(state,sorted(rows_by_day.get(day,[]),key=lambda x:(int(x.get("game_pk") or 0),int(x.get("at_bat_number") or 0))))

    panels={p:_panel_metric_summary(p,panel_obs[p],panel_date_acc[p],pred_counts[p],fold_dates[p]) for p in ("expanding","fixed")}
    valid=[x for x in panels.values() if x["objective_log_loss"]<90]
    if not valid:
        return {"objective_log_loss":99.0,"objective_brier":99.0,"tail_log_loss":99.0,"tail_brier":99.0,
                "tail_calibration_mae":99.0,"panels":panels,"date_scores":[],"included_cells":0}
    out={
        "objective_log_loss":sum(x["objective_log_loss"] for x in valid)/len(valid),
        "objective_brier":sum(x["objective_brier"] for x in valid)/len(valid),
        "tail_log_loss":sum(x["tail_log_loss"] for x in valid)/len(valid),
        "tail_brier":sum(x["tail_brier"] for x in valid)/len(valid),
        "tail_calibration_mae":sum(x["tail_calibration_mae"] for x in valid)/len(valid),
        "panels":panels,"included_cells":sum(x["included_cells"] for x in valid),
        "decision_rule":"sqrt(N_date)-weighted game-level log loss averaged equally across expanding and fixed-width panels; candidates within paired 1-SE advance to top-decile proper-score selection",
    }
    ds=[]
    for pn,pv in panels.items():
        for d in pv.get("date_scores",[]): ds.append({**d,"panel":pn})
    out["date_scores"]=ds
    return out


def _paired_one_se(candidate: dict[str,Any], primary: dict[str,Any]) -> float:
    a={(x["panel"],x["date"]):x for x in candidate.get("date_scores",[])}
    b={(x["panel"],x["date"]):x for x in primary.get("date_scores",[])}
    keys=sorted(set(a)&set(b))
    if len(keys)<3: return 0.0
    panel_sums=defaultdict(float)
    raw=[]
    for k in keys:
        w=math.sqrt(max(1.0,(float(a[k].get("n",1))+float(b[k].get("n",1)))/2.0))
        raw.append((k,w)); panel_sums[k[0]]+=w
    panels=max(1,len(panel_sums)); vals=[]
    for k,w in raw:
        nw=(w/panel_sums[k[0]])/panels
        vals.append((float(a[k]["log_loss"])-float(b[k]["log_loss"]),nw))
    mean=sum(v*w for v,w in vals)
    sw2=sum(w*w for _,w in vals)
    if sw2>=1.0-1e-12: return 0.0
    var=sum(w*(v-mean)**2 for v,w in vals)/max(1e-12,1.0-sw2)
    neff=1.0/max(sw2,1e-12)
    return math.sqrt(max(0.0,var)/max(1.0,neff))


def _select_candidate(entries: list[tuple[Any,dict[str,Any]]]) -> tuple[Any,dict[str,Any],dict[str,Any]]:
    """Two-stage predetermined selector: primary loss, then tail score inside paired 1-SE."""
    if not entries: raise ValueError("no candidate metrics")
    primary_value,primary=min(entries,key=lambda x:(float(x[1].get("objective_log_loss",99)),float(x[1].get("objective_brier",99))))
    admiss=[]; se_map={}
    for value,m in entries:
        se=_paired_one_se(m,primary); se_map[str(value)]=se
        diff=float(m.get("objective_log_loss",99))-float(primary.get("objective_log_loss",99))
        if diff<=se+1e-12: admiss.append((value,m))
    if not admiss: admiss=[(primary_value,primary)]
    selected_value,selected=min(admiss,key=lambda x:(float(x[1].get("tail_log_loss",99)),float(x[1].get("tail_brier",99)),
                                                      float(x[1].get("tail_calibration_mae",99)),float(x[1].get("objective_log_loss",99)),
                                                      float(x[1].get("objective_brier",99))))
    panel_winners={}
    fold_winners={}
    for panel in ("expanding","fixed"):
        panel_entries=[x for x in entries if float(((x[1].get("panels") or {}).get(panel) or {}).get("objective_log_loss",99))<90]
        if panel_entries:
            pv,pm=min(panel_entries,key=lambda x:float(((x[1].get("panels") or {}).get(panel) or {}).get("objective_log_loss",99)))
            panel_winners[panel]=pv
            fold_ids=sorted({int(f.get("fold")) for _,m in panel_entries for f in (((m.get("panels") or {}).get(panel) or {}).get("folds") or [])})
            fw=[]
            for fi in fold_ids:
                cand=[]
                for value,m in panel_entries:
                    fs=[f for f in (((m.get("panels") or {}).get(panel) or {}).get("folds") or []) if int(f.get("fold"))==fi]
                    if fs: cand.append((value,float(fs[0]["log_loss"])))
                if cand: fw.append({"fold":fi,"selected":min(cand,key=lambda x:x[1])[0]})
            fold_winners[panel]=fw
    diag={"primary_value":primary_value,"selected_value":selected_value,
          "admissible_values":[v for v,_ in admiss],"paired_one_se":se_map,
          "panel_winners":panel_winners,"fold_winners":fold_winners,
          "panel_agreement":panel_winners.get("expanding")==panel_winners.get("fixed") if len(panel_winners)==2 else None}
    return selected_value,selected,diag


def _raw_expanding_predictions(contexts_by_day: dict[str,list[dict[str,Any]]], rows_by_day: dict[str,list[dict[str,Any]]],
                                specs: list[dict[str,Any]], params: dict[str,Any], min_prior_pa: int=12) -> list[dict[str,Any]]:
    out=[]
    for spec in specs:
        if spec["panel"]!="expanding": continue
        fp=dict(params); fp["starter_share"]=float(spec["starter_share"]); fp["pa_distribution_by_slot"]=spec["pa_distribution_by_slot"]
        state=_bt_init_states(spec["train_rows"],fp)
        for day in spec["test_dates"]:
            for ctx in contexts_by_day.get(day,[]):
                pred=_bt_predict_candidate_raw(ctx,state,fp)
                if pred is None: continue
                markets,meta=pred
                if int(meta["prior_pa"])<min_prior_pa: continue
                out.append({**ctx,**meta,"probabilities":markets,"cv_panel":"expanding","cv_fold":int(spec["fold"])})
            _bt_update_states(state,sorted(rows_by_day.get(day,[]),key=lambda x:(int(x.get("game_pk") or 0),int(x.get("at_bat_number") or 0))))
    return out


def fit_deep_hyperparameters(conn: sqlite3.Connection, before_day: str) -> dict[str, Any]:
    signature, coverage = tuning_coverage_signature(conn, before_day)
    cached = conn.execute("SELECT * FROM model_fits WHERE tuning_version=? AND coverage_signature=? AND before_day=? ORDER BY id DESC LIMIT 1",[DEEP_TUNING_VERSION,signature,before_day]).fetchone()
    if cached:
        return {"status":"cached-fit","version":DEEP_TUNING_VERSION,"coverage":coverage,"params":json.loads(cached["params_json"]),"metrics":json.loads(cached["metrics_json"]),"fitted_at":cached["fitted_at"]}
    rows=tuning_rows(conn,before_day,canonical_only=True); dates=sorted({str(r["game_date"]) for r in rows})
    starter_all,pa_all,team_games=derive_game_environment(rows)
    fallback=dict(DEEP_FALLBACK_PARAMS); fallback["starter_share"]=starter_all; fallback["pa_distribution_by_slot"]=pa_all
    if len(dates)<55 or len(rows)<18000:
        metrics={"reason":"insufficient local history for v1.7.1 dual-panel rolling game fit","days":len(dates),"pa":len(rows),"team_games":team_games}
        return {"status":"fallback","version":DEEP_TUNING_VERSION,"coverage":coverage,"params":fallback,"metrics":metrics,"fitted_at":None}

    contexts,_=historical_start_contexts(rows)
    specs,rows_by_day,cv_meta=_build_dual_cv_specs(rows,dates)
    if not specs:
        metrics={"reason":"could not build v1.7.1 dual rolling folds",**cv_meta,"pa":len(rows),"team_games":team_games}
        return {"status":"fallback","version":DEEP_TUNING_VERSION,"coverage":coverage,"params":fallback,"metrics":metrics,"fitted_at":None}
    first_exp=next(x for x in specs if x["panel"]=="expanding")
    seed_rows=first_exp["train_rows"]
    starter_seed,pa_seed,_=derive_game_environment(seed_rows)
    params=dict(DEEP_FALLBACK_PARAMS); params["starter_share"]=starter_seed; params["pa_distribution_by_slot"]=pa_seed
    search_trace=[]
    seed=conn.execute("SELECT * FROM model_fits WHERE tuning_version=? AND before_day<=? ORDER BY before_day DESC,id DESC LIMIT 1",["1.0-rolling-pa-logloss-v1",before_day]).fetchone()
    if seed:
        sp=json.loads(seed["params_json"])
        for key in ("player_prior_pa","hand_prior_pa","pitcher_prior_pa","recent_prior_pa","recent_pa_decay"):
            if key in sp: params[key]=float(sp[key])
        search_trace.append({"stage":"SEED","parameter":"v1.1","selected":"latest leakage-safe PA fit","before_day":seed["before_day"]})
    else:
        search_trace.append({"stage":"SEED","parameter":"fallback","selected":"v1.7.1 neutral starting defaults"})

    # Retired experiment: keep exact v1.6/v1.5.1 behaviour for total-history reliability.
    params["batter_reliability_prior_pa"] = 0.0

    state_cache: dict[tuple[str,int,float],dict[str,Any]]={}
    cache: dict[str,dict[str,Any]]={}
    def fit_progress(message: str) -> None:
        if _backtest_status.get("running"):
            _backtest_status.update({"message": f"Auto-balance · {message}", "current_date": None})
    def evaluate(trial: dict[str,Any]) -> dict[str,Any]:
        simple={k:v for k,v in trial.items() if k not in {"pa_distribution_by_slot","calibration_models","calibration_meta","recent_half_life_pa","recent_half_life_games","starter_share"}}
        key=json.dumps(simple,sort_keys=True,separators=(",",":"))
        if key not in cache:
            cache[key]=_evaluate_dual_panels(contexts,rows_by_day,specs,trial,DEFAULT_MIN_PA,state_cache)
        return cache[key]

    fit_progress("building dual-panel baseline")
    default_m=evaluate(params)
    print(f"v1.7.1 dual-panel auto-fit: {len(rows):,} historical PA; {cv_meta.get('folds')} paired folds; fixed width {cv_meta.get('fixed_train_dates')} dates; default LL {default_m['objective_log_loss']:.5f}",flush=True)

    def around(center: float, lo: float, hi: float) -> list[float]:
        return sorted({round(max(lo,min(hi,v)),6) for v in (center*0.5,center,center*2.0)})

    grids={
        "player_prior_pa":around(float(params["player_prior_pa"]),10.0,600.0),
        "hand_prior_pa":around(float(params["hand_prior_pa"]),10.0,500.0),
        "pitcher_prior_pa":around(float(params["pitcher_prior_pa"]),20.0,1000.0),
        "player_effect":[0.0,0.5,0.8,1.0,1.25],"hand_effect":[0.0,0.5,1.0,1.5],
        "pitcher_effect":[0.0,0.5,1.0,1.5],
        # New features start OFF and must earn non-zero influence blindly before their
        # shrinkage is tuned. This keeps fallback behaviour identical to v1.4.1.
        "contact_quality_effect":[0.0,0.25,0.5,1.0,1.5],
        "contact_quality_prior_pa":[24.0,60.0,120.0,240.0],
        "bullpen_effect":[0.0,0.5,1.0,1.5],
        "bullpen_prior_pa":[150.0,400.0,900.0,1800.0],
        "park_effect":[0.0,0.5,1.0,1.5],
        "park_prior_pa":[400.0,900.0,1800.0,3600.0],
        "starter_share_scale":[0.0,0.25,0.5,0.75,1.0,1.25,1.5,1.75],
        "lineup_slot_strength":[0.0,0.25,0.5,0.75,1.0,1.25,1.5,1.75],"rho_scale":[0.0,0.5,1.0,1.5,2.0],
    }
    best_m=default_m
    for key,vals in grids.items():
        fit_progress(f"pass 1 · {key}")
        cand=[]
        for v in sorted(set([float(params[key])]+[float(x) for x in vals])):
            trial=dict(params); trial[key]=v; cand.append((v,evaluate(trial)))
        chosen,chosen_m,diag=_select_candidate(cand); params[key]=float(chosen); best_m=chosen_m
        search_trace.append({"stage":"GAME","pass":1,"parameter":key,"selected":chosen,
                             "primary_log_loss":chosen_m["objective_log_loss"],"tail_log_loss":chosen_m["tail_log_loss"],**diag})
        print(f"  {key} -> {float(chosen):g}  LL {chosen_m['objective_log_loss']:.5f}  tail {chosen_m['tail_log_loss']:.5f}",flush=True)

    fit_progress("pass 1 · recency family")
    decay_grid=sorted({0.92,0.96,0.985,round(float(params["recent_pa_decay"]),6)})
    prior_grid=sorted({18.0,48.0,120.0,round(float(params["recent_prior_pa"]),6)})
    shape=[]
    for decay in decay_grid:
        for prior in prior_grid:
            trial=dict(params); trial["recent_pa_decay"]=float(decay); trial["recent_prior_pa"]=float(prior); trial["recent_effect"]=1.0
            shape.append((f"{decay:.6f}|{prior:.3f}",evaluate(trial)))
    shape_key,_,shape_diag=_select_candidate(shape); dstr,pstr=str(shape_key).split('|'); rec_decay,rec_prior=float(dstr),float(pstr)
    effects=[]
    for eff in sorted({0.0,0.5,1.0,1.5,float(params.get("recent_effect",1.0))}):
        trial=dict(params); trial["recent_pa_decay"]=rec_decay; trial["recent_prior_pa"]=rec_prior; trial["recent_effect"]=float(eff)
        effects.append((float(eff),evaluate(trial)))
    rec_eff,rec_m,eff_diag=_select_candidate(effects)
    params["recent_pa_decay"],params["recent_prior_pa"],params["recent_effect"]=rec_decay,rec_prior,float(rec_eff); best_m=rec_m
    # Candidate-decay warm states are the largest transient object in the tuner. Once
    # recency is chosen, keep only the selected decay for pass 2 instead of retaining
    # every rejected memory shape for the rest of the process.
    selected_decay=round(float(rec_decay),6)
    state_cache={k:v for k,v in state_cache.items() if abs(k[2]-selected_decay)<1e-9}
    search_trace.append({"stage":"GAME","pass":1,"parameter":"recency_family","selected":{"decay":rec_decay,"prior":rec_prior,"effect":rec_eff},
                         "shape_selection":shape_diag,"effect_selection":eff_diag,"primary_log_loss":best_m["objective_log_loss"],"tail_log_loss":best_m["tail_log_loss"]})
    print(f"  recency -> decay {rec_decay:.3f} / prior {rec_prior:.0f} / effect {float(rec_eff):g}  LL {best_m['objective_log_loss']:.5f}  tail {best_m['tail_log_loss']:.5f}",flush=True)

    for key,vals in grids.items():
        fit_progress(f"pass 2 · {key}")
        ordered=sorted(set(float(v) for v in vals)); cur=float(params[key]); nearest=min(range(len(ordered)),key=lambda i:abs(ordered[i]-cur))
        local=sorted({cur,ordered[max(0,nearest-1)],ordered[nearest],ordered[min(len(ordered)-1,nearest+1)]})
        cand=[]
        for v in local:
            trial=dict(params); trial[key]=v; cand.append((v,evaluate(trial)))
        chosen,chosen_m,diag=_select_candidate(cand); params[key]=float(chosen); best_m=chosen_m
        search_trace.append({"stage":"GAME","pass":2,"parameter":key,"candidates":local,"selected":chosen,
                             "primary_log_loss":chosen_m["objective_log_loss"],"tail_log_loss":chosen_m["tail_log_loss"],**diag})

    # Final pre-prospective complexity challenge. The full model is not allowed to
    # keep extra feature families merely because coordinate descent found tiny gains.
    # Build one explicit LEAN challenger: player + hand + pitcher + structural
    # starter/lineup/rho machinery, with recency/xBA/bullpen/park and PA reliability OFF.
    # Re-optimize only the retained knobs locally, on the exact same folds. LEAN wins
    # only when its primary loss is within paired 1-SE of FULL and its tail log loss
    # is no worse. This is a parsimony gate, not another predictive feature.
    fit_progress("simplicity challenge · LEAN vs FULL")
    full_params=dict(params); full_m=best_m
    lean_params=dict(params)
    lean_params["batter_reliability_prior_pa"]=0.0
    for _k in ("recent_effect","contact_quality_effect","bullpen_effect","park_effect"):
        lean_params[_k]=0.0
    lean_m=evaluate(lean_params)
    lean_keys=("player_prior_pa","hand_prior_pa","pitcher_prior_pa",
               "player_effect","hand_effect","pitcher_effect",
               "starter_share_scale","lineup_slot_strength","rho_scale")
    lean_trace=[]
    for key in lean_keys:
        ordered=sorted(set(float(v) for v in grids[key]))
        cur=float(lean_params[key])
        nearest=min(range(len(ordered)),key=lambda i:abs(ordered[i]-cur))
        local=sorted({cur,ordered[max(0,nearest-1)],ordered[nearest],ordered[min(len(ordered)-1,nearest+1)]})
        cand=[]
        for v in local:
            trial=dict(lean_params); trial[key]=v
            cand.append((v,evaluate(trial)))
        chosen,chosen_m,diag=_select_candidate(cand)
        lean_params[key]=float(chosen); lean_m=chosen_m
        lean_trace.append({"parameter":key,"candidates":local,"selected":chosen,
                           "primary_log_loss":chosen_m["objective_log_loss"],
                           "tail_log_loss":chosen_m["tail_log_loss"],**diag})

    paired_se=_paired_one_se(lean_m,full_m)
    lean_delta=float(lean_m["objective_log_loss"])-float(full_m["objective_log_loss"])
    lean_primary_admissible=lean_delta <= paired_se + 1e-12
    lean_tail_no_worse=float(lean_m["tail_log_loss"]) <= float(full_m["tail_log_loss"]) + 1e-12
    selected_family="LEAN" if (lean_primary_admissible and lean_tail_no_worse) else "FULL"
    if selected_family=="LEAN":
        params=lean_params; best_m=lean_m
    else:
        params=full_params; best_m=full_m
    complexity_challenge={
        "selected_family":selected_family,
        "rule":"prefer LEAN only if its dual-panel primary log loss is within paired 1-SE of FULL and its top-decile log loss is no worse",
        "full_log_loss":float(full_m["objective_log_loss"]),
        "lean_log_loss":float(lean_m["objective_log_loss"]),
        "lean_minus_full_log_loss":lean_delta,
        "paired_one_se":paired_se,
        "full_tail_log_loss":float(full_m["tail_log_loss"]),
        "lean_tail_log_loss":float(lean_m["tail_log_loss"]),
        "lean_primary_admissible":lean_primary_admissible,
        "lean_tail_no_worse":lean_tail_no_worse,
        "lean_zeroed":["recent_effect","contact_quality_effect","bullpen_effect","park_effect","batter_reliability_prior_pa"],
        "lean_refit_trace":lean_trace,
    }
    search_trace.append({"stage":"COMPLEXITY","parameter":"model_family",**complexity_challenge})

    fit_progress("feature ablations")
    feature_ablations={}
    for feature_key in ("contact_quality_effect","bullpen_effect","park_effect"):
        selected_value=float(params.get(feature_key,0.0))
        if abs(selected_value)<=1e-12:
            feature_ablations[feature_key]={"selected":selected_value,"zero_log_loss":best_m["objective_log_loss"],"zero_tail_log_loss":best_m["tail_log_loss"],"log_loss_cost_of_zero":0.0,"tail_log_loss_cost_of_zero":0.0}
            continue
        trial=dict(params); trial[feature_key]=0.0
        zm=evaluate(trial)
        feature_ablations[feature_key]={
            "selected":selected_value,
            "zero_log_loss":zm["objective_log_loss"],
            "zero_tail_log_loss":zm["tail_log_loss"],
            "log_loss_cost_of_zero":zm["objective_log_loss"]-best_m["objective_log_loss"],
            "tail_log_loss_cost_of_zero":zm["tail_log_loss"]-best_m["tail_log_loss"],
        }

    fit_progress("slice calibration")
    selected_preds=_raw_expanding_predictions(contexts,rows_by_day,specs,params,DEFAULT_MIN_PA)
    cal_models,cal_meta,cal_report=_fit_calibration_models(selected_preds)
    params["calibration_models"]=cal_models; params["calibration_meta"]=cal_meta
    cal_cells=[v for mm in cal_report.values() for v in mm.values() if int(v.get("n") or 0)>=100 and int(v.get("positives") or 0)>=20 and int(v.get("n") or 0)-int(v.get("positives") or 0)>=20]
    cal_cv_macro=(sum(float(v["cv_log_loss"]) for v in cal_cells)/len(cal_cells)) if cal_cells else None
    cal_identity_macro=(sum(float(v["identity_cv_log_loss"]) for v in cal_cells)/len(cal_cells)) if cal_cells else None
    params["starter_share"]=starter_all; params["pa_distribution_by_slot"]=pa_all
    hl=math.log(.5)/math.log(params["recent_pa_decay"]) if 0<params["recent_pa_decay"]<1 else 999.0
    params["recent_half_life_pa"]=hl; params["recent_half_life_games"]=hl/4.3
    pa_diag=_eval_tuning(rows,cv_meta.get("first_test_date") or dates[-30],params,score_limit=30000)
    metrics={
        "objective":"dual deterministic rolling panels; sqrt(N_date)-weighted game-level binary log loss across supported H/TB/HR/XBH targets, equal panel weight",
        "selection_rule":"stage 1: minimum dual-panel log loss. Candidates within one paired date-level SE of that winner advance; stage 2 chooses lowest top-decile log loss, then top-decile Brier/calibration error. AUC/lift remain diagnostics.",
        "tail_rule":"top decile is selected separately within each game date from each candidate's own pre-outcome probabilities; supported market/target cells receive equal weight",
        "rho_rule":"rho is not killed by aggregate mean alone: any rho within paired 1-SE remains eligible for tail proper-score selection",
        "date_weighting":"sqrt(number of eligible player-games on date)",
        "cv_panels":cv_meta,"historical_pa":len(rows),"team_games":team_games,
        "default_game_log_loss":default_m["objective_log_loss"],"fitted_game_log_loss":best_m["objective_log_loss"],
        "default_game_brier":default_m["objective_brier"],"fitted_game_brier":best_m["objective_brier"],
        "default_tail_log_loss":default_m["tail_log_loss"],"fitted_tail_log_loss":best_m["tail_log_loss"],
        "default_tail_brier":default_m["tail_brier"],"fitted_tail_brier":best_m["tail_brier"],
        "default_tail_calibration_mae":default_m["tail_calibration_mae"],"fitted_tail_calibration_mae":best_m["tail_calibration_mae"],
        "game_log_loss_improvement_pct":100*(default_m["objective_log_loss"]-best_m["objective_log_loss"])/max(default_m["objective_log_loss"],1e-12),
        "panels":best_m.get("panels",{}),"included_cells":best_m.get("included_cells"),"search_trace":search_trace,
        "feature_ablations":feature_ablations,
        "complexity_challenge":complexity_challenge,
        "selected_model_family":complexity_challenge.get("selected_family"),
        "calibration_meta":cal_meta,"calibration_cv":cal_report,"calibration_cv_macro_log_loss":cal_cv_macro,
        "calibration_identity_macro_log_loss":cal_identity_macro,
        "calibration_macro_improvement":None if cal_cv_macro is None or cal_identity_macro is None else cal_identity_macro-cal_cv_macro,
        "interaction_policy":"main effects only; the v1.6 RHP slope audition is retired after selecting OFF in 0/3 independent audit windows; no interaction lattice",
        "robustness_weights":"none: no hand-set lambda/mu objective weights",
        "nothing_sacred":"v1.7.1 freezes the failed PA-reliability experiment OFF and subjects the remaining FULL model to one explicit LEAN family challenge on the same folds; individual xBA/bullpen/park effects still retain exact zero candidates",
        "feature_policy":"v1.7.1 is a pre-prospective simplification pass, not a new feature build. PA reliability is fixed OFF after negligible development contribution and 0/3 historical recheck selection. FULL competes once against a locally re-optimized LEAN family with generative recency/xBA/bullpen/park OFF; LEAN is preferred only inside paired 1-SE with no worse tail LL. RHP slope remains retired. Pitch-family/weather remain deferred until timestamp-complete history exists",
        "per_pa_diagnostic_log_loss":pa_diag.get("log_loss"),"per_pa_diagnostic_brier":pa_diag.get("brier"),
        "external_holdout":"history through Sep 13 is development-spent for model/family selection. v1.7.1 is the intended freeze candidate; clean prospective validation begins Sep 14 if no Sep 14+ outcomes are used for further tuning",
    }
    fitted_at=now_iso(); conn.execute("INSERT INTO model_fits(tuning_version,coverage_signature,before_day,fitted_at,params_json,metrics_json) VALUES (?,?,?,?,?,?)",[DEEP_TUNING_VERSION,signature,before_day,fitted_at,json.dumps(params,sort_keys=True),json.dumps(metrics,sort_keys=True)]); conn.commit()
    print("v1.7.1 auto-fit selected:",{k:round(v,4) if isinstance(v,float) else v for k,v in params.items() if k not in {"pa_distribution_by_slot","calibration_models","calibration_meta"}},flush=True)
    return {"status":"fitted","version":DEEP_TUNING_VERSION,"coverage":coverage,"params":params,"metrics":metrics,"fitted_at":fitted_at}

def get_deep_tuning(conn: sqlite3.Connection, before_day: str) -> dict[str, Any]:
    with _fit_lock:
        try:
            return fit_deep_hyperparameters(conn, before_day)
        except Exception as exc:
            # Deep scoring remains usable if the tuner encounters an unexpected local-data
            # edge case. The failure is surfaced in the UI instead of destroying the slate.
            return {
                "status": "fit-error-fallback",
                "version": DEEP_TUNING_VERSION,
                "coverage": {},
                "params": dict(DEEP_FALLBACK_PARAMS),
                "metrics": {"reason": f"auto-fit failed: {exc}"},
                "fitted_at": None,
            }



# ---------------------------------------------------------------------------
# v1.2 BACKTEST LAB
# ---------------------------------------------------------------------------
# The historical evaluator deliberately separates model tuning from game-level
# evaluation. Hyperparameters are fit using only dates before the locked holdout.
# Every holdout date is then predicted before that date's PA are added to state.
# Context is reconstructed from the realised game's first nine distinct batters
# and an inferred functional starter. A short first-pitcher stint (<=9 BF) followed
# by a bulk arm (>=12 BF) is treated as an opener pattern: the bulk arm is the
# functional starter and the opener remains bullpen history. This is explicitly an
# ORACLE-CONTEXT test, not a claim that historical pregame role snapshots were saved.

BACKTEST_MARKETS = ("hits", "total_bases", "home_runs", "extra_base_hits")
BACKTEST_ACTUAL_FIELD = {
    "hits": "actual_h",
    "total_bases": "actual_tb",
    "home_runs": "actual_hr",
    "extra_base_hits": "actual_xbh",
}


def _bt_zero_counts() -> dict[str, list[float]]:
    return {"ALL": _zero_arr(), "L": _zero_arr(), "R": _zero_arr()}


def _bt_add(arr: list[float], idx: int, amount: float = 1.0) -> None:
    arr[idx] += amount


def _bt_league_probs(counts: dict[str, list[float]]) -> dict[str, list[float]]:
    fallback = [FALLBACK_OUTCOME_PRIOR[k] for k in OUTCOME_KEYS]
    return {h: _arr_posterior(counts[h], fallback, 60.0) for h in ("ALL", "L", "R")}


def historical_start_contexts(rows: list[dict[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    """Reconstruct actual starters/order from terminal PA rows.

    For each team-game, the first nine distinct batters are treated as the starting
    batting order. Pitcher role is reconstructed from completed-game PA usage: the
    first pitcher is the starter unless he faces <=9 batters and a later pitcher
    faces >=12, in which case that bulk follower is the functional starter. This is
    outcome-safe for future dates but is labelled oracle context because the role is
    reconstructed from the completed game rather than a saved pregame snapshot.
    """
    grouped: dict[tuple[str, int, str], list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        day = str(r["game_date"])
        side = "away" if str(r.get("inning_topbot") or "").lower() == "top" else "home"
        grouped[(day, int(r["game_pk"]), side)].append(r)

    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    quality = {
        "team_games": 0,
        "team_games_with_9_starters": 0,
        "team_games_short_lineup": 0,
        "unknown_starter_hand": 0,
        "starter_candidates": 0,
        "functional_starter_inferences": 0,
        "opener_proxy_team_games": 0,
    }
    for (day, game_pk, side), rs in grouped.items():
        rs.sort(key=lambda x: int(x.get("at_bat_number") or 0))
        if not rs:
            continue
        quality["team_games"] += 1
        starter_info=_functional_starter_info(rs)
        first_pitcher = starter_info.get("pitcher")
        first_hand = str(starter_info.get("hand") or "?").upper()
        quality["functional_starter_inferences"] += 1
        if starter_info.get("opener_proxy"):
            quality["opener_proxy_team_games"] += 1
        if first_hand not in {"L", "R"}:
            quality["unknown_starter_hand"] += 1
        order: list[int] = []
        seen: set[int] = set()
        for r in rs:
            b = safe_int(r.get("batter"))
            if b is None or b in seen:
                continue
            seen.add(b)
            order.append(b)
            if len(order) >= 9:
                break
        if len(order) >= 9:
            quality["team_games_with_9_starters"] += 1
        else:
            quality["team_games_short_lineup"] += 1

        team = str((rs[0].get("away_team") if side == "away" else rs[0].get("home_team")) or "")
        opp = str((rs[0].get("home_team") if side == "away" else rs[0].get("away_team")) or "")
        by_batter: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for r in rs:
            b = safe_int(r.get("batter"))
            if b is not None:
                by_batter[b].append(r)

        for slot, batter in enumerate(order[:9], start=1):
            brs = by_batter.get(batter, [])
            h = tb = hr = xbh = 0
            for r in brs:
                outcome = pa_outcome(r.get("events"))
                if outcome in HIT_OUTCOMES:
                    h += 1
                    tb += OUTCOME_BASES[outcome]
                if outcome == "hr":
                    hr += 1
                if outcome in XBH_OUTCOMES:
                    xbh += 1
            by_day[day].append({
                "game_date": day,
                "game_pk": game_pk,
                "side": side,
                "team": team,
                "opponent": opp,
                "park_team": str(rs[0].get("home_team") or ""),
                "batter": batter,
                "pitcher": first_pitcher,
                "pitcher_hand": first_hand,
                "starter_role_proxy": "bulk_follower" if starter_info.get("opener_proxy") else "first_pitcher",
                "first_pitcher": starter_info.get("first_pitcher"),
                "first_pitcher_bf": starter_info.get("first_bf",0),
                "functional_starter_bf": starter_info.get("starter_bf",0),
                "lineup_order": slot,
                "actual_pa": len(brs),
                "actual_h": h,
                "actual_tb": tb,
                "actual_hr": hr,
                "actual_xbh": xbh,
            })
            quality["starter_candidates"] += 1
    return dict(by_day), quality


def _bt_init_states(rows: list[dict[str, Any]], params: dict[str, Any]) -> dict[str, Any]:
    player: dict[int, list[float]] = defaultdict(_zero_arr)
    handc: dict[tuple[int, str], list[float]] = defaultdict(_zero_arr)
    pitcher: dict[int, list[float]] = defaultdict(_zero_arr)
    recent: dict[int, list[float]] = defaultdict(_zero_arr)
    quality_recent: dict[int, dict[str,float]] = defaultdict(_quality_zero)
    league = _bt_zero_counts()
    player_games: dict[int, list[dict[str, Any]]] = defaultdict(list)
    env=_environment_zero()
    d = float(params.get("recent_pa_decay", DEEP_FALLBACK_PARAMS["recent_pa_decay"]))

    state={
        "player": player, "handc": handc, "pitcher": pitcher, "recent": recent,
        "quality_recent": quality_recent,
        "league": league, "player_games": player_games, "recent_pa_decay": d,
        **env,
    }
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_day[str(r["game_date"])].append(r)
    for day in sorted(by_day):
        rs = sorted(by_day[day], key=lambda x: (int(x.get("game_pk") or 0), int(x.get("at_bat_number") or 0)))
        _bt_update_states(state, rs)
    return state


def _bt_update_states(state: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    d = float(state["recent_pa_decay"])
    by_batter_rows: dict[int, list[dict[str, Any]]] = defaultdict(list)
    ordered=sorted(rows,key=lambda x:(int(x.get("game_pk") or 0),int(x.get("at_bat_number") or 0)))
    starter_info=_functional_starter_by_side(ordered)
    for r in ordered:
        b = safe_int(r.get("batter")); p = safe_int(r.get("pitcher"))
        if b is None or p is None:
            continue
        h = str(r.get("p_throws") or "?").upper()
        idx = _outcome_idx(r.get("events"))
        _bt_add(state["league"]["ALL"], idx)
        if h in {"L", "R"}:
            _bt_add(state["league"][h], idx)
        _bt_add(state["player"][b], idx)
        _bt_add(state["pitcher"][p], idx)
        if h in {"L", "R"}:
            _bt_add(state["handc"][(b, h)], idx)
        rr = state["recent"][b]
        for i in range(len(rr)):
            rr[i] *= d
        rr[idx] += 1.0
        qr=state["quality_recent"][b]
        _quality_decay(qr,d)
        _quality_update(qr,r)

        home=str(r.get("home_team") or ""); away=str(r.get("away_team") or "")
        if home:
            _bt_add(state["park_home"][home],idx)
        if away:
            _bt_add(state["park_road"][away],idx)
        info=starter_info.get(_batting_side_key(r)) or {}; starter=info.get("pitcher"); fld=_fielding_team(r)
        if fld and starter is not None and p!=starter:
            _bt_add(state["bullpen"][fld],idx)
        by_batter_rows[b].append(r)
    for batter, brs in by_batter_rows.items():
        state["player_games"][batter].extend(summarise_games(brs))


def _bt_predict_candidate_raw(ctx: dict[str, Any], state: dict[str, Any], params: dict[str, Any]) -> tuple[dict[str, dict[str, float]], dict[str, Any]] | None:
    batter = safe_int(ctx.get("batter")); pitcher = safe_int(ctx.get("pitcher"))
    if batter is None or pitcher is None:
        return None
    league = _bt_league_probs(state["league"])
    prior_pa = int(sum(state["player"][batter]))
    batter_reliability = _batter_reliability(prior_pa, float(params.get("batter_reliability_prior_pa", 0.0)))
    stable_raw = _arr_posterior(state["player"][batter], league["ALL"], float(params["player_prior_pa"]))
    stable = _arr_relative(league["ALL"], league["ALL"], stable_raw, float(params.get("player_effect", 1.0))*batter_reliability)
    hand = str(ctx.get("pitcher_hand") or "?").upper()
    if hand in {"L", "R"}:
        hand_anchor = _arr_relative(stable, league["ALL"], league[hand], 1.0)
        hand_raw = _arr_posterior(state["handc"][(batter, hand)], hand_anchor, float(params["hand_prior_pa"]))
        hp = _arr_relative(stable, stable, hand_raw, float(params.get("hand_effect", 1.0))*batter_reliability)
        league_hand = league[hand]
    else:
        hp = stable; league_hand = league["ALL"]

    rp = _arr_posterior(state["recent"][batter], stable, float(params["recent_prior_pa"]))
    pre_recent = _arr_relative(hp, stable, rp, float(params.get("recent_effect", 1.0))*batter_reliability)

    # Statcast expected batting average is known immediately after each historical PA
    # and therefore can be used on the next date without outcome leakage. Treat PAs
    # without a batted ball as zero expected hits, so the signal remains on a per-PA
    # scale compatible with the model's hit probability.
    stable_hit=sum(stable[i] for i,k in enumerate(OUTCOME_KEYS) if k in HIT_OUTCOMES)
    qhit,qeff=_quality_hit_posterior(state["quality_recent"][batter],stable_hit,float(params.get("contact_quality_prior_pa",80.0)))
    pre_quality=_arr_adjust_group_target(pre_recent,HIT_OUTCOMES,qhit,float(params.get("contact_quality_effect",0.0))*batter_reliability)

    pp = _arr_posterior(state["pitcher"][pitcher], league_hand, float(params["pitcher_prior_pa"]))
    starter_adjusted = _arr_relative(pre_quality, league_hand, pp, float(params.get("pitcher_effect", 1.0)))

    opp=str(ctx.get("opponent") or "")
    bp_counts=state["bullpen"][opp] if opp else _zero_arr()
    bp_raw=_arr_posterior(bp_counts,league["ALL"],float(params.get("bullpen_prior_pa",500.0)))
    bullpen_adjusted=_arr_relative(pre_quality,league["ALL"],bp_raw,float(params.get("bullpen_effect",0.0)))

    starter_share = max(0.0, min(1.0, float(params.get("starter_share", DEEP_FALLBACK_PARAMS["starter_share"])) * float(params.get("starter_share_scale", 1.0))))
    pitching_mix = _arr_norm([(1.0 - starter_share) * bullpen_adjusted[i] + starter_share * starter_adjusted[i] for i in range(len(OUTCOME_KEYS))])

    park=str(ctx.get("park_team") or "")
    ph=state["park_home"][park] if park else _zero_arr()
    pr=state["park_road"][park] if park else _zero_arr()
    park_prior=float(params.get("park_prior_pa",1200.0))
    park_home=_arr_posterior(ph,league["ALL"],park_prior)
    park_road=_arr_posterior(pr,league["ALL"],park_prior)
    park_support=min(float(sum(ph)),float(sum(pr)))
    park_reliability=park_support/(park_support+max(1.0,park_prior))
    final=_arr_relative(pitching_mix,park_road,park_home,float(params.get("park_effect",0.0))*park_reliability)

    probs = {OUTCOME_KEYS[i]: final[i] for i in range(len(OUTCOME_KEYS))}
    pa_dist = pa_distribution([], safe_int(ctx.get("lineup_order")), params)
    p_hit_stable = stable_hit
    rho, _, rho_games = estimate_hit_overdispersion(state["player_games"].get(batter, []), p_hit_stable, float(params.get("rho_scale", 1.0)))
    markets = market_probabilities(probs, pa_dist, rho)
    p_hit_recent_signal=sum(rp[i] for i,k in enumerate(OUTCOME_KEYS) if k in HIT_OUTCOMES)
    p_hit_quality_signal=sum(pre_quality[i] for i,k in enumerate(OUTCOME_KEYS) if k in HIT_OUTCOMES)
    recent_hit_delta = p_hit_recent_signal-p_hit_stable
    cm=params.get("calibration_meta") or {}
    pq1=float(cm.get("prior_q1",100.0)); pq2=float(cm.get("prior_q2",300.0))
    rq1=float(cm.get("recent_q1",-0.005)); rq2=float(cm.get("recent_q2",0.005))
    meta = {
        "prior_pa": prior_pa,
        "batter_reliability": batter_reliability,
        "hand_pa": int(sum(state["handc"][(batter, hand)])) if hand in {"L", "R"} else 0,
        "pitcher_pa": int(sum(state["pitcher"][pitcher])),
        "bullpen_pa": int(sum(bp_counts)),
        "park_home_pa": int(sum(ph)), "park_road_pa": int(sum(pr)), "park_reliability": park_reliability,
        "rho": rho, "rho_games": rho_games,
        "expected_pa": sum(n * p for n, p in pa_dist.items()),
        "recent_hit_delta": recent_hit_delta,
        "quality_hit_target": qhit, "quality_effective_pa": qeff,
        "quality_hit_delta": p_hit_quality_signal-sum(pre_recent[i] for i,k in enumerate(OUTCOME_KEYS) if k in HIT_OUTCOMES),
        "prior_band": "LOW" if prior_pa < pq1 else ("HIGH" if prior_pa > pq2 else "MID"),
        "recent_form_band": "COLD" if recent_hit_delta < rq1 else ("HOT" if recent_hit_delta > rq2 else "NEUTRAL"),
    }
    return markets, meta


def _bt_predict_candidate(ctx: dict[str, Any], state: dict[str, Any], params: dict[str, Any]) -> tuple[dict[str, dict[str, float]], dict[str, Any]] | None:
    raw=_bt_predict_candidate_raw(ctx,state,params)
    if raw is None: return None
    markets,meta=raw
    row={**ctx,**meta}
    return _apply_calibration_matrix(markets,row,params), meta

def _safe_logit(p: float) -> float:
    p = max(1e-6, min(1.0 - 1e-6, p))
    return math.log(p / (1.0 - p))


def _auc_binary(ps: list[float], ys: list[int]) -> float | None:
    n_pos = sum(ys); n_neg = len(ys) - n_pos
    if n_pos == 0 or n_neg == 0:
        return None
    pairs = sorted(zip(ps, ys), key=lambda x: x[0])
    rank_sum_pos = 0.0
    i = 0
    rank = 1
    while i < len(pairs):
        j = i + 1
        while j < len(pairs) and abs(pairs[j][0] - pairs[i][0]) <= 1e-15:
            j += 1
        avg_rank = (rank + (rank + (j - i) - 1)) / 2.0
        rank_sum_pos += avg_rank * sum(y for _, y in pairs[i:j])
        rank += j - i
        i = j
    u = rank_sum_pos - n_pos * (n_pos + 1) / 2.0
    return u / (n_pos * n_neg)


def _calibration_logistic(ps: list[float], ys: list[int]) -> tuple[float | None, float | None]:
    if len(ps) < 20 or sum(ys) == 0 or sum(ys) == len(ys):
        return None, None
    xs = [_safe_logit(p) for p in ps]
    a, b = 0.0, 1.0
    for _ in range(12):
        g0 = g1 = h00 = h01 = h11 = 0.0
        for x, y in zip(xs, ys):
            z = max(-30.0, min(30.0, a + b * x))
            q = 1.0 / (1.0 + math.exp(-z))
            w = max(1e-8, q * (1.0 - q))
            r = y - q
            g0 += r; g1 += r * x
            h00 += w; h01 += w * x; h11 += w * x * x
        h00 += 1e-6; h11 += 1e-6
        det = h00 * h11 - h01 * h01
        if abs(det) < 1e-12:
            break
        da = (g0 * h11 - g1 * h01) / det
        db = (g1 * h00 - g0 * h01) / det
        a += da; b += db
        if abs(da) + abs(db) < 1e-7:
            break
    if not (math.isfinite(a) and math.isfinite(b)):
        return None, None
    return a, b


def _calibration_bins(items: list[dict[str, Any]], bins: int = 10) -> list[dict[str, Any]]:
    if not items:
        return []
    xs = sorted(items, key=lambda r: float(r["p"]))
    out = []
    n = len(xs)
    for bi in range(bins):
        lo = (bi * n) // bins
        hi = ((bi + 1) * n) // bins
        chunk = xs[lo:hi]
        if not chunk:
            continue
        out.append({
            "bin": bi + 1,
            "n": len(chunk),
            "p_min": round(min(float(r["p"]) for r in chunk), 6),
            "p_max": round(max(float(r["p"]) for r in chunk), 6),
            "mean_p": round(sum(float(r["p"]) for r in chunk) / len(chunk), 6),
            "actual_rate": round(sum(int(r["y"]) for r in chunk) / len(chunk), 6),
        })
    return out


def _metric_core(items: list[dict[str, Any]]) -> dict[str, Any]:
    if not items:
        return {"n": 0}
    ps = [max(1e-9, min(1.0 - 1e-9, float(r["p"]))) for r in items]
    ys = [int(r["y"]) for r in items]
    n = len(items); actual = sum(ys); expected = sum(ps); mean_p = expected / n
    brier = sum((p - y) ** 2 for p, y in zip(ps, ys)) / n
    ll = -sum(y * math.log(p) + (1 - y) * math.log(1.0 - p) for p, y in zip(ps, ys)) / n
    flat_brier = sum((mean_p - y) ** 2 for y in ys) / n
    flat_ll = -sum(y * math.log(max(1e-9, mean_p)) + (1-y) * math.log(max(1e-9, 1.0-mean_p)) for y in ys) / n
    var = sum(p * (1.0 - p) for p in ps)
    z = (actual - expected) / math.sqrt(var) if var > 1e-12 else None
    auc = _auc_binary(ps, ys)
    ci, cs = _calibration_logistic(ps, ys)
    return {
        "n": n,
        "actual": actual,
        "actual_rate": round(actual / n, 6),
        "expected": round(expected, 4),
        "mean_predicted": round(mean_p, 6),
        "observed_minus_expected": round(actual - expected, 4),
        "z_independence_diagnostic": None if z is None else round(z, 4),
        "brier": round(brier, 6),
        "flat_mean_brier": round(flat_brier, 6),
        "brier_skill_vs_flat_mean": None if flat_brier <= 1e-12 else round(1.0 - brier / flat_brier, 6),
        "log_loss": round(ll, 6),
        "flat_mean_log_loss": round(flat_ll, 6),
        "auc": None if auc is None else round(auc, 6),
        "calibration_intercept": None if ci is None else round(ci, 6),
        "calibration_slope": None if cs is None else round(cs, 6),
    }


def _slice_metrics(items: list[dict[str, Any]], key_fn) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in items:
        groups[str(key_fn(r))].append(r)
    out = []
    for key in sorted(groups):
        m = _metric_core(groups[key])
        out.append({"slice": key, **m})
    return out


def _prior_pa_fine_band(value: int | float | None) -> str:
    n=max(0,int(value or 0))
    if n < 25: return "12-24"
    if n < 50: return "25-49"
    if n < 100: return "50-99"
    if n < 200: return "100-199"
    if n < 400: return "200-399"
    return "400+"


def _topn_metrics(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in items:
        by_day[str(r["game_date"])].append(r)
    overall = _metric_core(items)
    base = float(overall.get("actual_rate") or 0.0)
    out = []
    for topn in (5, 10, 20):
        chosen = []
        for day in sorted(by_day):
            chosen.extend(sorted(by_day[day], key=lambda x: float(x["p"]), reverse=True)[:topn])
        m = _metric_core(chosen)
        rate = float(m.get("actual_rate") or 0.0)
        m.update({"top_n": topn, "lift_vs_all_actual": None if base <= 0 else round(rate / base, 6)})
        out.append(m)
    return out


def backtest_market_summary(predictions: list[dict[str, Any]], market: str, target: int, date_order: list[str]) -> dict[str, Any]:
    actual_field = BACKTEST_ACTUAL_FIELD[market]
    items = []
    date_pos = {d: i for i, d in enumerate(date_order)}
    n_dates = max(1, len(date_order))
    for r in predictions:
        p = float((r["probabilities"].get(market) or {}).get(str(target), 0.0))
        y = 1 if int(r[actual_field]) >= target else 0
        item = {**r, "p": p, "y": y}
        idx = date_pos.get(str(r["game_date"]), 0)
        third = min(2, (idx * 3) // n_dates)
        item["chronology_third"] = ("EARLY", "MIDDLE", "LATE")[third]
        items.append(item)
    core = _metric_core(items)
    return {
        **core,
        "market": market,
        "target": target,
        "calibration": _calibration_bins(items, 10),
        "top_n": _topn_metrics(items),
        "slices": {
            "pitcher_hand": _slice_metrics(items, lambda r: r.get("pitcher_hand") or "?"),
            "lineup_band": _slice_metrics(items, lambda r: "1-3" if int(r.get("lineup_order") or 9) <= 3 else ("4-6" if int(r.get("lineup_order") or 9) <= 6 else "7-9")),
            "prior_pa": _slice_metrics(items, lambda r: r.get("prior_band") or ("<100" if int(r.get("prior_pa") or 0) < 100 else ("100-299" if int(r.get("prior_pa") or 0) < 300 else "300+"))),
            "prior_pa_fine": _slice_metrics(items, lambda r: _prior_pa_fine_band(r.get("prior_pa"))),
            "recent_form": _slice_metrics(items, lambda r: r.get("recent_form_band") or "UNKNOWN"),
            "chronology": _slice_metrics(items, lambda r: r["chronology_third"]),
        },
    }


def _backtest_all_metrics(predictions: list[dict[str, Any]], date_order: list[str]) -> dict[str, Any]:
    markets: dict[str, dict[str, Any]] = {}
    for market in BACKTEST_MARKETS:
        markets[market] = {}
        for target in range(1, 5):
            markets[market][str(target)] = backtest_market_summary(predictions, market, target, date_order)
    return {"markets": markets}


def _backtest_run_key(coverage_signature: str, holdout_start: str, holdout_end: str, holdout_days: int, min_prior_pa: int) -> str:
    raw = "|".join([
        BACKTEST_VERSION, DEEP_MODEL_VERSION, DEEP_TUNING_VERSION, coverage_signature,
        holdout_start, holdout_end, str(holdout_days), str(min_prior_pa), "oracle-context",
    ])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def backtest_latest_run(conn: sqlite3.Connection) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM backtest_runs ORDER BY id DESC LIMIT 1").fetchone()
    if not row:
        return None
    return backtest_run_payload(row)


def backtest_run_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": int(row["id"]), "run_key": row["run_key"],
        "backtest_version": row["backtest_version"], "model_version": row["model_version"],
        "tuning_version": row["tuning_version"], "coverage_signature": row["coverage_signature"],
        "holdout_start": row["holdout_start"], "holdout_end": row["holdout_end"],
        "holdout_days": int(row["holdout_days"]), "min_prior_pa": int(row["min_prior_pa"]),
        "context_mode": row["context_mode"], "created_at": row["created_at"],
        "tuning": json.loads(row["tuning_json"]), "context": json.loads(row["context_json"]),
        "metrics": json.loads(row["metrics_json"]),
    }



def historical_audit_latest_run(conn: sqlite3.Connection) -> dict[str, Any] | None:
    row=conn.execute("SELECT * FROM historical_audit_runs ORDER BY id DESC LIMIT 1").fetchone()
    return historical_audit_run_payload(row) if row else None


def historical_audit_run_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": int(row["id"]), "run_key": row["run_key"],
        "audit_version": row["audit_version"], "model_version": row["model_version"],
        "tuning_version": row["tuning_version"], "coverage_signature": row["coverage_signature"],
        "windows": int(row["windows"]), "window_days": int(row["window_days"]),
        "min_prior_pa": int(row["min_prior_pa"]), "created_at": row["created_at"],
        "config": json.loads(row["config_json"]), "result": json.loads(row["result_json"]),
    }


def _historical_audit_plan(dates: list[str], windows: int, window_days: int, min_train_dates: int = 55) -> list[list[str]]:
    """Choose deterministic, non-overlapping pseudo-prospective windows across history.

    Window outcomes never participate in that window's parameter fit. The first window
    begins only after enough prior dates exist for the nested tuner; later windows are
    spread across the remaining pre-development history instead of cherry-picked.
    """
    if len(dates) <= min_train_dates + window_days:
        return []
    pool=dates[min_train_dates:]
    max_nonoverlap=len(pool)//window_days
    use=min(max(2,int(windows)), max_nonoverlap)
    if use < 2:
        return []
    if use == 2:
        starts=[0, len(pool)-window_days]
    else:
        span=len(pool)-window_days
        starts=[int(round(i*span/(use-1))) for i in range(use)]
    # The arithmetic above is non-overlapping whenever len(pool) >= use*window_days.
    out=[]; last_end=-1
    for st in starts:
        st=max(last_end+1,st)
        block=pool[st:st+window_days]
        if len(block)!=window_days: continue
        out.append(block); last_end=st+window_days-1
    return out


def _audit_run_key(coverage_signature: str, cutoff_date: str, plan: list[list[str]], min_prior_pa: int) -> str:
    bits=[AUDIT_VERSION,DEEP_MODEL_VERSION,DEEP_TUNING_VERSION,coverage_signature,cutoff_date,str(min_prior_pa)]
    bits.extend(f"{b[0]}:{b[-1]}:{len(b)}" for b in plan)
    return hashlib.sha256("|".join(bits).encode("utf-8")).hexdigest()[:32]


def _audit_slice_row(summary: dict[str,Any], kind: str, label: str) -> dict[str,Any] | None:
    for r in ((summary.get("slices") or {}).get(kind) or []):
        if str(r.get("slice"))==label:
            return r
    return None


def _audit_window_score(conn: sqlite3.Connection, block: list[str], min_prior_pa: int, window_index: int, total_windows: int) -> tuple[list[dict[str,Any]],dict[str,Any],dict[str,Any]]:
    start,end=block[0],block[-1]
    end_exclusive=(date.fromisoformat(end)+timedelta(days=1)).isoformat()
    _audit_status.update({"current_window":window_index,"current_date":start,
                          "message":f"Window {window_index}/{total_windows}: fitting only history before {start}"})
    # Critical pseudo-blind boundary: this fit cannot see any outcome from the window.
    tuning=get_deep_tuning(conn,start)
    params=dict(tuning.get("params") or DEEP_FALLBACK_PARAMS)
    rows=tuning_rows(conn,end_exclusive,canonical_only=True)
    train_rows=[r for r in rows if str(r["game_date"])<start]
    if not train_rows:
        raise RuntimeError(f"No training rows before audit window {start}")
    contexts_by_day,context_quality=historical_start_contexts(rows)
    state=_bt_init_states(train_rows,params)
    rows_by_day: dict[str,list[dict[str,Any]]]=defaultdict(list)
    block_set=set(block)
    for r in rows:
        if str(r["game_date"]) in block_set:
            rows_by_day[str(r["game_date"])].append(r)
    preds=[]
    for di,day in enumerate(block,1):
        _audit_status.update({"current_window":window_index,"current_date":day,
                              "message":f"Window {window_index}/{total_windows}: predict {day} before reveal"})
        for ctx in contexts_by_day.get(day,[]):
            pred=_bt_predict_candidate(ctx,state,params)
            if pred is None: continue
            markets,meta=pred
            if int(meta["prior_pa"])<min_prior_pa: continue
            preds.append({
                **ctx,
                "prior_pa":int(meta["prior_pa"]),"hand_pa":int(meta["hand_pa"]),"pitcher_pa":int(meta["pitcher_pa"]),
                "expected_pa":round(float(meta["expected_pa"]),4),"rho":round(float(meta["rho"]),6),
                "recent_hit_delta":round(float(meta.get("recent_hit_delta",0.0)),8),
                "recent_form_band":meta.get("recent_form_band"),"prior_band":meta.get("prior_band"),
                "probabilities":markets,
            })
        day_rows=sorted(rows_by_day.get(day,[]),key=lambda x:(int(x.get("game_pk") or 0),int(x.get("at_bat_number") or 0)))
        _bt_update_states(state,day_rows)
        _audit_status["predictions"]=int(_audit_status.get("predictions") or 0)+sum(1 for r in preds if str(r["game_date"])==day)
    metrics=_backtest_all_metrics(preds,block)
    return preds,tuning,{"quality":context_quality,"metrics":metrics}


def run_historical_audit(windows: int = 3, window_days: int = 21, min_prior_pa: int = 12) -> dict[str,Any]:
    """Pseudo-prospective replication carousel, diagnostic-only.

    Each historical window gets a fresh fit whose `before_day` is the window start,
    then the whole window is scored chronologically with parameters frozen. Outcomes
    are revealed only after each date's predictions. The audit never changes the
    current production fit or auto-applies residual corrections; it only reports
    whether the same error topology reappears on other calendar blocks.
    """
    windows=max(2,min(int(windows),5)); window_days=max(7,min(int(window_days),30)); min_prior_pa=max(0,min(int(min_prior_pa),1000))
    with db_connect() as conn:
        all_dates=[str(r[0]) for r in conn.execute("SELECT DISTINCT game_date FROM pitches WHERE events IS NOT NULL AND events<>'' ORDER BY game_date").fetchall()]
        if len(all_dates)<80:
            raise RuntimeError(f"Need roughly 80+ historical game dates for replication audit; local DB has {len(all_dates)}")
        # Keep the repeatedly-inspected main development holdout out of the replication carousel.
        latest_bt=conn.execute("SELECT holdout_start,holdout_end FROM backtest_runs ORDER BY id DESC LIMIT 1").fetchone()
        if latest_bt and latest_bt["holdout_start"]:
            cutoff=str(latest_bt["holdout_start"])
            cutoff_source=f"latest development holdout starts {cutoff}"
        else:
            prepros=[d for d in all_dates if d < PROSPECTIVE_START]
            if len(prepros)<60: raise RuntimeError("Not enough pre-prospective history for audit")
            cutoff=prepros[-30] if len(prepros)>=85 else prepros[-14]
            cutoff_source="derived pre-prospective reserve"
        dates=[d for d in all_dates if d<cutoff]
        plan=_historical_audit_plan(dates,windows,window_days,55)
        if len(plan)<2:
            raise RuntimeError(f"Not enough pre-{cutoff} history for at least two {window_days}-date audit windows after 55 training dates")
        actual_windows=len(plan)
        coverage_signature,coverage=tuning_coverage_signature(conn,cutoff)
        run_key=_audit_run_key(coverage_signature,cutoff,plan,min_prior_pa)
        existing=conn.execute("SELECT * FROM historical_audit_runs WHERE run_key=?",[run_key]).fetchone()
        if existing:
            out=historical_audit_run_payload(existing); out["reused"]=True; return out

        all_preds=[]; window_results=[]; all_window_dates=[]
        _audit_status.update({"total":actual_windows,"done":0,"predictions":0})
        for wi,block in enumerate(plan,1):
            preds,tuning,extra=_audit_window_score(conn,block,min_prior_pa,wi,actual_windows)
            all_preds.extend(preds); all_window_dates.extend(block)
            hit2=(((extra["metrics"].get("markets") or {}).get("hits") or {}).get("2") or {})
            rhp=_audit_slice_row(hit2,"pitcher_hand","R")
            mid=_audit_slice_row(hit2,"prior_pa","MID")
            cal2=((((tuning.get("metrics") or {}).get("calibration_cv") or {}).get("hits") or {}).get("2") or {})
            params=tuning.get("params") or {}
            window_results.append({
                "index":wi,"start":block[0],"end":block[-1],"game_dates":len(block),
                "train_dates":sum(1 for d in dates if d<block[0]),"player_games":len(preds),
                "tuning_status":tuning.get("status"),
                "rhp_slope_selected_hits_2":bool(cal2.get("rhp_slope")),
                "rhp_slope_kind_hits_2":cal2.get("kind"),
                "selected_effects":{k:params.get(k) for k in ("player_effect","hand_effect","recent_effect","pitcher_effect","contact_quality_effect","bullpen_effect","park_effect","starter_share_scale","lineup_slot_strength","rho_scale")},
                "model_family":(tuning.get("metrics") or {}).get("selected_model_family") or "FULL",
                "complexity_challenge":(tuning.get("metrics") or {}).get("complexity_challenge") or {},
                "feature_ablations":(tuning.get("metrics") or {}).get("feature_ablations") or {},
                "metrics":extra["metrics"],
                "hits_2":hit2,
                "rhp_gap_pp":None if not rhp else round(100*(float(rhp.get("mean_predicted") or 0)-float(rhp.get("actual_rate") or 0)),3),
                "mid_prior_gap_pp":None if not mid else round(100*(float(mid.get("mean_predicted") or 0)-float(mid.get("actual_rate") or 0)),3),
                "context_quality":extra.get("quality") or {},
            })
            _audit_status.update({"done":wi,"message":f"Completed historical window {wi}/{actual_windows}"})

        aggregate=_backtest_all_metrics(all_preds,all_window_dates)
        hit2agg=(((aggregate.get("markets") or {}).get("hits") or {}).get("2") or {})
        rhpagg=_audit_slice_row(hit2agg,"pitcher_hand","R")
        midagg=_audit_slice_row(hit2agg,"prior_pa","MID")
        result={
            "mode":"pseudo-prospective-diagnostic",
            "cutoff_date":cutoff,"cutoff_source":cutoff_source,"prospective_start":PROSPECTIVE_START,
            "window_plan":[{"index":i+1,"start":b[0],"end":b[-1],"game_dates":len(b)} for i,b in enumerate(plan)],
            "windows":window_results,
            "aggregate_metrics":aggregate,
            "diagnostic_summary":{
                "hits_2_rhp_gap_pp":None if not rhpagg else round(100*(float(rhpagg.get("mean_predicted") or 0)-float(rhpagg.get("actual_rate") or 0)),3),
                "hits_2_mid_prior_gap_pp":None if not midagg else round(100*(float(midagg.get("mean_predicted") or 0)-float(midagg.get("actual_rate") or 0)),3),
                "rhp_slope_selected_windows":sum(1 for w in window_results if w.get("rhp_slope_selected_hits_2")),
                "lean_family_selected_windows":sum(1 for w in window_results if str(w.get("model_family") or "FULL")=="LEAN"),
                "window_count":actual_windows,
            },
            "method":"for each window, nested model/calibration fit uses only dates before window start; parameters are frozen for the full window; sufficient statistics update only after each scored date",
            "use_policy":"development diagnostic only for v1.7.1 family pruning; these historical windows are development-spent. Outcomes are never auto-fed into tuning. Freeze the chosen family before Sep 14 games; clean validation is prospective Sep 14+",
            "coverage":coverage,"prediction_count":len(all_preds),
        }
        config={"requested_windows":windows,"actual_windows":actual_windows,"window_days":window_days,"min_prior_pa":min_prior_pa,"cutoff_date":cutoff,"plan":result["window_plan"]}
        created=now_iso()
        cur=conn.execute("INSERT INTO historical_audit_runs(run_key,audit_version,model_version,tuning_version,coverage_signature,windows,window_days,min_prior_pa,created_at,config_json,result_json) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [run_key,AUDIT_VERSION,DEEP_MODEL_VERSION,DEEP_TUNING_VERSION,coverage_signature,actual_windows,window_days,min_prior_pa,created,json.dumps(config,sort_keys=True),json.dumps(result,sort_keys=True)])
        conn.commit(); row=conn.execute("SELECT * FROM historical_audit_runs WHERE id=?",[int(cur.lastrowid)]).fetchone()
        out=historical_audit_run_payload(row); out["reused"]=False; return out


def historical_audit_worker(windows: int, window_days: int, min_prior_pa: int) -> None:
    global _audit_status
    with _audit_lock:
        try:
            _audit_status.update({"running":True,"message":"Planning pseudo-prospective windows","done":0,"total":0,"current_window":None,"current_date":None,"error":None,"run_id":None,"started_at":now_iso(),"finished_at":None,"predictions":0})
            result=run_historical_audit(windows,window_days,min_prior_pa)
            _audit_status.update({"running":False,"message":"Complete"+(" · reused immutable audit" if result.get("reused") else ""),"run_id":result.get("id"),"current_window":None,"current_date":None,"finished_at":now_iso(),"error":None})
        except Exception as exc:
            _audit_status.update({"running":False,"message":"Failed","error":str(exc),"finished_at":now_iso(),"current_window":None,"current_date":None})
            print(f"Historical audit: ERROR: {exc}",flush=True)


def start_historical_audit(windows: int = 3, window_days: int = 21, min_prior_pa: int = 12) -> dict[str,Any]:
    if _audit_status.get("running"):
        return dict(_audit_status)
    threading.Thread(target=historical_audit_worker,args=(windows,window_days,min_prior_pa),daemon=True).start()
    return dict(_audit_status)

def run_locked_backtest(holdout_days: int = 30, min_prior_pa: int = 12) -> dict[str, Any]:
    """Run or reuse one immutable game-level holdout evaluation.

    All target matrices are emitted in one pass, so changing the UI market/target does
    not re-touch the holdout. An identical model/data/config run is reused from the
    append-only tables rather than recomputed.
    """
    holdout_days = max(7, min(int(holdout_days), 90))
    min_prior_pa = max(0, min(int(min_prior_pa), 1000))
    with db_connect() as conn:
        dates = [str(r[0]) for r in conn.execute(
            "SELECT DISTINCT game_date FROM pitches WHERE events IS NOT NULL AND events<>'' ORDER BY game_date"
        ).fetchall()]
        if len(dates) < 20:
            raise RuntimeError(f"Need at least 20 historical game dates; local DB has {len(dates)}")
        max_holdout = max(7, len(dates) - 12)
        holdout_days = min(holdout_days, max_holdout)
        holdout_dates = dates[-holdout_days:]
        holdout_start, holdout_end = holdout_dates[0], holdout_dates[-1]
        end_exclusive = (date.fromisoformat(holdout_end) + timedelta(days=1)).isoformat()
        coverage_signature, coverage = tuning_coverage_signature(conn, end_exclusive)
        run_key = _backtest_run_key(coverage_signature, holdout_start, holdout_end, holdout_days, min_prior_pa)
        existing = conn.execute("SELECT * FROM backtest_runs WHERE run_key=?", [run_key]).fetchone()
        if existing:
            payload = backtest_run_payload(existing)
            payload["reused"] = True
            return payload

        rows = tuning_rows(conn, end_exclusive, canonical_only=True)
        # Hyperparameters are selected without seeing the locked block.
        tuning = get_deep_tuning(conn, holdout_start)
        params = dict(tuning.get("params") or DEEP_FALLBACK_PARAMS)
        train_rows = [r for r in rows if str(r["game_date"]) < holdout_start]
        if not train_rows:
            raise RuntimeError("No pre-holdout training rows available")
        contexts_by_day, context_quality = historical_start_contexts(rows)
        state = _bt_init_states(train_rows, params)
        holdout_rows_by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
        holdout_date_set = set(holdout_dates)
        for r in rows:
            d = str(r["game_date"])
            if d in holdout_date_set:
                holdout_rows_by_day[d].append(r)

        predictions: list[dict[str, Any]] = []
        total = len(holdout_dates)
        _backtest_status.update({"total": total, "done": 0, "predictions": 0})
        for di, day in enumerate(holdout_dates, start=1):
            _backtest_status.update({"current_date": day, "message": f"Predicting {day} before reveal", "done": di - 1})
            # Every candidate on this date is scored from state containing only earlier dates.
            for ctx in contexts_by_day.get(day, []):
                pred = _bt_predict_candidate(ctx, state, params)
                if pred is None:
                    continue
                markets, meta = pred
                if int(meta["prior_pa"]) < min_prior_pa:
                    continue
                predictions.append({
                    **ctx,
                    "prior_pa": int(meta["prior_pa"]),
                    "hand_pa": int(meta["hand_pa"]),
                    "pitcher_pa": int(meta["pitcher_pa"]),
                    "expected_pa": round(float(meta["expected_pa"]), 4),
                    "rho": round(float(meta["rho"]), 6),
                    "recent_hit_delta": round(float(meta.get("recent_hit_delta", 0.0)), 8),
                    "recent_form_band": meta.get("recent_form_band"),
                    "prior_band": meta.get("prior_band"),
                    "probabilities": markets,
                })
            # Reveal/update only after all predictions for this date are frozen.
            day_rows = sorted(holdout_rows_by_day.get(day, []), key=lambda x: (int(x.get("game_pk") or 0), int(x.get("at_bat_number") or 0)))
            _bt_update_states(state, day_rows)
            _backtest_status.update({"done": di, "predictions": len(predictions), "message": f"Scored + revealed {day}"})

        if not predictions:
            raise RuntimeError("Backtest generated zero eligible player-games; lower Min prior PA or load more history")
        metrics = _backtest_all_metrics(predictions, holdout_dates)
        context = {
            "mode": "oracle-context",
            "description": "actual starting lineup/order plus functional opposing starter reconstructed from completed terminal PA rows; opener proxy is first<=9 BF with bulk follower>=12 BF; predictions use only earlier dates",
            "lineup_reconstruction": "first nine distinct batters for each team-game",
            "starter_reconstruction": "functional starter inferred from completed-game PA roles (bulk follower replaces short opener when first<=9 BF and follower>=12 BF)",
            "holdout_isolation": "deep hyperparameters fit only on dates before holdout_start; each holdout date predicted before that date updates player, handedness, recent/xBA quality, starter, bullpen, park and league state",
            "feature_provenance": "v1.7.1 keeps the timestamp-safe v1.6 feature plumbing, fixes PA reliability OFF, and adds only a same-fold FULL-vs-LEAN parsimony gate before prospective freeze",
            "quality_all_loaded_dates": context_quality,
            "coverage": coverage,
            "prediction_count": len(predictions),
        }
        created = now_iso()
        cur = conn.execute(
            """
            INSERT INTO backtest_runs(
                run_key, backtest_version, model_version, tuning_version, coverage_signature,
                holdout_start, holdout_end, holdout_days, min_prior_pa, context_mode,
                created_at, tuning_json, context_json, metrics_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [run_key, BACKTEST_VERSION, DEEP_MODEL_VERSION, DEEP_TUNING_VERSION, coverage_signature,
             holdout_start, holdout_end, holdout_days, min_prior_pa, "oracle-context",
             created, json.dumps(tuning, sort_keys=True), json.dumps(context, sort_keys=True), json.dumps(metrics, sort_keys=True)],
        )
        run_id = int(cur.lastrowid)
        conn.executemany(
            """
            INSERT INTO backtest_predictions(
                run_id, game_date, game_pk, batter, pitcher, team, opponent, lineup_order,
                pitcher_hand, prior_pa, hand_pa, pitcher_pa, expected_pa, recent_hit_delta,
                recent_form_band, prior_band, rho,
                actual_pa, actual_h, actual_tb, actual_hr, actual_xbh, probabilities_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [[run_id, r["game_date"], int(r["game_pk"]), int(r["batter"]), r.get("pitcher"), r.get("team"), r.get("opponent"),
              r.get("lineup_order"), r.get("pitcher_hand"), int(r["prior_pa"]), int(r.get("hand_pa") or 0), int(r.get("pitcher_pa") or 0),
              float(r.get("expected_pa") or 0.0), float(r.get("recent_hit_delta") or 0.0), r.get("recent_form_band"), r.get("prior_band"), float(r.get("rho") or 0.0),
              int(r["actual_pa"]), int(r["actual_h"]), int(r["actual_tb"]), int(r["actual_hr"]), int(r["actual_xbh"]), json.dumps(r["probabilities"], sort_keys=True)] for r in predictions],
        )
        conn.commit()
        row = conn.execute("SELECT * FROM backtest_runs WHERE id=?", [run_id]).fetchone()
        payload = backtest_run_payload(row)
        payload["reused"] = False
        return payload


def backtest_worker(holdout_days: int, min_prior_pa: int) -> None:
    global _backtest_status
    with _backtest_lock:
        try:
            _backtest_status.update({
                "running": True, "message": "Preparing locked holdout", "done": 0, "total": 0,
                "current_date": None, "error": None, "run_id": None, "run_key": None,
                "started_at": now_iso(), "finished_at": None, "predictions": 0,
            })
            result = run_locked_backtest(holdout_days, min_prior_pa)
            _backtest_status.update({
                "running": False, "message": "Complete" + (" · reused immutable run" if result.get("reused") else ""),
                "run_id": result.get("id"), "run_key": result.get("run_key"), "current_date": None,
                "finished_at": now_iso(), "error": None,
            })
        except Exception as exc:
            _backtest_status.update({"running": False, "message": "Failed", "error": str(exc), "finished_at": now_iso(), "current_date": None})
            print(f"Backtest: ERROR: {exc}", flush=True)


def start_backtest(holdout_days: int = 30, min_prior_pa: int = 12) -> dict[str, Any]:
    if _backtest_status.get("running"):
        return dict(_backtest_status)
    _backtest_status.update({
        "running": True, "message": "Starting", "done": 0, "total": 0, "current_date": None,
        "error": None, "run_id": None, "run_key": None, "started_at": now_iso(), "finished_at": None,
        "predictions": 0,
    })
    threading.Thread(target=backtest_worker, args=(holdout_days, min_prior_pa), daemon=True).start()
    return dict(_backtest_status)

def weighted_recent_outcome_counts_pa_decay(pa_rows: list[dict[str, Any]], pa_decay: float) -> tuple[dict[str, float], float]:
    counts = {k: 0.0 for k in OUTCOME_KEYS}
    eff_n = 0.0
    for i, r in enumerate(reversed(pa_rows)):
        w = pa_decay ** i
        if w < 0.0005:
            break
        counts[pa_outcome(r.get("events"))] += w
        eff_n += w
    return counts, eff_n

def weighted_recent_quality_pa_decay(pa_rows: list[dict[str, Any]], pa_decay: float) -> dict[str,float]:
    q=_quality_zero()
    for i,r in enumerate(reversed(pa_rows)):
        w=pa_decay**i
        if w<0.0005:
            break
        _quality_update(q,r,w)
    return q


def adjust_group_probability(probs: dict[str,float], keys: set[str], target_prob: float, effect: float) -> dict[str,float]:
    arr=[float(probs.get(k,0.0)) for k in OUTCOME_KEYS]
    out=_arr_adjust_group_target(arr,keys,target_prob,effect)
    return {k:out[i] for i,k in enumerate(OUTCOME_KEYS)}


def deep_environment_context(conn: sqlite3.Connection, before_day: str) -> dict[str,Any]:
    # Historical/local only: no network call. Every row is strictly earlier than the
    # slate being scored, so park and bullpen features obey the same pregame boundary.
    return _environment_from_rows(tuning_rows(conn,before_day))

def deep_generative_model(
    candidate: dict[str, Any], pa_rows: list[dict[str, Any]], pitcher_pa_rows: list[dict[str, Any]],
    games: list[dict[str, Any]], league_priors: dict[str, dict[str, float]], tuning: dict[str, Any],
    environment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    params = tuning.get("params") or DEEP_FALLBACK_PARAMS
    environment=environment or _environment_zero()
    league_all = league_priors.get("ALL") or FALLBACK_OUTCOME_PRIOR
    all_counts, n_all = outcome_counts(pa_rows)
    batter_reliability = _batter_reliability(n_all, float(params.get("batter_reliability_prior_pa", 0.0)))
    stable_raw = posterior_probs(all_counts, league_all, float(params["player_prior_pa"]))
    stable = relative_adjust(league_all, league_all, stable_raw, float(params.get("player_effect", 1.0))*batter_reliability)

    hand = str((candidate.get("pitcher") or {}).get("hand") or "?").upper()
    hand_n = 0.0
    if hand in {"L", "R"}:
        hand_counts, hand_n = outcome_counts(pa_rows, hand)
        league_hand = league_priors.get(hand) or league_all
        hand_anchor = relative_adjust(stable, league_all, league_hand, 1.0)
        hand_raw = posterior_probs(hand_counts, hand_anchor, float(params["hand_prior_pa"]))
        hand_probs = relative_adjust(stable, stable, hand_raw, float(params.get("hand_effect", 1.0))*batter_reliability)
    else:
        league_hand = league_all
        hand_probs = stable

    pa_decay = float(params["recent_pa_decay"])
    recent_counts, recent_eff_n = weighted_recent_outcome_counts_pa_decay(pa_rows, pa_decay)
    recent_probs = posterior_probs(recent_counts, stable, float(params["recent_prior_pa"]))
    pre_recent = relative_adjust(hand_probs, stable, recent_probs, float(params.get("recent_effect", 1.0))*batter_reliability)

    quality_state=weighted_recent_quality_pa_decay(pa_rows,pa_decay)
    stable_hit_prob=sum(stable[k] for k in HIT_OUTCOMES)
    qhit,qeff=_quality_hit_posterior(quality_state,stable_hit_prob,float(params.get("contact_quality_prior_pa",80.0)))
    pre_quality=adjust_group_probability(pre_recent,HIT_OUTCOMES,qhit,float(params.get("contact_quality_effect",0.0))*batter_reliability)

    pitcher_counts, pitcher_n = outcome_counts(pitcher_pa_rows)
    starter_share = max(0.0, min(1.0,
        float(params.get("starter_share", DEEP_FALLBACK_PARAMS["starter_share"]))
        * float(params.get("starter_share_scale", 1.0))
    ))
    if pitcher_n > 0:
        pitcher_probs = posterior_probs(pitcher_counts, league_hand, float(params["pitcher_prior_pa"]))
        starter_adjusted = relative_adjust(pre_quality, league_hand, pitcher_probs, float(params.get("pitcher_effect", 1.0)))
    else:
        pitcher_probs = dict(league_hand)
        starter_adjusted = dict(pre_quality)

    opp=str(candidate.get("opp_team") or "")
    bp_arr=(environment.get("bullpen") or {}).get(opp) if opp else None
    bp_arr=list(bp_arr) if bp_arr is not None else _zero_arr()
    bp_probs_arr=_arr_posterior(bp_arr,[league_all[k] for k in OUTCOME_KEYS],float(params.get("bullpen_prior_pa",500.0)))
    bullpen_probs={k:bp_probs_arr[i] for i,k in enumerate(OUTCOME_KEYS)}
    bullpen_adjusted=relative_adjust(pre_quality,league_all,bullpen_probs,float(params.get("bullpen_effect",0.0)))
    pitching_mix=normalise_probs({
        k:(1.0-starter_share)*bullpen_adjusted[k]+starter_share*starter_adjusted[k]
        for k in OUTCOME_KEYS
    })

    park=str(candidate.get("park_team") or "")
    ph=(environment.get("park_home") or {}).get(park) if park else None
    pr=(environment.get("park_road") or {}).get(park) if park else None
    ph_arr=list(ph) if ph is not None else _zero_arr(); pr_arr=list(pr) if pr is not None else _zero_arr()
    league_arr=[league_all[k] for k in OUTCOME_KEYS]
    park_prior=float(params.get("park_prior_pa",1200.0))
    park_home_arr=_arr_posterior(ph_arr,league_arr,park_prior)
    park_road_arr=_arr_posterior(pr_arr,league_arr,park_prior)
    park_home={k:park_home_arr[i] for i,k in enumerate(OUTCOME_KEYS)}
    park_road={k:park_road_arr[i] for i,k in enumerate(OUTCOME_KEYS)}
    park_support=min(float(sum(ph_arr)),float(sum(pr_arr)))
    park_reliability=park_support/(park_support+max(1.0,park_prior))
    final_probs=relative_adjust(pitching_mix,park_road,park_home,float(params.get("park_effect",0.0))*park_reliability)

    pads = pa_distribution(games, candidate.get("lineup_order"), params)
    p_hit_stable = stable_hit_prob
    hit_rho, hit_rho_raw, rho_games = estimate_hit_overdispersion(games, p_hit_stable, float(params.get("rho_scale", 1.0)))
    base_market_raw = market_probabilities(hand_probs, pads, hit_rho)
    recent_market_raw = market_probabilities(pre_recent, pads, hit_rho)
    quality_market_raw = market_probabilities(pre_quality, pads, hit_rho)
    pitching_market_raw = market_probabilities(pitching_mix, pads, hit_rho)
    final_market_raw = market_probabilities(final_probs, pads, hit_rho)
    p_hit_stable_signal = stable_hit_prob
    p_hit_recent_signal = sum(recent_probs[k] for k in HIT_OUTCOMES)
    cal_row = {
        "pitcher_hand": hand,
        "lineup_order": candidate.get("lineup_order"),
        "prior_pa": int(n_all),
        "recent_hit_delta": p_hit_recent_signal - p_hit_stable_signal,
    }
    base_market = _apply_calibration_matrix(base_market_raw, cal_row, params)
    recent_market = _apply_calibration_matrix(recent_market_raw, cal_row, params)
    quality_market = _apply_calibration_matrix(quality_market_raw, cal_row, params)
    pitching_market = _apply_calibration_matrix(pitching_market_raw, cal_row, params)
    final_market = _apply_calibration_matrix(final_market_raw, cal_row, params)

    deltas: dict[str, dict[str, dict[str, float]]] = {}
    for metric in final_market:
        deltas[metric] = {}
        for target in final_market[metric]:
            b=base_market[metric][target]; r=recent_market[metric][target]; q=quality_market[metric][target]
            m=pitching_market[metric][target]; f=final_market[metric][target]
            deltas[metric][target] = {
                "base": round(b, 6),
                # Preserve old UI two-delta decomposition while exposing the finer v1.5 pieces.
                "recent_delta": round(q - b, 6),
                "matchup_delta": round(f - q, 6),
                "outcome_recency_delta": round(r-b,6),
                "contact_quality_delta": round(q-r,6),
                "pitching_delta": round(m-q,6),
                "park_delta": round(f-m,6),
                "final": round(f, 6),
            }

    full_games = [g for g in games if int(g.get("pa", 0) or 0) >= 3]
    obs_multi = (sum(1 for g in full_games if int(g.get("h", 0) or 0) >= 2) / len(full_games)) if full_games else 0.0
    exp_multi_iid = (sum(binomial_tail(int(g.get("pa", 0) or 0), p_hit_stable, 2) for g in full_games) / len(full_games)) if full_games else 0.0
    exp_multi_model = (sum(beta_binomial_tail(int(g.get("pa", 0) or 0), p_hit_stable, 2, hit_rho) for g in full_games) / len(full_games)) if full_games else 0.0

    hit_prob = sum(final_probs[k] for k in HIT_OUTCOMES)
    hit_mix_den = max(stable_hit_prob, 1e-9)
    pa_mean = sum(n * p for n, p in pads.items())
    if n_all >= 300 and (hand not in {"L", "R"} or hand_n >= 70):
        conf = "HIGH"
    elif n_all >= 120 and (hand not in {"L", "R"} or hand_n >= 25):
        conf = "MEDIUM"
    else:
        conf = "LOW"

    return {
        "version": DEEP_MODEL_VERSION,
        "tuning_version": tuning.get("version"),
        "tuning_status": tuning.get("status"),
        "confidence": conf,
        "samples": {
            "player_pa": int(n_all), "hand_pa": int(hand_n), "pitcher_pa": int(pitcher_n),
            "batter_reliability": round(batter_reliability,4),
            "bullpen_pa": int(sum(bp_arr)), "park_home_pa": int(sum(ph_arr)), "park_road_pa": int(sum(pr_arr)),
            "park_reliability": round(park_reliability,4),
            "quality_effective_pa": round(qeff,1),
            "recent_effective_pa": round(recent_eff_n, 1), "full_games": len(full_games),
        },
        "pa_decay": round(pa_decay, 6),
        "expected_pa": round(pa_mean, 3),
        "pa_distribution": {str(k): round(v, 5) for k, v in pads.items()},
        "outcome_probabilities": {
            "stable": {k: round(v, 6) for k, v in stable.items()},
            "hand_adjusted": {k: round(v, 6) for k, v in hand_probs.items()},
            "recent_adjusted": {k: round(v, 6) for k, v in pre_recent.items()},
            "contact_quality_adjusted": {k: round(v, 6) for k, v in pre_quality.items()},
            "pitcher_allowed": {k: round(v, 6) for k, v in pitcher_probs.items()},
            "starter_adjusted": {k: round(v, 6) for k, v in starter_adjusted.items()},
            "bullpen_allowed": {k: round(v, 6) for k, v in bullpen_probs.items()},
            "bullpen_adjusted": {k: round(v, 6) for k, v in bullpen_adjusted.items()},
            "park_home": {k: round(v, 6) for k, v in park_home.items()},
            "park_road": {k: round(v, 6) for k, v in park_road.items()},
            "final": {k: round(v, 6) for k, v in final_probs.items()},
        },
        "probabilities": {m: {t: round(v, 6) for t, v in ts.items()} for m, ts in final_market.items()},
        "deltas": deltas,
        "profile": {
            "per_pa_hit": round(hit_prob, 6),
            "per_pa_hr": round(final_probs["hr"], 6),
            "stable_per_pa_hit": round(stable_hit_prob, 6),
            "stable_per_pa_hr": round(stable["hr"], 6),
            "contact_quality_target_hit":round(qhit,6),
            "contact_quality_delta":round(sum(pre_quality[k] for k in HIT_OUTCOMES)-sum(pre_recent[k] for k in HIT_OUTCOMES),6),
            "hit_composition": {
                "single": round(stable["1b"] / hit_mix_den, 6),
                "double": round(stable["2b"] / hit_mix_den, 6),
                "triple": round(stable["3b"] / hit_mix_den, 6),
                "home_run": round(stable["hr"] / hit_mix_den, 6),
            },
            "observed_hit_games": observed_game_shape(games, "h"),
            "observed_tb_games": observed_game_shape(games, "tb"),
            "multi_hit_observed": round(obs_multi, 6),
            "multi_hit_expected_iid": round(exp_multi_iid, 6),
            "multi_hit_expected_model": round(exp_multi_model, 6),
            "multi_hit_residual": round(obs_multi - exp_multi_model, 6),
            "hit_overdispersion_rho": round(hit_rho, 6),
            "hit_overdispersion_raw": round(hit_rho_raw, 6),
            "hit_overdispersion_games": rho_games,
        },
        "streak": streak_diagnostics(games, p_hit_stable, hit_rho),
        "notes": {
            "probabilities_are": "internally cross-fitted/calibrated model estimates; v1.7.1 clean prospective validation starts Sep 14 if this build is frozen before those games; Sep 13 and prior audit windows are development-spent",
            "contact_quality": "rolling Statcast xBA expected hits uses prior PAs only; its influence and shrinkage are blind-tuned and may be zero",
            "bullpen": "opponent relief outcome distribution is reconstructed from prior games only; a short opener (<=9 BF) followed by a bulk arm (>=12 BF) is treated as bullpen while the bulk follower is the functional starter; effect may be zero",
            "park": "home-vs-road environment distribution for the stadium franchise uses prior games only; the effect may be zero",
            "pitch_family_weather": "not used in v1.7.1: historical pitch-family storage/weather snapshots are not yet timestamp-complete enough for an honest blind comparison",
            "streak_adjustment": "serial correlation remains diagnostic only; recent-state influence itself is tunable and may be driven to zero",
            "within_game_clustering": "beta-binomial hit clustering influence is tunable via rho_scale and may be driven to zero",
            "starter_share": round(float(starter_share), 6),
            "auto_fit": "v1.7.1 fixes PA reliability OFF, retains the v1.5 local-feature/opener architecture, keeps the RHP-specific slope retired, and runs one same-fold FULL-vs-LEAN parsimony challenge before calibration",
            "historical_hand": "PA-level pitcher hand from Statcast; tonight uses probable starter hand",
        },
    }


def simple_ratio(rows: list[dict[str, Any]], event_num: str | None = None, hand: str | None = None) -> dict[str, float]:
    pa = ab = h = hr = k = tb = bb = ibb = hbp = 0
    for r in rows:
        if hand and r.get("p_throws") != hand:
            continue
        event = r.get("events") or ""
        pa += 1
        if is_ab(event):
            ab += 1
        if event in HIT_EVENTS:
            h += 1
            tb += HIT_EVENTS[event]
        if event == "home_run":
            hr += 1
        if event in STRIKEOUT_EVENTS:
            k += 1
        if event in {"walk", "intent_walk"}:
            bb += 1
        if event == "intent_walk":
            ibb += 1
        if event == "hit_by_pitch":
            hbp += 1
    return {
        "pa": pa, "ab": ab, "h": h, "hr": hr, "k": k, "tb": tb,
        "bb": bb, "ibb": ibb, "hbp": hbp,
        "avg": (h / ab) if ab else 0.0,
        "hr_pa": (hr / pa) if pa else 0.0,
        "k_pa": (k / pa) if pa else 0.0,
        "bb_pa": (bb / pa) if pa else 0.0,
        "hbp_pa": (hbp / pa) if pa else 0.0,
        "free_pa": ((bb + hbp) / pa) if pa else 0.0,
        "ab_share": (ab / pa) if pa else 0.0,
        "tb_ab": (tb / ab) if ab else 0.0,
    }


def batted_ball_metrics(rows: list[dict[str, Any]], hand: str | None = None) -> dict[str, float | int | None]:
    # Metric-specific denominators keep provisional missing fields as unknown.
    # StatsAPI can supply EV/LA before Savant supplies xBA/barrel.
    bbe = []
    for r in rows:
        if hand and r.get("p_throws") != hand:
            continue
        if r.get("launch_speed") is not None:
            bbe.append(r)
    if not bbe:
        return {"bbe": 0, "hardhit": None, "barrel": None, "xba": None, "xwoba": None, "sweetspot": None}
    hard = sum(1 for r in bbe if (r.get("launch_speed") or 0) >= 95)
    barrel_rows = [r for r in bbe if r.get("barrel") is not None]
    xbas = [float(r["estimated_ba"]) for r in bbe if r.get("estimated_ba") is not None]
    xwobas = [float(r["estimated_woba"]) for r in bbe if r.get("estimated_woba") is not None]
    angle_rows = [r for r in bbe if r.get("launch_angle") is not None]
    sweet = sum(1 for r in angle_rows if 8 <= float(r["launch_angle"]) <= 32)
    return {
        "bbe": len(bbe),
        "hardhit": hard / len(bbe),
        "barrel": (sum(1 for r in barrel_rows if r.get("barrel") == 1) / len(barrel_rows)) if barrel_rows else None,
        "xba": sum(xbas) / len(xbas) if xbas else None,
        "xwoba": sum(xwobas) / len(xwobas) if xwobas else None,
        "sweetspot": (sweet / len(angle_rows)) if angle_rows else None,
    }


def pitch_discipline(rows: list[dict[str, Any]]) -> dict[str, float | int | None]:
    swings = 0
    whiffs = 0
    for r in rows:
        d = r.get("description") or ""
        if d in SWING_DESCRIPTIONS:
            swings += 1
        if d in WHIFF_DESCRIPTIONS:
            whiffs += 1
    return {"swings": swings, "whiffs": whiffs, "whiff_rate": (whiffs / swings) if swings else None}


def smooth_rate(num: float, den: float, prior_rate: float, prior_n: float) -> float:
    return (num + prior_rate * prior_n) / (den + prior_n) if den + prior_n else prior_rate


def lineup_score(order: int | None) -> float:
    if order is None:
        return 50.0
    mapping = {1: 100, 2: 100, 3: 95, 4: 95, 5: 85, 6: 70, 7: 55, 8: 40, 9: 30}
    return float(mapping.get(order, 50))


def make_components(values: dict[str, float], weights: dict[str, float]) -> tuple[float, list[dict[str, Any]]]:
    rows = []
    total = 0.0
    for name, weight in weights.items():
        val = clamp(values.get(name, 50.0))
        points = val * weight
        total += points
        rows.append({"name": name, "subscore": round(val, 1), "weight": round(weight * 100, 1), "points": round(points, 1)})
    rows.sort(key=lambda x: x["points"], reverse=True)
    return round(total, 1), rows


def pitcher_local_metrics(pa_rows: list[dict[str, Any]], pitch_rows_: list[dict[str, Any]]) -> dict[str, Any]:
    base = simple_ratio(pa_rows)
    bbe = batted_ball_metrics(pitch_rows_)
    return {**base, **bbe}


def compute_player(
    candidate: dict[str, Any],
    pa_rows: list[dict[str, Any]],
    all_pitch_rows: list[dict[str, Any]],
    pitcher_local: dict[str, Any] | None,
    decay: float,
    *,
    deep: bool = False,
    league_priors: dict[str, dict[str, float]] | None = None,
    pitcher_pa_rows: list[dict[str, Any]] | None = None,
    deep_tuning: dict[str, Any] | None = None,
    deep_environment: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if not pa_rows:
        return None
    games = summarise_games(pa_rows)
    baseline = simple_ratio(pa_rows)
    hand = (candidate.get("pitcher") or {}).get("hand") or "?"
    if hand in {"L", "R"}:
        split = simple_ratio(pa_rows, hand=hand)
    else:
        split = {"pa": 0, "ab": 0, "h": 0, "hr": 0, "k": 0, "tb": 0, "bb": 0, "ibb": 0, "hbp": 0, "avg": 0.0, "hr_pa": 0.0, "k_pa": 0.0, "bb_pa": 0.0, "hbp_pa": 0.0, "free_pa": 0.0, "ab_share": 0.0, "tb_ab": 0.0}
    bbe = batted_ball_metrics(all_pitch_rows)
    discipline = pitch_discipline(all_pitch_rows)

    recent_hab = weighted_rate(games, "h", "ab", decay, 10)
    recent_hrpa = weighted_rate(games, "hr", "pa", decay, 10)
    recent_tbab = weighted_rate(games, "tb", "ab", decay, 10)
    recent_extra_tbab = weighted_extra_tb_ab(games, decay, 10)
    recent_bbpa = weighted_rate(games, "bb", "pa", decay, 10)
    recent_hbppa = weighted_rate(games, "hbp", "pa", decay, 10)
    hit_game = weighted_binary_hit_rate(games, decay, 10)

    split_avg = smooth_rate(split["h"], split["ab"], baseline["avg"], 40.0)
    split_hr = smooth_rate(split["hr"], split["pa"], baseline["hr_pa"], 60.0)

    p_era = safe_float((candidate.get("pitcher") or {}).get("era"))
    pl = pitcher_local or {}
    p_xba = pl.get("xba")
    p_hrpa = pl.get("hr_pa")
    p_barrel = pl.get("barrel")
    p_hardhit = pl.get("hardhit")
    p_kpa = pl.get("k_pa")

    pitcher_contact = (
        0.15 * scale(p_era, 3.0, 6.0) +
        0.40 * scale(p_xba, 0.215, 0.315) +
        0.25 * (100.0 - scale(p_kpa, 0.12, 0.32)) +
        0.20 * scale(p_hardhit, 0.30, 0.50)
    )
    pitcher_hr = (
        0.10 * scale(p_era, 3.0, 6.0) +
        0.45 * scale(p_hrpa, 0.020, 0.065) +
        0.30 * scale(p_barrel, 0.045, 0.145) +
        0.15 * scale(p_hardhit, 0.30, 0.52)
    )

    contact_vals = {
        "Recent H/AB": scale(recent_hab, 0.16, 0.42),
        "Hit-game rate": scale(hit_game, 0.40, 0.90),
        "Local baseline": scale(baseline["avg"], 0.18, 0.34),
        "Handedness split": scale(split_avg, 0.18, 0.36),
        "xBA": scale(bbe.get("xba"), 0.18, 0.34),
        "Avoids strikeouts": 100.0 - scale(baseline["k_pa"], 0.10, 0.34),
        "Hard-hit rate": scale(bbe.get("hardhit"), 0.25, 0.55),
        "Starter matchup": pitcher_contact,
        "Lineup slot": lineup_score(candidate.get("lineup_order")),
    }
    hr_vals = {
        "Recent HR/PA": scale(recent_hrpa, 0.005, 0.105),
        "Recent extra TB/AB": scale(recent_extra_tbab, 0.03, 0.55),
        "Local HR baseline": scale(baseline["hr_pa"], 0.015, 0.085),
        "Handedness HR split": scale(split_hr, 0.015, 0.095),
        "Barrel rate": scale(bbe.get("barrel"), 0.025, 0.20),
        "Hard-hit rate": scale(bbe.get("hardhit"), 0.25, 0.55),
        "Sweet-spot rate": scale(bbe.get("sweetspot"), 0.18, 0.42),
        "Starter matchup": pitcher_hr,
        "Lineup slot": lineup_score(candidate.get("lineup_order")),
    }
    contact_score, contact_components = make_components(contact_vals, CONTACT_WEIGHTS)
    hr_score, hr_components = make_components(hr_vals, HR_WEIGHTS)
    lineup_context = lineup_score(candidate.get("lineup_order"))

    recent3 = games[-3:]
    prior7 = games[-10:-3]
    r3_ab = sum(g["ab"] for g in recent3)
    r3_h = sum(g["h"] for g in recent3)
    p7_ab = sum(g["ab"] for g in prior7)
    p7_h = sum(g["h"] for g in prior7)
    r3_avg = r3_h / r3_ab if r3_ab else baseline["avg"]
    p7_avg = p7_h / p7_ab if p7_ab else baseline["avg"]
    r3_hrpa = sum(g["hr"] for g in recent3) / max(1, sum(g["pa"] for g in recent3))
    p7_hrpa = sum(g["hr"] for g in prior7) / max(1, sum(g["pa"] for g in prior7)) if prior7 else baseline["hr_pa"]
    trend_delta = (r3_avg - p7_avg) * 0.75 + (r3_hrpa - p7_hrpa) * 1.25
    trend_score = round(clamp(50 + trend_delta * 180), 1)
    if trend_delta >= 0.12:
        trend = "↑↑"
    elif trend_delta >= 0.035:
        trend = "↑"
    elif trend_delta <= -0.12:
        trend = "↓↓"
    elif trend_delta <= -0.035:
        trend = "↓"
    else:
        trend = "→"

    split_pa = split["pa"]
    if baseline["pa"] >= 150 and split_pa >= 60:
        confidence = "HIGH"
    elif baseline["pa"] >= 60 and split_pa >= 20:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    goal_scores = goal_score_table(
        games, decay, contact_score, hr_score, pitcher_contact, pitcher_hr, lineup_context
    )
    last10 = []
    for g in reversed(games[-10:]):
        last10.append({
            "date": g["date"], "opp": f"{g['home_away']} {g['opp']}",
            "start_hand": g.get("start_hand"),
            "ab": g["ab"], "h": g["h"], "tb": g["tb"], "hr": g["hr"], "xbh": g["xbh"], "k": g["k"],
            "bb": g.get("bb", 0), "ibb": g.get("ibb", 0), "hbp": g.get("hbp", 0),
        })

    deep_model = None
    if deep and league_priors:
        deep_model = deep_generative_model(
            candidate, pa_rows, pitcher_pa_rows or [], games, league_priors,
            deep_tuning or {"params": DEEP_FALLBACK_PARAMS}, deep_environment
        )

    result = {
        **candidate,
        "contact_score": contact_score,
        "hr_score": hr_score,
        "trend_score": trend_score,
        "trend": trend,
        "trend_delta": round(trend_delta, 4),
        "confidence": confidence,
        "sample": {
            "pa": baseline["pa"], "ab": baseline["ab"], "split_pa": split_pa, "bbe": bbe.get("bbe", 0),
            "bb": baseline.get("bb", 0), "ibb": baseline.get("ibb", 0), "hbp": baseline.get("hbp", 0),
            "swings": discipline.get("swings", 0), "whiffs": discipline.get("whiffs", 0),
        },
        "rates": {
            "recent_h_ab": round(recent_hab or 0, 3),
            "hit_game_rate": round(hit_game or 0, 3),
            "baseline_avg": round(baseline["avg"], 3),
            "baseline_hr_pa": round(baseline["hr_pa"], 3),
            "split_avg": round(split_avg, 3),
            "split_hr_pa": round(split_hr, 3),
            "k_pa": round(baseline["k_pa"], 3),
            "bb_pa": round(baseline.get("bb_pa", 0), 3),
            "hbp_pa": round(baseline.get("hbp_pa", 0), 3),
            "free_pa": round(baseline.get("free_pa", 0), 3),
            "ab_share": round(baseline.get("ab_share", 0), 3),
            "split_bb_pa": round(split.get("bb_pa", 0), 3),
            "split_hbp_pa": round(split.get("hbp_pa", 0), 3),
            "recent_bb_pa": round(recent_bbpa or 0, 3),
            "recent_hbp_pa": round(recent_hbppa or 0, 3),
            "hardhit": round(bbe.get("hardhit") or 0, 3),
            "barrel": round(bbe.get("barrel") or 0, 3),
            "xba": round(bbe.get("xba") or 0, 3),
            "whiff_rate": round(discipline.get("whiff_rate") or 0, 3),
            "recent_tb_ab": round(recent_tbab or 0, 3),
            "recent_extra_tb_ab": round(recent_extra_tbab or 0, 3),
            "sweetspot": round(bbe.get("sweetspot") or 0, 3),
        },
        "contact_components": contact_components,
        "hr_components": hr_components,
        "goal_scores": goal_scores,
        "goal_scoring_version": SCORING_VERSION,
        "last10": last10,
    }
    if deep_model is not None:
        result["deep_model"] = deep_model
    return result


def successful_date_rows(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT game_date FROM (
            SELECT game_date FROM fetched_dates WHERE status='ok'
            UNION ALL
            SELECT game_date FROM fetch_history WHERE status='ok'
        ) ORDER BY game_date
        """
    ).fetchall()
    return [str(r[0]) for r in rows]


def coverage_info(conn: sqlite3.Connection) -> dict[str, Any]:
    dates = successful_date_rows(conn)
    pitch_count = conn.execute("SELECT COUNT(*) FROM pitches").fetchone()[0]
    history_count = conn.execute("SELECT COUNT(*) FROM fetch_history").fetchone()[0]
    source_row = conn.execute(
        """
        SELECT MAX(game_date) AS history_through,
               SUM(CASE WHEN source='statsapi_provisional' THEN 1 ELSE 0 END) AS provisional_pitches,
               COUNT(DISTINCT CASE WHEN source='statsapi_provisional' THEN game_date END) AS provisional_days,
               MAX(CASE WHEN source='statsapi_provisional' THEN game_date END) AS provisional_last_date
        FROM pitches
        """
    ).fetchone()
    provisional_pitches = int(source_row["provisional_pitches"] or 0)
    history_through = source_row["history_through"]
    canonical_last = dates[-1] if dates else None
    return {
        "first_date": dates[0] if dates else None,
        "last_date": canonical_last,
        "canonical_last_date": canonical_last,
        "history_through": history_through,
        "days": len(dates),
        "pitches": int(pitch_count or 0),
        "fetch_snapshots": int(history_count or 0),
        "provisional_pitches": provisional_pitches,
        "provisional_days": int(source_row["provisional_days"] or 0),
        "provisional_last_date": source_row["provisional_last_date"],
        "history_status": "provisional_overlay" if provisional_pitches else "canonical",
        "canonical_append_only": True,
        "provisional_overlay": bool(provisional_pitches),
        "append_only": not bool(provisional_pitches),
    }


def score_candidates_state(
    day: str,
    candidates: list[dict[str, Any]],
    games: list[dict[str, Any]],
    decay: float,
    min_pa: int,
    warning: str | None = None,
    deep: bool = False,
) -> dict[str, Any]:
    """Recompute every derived score from immutable local Statcast rows."""
    with db_connect() as conn:
        coverage = coverage_info(conn)
        batter_ids = sorted({int(c["id"]) for c in candidates if c.get("id") is not None})
        pitcher_ids = sorted({int(c["pitcher_id"]) for c in candidates if c.get("pitcher_id")})
        pa_by_batter = terminal_pa_rows(conn, batter_ids, day, by="batter")
        pitches_by_batter = pitch_rows(conn, batter_ids, day, by="batter")
        pa_by_pitcher = terminal_pa_rows(conn, pitcher_ids, day, by="pitcher")
        pitches_by_pitcher = pitch_rows(conn, pitcher_ids, day, by="pitcher")
        league_priors = league_outcome_priors(conn, day) if deep else None
        deep_environment = deep_environment_context(conn, day) if deep else None
        deep_tuning = get_deep_tuning(conn, day) if deep else None

    pitcher_local = {
        pid: pitcher_local_metrics(pa_by_pitcher.get(pid, []), pitches_by_pitcher.get(pid, []))
        for pid in pitcher_ids
    }
    players = []
    for c in candidates:
        pid = int(c["id"])
        computed = compute_player(
            c,
            pa_by_batter.get(pid, []),
            pitches_by_batter.get(pid, []),
            pitcher_local.get(c.get("pitcher_id")),
            decay,
            deep=deep,
            league_priors=league_priors,
            pitcher_pa_rows=pa_by_pitcher.get(c.get("pitcher_id"), []),
            deep_tuning=deep_tuning,
            deep_environment=deep_environment,
        )
        if computed and computed["sample"]["pa"] >= min_pa:
            players.append(computed)

    players.sort(key=lambda p: p["contact_score"], reverse=True)
    state = {
        "generated_at": now_iso(),
        "date": day,
        "decay": decay,
        "min_pa": min_pa,
        "coverage": coverage,
        "games": games,
        "players": players,
        "counts": {
            "candidates": len(candidates),
            "ranked": len(players),
            "confirmed": sum(1 for p in players if p.get("confirmed")),
        },
        "weights": {"contact": CONTACT_WEIGHTS, "hr": HR_WEIGHTS},
        "scoring_version": SCORING_VERSION,
        "analysis_mode": "deep" if deep else "quick",
        "deep_model_version": DEEP_MODEL_VERSION if deep else None,
        "deep_tuning": deep_tuning if deep else None,
    }
    if warning:
        state["warning"] = warning
    return state


def save_state_snapshot(state: dict[str, Any]) -> None:
    try:
        raw = json.dumps(state, ensure_ascii=False).encode("utf-8")
        day_dir = STATE_CACHE_DIR / str(state.get("date"))
        day_dir.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(raw).hexdigest()[:16]
        stamp = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%f%z")
        with (day_dir / f"{stamp}-{digest}.json").open("xb") as fh:
            fh.write(raw)
    except Exception:
        pass


def build_state(day: str, decay: float = DEFAULT_DECAY, min_pa: int = DEFAULT_MIN_PA, force: bool = False, deep: bool = False) -> dict[str, Any]:
    candidates, _, games = build_daily_candidates(day, force=force)
    state = score_candidates_state(day, candidates, games, decay, min_pa, deep=deep)
    save_state_snapshot(state)
    return state


def recompute_from_cached_slate(
    day: str, stale: dict[str, Any], decay: float, min_pa: int, reason: str, deep: bool = False
) -> dict[str, Any] | None:
    # Old state snapshots are allowed to supply only slate identity/candidate metadata.
    # Their derived scores are never trusted across scoring-version changes.
    candidates = [dict(p) for p in stale.get("players", []) if p.get("id") is not None]
    if not candidates:
        return None
    state = score_candidates_state(
        day,
        candidates,
        list(stale.get("games") or []),
        decay,
        min_pa,
        warning=f"Daily MLB refresh failed; slate metadata is cached, but all batter scores were recomputed locally with {SCORING_VERSION}: {reason}",
        deep=deep,
    )
    state["stale_daily"] = True
    save_state_snapshot(state)
    return state


def cached_state_for(day: str, require_current_scoring: bool = False) -> dict[str, Any] | None:
    try:
        day_dir = STATE_CACHE_DIR / day
        if day_dir.exists():
            files = [p for p in day_dir.glob("*.json") if p.is_file()]
            if files:
                p = max(files, key=lambda x: x.stat().st_mtime)
                state = json.loads(p.read_text(encoding="utf-8"))
                if state.get("date") == day and (not require_current_scoring or state.get("scoring_version") == SCORING_VERSION):
                    return state
        # Read-only compatibility with v0.3's single mutable state cache.
        if LEGACY_STATE_CACHE_PATH.exists():
            state = json.loads(LEGACY_STATE_CACHE_PATH.read_text(encoding="utf-8"))
            if state.get("date") == day and (not require_current_scoring or state.get("scoring_version") == SCORING_VERSION):
                return state
    except Exception:
        return None
    return None


def statcast_url(day: str) -> str:
    params = {
        "all": "true",
        "type": "details",
        "hfPT": "",
        "hfAB": "",
        "hfBBT": "",
        "hfPR": "",
        "hfZ": "",
        "stadium": "",
        "hfBBL": "",
        "hfNewZones": "",
        "hfGT": "R|PO|",
        "hfSea": "",
        "hfSit": "",
        "player_type": "pitcher",
        "hfOuts": "",
        "opponent": "",
        "pitcher_throws": "",
        "batter_stands": "",
        "hfSA": "",
        "game_date_gt": day,
        "game_date_lt": day,
        "team": "",
        "position": "",
        "hfRO": "",
        "home_road": "",
        "hfFlag": "",
        "metric_1": "",
        "hfInn": "",
        "min_pitches": "0",
        "min_results": "0",
        "group_by": "name",
        "sort_col": "pitches",
        "player_event_sort": "h_launch_speed",
        "sort_order": "desc",
        "min_abs": "0",
    }
    return f"{SAVANT_BASE}?{urlencode(params)}"


def save_raw_statcast_snapshot(day: str, raw: bytes) -> str:
    d = RAW_STATCAST_DIR / day
    d.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(raw).hexdigest()
    p = d / f"{digest}.csv"
    # Content-addressed immutable archive: identical retries dedupe, changed upstream data appends.
    if not p.exists():
        with p.open("xb") as fh:
            fh.write(raw)
    return str(p.relative_to(ROOT))


def fetch_statcast_day(day: str) -> tuple[list[dict[str, Any]], str]:
    raw = http_get_bytes(statcast_url(day), timeout=120, retries=4)
    raw_snapshot = save_raw_statcast_snapshot(day, raw)
    text = raw.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for r in reader:
        game_pk = safe_int(r.get("game_pk"))
        abn = safe_int(r.get("at_bat_number"))
        pn = safe_int(r.get("pitch_number"))
        if game_pk is None or abn is None or pn is None:
            continue
        rows.append({
            "game_date": r.get("game_date") or day,
            "game_pk": game_pk,
            "at_bat_number": abn,
            "pitch_number": pn,
            "batter": safe_int(r.get("batter")),
            "pitcher": safe_int(r.get("pitcher")),
            "events": (r.get("events") or "").strip() or None,
            "description": (r.get("description") or "").strip() or None,
            "stand": (r.get("stand") or "").strip() or None,
            "p_throws": (r.get("p_throws") or "").strip() or None,
            "home_team": (r.get("home_team") or "").strip() or None,
            "away_team": (r.get("away_team") or "").strip() or None,
            "inning_topbot": (r.get("inning_topbot") or "").strip() or None,
            "launch_speed": safe_float(r.get("launch_speed")),
            "launch_angle": safe_float(r.get("launch_angle")),
            "estimated_ba": safe_float(r.get("estimated_ba_using_speedangle")),
            "estimated_woba": safe_float(r.get("estimated_woba_using_speedangle")),
            "barrel": safe_int(r.get("barrel")),
            "bb_type": (r.get("bb_type") or "").strip() or None,
            "hit_distance": safe_float(r.get("hit_distance_sc")),
            "source": "savant",
        })
    return rows, raw_snapshot


_STATSAPI_PITCH_DESCRIPTION = {
    "S": "swinging_strike",
    "W": "swinging_strike_blocked",
    "F": "foul",
    "T": "foul_tip",
    "M": "missed_bunt",
    "L": "foul_bunt",
    "X": "hit_into_play",
    "D": "hit_into_play_no_out",
    "E": "hit_into_play_score",
    "C": "called_strike",
    "B": "ball",
    "*B": "blocked_ball",
    "H": "hit_by_pitch",
    "P": "pitchout",
}


def _statsapi_slug(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    for old, new in ((" ", "_"), ("-", "_"), ("/", "_")):
        text = text.replace(old, new)
    text = "".join(ch for ch in text if ch.isalnum() or ch == "_")
    while "__" in text:
        text = text.replace("__", "_")
    return text.strip("_") or None


def _statsapi_event(result: dict[str, Any]) -> str | None:
    event = _statsapi_slug(result.get("eventType") or result.get("event"))
    aliases = {
        "intentional_walk": "intent_walk",
        "catcher_interference": "catcher_interf",
        "sacrifice_fly": "sac_fly",
        "sacrifice_bunt": "sac_bunt",
        "home_run": "home_run",
    }
    return aliases.get(event or "", event)


def _statsapi_pitch_description(play_event: dict[str, Any]) -> str | None:
    details = play_event.get("details") or {}
    code = str(details.get("code") or "").strip()
    if code in _STATSAPI_PITCH_DESCRIPTION:
        return _STATSAPI_PITCH_DESCRIPTION[code]
    desc = _statsapi_slug(details.get("description"))
    aliases = {
        "swinging_strike": "swinging_strike",
        "swinging_strike_blocked": "swinging_strike_blocked",
        "foul": "foul",
        "foul_tip": "foul_tip",
        "missed_bunt": "missed_bunt",
        "foul_bunt": "foul_bunt",
        "in_play_outs": "hit_into_play",
        "in_play_no_out": "hit_into_play_no_out",
        "in_play_runs": "hit_into_play_score",
    }
    return aliases.get(desc or "", desc)


def completed_regular_games(day: str, force: bool = False) -> list[dict[str, Any]]:
    data = fetch_json(
        mlb_url("/schedule", sportId=1, date=day, gameType="R"),
        ttl=1800,
        force=force,
    )
    games: list[dict[str, Any]] = []
    for d in data.get("dates", []):
        for game in d.get("games", []):
            status = game.get("status") or {}
            abstract = str(status.get("abstractGameState") or "").lower()
            detailed = str(status.get("detailedState") or "").lower()
            code = str(status.get("statusCode") or "").upper()
            if abstract == "final" or detailed.startswith("final") or code in {"F", "O"}:
                games.append(game)
    return games


def _statsapi_feed_rows(day: str, game_pk: int, feed: dict[str, Any]) -> list[dict[str, Any]]:
    game_data = feed.get("gameData") or {}
    live_data = feed.get("liveData") or {}
    teams = game_data.get("teams") or {}
    home_team = ((teams.get("home") or {}).get("abbreviation") or "").strip() or None
    away_team = ((teams.get("away") or {}).get("abbreviation") or "").strip() or None
    official_day = str(((game_data.get("datetime") or {}).get("officialDate") or day))[:10]
    all_plays = ((live_data.get("plays") or {}).get("allPlays") or [])
    rows: list[dict[str, Any]] = []

    for play in all_plays:
        result = play.get("result") or {}
        result_type = str(result.get("type") or "").strip()
        if result_type and result_type != "atBat":
            continue
        matchup = play.get("matchup") or {}
        batter = safe_int((matchup.get("batter") or {}).get("id"))
        pitcher = safe_int((matchup.get("pitcher") or {}).get("id"))
        if batter is None or pitcher is None:
            continue
        about = play.get("about") or {}
        raw_ab = safe_int(play.get("atBatIndex"))
        if raw_ab is None:
            raw_ab = safe_int(about.get("atBatIndex"))
        if raw_ab is None:
            continue
        # StatsAPI atBatIndex is zero-based; Savant at_bat_number is one-based.
        at_bat_number = raw_ab + 1
        event = _statsapi_event(result)
        stand = str((matchup.get("batSide") or {}).get("code") or "").strip() or None
        p_throws = str((matchup.get("pitchHand") or {}).get("code") or "").strip() or None
        is_top = about.get("isTopInning")
        if is_top is None:
            is_top = str(about.get("halfInning") or "").lower() == "top"
        inning_topbot = "Top" if is_top else "Bot"

        pitch_events = [pe for pe in (play.get("playEvents") or []) if pe.get("isPitch") is True]
        used_pitch_numbers: set[int] = set()
        if not pitch_events:
            rows.append({
                "game_date": official_day,
                "game_pk": game_pk,
                "at_bat_number": at_bat_number,
                "pitch_number": 0,
                "batter": batter,
                "pitcher": pitcher,
                "events": event,
                "description": None,
                "stand": stand,
                "p_throws": p_throws,
                "home_team": home_team,
                "away_team": away_team,
                "inning_topbot": inning_topbot,
                "launch_speed": None,
                "launch_angle": None,
                "estimated_ba": None,
                "estimated_woba": None,
                "barrel": None,
                "bb_type": None,
                "hit_distance": None,
                "source": "statsapi_provisional",
            })
            continue

        for idx, pe in enumerate(pitch_events, start=1):
            pn = safe_int(pe.get("pitchNumber")) or idx
            while pn in used_pitch_numbers:
                pn += 1000
            used_pitch_numbers.add(pn)
            hit = pe.get("hitData") or {}
            trajectory = _statsapi_slug(hit.get("trajectory"))
            rows.append({
                "game_date": official_day,
                "game_pk": game_pk,
                "at_bat_number": at_bat_number,
                "pitch_number": pn,
                "batter": batter,
                "pitcher": pitcher,
                "events": event if idx == len(pitch_events) else None,
                "description": _statsapi_pitch_description(pe),
                "stand": stand,
                "p_throws": p_throws,
                "home_team": home_team,
                "away_team": away_team,
                "inning_topbot": inning_topbot,
                "launch_speed": safe_float(hit.get("launchSpeed")),
                "launch_angle": safe_float(hit.get("launchAngle")),
                "estimated_ba": None,
                "estimated_woba": None,
                "barrel": None,
                "bb_type": trajectory,
                "hit_distance": safe_float(hit.get("totalDistance")),
                "source": "statsapi_provisional",
            })
    return rows


def fetch_statsapi_provisional_day(day: str, force: bool = True) -> tuple[list[dict[str, Any]], set[int], set[int], list[dict[str, str]]]:
    games = completed_regular_games(day, force=force)
    expected = {int(g["gamePk"]) for g in games if g.get("gamePk") is not None}
    rows: list[dict[str, Any]] = []
    fetched: set[int] = set()
    errors: list[dict[str, str]] = []
    if not expected:
        return rows, fetched, expected, errors

    def load_game(game_pk: int) -> tuple[int, list[dict[str, Any]]]:
        feed = fetch_json(mlb_live_url(f"/game/{game_pk}/feed/live"), ttl=86400, force=force)
        return game_pk, _statsapi_feed_rows(day, game_pk, feed)

    with ThreadPoolExecutor(max_workers=MLB_WORKERS) as pool:
        futs = {pool.submit(load_game, gp): gp for gp in sorted(expected)}
        for fut in as_completed(futs):
            gp = futs[fut]
            try:
                game_pk, game_rows = fut.result()
                if game_rows:
                    rows.extend(game_rows)
                    fetched.add(game_pk)
                else:
                    errors.append({"game_pk": str(game_pk), "error": "No completed plate-appearance rows in feed"})
            except Exception as exc:
                errors.append({"game_pk": str(gp), "error": str(exc)[:300]})
    return rows, fetched, expected, errors


def record_fetch_history(day: str, row_count: int, status: str, note: str | None = None, raw_snapshot: str | None = None) -> None:
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO fetch_history(game_date,row_count,fetched_at,status,note,raw_snapshot) VALUES(?,?,?,?,?,?)",
            (day, row_count, now_iso(), status, note, raw_snapshot),
        )


def _pitch_columns() -> list[str]:
    return [
        "game_date", "game_pk", "at_bat_number", "pitch_number", "batter", "pitcher",
        "events", "description", "stand", "p_throws", "home_team", "away_team",
        "inning_topbot", "launch_speed", "launch_angle", "estimated_ba", "estimated_woba",
        "barrel", "bb_type", "hit_distance", "source",
    ]


def store_statcast_day(
    day: str,
    rows: list[dict[str, Any]],
    raw_snapshot: str | None = None,
    expected_game_pks: set[int] | None = None,
) -> int:
    """Store canonical Savant rows and retire provisional rows game-by-game.

    If a provisional game exists, canonical data replaces it only when Savant has at
    least as many completed plate appearances for that game. This prevents an early
    partial Savant publication from deleting a complete StatsAPI D-1 overlay.
    """
    cols = _pitch_columns()
    sql = f"INSERT OR IGNORE INTO pitches ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})"
    by_game: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        gp = safe_int(row.get("game_pk"))
        if gp is not None:
            rr = dict(row)
            rr["source"] = "savant"
            by_game[gp].append(rr)

    accepted_games: set[int] = set()
    written_rows = 0
    with db_connect() as conn:
        for gp, game_rows in by_game.items():
            canonical_pa = sum(1 for r in game_rows if r.get("events"))
            provisional_pa = int(conn.execute(
                "SELECT COUNT(*) FROM pitches WHERE game_pk=? AND source='statsapi_provisional' AND events IS NOT NULL AND events<>''",
                [gp],
            ).fetchone()[0] or 0)
            if provisional_pa and canonical_pa < provisional_pa:
                continue
            conn.execute("DELETE FROM pitches WHERE game_pk=? AND source='statsapi_provisional'", [gp])
            before = conn.total_changes
            conn.executemany(sql, [[r.get(c) for c in cols] for r in game_rows])
            written_rows += conn.total_changes - before
            accepted_games.add(gp)

        expected = set(expected_game_pks or [])
        if not rows:
            status = "empty"
            note = "No rows returned; kept retryable in case Savant has not published the date yet."
        elif expected and not expected.issubset(accepted_games):
            status = "partial"
            missing = sorted(expected - accepted_games)
            note = f"Canonical Savant incomplete for {len(missing)} expected game(s); provisional overlay retained where available: {missing[:8]}"
        else:
            status = "ok"
            note = f"Canonical Savant accepted for {len(accepted_games)} game(s); any matching provisional overlay was retired."
        conn.execute(
            "INSERT INTO fetch_history(game_date,row_count,fetched_at,status,note,raw_snapshot) VALUES(?,?,?,?,?,?)",
            (day, len(rows), now_iso(), status, note, raw_snapshot),
        )
        conn.execute(
            "INSERT INTO fetched_dates(game_date,row_count,fetched_at,status,note) VALUES(?,?,?,?,?) "
            "ON CONFLICT(game_date) DO UPDATE SET row_count=excluded.row_count,fetched_at=excluded.fetched_at,status=excluded.status,note=excluded.note",
            (day, len(rows), now_iso(), status, note),
        )
    return int(written_rows)


def store_statsapi_provisional_day(
    day: str,
    rows: list[dict[str, Any]],
    fetched_game_pks: set[int],
    expected_game_pks: set[int],
    errors: list[dict[str, str]] | None = None,
) -> int:
    cols = _pitch_columns()
    sql = f"INSERT OR IGNORE INTO pitches ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})"
    errors = errors or []
    with db_connect() as conn:
        canonical_games = {int(r[0]) for r in conn.execute(
            "SELECT DISTINCT game_pk FROM pitches WHERE game_date=? AND source<>'statsapi_provisional'",
            [day],
        ).fetchall()}
        target_games = set(fetched_game_pks) - canonical_games
        target_rows = [r for r in rows if int(r.get("game_pk") or -1) in target_games]
        if target_games:
            marks = ",".join("?" for _ in target_games)
            conn.execute(
                f"DELETE FROM pitches WHERE source='statsapi_provisional' AND game_pk IN ({marks})",
                sorted(target_games),
            )
        before = conn.total_changes
        conn.executemany(sql, [[r.get(c) for c in cols] for r in target_rows])
        written = conn.total_changes - before
        unresolved = sorted(set(expected_game_pks) - canonical_games - set(fetched_game_pks))
        note = (
            f"StatsAPI provisional D-1 overlay: {len(target_games)} game(s), {len(target_rows)} pitch rows. "
            "Savant-only fields such as xBA/barrel remain missing until canonical replacement."
        )
        if unresolved or errors:
            note += f" Unresolved games={unresolved[:8]}; feed errors={len(errors)}."
        conn.execute(
            "INSERT INTO fetch_history(game_date,row_count,fetched_at,status,note,raw_snapshot) VALUES(?,?,?,?,?,?)",
            (day, len(target_rows), now_iso(), "provisional", note, None),
        )
    return int(written)


def existing_fetched_dates() -> set[str]:
    with db_connect() as conn:
        return set(successful_date_rows(conn))


def source_game_coverage(day: str) -> tuple[set[int], set[int]]:
    with db_connect() as conn:
        canonical = {int(r[0]) for r in conn.execute(
            "SELECT DISTINCT game_pk FROM pitches WHERE game_date=? AND source<>'statsapi_provisional'", [day]
        ).fetchall()}
        provisional = {int(r[0]) for r in conn.execute(
            "SELECT DISTINCT game_pk FROM pitches WHERE game_date=? AND source='statsapi_provisional'", [day]
        ).fetchall()}
    return canonical, provisional


def provisional_day_eligible(day: str, today: date | None = None) -> bool:
    today = today or date.today()
    try:
        age = (today - date.fromisoformat(day)).days
    except ValueError:
        return False
    return 1 <= age <= PROVISIONAL_LOOKBACK_DAYS


def dates_requiring_sync(game_dates: list[str], fetched: set[str], today: date | None = None) -> list[str]:
    """Return dates that need canonical fetch/reconciliation.

    Recent dates inside the provisional lookback are intentionally reprocessed even
    when an older build/run marked them status=ok.  This is required for two cases:
    (1) v1.7.1 databases that stamped a late/partial Savant date as ok before the
        provisional bridge existed; and
    (2) a later sync that needs to retire a provisional overlay once Savant catches up.

    Older successful dates remain skipped, preserving the append-only/cache behavior.
    """
    today = today or date.today()
    return [
        day for day in game_dates
        if day not in fetched or provisional_day_eligible(day, today)
    ]


def game_dates_between(start: date, end: date) -> list[str]:
    if start > end:
        return []
    # One schedule request for the range; avoid hitting Savant on off-days.
    url = mlb_url("/schedule", sportId=1, startDate=start.isoformat(), endDate=end.isoformat(), gameType="R")
    try:
        data = fetch_json(url, ttl=86400, force=False)
        result = [d["date"] for d in data.get("dates", []) if d.get("games")]
        if result:
            return result
    except Exception:
        pass
    # Fallback: every date. Savant returns zero rows on off-days.
    out = []
    cur = start
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def season_start(year: int) -> date:
    url = mlb_url("/schedule", sportId=1, startDate=f"{year}-03-01", endDate=f"{year}-04-15", gameType="R")
    try:
        data = fetch_json(url, ttl=86400)
        dates = [date.fromisoformat(d["date"]) for d in data.get("dates", []) if d.get("games")]
        if dates:
            return min(dates)
    except Exception:
        pass
    return date(year, 3, 20)


def backfill_worker(mode: str, days: int | None = None) -> None:
    global _backfill_status
    with _backfill_lock:
        try:
            print(f"Backfill: resolving game dates for mode={mode} ...", flush=True)
            today = date.today()
            end = today - timedelta(days=1)
            fetched = existing_fetched_dates()
            if mode == "season":
                start = season_start(today.year)
            elif mode == "sync":
                if fetched:
                    latest = max(date.fromisoformat(d) for d in fetched)
                    # Re-scan a short lookback so an earlier empty/late-published date is retried.
                    start = latest - timedelta(days=7)
                else:
                    start = end - timedelta(days=29)
            else:
                n = max(1, min(int(days or 30), 370))
                start = end - timedelta(days=n - 1)
            game_dates = game_dates_between(start, end)
            wanted = dates_requiring_sync(game_dates, fetched, today)
            _backfill_status.update({
                "running": True, "mode": mode, "done": 0, "total": len(wanted),
                "current_date": None, "message": "Preparing", "error": None,
                "started_at": now_iso(), "finished_at": None, "failed": 0, "errors": [],
            })
            recent_rechecks = sum(1 for d in wanted if d in fetched and provisional_day_eligible(d, today))
            print(
                f"Backfill: mode={mode} range={start.isoformat()}..{end.isoformat()} "
                f"work={len(wanted)} recent_rechecks={recent_rechecks}"
            )
            if not wanted:
                _backfill_status.update({"running": False, "message": "Already current", "finished_at": now_iso()})
                print("Backfill: already current")
                return

            failures: list[dict[str, str]] = []
            for idx, day in enumerate(wanted, start=1):
                recent = provisional_day_eligible(day, today)
                expected_game_pks: set[int] = set()
                if recent:
                    try:
                        expected_game_pks = {int(g["gamePk"]) for g in completed_regular_games(day, force=True)}
                    except Exception as exc:
                        print(f"Backfill [{idx}/{len(wanted)}]: schedule check failed for {day}: {exc}", flush=True)

                _backfill_status.update({"current_date": day, "message": f"Fetching canonical {day}"})
                print(f"Backfill [{idx}/{len(wanted)}]: fetching canonical Savant {day} ...", flush=True)
                savant_error: str | None = None
                savant_rows: list[dict[str, Any]] = []
                try:
                    savant_rows, raw_snapshot = fetch_statcast_day(day)
                    inserted = store_statcast_day(day, savant_rows, raw_snapshot, expected_game_pks or None)
                    print(f"Backfill [{idx}/{len(wanted)}]: Savant rows={len(savant_rows):,}, newly stored={inserted:,} for {day} · raw {raw_snapshot}", flush=True)
                except Exception as exc:
                    savant_error = str(exc)
                    record_fetch_history(day, 0, "error", savant_error[:500], None)
                    print(f"Backfill [{idx}/{len(wanted)}]: Savant ERROR {day}: {savant_error}", flush=True)

                canonical_games, provisional_games = source_game_coverage(day)
                missing_recent = expected_game_pks - canonical_games if expected_game_pks else set()
                if recent and (savant_error or missing_recent or not savant_rows):
                    _backfill_status.update({"message": f"Canonical late; filling {day} from MLB StatsAPI"})
                    print(f"Backfill [{idx}/{len(wanted)}]: canonical late/incomplete; building provisional D-1 overlay ...", flush=True)
                    try:
                        prows, fetched_games, expected_from_api, per_game_errors = fetch_statsapi_provisional_day(day, force=True)
                        if expected_from_api:
                            expected_game_pks = expected_from_api
                        pinserted = store_statsapi_provisional_day(
                            day, prows, fetched_games, expected_game_pks, per_game_errors
                        )
                        canonical_games, provisional_games = source_game_coverage(day)
                        print(
                            f"Backfill [{idx}/{len(wanted)}]: provisional rows={len(prows):,}, newly stored={pinserted:,}, "
                            f"canonical games={len(canonical_games)}, provisional games={len(provisional_games)}",
                            flush=True,
                        )
                    except Exception as exc:
                        msg = f"StatsAPI provisional fallback failed: {exc}"
                        record_fetch_history(day, 0, "provisional_error", msg[:500], None)
                        print(f"Backfill [{idx}/{len(wanted)}]: {msg}", flush=True)

                canonical_games, provisional_games = source_game_coverage(day)
                covered_games = canonical_games | provisional_games
                if expected_game_pks:
                    unresolved = expected_game_pks - covered_games
                else:
                    unresolved = set()
                if savant_error and (not recent or unresolved or not covered_games):
                    failures.append({"date": day, "error": savant_error[:500]})
                elif recent and expected_game_pks and unresolved:
                    failures.append({"date": day, "error": f"Unresolved completed games after fallback: {sorted(unresolved)}"})

                source_msg = "canonical"
                if provisional_games:
                    source_msg = f"canonical + provisional D-1 ({len(provisional_games)} game(s))"
                _backfill_status.update({
                    "done": idx,
                    "failed": len(failures),
                    "errors": failures[-10:],
                    "message": f"Processed {day} · {source_msg}",
                })
                if idx < len(wanted):
                    time.sleep(SAVANT_DELAY)

            if failures:
                _backfill_status.update({
                    "running": False,
                    "message": f"Complete with {len(failures)} failed date(s)",
                    "current_date": None,
                    "error": f"{len(failures)} date(s) failed; use SYNC DATA to retry them.",
                    "finished_at": now_iso(),
                })
                print(f"Backfill: complete with {len(failures)} failed date(s); SYNC DATA will retry them.", flush=True)
            else:
                _backfill_status.update({"running": False, "message": "Complete", "current_date": None, "finished_at": now_iso()})
                print("Backfill: complete", flush=True)
        except Exception as exc:
            _backfill_status.update({"running": False, "error": str(exc), "message": "Failed", "finished_at": now_iso()})
            print(f"Backfill: FATAL: {exc}", flush=True)

def start_backfill(mode: str, days: int | None = None) -> dict[str, Any]:
    # Mark running before the worker performs its first network request. Otherwise
    # the UI can poll during startup, see Idle, and stop polling forever.
    if _backfill_status.get("running"):
        return dict(_backfill_status)
    _backfill_status.update({
        "running": True,
        "mode": mode,
        "done": 0,
        "total": 0,
        "current_date": None,
        "message": "Starting / resolving MLB game dates",
        "error": None,
        "started_at": now_iso(),
        "finished_at": None,
        "failed": 0,
        "errors": [],
    })
    thread = threading.Thread(target=backfill_worker, args=(mode, days), daemon=True)
    thread.start()
    return dict(_backfill_status)


class Handler(SimpleHTTPRequestHandler):
    server_version = f"FailflumeScanner/{APP_VERSION}"

    def end_headers(self) -> None:
        # Allows the static HTML to diagnose/connect to localhost even if the user
        # accidentally opened index.html via file:// instead of through this server.
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("X-Content-Type-Options", "nosniff")
        super().end_headers()

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[{self.log_date_time_string()}] {fmt % args}")

    def _json(self, obj: Any, status: int = 200) -> None:
        raw = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def _read_json(self) -> dict[str, Any]:
        length = safe_int(self.headers.get("Content-Length")) or 0
        if length <= 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/state":
            qs = parse_qs(parsed.query)
            day = qs.get("date", [date.today().isoformat()])[0]
            decay = safe_float(qs.get("decay", [str(DEFAULT_DECAY)])[0]) or DEFAULT_DECAY
            decay = max(0.50, min(decay, 0.98))
            min_pa = safe_int(qs.get("min_pa", [str(DEFAULT_MIN_PA)])[0]) or DEFAULT_MIN_PA
            force = qs.get("force", ["0"])[0] == "1"
            deep = qs.get("deep", ["0"])[0] == "1"
            try:
                with _state_lock:
                    state = build_state(day, decay=decay, min_pa=min_pa, force=force, deep=deep)
                self._json(state)
            except Exception as exc:
                stale = cached_state_for(day)
                rebuilt = None
                if stale and not force:
                    try:
                        with _state_lock:
                            rebuilt = recompute_from_cached_slate(day, stale, decay, min_pa, str(exc), deep=deep)
                    except Exception:
                        rebuilt = None
                if rebuilt:
                    self._json(rebuilt)
                else:
                    self._json({
                        "error": str(exc),
                        "date": day,
                        "hint": "Daily MLB metadata could not be refreshed and no cached slate could be safely rescored. Historical Statcast data is still untouched."
                    }, status=500)
            return
        if parsed.path == "/api/health":
            try:
                with db_connect() as conn:
                    coverage = coverage_info(conn)
                self._json({
                    "ok": True,
                    "version": APP_VERSION,
                    "time": now_iso(),
                    "db": str(DB_PATH),
                    "coverage": coverage,
                })
            except Exception as exc:
                self._json({"ok": False, "error": str(exc)}, status=500)
            return
        if parsed.path == "/api/backfill_status":
            with db_connect() as conn:
                coverage = coverage_info(conn)
            self._json({"job": dict(_backfill_status), "coverage": coverage, "delay_seconds": SAVANT_DELAY})
            return
        if parsed.path == "/api/backtest_status":
            self._json({"job": dict(_backtest_status), "backtest_version": BACKTEST_VERSION})
            return
        if parsed.path == "/api/backtest_latest":
            with db_connect() as conn:
                latest = backtest_latest_run(conn)
            self._json({"run": latest, "backtest_version": BACKTEST_VERSION})
            return
        if parsed.path == "/api/audit_status":
            self._json({"job": dict(_audit_status), "audit_version": AUDIT_VERSION})
            return
        if parsed.path == "/api/audit_latest":
            with db_connect() as conn:
                latest = historical_audit_latest_run(conn)
            self._json({"run": latest, "audit_version": AUDIT_VERSION})
            return
        if parsed.path == "/api/audit_run":
            qs = parse_qs(parsed.query)
            rid = safe_int(qs.get("id", [None])[0])
            if rid is None:
                self._json({"error": "id is required"}, status=400)
                return
            with db_connect() as conn:
                row = conn.execute("SELECT * FROM historical_audit_runs WHERE id=?", [rid]).fetchone()
            if not row:
                self._json({"error": "historical audit run not found"}, status=404)
                return
            self._json({"run": historical_audit_run_payload(row)})
            return
        if parsed.path == "/api/backtest_run":
            qs = parse_qs(parsed.query)
            rid = safe_int(qs.get("id", [None])[0])
            if rid is None:
                self._json({"error": "id is required"}, status=400)
                return
            with db_connect() as conn:
                row = conn.execute("SELECT * FROM backtest_runs WHERE id=?", [rid]).fetchone()
            if not row:
                self._json({"error": "backtest run not found"}, status=404)
                return
            self._json({"run": backtest_run_payload(row)})
            return
        if parsed.path == "/api/backtest_export":
            qs = parse_qs(parsed.query)
            rid = safe_int(qs.get("id", [None])[0])
            if rid is None:
                self._json({"error": "id is required"}, status=400)
                return
            with db_connect() as conn:
                row = conn.execute("SELECT * FROM backtest_runs WHERE id=?", [rid]).fetchone()
                if not row:
                    self._json({"error": "backtest run not found"}, status=404)
                    return
                pred_rows = conn.execute("SELECT * FROM backtest_predictions WHERE run_id=? ORDER BY game_date, game_pk, lineup_order", [rid]).fetchall()
            preds = []
            for r in pred_rows:
                preds.append({
                    "game_date": r["game_date"], "game_pk": int(r["game_pk"]), "batter": int(r["batter"]),
                    "pitcher": r["pitcher"], "team": r["team"], "opponent": r["opponent"],
                    "lineup_order": r["lineup_order"], "pitcher_hand": r["pitcher_hand"],
                    "prior_pa": int(r["prior_pa"]), "hand_pa": int(r["hand_pa"] or 0), "pitcher_pa": int(r["pitcher_pa"] or 0),
                    "expected_pa": r["expected_pa"], "recent_hit_delta": r["recent_hit_delta"],
                    "recent_form_band": r["recent_form_band"], "prior_band": r["prior_band"], "rho": r["rho"],
                    "actual_pa": int(r["actual_pa"]),
                    "actual_h": int(r["actual_h"]), "actual_tb": int(r["actual_tb"]),
                    "actual_hr": int(r["actual_hr"]), "actual_xbh": int(r["actual_xbh"]),
                    "probabilities": json.loads(r["probabilities_json"]),
                })
            self._json({"run": backtest_run_payload(row), "predictions": preds})
            return
        if parsed.path == "/api/config":
            self._json({
                "default_decay": DEFAULT_DECAY,
                "default_min_pa": DEFAULT_MIN_PA,
                "savant_delay_seconds": SAVANT_DELAY,
                "mlb_min_interval_seconds": MLB_MIN_INTERVAL,
                "mlb_workers": MLB_WORKERS,
                "force_cache_floor_seconds": FORCE_CACHE_FLOOR_SECONDS,
                "provisional_lookback_days": PROVISIONAL_LOOKBACK_DAYS,
                "contact_weights": CONTACT_WEIGHTS,
                "hr_weights": HR_WEIGHTS,
                "goal_scoring_version": SCORING_VERSION,
                "deep_model_version": DEEP_MODEL_VERSION,
                "deep_tuning_version": DEEP_TUNING_VERSION,
                "backtest_version": BACKTEST_VERSION,
                "audit_version": AUDIT_VERSION,
                "prospective_start": PROSPECTIVE_START,
                "data_bridge_prospective_start": DATA_BRIDGE_PROSPECTIVE_START,
            })
            return
        if parsed.path == "/":
            self.path = "/index.html"
        return super().do_GET()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/backfill":
            body = self._read_json()
            mode = str(body.get("mode") or "days")
            days = safe_int(body.get("days"))
            if mode not in {"days", "season", "sync"}:
                self._json({"error": "mode must be days, season, or sync"}, status=400)
                return
            self._json(start_backfill(mode, days), status=202)
            return
        if parsed.path == "/api/backtest":
            body = self._read_json()
            holdout_days = safe_int(body.get("holdout_days")) or 30
            min_prior_pa = safe_int(body.get("min_prior_pa"))
            if min_prior_pa is None:
                min_prior_pa = DEFAULT_MIN_PA
            self._json(start_backtest(holdout_days, min_prior_pa), status=202)
            return
        if parsed.path == "/api/audit":
            body = self._read_json()
            windows = safe_int(body.get("windows")) or 3
            window_days = safe_int(body.get("window_days")) or 21
            min_prior_pa = safe_int(body.get("min_prior_pa"))
            if min_prior_pa is None:
                min_prior_pa = DEFAULT_MIN_PA
            self._json(start_historical_audit(windows, window_days, min_prior_pa), status=202)
            return
        self._json({"error": "Not found"}, status=404)


def open_local_browser(url: str) -> None:
    # webbrowser.open() can silently return False on Windows depending on file/protocol
    # associations. os.startfile uses the Windows shell directly and is much more reliable.
    try:
        if os.name == "nt" and hasattr(os, "startfile"):
            os.startfile(url)  # type: ignore[attr-defined]
            print(f"Browser: opened {url} via Windows shell", flush=True)
            return
    except Exception as exc:
        print(f"Browser: Windows shell open failed: {exc}", flush=True)
    try:
        ok = webbrowser.open(url, new=2)
        print(f"Browser: webbrowser.open returned {ok} for {url}", flush=True)
    except Exception as exc:
        print(f"Browser: automatic open failed: {exc}", flush=True)
        print(f"Browser: open this manually: {url}", flush=True)


def maybe_start_initial_backfill(days: int) -> None:
    if days <= 0:
        return
    try:
        with db_connect() as conn:
            cov = coverage_info(conn)
        if not cov.get("days"):
            print(f"Local Statcast database is empty; automatically starting a {days}-day backfill.", flush=True)
            start_backfill("days", days)
        else:
            print(f"Local Statcast coverage: {cov.get('first_date')} -> {cov.get('last_date')} ({cov.get('days')} dates, {cov.get('pitches'):,} pitches)", flush=True)
    except Exception as exc:
        print(f"Initial backfill check failed: {exc}", flush=True)


def run_server(host: str, port: int, open_browser: bool = False, auto_backfill_days: int = AUTO_BACKFILL_DAYS) -> None:
    os.chdir(STATIC_DIR)
    httpd = ThreadingHTTPServer((host, port), Handler)
    url = f"http://{host}:{port}"
    print(f"FAILFLUME Batter Scanner: {url}")
    print(f"Database: {DB_PATH}")
    print(f"Savant minimum request interval: {SAVANT_DELAY:.2f}s")
    print(f"MLB minimum request-start interval: {MLB_MIN_INTERVAL:.2f}s · workers={MLB_WORKERS}")
    print(f"Manual refresh cache floor: {FORCE_CACHE_FLOOR_SECONDS}s per endpoint")
    print(f"Auto-backfill on empty DB: {auto_backfill_days} days")
    print(f"D-1 provisional StatsAPI window: {PROVISIONAL_LOOKBACK_DAYS} day(s)")
    # At this point the socket is bound. Start background work and browser only now.
    maybe_start_initial_backfill(auto_backfill_days)
    if open_browser:
        threading.Timer(0.35, open_local_browser, args=(url,)).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


def main() -> None:
    parser = argparse.ArgumentParser(description="FAILFLUME Batter Scanner")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--backfill-days", type=int)
    parser.add_argument("--backfill-season", action="store_true")
    parser.add_argument("--sync", action="store_true")
    parser.add_argument("--open", action="store_true", help="Open the browser after the local server binds")
    parser.add_argument("--auto-backfill-days", type=int, default=AUTO_BACKFILL_DAYS, help="Automatically backfill this many days if the local DB is empty; 0 disables")
    parser.add_argument("--backtest", action="store_true", help="Run/reuse the locked game-level historical backtest and print JSON")
    parser.add_argument("--holdout-days", type=int, default=30, help="Number of final historical game dates reserved for --backtest")
    parser.add_argument("--backtest-min-pa", type=int, default=DEFAULT_MIN_PA, help="Minimum prior PA for historical backtest candidates")
    parser.add_argument("--audit", action="store_true", help="Run/reuse the diagnostic pseudo-prospective historical replication carousel and print JSON")
    parser.add_argument("--audit-windows", type=int, default=3, help="Number of non-overlapping historical audit windows")
    parser.add_argument("--audit-window-days", type=int, default=21, help="Game dates per historical audit window")
    args = parser.parse_args()
    init_db()
    if args.backtest:
        result = run_locked_backtest(args.holdout_days, args.backtest_min_pa)
        print(json.dumps(result, indent=2))
        return
    if args.audit:
        result = run_historical_audit(args.audit_windows, args.audit_window_days, args.backtest_min_pa)
        print(json.dumps(result, indent=2))
        return
    if args.backfill_season or args.backfill_days or args.sync:
        mode = "season" if args.backfill_season else ("sync" if args.sync else "days")
        backfill_worker(mode, args.backfill_days)
        print(json.dumps(_backfill_status, indent=2))
        return
    run_server(args.host, args.port, open_browser=args.open, auto_backfill_days=max(0, min(args.auto_backfill_days, 370)))


if __name__ == "__main__":
    main()
