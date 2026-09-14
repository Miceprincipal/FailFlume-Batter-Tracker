import importlib.util
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("failflume_app", ROOT / "app.py")
app = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(app)


def test_recent_ok_date_is_reconciled():
    today = date(2026, 9, 14)
    days = ["2026-09-12", "2026-09-13"]
    fetched = set(days)
    assert app.dates_requiring_sync(days, fetched, today) == ["2026-09-12", "2026-09-13"]


def test_old_ok_date_stays_skipped():
    today = date(2026, 9, 14)
    days = ["2026-09-01", "2026-09-13"]
    fetched = set(days)
    assert app.dates_requiring_sync(days, fetched, today) == ["2026-09-13"]


def test_missing_old_date_is_still_fetched():
    today = date(2026, 9, 14)
    days = ["2026-09-01", "2026-09-13"]
    fetched = {"2026-09-13"}
    assert app.dates_requiring_sync(days, fetched, today) == ["2026-09-01", "2026-09-13"]


def test_outside_lookback_boundary_is_not_rechecked():
    today = date(2026, 9, 14)
    # Default lookback is 2 days, so Sep 11 is too old while Sep 12/13 recheck.
    days = ["2026-09-11", "2026-09-12", "2026-09-13"]
    fetched = set(days)
    assert app.dates_requiring_sync(days, fetched, today) == ["2026-09-12", "2026-09-13"]
