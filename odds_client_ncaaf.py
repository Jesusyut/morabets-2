# odds_client_ncaaf.py
from __future__ import annotations
import os, time
from datetime import datetime, timedelta, timezone as tz
from typing import Any, Dict, List, Optional
import requests

BASE = "https://api.the-odds-api.com"
API_KEY = os.getenv("ODDS_API_KEY") or os.getenv("THE_ODDS_API_KEY") or ""
REGIONS = os.getenv("ODDS_REGIONS", "us")
ODDS_FORMAT = "american"
PREFERRED_BOOKMAKER_KEYS = [b for b in (os.getenv("ODDS_PREFERRED_BOOKS","").lower().split(",")) if b]

session = requests.Session()
session.headers.update({"User-Agent":"MoraBets/1.0"})

def _get_json(path: str, **params) -> Dict[str, Any]:
    assert API_KEY, "ODDS_API_KEY missing"
    url = f"{BASE}/v4{path}"
    params["apiKey"] = API_KEY
    r = session.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json() or {}

def list_events_ncaaf(hours_ahead: int = 48, date: Optional[str] = None) -> List[Dict[str, Any]]:
    from markets_ncaaf import NCAAF_SPORT_KEY
    if date:
        # expect YYYY-MM-DD; use UTC day window
        start = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=tz.utc, hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
    else:
        start = datetime.utcnow().replace(tzinfo=tz.utc, microsecond=0)
        end = start + timedelta(hours=hours_ahead)
    return _get_json(
        f"/sports/{NCAAF_SPORT_KEY}/events",
        commenceTimeFrom=start.isoformat().replace("+00:00","Z"),
        commenceTimeTo=end.isoformat().replace("+00:00","Z"),
    )

def event_odds_ncaaf(event_id: str, markets: List[str]) -> Dict[str, Any]:
    """Fetch odds for a single event and comma-joined markets (player props)."""
    from markets_ncaaf import NCAAF_SPORT_KEY
    base_params = {
        "regions": REGIONS, "oddsFormat": ODDS_FORMAT,
        "markets": ",".join(markets),
    }
    # preferred books first (if set), then fallback to any
    params = dict(base_params)
    if PREFERRED_BOOKMAKER_KEYS:
        params["bookmakers"] = ",".join(PREFERRED_BOOKMAKER_KEYS)
    data = _get_json(f"/sports/{NCAAF_SPORT_KEY}/events/{event_id}/odds", **params)
    if not (data.get("bookmakers") or []):
        data = _get_json(f"/sports/{NCAAF_SPORT_KEY}/events/{event_id}/odds", **base_params)
    return data
