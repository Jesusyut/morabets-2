# odds_client_ufc.py
from __future__ import annotations
import os
from datetime import datetime, timedelta, timezone as tz
from typing import Any, Dict, List, Optional
import requests
from cache_ttl import get as cache_get, setex as cache_setex
from markets_ufc import UFC_SPORT_KEY

BASE = "https://api.the-odds-api.com"
API_KEY = os.getenv("ODDS_API_KEY") or os.getenv("THE_ODDS_API_KEY") or ""
REGIONS = os.getenv("ODDS_REGIONS", "us")
ODDS_FORMAT = "american"
PREFERRED_BOOKMAKER_KEYS = [b for b in os.getenv("ODDS_PREFERRED_BOOKS","").lower().split(",") if b]

CACHE_SEC_EVENTS = int(os.getenv("UFC_EVENTS_CACHE_SEC", "60"))
CACHE_SEC_EVENT_ODDS = int(os.getenv("UFC_EVENT_ODDS_CACHE_SEC", "60"))
CACHE_SEC_EVENT_MARKETS = int(os.getenv("UFC_EVENT_MARKETS_CACHE_SEC", "300"))

_sess = requests.Session()
_sess.headers.update({"User-Agent": "MoraBets/1.0 (+UFC v4)"})

def _get_json(path: str, **params) -> Dict[str, Any]:
    assert API_KEY, "ODDS_API_KEY missing"
    url = f"{BASE}/v4{path}"
    params["apiKey"] = API_KEY
    r = _sess.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json() or {}

def list_events_ufc(hours_ahead: int = 72, date: Optional[str] = None) -> List[Dict[str, Any]]:
    if date:
        start = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=tz.utc, hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        key = f"ufc:events:date:{date}"
    else:
        start = datetime.utcnow().replace(tzinfo=tz.utc, microsecond=0)
        end = start + timedelta(hours=hours_ahead)
        key = f"ufc:events:ha:{hours_ahead}"
    hit = cache_get(key)
    if hit is not None: return hit
    data = _get_json(
        f"/sports/{UFC_SPORT_KEY}/events",
        commenceTimeFrom=start.isoformat().replace("+00:00","Z"),
        commenceTimeTo=end.isoformat().replace("+00:00","Z"),
    )
    cache_setex(key, CACHE_SEC_EVENTS, data)
    return data

def event_markets_ufc(event_id: str) -> Dict[str, Any]:
    key = f"ufc:event:{event_id}:markets"
    hit = cache_get(key)
    if hit is not None: return hit
    data = _get_json(f"/sports/{UFC_SPORT_KEY}/events/{event_id}/markets", regions=REGIONS)
    cache_setex(key, CACHE_SEC_EVENT_MARKETS, data)
    return data

def event_odds_ufc(event_id: str, markets: List[str]) -> Dict[str, Any]:
    mk = ",".join(markets)
    key = f"ufc:event:{event_id}:mk:{mk}"
    hit = cache_get(key)
    if hit is not None: return hit
    base_params = {"regions": REGIONS, "oddsFormat": ODDS_FORMAT, "markets": mk}
    params = dict(base_params)
    if PREFERRED_BOOKMAKER_KEYS:
        params["bookmakers"] = ",".join(PREFERRED_BOOKMAKER_KEYS)
    data = _get_json(f"/sports/{UFC_SPORT_KEY}/events/{event_id}/odds", **params)
    if not (data.get("bookmakers") or []):
        data = _get_json(f"/sports/{UFC_SPORT_KEY}/events/{event_id}/odds", **base_params)
    cache_setex(key, CACHE_SEC_EVENT_ODDS, data)
    return data
