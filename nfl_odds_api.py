# nfl_odds_api.py
from __future__ import annotations
import os, time
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Any, Dict, List, Tuple, Optional

import requests

# ---- Config / constants ----
BASE = "https://api.the-odds-api.com"
SPORT_KEY = "americanfootball_nfl"  # NFL
API_KEY = os.getenv("ODDS_API_KEY") or os.getenv("THE_ODDS_API_KEY") or ""
REGIONS = os.getenv("ODDS_REGIONS", "us")
ODDS_FORMAT = "american"
PREFERRED_BOOKMAKER_KEYS = [b for b in os.getenv("ODDS_PREFERRED_BOOKS","").lower().split(",") if b]

session = requests.Session()
session.headers.update({"User-Agent": "MoraBets/1.0 (+NFL props v4)"})

# ---- NFL player prop markets (from Odds API docs: NFL/NCAAF/CFL Player Props) ----
NFL_PLAYER_PROP_MARKETS: List[str] = [
    "player_pass_yds", "player_pass_tds", "player_pass_attempts", "player_pass_completions",
    "player_pass_interceptions", "player_rush_yds", "player_rush_attempts", "player_rush_tds",
    "player_receptions", "player_reception_yds", "player_reception_tds",
    "player_reception_longest", "player_pass_longest_completion",
    "player_anytime_td", "player_1st_td", "player_last_td",
    "player_field_goals", "player_kicking_points",
    "player_sacks", "player_solo_tackles", "player_tackles_assists",
    "player_pass_rush_reception_yds", "player_pass_rush_reception_tds",
]
# (Add alternates later if you want *_alternate keys)

# ---- No-vig helpers (use your local module if available) ----
try:
    from novig import american_to_prob, novig_two_way
except Exception:
    # Fallbacks
    def american_to_prob(odds: int | float) -> float:
        o = float(odds)
        return 100.0/(o+100.0) if o > 0 else (-o)/(100.0 - o)
    def novig_two_way(oddsa: int, oddsb: int) -> Tuple[float,float]:
        pa, pb = american_to_prob(oddsa), american_to_prob(oddsb)
        z = (pa + pb) or 1.0
        return pa/z, pb/z

def prob_to_american(p: float) -> int:
    if p <= 0 or p >= 1: return 0
    return int(round(-100*p/(1-p))) if p >= 0.5 else int(round(100*(1-p)/p))

# ---- HTTP helpers ----
def _get_json(path: str, **params) -> Dict[str, Any]:
    assert API_KEY, "ODDS_API_KEY missing"
    url = f"{BASE}/v4{path}"
    params["apiKey"] = API_KEY
    r = session.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json() or {}

def list_nfl_events(hours_ahead: int = 48) -> List[Dict[str, Any]]:
    now = datetime.utcnow().replace(microsecond=0)
    end = now + timedelta(hours=hours_ahead)
    return _get_json(
        f"/sports/{SPORT_KEY}/events",
        commenceTimeFrom=now.isoformat() + "Z",
        commenceTimeTo=end.isoformat() + "Z",
    )

def nfl_event_odds(event_id: str, markets: List[str]) -> Dict[str, Any]:
    base_params = {"regions": REGIONS, "oddsFormat": ODDS_FORMAT, "markets": ",".join(markets)}
    params = dict(base_params)
    if PREFERRED_BOOKMAKER_KEYS:
        params["bookmakers"] = ",".join(PREFERRED_BOOKMAKER_KEYS)
    data = _get_json(f"/sports/{SPORT_KEY}/events/{event_id}/odds", **params)
    # fallback without bookmaker filter if empty
    if not (data.get("bookmakers") or []):
        data = _get_json(f"/sports/{SPORT_KEY}/events/{event_id}/odds", **base_params)
    return data

# ---- Pair Over/Under (or Yes/No) for a given market ----
def _pair_outcomes(bookmakers: List[Dict[str,Any]], stat_key: str) -> dict:
    """
    Returns dict: {(player, stat_key, point): {"over": {...}, "under": {...}}}
    """
    pairs = defaultdict(lambda: {"over": None, "under": None})
    for b in bookmakers or []:
        bkey = b.get("key","")
        for m in b.get("markets", []):
            if m.get("key") != stat_key:
                continue
            for out in m.get("outcomes", []):
                player = out.get("description") or out.get("name") or ""
                side   = (out.get("name") or "").lower()  # "Over"/"Under" or "Yes"/"No"
                point  = out.get("point")
                price  = out.get("price")
                if not player or price is None:
                    continue
                # Normalize Yes/No -> Over/Under for binary props (e.g., anytime TD)
                if side not in ("over","under"):
                    side = "over" if side in ("yes", "anytime_td") else ("under" if side=="no" else side)
                k = (player, stat_key, point)
                tick = {"book": bkey, "price": int(price), "point": point}
                if side == "over" and not pairs[k]["over"]:
                    pairs[k]["over"] = tick
                elif side == "under" and not pairs[k]["under"]:
                    pairs[k]["under"] = tick
    return pairs

def _attach_fair(row: Dict[str,Any], over: Dict[str,Any] | None, under: Dict[str,Any] | None):
    fair = {"prob": {}, "american": {}}
    if over and under:
        p_over, p_under = novig_two_way(over["price"], under["price"])
        fair["prob"]["over"], fair["prob"]["under"] = p_over, p_under
        fair["american"]["over"] = prob_to_american(p_over)
        fair["american"]["under"] = prob_to_american(p_under)
        row["book"] = over["book"]
    else:
        side = "over" if over else "under"
        tick = over or under
        p = american_to_prob(tick["price"])
        fair["prob"][side] = p
        fair["american"][side] = tick["price"]
        row["book"] = tick["book"]
    row["fair"] = fair

# ---- Public: fetch NFL player props (MLB-shaped rows) ----
def fetch_nfl_player_props(hours_ahead: int = 48) -> List[Dict[str, Any]]:
    events = list_nfl_events(hours_ahead=hours_ahead)
    all_props: List[Dict[str,Any]] = []

    # split markets into two small batches to be kind to rate limits
    batches = [NFL_PLAYER_PROP_MARKETS[:8], NFL_PLAYER_PROP_MARKETS[8:]]
    for ev in events:
        eid = ev.get("id")
        if not eid: 
            continue
        home, away = ev.get("home_team","Home"), ev.get("away_team","Away")
        matchup = f"{away} @ {home}"
        sidebook = {}

        for i, mk in enumerate(batches):
            try:
                if i: time.sleep(1)  # tiny spacing between calls
                data = nfl_event_odds(eid, mk)
                for stat_key in mk:
                    sb = _pair_outcomes(data.get("bookmakers", []), stat_key)
                    sidebook.update(sb)
            except Exception as e:
                print(f"[NFL] warn: event {eid} markets {mk} failed: {e}")

        # shape rows like MLB
        for (player, stat_key, point), sides in sidebook.items():
            over, under = sides.get("over"), sides.get("under")
            row = {
                "league": "nfl",
                "matchup": matchup,
                "player": player,
                "stat": stat_key,
                "line": point,
                "shop": {},
            }
            if over:  row["shop"]["over"]  = {"american": over["price"],  "book": over["book"]}
            if under: row["shop"]["under"] = {"american": under["price"], "book": under["book"]}
            row["side"] = "both" if (over and under) else ("over" if over else ("under" if under else "unknown"))
            _attach_fair(row, over, under)
            all_props.append(row)

    # sort strongest first (by fair prob over, like MLB)
    def keyfn(p):
        return ((p.get("fair") or {}).get("prob") or {}).get("over") or 0.0
    all_props.sort(key=keyfn, reverse=True)
    return all_props

# ---- Back-compat alias for legacy import paths ----
def fetch_nfl_props(*args, **kwargs):
    """
    Backwards-compatible wrapper. Older app.py imports `fetch_nfl_props`,
    new code uses `fetch_nfl_player_props`. Keep both.
    """
    return fetch_nfl_player_props(*args, **kwargs)

# Optional explicit export list (harmless if not used)
try:
    __all__ = list(set((__all__ if '__all__' in globals() else []) + [
        'fetch_nfl_player_props', 'fetch_nfl_props'
    ]))
except Exception:
    pass