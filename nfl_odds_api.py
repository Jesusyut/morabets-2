import os, sys, requests, json
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple

ODDS_API_KEY = os.getenv("ODDS_API_KEY")
BASE = "https://api.the-odds-api.com/v4"

def _debug_log(tag: str, url: str, params: dict):
    try:
        print(f"[NFL][{tag}] GET {url} params={json.dumps(params, sort_keys=True)}")
    except Exception:
        pass

def list_event_markets(sport_key: str, event_id: str) -> list[str]:
    url = f"{BASE}/sports/{sport_key}/events/{event_id}/markets"
    params = {"regions": "us"}
    _debug_log("markets-list", url, params)
    data, _ = _get(url, params)
    markets = data if isinstance(data, list) else data.get("markets", [])
    return [m.get("key") for m in markets if isinstance(m, dict) and m.get("key")]

PRIMARY_BOOKS = ["draftkings","fanduel"]  # preseason focus
ALL_BOOKS = ["draftkings","fanduel","betmgm","caesars","pointsbetus"]

DEFAULT_MARKETS = [
    "player_pass_yds",
    "player_pass_tds",
    "player_rush_yds",
    "player_rush_tds",
    "player_receptions",
    "player_reception_yds",
    "player_reception_tds",
    # optional, but supported:
    # "player_pass_interceptions",
    # "player_rush_attempts",
    # "player_reception_longest",
    # "player_pass_longest_completion",
    # "player_kicking_points",
]

# Extended markets for regular season
EXTENDED_MARKETS = [
    "player_pass_yds",
    "player_pass_tds",
    "player_rush_yds",
    "player_rush_tds",
    "player_receptions",
    "player_reception_yds",
    "player_reception_tds",
    # optional, but supported:
    # "player_pass_interceptions",
    # "player_rush_attempts",
    # "player_reception_longest",
    # "player_pass_longest_completion",
    # "player_kicking_points",
]

def _get(url: str, params: Dict[str, Any], timeout: int = 20) -> Tuple[Any, Dict[str,str]]:
    if not ODDS_API_KEY:
        raise RuntimeError("ODDS_API_KEY is not set")
    q = {**params, "apiKey": ODDS_API_KEY}
    r = requests.get(url, params=q, timeout=timeout)
    hdrs = {k.lower(): v for k,v in r.headers.items()}
    if r.status_code != 200:
        try:
            detail = r.json()
        except Exception:
            detail = r.text
        raise RuntimeError(f"Odds API error {r.status_code} at {url}: {detail}")
    try:
        return r.json(), hdrs
    except Exception as e:
        raise RuntimeError(f"Invalid JSON at {url}: {e}")

def _log_headers(tag: str, hdrs: Dict[str,str]):
    rem = hdrs.get("x-requests-remaining")
    used = hdrs.get("x-requests-used")
    lim  = hdrs.get("x-requests-limit")
    if rem or used or lim:
        print(f"[NFL][{tag}] usage remaining={rem} used={used} limit={lim}", file=sys.stderr)

def _detect_nfl_sport_key(hours_ahead: int = 168) -> str:
    now = datetime.now(timezone.utc)
    end = now + timedelta(hours=hours_ahead)
    window = {
        "commenceTimeFrom": now.replace(microsecond=0).isoformat().replace('+00:00', 'Z'),
        "commenceTimeTo": end.replace(microsecond=0).isoformat().replace('+00:00', 'Z'),
        "regions": "us",
        "oddsFormat": "american",
    }
    preseason = "americanfootball_nfl_preseason"
    regular = "americanfootball_nfl"
    try:
        ev, hdrs = _get(f"{BASE}/sports/{preseason}/events", window)
        _log_headers("detect-pre", hdrs)
        if ev:
            return preseason
    except Exception:
        pass
    return regular

def _list_events(sport_key: str, hours_ahead: int = 168) -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    end = now + timedelta(hours=hours_ahead)
    ev, hdrs = _get(
        f"{BASE}/sports/{sport_key}/events",
        {
            "commenceTimeFrom": now.replace(microsecond=0).isoformat().replace('+00:00', 'Z'),
            "commenceTimeTo": end.replace(microsecond=0).isoformat().replace('+00:00', 'Z'),
            "regions": "us",
            "oddsFormat": "american",
        },
    )
    _log_headers(f"events-{sport_key}", hdrs)
    return ev

def _event_odds(sport_key: str, event_id: str, markets: List[str], books: Optional[List[str]] = None) -> Dict[str, Any]:
    # mirror MLB: no bookmaker filter by default, CSV for markets
    url = f"{BASE}/sports/{sport_key}/events/{event_id}/odds"
    params = {
        "regions": "us",
        "oddsFormat": "american",
        "markets": ",".join(markets),
    }
    if books:
        params["bookmakers"] = ",".join(books)
    _debug_log("event-odds", url, params)
    data, hdrs = _get(url, params)
    _log_headers(f"event-{event_id}", hdrs)
    payloads = data if isinstance(data, list) else [data]
    for p in payloads:
        if isinstance(p, dict) and p.get("bookmakers"):
            return p
    return payloads[0] if payloads else {}

def _build_event_shell(ev: Dict[str, Any], bookmakers: List[Dict[str,Any]]) -> Dict[str, Any]:
    return {
        "id": ev.get("id"),
        "commence_time": ev.get("commence_time"),
        "home_team": ev.get("home_team"),
        "away_team": ev.get("away_team"),
        "teams": ev.get("teams", []),
        "bookmakers": bookmakers or [],
    }

def fetch_nfl_props(
    markets: Optional[List[str]] = None,
    hours_ahead: int = 168,
) -> List[Dict[str, Any]]:
    mkts = markets or DEFAULT_MARKETS
    sport_key = _detect_nfl_sport_key(hours_ahead)
    events = _list_events(sport_key, hours_ahead)
    out: List[Dict[str, Any]] = []

    for ev in events:
        ev_id = ev.get("id")
        if not ev_id:
            continue
        try:
            p = _event_odds(sport_key, ev_id, mkts, None)  # mirror MLB: no bookmaker filter
        except RuntimeError as e:
            print(f"[NFL] Event {ev_id} failed: {e}")
            continue
        
        # keep ONLY events that actually have markets
        if p.get("bookmakers"):
            out.append({
                "id": ev_id,
                "commence_time": ev.get("commence_time"),
                "home_team": ev.get("home_team"),
                "away_team": ev.get("away_team"),
                "teams": ev.get("teams", []),
                "bookmakers": p.get("bookmakers", []),
            })
    return out

# ----- Environment (favored team + totals) -----
def _bulk_odds(sport_key: str, markets: List[str], hours_ahead: int) -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    end = now + timedelta(hours=hours_ahead)
    data, hdrs = _get(
        f"{BASE}/sports/{sport_key}/odds",
        {
            "regions": "us",
            "oddsFormat": "american",
            "dateFormat": "iso",
            "markets": ",".join(markets),
            "commenceTimeFrom": now.replace(microsecond=0).isoformat().replace('+00:00', 'Z'),
            "commenceTimeTo": end.replace(microsecond=0).isoformat().replace('+00:00', 'Z'),
        },
    )
    _log_headers(f"bulk-{sport_key}", hdrs)
    return data

def _classify_environment(total_point: float, over_odds: int, under_odds: int) -> str:
    try:
        t = float(total_point)
    except Exception:
        return "Neutral"
    if t >= 47.5 and isinstance(over_odds, (int,float)) and isinstance(under_odds, (int,float)) and over_odds <= under_odds:
        return "High"
    if t <= 41.5 and isinstance(over_odds, (int,float)) and isinstance(under_odds, (int,float)) and under_odds <= over_odds:
        return "Low"
    return "Neutral"

def get_nfl_game_environment_map(hours_ahead: int = 96) -> Dict[str, Dict[str, Any]]:
    from team_abbreviations import TEAM_ABBREVIATIONS
    sport_key = _detect_nfl_sport_key(hours_ahead)
    data = _bulk_odds(sport_key, ["h2h","totals"], hours_ahead)
    env_map: Dict[str, Dict[str, Any]] = {}
    for event in data:
        home = event.get("home_team",""); away = event.get("away_team","")
        if not home or not away: continue
        H = TEAM_ABBREVIATIONS.get(home, home); A = TEAM_ABBREVIATIONS.get(away, away)
        matchup_key = f"{A} @ {H}"

        total_point = over_odds = under_odds = home_ml = away_ml = None
        for bm in event.get("bookmakers", []):
            for market in bm.get("markets", []):
                if market.get("key") == "totals":
                    for o in market.get("outcomes", []):
                        if o.get("name") == "Over": total_point, over_odds = o.get("point"), o.get("price")
                        elif o.get("name") == "Under": under_odds = o.get("price")
                elif market.get("key") == "h2h":
                    for o in market.get("outcomes", []):
                        if o.get("name") == home: home_ml = o.get("price")
                        elif o.get("name") == away: away_ml = o.get("price")

        favored = None
        if home_ml is not None and away_ml is not None:
            favored = H if home_ml < away_ml else A

        env_map[matchup_key] = {
            "environment": _classify_environment(total_point, over_odds, under_odds) if total_point is not None else "Neutral",
            "total": total_point,
            "over_odds": over_odds,
            "under_odds": under_odds,
            "favored_team": favored,
            "home_team": H,
            "away_team": A,
        }
    return env_map

if __name__ == "__main__":
    try:
        sk = _detect_nfl_sport_key()
        print("sport_key:", sk)
        props = fetch_nfl_props(hours_ahead=96)
        print("events_with_props:", len(props))
    except Exception as e:
        print("Error:", e)