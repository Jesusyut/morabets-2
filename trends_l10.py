# trends_l10.py
import re
import time
import logging
from functools import lru_cache
from urllib.parse import quote
import requests
from datetime import datetime

log = logging.getLogger("trends_l10")

MLB_BASE = "https://statsapi.mlb.com/api/v1"
DEFAULT_TIMEOUT = 10

_ROMAN_SUFFIXES = {" II", " III", " IV", " V"}
_JR_SUFFIXES = {" JR", " JR.", " SR", " SR."}

def _normalize_name(name: str) -> str:
    """
    Make the search more tolerant:
    - collapse whitespace
    - drop periods
    - keep diacritics (MLB API handles them)
    - optionally strip common suffixes for a broader match
    """
    n = re.sub(r"\s+", " ", name or "").strip()
    n = n.replace(".", "")
    upper = " " + n.upper()
    # If there is a suffix, try search *with* and *without* it.
    return n, upper

@lru_cache(maxsize=2048)
def _lookup_person_candidates(name: str):
    # Primary: people/search?names=
    url = f"{MLB_BASE}/people/search?names={quote(name)}"
    r = requests.get(url, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    data = r.json() or {}
    return data.get("people", [])

def _pick_person_id(name: str):
    """
    Try the exact name, then try without suffixes if needed.
    Choose active MLB player when multiple results.
    """
    exact, upper = _normalize_name(name)

    def choose(cands):
        if not cands:
            return None
        # Prefer players with currentTeam (MLB) and primaryPosition
        def key(p):
            active = int(bool(p.get("currentTeam")))
            level = p.get("currentTeam", {}).get("link", "")
            # crude MLB vs MiLB signal; presence of team id is good enough
            pos = int(bool(p.get("primaryPosition")))
            return (active, pos, -p.get("id", 0))
        cands_sorted = sorted(cands, key=key, reverse=True)
        return cands_sorted[0].get("id")

    # 1) exact
    cands = _lookup_person_candidates(exact)
    pid = choose(cands)
    if pid:
        return pid

    # 2) try stripping common suffixes
    stripped = upper
    for suf in sorted(_ROMAN_SUFFIXES | _JR_SUFFIXES, key=len, reverse=True):
        if stripped.endswith(suf):
            stripped = stripped[: -len(suf)]
            break
    stripped = stripped.strip()
    if stripped and stripped != upper.strip():
        cands2 = _lookup_person_candidates(stripped.title())
        pid = choose(cands2)
        if pid:
            return pid

    # 3) last-ditch: first word + last word
    parts = exact.split(" ")
    if len(parts) >= 2:
        cands3 = _lookup_person_candidates(f"{parts[0]} {parts[-1]}")
        pid = choose(cands3)
        if pid:
            return pid

    return None

@lru_cache(maxsize=4096)
def _fetch_game_logs(person_id: int, group: str, season: int):
    """
    Returns gameLog splits list for season.
    """
    url = f"{MLB_BASE}/people/{person_id}/stats"
    params = {"stats": "gameLog", "group": group, "season": str(season)}
    r = requests.get(url, params=params, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    js = r.json() or {}
    stats = (js.get("stats") or [])
    if not stats:
        return []
    return (stats[0] or {}).get("splits", []) or []

def _parse_date(d):
    try:
        return datetime.strptime(d, "%Y-%m-%d")
    except Exception:
        return datetime.min

def get_last_10_trend(player_name: str, stat_key: str = "hits", season: int = None):
    """
    Standalone L10 trend builder for hitters.
    - Resolves player id
    - Pulls gameLog for season, backfills prev season to reach 10
    - Returns list[dict] sorted newest->oldest with per-game stat_key
    """
    if season is None:
        season = datetime.now().year

    pid = _pick_person_id(player_name)
    if not pid:
        log.warning("[L10] resolve failed (no id) %s", player_name)
        return {"player": player_name, "games": [], "count": 0}

    # group is 'hitting' for batters
    logs = list(_fetch_game_logs(pid, "hitting", season))
    if len(logs) < 10:
        prev = _fetch_game_logs(pid, "hitting", season - 1)
        logs.extend(prev)

    # Normalize newest first (StatsAPI is usually newest first, but enforce by date)
    logs.sort(key=lambda s: _parse_date(s.get("date", "")), reverse=True)
    recent = logs[:10]

    # pull the stat; common keys in StatAPI for batters:
    # hits, totalBases, homeRuns, runs, rbi, baseOnBalls, stolenBases, strikeOuts
    stat_field_map = {
        "hits": "hits",
        "total_bases": "totalBases",
        "tb": "totalBases",
        "home_runs": "homeRuns",
        "runs": "runs",
        "rbi": "rbi",
        "walks": "baseOnBalls",
        "stolen_bases": "stolenBases",
        "strikeouts": "strikeOuts",
    }
    fld = stat_field_map.get(stat_key.lower(), stat_key)

    games = []
    for s in recent:
        statline = ((s.get("stat") or {}))
        games.append({
            "date": s.get("date"),
            "opponent": (s.get("opponent") or {}).get("name"),
            "result": statline.get("result"),
            "value": statline.get(fld, 0),
            "raw": statline,
        })

    return {"player": player_name, "count": len(games), "games": games}

# Backward compatibility functions
def resolve_mlb_player_id(name: str):
    """Backward compatibility wrapper"""
    return _pick_person_id(name)

def _game_logs(player_id: int, group: str, season: int = None):
    """Backward compatibility wrapper"""
    if season is None:
        season = datetime.now().year
    return _fetch_game_logs(player_id, group, season)

def compute_l10(name: str, stat_key: str, line: float, lookback: int = 10):
    """Backward compatibility wrapper for existing code"""
    if not name or line is None:
        return None
    
    # Map stat keys to the new format
    stat_map = {
        "batter_hits": "hits",
        "batter_total_bases": "total_bases",
        "batter_home_runs": "home_runs",
        "rbis": "rbi",
        "runs": "runs",
        "hits": "hits",
        "total_bases": "total_bases",
        "home_runs": "home_runs",
    }
    
    mapped_stat = stat_map.get(stat_key, stat_key)
    
    try:
        trend_data = get_last_10_trend(name, mapped_stat)
        if not trend_data or not trend_data.get("games"):
            return None
            
        games = trend_data["games"][:lookback]
        if not games:
            return None
            
        # Calculate over/under stats
        over_count = sum(1 for game in games if game.get("value", 0) >= line)
        total_games = len(games)
        
        if total_games == 0:
            return None
            
        avg_value = sum(game.get("value", 0) for game in games) / total_games
        values = [game.get("value", 0) for game in games]
        values.sort()
        median_value = values[total_games // 2] if total_games % 2 else (values[total_games // 2 - 1] + values[total_games // 2]) / 2
        
        return {
            "games": total_games,
            "over_hits": over_count,
            "rate_over": round(over_count / total_games, 3),
            "avg": round(avg_value, 3),
            "median": round(median_value, 3)
        }
    except Exception as e:
        log.warning("[L10] compute_l10 failed for %s: %s", name, e)
        return None

def annotate_props_with_l10(props_by_matchup, league: str, lookback: int = 10):
    """Backward compatibility wrapper for existing code"""
    if league.lower() != "mlb":
        return props_by_matchup
        
    for mu, props in props_by_matchup.items():
        for p in props:
            try:
                tr = compute_l10(p.get("player"), p.get("stat"), p.get("line"), lookback=lookback)
                if tr:
                    p.setdefault("meta", {})
                    p["meta"]["l10"] = tr
            except Exception:
                continue
    return props_by_matchup
