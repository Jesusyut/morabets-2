# trends_l10.py
import re, time, unicodedata, threading
from functools import lru_cache
import requests
from datetime import datetime

MLB_STATS_API = "https://statsapi.mlb.com/api/v1"
STATS_TIMEOUT = 8

# log object you already use in this file
try:
    LOG
except NameError:
    import logging
    LOG = logging.getLogger("trends_l10")
    LOG.setLevel(logging.INFO)

# Negative cache (avoid spamming the same failing name)
_NEG_PID: dict[str, float] = {}
_NEG_LOCK = threading.Lock()
_NEG_TTL = 900  # seconds

# Optional alias map for frequent edge cases
_ALIAS = {
    "jp crawford": "J.P. Crawford",
    "j p crawford": "J.P. Crawford",
    "cj kayfus": "C.J. Kayfus",
    "c j kayfus": "C.J. Kayfus",
}

def _apply_alias(name: str) -> str:
    key = name.lower().replace(".", "").strip()
    return _ALIAS.get(key, name)

def _nfkd(s: str) -> str:
    # keep diacritics by default; only strip if you want later
    return unicodedata.normalize("NFKD", s or "")

_SUFFIXES = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v"}

def _strip_suffix(name: str) -> str:
    parts = name.replace(".", "").split()
    if parts and parts[-1].lower() in _SUFFIXES:
        return " ".join(parts[:-1])
    return name

def _initials_to_compact(name: str) -> str:
    """
    'J.P. Crawford'   -> 'JP Crawford'
    'J. P. Crawford'  -> 'JP Crawford'
    'C. J. Kayfus'    -> 'CJ Kayfus'
    """
    n = name
    # collapse 'J. P.' or 'J.P.' to 'JP'
    n = re.sub(r"\b([A-Z])\.\s*([A-Z])\.\b", r"\1\2", n)
    # single dotted initial 'J.' -> 'J'
    n = re.sub(r"\b([A-Z])\.\b", r"\1", n)
    # collapse multiple spaces
    n = re.sub(r"\s{2,}", " ", n).strip()
    return n

def _variants(name: str):
    """
    Yield a sequence of search variants, most-specific first.
    """
    base = _apply_alias(_nfkd(name).strip())
    # 0) as-is
    yield base
    # 1) compact initials (keeps dots elsewhere)
    compact = _initials_to_compact(base)
    if compact != base:
        yield compact
    # 2) remove all periods
    no_dots = base.replace(".", "")
    if no_dots != base:
        yield no_dots
    # 3) compact + no periods
    compact_no_dots = compact.replace(".", "")
    if compact_no_dots not in (base, compact, no_dots):
        yield compact_no_dots
    # 4) strip suffixes like Jr./III
    stripped = _strip_suffix(no_dots)
    if stripped not in (base, compact, no_dots, compact_no_dots):
        yield stripped
    # 5) first + last only
    parts = re.split(r"\s+", stripped)
    if len(parts) >= 2:
        fl = f"{parts[0]} {parts[-1]}".strip()
        if fl not in (base, compact, no_dots, compact_no_dots, stripped):
            yield fl

@lru_cache(maxsize=2048)
def _people_search(names: str) -> list[dict]:
    r = requests.get(f"{MLB_STATS_API}/people/search", params={"names": names}, timeout=STATS_TIMEOUT)
    r.raise_for_status()
    return (r.json() or {}).get("people", []) or []

def resolve_mlb_player_id(name: str) -> int | None:
    if not name:
        return None

    # Negative cache guard
    now = time.time()
    with _NEG_LOCK:
        t = _NEG_PID.get(name.lower())
        if t and now - t < _NEG_TTL:
            return None

    tried = []
    pid: int | None = None

    for q in _variants(name):
        tried.append(q)
        try:
            people = _people_search(q)
        except Exception as e:
            LOG.warning("[L10] people/search failed '%s': %s", q, e)
            continue
        if not people:
            continue

        # prefer exact case-insens match; else first
        ln = q.lower()
        exact = [p for p in people if (p.get("fullName") or "").lower() == ln]
        cand = exact[0] if exact else people[0]
        pid = int(cand.get("id")) if cand.get("id") is not None else None
        if pid:
            break

    if not pid:
        LOG.warning("[L10] resolve failed (no id) %s | tried=%s", name, " | ".join(tried))
        with _NEG_LOCK:
            _NEG_PID[name.lower()] = now
    return pid

@lru_cache(maxsize=4096)
def _fetch_game_logs(person_id: int, group: str, season: int):
    """
    Returns gameLog splits list for season.
    """
    url = f"{MLB_STATS_API}/people/{person_id}/stats"
    params = {"stats": "gameLog", "group": group, "season": str(season)}
    r = requests.get(url, params=params, timeout=STATS_TIMEOUT)
    r.raise_for_status()
    js = r.json() or {}
    stats = (js.get("stats") or [])
    return stats[0].get("splits", []) if stats else []

def _parse_date(date_str: str) -> datetime:
    """Parse MLB date format (2024-03-28) to datetime."""
    return datetime.strptime(date_str, "%Y-%m-%d")

def get_last_10_trend(player_name: str, stat_type: str, threshold: float) -> dict:
    """
    Get last 10 games trend for a player and stat.
    Returns: {"player_name": str, "games": int, "rate_over": float, "confidence": str}
    """
    # Resolve player ID with improved resolver
    pid = resolve_mlb_player_id(player_name)
    if not pid:
        # return graceful miss; no more spammy retries for 15 minutes
        return {"player_name": player_name, "games": 0, "rate_over": 0.0, "confidence": "no_id"}

    # Map stat types to MLB API groups
    stat_mapping = {
        "hits": "hitting",
        "total_bases": "hitting", 
        "batter_hits": "hitting",
        "batter_total_bases": "hitting",
        "runs": "hitting",
        "rbis": "hitting",
        "home_runs": "hitting",
        "strikeouts": "hitting",
        "walks": "hitting",
        "stolen_bases": "hitting",
        "doubles": "hitting",
        "triples": "hitting",
    }
    
    group = stat_mapping.get(stat_type, "hitting")
    
    # Get current year and previous year
    current_year = datetime.now().year
    years_to_try = [current_year, current_year - 1]
    
    all_games = []
    
    for year in years_to_try:
        try:
            game_logs = _fetch_game_logs(pid, group, year)
            for game in game_logs:
                game_date = _parse_date(game.get("date", ""))
                # Only include games from last 10 games (most recent first)
                all_games.append((game_date, game))
        except Exception as e:
            LOG.warning(f"[L10] Failed to fetch {year} logs for {player_name}: {e}")
            continue
    
    # Sort by date (most recent first) and take last 10
    all_games.sort(key=lambda x: x[0], reverse=True)
    recent_games = all_games[:10]
    
    if not recent_games:
        return {"player_name": player_name, "games": 0, "rate_over": 0.0, "confidence": "no_games"}
    
    # Count games over threshold
    over_count = 0
    for _, game in recent_games:
        stat_value = _extract_stat_value(game, stat_type)
        if stat_value is not None and stat_value > threshold:
            over_count += 1
    
    rate_over = over_count / len(recent_games) if recent_games else 0.0
    
    # Determine confidence level
    if len(recent_games) >= 8:
        confidence = "high"
    elif len(recent_games) >= 5:
        confidence = "medium"
    else:
        confidence = "low"
    
    return {
        "player_name": player_name,
        "games": len(recent_games),
        "rate_over": round(rate_over, 3),
        "confidence": confidence
    }

def _extract_stat_value(game: dict, stat_type: str) -> float | None:
    """Extract stat value from game log based on stat type."""
    stat_mapping = {
        "hits": "hits",
        "total_bases": "totalBases",
        "batter_hits": "hits", 
        "batter_total_bases": "totalBases",
        "runs": "runs",
        "rbis": "rbi",
        "home_runs": "homeRuns",
        "strikeouts": "strikeOuts",
        "walks": "baseOnBalls",
        "stolen_bases": "stolenBases",
        "doubles": "doubles",
        "triples": "triples",
    }
    
    mlb_stat = stat_mapping.get(stat_type)
    if not mlb_stat:
        return None
    
    value = game.get("stat", {}).get(mlb_stat)
    if value is None:
        return None
    
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def compute_l10(player_name: str, stat_type: str, threshold: float) -> dict:
    """Wrapper for get_last_10_trend with consistent naming."""
    return get_last_10_trend(player_name, stat_type, threshold)

def annotate_props_with_l10(props_by_matchup: dict, league: str = "mlb", lookback: int = 10) -> dict:
    """
    Annotate props with L10 trends for MLB.
    """
    if league.lower() != "mlb":
        return props_by_matchup
    
    for matchup, props in props_by_matchup.items():
        for prop in props:
            player_name = prop.get("player", "")
            stat_type = prop.get("stat", "")
            line = prop.get("line", 0)
            
            if player_name and stat_type and line is not None:
                try:
                    trend = compute_l10(player_name, stat_type, float(line))
                    prop["meta"] = prop.get("meta", {})
                    prop["meta"]["l10"] = trend
                except Exception as e:
                    LOG.warning(f"[L10] Failed to compute trend for {player_name} {stat_type}: {e}")
                    continue
    
    return props_by_matchup
