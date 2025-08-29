# trends_l10.py
import os, time, json, logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import requests

LOG = logging.getLogger(__name__)

# --- Simple cache: Redis if present, else in-memory --------------------------------
try:
    import redis  # optional
except Exception:
    redis = None

_mem_cache: Dict[str, Tuple[float, Any]] = {}

def _rc():
    url = os.getenv("REDIS_URL")
    if not url or not redis: return None
    try: return redis.from_url(url, decode_responses=True)
    except Exception: return None

def _cget(k: str, ttl: int):
    r = _rc()
    if r:
        try:
            v = r.get(k)
            if v: return json.loads(v)
        except Exception: pass
    row = _mem_cache.get(k)
    if row and (time.time() - row[0] < ttl): return row[1]
    return None

def _cset(k: str, v: Any, ttl: int):
    r = _rc()
    if r:
        try: 
            r.setex(k, ttl, json.dumps(v))
            return
        except Exception: pass
    _mem_cache[k] = (time.time(), v)

# --- MLB Stats API helpers ----------------------------------------------------------
MLB_STATS_API = "https://statsapi.mlb.com/api/v1"
STATS_TIMEOUT = float(os.getenv("STATSAPI_TIMEOUT", "6"))

def resolve_mlb_player_id(name: str) -> Optional[int]:
    """Resolve player id via MLB people search."""
    if not name: return None
    ck = f"l10:pid:{name.lower()}"
    hit = _cget(ck, 7*24*3600)
    if hit is not None: return hit
    try:
        r = requests.get(f"{MLB_STATS_API}/people", params={"search": name}, timeout=STATS_TIMEOUT)
        r.raise_for_status()
        people = (r.json() or {}).get("people") or []
        pid = int(people[0]["id"]) if people else None
        _cset(ck, pid, 7*24*3600)
        return pid
    except Exception as e:
        LOG.warning(f"[L10] resolve failed {name}: {e}")
        _cset(ck, None, 3600)
        return None

def _game_logs(player_id: int, stat_group: str, season: Optional[int] = None) -> List[Dict[str, Any]]:
    """Return per-game splits for season (defaults to current season)."""
    season = season or datetime.utcnow().year
    url = f"{MLB_STATS_API}/people/{player_id}"
    params = {"hydrate": f"stats(group=[{stat_group}],type=[gameLog],season={season})"}
    r = requests.get(url, params=params, timeout=STATS_TIMEOUT)
    r.raise_for_status()
    data = r.json() or {}
    stats = ((data.get("people") or [{}])[0].get("stats") or [])
    return (stats[0].get("splits") or []) if stats else []

def _value_from_split(split: Dict[str, Any], stat_key: str) -> Optional[float]:
    st = split.get("stat") or {}
    if stat_key == "batter_hits":          return float(st.get("hits") or 0)
    if stat_key == "batter_home_runs":     return float(st.get("homeRuns") or 0)
    if stat_key == "batter_total_bases":
        singles = float(st.get("hits") or 0) - float(st.get("doubles") or 0) - float(st.get("triples") or 0) - float(st.get("homeRuns") or 0)
        return max(0.0, singles) + 2*float(st.get("doubles") or 0) + 3*float(st.get("triples") or 0) + 4*float(st.get("homeRuns") or 0)
    if stat_key == "rbis":                 return float(st.get("rbi") or 0)
    if stat_key == "runs":                 return float(st.get("runs") or 0)
    if stat_key == "pitcher_strikeouts":   return float(st.get("strikeOuts") or 0)
    return None

def compute_l10(name: str, stat_key: str, line: float, lookback: int = 10) -> Optional[Dict[str, Any]]:
    """
    L10 trend for (player, stat, line):
      { "games":N, "over_hits":K, "rate_over":K/N, "avg":μ, "median":m }
    """
    if not name or line is None: return None
    pid = resolve_mlb_player_id(name)
    if not pid: return None

    ck = f"l10:trend:{pid}:{stat_key}:{line}"
    hit = _cget(ck, 30*60)
    if hit is not None: return hit

    group = "hitting" if stat_key.startswith("batter_") or stat_key in ("rbis","runs") else "pitching"
    try:
        splits = _game_logs(pid, group)
    except Exception as e:
        LOG.warning(f"[L10] logs failed {name}/{pid}: {e}")
        return None

    vals: List[float] = []
    for s in splits[:lookback]:
        v = _value_from_split(s, stat_key)
        if v is not None: vals.append(float(v))

    if not vals: return None
    n = len(vals)
    k = sum(1 for v in vals if v >= float(line))
    avg = sum(vals)/n
    med_sorted = sorted(vals)
    m = med_sorted[n//2] if n % 2 == 1 else (med_sorted[n//2-1] + med_sorted[n//2]) / 2.0

    out = {"games": n, "over_hits": int(k), "rate_over": round(k/n,3), "avg": round(avg,3), "median": round(m,3)}
    _cset(ck, out, 30*60)
    return out

def annotate_props_with_l10(props_by_matchup: Dict[str, List[Dict[str, Any]]],
                            league: str, lookback: int = 10) -> Dict[str, List[Dict[str, Any]]]:
    """Best-effort: attaches trend under prop['meta']['l10']; never breaks response."""
    if league.lower() != "mlb": return props_by_matchup
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
