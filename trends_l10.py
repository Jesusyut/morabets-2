# trends_l10.py
import os, time, json, logging, requests
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime

LOG = logging.getLogger(__name__)
MLB_STATS_API = "https://statsapi.mlb.com/api/v1"
STATS_TIMEOUT = float(os.getenv("STATSAPI_TIMEOUT", "6"))

# --- cache (Redis -> mem) ---
try:
    import redis
except Exception:
    redis = None
_mem: Dict[str, Tuple[float, Any]] = {}

def _rc():
    url = os.getenv("REDIS_URL")
    if not url or not redis: return None
    try: return redis.from_url(url, decode_responses=True)
    except Exception: return None

def _get(k, ttl):
    r = _rc()
    if r:
        try:
            v = r.get(k)
            if v: return json.loads(v)
        except Exception: pass
    row = _mem.get(k)
    if row and (time.time()-row[0] < ttl): return row[1]
    return None

def _set(k, v, ttl):
    r = _rc()
    if r:
        try: r.setex(k, ttl, json.dumps(v)); return
        except Exception: pass
    _mem[k] = (time.time(), v)

# --- aliases so L10 works across names from odds feed/UI ---
STAT_ALIASES = {
    "runs": "runs",
    "player_runs": "runs",
    "batter_runs_scored": "runs",
    "rbis": "rbis",
    "batter_rbis": "rbis",
    "batter_hits": "batter_hits",
    "hits": "batter_hits",
    "batter_total_bases": "batter_total_bases",
    "total_bases": "batter_total_bases",
    "batter_home_runs": "batter_home_runs",
    "home_runs": "batter_home_runs",
    "pitcher_strikeouts": "pitcher_strikeouts",
    "strikeouts": "pitcher_strikeouts",
}

def _canon(stat_key: str) -> str:
    return STAT_ALIASES.get((stat_key or "").lower(), (stat_key or "").lower())

def resolve_mlb_player_id(name: str) -> Optional[int]:
    if not name: return None
    ck = f"l10:pid:{name.lower()}"
    hit = _get(ck, 7*24*3600)
    if hit is not None: return hit
    try:
        r = requests.get(f"{MLB_STATS_API}/people", params={"search": name}, timeout=STATS_TIMEOUT)
        r.raise_for_status()
        people = (r.json() or {}).get("people") or []
        pid = int(people[0]["id"]) if people else None
        _set(ck, pid, 7*24*3600)
        return pid
    except Exception as e:
        LOG.warning(f"[L10] resolve failed {name}: {e}")
        _set(ck, None, 3600)
        return None

def _game_logs(player_id: int, group: str, season: Optional[int] = None) -> List[Dict[str, Any]]:
    season = season or datetime.utcnow().year
    url = f"{MLB_STATS_API}/people/{player_id}"
    params = {"hydrate": f"stats(group=[{group}],type=[gameLog],season={season})"}
    r = requests.get(url, params=params, timeout=STATS_TIMEOUT)
    r.raise_for_status()
    data = r.json() or {}
    stats = ((data.get("people") or [{}])[0].get("stats") or [])
    return (stats[0].get("splits") or []) if stats else []

def _val(split: Dict[str, Any], stat_key: str) -> Optional[float]:
    st = split.get("stat") or {}
    if stat_key == "batter_hits":        return float(st.get("hits") or 0)
    if stat_key == "batter_home_runs":   return float(st.get("homeRuns") or 0)
    if stat_key == "batter_total_bases":
        singles = float(st.get("hits") or 0) - float(st.get("doubles") or 0) - float(st.get("triples") or 0) - float(st.get("homeRuns") or 0)
        return max(0.0, singles) + 2*float(st.get("doubles") or 0) + 3*float(st.get("triples") or 0) + 4*float(st.get("homeRuns") or 0)
    if stat_key == "rbis":               return float(st.get("rbi") or 0)
    if stat_key == "runs":               return float(st.get("runs") or 0)
    if stat_key == "pitcher_strikeouts": return float(st.get("strikeOuts") or 0)
    return None

def compute_l10(name: str, stat_key: str, line: float, lookback: int = 10) -> Optional[Dict[str, Any]]:
    stat_key = _canon(stat_key)
    if not name or line is None: return None
    pid = resolve_mlb_player_id(name)
    if not pid: return None
    ck = f"l10:trend:{pid}:{stat_key}:{line}"
    hit = _get(ck, 30*60)
    if hit is not None: return hit
    group = "hitting" if stat_key.startswith("batter_") or stat_key in ("rbis","runs") else "pitching"
    try:
        splits = _game_logs(pid, group)
    except Exception as e:
        LOG.warning(f"[L10] logs failed {name}/{pid}: {e}")
        return None
    vals: List[float] = []
    for s in splits[:lookback]:
        v = _val(s, stat_key)
        if v is not None: vals.append(float(v))
    if not vals: return None
    n = len(vals)
    k = sum(1 for v in vals if v >= float(line))
    avg = sum(vals)/n
    mvals = sorted(vals)
    med = mvals[n//2] if n % 2 else (mvals[n//2-1] + mvals[n//2]) / 2.0
    out = {"games": n, "over_hits": int(k), "rate_over": round(k/n,3), "avg": round(avg,3), "median": round(med,3)}
    _set(ck, out, 30*60)
    return out

def annotate_props_with_l10(props_by_matchup: Dict[str, List[Dict[str, Any]]],
                            league: str, lookback: int = 10) -> Dict[str, List[Dict[str, Any]]]:
    if league.lower() != "mlb": return props_by_matchup
    for mu, props in props_by_matchup.items():
        for p in props:
            try:
                tr = compute_l10(p.get("player"), _canon(p.get("stat")), p.get("line"), lookback=lookback)
                if tr:
                    p.setdefault("meta", {})
                    p["meta"]["l10"] = tr
            except Exception:
                continue
    return props_by_matchup
