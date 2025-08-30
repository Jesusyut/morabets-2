# contextual.py
import os, math, time
import requests
from datetime import date
from functools import lru_cache
import json

MLB = "https://statsapi.mlb.com/api/v1"
TIMEOUT = float(os.getenv("MLB_TIMEOUT","4"))

_session = requests.Session()
_session.headers.update({"User-Agent":"MoraBets/1.0"})

# --- Add below your existing imports/session ---
try:
    from redis import Redis
    R = Redis.from_url(os.getenv("REDIS_URL","redis://localhost:6379/0"), decode_responses=True)
except Exception:
    R = None

def _cache_key(player, stat, th):
    return f"ctx:{date.today().isoformat()}:{player}:{stat}:{th}"

# Optional tiny in-process fallback cache for hot calls (1k entries)
@lru_cache(maxsize=1024)
def _memo_key(k: str) -> str:
    return k  # lru works on the string key

# Map FE -> StatsAPI stat fields (batter only here; extend if needed)
STAT_KEY_MAP = {
    "batter_hits":"hits",
    "hits":"hits",
    "batter_total_bases":"totalBases",
    "total_bases":"totalBases",
    "tb":"totalBases",
    "batter_home_runs":"homeRuns",
    "home_runs":"homeRuns",
    "batter_runs":"runs",
    "runs":"runs",
    "batter_runs_batted_in":"rbi",
    "rbi":"rbi",
    "batter_walks":"baseOnBalls",
    "walks":"baseOnBalls",
    "batter_stolen_bases":"stolenBases",
    "stolen_bases":"stolenBases",
    "batter_strikeouts":"strikeOuts",
    "strikeouts":"strikeOuts",
}

def _get(url, params=None, timeout=TIMEOUT):
    for i in range(3):
        try:
            r = _session.get(url, params=params, timeout=timeout)
            if r.ok: return r
        except Exception:
            if i == 2: raise
            time.sleep(0.25*(i+1))
    raise RuntimeError("MLB request failed")

def _resolve_player_id(name:str)->int:
    r = _get(f"{MLB}/people/search", params={"names": name})
    js = r.json() or {}
    people = js.get("people") or []
    if not people:
        raise ValueError(f"player not found: {name}")
    return int(people[0]["id"])

def _game_logs(pid:int, season:int, group:str="hitting"):
    r = _get(f"{MLB}/people/{pid}/stats", params={"stats":"gameLog","season":season,"group":group})
    js = r.json() or {}
    return ((js.get("stats") or [{}])[0] or {}).get("splits", []) or []

def _conf_label(rate:float, n:int)->str:
    if n < 6: return "low"
    se = math.sqrt(max(rate*(1-rate),1e-9)/max(n,1))
    z = abs(rate-0.5)/max(se,1e-9)
    if n>=8 and z>=1.5: return "high"
    if z>=0.8: return "medium"
    return "low"

def get_contextual_hit_rate(player_name:str, stat_type:str, threshold:float):
    """
    MLB StatsAPI ONLY. Independent of Odds/Enrichment.
    Returns: { hit_rate, sample_size, confidence, threshold }
    """
    pid = _resolve_player_id(player_name)
    key = STAT_KEY_MAP.get((stat_type or "").lower(), stat_type)

    logs = _game_logs(pid, date.today().year, "hitting")
    if len(logs) < 10:
        logs += _game_logs(pid, date.today().year - 1, "hitting")

    vals = []
    for s in logs[:10]:
        st = s.get("stat") or {}
        vals.append(float(st.get(key, 0) or 0))

    n = len(vals)
    if n == 0:
        return {"hit_rate":0.0,"sample_size":0,"confidence":"low","threshold":float(threshold)}

    overs = sum(1 for v in vals if v >= float(threshold))
    rate = overs / n
    return {
        "hit_rate": round(rate,4),
        "sample_size": n,
        "confidence": _conf_label(rate, n),
        "threshold": float(threshold),
    }

def get_contextual_hit_rate_cached(player_name: str, stat_type: str, threshold: float):
    """
    Thin caching wrapper over your existing get_contextual_hit_rate().
    - First check Redis by per-day key
    - Then in-process LRU
    - Compute and backfill both caches on miss
    """
    key = _cache_key(player_name, stat_type, threshold)

    # Redis first
    if R:
        cached = R.get(key)
        if cached:
            return json.loads(cached)

    # in-process second
    try:
        payload = json.loads(_memo_key(key))
        return payload
    except Exception:
        pass

    # Compute using your existing function (unchanged)
    payload = get_contextual_hit_rate(player_name, stat_type, threshold)

    # Store
    try:
        if R:
            R.setex(key, 6*3600, json.dumps(payload))
        _memo_key.cache_clear()          # keep LRU tidy
        _memo_key(json.dumps(payload))   # prime LRU with the payload as the value
        _memo_key(key)                   # pair the key (lru needs the same function input)
    except Exception:
        pass

    return payload