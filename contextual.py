# contextual.py
import os, math, time
import requests
from datetime import date

MLB = "https://statsapi.mlb.com/api/v1"
TIMEOUT = float(os.getenv("MLB_TIMEOUT","4"))

_session = requests.Session()
_session.headers.update({"User-Agent":"MoraBets/1.0"})

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