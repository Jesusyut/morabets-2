# mlb_trends.py
import os, time, math
from datetime import date
import requests
from requests.adapters import HTTPAdapter, Retry

MLB = "https://statsapi.mlb.com/api/v1"
TIMEOUT = float(os.getenv("MLB_TIMEOUT","4"))

session = requests.Session()
session.headers.update({"User-Agent":"MoraBets/1.0"})
session.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=0.3, status_forcelist=[429,500,502,503,504])))

STAT_KEY_MAP = {
    "batter_hits":"hits", "hits":"hits",
    "batter_total_bases":"totalBases", "total_bases":"totalBases", "tb":"totalBases",
}

def _get(url, params=None):
    return session.get(url, params=params, timeout=TIMEOUT)

def resolve_player_id(name:str)->int:
    r = _get(f"{MLB}/people/search", params={"names": name})
    js = r.json() or {}
    people = js.get("people") or []
    if not people: raise ValueError(f"player not found: {name}")
    return int(people[0]["id"])

def game_logs(pid:int, season:int, group:str="hitting"):
    r = _get(f"{MLB}/people/{pid}/stats", params={"stats":"gameLog","season":season,"group":group})
    js = r.json() or {}
    return ((js.get("stats") or [{}])[0] or {}).get("splits", []) or []

def last10_rate(player_name:str, stat_type:str, threshold:float):
    pid = resolve_player_id(player_name)
    key = STAT_KEY_MAP.get(stat_type.lower(), stat_type)
    logs = game_logs(pid, date.today().year) or []
    if len(logs) < 10:
        logs += game_logs(pid, date.today().year - 1)
    vals = []
    for s in logs[:10]:
        stat = (s.get("stat") or {})
        vals.append(float(stat.get(key, 0) or 0))
    n = len(vals)
    if n == 0: return {"hit_rate":0.0,"sample_size":0,"confidence":"low","threshold":float(threshold)}
    overs = sum(1 for v in vals if v >= float(threshold))
    rate = overs / n
    # simple confidence label
    se = math.sqrt(max(rate*(1-rate),1e-9)/max(n,1))
    z = abs(rate-0.5)/max(se,1e-9)
    conf = "high" if (n>=8 and z>=1.5) else ("medium" if z>=0.8 else "low")
    return {"hit_rate":round(rate,4),"sample_size":n,"confidence":conf,"threshold":float(threshold)}
