import os, math, time
import requests
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent":"MoraBets/1.0"})
TIMEOUT = float(os.getenv("MLB_TIMEOUT", "4"))

def _http_get(url, params=None, timeout=TIMEOUT):
    # simple retry
    for i in range(3):
        try:
            r = SESSION.get(url, params=params, timeout=timeout)
            if r.ok: return r
        except Exception:
            if i == 2: raise
            time.sleep(0.25 * (i+1))
    raise RuntimeError("MLB request failed")

def _resolve_player_id(name:str)->int:
    # MLB people search
    r = _http_get("https://statsapi.mlb.com/api/v1/people/search", params={"names": name})
    data = r.json() or {}
    people = data.get("people") or []
    if not people:
        raise ValueError(f"player not found: {name}")
    return int(people[0]["id"])

def _game_logs(pid:int, group:str="hitting", season:int=None)->list:
    if season is None:
        season = date.today().year
    # gameLog for season
    params = {
        "stats": "gameLog",
        "group": group,
        "season": season
    }
    r = _http_get(f"https://statsapi.mlb.com/api/v1/people/{pid}/stats", params=params)
    js = r.json() or {}
    splits = (((js.get("stats") or [])[0] or {}).get("splits") or [])
    # Normalize each split to dict with date + stat map
    out=[]
    for s in splits:
        d = (s.get("date") or s.get("gameDate") or "")
        st = (s.get("stat") or {})
        out.append({"date": d, "stat": st})
    # Sort newest first
    out.sort(key=lambda x: x["date"], reverse=True)
    return out

def _map_stat_value(stat_type:str, stat_obj:dict)->float:
    s = (stat_type or "").lower()
    if s in ("hits","batter_hits"):   return float(stat_obj.get("hits", 0))
    if s in ("total_bases","batter_total_bases","tb"): return float(stat_obj.get("totalBases", 0))
    # fallback: 0 so it won't count as over
    return float(stat_obj.get(s, 0) or 0)

def _confidence_label(rate:float, n:int)->str:
    # quick binomial std err check vs 0.5 baseline
    if n < 6: return "low"
    se = math.sqrt(max(rate*(1-rate), 1e-9)/max(n,1))
    z = abs(rate - 0.5) / max(se, 1e-9)
    if n >= 8 and z >= 1.5: return "high"
    if z >= 0.8: return "medium"
    return "low"

# Enhanced Enrichment: Pitcher Split Matchups
def get_pitcher_splits_multiplier(pitcher_id, batter_handedness):
    """Get pitcher's performance vs specific handedness (L/R)"""
    try:
        response = requests.get(
            f"{MLB_STATS_API}/people/{pitcher_id}/stats",
            params={
                "stats": "vsHand",
                "season": "2025",
                "group": "pitching"
            },
            timeout=10
        )
        
        if response.status_code != 200:
            return 1.0
            
        data = response.json()
        splits = data.get("stats", [])
        
        for split in splits:
            split_data = split.get("splits", [])
            for hand_split in split_data:
                split_name = hand_split.get("split", {}).get("description", "")
                
                # Match handedness
                if (batter_handedness == "L" and "Left" in split_name) or \
                   (batter_handedness == "R" and "Right" in split_name):
                    
                    stats = hand_split.get("stat", {})
                    era = stats.get("era", 4.5)
                    whip = stats.get("whip", 1.3)
                    
                    # Higher ERA/WHIP = easier for batters
                    if era > 5.0 or whip > 1.4:
                        return 1.08  # 8% boost vs struggling pitcher
                    elif era < 3.0 and whip < 1.1:
                        return 0.92  # 8% reduction vs dominant pitcher
                    
        return 1.0
        
    except Exception as e:
        logger.debug(f"Pitcher splits error for pitcher {pitcher_id}: {e}")
        return 1.0

MLB_STATS_API = "https://statsapi.mlb.com/api/v1"

STAT_KEY_MAP = {
    "batter_total_bases": "totalBases",
    "batter_hits": "hits",
    "batter_runs_batted_in": "rbi",  # Updated to match new API key
    "batter_runs": "runs", 
    "batter_home_runs": "homeRuns",
    "batter_stolen_bases": "stolenBases",
    "batter_walks": "baseOnBalls",
    "batter_strikeouts": "strikeOuts",
    "batter_hits_runs_rbis": "combinedStats",
    "pitcher_strikeouts": "strikeOuts",
    "pitcher_hits_allowed": "hits",
    "pitcher_earned_runs": "earnedRuns",
    "pitcher_walks": "baseOnBalls",
    "pitcher_outs": "outs",
    "batter_fantasy_score": "fantasyPoints",
    "pitcher_fantasy_score": "fantasyPoints",
    # legacy batter names
    "hits": "hits",
    "total_bases": "totalBases",
    "home_runs": "homeRuns",
    "runs": "runs",
    "rbi": "rbi",
    "walks": "baseOnBalls",
    "stolen_bases": "stolenBases",
    "strikeouts": "strikeOuts",
    "tb": "totalBases",
}

def get_player_id(player_name):
    """Get MLB player ID from name"""
    try:
        resp = requests.get(
            f"{MLB_STATS_API}/people/search", 
            params={"names": player_name},
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        
        if data.get("people"):
            return data["people"][0]["id"]
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching player ID for {player_name}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error getting player ID for {player_name}: {e}")
        return None

def get_opponent_context(player_id):
    """Get opponent context for a player"""
    try:
        stats_resp = requests.get(
            f"{MLB_STATS_API}/people/{player_id}/stats",
            params={
                "stats": "gameLog",
                "season": "2025",
                "group": "hitting"
            },
            timeout=10
        )
        stats_resp.raise_for_status()
        data = stats_resp.json()
        
        stats = data.get("stats", [])
        if not stats:
            return None
        
        logs = stats[0].get("splits", [])
        if not logs:
            return None
        
        latest_game = logs[0]
        return (
            latest_game.get("team", {}).get("id"),
            latest_game.get("opponent", {}).get("id"),
            latest_game.get("pitcher", {}).get("hand", {}).get("code")
        )
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching opponent context for player {player_id}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error getting opponent context for player {player_id}: {e}")
        return None

def get_fallback_hit_rate(player_name, stat_type, threshold):
    """Generate realistic fallback hit rate based on MLB averages"""
    fallback_rates = {
        # Batting stats - based on MLB averages
        "hits": 0.35,
        "totalBases": 0.40,
        "rbi": 0.25,
        "runs": 0.30,
        "homeRuns": 0.15,
        "stolenBases": 0.08,
        "baseOnBalls": 0.20,
        "strikeOuts": 0.65,
        "combinedStats": 0.50,
        "fantasyPoints": 0.45,
        
        # Pitching stats - based on MLB averages  
        "pitcher_strikeouts": 0.55,
        "pitcher_hits_allowed": 0.45,
        "pitcher_earned_runs": 0.35,
        "pitcher_walks": 0.20,
        "pitcher_outs": 0.75
    }
    
    # Use the MLB stat key for lookup
    mlb_stat_key = STAT_KEY_MAP.get(stat_type, stat_type)
    base_rate = fallback_rates.get(mlb_stat_key, 0.35)
    
    # Adjust for threshold difficulty
    if threshold >= 5:
        base_rate *= 0.65
    elif threshold >= 3:
        base_rate *= 0.80
    elif threshold >= 1.5:
        base_rate *= 0.90
    
    return {
        "player": player_name,
        "stat": mlb_stat_key,
        "threshold": threshold,
        "hit_rate": round(base_rate, 2),
        "sample_size": 10,
        "confidence": "Low",
        "note": "Fallback calculation based on MLB averages"
    }

def get_contextual_hit_rate(player_name:str, stat_type:str, threshold:float):
    """
    Returns a dict like:
    {
      "hit_rate": 0.7,
      "sample_size": 10,
      "confidence": "high",
      "threshold": 1.5
    }
    """
    pid = _resolve_player_id(player_name)

    logs = []
    # Pull current season first; if <10 games, also pull last season
    yr = date.today().year
    logs.extend(_game_logs(pid, group="hitting", season=yr))
    if len(logs) < 10:
        logs.extend(_game_logs(pid, group="hitting", season=yr-1))

    # take last 10 appearances with stat values
    filtered=[]
    for g in logs:
        v = _map_stat_value(stat_type, g["stat"])
        filtered.append(v)
        if len(filtered) >= 10: break

    n = len(filtered)
    if n == 0:
        return {"hit_rate": 0.0, "sample_size": 0, "confidence": "low", "threshold": float(threshold)}

    overs = sum(1 for v in filtered if v >= float(threshold))
    rate = overs / n
    conf = _confidence_label(rate, n)

    return {
        "hit_rate": round(rate, 4),
        "sample_size": n,
        "confidence": conf,
        "threshold": float(threshold)
    }