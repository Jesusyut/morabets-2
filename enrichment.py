import requests
from datetime import datetime
import logging
import json
import time
import os

# --- NEW: MLB player-id resolver (cached) ---
from functools import lru_cache
import httpx
import re

def _normalize_name(name: str) -> str:
    # "Mookie Betts" -> "mookie betts"
    return re.sub(r"\s+", " ", (name or "").strip()).lower()

@lru_cache(maxsize=4096)
def _mlb_player_id_cache(norm_name: str) -> str | None:
    """
    Resolve MLB player id from StatsAPI using the search endpoint.
    Cached in-process via lru_cache to avoid repeated calls.
    Returns string id or None if not found.
    """
    if not norm_name:
        return None
    url = "https://statsapi.mlb.com/api/v1/people/search"
    params = {"names": norm_name}
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json() or {}
            people = data.get("people") or []
            # Try exact match on normalized full name; else first result.
            for p in people:
                full = _normalize_name(p.get("fullName", ""))
                if full == norm_name and p.get("id"):
                    return str(p["id"])
            if people and people[0].get("id"):
                return str(people[0]["id"])
    except Exception:
        # Keep silent; resolver is best-effort.
        return None
    return None

def resolve_mlb_player_id(full_name: str) -> str | None:
    """Public wrapper: normalizes and queries the cached resolver."""
    return _mlb_player_id_cache(_normalize_name(full_name or ""))

logger = logging.getLogger(__name__)

def _safe_init_fair(row: dict) -> None:
    row.setdefault("fair", {})
    row["fair"].setdefault("book", "")
    row["fair"].setdefault("prob", {})
    row["fair"]["prob"].setdefault("over", 0.0)
    row["fair"]["prob"].setdefault("under", 0.0)

# Find and replace any direct assignment to row["fair"] with:
# _safe_init_fair(row)

# Example usage in this file wherever fair is referenced:
# _safe_init_fair(row)
# # then only set fields if missing, never force 0/0 over existing non-zeros
# p = row["fair"]["prob"]
# if not (p.get("over") or p.get("under")):
#     # only if still empty, you may put placeholders
#     p["over"]  = p.get("over", 0.0)
#     p["under"] = p.get("under", 0.0)

# Enhanced Enrichment: Park Factor Analysis
def load_park_factors():
    """Load park factors from JSON file with safe fallback"""
    try:
        with open("park_factors.json", "r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load park factors: {e}")
        return {}

def apply_park_factor(prop, stadium_name):
    """Apply park factor multiplier based on stadium and stat type"""
    try:
        park_factors = load_park_factors()
        factors = park_factors.get(stadium_name, {})
        
        stat_type = prop.get("stat_type", "").lower()
        
        if "home_runs" in stat_type or "hr" in stat_type:
            return factors.get("hr_factor", 1.0)
        elif "total_bases" in stat_type or "tb" in stat_type:
            return factors.get("tb_factor", 1.0)
        elif "hits" in stat_type:
            return factors.get("hits_factor", 1.0)
        elif "runs" in stat_type:
            return factors.get("run_factor", 1.0)
        
        return 1.0
    except Exception as e:
        logger.debug(f"Park factor error for {stadium_name}: {e}")
        return 1.0

# Enhanced Enrichment: Recent Player Form
def get_recent_form_multiplier(player_id, stat_type):
    """Calculate recent form multiplier based on last 5 games vs season average"""
    try:
        response = requests.get(
            f"{MLB_STATS_API}/people/{player_id}/stats",
            params={
                "stats": "gameLog",
                "season": "2025",
                "group": "hitting" if "batter_" in stat_type else "pitching"
            },
            timeout=10
        )
        
        if response.status_code != 200:
            return 1.0
            
        data = response.json()
        stats = data.get("stats", [])
        if not stats:
            return 1.0
        
        logs = stats[0].get("splits", [])
        if len(logs) < 5:
            return 1.0
        
        # Get last 5 games
        recent_games = logs[:5]
        
        # Calculate recent average for the specific stat
        stat_key_map = {
            "batter_hits": "hits",
            "batter_total_bases": "totalBases", 
            "batter_home_runs": "homeRuns",
            "pitcher_strikeouts": "strikeOuts",
            "pitcher_earned_runs": "earnedRuns"
        }
        
        stat_key = stat_key_map.get(stat_type)
        if not stat_key:
            return 1.0
        
        recent_total = sum(game.get("stat", {}).get(stat_key, 0) for game in recent_games)
        recent_avg = recent_total / 5.0
        
        # Get season average
        season_stats = logs[0].get("stat", {})
        games_played = season_stats.get("gamesPlayed", 1)
        season_total = season_stats.get(stat_key, 0)
        season_avg = season_total / max(games_played, 1)
        
        # Calculate form multiplier (cap at +/-20%)
        if season_avg > 0:
            form_ratio = recent_avg / season_avg
            return max(0.8, min(1.2, form_ratio))
        
        return 1.0
        
    except Exception as e:
        logger.debug(f"Recent form error for player {player_id}: {e}")
        return 1.0

# Enhanced Enrichment: Bullpen Fatigue Context
def get_bullpen_fatigue_multiplier(team_name):
    """Analyze bullpen usage over past 3 games"""
    try:
        # This is a simplified version - in production you'd call MLB API for actual bullpen data
        high_usage_teams = ["Red Sox", "Royals", "Angels", "White Sox", "Rockies"]
        
        if team_name in high_usage_teams:
            return 1.05  # 5% boost for hitting props against fatigued bullpens
        return 1.0
        
    except Exception as e:
        logger.debug(f"Bullpen fatigue error for {team_name}: {e}")
        return 1.0

# Enhanced Enrichment: Lineup Position Influence  
def get_lineup_position_multiplier(player_name):
    """Apply multiplier based on typical lineup position"""
    try:
        # Simplified - would integrate with actual lineup data in production
        top_order_players = ["Mookie Betts", "Aaron Judge", "Juan Soto", "Freddie Freeman"]
        bottom_order_players = ["Kyle Higashioka", "Nick Ahmed", "Jake Meyers"]
        
        if player_name in top_order_players:
            return 1.08  # 8% boost for 1-4 hitters
        elif player_name in bottom_order_players:
            return 0.95  # 5% reduction for 7-9 hitters
        
        return 1.0
        
    except Exception as e:
        logger.debug(f"Lineup position error for {player_name}: {e}")
        return 1.0

MLB_STATS_API = "https://statsapi.mlb.com/api/v1"

def cache_props_to_file(props, filename="mlb_props_cache.json"):
    """Redis-free prop caching using flat JSON file"""
    try:
        # Attach player_ids for MLB props before caching
        league = "mlb" if "mlb" in filename.lower() else "nfl"
        props = _attach_player_ids_if_needed(props, league)
        
        with open(filename, "w") as f:
            json.dump(props, f)
        print(f"[CACHE] Props saved to {filename} ✅")
        return True
    except Exception as e:
        print(f"[CACHE ERROR] Failed to write cache: {e}")
        return False

def load_props_from_file(filename="mlb_props_cache.json"):
    """Load props from file cache"""
    try:
        with open(filename, "r") as f:
            props = json.load(f)
        print(f"[CACHE] Loaded {len(props)} props from {filename}")
        
        # Attach player_ids for MLB props
        league = "mlb" if "mlb" in filename.lower() else "nfl"
        props = _attach_player_ids_if_needed(props, league)
        
        return props
    except FileNotFoundError:
        print(f"[CACHE] No cache file found: {filename}")
        return []
    except Exception as e:
        print(f"[CACHE ERROR] Failed to load cache: {e}")
        return []

# In-memory cache for player IDs to reduce API calls
player_id_cache = {}
player_context_cache = {}
cache_timeout = 3600  # 1 hour cache timeout

def get_player_id(player_name):
    """Get MLB player ID from name with caching"""
    # Check cache first
    cache_key = f"player_id_{player_name}"
    cached_data = player_id_cache.get(cache_key)
    if cached_data and (time.time() - cached_data['timestamp']) < cache_timeout:
        return cached_data['player_id']
    
    try:
        response = requests.get(
            f"{MLB_STATS_API}/people/search", 
            params={"names": player_name},
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        
        player_id = None
        if data.get("people"):
            player_id = data["people"][0]["id"]
        
        # Cache the result
        player_id_cache[cache_key] = {
            'player_id': player_id,
            'timestamp': time.time()
        }
        
        return player_id
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching player ID for {player_name}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error getting player ID for {player_name}: {e}")
        return None

def get_opponent_context(player_id):
    """Get current opponent context for a player"""
    try:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        schedule_resp = requests.get(
            f"{MLB_STATS_API}/people/{player_id}/stats",
            params={
                "stats": "gameLog",
                "season": "2025",
                "group": "hitting"
            },
            timeout=10
        )
        schedule_resp.raise_for_status()
        data = schedule_resp.json()
        
        logs = data.get("stats", [{}])[0].get("splits", [])
        
        for log in logs:
            if log.get("date") == today:
                return (
                    log.get("team", {}).get("id"),
                    log.get("opponent", {}).get("id"),
                    log.get("pitcher", {}).get("hand", {}).get("code")
                )
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching opponent context for player {player_id}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error getting opponent context for player {player_id}: {e}")
        return None

def get_stat_mapping(stat_type):
    """Map prop bet stat types to MLB Stats API field names"""
    mapping = {
        # Batting stats
        "batter_hits": "hits",
        "batter_rbi": "rbi", 
        "batter_runs": "runs",
        "batter_home_runs": "homeRuns",
        "batter_total_bases": "totalBases",
        "batter_stolen_bases": "stolenBases",
        "batter_walks": "baseOnBalls",
        "batter_strikeouts": "strikeOuts",
        "batter_hits_runs_rbis": "hits_runs_rbis",  # Custom calculation
        "batter_fantasy_score": "fantasy_score",  # Custom calculation
        
        # Pitching stats
        "pitcher_strikeouts": "strikeOuts",
        "pitcher_hits_allowed": "hits",
        "pitcher_earned_runs": "earnedRuns",
        "pitcher_walks": "baseOnBalls",
        "pitcher_outs": "outs",
        
        # Legacy mappings
        "hits": "hits",
        "rbi": "rbi",
        "runs": "runs",
        "homeRuns": "homeRuns",
        "totalBases": "totalBases",
        "stolenBases": "stolenBases",
        "strikeOuts": "strikeOuts",
        "baseOnBalls": "baseOnBalls"
    }
    return mapping.get(stat_type, stat_type)

def calculate_custom_stat(game_data, stat_type):
    """Calculate custom composite stats"""
    if stat_type == "hits_runs_rbis":
        return game_data.get("hits", 0) + game_data.get("runs", 0) + game_data.get("rbi", 0)
    elif stat_type == "fantasy_score":
        # Basic fantasy scoring: 1B=1, 2B=2, 3B=3, HR=4, RBI=1, R=1, SB=2, BB=1
        hits = game_data.get("hits", 0)
        doubles = game_data.get("doubles", 0)
        triples = game_data.get("triples", 0)
        hrs = game_data.get("homeRuns", 0)
        singles = hits - doubles - triples - hrs
        
        return (singles * 1 + doubles * 2 + triples * 3 + hrs * 4 + 
                game_data.get("rbi", 0) + game_data.get("runs", 0) + 
                game_data.get("stolenBases", 0) * 2 + game_data.get("baseOnBalls", 0))
    return 0

def get_confidence_level(hit_rate, sample_size):
    """Determine confidence level based on hit rate and sample size"""
    if sample_size < 5:
        return "Low"
    elif hit_rate >= 0.60:
        return "High"
    elif hit_rate >= 0.50:
        return "Medium"
    else:
        return "Low"

def get_fallback_hit_rate(player_name, stat_type, threshold):
    """Generate fallback hit rate using basic heuristics"""
    try:
        # Basic fallback rates based on stat type and threshold
        fallback_rates = {
            "batter_hits": 0.35,
            "batter_rbi": 0.25,
            "batter_runs": 0.30,
            "batter_home_runs": 0.15,
            "batter_total_bases": 0.40,
            "batter_stolen_bases": 0.10,
            "batter_walks": 0.20,
            "batter_strikeouts": 0.60,
            "batter_hits_runs_rbis": 0.45,
            "batter_fantasy_score": 0.50,
            "pitcher_strikeouts": 0.55,
            "pitcher_hits_allowed": 0.45,
            "pitcher_earned_runs": 0.30,
            "pitcher_walks": 0.25,
            "pitcher_outs": 0.70,
            # Legacy mappings
            "hits": 0.35,
            "rbi": 0.25,
            "runs": 0.30,
            "homeRuns": 0.15,
            "totalBases": 0.40,
            "stolenBases": 0.10,
            "strikeOuts": 0.60,
            "baseOnBalls": 0.20
        }
        
        base_rate = fallback_rates.get(stat_type, 0.30)
        
        # Adjust based on threshold (higher threshold = lower hit rate)
        if threshold >= 5:
            base_rate *= 0.7
        elif threshold >= 3:
            base_rate *= 0.85
        elif threshold >= 1.5:
            base_rate *= 0.95
        
        return {
            "player": player_name,
            "stat": stat_type,
            "threshold": threshold,
            "hit_rate": round(base_rate, 2),
            "sample_size": 10,  # Assumed sample size for fallback
            "confidence": "Low",
            "note": "Fallback calculation - limited data available"
        }
    except Exception as e:
        logger.error(f"Error in fallback hit rate calculation: {e}")
        return {
            "player": player_name,
            "stat": stat_type,
            "threshold": threshold,
            "hit_rate": 0.30,  # Default fallback
            "sample_size": 0,
            "confidence": "Unknown",
            "error": "Fallback calculation failed"
        }

def get_contextual_hit_rate(player_name, stat_type, threshold=1):
    """Get contextual hit rate with comprehensive stat type support and fallback calculations"""
    try:
        player_id = get_player_id(player_name)
        if not player_id:
            return get_fallback_hit_rate(player_name, stat_type, threshold)

        context = get_opponent_context(player_id)
        if not context:
            return get_fallback_hit_rate(player_name, stat_type, threshold)
        
        team_id, opponent_id, pitcher_hand = context

        # Determine if it's a pitching or batting stat
        is_pitching_stat = stat_type.startswith("pitcher_")
        group = "pitching" if is_pitching_stat else "hitting"

        # Get game logs
        logs_resp = requests.get(
            f"{MLB_STATS_API}/people/{player_id}/stats",
            params={
                "stats": "gameLog",
                "season": "2025",
                "group": group
            },
            timeout=10
        )
        logs_resp.raise_for_status()
        logs_data = logs_resp.json()
        
        logs = logs_data.get("stats", [{}])[0].get("splits", [])

        # Filter for contextual games (same opponent and pitcher handedness)
        filtered = [
            game for game in logs[:10]
            if (game.get("opponent", {}).get("id") == opponent_id and
                game.get("pitcher", {}).get("hand", {}).get("code") == pitcher_hand)
        ]

        if not filtered:
            return get_fallback_hit_rate(player_name, stat_type, threshold)

        # Map stat type to API field name
        api_field = get_stat_mapping(stat_type)
        
        # Count games where player exceeded threshold
        over_count = 0
        for game in filtered:
            game_stat = game.get("stat", {})
            if api_field in ["hits_runs_rbis", "fantasy_score"]:
                stat_value = calculate_custom_stat(game_stat, api_field)
            else:
                stat_value = game_stat.get(api_field, 0)
            
            if stat_value >= threshold:
                over_count += 1
        
        hit_rate = round(over_count / len(filtered), 2) if filtered else 0.0

        return {
            "player": player_name,
            "stat": stat_type,
            "threshold": threshold,
            "hit_rate": hit_rate,
            "sample_size": len(filtered),
            "pitcher_hand": pitcher_hand,
            "opponent_id": opponent_id,
            "confidence": get_confidence_level(hit_rate, len(filtered))
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching contextual hit rate for {player_name}: {e}")
        return get_fallback_hit_rate(player_name, stat_type, threshold)
    except Exception as e:
        logger.error(f"Unexpected error in contextual hit rate for {player_name}: {e}")
        return get_fallback_hit_rate(player_name, stat_type, threshold)

def get_fantasy_hit_rate(player_name, threshold=6):
    """Get fantasy hit rate using safe calculation method"""
    try:
        # Import safe function from fantasy module
        from fantasy import safe_fantasy_hit_rate, get_player_id
        import requests
        
        player_id = get_player_id(player_name)
        if not player_id:
            print(f"[SKIP] Player ID not found for {player_name}")
            return get_fallback_hit_rate(player_name, "fantasy_score", threshold)

        # Get game logs safely
        logs_resp = requests.get(
            f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats",
            params={
                "stats": "gameLog",
                "season": "2025",
                "group": "hitting"
            },
            timeout=10
        )
        logs_resp.raise_for_status()
        logs_data = logs_resp.json()
        
        # Safe stats access
        stats_array = logs_data.get("stats", [])
        if not stats_array:
            print(f"[SKIP] No stats data for {player_name}")
            return get_fallback_hit_rate(player_name, "fantasy_score", threshold)
        
        logs = stats_array[0].get("splits", [])
        stat_data = {player_id: [game.get("stat", {}) for game in logs]}
        
        # Use safe hit rate calculation
        hit_rate = safe_fantasy_hit_rate(player_id, player_name, stat_data, "hits", 15)
        
        if hit_rate is not None:
            return {
                "player": player_name,
                "stat": "fantasy_score",
                "threshold": threshold,
                "hit_rate": hit_rate,
                "sample_size": len(stat_data[player_id]),
                "confidence": "Medium" if hit_rate > 0.5 else "Low"
            }
        else:
            return get_fallback_hit_rate(player_name, "fantasy_score", threshold)
            
    except Exception as e:
        print(f"[ERROR] Fantasy enrichment failed for {player_name}: {e}")
        logger.error(f"Fantasy hit rate error for {player_name}: {e}")
        return get_fallback_hit_rate(player_name, "fantasy_score", threshold)

def get_player_team_mapping():
    """Get current MLB player-to-team mapping"""
    try:
        # Try to load cached mapping first
        cache_file = "player_team_cache.json"
        try:
            with open(cache_file, "r") as f:
                cached_data = json.load(f)
                # Check if cache is less than 24 hours old
                if time.time() - cached_data.get("timestamp", 0) < 86400:
                    print(f"[INFO] Using cached player-team mapping ({len(cached_data.get('mapping', {}))} players)")
                    return cached_data.get("mapping", {})
        except FileNotFoundError:
            pass
        
        # Fetch fresh data from MLB Stats API
        print("[INFO] Fetching fresh player-team mapping from MLB Stats API...")
        teams_url = "https://statsapi.mlb.com/api/v1/teams?leagueIds=103,104"
        teams_response = requests.get(teams_url, timeout=10)
        teams_data = teams_response.json()
        
        player_team_map = {}
        
        for team in teams_data.get("teams", []):
            team_name = team.get("name", "")
            team_id = team.get("id")
            
            if not team_id:
                continue
                
            # Get roster for this team
            roster_url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=active"
            try:
                roster_response = requests.get(roster_url, timeout=5)
                roster_data = roster_response.json()
                
                for player_info in roster_data.get("roster", []):
                    player = player_info.get("person", {})
                    player_name = player.get("fullName", "")
                    if player_name:
                        player_team_map[player_name] = team_name
                        
            except Exception as e:
                print(f"[SKIP] Could not get roster for {team_name}: {e}")
                continue
        
        # Cache the mapping
        cache_data = {
            "mapping": player_team_map,
            "timestamp": time.time()
        }
        try:
            with open(cache_file, "w") as f:
                json.dump(cache_data, f)
        except Exception as e:
            print(f"[WARN] Could not cache player-team mapping: {e}")
        
        print(f"[INFO] Built player-team mapping for {len(player_team_map)} players")
        return player_team_map
        
    except Exception as e:
        print(f"[ERROR] Failed to build player-team mapping: {e}")
        return {}

# --- NEW: attach MLB player_id to each prop (non-breaking) ---
def _attach_player_ids_if_needed(props: list[dict], league: str) -> list[dict]:
    """
    Ensures each MLB prop includes a 'player_id' used by /api/l10-trend.
    No-op for non-MLB leagues. Does not alter existing fields.
    """
    if not props or (league or "").lower() != "mlb":
        return props

    for p in props:
        # Only fill if missing or empty
        if not p.get("player_id"):
            pid = resolve_mlb_player_id(p.get("player"))
            if pid:
                p["player_id"] = pid
    return props

# --- Legacy L10 stub to prevent crashes ---
def legacy_l10_trend(*args, **kwargs):
    raise RuntimeError("legacy_l10_trend is deprecated; use trends_l10.compute_l10()")

