import logging
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

MLB_STATS_API = "https://statsapi.mlb.com/api/v1"

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
    except Exception as e:
        logger.error(f"Error fetching player ID for {player_name}: {e}")
        return None

def calculate_fantasy_points(game_stats):
    """Calculate fantasy points based on standard scoring system"""
    try:
        # Standard fantasy scoring
        points = 0
        
        # Hitting stats
        points += game_stats.get("hits", 0) * 3
        points += game_stats.get("doubles", 0) * 2  # +2 bonus for doubles
        points += game_stats.get("triples", 0) * 5  # +5 bonus for triples
        points += game_stats.get("homeRuns", 0) * 4  # +4 bonus for home runs
        points += game_stats.get("runs", 0) * 2
        points += game_stats.get("rbi", 0) * 2
        points += game_stats.get("stolenBases", 0) * 5
        points += game_stats.get("baseOnBalls", 0) * 2
        points += game_stats.get("hitByPitch", 0) * 2
        
        return points
    except Exception as e:
        logger.error(f"Error calculating fantasy points: {e}")
        return 0

def safe_fantasy_hit_rate(player_id, player_name, stat_data, stat_key="hits", window=10):
    """
    Safely calculate hit rate for a player over the last `window` games.
    Skips players with insufficient data or broken stat entries.

    :param player_id: str - unique ID used to look up stats
    :param player_name: str - used for logging
    :param stat_data: dict - full player stats from MLB Stats API
    :param stat_key: str - the key to check (e.g. "hits", "strikeouts")
    :param window: int - number of games to average over
    :return: float or None
    """
    try:
        if player_id not in stat_data:
            print(f"[SKIP] No stats found for {player_name}")
            return None

        games = stat_data[player_id][-window:]
        if len(games) < 5:
            print(f"[SKIP] Not enough games for {player_name}")
            return None

        total = sum(1 for g in games if g.get(stat_key, 0) > 0)
        return round(total / len(games), 2)

    except Exception as e:
        print(f"[ERROR] Hit rate failed for {player_name}: {e}")
        return None

def get_fantasy_hit_rate(player_name, threshold=6):
    """Get fantasy hit rate for a player with safe error handling"""
    try:
        player_id = get_player_id(player_name)
        if not player_id:
            print(f"[SKIP] Player ID not found for {player_name}")
            return {"error": f"Player '{player_name}' not found"}

        # Get game logs with safe API access
        logs_resp = requests.get(
            f"{MLB_STATS_API}/people/{player_id}/stats",
            params={
                "stats": "gameLog",
                "season": "2025",
                "group": "hitting"
            },
            timeout=10
        )
        logs_resp.raise_for_status()
        logs_data = logs_resp.json()
        
        # Safe access to stats array
        stats_array = logs_data.get("stats", [])
        if not stats_array:
            print(f"[SKIP] No stats data available for {player_name}")
            return {
                "error": "No stats data available",
                "player": player_name,
                "threshold": threshold,
                "sample_size": 0
            }
        
        logs = stats_array[0].get("splits", [])
        
        # Use safe hit rate calculation
        stat_data = {player_id: [game.get("stat", {}) for game in logs]}
        hit_rate = safe_fantasy_hit_rate(player_id, player_name, stat_data, "hits", 15)
        
        if hit_rate is None:
            return {
                "error": "Insufficient data for reliable calculation",
                "player": player_name,
                "threshold": threshold,
                "sample_size": len(logs) if logs else 0
            }

        return {
            "player": player_name,
            "threshold": threshold,
            "fantasy_hit_rate": hit_rate,
            "sample_size": len(stat_data[player_id]),
            "games_over": int(hit_rate * len(stat_data[player_id]))
        }
        
    except Exception as e:
        print(f"[ERROR] Fantasy calculation failed for {player_name}: {e}")
        logger.error(f"Error calculating fantasy hit rate for {player_name}: {e}")
        return {"error": f"Failed to calculate fantasy hit rate: {str(e)}"}
