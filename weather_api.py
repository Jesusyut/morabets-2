"""
Weather API Integration for Enhanced Prop Enrichment
"""
import requests
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Enhanced Enrichment: Weather Context Integration
def get_weather_multiplier(city, stat_type):
    """Get weather impact multiplier for batting props based on conditions"""
    try:
        # This is a simplified weather analysis
        # In production, you would integrate with OpenWeatherMap API:
        # api_key = os.getenv("WEATHER_API_KEY")
        # url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
        
        # For now, return basic weather assumptions for major cities
        weather_conditions = {
            "Denver": {"temp": 75, "wind_speed": 8, "wind_direction": "out"},  # Coors Field
            "Boston": {"temp": 68, "wind_speed": 12, "wind_direction": "out"}, # Fenway
            "New York": {"temp": 72, "wind_speed": 6, "wind_direction": "variable"}, # Yankee Stadium
            "San Francisco": {"temp": 58, "wind_speed": 15, "wind_direction": "in"}, # Oracle Park
            "Miami": {"temp": 82, "wind_speed": 5, "wind_direction": "calm"}, # Marlins Park
        }
        
        conditions = weather_conditions.get(city, {"temp": 72, "wind_speed": 5, "wind_direction": "calm"})
        
        multiplier = 1.0
        
        # Home Run and Total Bases boosts
        if "home_runs" in stat_type or "total_bases" in stat_type:
            # Hot weather helps ball carry
            if conditions["temp"] > 80:
                multiplier *= 1.05
            elif conditions["temp"] < 55:
                multiplier *= 0.95
            
            # Wind direction impact
            if conditions["wind_direction"] == "out" and conditions["wind_speed"] > 10:
                multiplier *= 1.08  # Strong tailwind
            elif conditions["wind_direction"] == "in" and conditions["wind_speed"] > 10:
                multiplier *= 0.92  # Strong headwind
        
        return multiplier
        
    except Exception as e:
        logger.debug(f"Weather analysis error for {city}: {e}")
        return 1.0

# Enhanced Enrichment: Implied Run Total Analysis
def get_implied_run_total_multiplier(team_odds, stat_type):
    """Calculate team run expectancy multiplier based on moneyline odds"""
    try:
        # Convert American odds to implied probability
        if team_odds > 0:
            implied_prob = 100 / (team_odds + 100)
        else:
            implied_prob = abs(team_odds) / (abs(team_odds) + 100)
        
        # Estimate implied team total (simplified model)
        # Strong favorites (~65%+) typically score more runs
        if implied_prob >= 0.65:
            return 1.06  # 6% boost for strong favorites
        elif implied_prob >= 0.55:
            return 1.03  # 3% boost for moderate favorites
        elif implied_prob <= 0.35:
            return 0.95  # 5% reduction for heavy underdogs
        
        return 1.0
        
    except Exception as e:
        logger.debug(f"Implied run total error for odds {team_odds}: {e}")
        return 1.0

# Enhanced Enrichment: Line Movement Analysis
def detect_steam_move(opening_odds, current_odds):
    """Detect if line has moved significantly (steam/sharp action)"""
    try:
        if not opening_odds or not current_odds:
            return False, 0
        
        # Calculate percentage move
        if opening_odds > 0 and current_odds > 0:
            pct_change = (current_odds - opening_odds) / opening_odds
        elif opening_odds < 0 and current_odds < 0:
            pct_change = (abs(current_odds) - abs(opening_odds)) / abs(opening_odds)
        else:
            # Odds changed signs, significant move
            return True, 1.0
        
        # 15% move in 12 hours indicates steam
        steam_detected = abs(pct_change) >= 0.15
        
        return steam_detected, pct_change
        
    except Exception as e:
        logger.debug(f"Steam detection error: {e}")
        return False, 0