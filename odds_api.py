import requests
from datetime import datetime, timedelta
import os
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from copy import deepcopy
from typing import Dict, Any, List, Tuple, Optional
from contextual import get_contextual_hit_rate
from fantasy import get_fantasy_hit_rate
from probability import (
    american_to_implied,
    fair_probs_from_two_sided,
    fair_odds_from_prob,
)

logger = logging.getLogger(__name__)

BASE = "https://api.the-odds-api.com"
API_KEY = os.getenv("ODDS_API_KEY", "").strip()

# bookmaker *keys* (lowercase). Override via ODDS_BOOKMAKERS env if needed.
PREFERRED_BOOKMAKER_KEYS: List[str] = [
    b.strip() for b in (os.getenv("ODDS_BOOKMAKERS") or
                        "fanduel,draftkings,betmgm,caesars,pointsbetus").split(",")
    if b.strip()
]

DEBUG_PROB = os.getenv("DEBUG_PROB", "0") == "1"
def _dbg(*args):
    if DEBUG_PROB:
        print("[FAIR]", *args)

def _norm_point(val) -> Optional[str]:
    """Normalize prop line so '0.5' pairs with '0.50' (3 dp string)."""
    if val is None:
        return None
    try:
        return f"{Decimal(str(val)).quantize(Decimal('0.001'))}"
    except (InvalidOperation, ValueError, TypeError):
        s = str(val).strip()
        return s if s else None

def _resolve_side_and_player(name: Optional[str], desc: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Books flip fields; return ('over'|'under', player_name).
    """
    n = (name or "").strip(); d = (desc or "").strip()
    ln, ld = n.lower(), d.lower()
    if ln in ("over","under"): return ln, d or n
    if ld in ("over","under"): return ld, n or d
    if "over" in ln:  return "over",  d or n
    if "under" in ln: return "under", d or n
    if "over" in ld:  return "over",  n or d
    if "under" in ld: return "under", n or d
    return None, None

def _pair_outcomes(bookmakers: List[Dict[str, Any]], stat_key: str) -> Dict[Tuple[str, str, Optional[str]], Dict[str, Optional[Dict[str, Any]]]]:
    """
    (player, stat, point_key) -> { 'over': {price, book} | None, 'under': {...} | None }
    Prefer FanDuel if duplicates.
    """
    sidebook = defaultdict(lambda: {"over": None, "under": None})
    for bk in bookmakers or []:
        book_name = (bk.get("key") or bk.get("title") or "").strip().lower()
        for m in bk.get("markets") or []:
            if m.get("key") != stat_key:
                continue
            for o in m.get("outcomes") or []:
                side, player = _resolve_side_and_player(o.get("name"), o.get("description"))
                if side not in ("over","under") or not player:
                    continue
                price = o.get("price")
                if price is None:
                    continue
                point = _norm_point(o.get("point"))
                key = (player, stat_key, point)
                keep = sidebook[key][side] is None or sidebook[key][side].get("book") != "fanduel"
                if keep:
                    sidebook[key][side] = {"price": int(price), "book": book_name}
    return sidebook

def _attach_fair_or_implied(row: Dict[str, Any]) -> None:
    """
    1) both sides -> no-vig probs
    2) one side   -> implied from that side (other = 1-p)
    3) fallback   -> implied from generic 'odds'
    """
    shop = row.get("shop") or {}
    over_am  = (shop.get("over")  or {}).get("american")
    under_am = (shop.get("under") or {}).get("american")
    fallback = row.get("odds")

    row.setdefault("fair", {})
    row["fair"].setdefault("prob", {"over": 0.0, "under": 0.0})
    _dbg("row", row.get("player"), row.get("stat"), row.get("line"), "over=", over_am, "under=", under_am, "fb=", fallback)

    if over_am is not None and under_am is not None:
        p_over, p_under = fair_probs_from_two_sided(over_am, under_am)
        if p_over is not None and p_under is not None:
            row["fair"]["prob"]["over"]  = round(float(p_over), 4)
            row["fair"]["prob"]["under"] = round(float(p_under), 4)
            row["fair"]["american"] = {
                "over":  fair_odds_from_prob(p_over),
                "under": fair_odds_from_prob(p_under),
            }
            row["fair"]["book"] = (shop.get("over") or {}).get("book") or (shop.get("under") or {}).get("book") or (row.get("bookmaker") or "")
            return

    if over_am is not None and under_am is None:
        p = american_to_implied(over_am)
        row["fair"]["prob"]["over"]  = round(p, 4)
        row["fair"]["prob"]["under"] = round(1.0 - p, 4)
        row["fair"]["book"] = (shop.get("over") or {}).get("book") or (row.get("bookmaker") or "")
        return

    if under_am is not None and over_am is None:
        p = american_to_implied(under_am)
        row["fair"]["prob"]["under"] = round(p, 4)
        row["fair"]["prob"]["over"]  = round(1.0 - p, 4)
        row["fair"]["book"] = (shop.get("under") or {}).get("book") or (row.get("bookmaker") or "")
        return

    if fallback is not None:
        p = american_to_implied(fallback)
        row["fair"]["prob"]["over"]  = round(p, 4)
        row["fair"]["prob"]["under"] = round(1.0 - p, 4)
        row["fair"]["book"] = row.get("bookmaker") or ""
        return

def _event_odds(event_id: str, markets: List[str]) -> Dict[str, Any]:
    """
    Try with bookmaker KEYS first, then fallback without the filter.
    """
    base_params = {
        "apiKey": API_KEY, "regions": "us", "oddsFormat": "american",
        "markets": ",".join(markets),
    }
    params = dict(base_params)
    if PREFERRED_BOOKMAKER_KEYS:
        params["bookmakers"] = ",".join(PREFERRED_BOOKMAKER_KEYS)
    r = requests.get(f"{BASE}/v4/sports/baseball_mlb/events/{event_id}/odds", params=params, timeout=20)
    r.raise_for_status()
    data = r.json() or {}
    if not (data.get("bookmakers") or []):
        r2 = requests.get(f"{BASE}/v4/sports/baseball_mlb/events/{event_id}/odds", params=base_params, timeout=20)
        r2.raise_for_status()
        data = r2.json() or {}
    return data

def get_favored_team(game):
    """
    Determine the favored team based on moneyline odds
    Lower odds = favored team (e.g., -140 is favored over +120)
    """
    home_odds = game.get("home_odds")
    away_odds = game.get("away_odds")
    
    if home_odds is None or away_odds is None:
        return None  # Can't calculate favored team
        
    # Convert odds to numerical values for comparison
    # Negative odds are favorites, positive odds are underdogs
    home_team = game.get("home_team")
    away_team = game.get("away_team")
    
    # Lower odds value = favorite
    if home_odds < away_odds:
        return home_team
    else:
        return away_team

def parse_game_data():
    """Fetch moneylines with preferred sportsbooks first, fallback to all if needed"""
    now = datetime.utcnow()
    future = now + timedelta(hours=48)
    start_time = now.replace(microsecond=0).isoformat() + "Z"
    end_time = future.replace(microsecond=0).isoformat() + "Z"

    if not ODDS_API_KEY:
        print("[ERROR] ODDS_API_KEY is not set")
        return []

    # Try preferred sportsbooks first
    try:
        print(f"[DEBUG] Fetching moneylines from preferred sportsbooks: {PREFERRED_SPORTSBOOKS}")
        response = requests.get(
            f"{BASE_URL}/sports/baseball_mlb/odds",
            params={
                "apiKey": ODDS_API_KEY,
                "regions": "us",
                "markets": "h2h",
                "oddsFormat": "american",
                "commenceTimeFrom": start_time,
                "commenceTimeTo": end_time,
                "bookmakers": ",".join(PREFERRED_SPORTSBOOKS)
            },
            timeout=20
        )
        response.raise_for_status()
        data = response.json()
        print(f"[INFO] Retrieved {len(data)} moneyline matchups from preferred sportsbooks")
        
        # If we got good data, return it
        if data and len(data) > 0:
            return data
        else:
            print("[WARNING] No moneylines from preferred sportsbooks, falling back to all sportsbooks")
            
    except Exception as e:
        print(f"[ERROR] Failed to fetch odds from preferred sportsbooks: {e}, falling back to all sportsbooks")

    # Fallback to all sportsbooks
    try:
        print("[DEBUG] Fetching moneylines from all sportsbooks")
        response = requests.get(
            f"{BASE_URL}/sports/baseball_mlb/odds",
            params={
                "apiKey": ODDS_API_KEY,
                "regions": "us",
                "markets": "h2h",
                "oddsFormat": "american",
                "commenceTimeFrom": start_time,
                "commenceTimeTo": end_time
            },
            timeout=20
        )
        response.raise_for_status()
        data = response.json()
        print(f"[INFO] Retrieved {len(data)} moneyline matchups from all sportsbooks")
        return data
    except Exception as e:
        print(f"[ERROR] Failed to fetch odds from all sportsbooks: {e}")
        return []

def get_matchup_map():
    """Get today's games with accurate team matchups from Odds API"""
    from team_abbreviations import TEAM_ABBREVIATIONS
    
    now = datetime.utcnow()
    future = now + timedelta(hours=48)
    start_time = now.replace(microsecond=0).isoformat() + "Z"
    end_time = future.replace(microsecond=0).isoformat() + "Z"

    if not ODDS_API_KEY:
        print("[ERROR] ODDS_API_KEY is not set")
        return {}

    try:
        response = requests.get(
            f"{BASE_URL}/sports/baseball_mlb/odds",
            params={
                "apiKey": ODDS_API_KEY,
                "regions": "us",
                "markets": "h2h",
                "oddsFormat": "american",
                "commenceTimeFrom": start_time,
                "commenceTimeTo": end_time,
                "bookmakers": ",".join(PREFERRED_SPORTSBOOKS)
            },
            timeout=20
        )
        response.raise_for_status()
        games = response.json()
        
        matchup_map = {}
        for game in games:
            home_team = game.get("home_team", "")
            away_team = game.get("away_team", "")
            game_id = game.get("id", "")
            
            # Convert team names to abbreviations
            home_abbr = TEAM_ABBREVIATIONS.get(home_team, home_team)
            away_abbr = TEAM_ABBREVIATIONS.get(away_team, away_team)
            
            matchup_str = f"{away_abbr} @ {home_abbr}"
            matchup_map[matchup_str] = {
                "teams": [home_abbr, away_abbr],
                "game_id": game_id,
                "home_team": home_team,
                "away_team": away_team
            }
        
        print(f"[INFO] Built matchup map with {len(matchup_map)} games: {list(matchup_map.keys())}")
        return matchup_map
        
    except Exception as e:
        print(f"[ERROR] Failed to build matchup map: {e}")
        return {}

def get_mlb_totals_odds():
    """Fetch over/under totals odds for MLB games"""
    now = datetime.utcnow()
    future = now + timedelta(hours=48)
    start_time = now.replace(microsecond=0).isoformat() + "Z"
    end_time = future.replace(microsecond=0).isoformat() + "Z"

    if not ODDS_API_KEY:
        print("[ERROR] ODDS_API_KEY is not set")
        return []

    try:
        print("[DEBUG] Fetching MLB totals odds")
        response = requests.get(
            f"{BASE_URL}/sports/baseball_mlb/odds",
            params={
                "apiKey": ODDS_API_KEY,
                "regions": "us",
                "markets": "totals",
                "oddsFormat": "american",
                "commenceTimeFrom": start_time,
                "commenceTimeTo": end_time,
                "bookmakers": ",".join(PREFERRED_SPORTSBOOKS)
            },
            timeout=20
        )
        response.raise_for_status()
        data = response.json()
        print(f"[INFO] Retrieved totals odds for {len(data)} MLB games")
        return data
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch totals odds: {e}")
        return []

def get_mlb_game_environment_map():
    """Get environment classification and favored team for each MLB game"""
    from mlb_game_enrichment import classify_game_environment
    from team_abbreviations import TEAM_ABBREVIATIONS
    
    totals_data = get_mlb_totals_odds()
    moneyline_data = parse_game_data()  # Get moneylines for favored team calculation
    env_map = {}
    
    # Create a lookup for moneyline odds by team matchup
    moneyline_lookup = {}
    for game in moneyline_data:
        home_team = game.get("home_team", "")
        away_team = game.get("away_team", "")
        
        if home_team and away_team:
            home_abbr = TEAM_ABBREVIATIONS.get(home_team, home_team)
            away_abbr = TEAM_ABBREVIATIONS.get(away_team, away_team)
            matchup_key = f"{away_abbr} @ {home_abbr}"
            
            # Extract moneyline odds
            for bookmaker in game.get("bookmakers", []):
                for market in bookmaker.get("markets", []):
                    if market.get("key") == "h2h":  # head-to-head (moneyline)
                        outcomes = market.get("outcomes", [])
                        
                        home_odds = None
                        away_odds = None
                        
                        for outcome in outcomes:
                            if outcome.get("name") == home_team:
                                home_odds = outcome.get("price")
                            elif outcome.get("name") == away_team:
                                away_odds = outcome.get("price")
                        
                        if home_odds and away_odds:
                            # Determine favored team
                            favored_team = home_abbr if home_odds < away_odds else away_abbr
                            
                            moneyline_lookup[matchup_key] = {
                                "home_odds": home_odds,
                                "away_odds": away_odds,
                                "favored_team": favored_team
                            }
                            break
                if matchup_key in moneyline_lookup:
                    break

    for game in totals_data:
        try:
            home_team = game.get("home_team", "")
            away_team = game.get("away_team", "")
            
            if not home_team or not away_team:
                continue
                
            # Convert to abbreviations
            home_abbr = TEAM_ABBREVIATIONS.get(home_team, home_team)
            away_abbr = TEAM_ABBREVIATIONS.get(away_team, away_team)
            matchup_key = f"{away_abbr} @ {home_abbr}"
                
            # Find totals market in bookmakers
            for bookmaker in game.get("bookmakers", []):
                for market in bookmaker.get("markets", []):
                    if market.get("key") == "totals":
                        outcomes = market.get("outcomes", [])
                        
                        total_point = None
                        over_odds = None
                        under_odds = None
                        
                        for outcome in outcomes:
                            if outcome.get("name") == "Over":
                                total_point = outcome.get("point")
                                over_odds = outcome.get("price")
                            elif outcome.get("name") == "Under":
                                under_odds = outcome.get("price")
                        
                        if total_point and over_odds and under_odds:
                            label = classify_game_environment(total_point, over_odds, under_odds)
                            
                            # Get favored team from moneyline lookup
                            moneyline_info = moneyline_lookup.get(matchup_key, {})
                            favored_team = moneyline_info.get("favored_team")
                            
                            env_map[matchup_key] = {
                                "environment": label,
                                "total": total_point,
                                "over_odds": over_odds,
                                "under_odds": under_odds,
                                "favored_team": favored_team,
                                "home_team": home_abbr,
                                "away_team": away_abbr
                            }
                            
                            fav_indicator = f" (Fav: {favored_team})" if favored_team else ""
                            print(f"[ENV] {matchup_key}: {label} (Total: {total_point}){fav_indicator}")
                            break
                if matchup_key in env_map:
                    break
                    
        except Exception as e:
            logger.debug(f"Error processing game environment for {game}: {e}")
            continue

    print(f"[INFO] Classified {len(env_map)} game environments with favored teams")
    return env_map

    # Try preferred sportsbooks first
    try:
        print(f"[DEBUG] Fetching moneylines from preferred sportsbooks: {PREFERRED_SPORTSBOOKS}")
        response = requests.get(
            f"{BASE_URL}/sports/baseball_mlb/odds",
            params={
                "apiKey": ODDS_API_KEY,
                "regions": "us",
                "markets": "h2h",
                "oddsFormat": "american",
                "commenceTimeFrom": start_time,
                "commenceTimeTo": end_time,
                "bookmakers": ",".join(PREFERRED_SPORTSBOOKS)
            },
            timeout=20
        )
        response.raise_for_status()
        data = response.json()
        print(f"[INFO] Retrieved {len(data)} moneyline matchups from preferred sportsbooks")
        
        # If we got good data, return it
        if data and len(data) > 0:
            return data
        else:
            print("[WARNING] No moneylines from preferred sportsbooks, falling back to all sportsbooks")
            
    except Exception as e:
        print(f"[ERROR] Failed to fetch odds from preferred sportsbooks: {e}, falling back to all sportsbooks")

    # Fallback to all sportsbooks
    try:
        print("[DEBUG] Fetching moneylines from all sportsbooks")
        response = requests.get(
            f"{BASE_URL}/sports/baseball_mlb/odds",
            params={
                "apiKey": ODDS_API_KEY,
                "regions": "us",
                "markets": "h2h",
                "oddsFormat": "american",
                "commenceTimeFrom": start_time,
                "commenceTimeTo": end_time
            },
            timeout=20
        )
        response.raise_for_status()
        data = response.json()
        print(f"[INFO] Retrieved {len(data)} moneyline matchups from all sportsbooks")
        return data
    except Exception as e:
        print(f"[ERROR] Failed to fetch odds from all sportsbooks: {e}")
        return []

def fetch_player_props():
    """Fetch player props with preferred sportsbooks first, fallback to all if needed"""
    now = datetime.utcnow()
    future = now + timedelta(hours=48)
    start_time = now.replace(microsecond=0).isoformat() + "Z"
    end_time = future.replace(microsecond=0).isoformat() + "Z"

    if not ODDS_API_KEY:
        print("[ERROR] ODDS_API_KEY is not set")
        return []

    try:
        event_resp = requests.get(
            f"{BASE_URL}/sports/baseball_mlb/events",
            params={
                "apiKey": ODDS_API_KEY,
                "commenceTimeFrom": start_time,
                "commenceTimeTo": end_time
            },
            timeout=20
        )
        event_resp.raise_for_status()
        events = event_resp.json()
        print(f"[INFO] Found {len(events)} events")
    except Exception as e:
        print(f"[ERROR] Failed to fetch MLB events: {e}")
        return []

    # Return flat list for backward compatibility
    all_props = []
    print(f"[DEBUG] Starting prop collection for {len(events)} events")
    
    # Define available markets only (7 markets total) - confirmed working with API
    markets_batch_1 = ["batter_hits", "batter_home_runs", "batter_total_bases"]
    markets_batch_2 = ["pitcher_strikeouts", "pitcher_earned_runs", "pitcher_outs", "pitcher_hits_allowed"]
    
    print(f"[DEBUG] Using verified markets: {markets_batch_1 + markets_batch_2}")
    
    all_markets = [markets_batch_1, markets_batch_2]

    for event in events:
        eid = event.get("id")
        if not eid:
            continue

        # Create matchup key from event data
        home_team = event.get("home_team", "Unknown")
        away_team = event.get("away_team", "Unknown")
        matchup_key = f"{away_team} @ {home_team}"

        # Reset per-event aggregator
        sidebook = defaultdict(lambda: {"over": None, "under": None})

        # Process each market batch to avoid rate limiting
        for batch_idx, markets in enumerate(all_markets):
            try:
                # Add delay between batches to respect rate limits
                if batch_idx > 0:
                    time.sleep(1)
                
                data = _event_odds(eid, markets)
                
                # Log successful market response
                if data.get("bookmakers"):
                    successful_markets = [m.get('key') for m in data.get('bookmakers', [])[0].get('markets', [])]
                    print(f"[DEBUG] Event {eid} batch {batch_idx} fetched props for markets: {successful_markets}")
                
                # Use the new helper function to pair outcomes
                for stat_key in markets:
                    batch_sidebook = _pair_outcomes(data.get("bookmakers", []), stat_key)
                    for key, sides in batch_sidebook.items():
                        if sides["over"] or sides["under"]:
                            sidebook[key] = sides
                                
            except Exception as e:
                print(f"[ERROR] Failed to fetch props for event {eid} batch {batch_idx}: {e}")
                continue

        # After scanning the event, build rows with fair odds calculation
        props_for_matchup = []

        for (player, stat_key, point), sides in sidebook.items():
            # sides = {"over": {...} or None, "under": {...} or None}
            over  = sides.get("over")
            under = sides.get("under")

            row = {
                "player": player,
                "stat":   stat_key,
                "line":   point,
            }

            # Populate shop (used for no-vig) when we have either side
            if over or under:
                row["shop"] = {}
                if over:
                    row["shop"]["over"] = {"american": int(over["price"]), "book": over["book"]}
                if under:
                    row["shop"]["under"] = {"american": int(under["price"]), "book": under["book"]}

            # Always provide a generic fallback price & book for implied calc
            row["bookmaker"] = (over or under or {}).get("book")
            row["odds"]      = (over or under or {}).get("price")

            # compute probabilities FIRST (so we have something to protect)
            _attach_fair_or_implied(row)

            # Append to the list
            props_for_matchup.append(row)

        # Add props to the flat list for backward compatibility
        all_props.extend(props_for_matchup)
        
        print(f"[DEBUG] Event {eid} ({matchup_key}): Collected {len(props_for_matchup)} props")

    print(f"[INFO] Final count of props: {len(all_props)}")
    print(f"[DEBUG] Final props fetched: {len(all_props)}")
    print(f"🔍 DEBUG: Fetched {len(all_props)} raw props from API")
    
    # Debug: Show stat type breakdown
    stat_counts = {}
    for prop in all_props:
        stat = prop.get('stat', 'unknown')
        stat_counts[stat] = stat_counts.get(stat, 0) + 1
    
    print(f"[DEBUG] Props by stat type: {stat_counts}")
    return all_props

def deduplicate_props(props):
    """Deduplicate props: keep one prop per unique player+stat+line combination"""
    unique_props = {}
    
    for prop in props:
        # Create unique key for each player+stat+line combination
        key = f"{prop['player']}_{prop['stat']}_{prop['line']}"
        
        # If this is the first occurrence or has better odds, keep it
        if key not in unique_props:
            unique_props[key] = prop
        else:
            # Keep the prop with better odds (higher absolute value for positive odds)
            current_odds = unique_props[key]['odds']
            new_odds = prop['odds']
            
            # For positive odds, higher is better; for negative odds, closer to 0 is better
            if (current_odds > 0 and new_odds > current_odds) or (current_odds < 0 and new_odds > current_odds):
                unique_props[key] = prop
    
    deduplicated = list(unique_props.values())
    print(f"[INFO] Deduplication: {len(props)} props -> {len(deduplicated)} unique props")
    return deduplicated

def enrich_prop(prop):
    """Enrich a single prop with contextual and fantasy hit rates - with robust error handling"""
    try:
        # Get contextual hit rate with fallback
        contextual = None
        try:
            contextual = get_contextual_hit_rate(
                prop["player"], 
                stat_type=prop["stat"], 
                threshold=prop["line"]
            )
        except Exception as e:
            print(f"[WARN] Contextual hit rate error for {prop['player']}: {e}")
            contextual = {
                "player": prop["player"],
                "stat": prop["stat"],
                "threshold": prop["line"],
                "hit_rate": None,
                "confidence": "Unknown",
                "error": f"Contextual calculation failed: {str(e)}"
            }
        
        # Ensure we always have a contextual object
        if not contextual or contextual.get("error"):
            contextual = {
                "player": prop["player"],
                "stat": prop["stat"],
                "threshold": prop["line"],
                "hit_rate": 0.30,  # Default fallback
                "confidence": "Low",
                "note": "Using fallback hit rate"
            }
        
        # Enhanced Enrichment: Apply pro-level betting context multipliers
        try:
            from enrichment import (apply_park_factor, get_recent_form_multiplier, 
                                  get_bullpen_fatigue_multiplier, get_lineup_position_multiplier,
                                  get_player_id)
            
            base_hit_rate = contextual.get("hit_rate", 0.30)
            enhanced_multiplier = 1.0
            enhancement_factors = []
            
            # Park Factor Analysis
            stadium = prop.get("venue", "")
            if stadium:
                park_multiplier = apply_park_factor(prop, stadium)
                if park_multiplier != 1.0:
                    enhanced_multiplier *= park_multiplier
                    enhancement_factors.append(f"Park: {park_multiplier:.2f}")
            
            # Recent Form Analysis
            player_id = get_player_id(prop["player"])
            if player_id:
                form_multiplier = get_recent_form_multiplier(player_id, prop["stat"])
                if form_multiplier != 1.0:
                    enhanced_multiplier *= form_multiplier
                    enhancement_factors.append(f"Form: {form_multiplier:.2f}")
            
            # Bullpen Fatigue Context
            opponent_team = prop.get("opponent_team", "")
            if opponent_team:
                bullpen_multiplier = get_bullpen_fatigue_multiplier(opponent_team)
                if bullpen_multiplier != 1.0:
                    enhanced_multiplier *= bullpen_multiplier
                    enhancement_factors.append(f"Bullpen: {bullpen_multiplier:.2f}")
            
            # Lineup Position Influence
            lineup_multiplier = get_lineup_position_multiplier(prop["player"])
            if lineup_multiplier != 1.0:
                enhanced_multiplier *= lineup_multiplier
                enhancement_factors.append(f"Lineup: {lineup_multiplier:.2f}")
            
            # Apply enhanced multiplier to hit rate (cap between 0.05 and 0.95)
            if isinstance(base_hit_rate, (int, float)) and base_hit_rate > 0:
                enhanced_hit_rate = min(0.95, max(0.05, base_hit_rate * enhanced_multiplier))
                
                # Update contextual data with enhanced analysis
                contextual["enhanced_hit_rate"] = round(enhanced_hit_rate, 3)
                contextual["enhancement_multiplier"] = round(enhanced_multiplier, 3)
                contextual["enhancement_factors"] = enhancement_factors
                contextual["original_hit_rate"] = base_hit_rate
                
                if enhancement_factors:
                    print(f"[ENHANCED] {prop['player']}: {base_hit_rate:.2f} -> {enhanced_hit_rate:.2f} ({', '.join(enhancement_factors)})")
            
        except Exception as enhancement_error:
            print(f"[DEBUG] Enhanced enrichment failed for {prop['player']}: {enhancement_error}")
            # Continue with basic contextual data if enhancement fails
        
        # Get fantasy hit rate with fallback
        fantasy = None
        try:
            fantasy = get_fantasy_hit_rate(prop["player"], threshold=prop["line"])
        except Exception as e:
            print(f"[WARN] Fantasy hit rate error for {prop['player']}: {e}")
            fantasy = {
                "player": prop["player"],
                "threshold": prop["line"],
                "hit_rate": 0.35,  # Default fallback
                "confidence": "Low",
                "note": "Using fallback fantasy rate"
            }
        
        # Ensure we always have a fantasy object
        if not fantasy:
            fantasy = {
                "player": prop["player"],
                "threshold": prop["line"],
                "hit_rate": 0.35,  # Default fallback
                "confidence": "Low",
                "note": "Using fallback fantasy rate"
            }
        
        # Preserve existing fair odds if already computed (don't clobber)
        existing_fair = prop.get("fair")
        if existing_fair and existing_fair.get("prob"):
            prob = existing_fair["prob"]
            if prob.get("over", 0.0) != 0.0 or prob.get("under", 0.0) != 0.0:
                # Fair odds already computed, preserve them
                pass
            else:
                # Fair odds were computed but are 0/0, try to recompute
                try:
                    from probability import fair_probs_from_two_sided, fair_odds_from_prob
                    
                    def _attach_fair(prop, over_price=None, under_price=None, home_price=None, away_price=None, fav_price=None, dog_price=None):
                        def set_fair(pA, pB, sideA, sideB):
                            if pA is None: return
                            prop.setdefault("fair", {})
                            prop["fair"]["prob"] = { sideA: round(pA,4), sideB: round(pB,4) }
                            prop["fair"]["american"] = {
                                sideA: fair_odds_from_prob(pA),
                                sideB: fair_odds_from_prob(pB),
                            }

                        # Totals (Over/Under)
                        if over_price is not None and under_price is not None:
                            p_over, p_under = fair_probs_from_two_sided(float(over_price), float(under_price))
                            set_fair(p_over, p_under, "over", "under")
                            return

                        # Moneyline (Home/Away)
                        if home_price is not None and away_price is not None:
                            p_home, p_away = fair_probs_from_two_sided(float(home_price), float(away_price))
                            set_fair(p_home, p_away, "home", "away")
                            return

                        # Spread (Fav/Dog)
                        if fav_price is not None and dog_price is not None:
                            p_fav, p_dog = fair_probs_from_two_sided(float(fav_price), float(dog_price))
                            set_fair(p_fav, p_dog, "favorite", "underdog")
                            return

                    # Extract existing odds from current structure and attach fair probabilities
                    shop = prop.get("shop") or {}
                    over_am = shop.get("over", {}).get("american")
                    under_am = shop.get("under", {}).get("american")
                    _attach_fair(prop, over_price=over_am, under_price=under_am)
                    
                except Exception as fair_error:
                    print(f"[DEBUG] Fair probability calculation failed for {prop.get('player', 'Unknown')}: {fair_error}")
                    # Continue without fair probabilities if calculation fails
        
        # Return enriched prop
        return {
            **prop,
            "contextual_hit_rate": contextual,
            "fantasy_hit_rate": fantasy,
            "enriched": True
        }
    except Exception as e:
        print(f"[ERROR] Failed to enrich prop for {prop.get('player', 'Unknown')}: {e}")
        # Return original prop with error indication
        return {
            **prop,
            "contextual_hit_rate": {
                "hit_rate": 0.30,
                "confidence": "Low",
                "error": "Enrichment failed"
            },
            "fantasy_hit_rate": {
                "hit_rate": 0.35,
                "confidence": "Low",
                "error": "Enrichment failed"
            },
            "enriched": False,
            "error": str(e)
        }

def enrich_player_props(props):
    """Enrich player props with contextual and fantasy hit rates using parallel processing"""
    if not props:
        return []
    
    print(f"[INFO] Starting enrichment for {len(props)} props")
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=10) as executor:
        enriched_props = list(executor.map(enrich_prop, props))
    
    # Count successful enrichments
    successful_enrichments = sum(1 for prop in enriched_props if prop.get("enriched", False))
    print(f"[INFO] Enrichment complete: {successful_enrichments}/{len(props)} props successfully enriched")
    
    return enriched_props