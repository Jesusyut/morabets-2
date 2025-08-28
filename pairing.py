"""
No-Vig Mode: Market pairing and prop building without enrichment
"""
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from novig import novig_two_way, american_to_prob, is_valid_odds

# Markets that work with the API and have consistent structure
ALLOWED_MARKETS = [
    "batter_hits",
    "batter_home_runs", 
    "batter_total_bases",
    "pitcher_strikeouts",
    "pitcher_earned_runs",
    "pitcher_outs",
    "pitcher_hits_allowed"
]

def _normalize_point(val) -> Optional[str]:
    """Normalize prop line so '0.5' pairs with '0.50' (3 dp string)"""
    if val is None:
        return None
    try:
        return f"{Decimal(str(val)).quantize(Decimal('0.001'))}"
    except (InvalidOperation, ValueError, TypeError):
        s = str(val).strip()
        return s if s else None

def _resolve_side_and_player(name: Optional[str], desc: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Extract side (over/under) and player name from outcome fields"""
    n = (name or "").strip()
    d = (desc or "").strip()
    ln, ld = n.lower(), d.lower()
    
    # Check for exact matches first
    if ln in ("over", "under"):
        return ln, d or n
    if ld in ("over", "under"):
        return ld, n or d
    
    # Check for partial matches
    if "over" in ln:
        return "over", d or n
    if "under" in ln:
        return "under", d or n
    if "over" in ld:
        return "over", n or d
    if "under" in ld:
        return "under", n or d
    
    return None, None

def _pair_outcomes(bookmakers: List[Dict[str, Any]], stat_key: str) -> Dict[Tuple[str, str, Optional[str]], Dict[str, Optional[Dict[str, Any]]]]:
    """
    Pair over/under outcomes for each player+stat+line combination.
    Returns: (player, stat_key, point_key) -> {'over': {price, book}|None, 'under': {...}|None}
    Prefers FanDuel when duplicates occur.
    """
    sidebook = defaultdict(lambda: {"over": None, "under": None})
    
    for bookmaker in bookmakers or []:
        book_name = (bookmaker.get("key") or bookmaker.get("title") or "").strip().lower()
        
        for market in bookmaker.get("markets") or []:
            if market.get("key") != stat_key:
                continue
                
            for outcome in market.get("outcomes") or []:
                side, player = _resolve_side_and_player(outcome.get("name"), outcome.get("description"))
                
                if side not in ("over", "under") or not player:
                    continue
                    
                price = outcome.get("price")
                if not is_valid_odds(price):
                    continue
                    
                point = _normalize_point(outcome.get("point"))
                key = (player, stat_key, point)
                
                # Prefer FanDuel, otherwise keep first seen
                current = sidebook[key][side]
                keep = current is None or current.get("book") != "fanduel"
                
                if keep:
                    sidebook[key][side] = {
                        "price": int(price),
                        "book": book_name
                    }
    
    return sidebook

def build_props_novig(league: str, offers: List[Dict[str, Any]], prefer_books: Optional[List[str]] = None) -> Dict[str, List[Dict[str, Any]]]:
    """
    Build props with no-vig calculations from flat offers.
    
    Args:
        offers: List of flat offers with player, stat, line, side, odds, book
        min_prob: Minimum probability threshold (0.0 to 1.0) for filtering
    
    Returns:
        List of props with fair probabilities calculated
    """
    # Group offers by player+stat+line, preserving matchup info
    grouped = defaultdict(lambda: {"over": None, "under": None, "matchup": None})
    
    for offer in offers:
        player = offer.get("player", "").strip()
        stat = offer.get("stat", "").strip()
        line = _normalize_point(offer.get("line"))
        side = offer.get("side", "").lower()
        odds = offer.get("odds")
        book = offer.get("book", "").strip()
        matchup = offer.get("matchup", "").strip()
        
        if not all([player, stat, line, side in ("over", "under"), is_valid_odds(odds)]):
            continue
            
        key = (player, stat, line)
        
        # Prefer FanDuel, otherwise keep first seen
        current = grouped[key][side]
        keep = current is None or current.get("book") != "fanduel"
        
        if keep:
            grouped[key][side] = {
                "price": int(odds),
                "book": book
            }
            # Store matchup info
            if not grouped[key]["matchup"]:
                grouped[key]["matchup"] = matchup
    
    # Build props with no-vig calculations and group by matchup
    grouped_props = {}
    
    for (player, stat, line), sides in grouped.items():
        over = sides.get("over")
        under = sides.get("under")
        matchup = sides.get("matchup")
        
        # Skip if no valid sides
        if not over and not under:
            continue
        
        # Use matchup from offers, fallback to simple key
        matchup_key = matchup if matchup else f"{league.upper()} Game - {player}"
        
        if matchup_key not in grouped_props:
            grouped_props[matchup_key] = []
        
        prop = {
            "player": player,
            "stat": stat,
            "line": line,
            "side": "over" if over else "under",
            "odds": (over or under)["price"],
            "book": (over or under)["book"],
            "shop": {},
            "fair": {"prob": {"over": 0.0, "under": 0.0}}
        }
        
        # Populate shop data
        if over:
            prop["shop"]["over"] = {"american": over["price"], "book": over["book"]}
        if under:
            prop["shop"]["under"] = {"american": under["price"], "book": under["book"]}
        
        # Calculate fair probabilities
        if over and under:
            # Both sides available - use no-vig
            p_over, p_under = novig_two_way(over["price"], under["price"])
            prop["fair"]["prob"]["over"] = round(p_over, 4)
            prop["fair"]["prob"]["under"] = round(p_under, 4)
            prop["fair"]["book"] = over["book"] if over["book"] == "fanduel" else under["book"]
        else:
            # Single side - use implied probability
            single_side = over or under
            p = american_to_prob(single_side["price"])
            if over:
                prop["fair"]["prob"]["over"] = round(p, 4)
                prop["fair"]["prob"]["under"] = round(1.0 - p, 4)
            else:
                prop["fair"]["prob"]["under"] = round(p, 4)
                prop["fair"]["prob"]["over"] = round(1.0 - p, 4)
            prop["fair"]["book"] = single_side["book"]
        
        grouped_props[matchup_key].append(prop)
    
    return grouped_props
