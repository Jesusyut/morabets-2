import logging
from typing import List, Dict, Any
from collections import defaultdict

logger = logging.getLogger(__name__)

def deduplicate_props_by_player(props: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Return only the best prop for each player based on:
    1. Highest contextual hit rate (if available)
    2. Most recent/active prop
    3. Best odds value
    """
    if not props:
        return []
    
    # Group props by player
    player_props = defaultdict(list)
    for prop in props:
        player = prop.get('player', 'Unknown')
        player_props[player].append(prop)
    
    deduplicated_props = []
    
    for player, player_prop_list in player_props.items():
        if not player_prop_list:
            continue
            
        # Sort by preference:
        # 1. Props with valid contextual hit rate first
        # 2. Then by hit rate value (descending)
        # 3. Then by odds value (higher odds = better value)
        def sort_key(prop):
            contextual = prop.get('contextual_hit_rate', {})
            has_valid_hit_rate = (
                isinstance(contextual, dict) and 
                contextual.get('hit_rate') is not None and
                not contextual.get('error')
            )
            
            hit_rate = contextual.get('hit_rate', 0) if has_valid_hit_rate else -1
            
            # Convert American odds to decimal for comparison
            odds_str = str(prop.get('odds', '+100'))
            try:
                if odds_str.startswith('+'):
                    decimal_odds = int(odds_str[1:]) / 100 + 1
                elif odds_str.startswith('-'):
                    decimal_odds = 100 / int(odds_str[1:]) + 1
                else:
                    decimal_odds = 1.0
            except (ValueError, ZeroDivisionError, AttributeError):
                decimal_odds = 1.0
            
            return (has_valid_hit_rate, hit_rate, decimal_odds)
        
        # Get the best prop for this player
        best_prop = max(player_prop_list, key=sort_key)
        deduplicated_props.append(best_prop)
        
        logger.debug(f"Selected best prop for {player}: {best_prop.get('stat')} {best_prop.get('threshold')} (hit_rate: {best_prop.get('contextual_hit_rate', {}).get('hit_rate', 'N/A')})")
    
    return deduplicated_props

def get_stat_display_name(stat_type: str) -> str:
    """Convert internal stat names to display-friendly names"""
    stat_names = {
        'batter_total_bases': 'Total Bases',
        'batter_hits': 'Hits',
        'batter_rbi': 'RBI',
        'batter_runs': 'Runs',
        'batter_home_runs': 'Home Runs',
        'batter_stolen_bases': 'Stolen Bases',
        'batter_walks': 'Walks',
        'batter_strikeouts': 'Strikeouts',
        'batter_hits_runs_rbis': 'H+R+RBI',
        'pitcher_strikeouts': 'Strikeouts',
        'pitcher_hits_allowed': 'Hits Allowed',
        'pitcher_earned_runs': 'Earned Runs',
        'pitcher_walks': 'Walks',
        'pitcher_outs': 'Outs Recorded',
        'batter_fantasy_score': 'Fantasy Score',
        'pitcher_fantasy_score': 'Fantasy Score'
    }
    return stat_names.get(stat_type, stat_type.replace('_', ' ').title())

def get_player_avatar_url(player_name: str) -> str:
    """Get player avatar URL - placeholder for now, can be enhanced with MLB API"""
    # For now, use initials-based avatar
    initials = ''.join([name[0] for name in player_name.split() if name])[:2].upper()
    return f"https://ui-avatars.com/api/?name={initials}&background=0d1117&color=fff&size=80&bold=true"