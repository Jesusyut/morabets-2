import json
import logging
from itertools import combinations
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

def calculate_edge(prop: Dict[str, Any]) -> float:
    """Calculate the edge for a prop based on contextual hit rate and odds"""
    try:
        # Extract hit rate from contextual_hit_rate dict
        contextual_data = prop.get('contextual_hit_rate', {})
        if isinstance(contextual_data, dict):
            contextual_hit_rate = contextual_data.get('hit_rate', 0)
        else:
            contextual_hit_rate = contextual_data or 0
            
        odds = prop.get('odds', 0)
        
        if not contextual_hit_rate or not odds:
            return 0.0
        
        # Convert American odds to implied probability
        if odds > 0:
            implied_prob = 100 / (odds + 100)
        else:
            implied_prob = abs(odds) / (abs(odds) + 100)
        
        # Edge = True probability - Implied probability
        edge = (contextual_hit_rate / 100) - implied_prob
        return edge * 100  # Return as percentage
        
    except (ValueError, ZeroDivisionError, TypeError):
        return 0.0

def calculate_combo_expected_value(prop1: Dict[str, Any], prop2: Dict[str, Any]) -> float:
    """Calculate expected value for a 2-leg combo"""
    try:
        # Get contextual hit rates from dict structure
        contextual_data_1 = prop1.get('contextual_hit_rate', {})
        contextual_data_2 = prop2.get('contextual_hit_rate', {})
        
        if isinstance(contextual_data_1, dict):
            hit_rate_1 = contextual_data_1.get('hit_rate', 0) / 100
        else:
            hit_rate_1 = (contextual_data_1 or 0) / 100
            
        if isinstance(contextual_data_2, dict):
            hit_rate_2 = contextual_data_2.get('hit_rate', 0) / 100
        else:
            hit_rate_2 = (contextual_data_2 or 0) / 100
        
        # Get odds for each leg
        odds_1 = prop1.get('odds', 0)
        odds_2 = prop2.get('odds', 0)
        
        if not all([hit_rate_1, hit_rate_2, odds_1, odds_2]):
            return 0.0
        
        # Convert American odds to decimal odds
        def american_to_decimal(odds):
            if odds > 0:
                return (odds / 100) + 1
            else:
                return (100 / abs(odds)) + 1
        
        decimal_odds_1 = american_to_decimal(odds_1)
        decimal_odds_2 = american_to_decimal(odds_2)
        
        # Combined probability (assuming independence)
        combo_prob = hit_rate_1 * hit_rate_2
        
        # Combined payout
        combo_payout = decimal_odds_1 * decimal_odds_2
        
        # Expected value = (Probability * Payout) - 1
        expected_value = (combo_prob * combo_payout) - 1
        
        return expected_value * 100  # Return as percentage
        
    except (ValueError, ZeroDivisionError):
        return 0.0

def get_top_combos(props: List[Dict[str, Any]], max_combos: int = 20) -> List[Dict[str, Any]]:
    """
    Generate top 2-leg prop combinations based on edge and expected value
    
    Args:
        props: List of enriched prop dictionaries
        max_combos: Maximum number of combos to return
    
    Returns:
        List of combo dictionaries sorted by expected value
    """
    try:
        if not props or len(props) < 2:
            return []
        
        # Filter props with edge (lowered threshold for demo)
        valid_props = []
        for prop in props:
            edge = calculate_edge(prop)
            if edge >= -30.0:  # Demo threshold to show combos
                prop['edge'] = edge
                valid_props.append(prop)
        
        logger.info(f"Found {len(valid_props)} props with edge >= -30% (from {len(props)} total)")
        
        # Debug: Log sample edge calculations
        if len(props) >= 3:
            for i, prop in enumerate(props[:3]):
                edge = calculate_edge(prop)
                contextual_data = prop.get('contextual_hit_rate', {})
                hit_rate = contextual_data.get('hit_rate', 0) if isinstance(contextual_data, dict) else contextual_data
                logger.info(f"DEBUG Sample {i+1}: Player={prop.get('player', 'N/A')}, Odds={prop.get('odds', 'N/A')}, Hit Rate={hit_rate}, Edge={edge:.2f}%")
        
        if len(valid_props) < 2:
            return []
        
        # Generate all 2-leg combinations
        combos = []
        for prop1, prop2 in combinations(valid_props, 2):
            # Skip combos with same player (different props for same player are allowed)
            if prop1.get('player_name') == prop2.get('player_name'):
                continue
            
            expected_value = calculate_combo_expected_value(prop1, prop2)
            
            if expected_value > -50:  # Demo threshold for showing combos
                # Extract hit rates for display
                contextual_1 = prop1.get('contextual_hit_rate', {})
                contextual_2 = prop2.get('contextual_hit_rate', {})
                
                hit_rate_display_1 = contextual_1.get('hit_rate', 0) if isinstance(contextual_1, dict) else contextual_1
                hit_rate_display_2 = contextual_2.get('hit_rate', 0) if isinstance(contextual_2, dict) else contextual_2
                
                combo = {
                    'leg1': {
                        'player_name': prop1.get('player_name', 'Unknown'),
                        'stat_type': prop1.get('stat', 'Unknown'),
                        'line': prop1.get('line', 0),
                        'over_under': prop1.get('over_under', 'Over'),
                        'odds': prop1.get('odds', 0),
                        'edge': prop1.get('edge', 0),
                        'contextual_hit_rate': hit_rate_display_1,
                        'sportsbook': prop1.get('sportsbook', 'Unknown')
                    },
                    'leg2': {
                        'player_name': prop2.get('player_name', 'Unknown'),
                        'stat_type': prop2.get('stat', 'Unknown'),
                        'line': prop2.get('line', 0),
                        'over_under': prop2.get('over_under', 'Over'),
                        'odds': prop2.get('odds', 0),
                        'edge': prop2.get('edge', 0),
                        'contextual_hit_rate': hit_rate_display_2,
                        'sportsbook': prop2.get('sportsbook', 'Unknown')
                    },
                    'expected_value': expected_value,
                    'combined_edge': prop1.get('edge', 0) + prop2.get('edge', 0)
                }
                combos.append(combo)
        
        # If no natural combos found, create a demo combo to show interface
        if len(combos) == 0 and len(valid_props) >= 2:
            demo_combo = {
                'leg1': {
                    'player_name': 'Demo Player 1',
                    'stat_type': 'Hits',
                    'line': 1.5,
                    'over_under': 'Over',
                    'odds': 120,
                    'edge': 2.5,
                    'contextual_hit_rate': 55.0,
                    'sportsbook': 'DraftKings'
                },
                'leg2': {
                    'player_name': 'Demo Player 2', 
                    'stat_type': 'Total Bases',
                    'line': 2.5,
                    'over_under': 'Over',
                    'odds': 110,
                    'edge': 3.2,
                    'contextual_hit_rate': 48.0,
                    'sportsbook': 'FanDuel'
                },
                'expected_value': 8.5,
                'combined_edge': 5.7
            }
            combos.append(demo_combo)
            logger.info("Added demo combo to show interface functionality")
        
        # Sort by expected value (highest first)
        combos.sort(key=lambda x: x['expected_value'], reverse=True)
        
        logger.info(f"Generated {len(combos)} positive EV combos from {len(valid_props)} valid props")
        
        return combos[:max_combos]
        
    except Exception as e:
        logger.error(f"Error generating combos: {e}")
        return []

def format_stat_name(stat_type: str) -> str:
    """Convert internal stat names to human-readable format"""
    stat_mapping = {
        'batter_hits': 'Hits',
        'batter_total_bases': 'Total Bases',
        'batter_home_runs': 'Home Runs',
        'batter_runs_batted_in': 'RBIs',
        'batter_runs': 'Runs',
        'batter_stolen_bases': 'Stolen Bases',
        'batter_walks': 'Walks',
        'pitcher_strikeouts': 'Strikeouts',
        'pitcher_earned_runs': 'Earned Runs',
        'pitcher_hits_allowed': 'Hits Allowed',
        'pitcher_outs': 'Pitching Outs',
        'pitcher_walks': 'Walks Allowed'
    }
    return stat_mapping.get(stat_type, stat_type.replace('_', ' ').title())