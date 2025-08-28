"""
No-Vig Mode: American odds to probability conversion and no-vig calculations
"""
from typing import Tuple, Optional
from probability import american_to_implied, fair_probs_from_two_sided

def american_to_prob(odds: int) -> float:
    """Convert American odds to implied probability (0.0 to 1.0)"""
    return american_to_implied(odds)

def novig_two_way(over_odds: int, under_odds: int) -> Tuple[float, float]:
    """
    Calculate no-vig probabilities from two-way American odds.
    Returns (over_prob, under_prob) as floats between 0.0 and 1.0
    """
    p_over, p_under = fair_probs_from_two_sided(over_odds, under_odds)
    if p_over is None or p_under is None:
        # Fallback to implied if no-vig calculation fails
        p_over = american_to_implied(over_odds)
        p_under = american_to_implied(under_odds)
    return float(p_over), float(p_under)

def is_valid_odds(odds: int) -> bool:
    """Check if American odds are valid (not 0 or None)"""
    return odds is not None and odds != 0

def calculate_edge(true_prob: float, implied_prob: float) -> float:
    """Calculate betting edge as percentage"""
    return (true_prob - implied_prob) * 100
