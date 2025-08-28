import logging
from functools import reduce

logger = logging.getLogger(__name__)

def implied_probability(odds):
    """Calculate implied probability from American odds"""
    try:
        if odds > 0:
            return round(100 / (odds + 100), 4)
        else:
            return round(abs(odds) / (abs(odds) + 100), 4)
    except (ValueError, ZeroDivisionError) as e:
        logger.error(f"Error calculating implied probability for odds {odds}: {e}")
        return 0.0

def calculate_edge(true_prob, book_odds):
    """Calculate betting edge"""
    try:
        if not (0 <= true_prob <= 1):
            raise ValueError("True probability must be between 0 and 1")
        
        imp_prob = implied_probability(book_odds)
        edge = round((true_prob - imp_prob) * 100, 2)
        
        return {
            "true_prob": true_prob,
            "book_odds": book_odds,
            "implied_prob": imp_prob,
            "edge": edge
        }
    except Exception as e:
        logger.error(f"Error calculating edge: {e}")
        return {"error": str(e)}

def kelly_bet_size(prob, odds, bankroll):
    """Calculate Kelly criterion bet size"""
    try:
        if not (0 <= prob <= 1):
            raise ValueError("Probability must be between 0 and 1")
        if bankroll <= 0:
            raise ValueError("Bankroll must be positive")
        
        # Convert American odds to decimal odds
        if odds > 0:
            decimal_odds = (odds / 100) + 1
        else:
            decimal_odds = (100 / abs(odds)) + 1
        
        # Kelly formula: f = (bp - q) / b
        # where b = decimal_odds - 1, p = prob, q = 1 - prob
        b = decimal_odds - 1
        edge = prob * b - (1 - prob)
        kelly_fraction = edge / b if b > 0 else 0
        
        # Apply fractional Kelly (typically 25% of full Kelly for risk management)
        fractional_kelly = kelly_fraction * 0.25
        
        # Ensure bet size is not negative
        bet_size = max(0, round(bankroll * fractional_kelly, 2))
        
        return bet_size
    except Exception as e:
        logger.error(f"Error calculating Kelly bet size: {e}")
        return 0.0

def calculate_parlay_edge(probabilities, book_odds):
    """Calculate parlay edge"""
    try:
        if not probabilities or not isinstance(probabilities, list):
            raise ValueError("Probabilities must be a non-empty list")
        
        for prob in probabilities:
            if not (0 <= prob <= 1):
                raise ValueError("All probabilities must be between 0 and 1")
        
        # Calculate combined probability
        combined_prob = reduce(lambda x, y: x * y, probabilities)
        
        # Calculate true fair odds
        true_odds = (1 / combined_prob) if combined_prob > 0 else float('inf')
        
        # Convert book odds to decimal format for comparison
        if book_odds > 0:
            decimal_book_odds = (book_odds / 100) + 1
        else:
            decimal_book_odds = (100 / abs(book_odds)) + 1
        
        # Calculate edge
        edge = round(true_odds - decimal_book_odds, 2)
        
        return {
            "combined_prob": round(combined_prob * 100, 2),
            "true_odds": round(true_odds, 2),
            "book_odds": book_odds,
            "decimal_book_odds": round(decimal_book_odds, 2),
            "edge": edge
        }
    except Exception as e:
        logger.error(f"Error calculating parlay edge: {e}")
        return {"error": str(e)}

def american_to_decimal(odds):
    """American -> decimal (e.g., +120 -> 2.20, -150 -> 1+(100/150)=1.6667)"""
    v = float(odds)
    return 1.0 + (v / 100.0 if v > 0 else 100.0 / abs(v))

def american_to_implied(odds):
    """American -> implied probability (0..1)"""
    v = float(odds)
    return 100.0 / (v + 100.0) if v > 0 else abs(v) / (abs(v) + 100.0)

def implieds_to_no_vig(p_a, p_b):
    """Normalize two implied probabilities to remove vig."""
    s = p_a + p_b
    if s <= 0:
        return None, None
    return p_a / s, p_b / s

def decimal_to_american(dec):
    """Decimal -> American (inverse of american_to_decimal)."""
    d = float(dec)
    if d <= 1.0:
        return 0
    b = d - 1.0
    # Return closest American as int
    return int(round(b * 100)) if b >= 1.0 else int(round(-100.0 / b))

def fair_probs_from_two_sided(odds_a, odds_b):
    """
    Given two American prices for opposite sides, return vig-free fair probabilities (p_a, p_b).
    """
    p_a = american_to_implied(odds_a)
    p_b = american_to_implied(odds_b)
    return implieds_to_no_vig(p_a, p_b)

def fair_odds_from_prob(p):
    """Probability (0..1) -> fair American odds."""
    p = float(p)
    if p <= 0 or p >= 1:
        return 0
    dec = 1.0 / p
    return decimal_to_american(dec)

def american_to_prob(odds):
    """Convert American odds to probability (idempotent helper)"""
    if odds is None: 
        return None
    o = float(odds)
    return 100.0/(o+100.0) if o > 0 else (-o)/(100.0 - o)

def no_vig_two_way(odds_a, odds_b):
    """Calculate no-vig probabilities from two American odds (idempotent helper)"""
    pa = american_to_prob(odds_a)
    pb = american_to_prob(odds_b)
    if not pa or not pb: 
        return None, None
    s = pa + pb
    if s == 0: 
        return None, None
    return pa/s, pb/s