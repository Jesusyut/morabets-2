from typing import Optional, Tuple

def american_to_prob(odds: Optional[int]) -> Optional[float]:
    if odds is None or odds == 0:
        return None
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return (-odds) / ((-odds) + 100.0)

def novig_two_way(over_odds: Optional[int], under_odds: Optional[int]) -> Tuple[Optional[float], Optional[float]]:
    p_over  = american_to_prob(over_odds)
    p_under = american_to_prob(under_odds)
    if p_over is None or p_under is None:
        return (None, None)
    s = p_over + p_under
    if s <= 0:
        return (None, None)
    return (round(p_over / s, 4), round(p_under / s, 4))
