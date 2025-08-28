# services/odds_totals_context.py
from typing import Dict, Any, Optional, Tuple, List

PREFERRED_BOOKS = ["fanduel","draftkings","betmgm","caesars","pointsbetus"]

def american_to_prob(american: Optional[int]) -> float:
    if american is None: return 0.0
    a = int(american)
    return 100.0/(a+100.0) if a>=0 else (-a)/((-a)+100.0)

def no_vig_two_way(p_a: float, p_b: float) -> Tuple[float,float]:
    s = (p_a or 0.0) + (p_b or 0.0)
    if s <= 0: return 0.0, 0.0
    return p_a/s, p_b/s

def _books(bms: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    if not bms: return []
    pref = [b for b in bms if (b.get("key") in PREFERRED_BOOKS)]
    return pref or bms

# thresholds you can tweak
TOTAL_OVER_STRONG = 0.58      # => High-Scoring
TOTAL_UNDER_STRONG = 0.58     # => Low-Scoring
TOTAL_NEUTRAL_LO = 0.48
TOTAL_NEUTRAL_HI = 0.54

def compute_totals_context(event_odds: Dict[str,Any]) -> Dict[str,Any]:
    """
    Returns:
      {
        "event_id": "...",
        "start_iso": "<ISO8601>",
        "total_point": <float|None>,
        "true_prob_over": <float|None>,
        "true_prob_under": <float|None>,
        "tier": "High-Scoring" | "Neutral" | "Low-Scoring" | "Unknown"
      }
    """
    if not event_odds: return {}

    # normalize start time
    start = event_odds.get("commence_time") or event_odds.get("start_time") or event_odds.get("game_time")
    # some feeds nest under 'commence_time' as ISO; we just echo it back
    event_id = event_odds.get("id")

    bms = event_odds.get("bookmakers") or []
    bms = _books(bms)

    # find a line and de-vig
    total_point = None
    over_probs, under_probs = [], []

    # try to pick the book's totals line first (FanDuel preferred)
    # We'll accept the first totals market we find per book.
    for b in bms:
        for m in (b.get("markets") or []):
            if m.get("key") == "totals":
                over_p, under_p, point = None, None, None
                for o in (m.get("outcomes") or []):
                    nm, pr, pt = o.get("name"), o.get("price"), o.get("point")
                    if nm == "Over":  over_p = american_to_prob(pr);  point = pt if pt is not None else point
                    if nm == "Under": under_p = american_to_prob(pr); point = pt if pt is not None else point
                if over_p is not None and under_p is not None:
                    p_o, p_u = no_vig_two_way(over_p, under_p)
                    if p_o and p_u:
                        over_probs.append(p_o); under_probs.append(p_u)
                        if total_point is None and point is not None:
                            total_point = point
                break  # one totals market per book is enough

    true_over  = round(sum(over_probs)/len(over_probs), 4) if over_probs else None
    true_under = round(sum(under_probs)/len(under_probs), 4) if under_probs else None

    # tier
    tier = "Unknown"
    if true_over is not None:
        if true_over >= TOTAL_OVER_STRONG: tier = "High-Scoring"
        elif TOTAL_NEUTRAL_LO <= true_over <= TOTAL_NEUTRAL_HI: tier = "Neutral"
        elif (1-true_over) >= TOTAL_UNDER_STRONG: tier = "Low-Scoring"
        else: tier = "Neutral"

    return {
        "event_id": event_id,
        "start_iso": start,
        "total_point": total_point,
        "true_prob_over": true_over,
        "true_prob_under": true_under,
        "tier": tier
    } 