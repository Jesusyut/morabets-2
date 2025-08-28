# services/true_odds.py
from typing import Optional, Tuple, Dict, Any, List

PREFERRED_BOOKS = ["fanduel","draftkings","betmgm","caesars","pointsbetus"]

MARKET_ALIASES = {
    "batter_hits": "player_hits",
    "batter_total_bases": "player_total_bases",
    "Hits": "player_hits",
    "Total Bases": "player_total_bases",
    "Home Runs": "player_home_runs",
    "Runs": "player_runs",
    "RBIs": "player_rbis",
    "Walks": "player_walks",
    "Stolen Bases": "player_stolen_bases",
    "Strikeouts": "player_strikeouts",
}

def _norm_market(m: Optional[str]) -> Optional[str]:
    if not m: return m
    return MARKET_ALIASES.get(m, m)

def _american_to_prob(american: Optional[int]) -> float:
    if american is None: return 0.0
    a = int(american)
    return 100.0/(a+100.0) if a>=0 else (-a)/((-a)+100.0)

def _no_vig_two_way(p_a: float, p_b: float) -> Tuple[float,float]:
    s = (p_a or 0.0) + (p_b or 0.0)
    if s <= 0: return 0.0, 0.0
    return p_a/s, p_b/s

def _books(bms: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    if not bms: return []
    pref = [b for b in bms if b.get("key") in PREFERRED_BOOKS]
    return pref or bms

def _same_point(prop_line, outcome_point) -> bool:
    if prop_line is None or outcome_point is None:
        # be permissive if book failed to send point
        return True
    try:
        return abs(float(prop_line) - float(outcome_point)) < 1e-6
    except Exception:
        return str(prop_line) == str(outcome_point)

def true_odds(event_odds: Dict[str,Any], market_key: str, line_point) -> Tuple[float,float,str]:
    """Return (true_over, true_under, source_book) or (0,0,'') if unavailable."""
    if not event_odds: return 0.0, 0.0, ""
    target_market = _norm_market(market_key)
    bms = _books(event_odds.get("bookmakers") or [])

    # Pass 1: exact line match
    for b in bms:
        for m in (b.get("markets") or []):
            if m.get("key") != target_market: continue
            over_p, under_p = None, None
            for o in (m.get("outcomes") or []):
                if not _same_point(line_point, o.get("point")): continue
                nm, pr = o.get("name"), o.get("price")
                if nm == "Over":  over_p  = _american_to_prob(pr)
                if nm == "Under": under_p = _american_to_prob(pr)
            if over_p is not None and under_p is not None:
                p_o, p_u = _no_vig_two_way(over_p, under_p)
                return round(p_o,4), round(p_u,4), b.get("key","")

    # Pass 2: first available Over/Under for that market
    for b in bms:
        for m in (b.get("markets") or []):
            if m.get("key") != target_market: continue
            over_p, under_p = None, None
            for o in (m.get("outcomes") or []):
                nm, pr = o.get("name"), o.get("price")
                if nm == "Over":  over_p  = _american_to_prob(pr)
                if nm == "Under": under_p = _american_to_prob(pr)
            if over_p is not None and under_p is not None:
                p_o, p_u = _no_vig_two_way(over_p, under_p)
                return round(p_o,4), round(p_u,4), b.get("key","")

    return 0.0, 0.0, "" 