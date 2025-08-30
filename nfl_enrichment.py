# nfl_enrichment.py
from __future__ import annotations
from typing import Dict, Any, List, Tuple

try:
    from novig import american_to_prob, novig_two_way
except Exception:
    def american_to_prob(odds: int | float) -> float:
        o = float(odds)
        return 100.0/(o+100.0) if o > 0 else (-o)/(100.0 - o)
    def novig_two_way(oddsa: int, oddsb: int) -> Tuple[float,float]:
        pa, pb = american_to_prob(oddsa), american_to_prob(oddsb)
        z = (pa + pb) or 1.0
        return pa/z, pb/z

def label_matchups_from_featured(markets: Dict[str, Any]) -> Dict[str, Any]:
    """
    Input shape suggestion (not enforced):
      markets = {"h2h": {"home": +120, "away": -140}, "totals": {"over": -105, "under": -115, "point": 46.5}}
    Output:
      {"no_vig_favorite":"AWAY","high_scoring":true/false}
    """
    out = {}
    h2h = markets.get("h2h") or {}
    if "home" in h2h and "away" in h2h:
        p_home, p_away = american_to_prob(h2h["home"]), american_to_prob(h2h["away"])
        z = (p_home + p_away) or 1.0
        ph, pa = p_home/z, p_away/z
        out["no_vig_favorite"] = "HOME" if ph > pa else "AWAY"
        out["no_vig_diff"] = abs(ph - pa)

    totals = markets.get("totals") or {}
    if "over" in totals and "under" in totals:
        po, pu = novig_two_way(totals["over"], totals["under"])
        # simple heuristic: >55% fair prob to OVER => "high scoring"
        out["high_scoring"] = (po >= 0.55)
        out["totals_point"] = totals.get("point")
    return out