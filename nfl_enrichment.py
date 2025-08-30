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

# --- Back-compat shim expected by app.py ---
# Older code imports enrich_nfl_props(props) to annotate props in-place.
# Until we wire real matchup labels (moneyline/totals feed), make it a no-op pass-through.

import logging as _logging
_log = _logging.getLogger("nfl_enrichment")
if not _log.handlers:
    _logging.basicConfig(level=_logging.INFO)

def enrich_nfl_props(props, *args, **kwargs):
    """
    Backwards-compatible interface: accept a list of prop rows and return a list.
    Currently a no-op so app.py won't crash. We can later call label_matchups_from_featured()
    if you pass featured H2H/Totals per matchup.
    """
    try:
        # If caller provided featured market data, you can enable this block later:
        # featured = kwargs.get("featured_markets_by_matchup")
        # if isinstance(featured, dict):
        #     # Example: attach 'no_vig_favorite' / 'high_scoring' to each row's meta
        #     for row in props or []:
        #         mu = row.get("matchup")
        #         if mu in featured:
        #             labels = label_matchups_from_featured(featured[mu])
        #             row.setdefault("meta", {}).update(labels)
        return props
    except Exception as e:
        _log.warning("enrich_nfl_props pass-through failed: %s", e)
        return props

# be explicit (harmless if __all__ not used)
try:
    __all__ = list(set((__all__ if '__all__' in globals() else []) + [
        "label_matchups_from_featured", "enrich_nfl_props"
    ]))
except Exception:
    pass