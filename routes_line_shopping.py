from flask import Blueprint, request, jsonify
import datetime
from probability import fair_probs_from_two_sided, fair_odds_from_prob

line_shop_bp = Blueprint("line_shop", __name__)

def american_to_decimal(american: int) -> float:
    a = float(american)
    return 1.0 + (a/100.0 if a > 0 else 100.0/abs(a))

def fair_decimal(p: float) -> float:
    p = max(min(float(p), 1 - 1e-6), 1e-6)
    return 1.0 / p

@line_shop_bp.get("/api/props/line-shopping")
def line_shopping_route():
    from app import load_enriched_props

    league = (request.args.get("league") or "mlb").lower()
    date_str = request.args.get("date") or datetime.date.today().strftime("%Y-%m-%d")

    min_p = float(request.args.get("min_p", "0.70"))     # default 70%
    ev_min = float(request.args.get("ev_min", "0.00"))   # allow showing all EV >= 0
    edge_bp_min = float(request.args.get("edge_bp_min", "0.0"))

    enriched = load_enriched_props(league, date_str) or []

    out = []
    for p in enriched:
        po = p.get("prob_over")
        pu = p.get("prob_under")
        shop = p.get("shop") or {}
        side, prob = None, None
        if isinstance(po, (int,float)) and po >= min_p:
            side, prob = "Over", float(po)
        if side is None and isinstance(pu, (int,float)) and pu >= min_p:
            side, prob = "Under", float(pu)
        if side is None:
            continue

        best = shop.get(side.lower()) if isinstance(shop, dict) else None
        if not best:
            continue
        american = best.get("american")
        book = best.get("book")
        if american is None or book is None:
            continue

        dec = american_to_decimal(int(american))
        fdr = fair_decimal(prob)
        ev = prob * dec - 1.0
        edge = dec / fdr - 1.0

        if ev < ev_min:
            continue
        if edge < edge_bp_min:
            continue

        # Add fair probabilities if both over and under odds are available
        fair_data = {}
        over_odds = shop.get("over", {}).get("american")
        under_odds = shop.get("under", {}).get("american")
        
        if over_odds is not None and under_odds is not None:
            try:
                p_over_fair, p_under_fair = fair_probs_from_two_sided(float(over_odds), float(under_odds))
                if p_over_fair is not None:
                    fair_data = {
                        "prob": {"over": round(p_over_fair, 4), "under": round(p_under_fair, 4)},
                        "american": {
                            "over": fair_odds_from_prob(p_over_fair),
                            "under": fair_odds_from_prob(p_under_fair),
                        }
                    }
            except Exception as e:
                # Skip fair calculation if there's an error
                pass

        out.append({
            "player": p.get("player"),
            "team": p.get("team"),
            "event_id": p.get("event_id"),
            "market": p.get("market"),
            "line": p.get("line"),
            "undervalued": {"any": True, "side": side},
            "shop": {"over": shop.get("over"), "under": shop.get("under")},
            "best": {"side": side, "book": book, "american": american, "decimal": dec},
            "metrics": {"p": prob, "ev": ev, "edge_vs_fair": edge},
            "fair": fair_data
        })

    out.sort(key=lambda r: r["metrics"]["ev"], reverse=True)
    return jsonify(out), 200 