from flask import Blueprint, request, jsonify
import datetime

evsimple_bp = Blueprint("evsimple", __name__, url_prefix="/api/ev-plays-simple")

def _to_float(x):
    try:
        if isinstance(x, str):
            x = x.strip().replace('%','').replace(',','')
        return float(x)
    except Exception:
        return None

def normalize_prob(p):
    """Accept 0–1 or 0–100 (or string). Returns 0–1 float or None."""
    v = _to_float(p)
    if v is None:
        return None
    if v > 1.0:  # looks like percent
        v = v / 100.0
    if v <= 0 or v >= 1:
        # clamp a little, but reject impossible values
        if 0.0 < v < 1.0:
            return v
        return None
    return v

def american_to_decimal(american):
    try:
        if isinstance(american, str):
            american = american.strip().replace('+','')
        a = int(american)
    except Exception:
        return None
    return 1.0 + (a/100.0 if a > 0 else 100.0/abs(a))

@evsimple_bp.get("")
def ev_plays_simple():
    # We prefer enriched props; fall back to raw if needed
    from app import load_enriched_props as _load_enriched
    try:
        from app import fetch_player_props as _fetch_raw
    except Exception:
        _fetch_raw = None

    league = (request.args.get("league") or "mlb").lower()
    date_str = request.args.get("date") or datetime.date.today().strftime("%Y-%m-%d")

    # Defaults: flexible and forgiving (user can override via query)
    min_p = float(request.args.get("min_p", "0.55"))    # >=55% win prob
    ev_min = float(request.args.get("ev_min", "0.01"))  # >=+1% EV

    # Load data
    enriched = _load_enriched(league, date_str) or []
    items = enriched

    # Fall back to raw if enriched empty (ensure fields exist)
    if not items and _fetch_raw:
        raw = _fetch_raw(league, date_str) or []
        items = raw

    out = []
    for p in items:
        # Accept either prob_over/prob_under or prob fields
        po = normalize_prob(p.get("prob_over"))
        pu = normalize_prob(p.get("prob_under"))
        # Choose best side (higher prob). If tie, prefer Over.
        side, winp = None, None
        if po is not None and (pu is None or po >= pu):
            side, winp = "Over", po
        elif pu is not None:
            side, winp = "Under", pu
        else:
            continue

        if winp < min_p:  # threshold
            continue

        shop = p.get("shop") or {}
        best = shop.get(side.lower()) if isinstance(shop, dict) else None
        if not best:
            continue

        dec = american_to_decimal(best.get("american"))
        if dec is None:
            continue

        # EV on $1 stake
        ev = winp * dec - 1.0
        if ev < ev_min:
            continue

        out.append({
            "player": p.get("player"),
            "team": p.get("team"),
            "event_id": p.get("event_id"),
            "market": p.get("market"),
            "line": p.get("line"),
            "undervalued": {"any": True, "side": side},
            "shop": {"over": shop.get("over"), "under": shop.get("under")},
            "best": {
                "side": side,
                "book": (best.get("book") if isinstance(best, dict) else None),
                "american": (best.get("american") if isinstance(best, dict) else None),
                "decimal": dec
            },
            "metrics": {"p": winp, "ev": ev}
        })

    out.sort(key=lambda r: r["metrics"]["ev"], reverse=True)

    # Also include a tiny header (date/league) like the main route does
    return jsonify({
        "date": date_str,
        "league": league,
        "props": out,
        "lines": []  # we focus on props for this simple endpoint
    }), 200 