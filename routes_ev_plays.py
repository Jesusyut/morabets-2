from __future__ import annotations
from flask import Blueprint, request, jsonify
from typing import Any, Dict, List, Optional, Tuple
import math

ev_plays_bp = Blueprint("ev_plays", __name__)

# ---------- Tunables (can be env'd later) ----------
DEFAULT_P_MIN = 0.65        # minimum engine prob to consider
DEFAULT_EDGE_BP_MIN = 0.03  # +3% vs implied (props)
DEFAULT_EV_MIN = 0.02       # ≥ +2% EV to include in EV tab
USE_NOVIG_FOR_PROPS = True  # normalize Over/Under when both sides exist
REQUIRE_PAIR_FOR_NOVIG = False

# ---------- Odds math ----------
def _to_float(x: Any) -> Optional[float]:
    try:
        f = float(x)
        if math.isfinite(f):
            return f
    except Exception:
        pass
    return None

def american_to_decimal(american: float) -> Optional[float]:
    a = _to_float(american)
    if a is None: return None
    return 1.0 + (a/100.0) if a > 0 else 1.0 + (100.0/abs(a))

def implied_prob_from_american(american: float) -> Optional[float]:
    a = _to_float(american)
    if a is None: return None
    return 100.0/(a+100.0) if a > 0 else abs(a)/(abs(a)+100.0)

def ev_from_decimal(p: float, dec: float) -> Optional[float]:
    if p is None or dec is None: return None
    return p*(dec-1.0) - (1.0-p)

def _pair_implied_no_vig(imp_a: Optional[float], imp_b: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    if imp_a is None or imp_b is None: return (None, None)
    s = imp_a + imp_b
    if s <= 0: return (None, None)
    return (imp_a/s, imp_b/s)

# ---------- Soft import helpers ----------
def _try_import_compute_line_shopping():
    try:
        from routes_line_shopping import compute_line_shopping  # type: ignore
        return compute_line_shopping
    except Exception:
        return None

def _try_import_engine_signals():
    """
    Optional hook you can add:
      fetch_line_engine_signals(league, date) -> dict[event_id] = {
         "moneyline": {"home": p, "away": p},
         "spread": { str(point): {"home": p, "away": p} }
      }
    If missing, we skip EV lines (no crash).
    """
    try:
        from app import fetch_line_engine_signals  # type: ignore
        return fetch_line_engine_signals
    except Exception:
        return None

# ---------- Game lines parsing ----------
def _best_price_outcome(market: Dict[str, Any], side_name: str) -> Optional[Dict[str, Any]]:
    best = None
    for o in market.get("outcomes", []) or []:
        # For h2h/spreads, many feeds use "name" {"home","away"} or team names.
        if str(o.get("name", "")).lower() != side_name:
            continue
        price = _to_float(o.get("price"))
        if price is None: continue
        dec = american_to_decimal(price)
        imp = implied_prob_from_american(price)
        if dec is None or imp is None: continue
        cand = {
            "american": int(price),
            "decimal": dec,
            "implied": imp,
            "point": _to_float(o.get("point")),
        }
        if best is None or cand["decimal"] > best["decimal"]:
            best = cand
    return best

def _collect_best_from_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns the event's best prices grouped by market key.
    For Odds API, typical keys: 'h2h' (moneyline), 'spreads'.
    """
    out = {"h2h": None, "spreads": []}
    for bk in event.get("bookmakers", []) or []:
        bname = bk.get("title") or bk.get("key") or "unknown"
        for m in bk.get("markets", []) or []:
            key = m.get("key")
            if key == "h2h":
                # track best home/away across all books
                best_home = _best_price_outcome(m, "home")
                best_away = _best_price_outcome(m, "away")
                # attach book for transparency (highest-dec source)
                if best_home: best_home["book"] = bname
                if best_away: best_away["book"] = bname
                prev = out["h2h"] or {}
                # keep better decimals if already present
                for side, cur in (("home", best_home), ("away", best_away)):
                    if cur:
                        prev_side = prev.get(side)
                        if (not prev_side) or (cur["decimal"] > prev_side["decimal"]):
                            prev[side] = cur
                out["h2h"] = prev
            elif key == "spreads":
                # Each market may carry a single point (e.g., -1.5/ +1.5)
                pt = _to_float(m.get("outcomes", [{}])[0].get("point")) if m.get("outcomes") else None
                best_home = _best_price_outcome(m, "home")
                best_away = _best_price_outcome(m, "away")
                if best_home: best_home["book"] = bname
                if best_away: best_away["book"] = bname
                out["spreads"].append({
                    "point": pt,
                    "home": best_home,
                    "away": best_away,
                })
    return out

def _is_runline(league: str, point: Optional[float]) -> bool:
    return league.lower() == "mlb" and point is not None and abs(point - 1.5) < 1e-9

# ---------- Engine prob lookup for lines ----------
def _get_engine_prob_line(
    signals_for_event: Dict[str, Any],
    market_key: str,
    side: str,
    point: Optional[float] = None
) -> Optional[float]:
    """
    Expected structure (if you provide):
      signals["moneyline"]["home"] = 0.58
      signals["spread"]["-1.5"]["home"] = 0.55
    """
    if not signals_for_event:
        return None
    mk = signals_for_event.get(market_key)
    if not mk:
        return None
    if market_key == "moneyline":
        return _to_float(mk.get(side))
    elif market_key == "spread":
        # stringify point to avoid float key issues
        key = f"{point:.1f}" if point is not None else None
        if key and key in mk:
            return _to_float(mk[key].get(side))
    return None

# ---------- EV builder for lines ----------
def _ev_lines_for_event(event: Dict[str, Any], league: str, signals: Optional[Dict[str, Any]], ev_min: float) -> List[Dict[str, Any]]:
    """
    Build EV candidates (moneyline, spreads) for a single event using engine probs.
    Only returns items with EV >= ev_min.
    """
    plays: List[Dict[str, Any]] = []
    best = _collect_best_from_event(event)
    home_team = event.get("home_team")
    away_team = event.get("away_team")
    ev_id = event.get("id")

    sigs = signals or {}

    # Moneyline (h2h)
    if isinstance(best.get("h2h"), dict):
        for side in ("home", "away"):
            offer = best["h2h"].get(side)
            if not offer: continue
            p = _get_engine_prob_line(sigs, "moneyline", side)
            if p is None: continue  # cannot compute EV
            # No-vig for ML if both sides exist
            imp_side = offer["implied"]
            other = best["h2h"].get("home" if side == "away" else "away")
            if other:
                imp_a, imp_b = _pair_implied_no_vig(offer["implied"], other["implied"])
                imp_side = imp_a if side == "home" else imp_b
            ev = ev_from_decimal(p, offer["decimal"])
            if ev is None or ev < ev_min: continue
            plays.append({
                "type": "moneyline",
                "event_id": ev_id,
                "side": side,
                "team": home_team if side == "home" else away_team,
                "price": {"american": offer["american"], "decimal": offer["decimal"], "implied": imp_side, "book": offer.get("book")},
                "metrics": {"p": p, "ev": ev},
                "label": f"{home_team} vs {away_team}",
            })

    # Spreads (includes MLB runline)
    for row in best.get("spreads", []) or []:
        pt = row.get("point")
        for side in ("home", "away"):
            offer = row.get(side)
            if not offer: continue
            p = _get_engine_prob_line(sigs, "spread", side, pt)
            if p is None: continue
            # no-vig if both sides are present at the same point
            imp_side = offer["implied"]
            other = row.get("home" if side == "away" else "away")
            if other:
                imp_a, imp_b = _pair_implied_no_vig(offer["implied"], other["implied"])
                imp_side = imp_a if side == "home" else imp_b
            ev = ev_from_decimal(p, offer["decimal"])
            if ev is None or ev < ev_min: continue
            plays.append({
                "type": "runline" if _is_runline(league, pt) else "spread",
                "event_id": ev_id,
                "side": side,
                "team": home_team if side == "home" else away_team,
                "point": pt,
                "price": {"american": offer["american"], "decimal": offer["decimal"], "implied": imp_side, "book": offer.get("book")},
                "metrics": {"p": p, "ev": ev},
                "label": f"{home_team} {('+' if (pt is not None and side=='away' and pt>0) or (pt is not None and side=='home' and pt<0) else '')}{pt or ''} vs {away_team}",
            })

    return plays

# ---------- Aggregator ----------
@ev_plays_bp.get("/api/ev-plays")
def ev_plays_route():
    """
    Returns a unified EV payload for the "EV Plays" tab:
      {
        "date": "YYYY-MM-DD",
        "league": "mlb",
        "props": [... undervalued props only ...],
        "lines": [... EV>=ev_min moneyline/spread/runline ...]
      }

    Query:
      - league (default: mlb)
      - date   (YYYY-MM-DD) [required]
      - min_p, edge_bp_min, ev_min override thresholds
      - novig=1 (apply no-vig to props when both sides present)
    """
    league = (request.args.get("league") or "mlb").lower()
    date_str = request.args.get("date")
    if not date_str:
        return jsonify({"error": "Missing ?date=YYYY-MM-DD"}), 400

    p_min = _to_float(request.args.get("min_p")) or DEFAULT_P_MIN
    edge_min = _to_float(request.args.get("edge_bp_min")) or DEFAULT_EDGE_BP_MIN
    ev_min = _to_float(request.args.get("ev_min")) or DEFAULT_EV_MIN
    use_novig = request.args.get("novig") == "1"

    # Soft import providers
    try:
        from app import fetch_events_odds, fetch_player_props  # type: ignore
    except Exception:
        return jsonify({"error": "Providers missing", "detail": "fetch_events_odds/fetch_player_props not found"}), 500

    # Load data (never crash)
    try:
        events = fetch_events_odds(league, date_str) or []
    except Exception:
        events = []
    try:
        props = fetch_player_props(league, date_str) or []
    except Exception:
        props = []

    # 1) PROPS: use enriched props with >=70% filter
    from app import load_enriched_props
    from routes_line_shopping import american_to_decimal

    min_p = float(request.args.get("min_p", "0.70"))
    ev_min = float(request.args.get("ev_min", "0.00"))

    enriched = load_enriched_props(league, date_str) or []
    props_payload = []
    for p in enriched:
        po = p.get("prob_over"); pu = p.get("prob_under"); shop = p.get("shop") or {}
        side, prob = None, None
        if isinstance(po,(int,float)) and po >= min_p: side, prob = "Over", float(po)
        if side is None and isinstance(pu,(int,float)) and pu >= min_p: side, prob = "Under", float(pu)
        if side is None: continue
        best = shop.get(side.lower()) if isinstance(shop, dict) else None
        if not best: continue
        american = best.get("american"); book = best.get("book")
        if american is None or book is None: continue
        dec = american_to_decimal(int(american)); ev = prob*dec - 1.0
        if ev < ev_min: continue

        props_payload.append({
            "player": p.get("player"),
            "team": p.get("team"),
            "event_id": p.get("event_id"),
            "market": p.get("market"),
            "line": p.get("line"),
            "undervalued": {"any": True, "side": side},
            "shop": {"over": shop.get("over"), "under": shop.get("under")},
            "metrics": {"p": prob, "ev": ev}
        })

    # 2) LINES: compute EV if engine line signals exist
    lines_payload: List[Dict[str, Any]] = []
    fetch_signals = _try_import_engine_signals()
    signals_map = {}
    if fetch_signals:
        try:
            signals_map = fetch_signals(league, date_str) or {}
        except Exception:
            signals_map = {}

    # If no signals, we can't compute EV for lines; skip silently
    if signals_map:
        for ev in events:
            ev_id = ev.get("id")
            sigs = signals_map.get(ev_id) or {}
            try:
                lines_payload.extend(_ev_lines_for_event(ev, league, sigs, ev_min))
            except Exception:
                continue

    # Sort lines by EV desc
    try:
        lines_payload.sort(key=lambda x: -(x.get("metrics", {}).get("ev") or -1e9))
    except Exception:
        pass

    return jsonify({
        "date": date_str,
        "league": league,
        "props": props_payload,
        "lines": lines_payload
    }), 200