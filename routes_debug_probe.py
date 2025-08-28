from flask import Blueprint, request, jsonify

dbg_bp = Blueprint("dbg", __name__)

@dbg_bp.get("/api/_probe")
def probe():
    from app import fetch_events_odds, fetch_player_props
    league = (request.args.get("league") or "mlb").lower()
    date = request.args.get("date")
    if not date:
        return jsonify({"error":"Missing ?date=YYYY-MM-DD"}), 400

    evs = fetch_events_odds(league, date) or []
    props = fetch_player_props(league, date) or []

    first = evs[0] if evs else {}
    has_h2h = any(m.get("key")=="h2h" for bk in first.get("bookmakers",[]) or [] for m in bk.get("markets",[]) or [])
    has_spreads = any(m.get("key")=="spreads" for bk in first.get("bookmakers",[]) or [] for m in bk.get("markets",[]) or [])

    return jsonify({
        "league": league,
        "date": date,
        "events_count": len(evs),
        "props_count": len(props),
        "first_event_id": first.get("id"),
        "first_event_bookmakers": len(first.get("bookmakers",[]) or []),
        "first_event_has_h2h": bool(has_h2h),
        "first_event_has_spreads": bool(has_spreads)
    }) 