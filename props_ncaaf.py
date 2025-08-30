# props_ncaaf.py
from __future__ import annotations
from collections import defaultdict
from typing import Any, Dict, List, Tuple, Optional
import time

from odds_client_ncaaf import list_events_ncaaf, event_odds_ncaaf
from markets_ncaaf import NCAAF_PLAYER_PROP_MARKETS

# --- no-vig utilities (use your local novig if present) ---
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

def prob_to_american(p: float) -> int:
    if p <= 0 or p >= 1: return 0
    return int(round(-100*p/(1-p))) if p >= 0.5 else int(round(100*(1-p)/p))

# --- helpers to pair Over/Under from Odds API shape ---
def _pair_outcomes(bookmakers: List[Dict[str,Any]], stat_key: str) -> dict:
    """
    Return {(player, stat, point): {"over": {...}, "under": {...}}}
    """
    pairs = defaultdict(lambda: {"over": None, "under": None})
    for b in bookmakers or []:
        bkey = b.get("key","")
        for m in b.get("markets", []):
            if m.get("key") != stat_key: 
                continue
            for out in m.get("outcomes", []):
                name = out.get("description") or out.get("name") or ""
                side = (out.get("name") or "").lower()  # "Over"/"Under" or "Yes"/"No"
                point = out.get("point")
                price = out.get("price")
                if not name or price is None:
                    continue
                # normalize sides: anytime TD often uses yes/no; treat yes as over
                if side not in ("over","under"):
                    side = "over" if side in ("yes", "anytime_td") else ("under" if side=="no" else side)
                k = (name, stat_key, point)
                entry = {"book": bkey, "price": int(price), "point": point}
                if side == "over" and not pairs[k]["over"]:
                    pairs[k]["over"] = entry
                elif side == "under" and not pairs[k]["under"]:
                    pairs[k]["under"] = entry
    return pairs

def _attach_fair(row: Dict[str,Any], over: Dict[str,Any] | None, under: Dict[str,Any] | None):
    fair = {"prob": {}, "american": {}}
    if over and under:
        p_over, p_under = novig_two_way(over["price"], under["price"])
        fair["prob"]["over"], fair["prob"]["under"] = p_over, p_under
        fair["american"]["over"] = prob_to_american(p_over)
        fair["american"]["under"] = prob_to_american(p_under)
        row["book"] = over["book"]  # arbitrary: show the over book
    else:
        # single-sided: show implied
        side = "over" if over else "under"
        tick = over or under
        p = american_to_prob(tick["price"])
        fair["prob"][side] = p
        fair["american"][side] = tick["price"]
        row["book"] = tick["book"]
    row["fair"] = fair

def fetch_ncaaf_player_props(hours_ahead: int = 48, date: Optional[str] = None) -> List[Dict[str,Any]]:
    """
    Returns flat list of props with fields:
      league, matchup, player, stat, line, fair{prob,american}, shop{over,under}, book, side
    """
    events = list_events_ncaaf(hours_ahead=hours_ahead, date=date)
    all_props: List[Dict[str,Any]] = []

    # split markets into two batches to keep payloads small
    batches = [NCAAF_PLAYER_PROP_MARKETS[:5], NCAAF_PLAYER_PROP_MARKETS[5:]]
    for ev in events:
        eid = ev.get("id")
        if not eid: 
            continue
        home, away = ev.get("home_team","Home"), ev.get("away_team","Away")
        matchup = f"{away} @ {home}"
        sidebook = {}

        for i, mk in enumerate(batches):
            try:
                if i: time.sleep(1)
                data = event_odds_ncaaf(eid, mk)
                sb = _pair_outcomes(data.get("bookmakers", []), mk[0])  # loop below redoes for all mk
                for stat_key in mk:
                    sb = _pair_outcomes(data.get("bookmakers", []), stat_key)
                    sidebook.update(sb)
            except Exception as e:
                print(f"[NCAAF] warn: event {eid} markets {mk} failed: {e}")

        # rows
        for (player, stat_key, point), sides in sidebook.items():
            over, under = sides.get("over"), sides.get("under")
            row = {
                "league": "ncaaf",
                "matchup": matchup,
                "player": player,
                "stat": stat_key,
                "line": point,
                "shop": {}
            }
            if over:  row["shop"]["over"]  = {"american": over["price"],  "book": over["book"]}
            if under: row["shop"]["under"] = {"american": under["price"], "book": under["book"]}
            row["side"] = "both" if (over and under) else ("over" if over else ("under" if under else "unknown"))
            _attach_fair(row, over, under)
            all_props.append(row)

    # sort strongest first (by fair over prob if present)
    def keyfn(p):
        return ((p.get("fair") or {}).get("prob") or {}).get("over") or 0.0
    all_props.sort(key=keyfn, reverse=True)
    return all_props
