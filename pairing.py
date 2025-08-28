"""
No-Vig Mode: Market pairing and prop building without enrichment
"""
from collections import defaultdict
from typing import Dict, List, Any, Optional, Tuple
from novig import novig_two_way

DEFAULT_BOOKS = ["draftkings", "fanduel", "betmgm"]

ALLOWED_MARKETS = {
    "mlb": {
        "batter_hits": (0.5, 3.5),
        "batter_home_runs": (0.5, 1.5),
        "batter_total_bases": (0.5, 3.5),
        "pitcher_strikeouts": (1.5, 12.5),
        # add more as needed (e.g., rbis, runs) if the feed exposes them
    }
}

def _market_ok(league: str, stat: str, line: float) -> bool:
    lo_hi = ALLOWED_MARKETS.get(league, {}).get(stat)
    if not lo_hi:
        return False
    lo, hi = lo_hi
    try:
        f = float(line)
    except Exception:
        return False
    return lo <= f <= hi

def build_props_novig(
    league: str,
    raw_offers: List[Dict[str, Any]],
    prefer_books: Optional[List[str]] = None
) -> Dict[str, List[Dict[str, Any]]]:
    """
    raw_offers items must contain:
      event_key, matchup, player, stat, line, side('over'|'under'), odds, book
    Returns:
      { matchup: [ {player, stat, line, odds, shop, fair:{book, prob:{over,under}}} ] }
    """
    books = [b.lower() for b in (prefer_books or DEFAULT_BOOKS)]
    by_key_book: Dict[Tuple, Dict[str, Any]] = defaultdict(dict)

    for o in raw_offers:
        book = (o.get("book") or "").lower()
        if book not in books:
            continue
        stat = o.get("stat")
        line = o.get("line")
        if stat is None or line is None or not _market_ok(league, stat, line):
            continue
        k = (o["event_key"], o["matchup"], o["player"], stat, line, book)
        e = by_key_book[k]
        e.update({
            "event_key": o["event_key"],
            "matchup": o["matchup"],
            "player": o["player"],
            "stat": stat,
            "line": line,
            "book": book
        })
        side = (o.get("side") or "").lower()
        if side == "over":
            e["over_odds"] = int(o["odds"])
        elif side == "under":
            e["under_odds"] = int(o["odds"])

    best: Dict[Tuple, Dict[str, Any]] = {}
    for (ek, mu, pl, st, ln, bk), e in by_key_book.items():
        if "over_odds" not in e or "under_odds" not in e:
            continue
        p_over, p_under = novig_two_way(e["over_odds"], e["under_odds"])
        if p_over is None:
            continue
        key = (ek, mu, pl, st, ln)
        cand = {
            "matchup": mu,
            "player": pl,
            "stat": st,
            "line": ln,
            "odds": e["over_odds"],   # keep legacy display
            "shop": bk,
            "fair": {"book": bk, "prob": {"over": p_over, "under": p_under}},
        }
        score = abs(p_over - 0.5)
        prev = best.get(key)
        if not prev or score > abs(prev["fair"]["prob"]["over"] - 0.5):
            best[key] = cand

    out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for (_, mu, _, _, _), prop in best.items():
        out[mu].append(prop)
    return out
