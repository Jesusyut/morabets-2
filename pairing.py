"""
No-Vig Mode: Market pairing and prop building without enrichment
"""
from collections import defaultdict
from typing import Dict, List, Any, Optional, Tuple
from novig import american_to_prob, novig_two_way

DEFAULT_BOOKS = ["draftkings", "fanduel", "betmgm"]

ALLOWED_MARKETS = {
    "mlb": {
        "batter_hits": (0.5, 3.5),
        "batter_home_runs": (0.5, 1.5),
        "batter_total_bases": (0.5, 3.5),
        "pitcher_strikeouts": (1.5, 12.5),
        "rbis": (0.5, 2.5),
        "runs": (0.5, 2.5),
    }
}

def _market_ok(league: str, stat: str, line: float) -> bool:
    rng = ALLOWED_MARKETS.get(league, {}).get(stat)
    if not rng: return False
    try:
        f = float(line)
    except Exception:
        return False
    # hard filter: TB over 1.5 is "useless"—skip anything with line > 1.5
    if stat == "batter_total_bases" and f > 1.5:
        return False
    return rng[0] <= f <= rng[1]

def build_props_novig(
    league: str,
    raw_offers: List[Dict[str, Any]],
    prefer_books: Optional[List[str]] = None,
    allow_crossbook: bool = True,
    allow_single_side_fallback: bool = True,
    default_overround: float = 0.04,
    # new knobs for confidence
    prefer_side: str = "over",           # "over" or "any"
    high_threshold: float = 0.70         # tag >= this as high confidence
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Returns { matchup: [ {player, stat, line, odds, shop, fair:{book,prob{over,under}}, meta{...}, priority_score} ] }
    Added:
      - meta.source: "novig" | "crossbook" | "single_side"
      - meta.flags: ["HIGH_OVER_70", "HIGH_ANY_70"] when applicable
      - priority_score: float for server-side sorting
    """
    books = [b.lower() for b in (prefer_books or DEFAULT_BOOKS)]
    # Aggregate by (event,matchup,player,stat,line)
    by_prop: Dict[Tuple, Dict[str, Dict[str, int]]] = defaultdict(lambda: defaultdict(dict))
    # Measure overround by book from fully paired lines
    book_overround_sum: Dict[str, float] = defaultdict(float)
    book_overround_n: Dict[str, int] = defaultdict(int)

    for o in raw_offers:
        book = (o.get("book") or "").lower()
        if book not in books: 
            continue
        stat = o.get("stat"); line = o.get("line")
        if stat is None or line is None or not _market_ok(league, stat, line):
            continue
        k = (o["event_key"], o["matchup"], o["player"], stat, line)
        side = (o.get("side") or "").lower()
        if side in ("over","under") and o.get("odds") is not None:
            by_prop[k][book][side] = int(o["odds"])

    # Overround per book
    for _, bookmap in by_prop.items():
        for b, sides in bookmap.items():
            if "over" in sides and "under" in sides:
                pO = american_to_prob(sides["over"]) or 0.0
                pU = american_to_prob(sides["under"]) or 0.0
                overround = max(0.0, (pO + pU) - 1.0)
                book_overround_sum[b] += overround
                book_overround_n[b] += 1

    avg_overround = {
        b: (book_overround_sum[b] / book_overround_n[b]) if book_overround_n[b] else default_overround
        for b in books
    }

    out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    def _decorate(prop: Dict[str, Any], source: str) -> Dict[str, Any]:
        """Attach flags and priority based on threshold and preference."""
        p_over = float(prop["fair"]["prob"]["over"])
        p_under = float(prop["fair"]["prob"]["under"])
        flags = []
        # primary: high over
        if p_over >= high_threshold:
            flags.append(f"HIGH_OVER_{int(high_threshold*100)}")
        # secondary: any side high
        if max(p_over, p_under) >= high_threshold:
            flags.append(f"HIGH_ANY_{int(high_threshold*100)}")
        prop.setdefault("meta", {})
        prop["meta"]["source"] = source
        prop["meta"]["flags"] = flags
        # priority: prefer high over, then any high, then by distance from 0.5
        if prefer_side == "over":
            base = (p_over - 0.5)
        else:
            base = (max(p_over, p_under) - 0.5)
        bonus = 1.0 if f"HIGH_OVER_{int(high_threshold*100)}" in flags else (0.5 if f"HIGH_ANY_{int(high_threshold*100)}" in flags else 0.0)
        prop["priority_score"] = round( (base * 2.0) + bonus, 6)  # roughly [-1, +3]
        return prop

    for (ek, mu, pl, st, ln), bookmap in by_prop.items():
        best = None
        best_score = -1.0

        # 1) within-book pairing (gold)
        for b, sides in bookmap.items():
            if "over" in sides and "under" in sides:
                p_over, p_under = novig_two_way(sides["over"], sides["under"])
                if p_over is None:
                    continue
                score = abs(p_over - 0.5)
                cand = {
                    "matchup": mu, "player": pl, "stat": st, "line": ln,
                    "odds": sides["over"], "shop": b,
                    "fair": {"book": b, "prob": {"over": p_over, "under": p_under}},
                }
                if score > best_score:
                    best, best_score = _decorate(cand, "novig"), score

        # 2) cross-book pairing (same line)
        if not best:
            if allow_crossbook:
                overs = [(b, v["over"]) for b,v in bookmap.items() if "over" in v]
                unders = [(b, v["under"]) for b,v in bookmap.items() if "under" in v]
                for bO, oOdds in overs:
                    for bU, uOdds in unders:
                        pO = american_to_prob(oOdds) or 0.0
                        pU = american_to_prob(uOdds) or 0.0
                        s = pO + pU
                        if s <= 0: 
                            continue
                        p_over = round(pO / s, 4); p_under = round(pU / s, 4)
                        score = abs(p_over - 0.5)
                        cand = {
                            "matchup": mu, "player": pl, "stat": st, "line": ln,
                            "odds": oOdds, "shop": f"{bO}~{bU}",
                            "fair": {"book": "crossbook", "prob": {"over": p_over, "under": p_under}},
                            "meta": {"paired": "crossbook", "over_book": bO, "under_book": bU}
                        }
                        if score > best_score:
                            best, best_score = _decorate(cand, "crossbook"), score

        # 3) single-side fallback (estimate missing side by avg overround)
        if not best and allow_single_side_fallback:
            for b, sides in bookmap.items():
                r = avg_overround.get(b, default_overround)
                if "over" in sides and "under" not in sides:
                    pO = american_to_prob(sides["over"]) or 0.0
                    p_over = max(0.01, min(0.99, round(pO / (1.0 + r), 4)))
                    p_under = round(1.0 - p_over, 4)
                    cand = {
                        "matchup": mu, "player": pl, "stat": st, "line": ln,
                        "odds": sides["over"], "shop": b,
                        "fair": {"book": "single_side", "prob": {"over": p_over, "under": p_under}},
                        "meta": {"paired": "single_side", "assumed_overround": r}
                    }
                    best = _decorate(cand, "single_side")
                    break
                if "under" in sides and "over" not in sides:
                    pU = american_to_prob(sides["under"]) or 0.0
                    p_under = max(0.01, min(0.99, round(pU / (1.0 + r), 4)))
                    p_over = round(1.0 - p_under, 4)
                    cand = {
                        "matchup": mu, "player": pl, "stat": st, "line": ln,
                        "odds": sides["under"], "shop": b,
                        "fair": {"book": "single_side", "prob": {"over": p_over, "under": p_under}},
                        "meta": {"paired": "single_side", "assumed_overround": r}
                    }
                    best = _decorate(cand, "single_side")
                    break

        if best:
            out[mu].append(best)

    def _p_over(prop): return float(prop["fair"]["prob"]["over"])

    # optional: pass in `over_only=True` from the route to hide low OVERs
    if locals().get("over_only", False):
        for mu in list(out.keys()):
            out[mu] = [p for p in out[mu] if _p_over(p) >= 0.50]  # you can bump to your min_prob
            if not out[mu]: del out[mu]

    # strict over-first sort
    for mu in out:
        out[mu].sort(key=_p_over, reverse=True)

    return out
