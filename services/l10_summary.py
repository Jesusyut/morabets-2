from typing import List, Dict, Any

def _passes_line(value: float, line) -> bool:
    try:
        return float(value) >= float(line)
    except:
        return False

# --- NEW: market -> StatsAPI stat key normalization ---
# Covers current batter & pitcher markets; add as you introduce more.
MARKET_TO_STAT = {
    # Batters
    "player_hits": "hits",
    "batter_hits": "hits",            # alias sometimes coming from FE
    "player_total_bases": "totalBases",
    "player_rbis": "rbi",
    "player_runs": "runs",
    "player_walks": "baseOnBalls",
    "player_stolen_bases": "stolenBases",
    "player_home_runs": "homeRuns",
    "player_singles": "singles",
    "player_doubles": "doubles",
    "player_triples": "triples",

    # Pitchers (gameLog 'pitching' group)
    "player_strikeouts": "strikeOuts",
    "player_hits_allowed": "hits",    # if you show HA props later
    "player_walks_allowed": "baseOnBalls",
    "player_earned_runs": "earnedRuns",
    "player_outs": "outs",            # sometimes used for pitching outs
}

def _extract_value(game: dict, market: str) -> float:
    # Normalize the market into a StatsAPI game key
    # 1) exact mapping
    stat_key = MARKET_TO_STAT.get(market)
    if not stat_key:
        # 2) fallback: strip common prefixes and try again
        stripped = market.replace("player_", "").replace("batter_", "")
        stat_key = MARKET_TO_STAT.get(stripped, stripped)

    # pull from the game dict; StatsAPI uses camelCase for many fields
    raw = game.get(stat_key, 0)
    try:
        return float(raw or 0)
    except Exception:
        return 0.0

def summarize_l10(games: List[Dict[str, Any]], market: str, line) -> Dict[str, Any]:
    last = (games or [])[-10:]
    if not last:
        return {"count": 0, "over_rate": None, "avg": None, "series": []}

    over_cnt, vals, series = 0, [], []
    for g in last:
        v = _extract_value(g, market)
        ok = _passes_line(v, line)
        over_cnt += 1 if ok else 0
        vals.append(v)
        series.append({
            "date": g.get("date"),
            "opp": g.get("opponent"),
            "value": v,
            "over": bool(ok),
        })

    n = len(last)
    avg = round(sum(vals)/n, 3)
    over_rate = round(over_cnt/n, 3)
    return {"count": n, "over_rate": over_rate, "avg": avg, "series": series} 