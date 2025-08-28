from typing import List, Dict, Any, Optional
import httpx

# ---------- MLB ----------
async def mlb_last10(player_id: int, group: str = "hitting", season: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Returns last 10 MLB games for a player from MLB StatsAPI, flattened into
    [{date, opponent, hits, total_bases, rbis, runs, walks, stolen_bases, strikeouts}, ...]
    Newest last.
    """
    params = {"stats": "gameLog", "group": group}
    if season:
        params["season"] = season
    url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()

    splits = (data.get("stats") or [{}])[0].get("splits") or []
    out: List[Dict[str, Any]] = []
    for s in splits[-10:]:
        g = s.get("stat", {}) or {}
        out.append({
            "date": s.get("date"),
            "opponent": (s.get("opponent") or {}).get("name"),
            "hits": float(g.get("hits", 0) or 0),
            "total_bases": float(g.get("totalBases", 0) or 0),
            "rbis": float(g.get("rbi", 0) or 0),
            "runs": float(g.get("runs", 0) or 0),
            "walks": float(g.get("baseOnBalls", 0) or 0),
            "stolen_bases": float(g.get("stolenBases", 0) or 0),
            "strikeouts": float(g.get("strikeOuts", 0) or 0),
        })
    return out

# ---------- NFL (placeholder) ----------
async def nfl_last10(player_id: str) -> List[Dict[str, Any]]:
    """
    Placeholder for NFL. Returns empty list for now.
    When season starts, replace with paid provider (SportsDataIO/Sportradar) game logs.
    """
    return [] 