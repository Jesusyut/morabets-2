# labels.py
import requests
from typing import Dict, Any, List, Optional
import os
from novig import novig_two_way

SPORT_KEYS = {"mlb":"baseball_mlb","nfl":"americanfootball_nfl","nba":"basketball_nba","nhl":"icehockey_nhl"}

def _abbr(team: str):
    try:
        from team_abbreviations import TEAM_ABBR
        return TEAM_ABBR.get(team, team)
    except Exception:
        return team

def _mk_matchup(away_team: str, home_team: str) -> str:
    a = (_abbr(away_team) or "").strip().replace(" ", "")
    h = (_abbr(home_team) or "").strip().replace(" ", "")
    return f"{a}@{h}"

def fetch_matchup_labels(league: str, books: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    { matchup: {
        favored_team, favored_prob, favored_book,
        total_line, prob_over_total, total_book, high_scoring
    } }
    """
    out: Dict[str, Dict[str, Any]] = {}
    ODDS_API_KEY = os.getenv("ODDS_API_KEY")
    if not ODDS_API_KEY: return out
    sport = SPORT_KEYS.get(league.lower())
    if not sport: return out

    url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": "us",
        "oddsFormat": "american",
        "dateFormat": "iso",
        "bookmakers": ",".join(books),
        "markets": "h2h,totals",
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()

    for ev in r.json() or []:
        away = ev.get("away_team") or ""
        home = ev.get("home_team") or ""
        matchup = _mk_matchup(away, home)

        fav_team = fav_prob = fav_book = None
        tot_line = prob_over = tot_book = None

        for bm in (ev.get("bookmakers") or []):
            book = (bm.get("key") or bm.get("title") or "").lower().replace(" ", "_")
            for mk in (bm.get("markets") or []):
                k = mk.get("key")
                oc = mk.get("outcomes") or []

                if k == "h2h" and len(oc) >= 2:
                    price_home = price_away = None
                    for o in oc:
                        nm = (o.get("name") or "")
                        if nm == home: price_home = int(o.get("price"))
                        if nm == away: price_away = int(o.get("price"))
                    if price_home is not None and price_away is not None:
                        p_home, p_away = novig_two_way(price_home, price_away)
                        if p_home is not None:
                            if p_home >= p_away:
                                if fav_prob is None or p_home > fav_prob:
                                    fav_team, fav_prob, fav_book = _abbr(home), p_home, book
                            else:
                                if fav_prob is None or p_away > fav_prob:
                                    fav_team, fav_prob, fav_book = _abbr(away), p_away, book

                if k == "totals" and len(oc) >= 2:
                    over = under = None; line = None
                    for o in oc:
                        nm = (o.get("name") or "").lower()
                        if o.get("point") is not None:
                            line = float(o["point"])
                        if nm == "over":  over  = int(o.get("price"))
                        if nm == "under": under = int(o.get("price"))
                    if over is not None and under is not None and line is not None:
                        p_over, p_under = novig_two_way(over, under)
                        if p_over is not None:
                            if (prob_over is None) or (abs(p_over-0.5) > abs(prob_over-0.5)):
                                tot_line, prob_over, tot_book = line, p_over, book

        if fav_team or tot_line is not None:
            out[matchup] = {
                "favored_team": fav_team,
                "favored_prob": fav_prob,
                "favored_book": fav_book,
                "total_line": tot_line,
                "prob_over_total": prob_over,
                "total_book": tot_book,
                "high_scoring": bool(tot_line is not None and tot_line >= 9.0),
            }
    return out
