# labels.py
import requests
from collections import defaultdict
from typing import Dict, Any, List, Optional, Tuple
import os
from novig import american_to_prob, novig_two_way

SPORT_KEYS = {
    "mlb": "baseball_mlb",
    "nfl": "americanfootball_nfl",
    "nba": "basketball_nba",
    "nhl": "icehockey_nhl",
}

# Threshold to tag "high scoring" in MLB; you can tune or make it percentile-based.
MLB_HIGH_TOTAL_THRESHOLD = 9.0

def _abbr(team, TEAM_ABBR=None):
    if TEAM_ABBR and team in TEAM_ABBR:
        return TEAM_ABBR[team]
    return team

def fetch_matchup_labels(league: str = "mlb",
                         date_iso: Optional[str] = None,
                         books: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
    """
    Returns per-matchup labels using featured markets (h2h + totals), de-vigged:
      {
        "AWAY@HOME": {
          "favored": {"team":"HOME", "prob":0.61, "book":"draftkings"},
          "total":   {"line": 9.0,  "prob_over":0.53, "book":"fanduel"},
          "high_scoring": true
        },
        ...
      }
    """
    ODDS_API_KEY = os.getenv("ODDS_API_KEY")
    if not ODDS_API_KEY:
        return {}

    books = [b.lower() for b in (books or ["draftkings", "fanduel", "betmgm"])]
    sport_key = SPORT_KEYS.get(league.lower())
    if not sport_key:
        return {}

    # One call for featured markets is allowed here
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": "us",
        "oddsFormat": "american",
        "dateFormat": "iso",
        "bookmakers": ",".join(books),
        "markets": "h2h,totals"
    }
    if date_iso:
        # Not strictly required; Odds API returns upcoming — date filter optional
        params["dateFormat"] = "iso"

    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
    except Exception:
        return {}

    try:
        from team_abbreviations import TEAM_ABBREVIATIONS as TEAM_ABBR
    except Exception:
        TEAM_ABBR = None

    out: Dict[str, Dict[str, Any]] = {}

    for ev in resp.json() or []:
        away = ev.get("away_team") or ""
        home = ev.get("home_team") or ""
        matchup = f"{_abbr(away, TEAM_ABBR)}@{_abbr(home, TEAM_ABBR)}"
        best_fav = None  # (prob, team, book)
        best_tot = None  # (line, prob_over, book)
        # choose best by strongest edge from 0.5
        best_fav_score = 0.0
        best_tot_score = 0.0

        for bm in (ev.get("bookmakers") or []):
            book = (bm.get("key") or bm.get("title") or "").lower().replace(" ", "_")

            # Moneyline
            for mk in (bm.get("markets") or []):
                if mk.get("key") == "h2h":
                    # two outcomes (teams). Map odds:
                    prices: Dict[str, Optional[int]] = {}
                    for oc in (mk.get("outcomes") or []):
                        name = (oc.get("name") or "").strip()
                        price = oc.get("price")
                        if not name or price is None:
                            continue
                        prices[name] = int(price)
                    # Need odds for both teams
                    if away in prices and home in prices:
                        p_away, p_home = novig_two_way(prices[away], prices[home])
                        fav_team, fav_prob = (away, p_away) if p_away >= p_home else (home, p_home)
                        score = abs(fav_prob - 0.5)
                        if fav_prob is not None and score > best_fav_score:
                            best_fav_score = score
                            best_fav = (float(fav_prob), fav_team, book)

                # Totals
                if mk.get("key") == "totals":
                    # There can be multiple totals entries; outcomes carry "Over"/"Under" and point
                    over_price = under_price = None
                    line = mk.get("outcomes")[0].get("point") if mk.get("outcomes") else None
                    for oc in (mk.get("outcomes") or []):
                        name = (oc.get("name") or "").lower()
                        price = oc.get("price")
                        point = oc.get("point")
                        if point is not None:
                            line = point
                        if name == "over":
                            over_price = price
                        elif name == "under":
                            under_price = price
                    if over_price is not None and under_price is not None and line is not None:
                        p_over, p_under = novig_two_way(int(over_price), int(under_price))
                        score = abs(p_over - 0.5)
                        if p_over is not None and score > best_tot_score:
                            best_tot_score = score
                            best_tot = (float(line), float(p_over), book)

        if best_fav or best_tot:
            entry: Dict[str, Any] = {}
            if best_fav:
                entry["favored"] = {"team": best_fav[1], "prob": best_fav[0], "book": best_fav[2]}
            if best_tot:
                entry["total"] = {"line": best_tot[0], "prob_over": best_tot[1], "book": best_tot[2]}
                # static threshold; optional: compute percentile across all lines instead
                entry["high_scoring"] = bool(best_tot[0] >= MLB_HIGH_TOTAL_THRESHOLD)
            out[matchup] = entry

    return out
