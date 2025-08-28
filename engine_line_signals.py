# engine_line_signals.py
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
import math
import statistics as stats
import random

# --- Tunables (safe defaults; make env-driven later) ---
K_LOGIT_TO_RATE = 0.75   # how strongly win prob skews λ_home vs λ_away
MC_SIMS = 8000           # Poisson Monte Carlo sample size (speed/accuracy trade-off)

# ---------- Odds math ----------
def _to_float(x: Any) -> Optional[float]:
    try:
        f = float(x)
        if math.isfinite(f):
            return f
    except Exception:
        pass
    return None

def implied_from_american(american: Any) -> Optional[float]:
    a = _to_float(american)
    if a is None: return None
    return 100.0/(a+100.0) if a > 0 else abs(a)/(abs(a)+100.0)

def _novig_pair(p_a: Optional[float], p_b: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    if p_a is None or p_b is None: return (None, None)
    s = p_a + p_b
    if s and s > 0:
        return (p_a/s, p_b/s)
    return (None, None)

# ---------- Extractors from event.market blobs ----------
def _best_outcome(market: Dict[str,Any], side_name: str) -> Optional[Dict[str,Any]]:
    best = None
    for o in market.get("outcomes", []) or []:
        if str(o.get("name","")).lower() != side_name:
            continue
        imp = implied_from_american(o.get("price"))
        if imp is None: continue
        cand = {
            "implied": imp,
            "american": int(_to_float(o.get("price")) or 0),
            "point": _to_float(o.get("point")),
        }
        # Keep the *best* price = highest decimal odds = lowest implied
        if (best is None) or (cand["implied"] < best["implied"]):
            best = cand
    return best

def _per_book_h2h_novig_probs(market: Dict[str,Any]) -> Optional[Tuple[float,float]]:
    home = _best_outcome(market, "home")
    away = _best_outcome(market, "away")
    if not home or not away: return None
    hp, ap = _novig_pair(home["implied"], away["implied"])
    if hp is None or ap is None: return None
    return (hp, ap)

def _per_book_spread_novig_probs(market: Dict[str,Any]) -> Optional[Tuple[float,float,float]]:
    # returns (point, home_p_nv, away_p_nv) for this book's spreads market
    home = _best_outcome(market, "home")
    away = _best_outcome(market, "away")
    if not home or not away: return None
    pt = home["point"]
    if pt is None:  # try away point if needed
        pt = away["point"]
    hp, ap = _novig_pair(home["implied"], away["implied"])
    if pt is None or hp is None or ap is None: return None
    return (pt, hp, ap)

def _consensus_h2h(event: Dict[str,Any]) -> Optional[Dict[str,float]]:
    pairs = []
    for bk in event.get("bookmakers", []) or []:
        for m in bk.get("markets", []) or []:
            if m.get("key") == "h2h":
                pr = _per_book_h2h_novig_probs(m)
                if pr: pairs.append(pr)
    if not pairs: return None
    home_list = [hp for hp,_ in pairs]
    away_list = [ap for _,ap in pairs]
    return {"home": stats.median(home_list), "away": stats.median(away_list)}

def _consensus_spreads_points(event: Dict[str,Any]) -> Dict[str, Dict[str,float]]:
    # returns { "point_str": {"home": p_nv, "away": p_nv}, ... }
    out: Dict[str, Dict[str,float]] = {}
    temp: Dict[str, List[Tuple[float,float]]] = {}
    for bk in event.get("bookmakers", []) or []:
        for m in bk.get("markets", []) or []:
            if m.get("key") == "spreads":
                row = _per_book_spread_novig_probs(m)
                if not row: continue
                pt, hp, ap = row
                pstr = f"{pt:.1f}"
                temp.setdefault(pstr, []).append((hp, ap))
    for pstr, pairs in temp.items():
        h_list = [hp for hp,_ in pairs]
        a_list = [ap for _,ap in pairs]
        if h_list and a_list:
            out[pstr] = {"home": stats.median(h_list), "away": stats.median(a_list)}
    return out

def _consensus_total(event: Dict[str,Any]) -> Optional[float]:
    # Median of available totals points across books (uses market 'totals')
    pts = []
    for bk in event.get("bookmakers", []) or []:
        for m in bk.get("markets", []) or []:
            if m.get("key") == "totals":
                # totals outcomes carry 'point' (e.g., 9.5)
                for o in m.get("outcomes", []) or []:
                    pt = _to_float(o.get("point"))
                    if pt is not None:
                        pts.append(pt)
    if not pts: return None
    return stats.median(pts)

# ---------- Map win probability + total -> run distribution ----------
def _logit(p: float) -> float:
    p = min(max(p, 1e-6), 1-1e-6)
    return math.log(p/(1-p))

def _inv_logit(z: float) -> float:
    return 1/(1+math.exp(-z))

def _split_totals(total_runs: float, p_home_win: float, k: float = K_LOGIT_TO_RATE) -> Tuple[float,float]:
    """
    Split total runs into λ_home, λ_away. Ratio controlled by k * logit(p_home_win).
    """
    z = k * _logit(p_home_win)
    r = math.exp(z)  # λ_home / λ_away
    lam_home = total_runs * (r / (1 + r))
    lam_away = total_runs - lam_home
    return (lam_home, lam_away)

def _poisson(lam: float) -> int:
    # Minimal Poisson sampler (Knuth) to avoid extra deps; fine for moderate λ.
    L = math.exp(-lam)
    k = 0
    p = 1.0
    while p > L:
        k += 1
        p *= random.random()
    return k - 1

def _mc_cover_prob(lh: float, la: float, side: str, point: float, sims: int = MC_SIMS) -> float:
    """
    Monte Carlo: sample home & away runs ~ Poisson(λ). Evaluate cover event.
    For side='home' with point e.g. -1.5: cover if (home - away) > -point (i.e., > 1.5) => diff >= 2.
    For side='away' with point e.g. +1.5: cover if (home - away) < point  (i.e., < 1.5) => diff <= 1.
    """
    wins = 0
    if side not in ("home", "away"): return 0.0
    for _ in range(sims):
        h = _poisson(lh)
        a = _poisson(la)
        diff = h - a
        if side == "home":
            if diff > -point:  # e.g., -1.5 => diff > 1.5 => diff >= 2
                wins += 1
        else:  # away
            if diff < point:   # e.g., +1.5 => diff < 1.5 => diff <= 1
                wins += 1
    return wins / sims

# ---------- Public: build engine signals ----------
def build_line_engine_signals(league: str, date_str: str, events: List[Dict[str,Any]]) -> Dict[str, Any]:
    """
    Returns:
      {
        "<event_id>": {
          "moneyline": {"home": p, "away": p},
          "spread": { "<point>": {"home": p, "away": p}, ... }
        }, ...
      }
    """
    out: Dict[str, Any] = {}
    for ev in events or []:
        ev_id = ev.get("id")
        if ev_id is None: 
            continue

        # 1) Consensus priors (no-vig) from books
        ml = _consensus_h2h(ev)        # {"home": p, "away": p} or None
        sp_map = _consensus_spreads_points(ev)  # {"-1.5": {...}, "2.5": {...}}
        total = _consensus_total(ev)   # float or None

        # If we can't get moneyline, skip this event entirely (very rare)
        if not ml:
            continue

        # 2) Engine moneyline (start as consensus; hook for adjustments if you add features)
        p_home_engine = ml["home"]
        p_away_engine = ml["away"]

        # TODO (optional): blend private signals on logit scale
        # z = _logit(p_home_engine) + theta0 + theta1*SP_diff + theta2*BullpenRestDiff + ...
        # p_home_engine = _inv_logit(z); p_away_engine = 1 - p_home_engine

        # 3) Engine spreads/runlines
        spread_engine: Dict[str, Dict[str, float]] = {}
        # Derive λ_home/λ_away from total + engine ML
        if total is not None:
            lam_home, lam_away = _split_totals(total, p_home_engine)
            # Ensure at least one spread point exists to report
            pts = set(sp_map.keys())
            # If no spread markets, still produce the standard MLB runline ±1.5
            if not pts and league.lower() == "mlb":
                pts = set(["-1.5", "1.5"])
            for pstr in sorted(pts, key=lambda s: float(s)):
                pt = float(pstr)
                # Convention: home outcome usually carries negative points when favored
                p_home_cover = _mc_cover_prob(lam_home, lam_away, "home", pt)
                p_away_cover = _mc_cover_prob(lam_home, lam_away, "away", pt)
                spread_engine[pstr] = {"home": p_home_cover, "away": p_away_cover}

        out[str(ev_id)] = {
            "moneyline": {"home": p_home_engine, "away": p_away_engine},
            "spread": spread_engine
        }
    return out