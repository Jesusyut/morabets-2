# novig_multi.py
from __future__ import annotations
from typing import List

def american_to_prob(odds: int | float) -> float:
    o = float(odds)
    return 100.0/(o+100.0) if o > 0 else (-o)/(100.0 - o)

def prob_to_american(p: float) -> int:
    if p <= 0 or p >= 1: return 0
    return int(round(-100*p/(1-p))) if p >= 0.5 else int(round(100*(1-p)/p))

def novig_two_way(a: int, b: int) -> tuple[float,float]:
    pa, pb = american_to_prob(a), american_to_prob(b)
    z = (pa + pb) or 1.0
    return pa/z, pb/z

def novig_multiway(odds_list: List[int]) -> List[float]:
    probs = [american_to_prob(o) for o in odds_list]
    z = sum(probs) or 1.0
    return [p/z for p in probs]
