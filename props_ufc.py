# props_ufc.py
from __future__ import annotations
from typing import Any, Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

from odds_client_ufc import list_events_ufc, event_markets_ufc, event_odds_ufc
from markets_ufc import UFC_ML_MARKET, UFC_MOV_PATTERNS, MOV_CANON
from novig_multi import novig_two_way, novig_multiway, prob_to_american
from ufc_enrichment import lookup_bio

MAX_WORKERS = int(os.getenv("ODDS_WORKERS", "4"))

def _any_matches(s: str, pats: List[str]) -> bool:
    t = s.lower()
    return any(p in t for p in pats)

def _canonical_bucket(outcome_name: str) -> str | None:
    t = (outcome_name or "").lower()
    for bucket, aliases in MOV_CANON.items():
        for a in aliases:
            if a in t: return bucket
    return None

def _collect_ml(bookmakers: List[Dict[str,Any]], fighters: Tuple[str,str]) -> List[Dict[str,Any]]:
    a, b = fighters
    best = {a: None, b: None}
    for bkr in bookmakers or []:
        bk = bkr.get("key","")
        for m in bkr.get("markets", []):
            if m.get("key") != UFC_ML_MARKET: continue
            for o in m.get("outcomes", []):
                name, price = o.get("name") or o.get("description"), o.get("price")
                if name in (a, b) and price is not None:
                    cur = best.get(name)
                    if (cur is None) or (abs(price) < abs(cur["price"])):
                        best[name] = {"price": int(price), "book": bk}
    rows = []
    if best[a] and best[b]:
        pa, pb = novig_two_way(best[a]["price"], best[b]["price"])
        rows.append({"type":"ml","fighter":a,"opponent":b,
                     "shop":{"ml":{"american":best[a]["price"],"book":best[a]["book"]}},
                     "fair":{"prob":{"ml":pa},"american":{"ml":prob_to_american(pa)}}})
        rows.append({"type":"ml","fighter":b,"opponent":a,
                     "shop":{"ml":{"american":best[b]["price"],"book":best[b]["book"]}},
                     "fair":{"prob":{"ml":pb},"american":{"ml":prob_to_american(pb)}}})
    return rows

def _collect_mov(bookmakers: List[Dict[str,Any]], fighter: str) -> Dict[str, Any]:
    best = {"ko": None, "sub": None, "dec": None}
    for bkr in bookmakers or []:
        bk = bkr.get("key","")
        for m in bkr.get("markets", []):
            k = (m.get("key") or "").lower()
            if not _any_matches(k, UFC_MOV_PATTERNS): continue
            for o in m.get("outcomes", []):
                name = (o.get("name") or o.get("description") or "")
                if fighter.lower() not in name.lower(): continue
                bucket = _canonical_bucket(name)
                if not bucket: continue
                price = o.get("price")
                if price is None: continue
                cur = best[bucket]
                if (cur is None) or (abs(price) < abs(cur["price"])):
                    best[bucket] = {"price": int(price), "book": bk}
    have = [b for b,v in best.items() if v]
    if len(have) < 2: return {}
    odds = [best[b]["price"] for b in ("ko","sub","dec") if best[b]]
    buckets = [b for b in ("ko","sub","dec") if best[b]]
    probs = novig_multiway(odds)
    fair_prob = dict(zip(buckets, probs))
    fair_amer = {b: prob_to_american(p) for b,p in fair_prob.items()}
    return {"buckets": {b: {"american": best[b]["price"], "book": best[b]["book"]} for b in buckets},
            "fair": {"prob": fair_prob, "american": fair_amer}}

def fetch_ufc_props(date: str | None = None) -> List[Dict[str,Any]]:
    events = list_events_ufc(date=date)
    fights: List[Dict[str,Any]] = []

    def _one(ev):
        out = None
        eid = ev.get("id")
        if not eid: return out
        a, b = ev.get("home_team",""), ev.get("away_team","")
        matchup = f"{b} vs {a}" if a and b else (ev.get("commence_time") or "TBD")
        try:
            mk = event_markets_ufc(eid)
            seen_keys = {k for bk in mk.get("bookmakers", []) for k in (bk.get("markets") or [])}
        except Exception:
            seen_keys = set()
        want = [UFC_ML_MARKET]
        mov_keys = [k for k in seen_keys if _any_matches(k, UFC_MOV_PATTERNS)]
        want.extend(sorted(set(mov_keys)))
        data = event_odds_ufc(eid, want)
        bms = data.get("bookmakers", [])
        ml_rows = _collect_ml(bms, (a, b))
        mov_a = _collect_mov(bms, a) if a else {}
        mov_b = _collect_mov(bms, b) if b else {}
        bio_a = lookup_bio(a); bio_b = lookup_bio(b)
        out = {"league":"ufc","event_id":eid,"matchup":matchup,
               "fighters":[
                   {"name":a,"ml":[r for r in ml_rows if r["fighter"]==a],"mov":mov_a,"bio":bio_a},
                   {"name":b,"ml":[r for r in ml_rows if r["fighter"]==b],"mov":mov_b,"bio":bio_b},
               ]}
        return out

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(_one, ev) for ev in events]
        for f in as_completed(futs):
            res = f.result()
            if res: fights.append(res)

    def fav_prob(f):
        probs=[]
        for fx in f["fighters"]:
            for r in fx.get("ml", []):
                p=((r.get("fair") or {}).get("prob") or {}).get("ml")
                if isinstance(p,(int,float)): probs.append(p)
        return max(probs) if probs else 0.0
    fights.sort(key=fav_prob, reverse=True)
    return fights

# Back-compat alias if earlier code called fetch_ufc_markets()
def fetch_ufc_markets(*args, **kwargs):
    return fetch_ufc_props(*args, **kwargs)
