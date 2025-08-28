from flask import Blueprint, request, jsonify
import datetime
evdiag_bp = Blueprint("evdiag", __name__)

def _to_float(x):
    try:
        if isinstance(x, str): x=x.strip().replace('%','').replace(',','')
        return float(x)
    except: return None

@evdiag_bp.get("/api/_ev-diagnostics")
def ev_diagnostics():
    from app import load_enriched_props as _load
    league=(request.args.get("league") or "mlb").lower()
    date=request.args.get("date") or datetime.date.today().strftime("%Y-%m-%d")
    items=_load(league, date) or []

    total=len(items); p_fields=0; pass55=0; have_shop=0; price_ok=0
    samples=[]
    for p in items:
        po=_to_float(p.get("prob_over")); pu=_to_float(p.get("prob_under"))
        if po is not None or pu is not None: p_fields+=1
        if po and po>1: po/=100.0
        if pu and pu>1: pu/=100.0
        winp=max([v for v in [po,pu] if isinstance(v,(int,float))] or [0])
        if winp>=0.55: pass55+=1
        side="Over" if (po is not None and po>= (pu or -1)) else ("Under" if pu is not None else None)
        shop=p.get("shop") or {}; best=shop.get(side.lower()) if side and isinstance(shop,dict) else None
        if best: 
            have_shop+=1
            a=_to_float(best.get("american"))
            if a is not None: price_ok+=1
        if len(samples)<3 and (side is None or not best):
            samples.append({"player":p.get("player"), "po":po, "pu":pu, "side":side, "shop_ok":bool(best)})
    return jsonify({"league":league,"date":date,"total_enriched":total,"has_prob_field":p_fields,
                    "meets_55pct":pass55,"has_shop_for_side":have_shop,"price_parse_ok":price_ok,"examples":samples}),200 