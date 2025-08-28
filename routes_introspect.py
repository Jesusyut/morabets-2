from flask import Blueprint, jsonify, current_app

introspect_bp = Blueprint("introspect", __name__)

@introspect_bp.get("/api/_routes")
def list_routes():
    rules = []
    for r in current_app.url_map.iter_rules():
        if r.endpoint == "static":  # skip Flask static
            continue
        rules.append({
            "rule": str(r),
            "endpoint": r.endpoint,
            "methods": sorted([m for m in r.methods if m not in ("HEAD","OPTIONS")])
        })
    rules.sort(key=lambda x: x["rule"])
    return jsonify({"count": len(rules), "routes": rules}) 