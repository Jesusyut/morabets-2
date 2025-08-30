# ufc_enrichment.py
from __future__ import annotations
import json, os
from typing import Dict, Any

_DB_PATH = os.getenv("UFC_BIO_JSON", "ufc_fighters.json")

def _load() -> Dict[str, Any]:
    if os.path.exists(_DB_PATH):
        try:
            with open(_DB_PATH, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

_BIO = _load()

def lookup_bio(name: str) -> Dict[str, Any]:
    for k, v in _BIO.items():
        if k.lower() == (name or "").lower():
            return {
                "reach": v.get("reach"), "age": v.get("age"),
                "camp": v.get("camp"), "recent_form": v.get("recent_form"),
                "short_notice": bool(v.get("short_notice", False))
            }
    return {"reach": None, "age": None, "camp": None, "recent_form": None, "short_notice": False}
