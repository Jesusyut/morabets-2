# cache_ttl.py
from __future__ import annotations
import os, time, json
from typing import Any, Optional

_REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_REST_URL")
_USE_REDIS = False
_r = None

if _REDIS_URL:
    try:
        import redis  # pip install redis
        _r = redis.from_url(_REDIS_URL, decode_responses=True)
        _USE_REDIS = True
    except Exception:
        _r = None
        _USE_REDIS = False

_mem: dict[str, tuple[float, str]] = {}

def setex(key: str, ttl_sec: int, value: Any) -> None:
    s = json.dumps(value, separators=(",", ":"))
    if _USE_REDIS and _r:
        try:
            _r.setex(key, ttl_sec, s)
            return
        except Exception:
            pass
    _mem[key] = (time.time() + ttl_sec, s)

def get(key: str) -> Optional[Any]:
    if _USE_REDIS and _r:
        try:
            s = _r.get(key)
            return None if s is None else json.loads(s)
        except Exception:
            pass
    tup = _mem.get(key)
    if not tup:
        return None
    exp, s = tup
    if time.time() > exp:
        _mem.pop(key, None)
        return None
    try:
        return json.loads(s)
    except Exception:
        return None
