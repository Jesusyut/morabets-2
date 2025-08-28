from flask import Blueprint, jsonify
import subprocess

ver_bp = Blueprint("ver", __name__)

@ver_bp.get("/api/_version")
def version():
    try:
        commit = subprocess.check_output(["git","rev-parse","--short","HEAD"]).decode().strip()
        branch = subprocess.check_output(["git","rev-parse","--abbrev-ref","HEAD"]).decode().strip()
    except Exception:
        commit = branch = "unknown"
    return jsonify({"branch": branch, "commit": commit}) 