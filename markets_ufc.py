# markets_ufc.py
UFC_SPORT_KEY = "mma_mixed_martial_arts"  # The Odds API sport key

# Featured market that's always there
UFC_ML_MARKET = "h2h"

# Method-of-victory keys vary by book; we'll detect per event using /markets.
# We'll include common patterns here to filter the markets list.
UFC_MOV_PATTERNS = [
    "method",            # e.g., "method_of_victory"
    "to_win_by",         # e.g., "to_win_by_ko_tko", "to_win_by_submission"
    "win_by",            # e.g., "win_by_points"
    "victory_method",
]

# Map bookmaker outcome names to our 3 buckets
MOV_CANON = {
    "ko": ["ko", "tko", "ko/tko", "ko or tko", "technical knockout", "knockout"],
    "sub": ["submission", "by submission", "wins by submission"],
    "dec": ["decision", "points", "win on points", "by decision"],
}
