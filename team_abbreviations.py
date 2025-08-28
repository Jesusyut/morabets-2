"""
MLB Team Abbreviations Mapping
Official 3-letter abbreviations for all 30 MLB teams
"""

TEAM_ABBREVIATIONS = {
    # American League East
    "Boston Red Sox": "BOS",
    "New York Yankees": "NYY",
    "Tampa Bay Rays": "TB",
    "Toronto Blue Jays": "TOR",
    "Baltimore Orioles": "BAL",
    
    # American League Central
    "Chicago White Sox": "CHW",
    "Cleveland Guardians": "CLE",
    "Detroit Tigers": "DET",
    "Kansas City Royals": "KC",
    "Minnesota Twins": "MIN",
    
    # American League West
    "Houston Astros": "HOU",
    "Los Angeles Angels": "LAA",
    "Oakland Athletics": "OAK",
    "Seattle Mariners": "SEA",
    "Texas Rangers": "TEX",
    
    # National League East
    "Atlanta Braves": "ATL",
    "Miami Marlins": "MIA",
    "New York Mets": "NYM",
    "Philadelphia Phillies": "PHI",
    "Washington Nationals": "WSH",
    
    # National League Central
    "Chicago Cubs": "CHC",
    "Cincinnati Reds": "CIN",
    "Milwaukee Brewers": "MIL",
    "Pittsburgh Pirates": "PIT",
    "St. Louis Cardinals": "STL",
    
    # National League West
    "Arizona Diamondbacks": "ARI",
    "Colorado Rockies": "COL",
    "Los Angeles Dodgers": "LAD",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF"
}

def get_team_abbreviation(full_name):
    """Convert full team name to 3-letter abbreviation"""
    return TEAM_ABBREVIATIONS.get(full_name, full_name[:3].upper())

def format_matchup(away_team, home_team):
    """Format matchup using team abbreviations"""
    away_abbr = get_team_abbreviation(away_team)
    home_abbr = get_team_abbreviation(home_team)
    return f"{away_abbr} @ {home_abbr}"