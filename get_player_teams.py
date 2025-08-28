"""
Get real MLB player-to-team mappings from MLB Stats API
"""
import requests
import json

def get_current_mlb_rosters():
    """Fetch current MLB rosters to map players to teams"""
    try:
        # Get all MLB teams
        teams_url = "https://statsapi.mlb.com/api/v1/teams?leagueIds=103,104"
        teams_response = requests.get(teams_url, timeout=10)
        teams_data = teams_response.json()
        
        player_team_map = {}
        
        for team in teams_data.get('teams', []):
            team_name = team.get('name', '')
            team_id = team.get('id')
            
            if not team_id:
                continue
                
            # Get roster for this team
            roster_url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=active"
            try:
                roster_response = requests.get(roster_url, timeout=5)
                roster_data = roster_response.json()
                
                for player_info in roster_data.get('roster', []):
                    player = player_info.get('person', {})
                    player_name = player.get('fullName', '')
                    if player_name:
                        player_team_map[player_name] = team_name
                        
            except Exception as e:
                print(f"[SKIP] Could not get roster for {team_name}: {e}")
                continue
        
        print(f"[INFO] Built player-team mapping for {len(player_team_map)} players")
        return player_team_map
        
    except Exception as e:
        print(f"[ERROR] Failed to build player-team mapping: {e}")
        return {}

if __name__ == "__main__":
    # Test the function
    mapping = get_current_mlb_rosters()
    print(f"Found {len(mapping)} player-team mappings")
    
    # Show some examples
    sample_players = list(mapping.keys())[:5]
    for player in sample_players:
        print(f"{player} -> {mapping[player]}")