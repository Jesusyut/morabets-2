def enrich_nfl_props(raw_props):
    enriched = []

    for game in raw_props:
        for bookmaker in game.get("bookmakers", []):
            for market in bookmaker.get("markets", []):
                stat_type = market.get("key")  # e.g., player_pass_yds
                for outcome in market.get("outcomes", []):
                    try:
                        player_name = outcome["name"]
                        line = float(outcome["point"])
                        odds = float(outcome["price"])
                        probability = 1 / odds if odds > 1 else 0.0
                        team = game.get("home_team") if game.get("home_team") in player_name else game.get("away_team")

                        enriched.append({
                            "player": player_name,
                            "team": team,
                            "opponent": game.get("away_team") if team == game.get("home_team") else game.get("home_team"),
                            "stat_type": stat_type.replace("player_", "").replace("_", " ").title(),
                            "line": line,
                            "odds": odds,
                            "probability": round(probability, 3),
                            "sport": "NFL"
                        })
                    except Exception as e:
                        print("Error enriching NFL prop:", e)
    return enriched