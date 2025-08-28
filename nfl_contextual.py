def add_nfl_context(props):
    contextualized = []

    for prop in props:
        try:
            probability = prop.get("probability", 0)
            stat_type = prop.get("stat_type", "").lower()

            context = {
                **prop,
                "sport": "NFL",
                "type": "yards" if "yard" in stat_type else "touchdowns" if "touchdown" in stat_type else "other",
                "confidence": "High" if probability >= 0.65 else "Moderate" if probability >= 0.55 else "Low"
            }

            contextualized.append(context)
        except Exception as e:
            print("Error adding NFL context:", e)

    return contextualized