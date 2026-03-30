"""
scripts/test_api.py
────────────────────
Run this FIRST to verify the ESPN API is working
and print live game data to your terminal.

No Kafka needed. No API key needed.

Run:  python scripts/test_api.py
      SPORT=cricket python scripts/test_api.py
"""

from ingestion.espn_client import ESPNClient
from config.settings import SPORT
import json


def main():
    print(f"\n{'='*55}")
    print(f"  ESPN API Test — Sport: {SPORT.upper()}")
    print(f"{'='*55}\n")

    client = ESPNClient(sport=SPORT)

    # 1. Check for live games
    print("Step 1: Checking for live games...")
    games = client.get_live_games()

    if not games:
        print("  No live games right now.")
        print("  Tip: Try during game time, or change SPORT in config/settings.py")
        print("\nStep 2: Fetching scoreboard for structure preview...")

        # Fall back to scoreboard to show structure
        import requests
        from config.settings import ESPN_BASE, ESPN_SPORT_PATHS, CRICKET_LEAGUE_IDS, CRICKET_LEAGUE

        if SPORT == "cricket":
            league_id = CRICKET_LEAGUE_IDS.get(CRICKET_LEAGUE, CRICKET_LEAGUE)
            url = f"{ESPN_BASE}/cricket/{league_id}/scoreboard"
        else:
            path = ESPN_SPORT_PATHS[SPORT]
            url = f"{ESPN_BASE}/{path}/scoreboard"

        data = requests.get(url).json()
        events = data.get("events", [])
        if events:
            e = events[0]
            print(f"\n  Sample game found: {e.get('name', 'N/A')}")
            print(f"  Status: {e.get('status', {}).get('type', {}).get('description', '')}")
            comps = e.get("competitions", [{}])[0].get("competitors", [])
            for c in comps:
                score = c.get("score", "N/A")
                print(f"    {c.get('homeAway','?').upper()}: "
                      f"{c.get('team', {}).get('displayName', '?')} "
                      f"— score: {score}")

                # Show innings detail for cricket
                if SPORT == "cricket":
                    for ls in c.get("linescores", []):
                        print(f"      Innings {ls.get('period')}: "
                              f"{ls.get('runs', 0)}/{ls.get('wickets', 0)} "
                              f"({ls.get('overs', 0)} ov) "
                              f"{'[batting]' if ls.get('isBatting') else ''}"
                              f"{'[current]' if ls.get('isCurrent') else ''}")
        else:
            print("  No events found in scoreboard response.")
        return

    print(f"  {len(games)} live game(s) found!\n")

    for i, game in enumerate(games, 1):
        if SPORT == "cricket":
            print(f"Match {i}: {game['away_team']} vs {game['home_team']}")
            print(f"  Score:    {game['away_score']} / {game['home_score']}")
            print(f"  Status:   {game.get('situation', '')}")
            print(f"  Venue:    {game.get('venue', '')}")
            print(f"  Format:   {game.get('match_format', '')}")
            print(f"  ID:       {game['game_id']}")

            # Show innings breakdown
            for label, key in [("Away", "away_innings"), ("Home", "home_innings")]:
                innings = game.get(key, [])
                if innings:
                    team = game.get(f"{label.lower()}_team", label)
                    print(f"  {team} innings:")
                    for inn in innings:
                        batting = " [batting]" if inn.get("is_batting") else ""
                        current = " [current]" if inn.get("is_current") else ""
                        print(f"    Inn {inn['innings_num']}: "
                              f"{inn['runs']}/{inn['wickets']} "
                              f"({inn['overs']} ov){batting}{current}")
            print()
        else:
            print(f"Game {i}: {game['away_team']} @ {game['home_team']}")
            print(f"  Score:  {game['away_score']} - {game['home_score']}")
            print(f"  Period: {game['period']}  Clock: {game['clock']}")
            print(f"  ID:     {game['game_id']}")
            print()

        # Fetch plays/events for first game
        if i == 1:
            if SPORT == "cricket":
                print("Step 2: Fetching match events...")
                events = client.get_plays(game["game_id"])
                if events:
                    print(f"  {len(events)} event(s) detected. Last 5:\n")
                    for ev in events[-5:]:
                        etype = ev.get("type", "")
                        print(f"    [{ev.get('clock','')}] ({etype}) {ev.get('description','')}")
                        if ev.get("athletes"):
                            print(f"    Players: {', '.join(ev['athletes'])}")
                        print()
                else:
                    print("  No events detected yet (try again after a few overs)")

                print("Step 3: Fetching match summary...")
                summary = client.get_game_summary(game["game_id"])
                if summary:
                    for team, data in summary.items():
                        if team.startswith("_"):
                            continue
                        print(f"\n  {team}:")
                        batting = data.get("batting", [])
                        if batting:
                            print("    Top batters:")
                            for b in batting[:5]:
                                print(f"      {b['name']}: {b['runs']} runs")
                        bowling = data.get("bowling", [])
                        if bowling:
                            print("    Top bowlers:")
                            for b in bowling[:5]:
                                print(f"      {b['name']}: {b['wickets']} wickets")

                    notes = summary.get("_notes", [])
                    if notes:
                        print(f"\n  Notes:")
                        for n in notes[:5]:
                            print(f"    - {n}")
                else:
                    print("  No summary data available")
            else:
                print("Step 2: Fetching recent plays for first game...")
                plays = client.get_plays(game["game_id"])
                if plays:
                    print(f"  {len(plays)} play(s) retrieved. Last 3:\n")
                    for p in plays[-3:]:
                        print(f"    [{p.get('clock','')}] {p.get('description','')}")
                        if p.get("athletes"):
                            print(f"    Players: {', '.join(p['athletes'])}")
                        print()
                else:
                    print("  No plays found (game may just be starting)")

    print("=" * 55)
    print("  API test complete! ESPN API is working.")
    if SPORT == "cricket":
        print("  Cricket mode active. Events are detected via score changes between polls.")
    print("  Next step: run docker-compose up -d  then  python -m ingestion.producer")
    print("=" * 55)


if __name__ == "__main__":
    main()
