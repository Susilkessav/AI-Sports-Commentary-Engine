"""
scripts/simulate_game.py
------------------------
Publishes realistic fake sports events to Kafka so you can
test the full pipeline when no live games are on.

Supports: NBA and Cricket (T20) simulations.

Run:
  SPORT=nba     python scripts/simulate_game.py
  SPORT=cricket python scripts/simulate_game.py
"""

import json
import time
import random
from kafka import KafkaProducer
from config.settings import KAFKA_BOOTSTRAP, TOPIC_RAW_EVENTS, SPORT


# ═══════════════════════════════════════════════════════════════
#  NBA DATA
# ═══════════════════════════════════════════════════════════════

NBA_PLAYS = [
    {"description": "Stephen Curry makes a 3-point jump shot from 28 feet", "athletes": ["Stephen Curry"], "score_value": True, "type": "Three Point Shot"},
    {"description": "LeBron James drives to the basket for a layup", "athletes": ["LeBron James"], "score_value": True, "type": "Layup"},
    {"description": "Jayson Tatum misses a mid-range jumper", "athletes": ["Jayson Tatum"], "score_value": False, "type": "Missed Shot"},
    {"description": "Nikola Jokic finds Jamal Murray with a no-look pass for the dunk", "athletes": ["Nikola Jokic", "Jamal Murray"], "score_value": True, "type": "Dunk"},
    {"description": "Luka Doncic with a step-back three from deep!", "athletes": ["Luka Doncic"], "score_value": True, "type": "Three Point Shot"},
    {"description": "Anthony Edwards steals the ball and goes coast to coast for the slam", "athletes": ["Anthony Edwards"], "score_value": True, "type": "Dunk"},
    {"description": "Shai Gilgeous-Alexander draws the foul, heading to the line", "athletes": ["Shai Gilgeous-Alexander"], "score_value": False, "type": "Foul"},
    {"description": "Giannis Antetokounmpo blocks the shot at the rim!", "athletes": ["Giannis Antetokounmpo"], "score_value": False, "type": "Block"},
    {"description": "Devin Booker pulls up from mid-range, nothing but net", "athletes": ["Devin Booker"], "score_value": True, "type": "Jump Shot"},
    {"description": "Timeout called by the Lakers. What a run by the Celtics!", "athletes": [], "score_value": False, "type": "Timeout"},
    {"description": "Kevin Durant with a turnaround fadeaway over the defender", "athletes": ["Kevin Durant"], "score_value": True, "type": "Fadeaway"},
    {"description": "Tyrese Haliburton threads the needle to Myles Turner for the alley-oop!", "athletes": ["Tyrese Haliburton", "Myles Turner"], "score_value": True, "type": "Alley-Oop"},
]


# ═══════════════════════════════════════════════════════════════
#  CRICKET DATA
# ═══════════════════════════════════════════════════════════════

IPL_TEAMS = {
    "CSK": {
        "full": "Chennai Super Kings",
        "batsmen": [
            "Ruturaj Gaikwad", "Devon Conway", "Ajinkya Rahane",
            "Shivam Dube", "Ravindra Jadeja", "MS Dhoni",
            "Ambati Rayudu", "Moeen Ali", "Dwaine Pretorius",
            "Deepak Chahar", "Tushar Deshpande",
        ],
        "bowlers": [
            "Deepak Chahar", "Tushar Deshpande", "Matheesha Pathirana",
            "Ravindra Jadeja", "Moeen Ali", "Maheesh Theekshana",
        ],
    },
    "MI": {
        "full": "Mumbai Indians",
        "batsmen": [
            "Rohit Sharma", "Ishan Kishan", "Suryakumar Yadav",
            "Tilak Varma", "Tim David", "Hardik Pandya",
            "Kieron Pollard", "Daniel Sams", "Jasprit Bumrah",
            "Piyush Chawla", "Arjun Tendulkar",
        ],
        "bowlers": [
            "Jasprit Bumrah", "Daniel Sams", "Piyush Chawla",
            "Hardik Pandya", "Kumar Kartikeya", "Arjun Tendulkar",
        ],
    },
    "RCB": {
        "full": "Royal Challengers Bengaluru",
        "batsmen": [
            "Virat Kohli", "Faf du Plessis", "Glenn Maxwell",
            "Rajat Patidar", "Dinesh Karthik", "Shahbaz Ahmed",
            "Suyash Prabhudessai", "Wanindu Hasaranga",
            "Harshal Patel", "Mohammed Siraj", "Josh Hazlewood",
        ],
        "bowlers": [
            "Mohammed Siraj", "Josh Hazlewood", "Harshal Patel",
            "Wanindu Hasaranga", "Shahbaz Ahmed", "Glenn Maxwell",
        ],
    },
    "KKR": {
        "full": "Kolkata Knight Riders",
        "batsmen": [
            "Venkatesh Iyer", "Rahmanullah Gurbaz", "Shreyas Iyer",
            "Nitish Rana", "Andre Russell", "Rinku Singh",
            "Sunil Narine", "Shardul Thakur", "Umesh Yadav",
            "Varun Chakravarthy", "Tim Southee",
        ],
        "bowlers": [
            "Umesh Yadav", "Tim Southee", "Shardul Thakur",
            "Sunil Narine", "Varun Chakravarthy", "Andre Russell",
        ],
    },
    "DC": {
        "full": "Delhi Capitals",
        "batsmen": [
            "David Warner", "Prithvi Shaw", "Mitchell Marsh",
            "Rishabh Pant", "Rovman Powell", "Axar Patel",
            "Lalit Yadav", "Shardul Thakur", "Anrich Nortje",
            "Kuldeep Yadav", "Khaleel Ahmed",
        ],
        "bowlers": [
            "Anrich Nortje", "Khaleel Ahmed", "Shardul Thakur",
            "Axar Patel", "Kuldeep Yadav", "Mitchell Marsh",
        ],
    },
    "RR": {
        "full": "Rajasthan Royals",
        "batsmen": [
            "Yashasvi Jaiswal", "Jos Buttler", "Sanju Samson",
            "Devdutt Padikkal", "Shimron Hetmyer", "Riyan Parag",
            "Ravichandran Ashwin", "Trent Boult", "Yuzvendra Chahal",
            "Prasidh Krishna", "Obed McCoy",
        ],
        "bowlers": [
            "Trent Boult", "Prasidh Krishna", "Obed McCoy",
            "Yuzvendra Chahal", "Ravichandran Ashwin", "Riyan Parag",
        ],
    },
    "SRH": {
        "full": "Sunrisers Hyderabad",
        "batsmen": [
            "Travis Head", "Abhishek Sharma", "Rahul Tripathi",
            "Heinrich Klaasen", "Aiden Markram", "Abdul Samad",
            "Washington Sundar", "Bhuvneshwar Kumar",
            "Umran Malik", "T Natarajan", "Marco Jansen",
        ],
        "bowlers": [
            "Bhuvneshwar Kumar", "Umran Malik", "T Natarajan",
            "Marco Jansen", "Washington Sundar", "Aiden Markram",
        ],
    },
    "GT": {
        "full": "Gujarat Titans",
        "batsmen": [
            "Shubman Gill", "Wriddhiman Saha", "Hardik Pandya",
            "David Miller", "Rahul Tewatia", "Abhinav Manohar",
            "Rashid Khan", "Mohammed Shami", "Alzarri Joseph",
            "Noor Ahmad", "Yash Dayal",
        ],
        "bowlers": [
            "Mohammed Shami", "Alzarri Joseph", "Yash Dayal",
            "Rashid Khan", "Noor Ahmad", "Rahul Tewatia",
        ],
    },
    "LSG": {
        "full": "Lucknow Super Giants",
        "batsmen": [
            "KL Rahul", "Quinton de Kock", "Deepak Hooda",
            "Krunal Pandya", "Marcus Stoinis", "Ayush Badoni",
            "Nicholas Pooran", "Mark Wood", "Avesh Khan",
            "Ravi Bishnoi", "Mohsin Khan",
        ],
        "bowlers": [
            "Mark Wood", "Avesh Khan", "Mohsin Khan",
            "Krunal Pandya", "Ravi Bishnoi", "Marcus Stoinis",
        ],
    },
    "PBKS": {
        "full": "Punjab Kings",
        "batsmen": [
            "Shikhar Dhawan", "Jonny Bairstow", "Liam Livingstone",
            "Sam Curran", "Jitesh Sharma", "Shahrukh Khan",
            "Harpreet Brar", "Kagiso Rabada", "Arshdeep Singh",
            "Rahul Chahar", "Nathan Ellis",
        ],
        "bowlers": [
            "Kagiso Rabada", "Arshdeep Singh", "Nathan Ellis",
            "Sam Curran", "Harpreet Brar", "Rahul Chahar",
        ],
    },
}

DISMISSAL_TYPES = [
    "bowled", "caught", "lbw", "run out", "stumped",
]

CAUGHT_DESCRIPTIONS = [
    "edges it and {fielder} takes a sharp catch at slip!",
    "skies it to {fielder} at mid-off!",
    "top-edges the pull and {fielder} takes a comfortable catch at fine leg!",
    "drives loosely and {fielder} dives forward at cover to take a stunner!",
    "lofts it but doesn't get the distance, {fielder} settles under it at long-on!",
]

BOWLED_DESCRIPTIONS = [
    "clean bowled! The stumps are shattered!",
    "bowled through the gate! Didn't read the line at all!",
    "plays all around it and the off stump is pegged back!",
    "beaten by the pace and the leg stump is rattled!",
]

LBW_DESCRIPTIONS = [
    "struck on the pad, big appeal... and the umpire raises the finger! LBW!",
    "trapped in front! That was plumb LBW!",
    "misses the flick, hit on the pad in line with middle stump. OUT! LBW!",
]

RUN_OUT_DESCRIPTIONS = [
    "mix-up between the batsmen! Direct hit at the striker's end! RUN OUT!",
    "pushed to cover, hesitates, and a brilliant throw from {fielder} runs him out!",
    "called for a quick single but the throw is accurate. Run out by inches!",
]

STUMPED_DESCRIPTIONS = [
    "dances down the track but misses, {keeper} is quick to whip the bails off! Stumped!",
    "comes down the wicket but the ball turns past the edge. {keeper} does the rest!",
]


def _over_str(over: int, ball: int) -> str:
    """Format over.ball (0-indexed over, 1-indexed ball)."""
    return f"{over}.{ball}"


def _phase_name(over: int) -> str:
    """Return the phase name for a given over number (0-indexed)."""
    if over < 6:
        return "powerplay"
    elif over < 14:
        return "middle"
    else:
        return "death"


def _ball_outcome_weights(phase: str, chasing: bool, wickets: int, required_rr: float | None):
    """
    Return a dict of outcome -> weight for ball simulation.
    Weights shift based on phase, match situation, and wickets lost.
    """
    # Base weights: dot, 1, 2, 3, 4, 6, wicket
    if phase == "powerplay":
        w = {"dot": 30, "1": 30, "2": 12, "3": 2, "4": 15, "6": 7, "W": 4}
    elif phase == "middle":
        w = {"dot": 35, "1": 32, "2": 10, "3": 2, "4": 10, "6": 4, "W": 7}
    else:  # death
        w = {"dot": 20, "1": 25, "2": 15, "3": 2, "4": 18, "6": 12, "W": 8}

    # If chasing and required run rate is high, batsmen swing harder
    if chasing and required_rr is not None and required_rr > 10:
        aggression = min((required_rr - 10) / 5, 1.0)  # scale 0-1
        w["4"] = int(w["4"] * (1 + aggression * 0.5))
        w["6"] = int(w["6"] * (1 + aggression * 0.8))
        w["W"] = int(w["W"] * (1 + aggression * 0.3))
        w["dot"] = int(w["dot"] * (1 - aggression * 0.3))

    # Lose wickets -> more cautious
    if wickets >= 5:
        w["dot"] = int(w["dot"] * 1.3)
        w["4"] = int(w["4"] * 0.7)
        w["6"] = int(w["6"] * 0.5)

    return w


def _pick_outcome(weights: dict) -> str:
    outcomes = list(weights.keys())
    vals = list(weights.values())
    return random.choices(outcomes, weights=vals, k=1)[0]


def _pick_dismissal(bowler_name: str, fielding_team_batsmen: list) -> tuple[str, str]:
    """Return (dismissal_type, description)."""
    dtype = random.choice(DISMISSAL_TYPES)
    fielder = random.choice(fielding_team_batsmen[:6])  # top-order more likely fielders

    if dtype == "bowled":
        desc = f"{bowler_name} to the batsman, " + random.choice(BOWLED_DESCRIPTIONS)
    elif dtype == "caught":
        template = random.choice(CAUGHT_DESCRIPTIONS)
        desc = f"{bowler_name} to the batsman, " + template.format(fielder=fielder)
    elif dtype == "lbw":
        desc = f"{bowler_name} to the batsman, " + random.choice(LBW_DESCRIPTIONS)
    elif dtype == "run out":
        template = random.choice(RUN_OUT_DESCRIPTIONS)
        desc = template.format(fielder=fielder)
    else:  # stumped
        keeper = fielding_team_batsmen[5]  # approximate keeper position
        template = random.choice(STUMPED_DESCRIPTIONS)
        desc = template.format(keeper=keeper)

    return dtype, desc


# ═══════════════════════════════════════════════════════════════
#  NBA SIMULATION
# ═══════════════════════════════════════════════════════════════

def simulate_nba(producer: KafkaProducer):
    """Run the NBA game simulation."""
    print("[Simulator] Mode: NBA")
    print("=" * 60)

    game_id = "SIM-NBA-001"
    home_score = 0
    away_score = 0
    period = 1
    clock_min = 12

    for i in range(len(NBA_PLAYS)):
        play = NBA_PLAYS[i]

        # Update score
        if play["score_value"]:
            pts = 3 if "3-point" in play["description"] or "three" in play["description"].lower() else 2
            if random.random() > 0.5:
                home_score += pts
            else:
                away_score += pts

        # Update clock
        clock_min = max(0, clock_min - random.randint(0, 2))
        clock_sec = random.randint(0, 59)
        clock_str = f"{clock_min}:{clock_sec:02d}"

        if clock_min == 0 and i > 0 and i % 4 == 0:
            period = min(period + 1, 4)
            clock_min = 12

        event = {
            "event_type": "play",
            "game_id": game_id,
            "sport": "nba",
            "home_team": "Boston Celtics",
            "away_team": "Los Angeles Lakers",
            "home_score": str(home_score),
            "away_score": str(away_score),
            "play_id": f"play-{i+1}",
            "period": period,
            "clock": clock_str,
            "team": play.get("athletes", [""])[0].split()[-1] if play.get("athletes") else "",
            **play,
        }

        producer.send(TOPIC_RAW_EVENTS, key=game_id, value=event)
        producer.flush()

        print(f"  [{clock_str}] Q{period} | {away_score}-{home_score} | {play['description'][:70]}")
        time.sleep(5)

    print(f"\n[Simulator] Done -- sent {len(NBA_PLAYS)} plays.")


# ═══════════════════════════════════════════════════════════════
#  CRICKET T20 SIMULATION
# ═══════════════════════════════════════════════════════════════

def simulate_cricket(producer: KafkaProducer):
    """Run a full T20 cricket match simulation with two innings."""
    # Pick two random IPL teams
    team_keys = random.sample(list(IPL_TEAMS.keys()), 2)
    home_key, away_key = team_keys
    home = IPL_TEAMS[home_key]
    away = IPL_TEAMS[away_key]

    print(f"[Simulator] Mode: Cricket T20")
    print(f"[Simulator] {home['full']} ({home_key}) vs {away['full']} ({away_key})")
    print("=" * 60)

    # Toss
    toss_winner = random.choice([home_key, away_key])
    toss_decision = random.choice(["bat", "bowl"])
    print(f"[Toss] {toss_winner} won the toss and elected to {toss_decision}")
    print()

    if toss_decision == "bat":
        batting_order = [(home_key, home, away_key, away), (away_key, away, home_key, home)]
    else:
        batting_order = [(away_key, away, home_key, home), (home_key, home, away_key, away)]

    game_id = f"SIM-T20-{home_key}-{away_key}"
    innings_scores = []  # (runs, wickets) for each innings
    ball_count = 0

    for innings_num, (bat_key, bat_team, bowl_key, bowl_team) in enumerate(batting_order, start=1):
        runs = 0
        wickets = 0
        extras = 0
        batsman_idx = 0  # index into batting order
        non_striker_idx = 1
        target = innings_scores[0][0] + 1 if innings_num == 2 else None
        chasing = innings_num == 2

        # Milestones already announced
        announced_50 = False
        announced_100 = False
        announced_team_100 = False
        announced_team_150 = False
        announced_team_200 = False

        # Per-batsman runs tracking
        batsman_runs = {}
        for name in bat_team["batsmen"]:
            batsman_runs[name] = 0

        print(f"\n{'='*60}")
        print(f"  INNINGS {innings_num}: {bat_team['full']} batting")
        if chasing:
            print(f"  Target: {target}")
        print(f"{'='*60}\n")

        for over in range(20):
            phase = _phase_name(over)

            # Strategic timeout at over 6 and 14
            if over in (6, 14):
                ball_count += 1
                timeout_event = {
                    "event_type": "play",
                    "game_id": game_id,
                    "sport": "cricket",
                    "home_team": home["full"],
                    "away_team": away["full"],
                    "home_score": _format_cricket_score(innings_num, innings_scores, runs, wickets, bat_key, home_key),
                    "away_score": _format_cricket_score_other(innings_num, innings_scores, runs, wickets, bat_key, home_key),
                    "play_id": f"sim-{ball_count}",
                    "period": innings_num,
                    "clock": f"{over}.0 ov",
                    "team": bat_key,
                    "description": f"Strategic timeout. {bat_team['full']} are {runs}/{wickets} after {over} overs.",
                    "score_value": False,
                    "athletes": [],
                    "type": "timeout",
                }
                producer.send(TOPIC_RAW_EVENTS, key=game_id, value=timeout_event)
                producer.flush()
                print(f"  >>> STRATEGIC TIMEOUT at {over} overs | {bat_key} {runs}/{wickets}")
                time.sleep(3)

            # Pick bowler for this over
            bowler = random.choice(bowl_team["bowlers"])

            for ball in range(1, 7):
                if wickets >= 10:
                    break

                ball_count += 1
                batsman = bat_team["batsmen"][batsman_idx]

                # Calculate required run rate for 2nd innings
                required_rr = None
                if chasing and target is not None:
                    balls_remaining = (20 - over) * 6 - ball + 1
                    runs_needed = target - runs
                    if balls_remaining > 0:
                        required_rr = (runs_needed / balls_remaining) * 6
                    else:
                        required_rr = 99.0

                weights = _ball_outcome_weights(phase, chasing, wickets, required_rr)
                outcome = _pick_outcome(weights)

                score_value = False
                event_type_str = "runs"

                if outcome == "dot":
                    desc = f"{bowler} to {batsman}, no run. Defended back to the bowler."
                    score_value = False
                    run_scored = 0

                elif outcome == "1":
                    desc = f"{bowler} to {batsman}, 1 run. Pushed to the off side for a single."
                    score_value = True
                    run_scored = 1
                    # Rotate strike
                    batsman_idx, non_striker_idx = non_striker_idx, batsman_idx

                elif outcome == "2":
                    desc = f"{bowler} to {batsman}, 2 runs. Worked through midwicket, they come back for two."
                    score_value = True
                    run_scored = 2

                elif outcome == "3":
                    desc = f"{bowler} to {batsman}, 3 runs. Driven past cover, good running between the wickets!"
                    score_value = True
                    run_scored = 3
                    batsman_idx, non_striker_idx = non_striker_idx, batsman_idx

                elif outcome == "4":
                    boundaries = [
                        f"FOUR! {batsman} crunches it through the covers!",
                        f"FOUR! {batsman} pulls it to the midwicket boundary!",
                        f"FOUR! {batsman} drives beautifully past mid-off!",
                        f"FOUR! {batsman} cuts it past point, races to the boundary!",
                        f"FOUR! {batsman} flicks it off the pads, fine leg has no chance!",
                    ]
                    desc = f"{bowler} to {batsman}, " + random.choice(boundaries)
                    score_value = True
                    run_scored = 4
                    event_type_str = "boundary"

                elif outcome == "6":
                    sixes = [
                        f"SIX! {batsman} launches it over long-on into the stands!",
                        f"SIX! {batsman} smashes it over midwicket, that's massive!",
                        f"SIX! {batsman} slog-sweeps it into the crowd at deep square leg!",
                        f"SIX! {batsman} steps out and lofts it straight back over the bowler's head!",
                        f"SIX! {batsman} picks it up off the toes and deposits it into the second tier!",
                    ]
                    desc = f"{bowler} to {batsman}, " + random.choice(sixes)
                    score_value = True
                    run_scored = 6
                    event_type_str = "boundary"

                else:  # W - wicket
                    dismissal_type, dismissal_desc = _pick_dismissal(bowler, bowl_team["batsmen"])
                    desc = f"OUT! {batsman} is {dismissal_type}! {dismissal_desc}"
                    score_value = False
                    run_scored = 0
                    event_type_str = "wicket"
                    wickets += 1
                    # New batsman comes in
                    batsman_idx = min(batsman_idx, non_striker_idx)  # keep lower idx as non-striker
                    new_bat_idx = max(batsman_idx, non_striker_idx) + 1
                    if wickets < 10 and new_bat_idx < len(bat_team["batsmen"]):
                        if batsman_idx == min(batsman_idx, non_striker_idx):
                            # dismissed batsman was at striker end
                            batsman_idx = new_bat_idx
                        else:
                            non_striker_idx = new_bat_idx

                runs += run_scored
                if outcome != "W":
                    batsman_runs[batsman if outcome not in ("1", "3") else bat_team["batsmen"][non_striker_idx]] += run_scored

                clock_str = f"{_over_str(over, ball)} ov"

                # Build score strings
                home_score_str = _format_cricket_score(innings_num, innings_scores, runs, wickets, bat_key, home_key)
                away_score_str = _format_cricket_score_other(innings_num, innings_scores, runs, wickets, bat_key, home_key)

                event = {
                    "event_type": "play",
                    "game_id": game_id,
                    "sport": "cricket",
                    "home_team": home["full"],
                    "away_team": away["full"],
                    "home_score": home_score_str,
                    "away_score": away_score_str,
                    "play_id": f"sim-{ball_count}",
                    "period": innings_num,
                    "clock": clock_str,
                    "team": bat_key,
                    "description": desc,
                    "score_value": score_value,
                    "athletes": [batsman, bowler],
                    "type": event_type_str,
                }

                producer.send(TOPIC_RAW_EVENTS, key=game_id, value=event)
                producer.flush()

                # Console output
                rr_str = ""
                if chasing and required_rr is not None:
                    rr_str = f" | RRR: {required_rr:.2f}"
                print(f"  [{clock_str}] {bat_key} {runs}/{wickets} | {desc[:75]}{rr_str}")
                time.sleep(3)

                # --- Milestone checks ---
                # Batsman 50 / 100
                current_batsman_for_milestone = batsman if outcome not in ("1", "3") else bat_team["batsmen"][non_striker_idx]
                br = batsman_runs.get(current_batsman_for_milestone, 0)
                if br >= 100 and not announced_100:
                    announced_100 = True
                    ball_count += 1
                    _send_milestone(producer, game_id, innings_num, over, ball, home, away,
                                    home_score_str, away_score_str, bat_key,
                                    f"CENTURY! {current_batsman_for_milestone} reaches 100! What an innings!",
                                    [current_batsman_for_milestone, bowler], ball_count)
                    print(f"  *** CENTURY for {current_batsman_for_milestone}! ***")
                    time.sleep(3)
                elif br >= 50 and not announced_50:
                    announced_50 = True
                    ball_count += 1
                    _send_milestone(producer, game_id, innings_num, over, ball, home, away,
                                    home_score_str, away_score_str, bat_key,
                                    f"FIFTY! {current_batsman_for_milestone} brings up a well-made half-century!",
                                    [current_batsman_for_milestone, bowler], ball_count)
                    print(f"  *** FIFTY for {current_batsman_for_milestone}! ***")
                    time.sleep(3)

                # Team milestones
                if runs >= 200 and not announced_team_200:
                    announced_team_200 = True
                    ball_count += 1
                    _send_milestone(producer, game_id, innings_num, over, ball, home, away,
                                    home_score_str, away_score_str, bat_key,
                                    f"{bat_team['full']} have crossed 200! A commanding innings!",
                                    [batsman, bowler], ball_count)
                    print(f"  *** {bat_key} cross 200! ***")
                    time.sleep(3)
                elif runs >= 150 and not announced_team_150:
                    announced_team_150 = True
                    ball_count += 1
                    _send_milestone(producer, game_id, innings_num, over, ball, home, away,
                                    home_score_str, away_score_str, bat_key,
                                    f"{bat_team['full']} reach 150! Building a strong total.",
                                    [batsman, bowler], ball_count)
                    print(f"  *** {bat_key} reach 150! ***")
                    time.sleep(3)
                elif runs >= 100 and not announced_team_100:
                    announced_team_100 = True
                    ball_count += 1
                    _send_milestone(producer, game_id, innings_num, over, ball, home, away,
                                    home_score_str, away_score_str, bat_key,
                                    f"{bat_team['full']} bring up the 100! Steady progress.",
                                    [batsman, bowler], ball_count)
                    print(f"  *** {bat_key} reach 100! ***")
                    time.sleep(3)

                # Check if target chased
                if chasing and runs >= target:
                    print(f"\n  {bat_team['full']} win by {10 - wickets} wickets!")
                    break

            # End of over
            if wickets >= 10 or (chasing and runs >= target):
                break

            # Rotate strike at end of over
            batsman_idx, non_striker_idx = non_striker_idx, batsman_idx

            # Over summary
            current_rr = runs / (over + 1)
            print(f"  --- End of Over {over + 1} | {bat_key}: {runs}/{wickets} | Run Rate: {current_rr:.2f} ---")

        # End of innings
        innings_scores.append((runs, wickets))
        overs_faced = min(over + 1, 20)
        print(f"\n  INNINGS {innings_num} TOTAL: {bat_team['full']} {runs}/{wickets} ({overs_faced} overs)")

        if innings_num == 1:
            print(f"\n{'='*60}")
            print(f"  INNINGS BREAK")
            print(f"  {batting_order[1][1]['full']} need {runs + 1} runs to win")
            print(f"{'='*60}")
            # Send innings break event
            ball_count += 1
            break_event = {
                "event_type": "play",
                "game_id": game_id,
                "sport": "cricket",
                "home_team": home["full"],
                "away_team": away["full"],
                "home_score": _format_cricket_score(1, innings_scores, 0, 0, batting_order[1][0], home_key),
                "away_score": _format_cricket_score_other(1, innings_scores, 0, 0, batting_order[1][0], home_key),
                "play_id": f"sim-{ball_count}",
                "period": 1,
                "clock": "20.0 ov",
                "team": "",
                "description": f"Innings break. {batting_order[1][1]['full']} need {runs + 1} to win from 120 balls.",
                "score_value": False,
                "athletes": [],
                "type": "milestone",
            }
            producer.send(TOPIC_RAW_EVENTS, key=game_id, value=break_event)
            producer.flush()
            time.sleep(3)

    # Match result
    first_runs, first_wkts = innings_scores[0]
    second_runs, second_wkts = innings_scores[1]
    first_bat_team = batting_order[0][1]["full"]
    second_bat_team = batting_order[1][1]["full"]

    print(f"\n{'='*60}")
    if second_runs >= first_runs + 1:
        margin = 10 - second_wkts
        print(f"  RESULT: {second_bat_team} win by {margin} wickets!")
    elif first_runs > second_runs:
        margin = first_runs - second_runs
        print(f"  RESULT: {first_bat_team} win by {margin} runs!")
    else:
        print(f"  RESULT: Match tied!")
    print(f"  {first_bat_team}: {first_runs}/{first_wkts}")
    print(f"  {second_bat_team}: {second_runs}/{second_wkts}")
    print(f"{'='*60}")
    print(f"\n[Simulator] Done -- sent {ball_count} events.")


def _format_cricket_score(innings_num, innings_scores, current_runs, current_wickets, bat_key, home_key):
    """Format score string for the home team."""
    if bat_key == home_key:
        return f"{current_runs}/{current_wickets}"
    else:
        # Home team batted first or hasn't batted yet
        if innings_num == 1:
            return "0/0"
        else:
            return f"{innings_scores[0][0]}/{innings_scores[0][1]}"


def _format_cricket_score_other(innings_num, innings_scores, current_runs, current_wickets, bat_key, home_key):
    """Format score string for the away team."""
    if bat_key != home_key:
        return f"{current_runs}/{current_wickets}"
    else:
        if innings_num == 1:
            return "0/0"
        else:
            return f"{innings_scores[0][0]}/{innings_scores[0][1]}"


def _send_milestone(producer, game_id, innings_num, over, ball, home, away,
                    home_score_str, away_score_str, bat_key, desc, athletes, ball_count):
    """Send a milestone event to Kafka."""
    event = {
        "event_type": "play",
        "game_id": game_id,
        "sport": "cricket",
        "home_team": home["full"],
        "away_team": away["full"],
        "home_score": home_score_str,
        "away_score": away_score_str,
        "play_id": f"sim-{ball_count}",
        "period": innings_num,
        "clock": f"{_over_str(over, ball)} ov",
        "team": bat_key,
        "description": desc,
        "score_value": False,
        "athletes": athletes,
        "type": "milestone",
    }
    producer.send(TOPIC_RAW_EVENTS, key=game_id, value=event)
    producer.flush()


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    print(f"[Simulator] Connected to Kafka at {KAFKA_BOOTSTRAP}")
    print(f"[Simulator] Publishing to topic: {TOPIC_RAW_EVENTS}")
    print(f"[Simulator] Sport: {SPORT}\n")

    if SPORT == "cricket":
        simulate_cricket(producer)
    else:
        simulate_nba(producer)

    producer.close()


if __name__ == "__main__":
    main()
