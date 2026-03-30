"""
tests/test_cricket.py
---------------------
Comprehensive tests for cricket functionality in the ESPN client
and the LLM commentator module.
"""

import pytest
from unittest.mock import patch

from ingestion.espn_client import ESPNClient


# ── Helpers ────────────────────────────────────────────────────

def _make_competitor(home_away, team_name, score, linescores=None):
    """Build a fake ESPN competitor dict."""
    comp = {
        "homeAway": home_away,
        "team": {"displayName": team_name},
        "score": score,
    }
    if linescores is not None:
        comp["linescores"] = linescores
    return comp


def _make_event(
    game_id="12345",
    home_name="Mumbai Indians",
    away_name="Chennai Super Kings",
    home_score="185/4",
    away_score="120/3",
    home_linescores=None,
    away_linescores=None,
    state="in",
    short_detail="MI bat - 18.2 ov",
    venue="Wankhede Stadium",
    note="Match 23 of 74",
):
    """Build a fake ESPN scoreboard event for cricket."""
    return {
        "id": game_id,
        "status": {
            "type": {
                "state": state,
                "shortDetail": short_detail,
            }
        },
        "competitions": [
            {
                "competitors": [
                    _make_competitor("home", home_name, home_score, home_linescores or []),
                    _make_competitor("away", away_name, away_score, away_linescores or []),
                ],
                "venue": {"fullName": venue},
                "note": note,
            }
        ],
    }


def _cricket_client():
    """Return an ESPNClient configured for cricket without hitting the network."""
    with patch.dict("os.environ", {"SPORT": "cricket", "CRICKET_LEAGUE": "ipl"}):
        with patch("config.settings.SPORT", "cricket"):
            client = ESPNClient(sport="cricket")
    return client


# ================================================================
#  1. _parse_cricket_game
# ================================================================

class TestParseCricketGame:
    def test_basic_fields(self):
        client = _cricket_client()
        home_ls = [
            {"period": 1, "runs": 185, "wickets": 4, "overs": 18.2,
             "isBatting": True, "isCurrent": 1, "description": "MI 1st innings"},
        ]
        event = _make_event(
            home_linescores=home_ls,
            away_linescores=[],
            home_score="185/4",
            away_score="120/3",
        )
        result = client._parse_cricket_game(event)

        assert result["game_id"] == "12345"
        assert result["sport"] == "cricket"
        assert result["home_team"] == "Mumbai Indians"
        assert result["away_team"] == "Chennai Super Kings"
        assert result["home_score"] == "185/4"
        assert result["away_score"] == "120/3"
        assert result["venue"] == "Wankhede Stadium"
        assert result["match_format"] == "Match 23 of 74"

    def test_innings_data_populated(self):
        client = _cricket_client()
        home_ls = [
            {"period": 1, "runs": 185, "wickets": 4, "overs": 20,
             "isBatting": False, "isCurrent": 0, "description": ""},
        ]
        away_ls = [
            {"period": 2, "runs": 120, "wickets": 3, "overs": 15.4,
             "isBatting": True, "isCurrent": 1, "description": ""},
        ]
        event = _make_event(home_linescores=home_ls, away_linescores=away_ls)
        result = client._parse_cricket_game(event)

        assert len(result["home_innings"]) == 1
        assert result["home_innings"][0]["runs"] == 185
        assert len(result["away_innings"]) == 1
        assert result["away_innings"][0]["runs"] == 120

    def test_current_innings_from_event(self):
        client = _cricket_client()
        home_ls = [
            {"period": 1, "runs": 200, "wickets": 10, "overs": 48.3,
             "isBatting": False, "isCurrent": 0, "description": ""},
        ]
        away_ls = [
            {"period": 2, "runs": 95, "wickets": 2, "overs": 22.1,
             "isBatting": True, "isCurrent": 1, "description": ""},
        ]
        event = _make_event(home_linescores=home_ls, away_linescores=away_ls)
        result = client._parse_cricket_game(event)

        assert result["current_innings"]["innings_num"] == 2
        assert result["current_innings"]["runs"] == 95
        assert result["current_innings"]["wickets"] == 2
        assert result["current_innings"]["overs"] == "22.1"
        assert result["period"] == 2
        assert result["clock"] == "22.1"

    def test_missing_venue(self):
        client = _cricket_client()
        event = _make_event(venue="")
        result = client._parse_cricket_game(event)
        assert result["venue"] == ""


# ================================================================
#  2. _format_innings
# ================================================================

class TestFormatInnings:
    def test_single_innings(self):
        client = _cricket_client()
        linescores = [
            {"period": 1, "runs": 210, "wickets": 7, "overs": 50,
             "isBatting": False, "isCurrent": 0, "description": "All out"},
        ]
        result = client._format_innings(linescores)
        assert len(result) == 1
        assert result[0]["innings_num"] == 1
        assert result[0]["runs"] == 210
        assert result[0]["wickets"] == 7
        assert result[0]["overs"] == 50
        assert result[0]["is_batting"] is False
        assert result[0]["is_current"] is False
        assert result[0]["description"] == "All out"

    def test_multiple_innings(self):
        client = _cricket_client()
        linescores = [
            {"period": 1, "runs": 300, "wickets": 10, "overs": 80,
             "isBatting": False, "isCurrent": 0, "description": ""},
            {"period": 2, "runs": 150, "wickets": 3, "overs": 40,
             "isBatting": True, "isCurrent": 1, "description": ""},
        ]
        result = client._format_innings(linescores)
        assert len(result) == 2
        assert result[0]["runs"] == 300
        assert result[1]["is_current"] is True
        assert result[1]["is_batting"] is True

    def test_empty_linescores(self):
        client = _cricket_client()
        assert client._format_innings([]) == []

    def test_defaults_for_missing_keys(self):
        client = _cricket_client()
        result = client._format_innings([{}])
        assert result[0]["innings_num"] == 0
        assert result[0]["runs"] == 0
        assert result[0]["wickets"] == 0
        assert result[0]["overs"] == 0
        assert result[0]["is_batting"] is False
        assert result[0]["is_current"] is False
        assert result[0]["description"] == ""


# ================================================================
#  3. _get_current_innings
# ================================================================

class TestGetCurrentInnings:
    def test_current_in_home(self):
        client = _cricket_client()
        home_ls = [{"isCurrent": 1, "period": 1, "runs": 50, "wickets": 1, "overs": 8.3, "isBatting": True}]
        away_ls = []
        result = client._get_current_innings(home_ls, away_ls)
        assert result["innings_num"] == 1
        assert result["runs"] == 50
        assert result["overs"] == "8.3"
        assert result["is_batting"] is True

    def test_current_in_away(self):
        client = _cricket_client()
        home_ls = [{"isCurrent": 0, "period": 1, "runs": 200, "wickets": 10, "overs": 50}]
        away_ls = [{"isCurrent": 1, "period": 2, "runs": 80, "wickets": 2, "overs": 20, "isBatting": True}]
        result = client._get_current_innings(home_ls, away_ls)
        assert result["innings_num"] == 2
        assert result["runs"] == 80

    def test_no_current_returns_default(self):
        client = _cricket_client()
        result = client._get_current_innings([], [])
        assert result == {
            "innings_num": 1, "runs": 0, "wickets": 0,
            "overs": "0", "is_batting": False,
        }

    def test_first_current_wins(self):
        """When multiple linescores have isCurrent=1, the first one found is returned."""
        client = _cricket_client()
        home_ls = [{"isCurrent": 1, "period": 1, "runs": 100, "wickets": 3, "overs": 15, "isBatting": True}]
        away_ls = [{"isCurrent": 1, "period": 2, "runs": 50, "wickets": 0, "overs": 5, "isBatting": True}]
        result = client._get_current_innings(home_ls, away_ls)
        assert result["innings_num"] == 1
        assert result["runs"] == 100


# ================================================================
#  4. _detect_changes  (MOST IMPORTANT)
# ================================================================

class TestDetectChanges:
    def setup_method(self):
        self.client = _cricket_client()
        self.game_id = "G1"

    def _state(self, team_name, runs, wickets, overs):
        return {
            "teams": {
                team_name: {
                    "stats": {},
                    "linescores": [{"runs": runs, "wickets": wickets, "overs": overs}],
                }
            }
        }

    def _two_team_state(self, t1, r1, w1, o1, t2, r2, w2, o2):
        return {
            "teams": {
                t1: {"stats": {}, "linescores": [{"runs": r1, "wickets": w1, "overs": o1}]},
                t2: {"stats": {}, "linescores": [{"runs": r2, "wickets": w2, "overs": o2}]},
            }
        }

    # ── Wicket detection ──────────────────────────────────────

    def test_wicket_detected(self):
        prev = self._state("India", 100, 3, 20)
        curr = self._state("India", 100, 4, 20.1)
        events = self.client._detect_changes(self.game_id, prev, curr)

        wicket_events = [e for e in events if e["type"] == "wicket"]
        assert len(wicket_events) == 1
        assert "WICKET" in wicket_events[0]["description"]
        assert wicket_events[0]["team"] == "India"
        assert wicket_events[0]["score_value"] is True
        assert "100/4" in wicket_events[0]["description"]

    def test_multiple_wickets_same_poll(self):
        """Rare: two wickets fall between polls (e.g., run out on same ball)."""
        prev = self._state("India", 100, 3, 20)
        curr = self._state("India", 100, 5, 20.1)
        events = self.client._detect_changes(self.game_id, prev, curr)

        wicket_events = [e for e in events if e["type"] == "wicket"]
        assert len(wicket_events) == 1  # one wicket event per innings comparison
        assert "100/5" in wicket_events[0]["description"]

    # ── Boundary detection ────────────────────────────────────

    def test_four_detected(self):
        prev = self._state("India", 100, 3, 20)
        curr = self._state("India", 104, 3, 20.1)
        events = self.client._detect_changes(self.game_id, prev, curr)

        boundary_events = [e for e in events if e["type"] == "boundary"]
        assert len(boundary_events) == 1
        assert "FOUR!" in boundary_events[0]["description"]
        assert boundary_events[0]["score_value"] is True

    def test_five_runs_still_four(self):
        """5 runs diff (4 + overthrow) should be FOUR! not SIX!."""
        prev = self._state("India", 100, 3, 20)
        curr = self._state("India", 105, 3, 20.1)
        events = self.client._detect_changes(self.game_id, prev, curr)

        boundary_events = [e for e in events if e["type"] == "boundary"]
        assert len(boundary_events) == 1
        assert "FOUR!" in boundary_events[0]["description"]

    def test_six_detected(self):
        prev = self._state("India", 100, 3, 20)
        curr = self._state("India", 106, 3, 20.1)
        events = self.client._detect_changes(self.game_id, prev, curr)

        boundary_events = [e for e in events if e["type"] == "boundary"]
        assert len(boundary_events) == 1
        assert "SIX!" in boundary_events[0]["description"]

    def test_seven_runs_is_six(self):
        """7 runs diff (6 + no-ball) should be SIX!."""
        prev = self._state("India", 100, 3, 20)
        curr = self._state("India", 107, 3, 20.1)
        events = self.client._detect_changes(self.game_id, prev, curr)

        boundary_events = [e for e in events if e["type"] == "boundary"]
        assert len(boundary_events) == 1
        assert "SIX!" in boundary_events[0]["description"]

    # ── Regular runs detection ────────────────────────────────

    def test_regular_single_run(self):
        prev = self._state("India", 100, 3, 20)
        curr = self._state("India", 101, 3, 20.1)
        events = self.client._detect_changes(self.game_id, prev, curr)

        run_events = [e for e in events if e["type"] == "runs"]
        assert len(run_events) == 1
        assert "add 1 run(s)" in run_events[0]["description"]
        assert run_events[0]["score_value"] is False

    def test_regular_two_runs(self):
        prev = self._state("India", 100, 3, 20)
        curr = self._state("India", 102, 3, 20.1)
        events = self.client._detect_changes(self.game_id, prev, curr)

        run_events = [e for e in events if e["type"] == "runs"]
        assert len(run_events) == 1
        assert "add 2 run(s)" in run_events[0]["description"]

    def test_three_runs(self):
        prev = self._state("India", 100, 3, 20)
        curr = self._state("India", 103, 3, 20.1)
        events = self.client._detect_changes(self.game_id, prev, curr)

        run_events = [e for e in events if e["type"] == "runs"]
        assert len(run_events) == 1
        assert "add 3 run(s)" in run_events[0]["description"]

    # ── Milestone detection ───────────────────────────────────

    def test_milestone_50(self):
        prev = self._state("India", 48, 1, 10)
        curr = self._state("India", 52, 1, 10.3)
        events = self.client._detect_changes(self.game_id, prev, curr)

        milestones = [e for e in events if e["type"] == "milestone"]
        assert len(milestones) == 1
        assert "50 runs" in milestones[0]["description"]
        assert milestones[0]["score_value"] is True

    def test_milestone_100(self):
        prev = self._state("India", 97, 2, 18)
        curr = self._state("India", 102, 2, 18.2)
        events = self.client._detect_changes(self.game_id, prev, curr)

        milestones = [e for e in events if e["type"] == "milestone"]
        assert len(milestones) == 1
        assert "100 runs" in milestones[0]["description"]

    def test_milestone_200(self):
        prev = self._state("India", 196, 4, 35)
        curr = self._state("India", 203, 4, 35.4)
        events = self.client._detect_changes(self.game_id, prev, curr)

        milestones = [e for e in events if e["type"] == "milestone"]
        assert len(milestones) == 1
        assert "200 runs" in milestones[0]["description"]

    def test_milestone_300(self):
        prev = self._state("India", 298, 6, 45)
        curr = self._state("India", 304, 6, 45.3)
        events = self.client._detect_changes(self.game_id, prev, curr)

        milestones = [e for e in events if e["type"] == "milestone"]
        assert len(milestones) == 1
        assert "300 runs" in milestones[0]["description"]

    def test_multiple_milestones_skipped(self):
        """Big jump crosses both 50 and 100 -- both milestones should fire."""
        prev = self._state("India", 48, 0, 8)
        curr = self._state("India", 102, 0, 8.1)
        events = self.client._detect_changes(self.game_id, prev, curr)

        milestones = [e for e in events if e["type"] == "milestone"]
        mile_values = sorted([int(m["description"].split("reach ")[1].split(" runs")[0]) for m in milestones])
        assert mile_values == [50, 100]

    # ── No events when nothing changed ────────────────────────

    def test_no_change_no_events(self):
        prev = self._state("India", 100, 3, 20)
        curr = self._state("India", 100, 3, 20)
        events = self.client._detect_changes(self.game_id, prev, curr)
        assert events == []

    def test_only_overs_advance_no_events(self):
        """Dot ball -- overs advance but no runs or wickets."""
        prev = self._state("India", 100, 3, 20)
        curr = self._state("India", 100, 3, 20.1)
        events = self.client._detect_changes(self.game_id, prev, curr)
        assert events == []

    # ── Multiple simultaneous changes ─────────────────────────

    def test_wicket_takes_priority_over_runs(self):
        """
        When both runs and wickets change, only the wicket event fires
        (the code uses elif, so wicket wins).
        """
        prev = self._state("India", 100, 3, 20)
        curr = self._state("India", 102, 4, 20.3)
        events = self.client._detect_changes(self.game_id, prev, curr)

        types = [e["type"] for e in events]
        assert "wicket" in types
        # runs event should NOT fire because wicket branch is taken first (elif)
        assert "runs" not in types
        assert "boundary" not in types

    def test_boundary_plus_milestone(self):
        """A boundary that also crosses a milestone produces both events."""
        prev = self._state("India", 148, 3, 25)
        curr = self._state("India", 154, 3, 25.1)
        events = self.client._detect_changes(self.game_id, prev, curr)

        types = [e["type"] for e in events]
        assert "boundary" in types
        assert "milestone" in types

    def test_changes_for_both_teams(self):
        """Both teams can have independent change events."""
        prev = self._two_team_state("India", 100, 3, 20, "Australia", 50, 1, 10)
        curr = self._two_team_state("India", 104, 3, 20.1, "Australia", 51, 1, 10.1)
        events = self.client._detect_changes(self.game_id, prev, curr)

        india_events = [e for e in events if e["team"] == "India"]
        aus_events = [e for e in events if e["team"] == "Australia"]
        assert len(india_events) >= 1  # boundary
        assert len(aus_events) >= 1    # single run

    def test_new_innings_no_previous(self):
        """New innings appears with no previous data -- treats previous as zeros."""
        prev = {"teams": {"India": {"stats": {}, "linescores": []}}}
        curr = self._state("India", 5, 0, 1)
        events = self.client._detect_changes(self.game_id, prev, curr)

        run_events = [e for e in events if e["type"] in ("runs", "boundary")]
        assert len(run_events) >= 1

    def test_event_fields_complete(self):
        """Verify every generated event has all required fields."""
        prev = self._state("India", 100, 3, 20)
        curr = self._state("India", 106, 3, 20.1)
        events = self.client._detect_changes(self.game_id, prev, curr)

        required_keys = {"play_id", "description", "score_value", "clock", "period", "team", "athletes", "type"}
        for event in events:
            assert required_keys.issubset(event.keys()), f"Missing keys in {event}"

    def test_clock_format(self):
        prev = self._state("India", 100, 3, 20)
        curr = self._state("India", 101, 3, 20.1)
        events = self.client._detect_changes(self.game_id, prev, curr)
        assert events[0]["clock"] == "20.1 ov"

    def test_period_is_innings_index_plus_one(self):
        prev = self._state("India", 100, 3, 20)
        curr = self._state("India", 101, 3, 20.1)
        events = self.client._detect_changes(self.game_id, prev, curr)
        # First (index 0) innings -> period 1
        assert events[0]["period"] == 1


# ================================================================
#  5. _extract_scorecard_events
# ================================================================

class TestExtractScorecardEvents:

    def _leaders_data(self, stat_name, value, is_batting, athlete_name="Virat Kohli",
                      team_name="India", period=1):
        return {
            "leaders": [
                {
                    "team": {"displayName": team_name},
                    "linescores": [
                        {
                            "isBatting": is_batting,
                            "period": period,
                            "leaders": [
                                {
                                    "name": stat_name,
                                    "leaders": [
                                        {
                                            "athlete": {"displayName": athlete_name},
                                            "displayValue": str(value),
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ]
        }

    def test_fifty_event(self):
        client = _cricket_client()
        data = self._leaders_data("runs", 67, is_batting=True)
        events = client._extract_scorecard_events(data, "G1")

        fifty_events = [e for e in events if e["type"] == "fifty"]
        assert len(fifty_events) == 1
        assert "FIFTY" in fifty_events[0]["description"]
        assert "Virat Kohli" in fifty_events[0]["description"]
        assert fifty_events[0]["athletes"] == ["Virat Kohli"]

    def test_century_event(self):
        client = _cricket_client()
        data = self._leaders_data("runs", 112, is_batting=True)
        events = client._extract_scorecard_events(data, "G1")

        century_events = [e for e in events if e["type"] == "century"]
        assert len(century_events) == 1
        assert "CENTURY" in century_events[0]["description"]
        assert "112" in century_events[0]["description"]

    def test_century_not_fifty(self):
        """100+ runs should produce a century event, NOT a fifty event."""
        client = _cricket_client()
        data = self._leaders_data("runs", 105, is_batting=True)
        events = client._extract_scorecard_events(data, "G1")

        types = [e["type"] for e in events]
        assert "century" in types
        assert "fifty" not in types

    def test_below_50_no_event(self):
        client = _cricket_client()
        data = self._leaders_data("runs", 35, is_batting=True)
        events = client._extract_scorecard_events(data, "G1")
        batting_events = [e for e in events if e["type"] in ("fifty", "century")]
        assert len(batting_events) == 0

    def test_bowling_spell_3_wickets(self):
        client = _cricket_client()
        data = self._leaders_data("wickets", 3, is_batting=False, athlete_name="Jasprit Bumrah")
        events = client._extract_scorecard_events(data, "G1")

        spell_events = [e for e in events if e["type"] == "bowling_spell"]
        assert len(spell_events) == 1
        assert "Jasprit Bumrah" in spell_events[0]["description"]
        assert "3 wickets" in spell_events[0]["description"]

    def test_five_wickets_event(self):
        client = _cricket_client()
        data = self._leaders_data("wickets", 5, is_batting=False, athlete_name="Pat Cummins")
        events = client._extract_scorecard_events(data, "G1")

        fw_events = [e for e in events if e["type"] == "five_wickets"]
        assert len(fw_events) == 1
        assert "FIVE-WICKET" in fw_events[0]["description"]
        assert "Pat Cummins" in fw_events[0]["description"]

    def test_five_wickets_not_bowling_spell(self):
        """5+ wickets should produce five_wickets, NOT bowling_spell."""
        client = _cricket_client()
        data = self._leaders_data("wickets", 6, is_batting=False)
        events = client._extract_scorecard_events(data, "G1")

        types = [e["type"] for e in events]
        assert "five_wickets" in types
        assert "bowling_spell" not in types

    def test_below_3_wickets_no_event(self):
        client = _cricket_client()
        data = self._leaders_data("wickets", 2, is_batting=False)
        events = client._extract_scorecard_events(data, "G1")
        bowling_events = [e for e in events if e["type"] in ("bowling_spell", "five_wickets")]
        assert len(bowling_events) == 0

    def test_batting_stat_while_not_batting_ignored(self):
        """Runs stat with isBatting=False should produce no batting events."""
        client = _cricket_client()
        data = self._leaders_data("runs", 75, is_batting=False)
        events = client._extract_scorecard_events(data, "G1")
        batting_events = [e for e in events if e["type"] in ("fifty", "century")]
        assert len(batting_events) == 0

    def test_bowling_stat_while_batting_ignored(self):
        """Wickets stat with isBatting=True should produce no bowling events."""
        client = _cricket_client()
        data = self._leaders_data("wickets", 4, is_batting=True)
        events = client._extract_scorecard_events(data, "G1")
        bowling_events = [e for e in events if e["type"] in ("bowling_spell", "five_wickets")]
        assert len(bowling_events) == 0

    def test_non_numeric_value_skipped(self):
        client = _cricket_client()
        data = self._leaders_data("runs", "N/A", is_batting=True)
        events = client._extract_scorecard_events(data, "G1")
        assert len(events) == 0

    def test_empty_leaders(self):
        client = _cricket_client()
        events = client._extract_scorecard_events({"leaders": []}, "G1")
        assert events == []

    def test_scorecard_event_fields(self):
        client = _cricket_client()
        data = self._leaders_data("runs", 55, is_batting=True, team_name="India", period=2)
        events = client._extract_scorecard_events(data, "G1")

        assert len(events) == 1
        e = events[0]
        assert e["team"] == "India"
        assert e["period"] == 2
        assert e["clock"] == ""
        assert e["score_value"] is True


# ================================================================
#  6. _parse_cricket_summary
# ================================================================

class TestParseCricketSummary:
    def _summary_data(self):
        return {
            "leaders": [
                {
                    "team": {"displayName": "India"},
                    "linescores": [
                        {
                            "isBatting": True,
                            "period": 1,
                            "leaders": [
                                {
                                    "name": "runs",
                                    "leaders": [
                                        {"athlete": {"displayName": "Rohit Sharma"}, "displayValue": "85"},
                                    ],
                                },
                            ],
                        },
                        {
                            "isBatting": False,
                            "period": 2,
                            "leaders": [
                                {
                                    "name": "wickets",
                                    "leaders": [
                                        {"athlete": {"displayName": "Jasprit Bumrah"}, "displayValue": "4"},
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ],
            "rosters": [],
            "notes": [{"text": "Toss: India won and elected to bat"}],
        }

    def test_batting_leaders_parsed(self):
        client = _cricket_client()
        result = client._parse_cricket_summary(self._summary_data())

        assert "India" in result
        batting = result["India"]["batting"]
        assert len(batting) == 1
        assert batting[0]["name"] == "Rohit Sharma"
        assert batting[0]["runs"] == "85"
        assert batting[0]["innings"] == 1

    def test_bowling_leaders_parsed(self):
        client = _cricket_client()
        result = client._parse_cricket_summary(self._summary_data())

        bowling = result["India"]["bowling"]
        assert len(bowling) == 1
        assert bowling[0]["name"] == "Jasprit Bumrah"
        assert bowling[0]["wickets"] == "4"
        assert bowling[0]["innings"] == 2

    def test_notes_parsed(self):
        client = _cricket_client()
        result = client._parse_cricket_summary(self._summary_data())

        assert "_notes" in result
        assert "Toss: India won and elected to bat" in result["_notes"]

    def test_empty_leaders(self):
        client = _cricket_client()
        result = client._parse_cricket_summary({"leaders": [], "rosters": [], "notes": []})
        assert "_notes" not in result  # empty notes list is falsy

    def test_rosters_add_players(self):
        client = _cricket_client()
        data = {
            "leaders": [
                {
                    "team": {"displayName": "India"},
                    "linescores": [],
                }
            ],
            "rosters": [
                {
                    "team": {"displayName": "India"},
                    "roster": [
                        {
                            "athlete": {"displayName": "Virat Kohli"},
                            "linescores": [],
                        }
                    ],
                }
            ],
            "notes": [],
        }
        result = client._parse_cricket_summary(data)
        assert "players" in result["India"]


# ================================================================
#  7. build_prompt for cricket
# ================================================================

class TestBuildPromptCricket:
    def test_cricket_prompt_format(self):
        with patch("llm.commentator.SPORT", "cricket"):
            from llm.commentator import _build_cricket_prompt

            event = {
                "home_team": "Mumbai Indians",
                "away_team": "Chennai Super Kings",
                "home_score": "185/4",
                "away_score": "120/3",
                "period": 2,
                "clock": "15.4 ov",
                "description": "FOUR! Dhoni drives through covers",
                "type": "boundary",
                "athletes": ["MS Dhoni"],
            }
            context = [
                {"clock": "15.2 ov", "description": "Single to mid-on"},
            ]
            prompt = _build_cricket_prompt(event, context)

            assert "MATCH:" in prompt
            assert "Chennai Super Kings vs Mumbai Indians" in prompt
            assert "Innings 2" in prompt
            assert "15.4 ov" in prompt
            assert "FOUR! Dhoni drives through covers" in prompt
            assert "boundary" in prompt
            assert "MS Dhoni" in prompt
            assert "RECENT EVENTS:" in prompt
            assert "Single to mid-on" in prompt

    def test_cricket_prompt_empty_context(self):
        with patch("llm.commentator.SPORT", "cricket"):
            from llm.commentator import _build_cricket_prompt

            event = {
                "home_team": "Team A",
                "away_team": "Team B",
                "home_score": "0/0",
                "away_score": "0/0",
                "period": 1,
                "clock": "0.1 ov",
                "description": "First ball",
                "type": "update",
                "athletes": [],
            }
            prompt = _build_cricket_prompt(event, [])
            assert "(start of innings)" in prompt

    def test_cricket_prompt_no_athletes(self):
        with patch("llm.commentator.SPORT", "cricket"):
            from llm.commentator import _build_cricket_prompt

            event = {
                "home_team": "A",
                "away_team": "B",
                "period": 1,
                "clock": "",
                "description": "Dot ball",
                "athletes": [],
            }
            prompt = _build_cricket_prompt(event, [])
            assert "N/A" in prompt


# ================================================================
#  8. get_system_prompt
# ================================================================

class TestGetSystemPrompt:
    def test_cricket_returns_cricket_prompt(self):
        with patch("llm.commentator.SPORT", "cricket"):
            from llm.commentator import get_system_prompt, CRICKET_SYSTEM_PROMPT
            result = get_system_prompt()
            assert result == CRICKET_SYSTEM_PROMPT

    def test_cricket_prompt_contains_terminology(self):
        from llm.commentator import CRICKET_SYSTEM_PROMPT
        assert "overs" in CRICKET_SYSTEM_PROMPT
        assert "wickets" in CRICKET_SYSTEM_PROMPT
        assert "boundaries" in CRICKET_SYSTEM_PROMPT

    def test_non_cricket_returns_standard(self):
        with patch("llm.commentator.SPORT", "nba"):
            from llm.commentator import get_system_prompt, SYSTEM_PROMPT
            result = get_system_prompt()
            assert result == SYSTEM_PROMPT

    def test_cricket_prompt_mentions_commentary_style(self):
        from llm.commentator import CRICKET_SYSTEM_PROMPT, COMMENTARY_STYLES
        assert "cricket" in CRICKET_SYSTEM_PROMPT.lower()
        # Style-specific prompts have commentator names
        assert "Harsha Bhogle" in COMMENTARY_STYLES["bhogle"]
        assert "Ian Bishop" in COMMENTARY_STYLES["bishop"]
        assert "Ravi Shastri" in COMMENTARY_STYLES["shastri"]
        assert "Richie Benaud" in COMMENTARY_STYLES["benaud"]
