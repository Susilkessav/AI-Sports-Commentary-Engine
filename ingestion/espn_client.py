"""
ingestion/espn_client.py
────────────────────────
Fetches live games and play-by-play events from ESPN's
hidden public API. No API key required.

Supports: NBA, NFL, NHL, Soccer, Cricket
Cricket uses numeric league IDs and a different data structure
(no ball-by-ball plays — we detect score changes between polls).

Also supports:
  - Recent / completed games with summaries
  - Upcoming / scheduled games with date/time
  - Full scoreboard (all states: pre, in, post)
"""

import time as _time
import requests
from datetime import datetime, timezone
from config.settings import (
    ESPN_BASE, ESPN_SPORT_PATHS, SPORT,
    CRICKET_LEAGUE, CRICKET_LEAGUE_IDS, ESPN_CRICKET_HEADER,
)

# ── Simple rate limiter ─────────────────────────────────────
_last_request_time = 0.0
MIN_REQUEST_GAP = 0.5  # minimum 500ms between ESPN requests


class ESPNClient:
    def __init__(self, sport: str = SPORT):
        self.sport = sport
        self.is_cricket = sport == "cricket"

        if self.is_cricket:
            league_id = CRICKET_LEAGUE_IDS.get(
                CRICKET_LEAGUE, CRICKET_LEAGUE
            )
            self.path = f"cricket/{league_id}"
            self.base = f"{ESPN_BASE}/{self.path}"
            # Track previous state to detect changes
            self._prev_scores: dict[str, dict] = {}
        else:
            self.path = ESPN_SPORT_PATHS[sport]
            self.base = f"{ESPN_BASE}/{self.path}"

    # ── Public methods ──────────────────────────────────────

    def get_live_games(self) -> list[dict]:
        """Return all currently live (in-progress) games."""
        if self.is_cricket:
            return self._get_cricket_games()

        url  = f"{self.base}/scoreboard"
        data = self._get(url)
        if not data:
            return []

        games = []
        for event in data.get("events", []):
            status = event.get("status", {}).get("type", {})
            if status.get("state") == "in":
                games.append(self._parse_game(event))
        return games

    def get_plays(self, game_id: str) -> list[dict]:
        """Return the most recent plays for a given game."""
        if self.is_cricket:
            return self._get_cricket_events(game_id)

        url  = f"{self.base}/summary"
        data = self._get(url, params={"event": game_id})
        if not data:
            return []

        plays = []
        for drive in data.get("drives", {}).get("current", {}).get("plays", []):
            plays.append(self._parse_play(drive))

        # Fallback: use play-by-play array if drives not available
        if not plays:
            for play in data.get("plays", [])[-10:]:
                plays.append(self._parse_play(play))

        return plays

    def get_game_summary(self, game_id: str) -> dict:
        """Return current score + key stats for a game."""
        url  = f"{self.base}/summary"
        data = self._get(url, params={"event": game_id})
        if not data:
            return {}
        if self.is_cricket:
            return self._parse_cricket_summary(data)
        return self._parse_summary(data)

    # ── Cricket-specific methods ──────────────────────────────

    def _get_cricket_games(self) -> list[dict]:
        """Fetch live cricket matches from the scoreboard."""
        url = f"{self.base}/scoreboard"
        data = self._get(url)
        if not data:
            return []

        games = []
        for event in data.get("events", []):
            status = event.get("status", {}).get("type", {})
            if status.get("state") == "in":
                games.append(self._parse_cricket_game(event))
        return games

    def get_all_live_cricket(self) -> list[dict]:
        """Fetch ALL live cricket across all leagues via header endpoint."""
        data = self._get(ESPN_CRICKET_HEADER, params={
            "sport": "cricket",
            "region": "in",
        })
        if not data:
            return []

        games = []
        for sport in data.get("sports", []):
            for league in sport.get("leagues", []):
                for event in league.get("events", []):
                    # Header endpoint may use flat "status" string or nested object
                    status = event.get("status", "")
                    state = status.get("type", {}).get("state") if isinstance(status, dict) else status
                    if state == "in":
                        games.append({
                            "game_id":   event.get("id"),
                            "sport":     "cricket",
                            "league":    league.get("name", ""),
                            "name":      event.get("name", ""),
                            "summary":   event.get("summary", ""),
                            "home_team": event.get("competitors", [{}])[-1].get("displayName", "Home"),
                            "away_team": event.get("competitors", [{}])[0].get("displayName", "Away"),
                            "home_score": event.get("competitors", [{}])[-1].get("score", "0"),
                            "away_score": event.get("competitors", [{}])[0].get("score", "0"),
                        })
        return games

    def _parse_cricket_game(self, event: dict) -> dict:
        """Parse a cricket event from the scoreboard into our game format."""
        competition = event.get("competitions", [{}])[0]
        competitors = competition.get("competitors", [])
        home = next((c for c in competitors if c.get("homeAway") == "home"), {})
        away = next((c for c in competitors if c.get("homeAway") == "away"), {})
        status = event.get("status", {})

        # Extract innings data from linescores
        home_linescores = home.get("linescores", [])
        away_linescores = away.get("linescores", [])

        home_innings = self._format_innings(home_linescores)
        away_innings = self._format_innings(away_linescores)

        # Current batting info
        current_innings = self._get_current_innings(home_linescores, away_linescores)

        # Cricket scores are like "280/9" (runs/wickets)
        home_score = home.get("score", "0")
        away_score = away.get("score", "0")

        return {
            "game_id":         event.get("id"),
            "sport":           "cricket",
            "home_team":       home.get("team", {}).get("displayName", "Home"),
            "away_team":       away.get("team", {}).get("displayName", "Away"),
            "home_score":      home_score,
            "away_score":      away_score,
            "home_innings":    home_innings,
            "away_innings":    away_innings,
            "current_innings": current_innings,
            "period":          current_innings.get("innings_num", 1),
            "clock":           current_innings.get("overs", ""),
            "situation":       status.get("type", {}).get("shortDetail", ""),
            "venue":           competition.get("venue", {}).get("fullName", ""),
            "match_format":    event.get("competitions", [{}])[0].get("note", ""),
        }

    def _format_innings(self, linescores: list[dict]) -> list[dict]:
        """Convert ESPN linescores to a list of innings summaries."""
        innings = []
        for ls in linescores:
            innings.append({
                "innings_num": ls.get("period", 0),
                "runs":        ls.get("runs", 0),
                "wickets":     ls.get("wickets", 0),
                "overs":       ls.get("overs", 0),
                "is_batting":  ls.get("isBatting", False),
                "is_current":  ls.get("isCurrent", 0) == 1,
                "description": ls.get("description", ""),
            })
        return innings

    def _get_current_innings(self, home_ls: list, away_ls: list) -> dict:
        """Find the current innings being played."""
        for ls in home_ls + away_ls:
            if ls.get("isCurrent", 0) == 1:
                return {
                    "innings_num": ls.get("period", 1),
                    "runs":        ls.get("runs", 0),
                    "wickets":     ls.get("wickets", 0),
                    "overs":       str(ls.get("overs", 0)),
                    "is_batting":  ls.get("isBatting", False),
                }
        return {"innings_num": 1, "runs": 0, "wickets": 0, "overs": "0", "is_batting": False}

    def _get_cricket_events(self, game_id: str) -> list[dict]:
        """
        Generate synthetic play events for cricket by detecting
        score changes between polls. Also fetches match summary
        for batting/bowling highlights.
        """
        url = f"{self.base}/summary"
        data = self._get(url, params={"event": game_id})
        if not data:
            return []

        events = []

        # Extract notes as events (fall of wickets, session breaks, milestones)
        for note in data.get("notes", []):
            text = note.get("text", "") if isinstance(note, dict) else str(note)
            if text:
                events.append({
                    "play_id":     f"note_{game_id}_{hash(text) & 0xFFFFFF:06x}",
                    "description": text,
                    "score_value": "wicket" in text.lower() or "out" in text.lower(),
                    "clock":       "",
                    "period":      1,
                    "team":        "",
                    "athletes":    [],
                    "type":        "note",
                })

        # Detect score changes from previous poll
        current_state = self._extract_match_state(data)
        prev_state = self._prev_scores.get(game_id)

        if prev_state and current_state:
            change_events = self._detect_changes(game_id, prev_state, current_state)
            events.extend(change_events)

        if current_state:
            self._prev_scores[game_id] = current_state

        # Extract batting milestones from rosters/scorecard
        scorecard_events = self._extract_scorecard_events(data, game_id)
        events.extend(scorecard_events)

        return events

    def _extract_match_state(self, data: dict) -> dict | None:
        """Extract current match state from summary data for change detection."""
        boxscore = data.get("boxscore", {})
        teams = boxscore.get("teams", [])
        if not teams:
            return None

        state = {"teams": {}}
        for t in teams:
            name = t.get("team", {}).get("displayName", "")
            stats = {}
            for s in t.get("statistics", []):
                stats[s.get("name", "")] = s.get("displayValue", "")
            linescores = []
            for ls in t.get("linescores", []):
                linescores.append({
                    "runs": ls.get("runs", 0),
                    "wickets": ls.get("wickets", 0),
                    "overs": ls.get("overs", 0),
                })
            state["teams"][name] = {"stats": stats, "linescores": linescores}
        return state

    def _detect_changes(self, game_id: str, prev: dict, curr: dict) -> list[dict]:
        """Compare previous and current state to generate change events."""
        events = []

        for team_name, curr_data in curr.get("teams", {}).items():
            prev_data = prev.get("teams", {}).get(team_name, {})
            prev_ls = prev_data.get("linescores", [])
            curr_ls = curr_data.get("linescores", [])

            for i, c_inn in enumerate(curr_ls):
                p_inn = prev_ls[i] if i < len(prev_ls) else {"runs": 0, "wickets": 0, "overs": 0}

                def _safe_num(val, default=0):
                    try:
                        return float(val)
                    except (TypeError, ValueError):
                        return default

                c_runs = _safe_num(c_inn.get("runs", 0))
                c_wkts = _safe_num(c_inn.get("wickets", 0))
                c_overs = _safe_num(c_inn.get("overs", 0))
                p_runs = _safe_num(p_inn.get("runs", 0))
                p_wkts = _safe_num(p_inn.get("wickets", 0))

                runs_diff = int(c_runs - p_runs)
                wickets_diff = int(c_wkts - p_wkts)

                # Calculate run rate
                overs = c_overs if c_overs > 0 else 0.1
                run_rate = round(c_runs / overs, 2)
                rr_str = f" (RR: {run_rate})"

                if wickets_diff > 0:
                    desc = (f"WICKET! {team_name} lose a wicket! "
                            f"Now {c_inn['runs']}/{c_inn['wickets']} "
                            f"after {c_inn['overs']} overs{rr_str}")
                    events.append({
                        "play_id":     f"wkt_{game_id}_{i}_{c_inn['wickets']}",
                        "description": desc,
                        "score_value": True,
                        "clock":       f"{c_inn['overs']} ov",
                        "period":      i + 1,
                        "team":        team_name,
                        "athletes":    [],
                        "type":        "wicket",
                    })
                elif runs_diff >= 4:
                    boundary = "SIX!" if runs_diff >= 6 else "FOUR!"
                    desc = (f"{boundary} {team_name} score {runs_diff} runs! "
                            f"Now {c_inn['runs']}/{c_inn['wickets']} "
                            f"after {c_inn['overs']} overs{rr_str}")
                    events.append({
                        "play_id":     f"runs_{game_id}_{i}_{c_inn['runs']}",
                        "description": desc,
                        "score_value": True,
                        "clock":       f"{c_inn['overs']} ov",
                        "period":      i + 1,
                        "team":        team_name,
                        "athletes":    [],
                        "type":        "boundary",
                    })
                elif runs_diff > 0:
                    desc = (f"{team_name} add {runs_diff} run(s). "
                            f"Score: {c_inn['runs']}/{c_inn['wickets']} "
                            f"after {c_inn['overs']} overs{rr_str}")
                    events.append({
                        "play_id":     f"runs_{game_id}_{i}_{c_inn['runs']}",
                        "description": desc,
                        "score_value": False,
                        "clock":       f"{c_inn['overs']} ov",
                        "period":      i + 1,
                        "team":        team_name,
                        "athletes":    [],
                        "type":        "runs",
                    })

                # Milestone: 50, 100, 150, 200, 250, 300 runs
                for milestone in [50, 100, 150, 200, 250, 300, 350, 400]:
                    if p_inn["runs"] < milestone <= c_inn["runs"]:
                        desc = (f"{team_name} reach {milestone} runs! "
                                f"Score: {c_inn['runs']}/{c_inn['wickets']} "
                                f"in {c_inn['overs']} overs{rr_str}")
                        events.append({
                            "play_id":     f"mile_{game_id}_{i}_{milestone}",
                            "description": desc,
                            "score_value": True,
                            "clock":       f"{c_inn['overs']} ov",
                            "period":      i + 1,
                            "team":        team_name,
                            "athletes":    [],
                            "type":        "milestone",
                        })

                # Powerplay completion (over 6)
                if p_inn["overs"] < 6 <= c_inn["overs"]:
                    desc = (f"End of Powerplay! {team_name} at {c_inn['runs']}/{c_inn['wickets']} "
                            f"after 6 overs{rr_str}")
                    events.append({
                        "play_id":     f"pp_{game_id}_{i}",
                        "description": desc,
                        "score_value": False,
                        "clock":       f"{c_inn['overs']} ov",
                        "period":      i + 1,
                        "team":        team_name,
                        "athletes":    [],
                        "type":        "powerplay",
                    })

        # 2nd innings: required run rate calculation
        all_teams = list(curr.get("teams", {}).keys())
        if len(all_teams) == 2:
            team_a, team_b = all_teams
            a_ls = curr["teams"][team_a].get("linescores", [])
            b_ls = curr["teams"][team_b].get("linescores", [])

            # Find if one team has completed innings and other is chasing
            if len(a_ls) >= 2 and len(b_ls) >= 2:
                for chasing_team, chasing_ls, target_ls in [
                    (team_a, a_ls, b_ls), (team_b, b_ls, a_ls)
                ]:
                    # Check if 2nd innings is active for this team
                    if len(chasing_ls) > 1 and chasing_ls[-1].get("overs", 0) > 0:
                        c_inn = chasing_ls[-1]
                        # Target is first innings score + 1
                        if target_ls and target_ls[0].get("runs", 0) > 0:
                            target = target_ls[0]["runs"] + 1
                            remaining = target - c_inn["runs"]
                            # Infer total overs from first innings
                            first_inn_overs = target_ls[0].get("overs", 20)
                            total_overs = 50 if first_inn_overs > 20 else 20
                            overs_left = total_overs - c_inn["overs"]
                            if overs_left > 0 and remaining > 0:
                                req_rate = round(remaining / overs_left, 2)
                                prev_chasing = prev.get("teams", {}).get(chasing_team, {}).get("linescores", [])
                                prev_overs = prev_chasing[-1].get("overs", 0) if len(prev_chasing) > 1 else 0

                                # Generate RRR update every 2 overs
                                prev_ov_int = int(prev_overs)
                                curr_ov_int = int(c_inn["overs"])
                                if curr_ov_int > prev_ov_int and curr_ov_int % 2 == 0:
                                    desc = (f"{chasing_team} need {remaining} runs from "
                                            f"{overs_left:.1f} overs. Required rate: {req_rate}")
                                    events.append({
                                        "play_id":     f"rrr_{game_id}_{curr_ov_int}",
                                        "description": desc,
                                        "score_value": False,
                                        "clock":       f"{c_inn['overs']} ov",
                                        "period":      2,
                                        "team":        chasing_team,
                                        "athletes":    [],
                                        "type":        "required_rate",
                                    })

        return events

    def _extract_scorecard_events(self, data: dict, game_id: str) -> list[dict]:
        """Extract notable batting/bowling performances from leaders data."""
        events = []
        leaders = data.get("leaders", [])

        for team_data in leaders:
            team_name = team_data.get("team", {}).get("displayName", "")

            for period_data in team_data.get("linescores", []):
                is_batting = period_data.get("isBatting", False)
                period = period_data.get("period", 1)

                for leader in period_data.get("leaders", []):
                    stat_name = leader.get("name", "")

                    for entry in leader.get("leaders", []):
                        name = entry.get("athlete", {}).get("displayName", "")
                        value = entry.get("displayValue", "0")

                        try:
                            val = int(value)
                        except (ValueError, TypeError):
                            continue

                        # Batting milestones
                        if stat_name == "runs" and is_batting:
                            if val >= 100:
                                events.append({
                                    "play_id":     f"bat_{game_id}_{name}_100",
                                    "description": f"CENTURY! {name} has scored {val} runs for {team_name}!",
                                    "score_value": True,
                                    "clock":       "",
                                    "period":      period,
                                    "team":        team_name,
                                    "athletes":    [name],
                                    "type":        "century",
                                })
                            elif val >= 50:
                                events.append({
                                    "play_id":     f"bat_{game_id}_{name}_50",
                                    "description": f"FIFTY! {name} reaches {val} runs for {team_name}!",
                                    "score_value": True,
                                    "clock":       "",
                                    "period":      period,
                                    "team":        team_name,
                                    "athletes":    [name],
                                    "type":        "fifty",
                                })

                        # Bowling milestones
                        if stat_name == "wickets" and not is_batting:
                            if val >= 5:
                                events.append({
                                    "play_id":     f"bowl_{game_id}_{name}_5w",
                                    "description": f"FIVE-WICKET HAUL! {name} has {val} wickets for {team_name}!",
                                    "score_value": True,
                                    "clock":       "",
                                    "period":      period,
                                    "team":        team_name,
                                    "athletes":    [name],
                                    "type":        "five_wickets",
                                })
                            elif val >= 3:
                                events.append({
                                    "play_id":     f"bowl_{game_id}_{name}_3w",
                                    "description": f"Great bowling! {name} has picked up {val} wickets for {team_name}!",
                                    "score_value": True,
                                    "clock":       "",
                                    "period":      period,
                                    "team":        team_name,
                                    "athletes":    [name],
                                    "type":        "bowling_spell",
                                })

        return events

    def _parse_cricket_summary(self, data: dict) -> dict:
        """Parse cricket match summary using leaders data for top performers."""
        summary = {}
        leaders = data.get("leaders", [])

        for team_data in leaders:
            team_name = team_data.get("team", {}).get("displayName", "Team")
            batting_leaders = []
            bowling_leaders = []

            for period_data in team_data.get("linescores", []):
                is_batting = period_data.get("isBatting", False)
                period = period_data.get("period", 1)

                for leader in period_data.get("leaders", []):
                    stat_name = leader.get("name", "")
                    for entry in leader.get("leaders", []):
                        name = entry.get("athlete", {}).get("displayName", "")
                        value = entry.get("displayValue", "0")
                        if is_batting and stat_name == "runs":
                            batting_leaders.append({
                                "name": name, "runs": value, "innings": period,
                            })
                        elif not is_batting and stat_name == "wickets":
                            bowling_leaders.append({
                                "name": name, "wickets": value, "innings": period,
                            })

            summary[team_name] = {
                "batting": batting_leaders,
                "bowling": bowling_leaders,
            }

        # Extract player-level detail from rosters
        for roster in data.get("rosters", []):
            team_name = roster.get("team", {}).get("displayName", "Team")
            if team_name not in summary:
                summary[team_name] = {"batting": [], "bowling": []}

            players = []
            for player in roster.get("roster", []):
                pname = player.get("athlete", {}).get("displayName", "")
                stats = self._extract_player_stats(player)
                if stats:
                    stats["name"] = pname
                    players.append(stats)
            summary[team_name]["players"] = players

        # Add notes (fall of wickets, toss, etc.)
        notes = data.get("notes", [])
        if notes:
            summary["_notes"] = [
                n.get("text", str(n)) if isinstance(n, dict) else str(n)
                for n in notes
            ]

        return summary

    def _extract_player_stats(self, player: dict) -> dict:
        """Extract batting/bowling stats from the nested player linescore structure."""
        stats = {}
        for period_ls in player.get("linescores", []):
            for ls in period_ls.get("linescores", []):
                for cat in ls.get("statistics", {}).get("categories", []):
                    for s in cat.get("stats", []):
                        if s.get("value", 0) != 0:
                            stats[s["name"]] = s.get("displayValue", str(s["value"]))
        return stats

    # ── Recent & Upcoming games ─────────────────────────────────

    def _enrich_event(self, event: dict, game: dict):
        """Add status, date, headline, and winner info to a parsed game."""
        status = event.get("status", {}).get("type", {})
        state = status.get("state", "")
        game["state"] = state
        game["status_detail"] = status.get("shortDetail", "")
        game["completed"] = status.get("completed", False)

        # Date
        date_str = event.get("date", "")
        game["date"] = date_str
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                game["date_display"] = dt.strftime("%b %d, %I:%M %p UTC")
                game["date_iso"] = dt.isoformat()
            except (ValueError, TypeError):
                game["date_display"] = date_str

        # Headlines / result summary from ESPN
        competition = event.get("competitions", [{}])[0]
        headlines = competition.get("headlines", [])
        if headlines:
            game["headline"] = headlines[0].get("shortLinkText", "")
            game["description"] = headlines[0].get("description", "")
        else:
            game["headline"] = ""
            game["description"] = ""

        # Notes (series info, match format, etc.)
        notes = competition.get("notes", [])
        if notes:
            game["notes"] = [n.get("headline", str(n)) if isinstance(n, dict) else str(n) for n in notes]
        else:
            game["notes"] = []

        # Winner detection for completed games
        if state == "post":
            competitors = competition.get("competitors", [])
            for c in competitors:
                if c.get("winner"):
                    game["winner"] = c.get("team", {}).get("displayName", "")
                    break

        return state

    def _parse_scoreboard_data(self, data: dict) -> dict:
        """Parse a scoreboard API response into live/recent/upcoming groups."""
        result = {"live": [], "recent": [], "upcoming": []}
        parser = self._parse_cricket_game if self.is_cricket else self._parse_game

        for event in data.get("events", []):
            game = parser(event)
            state = self._enrich_event(event, game)

            if state == "in":
                result["live"].append(game)
            elif state == "post":
                result["recent"].append(game)
            elif state == "pre":
                result["upcoming"].append(game)

        return result

    def get_scoreboard_all(self, lookback_days: int = 7) -> dict:
        """Return ALL games grouped by state: live, recent, upcoming.
        If no recent/upcoming games found for today, looks back up to
        `lookback_days` to find completed games and forward for upcoming.
        """
        from datetime import timedelta

        url = f"{self.base}/scoreboard"
        data = self._get(url)
        if not data:
            return {"live": [], "recent": [], "upcoming": []}

        result = self._parse_scoreboard_data(data)

        # If no recent completed games, look at previous days
        if not result["recent"]:
            today = datetime.now(timezone.utc).date()
            for days_ago in range(1, lookback_days + 1):
                past_date = today - timedelta(days=days_ago)
                date_str = past_date.strftime("%Y%m%d")
                past_data = self._get(url, params={"dates": date_str})
                if past_data:
                    past_result = self._parse_scoreboard_data(past_data)
                    result["recent"].extend(past_result.get("recent", []))
                    if result["recent"]:
                        break  # found recent games, stop looking further back

        # If no upcoming games, look at future days
        if not result["upcoming"]:
            today = datetime.now(timezone.utc).date()
            for days_ahead in range(1, lookback_days + 1):
                future_date = today + timedelta(days=days_ahead)
                date_str = future_date.strftime("%Y%m%d")
                future_data = self._get(url, params={"dates": date_str})
                if future_data:
                    future_result = self._parse_scoreboard_data(future_data)
                    result["upcoming"].extend(future_result.get("upcoming", []))
                    # Also grab "pre" state games from future dates
                    # Some sports list future games as neither pre/in/post on initial parse
                    for event in future_data.get("events", []):
                        status = event.get("status", {}).get("type", {})
                        state = status.get("state", "")
                        if state in ("pre", ""):
                            game = (self._parse_cricket_game if self.is_cricket else self._parse_game)(event)
                            self._enrich_event(event, game)
                            if game not in result["upcoming"]:
                                result["upcoming"].append(game)
                    if result["upcoming"]:
                        break

        return result

    def get_recent_games(self) -> list[dict]:
        """Return recently completed games with result summaries."""
        scoreboard = self.get_scoreboard_all()
        return scoreboard.get("recent", [])

    def get_upcoming_games(self) -> list[dict]:
        """Return upcoming/scheduled games."""
        scoreboard = self.get_scoreboard_all()
        return scoreboard.get("upcoming", [])

    def get_schedule(self, dates: str = None) -> list[dict]:
        """Return games for a specific date range.
        dates: YYYYMMDD or YYYYMMDD-YYYYMMDD (ESPN format)
        """
        url = f"{self.base}/scoreboard"
        params = {}
        if dates:
            params["dates"] = dates
        data = self._get(url, params=params)
        if not data:
            return []

        parser = self._parse_cricket_game if self.is_cricket else self._parse_game
        games = []
        for event in data.get("events", []):
            game = parser(event)
            self._enrich_event(event, game)
            games.append(game)
        return games

    # ── Standard sport methods (unchanged) ────────────────────

    def _get(self, url: str, params: dict = None) -> dict | None:
        global _last_request_time
        # Rate limiting: ensure minimum gap between requests
        elapsed = _time.time() - _last_request_time
        if elapsed < MIN_REQUEST_GAP:
            _time.sleep(MIN_REQUEST_GAP - elapsed)

        try:
            resp = requests.get(url, params=params, timeout=8)
            _last_request_time = _time.time()
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            _last_request_time = _time.time()
            print(f"[ESPNClient] Request failed: {e}")
            return None

    def _parse_game(self, event: dict) -> dict:
        competitors = event.get("competitions", [{}])[0].get("competitors", [])
        home = next((c for c in competitors if c.get("homeAway") == "home"), {})
        away = next((c for c in competitors if c.get("homeAway") == "away"), {})
        status = event.get("status", {})

        return {
            "game_id":    event.get("id"),
            "sport":      self.sport,
            "home_team":  home.get("team", {}).get("displayName", "Home"),
            "away_team":  away.get("team", {}).get("displayName", "Away"),
            "home_score": home.get("score", "0"),
            "away_score": away.get("score", "0"),
            "period":     status.get("period", 1),
            "clock":      status.get("displayClock", ""),
            "situation":  status.get("type", {}).get("shortDetail", ""),
            "venue":      event.get("competitions", [{}])[0]
                              .get("venue", {}).get("fullName", ""),
        }

    def _parse_play(self, play: dict) -> dict:
        return {
            "play_id":      play.get("id", ""),
            "description":  play.get("text", play.get("description", "")),
            "score_value":  play.get("scoringPlay", False),
            "clock":        play.get("clock", {}).get("displayValue", ""),
            "period":       play.get("period", {}).get("number", 1),
            "team":         play.get("team", {}).get("displayName", ""),
            "athletes":     [
                a.get("athlete", {}).get("displayName", "")
                for a in play.get("participants", [])
            ],
            "type":         play.get("type", {}).get("text", ""),
        }

    def _parse_summary(self, data: dict) -> dict:
        boxscore = data.get("boxscore", {})
        teams    = boxscore.get("teams", [])
        summary  = {}
        for t in teams:
            name = t.get("team", {}).get("displayName", "Team")
            stats = {
                s.get("name"): s.get("displayValue")
                for s in t.get("statistics", [])
            }
            summary[name] = stats
        return summary
