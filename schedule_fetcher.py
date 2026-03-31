#!/usr/bin/env python3
"""
schedule_fetcher.py
Pulls the current NBA season schedule using the nba_api package
and saves it to data/NBA_Sched.csv.
"""

import argparse
import csv
import datetime
import logging
import os

from nba_api.stats.endpoints import leaguegamefinder

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DATA_DIR      = os.path.join(os.path.dirname(__file__), "data")
SCHEDULE_FILE = os.path.join(DATA_DIR, "NBA_Sched.csv")


def current_season() -> int:
    now = datetime.datetime.now()
    return now.year if now.month >= 10 else now.year - 1


def season_string(year: int) -> str:
    return f"{year}-{str(year + 1)[2:]}"


def fetch_schedule(season: int) -> list:
    season_str = season_string(season)
    log.info("Fetching schedule for %s season...", season_str)

    gamefinder = leaguegamefinder.LeagueGameFinder(
        season_nullable=season_str,
        league_id_nullable="00",
        season_type_nullable="Regular Season"
    )

    games_df = gamefinder.get_data_frames()[0]
    log.info("Retrieved %d game records", len(games_df))

    away_games = games_df[games_df["MATCHUP"].str.contains("@")].copy()

    schedule = []
    for _, row in away_games.iterrows():
        parts = row["MATCHUP"].split(" @ ")
        if len(parts) != 2:
            continue
        visitor_abbr = parts[0].strip()
        home_abbr    = parts[1].strip()
        game_date    = str(row["GAME_DATE"])[:10]
        schedule.append({"Game": game_date, "Vistor": visitor_abbr, "Home": home_abbr})

    schedule.sort(key=lambda x: x["Game"])
    log.info("Parsed %d games", len(schedule))
    return schedule


def save_schedule(games: list, filepath: str):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["Game", "Vistor", "Home"])
        writer.writeheader()
        writer.writerows(games)
    log.info("Saved %d games to %s", len(games), filepath)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch NBA schedule")
    parser.add_argument("--season", type=int, default=current_season())
    args = parser.parse_args()

    games = fetch_schedule(args.season)
    save_schedule(games, SCHEDULE_FILE)
    print(f"\n✅ Schedule saved to {SCHEDULE_FILE} ({len(games)} games)")
    print(f"   Season: {season_string(args.season)}")
