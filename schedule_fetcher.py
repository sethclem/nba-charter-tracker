#!/usr/bin/env python3
"""
schedule_fetcher.py
Pulls the current NBA season schedule using the nba_api package
and saves it to data/NBA_Sched.csv.

Run this once at the start of each season, or whenever you want
to refresh the schedule data.

Usage:
    python schedule_fetcher.py
    python schedule_fetcher.py --season 2024
"""

import argparse
import csv
import datetime
import logging
import os

from nba_api.stats.endpoints import scheduleleaguev2

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

    schedule = scheduleleaguev2.ScheduleLeagueV2(
        season=season_str,
        league_id="00"
    )

    games_df = schedule.get_data_frames()[0]
    log.info("Retrieved %d game records", len(games_df))

    result = []
    result = []
    for _, row in games_df.iterrows():
        game_date = datetime.datetime.strptime(str(row["gameDate"])[:10], "%m/%d/%Y").strftime("%Y-%m-%d")
        visitor   = str(row["awayTeam_teamTricode"])
        home      = str(row["homeTeam_teamTricode"])

        if game_date and visitor and home:
            result.append({"Game": game_date, "Vistor": visitor, "Home": home})

    result.sort(key=lambda x: x["Game"])
    log.info("Parsed %d games", len(result))
    return result


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