#!/usr/bin/env python3
"""
NBA Charter Flight Tracker
Monitors Delta Air Lines charter flights and cross-references the NBA schedule
to identify teams traveling to away games. Sends Discord notifications.
"""

import os
import asyncio
import logging
from datetime import date, datetime, timedelta

import discord
import pandas as pd
import geopy.distance
from FlightRadar24 import FlightRadar24API
from dotenv import load_dotenv

load_dotenv()

import pytz
EASTERN = pytz.timezone("US/Eastern")

def today_eastern():
    return datetime.now(EASTERN).date()

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
DISCORD_TOKEN      = os.getenv("DISCORD_TOKEN")
DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID"))


POLL_INTERVAL      = 600          # seconds between FR24 polls (10 min)
AIRPORT_THRESHOLD  = 30           # max miles from airport to arena
SCHEDULE_LOOKAHEAD = 2            # days ahead to check for games

DATA_DIR      = os.path.join(os.path.dirname(__file__), "data")
COORDS_FILE   = os.path.join(DATA_DIR, "NBA_Coords.csv")
SCHEDULE_FILE = os.path.join(DATA_DIR, "NBA_Sched.csv")

# Delta charter callsign prefixes (DAL88xx, DAL89xx)
CHARTER_PREFIXES = ("DAL88", "DAL89")

# ── Load static data ───────────────────────────────────────────────────────────
nba_coords   = pd.read_csv(COORDS_FILE)
nba_schedule = pd.read_csv(SCHEDULE_FILE)

# Parse coordinate strings into tuples once at startup
nba_coords["lat"] = nba_coords["Coords"].apply(lambda x: float(x.split(",")[0].strip()))
nba_coords["lon"] = nba_coords["Coords"].apply(lambda x: float(x.split(",")[1].strip()))

log.info("Loaded %d arenas and %d scheduled games", len(nba_coords), len(nba_schedule))

# ── FR24 API ───────────────────────────────────────────────────────────────────
def get_fr24_api():
    return FlightRadar24API()


# ── Flight helpers ─────────────────────────────────────────────────────────────
def is_charter(callsign: str) -> bool:
    """Return True if the callsign matches a known Delta charter pattern."""
    return (
        any(callsign.startswith(p) for p in CHARTER_PREFIXES)
        and len(callsign) == 7
    )


import airportsdata
AIRPORTS = airportsdata.load("IATA")

def get_airport_coords(api, iata_code: str):
    """Look up lat/lon for an airport by IATA code using local airport database."""
    airport = AIRPORTS.get(iata_code)
    if airport:
        return (float(airport["lat"]), float(airport["lon"]))
    log.warning("Airport not found in database: %s", iata_code)
    return None


def nearest_arena(coords, arenas_df, threshold_miles=AIRPORT_THRESHOLD):
    """
    Find the nearest NBA arena to a lat/lon coordinate.
    Returns (team_code, distance_miles) or (None, None) if outside threshold.
    """
    best_team = None
    best_dist = float("inf")

    for _, row in arenas_df.iterrows():
        arena_coords = (row["lat"], row["lon"])
        dist = geopy.distance.geodesic(coords, arena_coords).mi
        if dist < best_dist:
            best_dist = dist
            best_team = row["Team"]

    if best_dist <= threshold_miles:
        return best_team, best_dist
    return None, best_dist


# ── Schedule helpers ───────────────────────────────────────────────────────────
def find_scheduled_game(visitor: str, home: str, schedule_df: pd.DataFrame) -> dict:
    """
    Check if visitor is playing at home within the lookahead window.
    Returns the matching row as a dict, or None.
    """
    today = today_eastern()

    for _, row in schedule_df.iterrows():
        try:
            game_date = datetime.strptime(str(row["Game"]).split(" ")[0], "%Y-%m-%d").date()
        except ValueError:
            continue

        days_diff = abs((game_date - today).days)

        if (
            str(row["Vistor"]) == visitor
            and str(row["Home"]) == home
            and days_diff <= SCHEDULE_LOOKAHEAD
        ):
            return {"date": game_date, "visitor": visitor, "home": home}

    return None


def find_road_trip_game(origin_team: str, dest_team: str, schedule_df: pd.DataFrame) -> dict:
    """
    Road trip check:
    - Find who recently played at origin_team's arena (they're the traveling team)
    - Check if that same team is dest_team's next home opponent
    """
    today = today_eastern()

    for _, row in schedule_df.iterrows():
        try:
            game_date = datetime.strptime(str(row["Game"]).split(" ")[0], "%Y-%m-%d").date()
        except ValueError:
            continue

        days_diff = (today - game_date).days

        # Who just visited origin_team's arena in the last 2 days?
        if str(row["Home"]) == origin_team and 0 <= days_diff <= 2:
            traveling_team = str(row["Vistor"])

            # Is that team dest_team's next home opponent?
            for _, row2 in schedule_df.iterrows():
                try:
                    game_date2 = datetime.strptime(str(row2["Game"]).split(" ")[0], "%Y-%m-%d").date()
                except ValueError:
                    continue

                future_diff = (game_date2 - today).days

                if (
                    str(row2["Vistor"]) == traveling_team
                    and str(row2["Home"]) == dest_team
                    and 0 <= future_diff <= SCHEDULE_LOOKAHEAD
                ):
                    return {
                        "date": game_date2,
                        "visitor": traveling_team,
                        "home": dest_team,
                        "road_trip": True
                    }

    return None

def find_return_home(origin_team: str, dest_team: str, schedule_df: pd.DataFrame) -> dict:
    """
    Return home check:
    - origin_team recently played an away game at origin city
    - dest_team is the same as the traveling team (flying home)
    """
    today = today_eastern()

    for _, row in schedule_df.iterrows():
        try:
            game_date = datetime.strptime(str(row["Game"]).split(" ")[0], "%Y-%m-%d").date()
        except ValueError:
            continue

        days_diff = (today - game_date).days

        # Did dest_team just play an away game at origin_team's arena?
        if (
            str(row["Vistor"]) == dest_team
            and str(row["Home"]) == origin_team
            and 0 <= days_diff <= 1
        ):
            # Check if they have a home game coming up
            upcoming_home_game = None
            for _, row2 in schedule_df.iterrows():
                try:
                    game_date2 = datetime.strptime(str(row2["Game"]).split(" ")[0], "%Y-%m-%d").date()
                except ValueError:
                    continue

                future_diff = (game_date2 - today).days

                if (
                    str(row2["Home"]) == dest_team
                    and 0 <= future_diff <= SCHEDULE_LOOKAHEAD
                ):
                    upcoming_home_game = {"date": game_date2, "visitor": str(row2["Vistor"]), "home": dest_team}

            return {
                "date": game_date,
                "visitor": dest_team,
                "home": origin_team,
                "return_home": True,
                "upcoming_home_game": upcoming_home_game
            }

    return None

# ── Discord bot ────────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

tracking_task = None   # holds the asyncio Task when active


async def build_notification(flight, game: dict, origin_team: str, dest_team: str) -> str:
    """Format a Discord notification message for a matched charter flight."""
    game_date = game["date"].strftime("%a %b %d")
    fr24_url = f"https://www.flightradar24.com/{flight.callsign}/{flight.id}"

    if game.get("return_home"):
        msg = [
            "🏠 **NBA Charter — Return Flight**",
            f"✈️  `{flight.callsign}`  |  {flight.origin_airport_iata} → {flight.destination_airport_iata}  |  {flight.aircraft_code}",
            f"🏟️  **{dest_team}** flying home after playing **{origin_team}**",
            f"📅  Away game: {game_date}",
            f"🔗  {fr24_url}",
        ]
        if game.get("upcoming_home_game"):
            hg = game["upcoming_home_game"]
            hg_date = hg["date"].strftime("%a %b %d")
            msg.append(f"🏠  Next home game vs **{hg['visitor']}** on {hg_date}")

    elif game.get("road_trip"):
        msg = [
            "🔄 **NBA Charter — Road Trip**",
            f"✈️  `{flight.callsign}`  |  {flight.origin_airport_iata} → {flight.destination_airport_iata}  |  {flight.aircraft_code}",
            f"🏟️  **{game['visitor']}** mid road trip, next up: **{dest_team}**",
            f"📅  Next game: {game_date}",
            f"🔗  {fr24_url}",
        ]

    else:
        msg = [
            "🏀 **NBA Charter — Away Game**",
            f"✈️  `{flight.callsign}`  |  {flight.origin_airport_iata} → {flight.destination_airport_iata}  |  {flight.aircraft_code}",
            f"🏟️  **{origin_team}** flying to play **{dest_team}**",
            f"📅  Game date: {game_date}",
            f"🔗  {fr24_url}",
        ]

    return "\n".join(msg)

async def poll_charters(channel: discord.TextChannel):
    """
    Main polling loop. Runs until cancelled.
    Checks FR24 every POLL_INTERVAL seconds for NBA charter flights.
    """
    api = get_fr24_api()
    seen_flights: set[str] = set()   # deduplicate within a session

    log.info("Polling started — checking every %ds", POLL_INTERVAL)

    while True:
        try:
            log.info("Fetching Delta flights from FR24...")
            dal_flights = api.get_flights(airline="DAL")

            charter_count = 0
            for flight in dal_flights:
                callsign = flight.callsign or ""

                if not is_charter(callsign):
                    continue

                # Deduplicate by callsign + date so we don't re-alert the same flight
                flight_key = f"{callsign}_{today_eastern()}"
                if flight_key in seen_flights:
                    continue

                origin_iata = flight.origin_airport_iata
                dest_iata   = flight.destination_airport_iata

                if not origin_iata or not dest_iata:
                    continue

                charter_count += 1
                log.info("Charter spotted: %s  %s→%s", callsign, origin_iata, dest_iata)

                # Look up airport coordinates
                origin_coords = get_airport_coords(api, origin_iata)
                dest_coords   = get_airport_coords(api, dest_iata)

                if not origin_coords or not dest_coords:
                    log.warning("Could not resolve coords for %s or %s", origin_iata, dest_iata)
                    continue

                # Find nearest NBA arenas
                origin_team, origin_dist = nearest_arena(origin_coords, nba_coords)
                dest_team,   dest_dist   = nearest_arena(dest_coords,   nba_coords)

                if not origin_team or not dest_team:
                    log.info(
                        "%s: arenas too far (origin %.0fmi, dest %.0fmi)",
                        callsign, origin_dist, dest_dist
                    )
                    continue

                log.info("%s: %s (%s) → %s (%s)", callsign, origin_team, origin_iata, dest_team, dest_iata)

                # Check schedule for a direct game match
                game = find_scheduled_game(origin_team, dest_team, nba_schedule)

                # If no direct match, check road trip pattern
                if not game:
                    game = find_road_trip_game(origin_team, dest_team, nba_schedule)

                # If no road trip match, check return home
                if not game:
                    game = find_return_home(origin_team, dest_team, nba_schedule)

                if game:
                    seen_flights.add(flight_key)
                    msg = await build_notification(flight, game, origin_team, dest_team)
                    await channel.send(msg)
                    log.info("Notification sent for %s", callsign)
                else:
                    log.info("%s: no matching game found in schedule", callsign)

        except Exception as e:
            log.error("Error during poll: %s", e)
            await channel.send(f"⚠️ Tracker error: `{e}` — will retry next poll.")

        await asyncio.sleep(POLL_INTERVAL)


@client.event
async def on_ready():
    log.info("Bot connected as %s", client.user)


@client.event
async def on_message(message: discord.Message):
    global tracking_task

    # Ignore messages from the bot itself
    if message.author == client.user:
        return

    # Only respond in the configured channel
    if message.channel.id != DISCORD_CHANNEL_ID:
        return

    content = message.content.strip().lower()

    if content == "start":
        if tracking_task and not tracking_task.done():
            await message.channel.send("👀 Already tracking! Type `stop` to pause.")
            return

        await message.channel.send(
            "🛫 **NBA Charter Tracker is live!**\n"
            f"Checking every {POLL_INTERVAL // 60} minutes for Delta charter flights.\n"
            "Type `stop` to pause."
        )
        channel = message.channel
        tracking_task = asyncio.create_task(poll_charters(channel))

    elif content == "stop":
        if tracking_task and not tracking_task.done():
            tracking_task.cancel()
            await message.channel.send("✋ Tracking paused. Type `start` to resume.")
            log.info("Tracking stopped by user.")
        else:
            await message.channel.send("Not currently tracking. Type `start` to begin.")

    elif content == "status":
        if tracking_task and not tracking_task.done():
            await message.channel.send("✅ Tracker is **active** and polling every 10 minutes.")
        else:
            await message.channel.send("⏸️ Tracker is **paused**. Type `start` to begin.")

    elif content == "help":
        await message.channel.send(
            "**NBA Charter Tracker Commands**\n"
            "`start` — begin monitoring Delta charter flights\n"
            "`stop`  — pause monitoring\n"
            "`status` — check if tracker is running\n"
            "`help`  — show this message"
        )


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise ValueError("DISCORD_TOKEN not set in environment / .env file")
    if not DISCORD_CHANNEL_ID:
        raise ValueError("DISCORD_CHANNEL_ID not set in environment / .env file")

    client.run(DISCORD_TOKEN)
