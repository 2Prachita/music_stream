"""
event_generator.py
──────────────────
Generates realistic music play events using:
  - Real track/artist data from Last.fm (loaded from Project 1 raw JSON
    OR fetched live from Last.fm API if JSON not available)
  - Faker for synthetic user behaviour (who, when, where, device)

Why this approach is valid (for interviews):
  Every company uses event simulators in development.
  The streaming infrastructure is what's being tested — not the data source.
  Using real Last.fm track IDs makes the events credible and connects
  this project directly to the audio_intelligence pipeline (Project 1).
"""

import json
import random
import logging
from datetime import datetime, timezone
from pathlib import Path
from faker import Faker

from producer.event_schema import PlayEvent

logger = logging.getLogger(__name__)
fake = Faker()

# ── Config ────────────────────────────────────────────────────────────
DEVICES   = ["mobile", "desktop", "smart_tv", "tablet"]
DEVICE_WEIGHTS = [0.55, 0.25, 0.12, 0.08]   # mobile-heavy, realistic

COUNTRIES = ["IN", "US", "GB", "DE", "BR", "FR", "CA", "AU", "JP", "NG"]
COUNTRY_WEIGHTS = [0.20, 0.18, 0.10, 0.08, 0.08, 0.07, 0.07, 0.06, 0.08, 0.08]

# Simulated user pool — consistent user IDs across events (repeat listeners)
NUM_USERS = 500
USER_IDS  = [f"user_{str(i).zfill(4)}" for i in range(NUM_USERS)]

# Path to Project 1 raw track data — change if your path differs
PROJECT1_TRACKS_GLOB = [
    Path("../audio_intelligence/data/raw/tracks"),   # relative from project root
    Path("data/tracks"),                              # local fallback
]

# ── Load tracks ────────────────────────────────────────────────────────

def _load_tracks_from_project1() -> list[dict]:
    """
    Try to load real Last.fm track data from Project 1's raw JSON files.
    Falls back to fetching from Last.fm API if files not found.
    """
    for base in PROJECT1_TRACKS_GLOB:
        if base.exists():
            json_files = sorted(base.glob("*/tracks.json"), reverse=True)
            if json_files:
                latest = json_files[0]
                with open(latest, "r") as f:
                    tracks = json.load(f)
                logger.info("Loaded %d tracks from Project 1: %s", len(tracks), latest)
                return tracks

    logger.warning("Project 1 track data not found — fetching from Last.fm API")
    return _fetch_tracks_from_lastfm()


def _fetch_tracks_from_lastfm() -> list[dict]:
    """
    Fallback: fetch top chart tracks from Last.fm API.
    Requires LASTFM_API_KEY env var.
    """
    import os
    import requests

    api_key = os.getenv("LASTFM_API_KEY")
    if not api_key:
        logger.error("LASTFM_API_KEY not set — using dummy tracks")
        return _dummy_tracks()

    try:
        resp = requests.get(
            "https://ws.audioscrobbler.com/2.0/",
            params={"method": "chart.getTopTracks", "api_key": api_key,
                    "format": "json", "limit": 200},
            timeout=15
        )
        tracks_raw = resp.json().get("tracks", {}).get("track", [])
        tracks = []
        for t in tracks_raw:
            artist = t.get("artist", {})
            tracks.append({
                "track_id":    t.get("mbid") or f"lfm:{t.get('name','').lower()}",
                "track_name":  t.get("name", "Unknown"),
                "artist_id":   artist.get("mbid") or f"lfm:{artist.get('name','').lower()}",
                "artist_name": artist.get("name", "Unknown"),
                "duration_ms": int(t.get("duration", 210)) * 1000,
                "popularity":  int(t.get("playcount", 0)),
            })
        logger.info("Fetched %d tracks from Last.fm API", len(tracks))
        return tracks if tracks else _dummy_tracks()
    except Exception as e:
        logger.error("Last.fm fetch failed: %s — using dummy tracks", e)
        return _dummy_tracks()


def _dummy_tracks() -> list[dict]:
    """Last resort: a small hardcoded track list so the pipeline always runs."""
    return [
        {"track_id": "t001", "track_name": "Blinding Lights",
         "artist_id": "a001", "artist_name": "The Weeknd", "duration_ms": 200040, "popularity": 1000},
        {"track_id": "t002", "track_name": "Shape of You",
         "artist_id": "a002", "artist_name": "Ed Sheeran",  "duration_ms": 233713, "popularity": 900},
        {"track_id": "t003", "track_name": "Dance Monkey",
         "artist_id": "a003", "artist_name": "Tones and I", "duration_ms": 209438, "popularity": 850},
        {"track_id": "t004", "track_name": "Levitating",
         "artist_id": "a004", "artist_name": "Dua Lipa",    "duration_ms": 203064, "popularity": 800},
        {"track_id": "t005", "track_name": "Watermelon Sugar",
         "artist_id": "a005", "artist_name": "Harry Styles", "duration_ms": 174000, "popularity": 750},
    ]


# ── Generator class ───────────────────────────────────────────────────

class PlayEventGenerator:
    """
    Generates a stream of realistic play events.
    Popularity-weighted: popular tracks get played more often.
    """

    def __init__(self):
        self.tracks = _load_tracks_from_project1()
        # Weight by popularity so chart-toppers appear more often
        pops = [t.get("popularity", 1) or 1 for t in self.tracks]
        total = sum(pops)
        self.weights = [p / total for p in pops]
        logger.info("Generator ready — %d tracks in pool, %d users",
                    len(self.tracks), NUM_USERS)

    def _pick_track(self) -> dict:
        return random.choices(self.tracks, weights=self.weights, k=1)[0]

    def _simulate_play(self, track: dict) -> tuple[int, bool]:
        """
        Simulate user behaviour:
        Returns (played_ms, skipped).

        Behaviour model:
          30% chance → skip early (0–30s)
          20% chance → skip mid (30%–60% through)
          50% chance → listen fully (80–100%)
        """
        duration = track.get("duration_ms", 210000) or 210000
        roll = random.random()
        if roll < 0.30:
            # Early skip
            played = random.randint(5000, 30000)
            return min(played, duration), True
        elif roll < 0.50:
            # Mid-song skip
            played = int(duration * random.uniform(0.3, 0.6))
            return played, True
        else:
            # Full listen (80–100%)
            played = int(duration * random.uniform(0.8, 1.0))
            return min(played, duration), False

    def generate(self) -> PlayEvent:
        """Generate a single realistic play event."""
        track    = self._pick_track()
        played_ms, skipped = self._simulate_play(track)

        return PlayEvent(
            event_id    = PlayEvent.make_event_id(),
            user_id     = random.choice(USER_IDS),
            track_id = (
                track.get("track_id")
                or track.get("id")
                or str(uuid.uuid4())
            ),
            track_name = (
                track.get("track_name")
                or track.get("name")
                or "Unknown Track"
            ),
            artist_name = (
                track.get("artist_name")
                or track.get("primary_artist_name")
                or track.get("artist")
                or "Unknown Artist"
            ),
            artist_id = (
                track.get("artist_id")
                or track.get("primary_artist_id")
            ), 
            duration_ms = track.get("duration_ms", 210000) or 210000,
            played_ms   = played_ms,
            skipped     = skipped,
            device      = random.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0],
            country     = random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS, k=1)[0],
            timestamp   = datetime.now(timezone.utc).isoformat(),
        )

    def generate_batch(self, n: int) -> list[PlayEvent]:
        return [self.generate() for _ in range(n)]