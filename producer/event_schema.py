"""
event_schema.py
───────────────
Defines the PlayEvent dataclass — the contract for every event
flowing through the pipeline. Both producer and consumer import this.

Keeping schema in one place means if you add a field,
you change it here and it propagates everywhere.
"""

from dataclasses import dataclass, asdict
from datetime import datetime
import json
import uuid


@dataclass
class PlayEvent:
    """
    A single music play event — emitted when a user plays a track.
    Mirrors what a real streaming platform (Spotify, Last.fm) would generate.
    """
    event_id:         str       # UUID — unique per event, used for deduplication
    user_id:          str       # synthetic user identifier
    track_id:         str       # Last.fm MBID from Project 1 data
    track_name:       str       # real track name from Last.fm
    artist_id:        str       # Last.fm artist identifier
    artist_name:      str       # real artist name from Last.fm
    duration_ms:      int       # track duration in milliseconds
    played_ms:        int       # how long the user actually listened
    skipped:          bool      # did user skip before 80% of track?
    device:           str       # mobile / desktop / smart_tv / tablet
    country:          str       # ISO country code
    timestamp:        str       # ISO 8601 UTC timestamp
    source:           str = "simulator"

    @property
    def completion_rate(self) -> float:
        """Fraction of track played — 0.0 to 1.0"""
        if self.duration_ms == 0:
            return 0.0
        return round(self.played_ms / self.duration_ms, 3)

    def to_json(self) -> str:
        """Serialise to JSON string for Kafka message value."""
        d = asdict(self)
        d["completion_rate"] = self.completion_rate
        return json.dumps(d, ensure_ascii=False)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["completion_rate"] = self.completion_rate
        return d

    @classmethod
    def from_json(cls, json_str: str) -> "PlayEvent":
        """Deserialise from Kafka message value."""
        d = json.loads(json_str)
        d.pop("completion_rate", None)  # computed, not stored
        return cls(**d)

    @classmethod
    def make_event_id(cls) -> str:
        return str(uuid.uuid4())