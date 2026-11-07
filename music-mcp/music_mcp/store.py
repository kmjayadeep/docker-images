"""SQLite store for digests, suggestions and operator feedback."""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone

from .names import artist_key, split_artists, title_key

SCHEMA = """
CREATE TABLE IF NOT EXISTS digests (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  note TEXT
);
CREATE TABLE IF NOT EXISTS suggestions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  digest_id INTEGER REFERENCES digests(id),
  created_at TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('track', 'album')),
  artist TEXT NOT NULL,
  title TEXT NOT NULL,
  album TEXT,
  year TEXT,
  reason TEXT,
  url TEXT,
  artist_key TEXT NOT NULL,
  title_key TEXT NOT NULL,
  feedback TEXT CHECK (feedback IN ('like', 'dislike', 'want')),
  feedback_at TEXT
);
CREATE INDEX IF NOT EXISTS suggestions_keys ON suggestions (artist_key, title_key);
"""

ACTIONS = ("like", "dislike", "want")


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class Store:
    def __init__(self, path: str):
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock:
            self._db.executescript(SCHEMA)
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.commit()

    def _rows(self, sql: str, *args) -> list[dict]:
        with self._lock:
            return [dict(r) for r in self._db.execute(sql, args).fetchall()]

    @staticmethod
    def _primary_artist_key(artist: str) -> str:
        parts = split_artists(artist)
        return artist_key(parts[0] if parts else artist)

    def already_suggested(self, artist: str, title: str) -> dict | None:
        tk = title_key(title)
        akeys = {artist_key(a) for a in split_artists(artist)} or {artist_key(artist)}
        for r in self._rows("SELECT * FROM suggestions WHERE title_key = ?", tk):
            if not akeys or r["artist_key"] in akeys or akeys & {artist_key(a) for a in split_artists(r["artist"])}:
                return r
        return None

    def disliked_artist_keys(self) -> set[str]:
        keys: set[str] = set()
        for r in self._rows("SELECT artist FROM suggestions WHERE feedback = 'dislike'"):
            keys.update(artist_key(a) for a in split_artists(r["artist"]))
        return keys

    def open_digest_id(self, minutes: int) -> int | None:
        """The newest digest if it was created in the last `minutes` minutes (one agent run = one digest)."""
        since = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).replace(microsecond=0).isoformat()
        rows = self._rows("SELECT id FROM digests WHERE created_at >= ? ORDER BY id DESC LIMIT 1", since)
        return rows[0]["id"] if rows else None

    def record_digest(self, items: list[dict], note: str | None = None,
                      append_to: int | None = None) -> tuple[int, list[dict]]:
        with self._lock:
            if append_to is not None:
                digest_id = append_to
            else:
                cur = self._db.execute("INSERT INTO digests (created_at, note) VALUES (?, ?)", (_now(), note))
                digest_id = cur.lastrowid
            stored = []
            for it in items:
                cur = self._db.execute(
                    """INSERT INTO suggestions (digest_id, created_at, kind, artist, title, album, year, reason, url,
                       artist_key, title_key) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        digest_id, _now(), it["kind"], it["artist"], it["title"], it.get("album"),
                        str(it["year"]) if it.get("year") else None, it.get("reason"), it.get("url"),
                        self._primary_artist_key(it["artist"]), title_key(it["title"]),
                    ),
                )
                stored.append({**it, "id": cur.lastrowid})
            self._db.commit()
        return digest_id, stored

    def set_feedback(self, suggestion_id: int, action: str) -> dict | None:
        if action not in ACTIONS:
            raise ValueError(f"action must be one of {ACTIONS}")
        with self._lock:
            cur = self._db.execute(
                "UPDATE suggestions SET feedback = ?, feedback_at = ? WHERE id = ?", (action, _now(), suggestion_id)
            )
            self._db.commit()
            if cur.rowcount == 0:
                return None
        return self._rows("SELECT * FROM suggestions WHERE id = ?", suggestion_id)[0]

    def history(self, days: int = 180, limit: int = 200) -> list[dict]:
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        return self._rows(
            """SELECT id, digest_id, created_at, kind, artist, title, album, feedback FROM suggestions
               WHERE created_at >= ? ORDER BY id DESC LIMIT ?""",
            since, limit,
        )

    def digest(self, digest_id: int | None = None) -> dict | None:
        if digest_id is None:
            rows = self._rows("SELECT * FROM digests ORDER BY id DESC LIMIT 1")
        else:
            rows = self._rows("SELECT * FROM digests WHERE id = ?", digest_id)
        if not rows:
            return None
        d = rows[0]
        d["items"] = self._rows(
            "SELECT id, kind, artist, title, album, year, reason, url, feedback FROM suggestions WHERE digest_id = ? ORDER BY id",
            d["id"],
        )
        return d

    def wants(self) -> list[dict]:
        return self._rows(
            "SELECT id, kind, artist, title, album, year, url, feedback_at FROM suggestions WHERE feedback = 'want' ORDER BY feedback_at"
        )

    def feedback_summary(self) -> dict:
        out: dict[str, dict[str, int]] = {a: {} for a in ACTIONS}
        for r in self._rows("SELECT artist, feedback FROM suggestions WHERE feedback IS NOT NULL"):
            for a in split_artists(r["artist"]) or [r["artist"]]:
                out[r["feedback"]][a] = out[r["feedback"]].get(a, 0) + 1
        totals = self._rows(
            "SELECT COUNT(*) AS suggested, SUM(feedback IS NOT NULL) AS rated, COUNT(DISTINCT digest_id) AS digests FROM suggestions"
        )[0]
        return {
            "totals": {k: v or 0 for k, v in totals.items()},
            "liked_artists": sorted(out["like"].items(), key=lambda x: -x[1])[:25],
            "wanted_artists": sorted(out["want"].items(), key=lambda x: -x[1])[:25],
            "disliked_artists": sorted(out["dislike"].items(), key=lambda x: -x[1])[:25],
        }
