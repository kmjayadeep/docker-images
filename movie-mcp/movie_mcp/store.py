"""SQLite store for movie digests, suggestions and operator feedback. Movies are keyed by TMDB id."""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone

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
  tmdb_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  year INTEGER,
  language TEXT,
  genres TEXT,
  reason TEXT,
  url TEXT,
  feedback TEXT CHECK (feedback IN ('like', 'dislike', 'want')),
  feedback_at TEXT
);
CREATE INDEX IF NOT EXISTS suggestions_tmdb ON suggestions (tmdb_id);
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

    def suggested_ids(self) -> set[int]:
        return {r["tmdb_id"] for r in self._rows("SELECT DISTINCT tmdb_id FROM suggestions")}

    def feedback_ids(self, action: str) -> list[int]:
        """TMDB ids with this feedback, newest first."""
        return [r["tmdb_id"] for r in self._rows(
            "SELECT tmdb_id FROM suggestions WHERE feedback = ? ORDER BY feedback_at DESC", action)]

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
                digest_id = self._db.execute(
                    "INSERT INTO digests (created_at, note) VALUES (?, ?)", (_now(), note)).lastrowid
            stored = []
            for it in items:
                cur = self._db.execute(
                    """INSERT INTO suggestions (digest_id, created_at, tmdb_id, title, year, language, genres, reason, url)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (digest_id, _now(), it["tmdb_id"], it["title"], it.get("year"), it.get("language"),
                     ", ".join(it.get("genres") or []) or None, it.get("reason"), it.get("url")),
                )
                stored.append({**it, "id": cur.lastrowid})
            self._db.commit()
        return digest_id, stored

    def set_feedback(self, suggestion_id: int, action: str) -> dict | None:
        if action not in ACTIONS:
            raise ValueError(f"action must be one of {ACTIONS}")
        with self._lock:
            cur = self._db.execute(
                "UPDATE suggestions SET feedback = ?, feedback_at = ? WHERE id = ?", (action, _now(), suggestion_id))
            self._db.commit()
            if cur.rowcount == 0:
                return None
        return self._rows("SELECT * FROM suggestions WHERE id = ?", suggestion_id)[0]

    def history(self, days: int = 365, limit: int = 200) -> list[dict]:
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        return self._rows(
            """SELECT id, digest_id, created_at, tmdb_id, title, year, language, feedback FROM suggestions
               WHERE created_at >= ? ORDER BY id DESC LIMIT ?""", since, limit)

    def digest(self, digest_id: int | None = None) -> dict | None:
        if digest_id is None:
            rows = self._rows("SELECT * FROM digests ORDER BY id DESC LIMIT 1")
        else:
            rows = self._rows("SELECT * FROM digests WHERE id = ?", digest_id)
        if not rows:
            return None
        d = rows[0]
        items = self._rows(
            """SELECT id, tmdb_id, title, year, language, genres, reason, url, feedback FROM suggestions
               WHERE digest_id = ? ORDER BY id""", d["id"])
        # `kind` and `search_url` keep the Telegram bot's digest rows generic across music and movies.
        d["items"] = [{**i, "kind": "movie", "search_url": i["url"]} for i in items]
        return d

    def wants(self) -> list[dict]:
        return self._rows(
            "SELECT id, tmdb_id, title, year, language, url, feedback_at FROM suggestions WHERE feedback = 'want' ORDER BY feedback_at")

    def feedback_summary(self) -> dict:
        rows = self._rows("SELECT title, year, language, genres, feedback FROM suggestions WHERE feedback IS NOT NULL ORDER BY feedback_at DESC")
        out: dict[str, list] = {a: [] for a in ACTIONS}
        genre_score: dict[str, int] = {}
        for r in rows:
            out[r["feedback"]].append(f'{r["title"]} ({r["year"]}, {r["language"]})')
            for g in (r["genres"] or "").split(", "):
                if g:
                    genre_score[g] = genre_score.get(g, 0) + (-1 if r["feedback"] == "dislike" else 1)
        totals = self._rows(
            "SELECT COUNT(*) AS suggested, SUM(feedback IS NOT NULL) AS rated, COUNT(DISTINCT digest_id) AS digests FROM suggestions")[0]
        return {
            "totals": {k: v or 0 for k, v in totals.items()},
            "liked": out["like"][:25], "wanted": out["want"][:25], "disliked": out["dislike"][:25],
            "genre_score": sorted(genre_score.items(), key=lambda x: -x[1]),
        }
