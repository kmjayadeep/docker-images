"""Discovery sources: Last.fm (official API, optional) and YouTube Music (ytmusicapi, unauthenticated).

Every function returns plain dicts with the same keys where possible:
  artist, title, album, year, kind (track|album|artist), source, url, score
"""

from __future__ import annotations

import asyncio
from functools import partial

import httpx
from ytmusicapi import YTMusic

YTM_WATCH = "https://music.youtube.com/watch?v={}"
YTM_BROWSE = "https://music.youtube.com/browse/{}"
YTM_CHANNEL = "https://music.youtube.com/channel/{}"


def _names(artists: list[dict] | None) -> str:
    return ", ".join(a["name"] for a in artists or [] if a.get("name"))


class LastFm:
    BASE = "https://ws.audioscrobbler.com/2.0/"

    def __init__(self, api_key: str | None, timeout: float = 20.0):
        self.enabled = bool(api_key)
        self._key = api_key
        self._client = httpx.AsyncClient(timeout=timeout, headers={"User-Agent": "music-mcp/1 (homelab)"})

    async def close(self) -> None:
        await self._client.aclose()

    async def _call(self, method: str, **params) -> dict:
        if not self.enabled:
            return {}
        r = await self._client.get(
            self.BASE, params={"method": method, "api_key": self._key, "format": "json", "autocorrect": 1, **params}
        )
        r.raise_for_status()
        data = r.json()
        if "error" in data:  # unknown artist or track: treat as no results
            return {}
        return data

    async def similar_artists(self, artist: str, limit: int = 15) -> list[dict]:
        data = await self._call("artist.getSimilar", artist=artist, limit=limit)
        return [
            {"kind": "artist", "artist": a["name"], "score": round(float(a.get("match", 0)), 2),
             "source": "lastfm", "url": a.get("url")}
            for a in data.get("similarartists", {}).get("artist", [])
        ]

    async def similar_tracks(self, artist: str, title: str, limit: int = 15) -> list[dict]:
        data = await self._call("track.getSimilar", artist=artist, track=title, limit=limit)
        return [
            {"kind": "track", "artist": t["artist"]["name"], "title": t["name"],
             "score": round(float(t.get("match", 0)), 2), "source": "lastfm", "url": t.get("url")}
            for t in data.get("similartracks", {}).get("track", [])
        ]

    async def top_tracks(self, artist: str, limit: int = 10) -> list[dict]:
        data = await self._call("artist.getTopTracks", artist=artist, limit=limit)
        return [
            {"kind": "track", "artist": t["artist"]["name"], "title": t["name"], "source": "lastfm",
             "url": t.get("url")}
            for t in data.get("toptracks", {}).get("track", [])
        ]


class YtMusic:
    """ytmusicapi is synchronous; every call runs in a worker thread."""

    def __init__(self, location: str = "IN", language: str = "en"):
        self._yt = YTMusic(location=location, language=language)

    async def _run(self, fn, *args, **kwargs):
        return await asyncio.to_thread(partial(fn, *args, **kwargs))

    async def find_song(self, artist: str, title: str) -> dict | None:
        res = await self._run(self._yt.search, f"{artist} {title}", filter="songs", limit=3)
        if not res:
            return None
        s = res[0]
        return self._song(s, source="ytmusic")

    def _song(self, s: dict, source: str, score: float | None = None) -> dict:
        album = s.get("album") or {}
        out = {
            "kind": "track",
            "artist": _names(s.get("artists")),
            "title": s.get("title", ""),
            "album": album.get("name") if isinstance(album, dict) else None,
            "year": s.get("year"),
            "source": source,
            "url": YTM_WATCH.format(s["videoId"]) if s.get("videoId") else None,
        }
        if score is not None:
            out["score"] = score
        return out

    async def related_tracks(self, artist: str, title: str, limit: int = 20) -> tuple[dict | None, list[dict]]:
        """YouTube Music radio after the seed song. Returns (resolved seed, related tracks)."""
        seed = await self._run(self._yt.search, f"{artist} {title}", filter="songs", limit=1)
        if not seed or not seed[0].get("videoId"):
            return None, []
        wp = await self._run(self._yt.get_watch_playlist, videoId=seed[0]["videoId"], limit=limit + 1)
        tracks = wp.get("tracks", [])[1:]
        n = max(len(tracks), 1)
        related = [self._song(t, source="ytmusic-radio", score=round(1 - i / n, 2)) for i, t in enumerate(tracks)]
        return self._song(seed[0], source="ytmusic"), related

    async def _artist(self, artist: str) -> dict | None:
        res = await self._run(self._yt.search, artist, filter="artists", limit=1)
        if not res or not res[0].get("browseId"):
            return None
        return await self._run(self._yt.get_artist, res[0]["browseId"])

    async def related_artists(self, artist: str, limit: int = 15) -> list[dict]:
        a = await self._artist(artist)
        if not a:
            return []
        rel = (a.get("related") or {}).get("results", [])[:limit]
        n = max(len(rel), 1)
        return [
            {"kind": "artist", "artist": r.get("title"), "score": round(1 - i / n, 2), "source": "ytmusic",
             "url": YTM_CHANNEL.format(r["browseId"]) if r.get("browseId") else None}
            for i, r in enumerate(rel)
        ]

    async def top_tracks(self, artist: str, limit: int = 10) -> list[dict]:
        a = await self._artist(artist)
        if not a:
            return []
        songs = (a.get("songs") or {}).get("results", [])[:limit]
        return [self._song(s, source="ytmusic") for s in songs]

    async def latest_releases(self, artist: str, limit: int = 6) -> list[dict]:
        """Albums and singles of one artist, newest first (YouTube Music lists them by year)."""
        a = await self._artist(artist)
        if not a:
            return []
        out = []
        for section, kind in (("albums", "album"), ("singles", "single")):
            for r in (a.get(section) or {}).get("results", []):
                out.append(
                    {"kind": "album", "release_type": kind, "artist": a.get("name") or artist,
                     "title": r.get("title"), "year": r.get("year"), "source": "ytmusic",
                     "url": YTM_BROWSE.format(r["browseId"]) if r.get("browseId") else None}
                )
        out.sort(key=lambda x: str(x.get("year") or ""), reverse=True)
        return out[:limit]

    async def search(self, query: str, kind: str = "songs", limit: int = 10) -> list[dict]:
        res = await self._run(self._yt.search, query, filter=kind, limit=limit)
        out = []
        for r in res[:limit]:
            if kind == "songs":
                out.append(self._song(r, source="ytmusic"))
            elif kind == "albums":
                out.append(
                    {"kind": "album", "artist": _names(r.get("artists")), "title": r.get("title"),
                     "year": r.get("year"), "source": "ytmusic",
                     "url": YTM_BROWSE.format(r["browseId"]) if r.get("browseId") else None}
                )
            else:
                out.append({"kind": "artist", "artist": r.get("artist") or r.get("title"), "source": "ytmusic",
                            "url": YTM_CHANNEL.format(r["browseId"]) if r.get("browseId") else None})
        return out
