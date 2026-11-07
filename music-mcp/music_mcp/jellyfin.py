"""Read-only Jellyfin client. Only GET requests; the API key never leaves this module."""

from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass, field

import httpx

from .names import ArtistIndex, same_title, split_artists, title_key

CACHE_SECONDS = 600


@dataclass
class Track:
    title: str
    artists: list[str]
    album: str
    album_artist: str
    year: int | None
    genres: list[str]
    added: str  # ISO date


@dataclass
class Album:
    id: str
    name: str
    artists: list[str]
    year: int | None
    genres: list[str]
    added: str


@dataclass
class Library:
    tracks: list[Track] = field(default_factory=list)
    albums: list[Album] = field(default_factory=list)
    artists: ArtistIndex = field(default_factory=ArtistIndex)
    artist_tracks: Counter = field(default_factory=Counter)  # canonical artist -> track count (any credit)
    album_artist_tracks: Counter = field(default_factory=Counter)  # album artist (composer for film music) -> tracks
    fetched_at: float = 0.0

    def owns_artist(self, artist: str) -> str | None:
        return self.artists.lookup(artist)

    def owns_track(self, artist: str, title: str) -> Track | None:
        wanted = {self.artists.lookup(a) for a in split_artists(artist)} - {None}
        key = title_key(title)
        for t in self.tracks:
            if title_key(t.title) != key and not same_title(t.title, title):
                continue
            if not wanted:
                return t
            have = {self.artists.lookup(a) for a in t.artists + split_artists(t.album_artist)}
            if wanted & have:
                return t
        return None


class Jellyfin:
    def __init__(self, url: str, api_key: str, user: str | None = None, timeout: float = 30.0):
        self._client = httpx.AsyncClient(
            base_url=url.rstrip("/"),
            headers={"Authorization": f'MediaBrowser Client="music-mcp", Version="1", Token="{api_key}"'},
            timeout=timeout,
        )
        self._user_name = user
        self._user_id: str | None = None
        self._library: Library | None = None

    async def close(self) -> None:
        await self._client.aclose()

    async def _get(self, path: str, **params) -> dict | list:
        r = await self._client.get(path, params={k: v for k, v in params.items() if v is not None})
        r.raise_for_status()
        return r.json()

    async def user_id(self) -> str | None:
        if self._user_id or not self._user_name:
            return self._user_id
        users = await self._get("/Users")
        for u in users:
            if u.get("Name", "").lower() == self._user_name.lower():
                self._user_id = u["Id"]
                break
        return self._user_id

    async def library(self, refresh: bool = False) -> Library:
        if not refresh and self._library and time.time() - self._library.fetched_at < CACHE_SECONDS:
            return self._library
        lib = Library(fetched_at=time.time())
        fields = "DateCreated,Genres,ProductionYear,AlbumArtist,Artists"
        audio = await self._get(
            "/Items", IncludeItemTypes="Audio", Recursive="true", Fields=fields, EnableImages="false"
        )
        for it in audio.get("Items", []):
            artists = []
            for credit in it.get("Artists") or [it.get("AlbumArtist") or ""]:
                artists.extend(split_artists(credit))
            canon = [lib.artists.add(a) for a in artists]
            for a in canon:
                lib.artist_tracks[a] += 1
            for a in split_artists(it.get("AlbumArtist") or ""):
                lib.album_artist_tracks[lib.artists.add(a)] += 1
            lib.tracks.append(
                Track(
                    title=it.get("Name", ""),
                    artists=canon,
                    album=it.get("Album", ""),
                    album_artist=it.get("AlbumArtist", ""),
                    year=it.get("ProductionYear"),
                    genres=it.get("Genres") or [],
                    added=(it.get("DateCreated") or "")[:10],
                )
            )
        albums = await self._get(
            "/Items", IncludeItemTypes="MusicAlbum", Recursive="true", Fields=fields, EnableImages="false"
        )
        for it in albums.get("Items", []):
            artists = [lib.artists.add(a) for a in split_artists(it.get("AlbumArtist") or "")]
            lib.albums.append(
                Album(
                    id=it["Id"],
                    name=it.get("Name", ""),
                    artists=artists,
                    year=it.get("ProductionYear"),
                    genres=it.get("Genres") or [],
                    added=(it.get("DateCreated") or "")[:10],
                )
            )
        self._library = lib
        return lib

    async def favourites(self, limit: int = 50) -> list[dict]:
        uid = await self.user_id()
        if not uid:
            return []
        data = await self._get(
            "/Items",
            userId=uid,
            IsFavorite="true",
            Recursive="true",
            IncludeItemTypes="Audio,MusicAlbum,MusicArtist",
            Fields="AlbumArtist,Artists",
            Limit=limit,
            EnableImages="false",
        )
        out = []
        for it in data.get("Items", []):
            out.append(
                {
                    "type": it.get("Type"),
                    "name": it.get("Name"),
                    "artist": ", ".join(it.get("Artists") or []) or it.get("AlbumArtist"),
                }
            )
        return out
