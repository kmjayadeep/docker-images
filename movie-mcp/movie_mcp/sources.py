"""Read-only clients: Jellyfin (library and watch state), Radarr (movies already added) and TMDB (catalogue).

Movies are identified by TMDB id everywhere; Jellyfin (ProviderIds.Tmdb) and Radarr (tmdbId) both carry it.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import date

import httpx

TMDB_MOVIE_URL = "https://www.themoviedb.org/movie/{}"
CACHE_SECONDS = 600


@dataclass
class LibraryMovie:
    tmdb_id: int | None
    title: str
    year: int | None
    genres: list[str]
    added: str | None
    played: bool
    last_played: str | None
    favourite: bool


@dataclass
class Library:
    movies: list[LibraryMovie] = field(default_factory=list)
    fetched_at: float = 0.0

    @property
    def tmdb_ids(self) -> set[int]:
        return {m.tmdb_id for m in self.movies if m.tmdb_id}

    def recently_watched(self, n: int = 10) -> list[LibraryMovie]:
        watched = [m for m in self.movies if m.last_played]
        return sorted(watched, key=lambda m: m.last_played or "", reverse=True)[:n]


class Jellyfin:
    def __init__(self, url: str, api_key: str, user: str | None, timeout: float = 30.0):
        self._client = httpx.AsyncClient(
            base_url=url.rstrip("/"), timeout=timeout, headers={"Authorization": f"MediaBrowser Token={api_key}"}
        )
        self._user_name = user
        self._user_id: str | None = None
        self._cache: Library | None = None
        self._lock = asyncio.Lock()

    async def _get(self, path: str, **params) -> dict | list:
        r = await self._client.get(path, params=params)
        r.raise_for_status()
        return r.json()

    async def _uid(self) -> str:
        if self._user_id is None:
            users = await self._get("/Users")
            match = [u for u in users if u.get("Name") == self._user_name] if self._user_name else users[:1]
            if not match:
                raise RuntimeError(f"Jellyfin user {self._user_name!r} not found")
            self._user_id = match[0]["Id"]
        return self._user_id

    async def library(self, refresh: bool = False) -> Library:
        async with self._lock:
            if self._cache and not refresh and time.time() - self._cache.fetched_at < CACHE_SECONDS:
                return self._cache
            uid = await self._uid()
            data = await self._get(
                f"/Users/{uid}/Items", IncludeItemTypes="Movie", Recursive="true",
                Fields="ProviderIds,Genres,DateCreated,ProductionYear", EnableUserData="true",
            )
            lib = Library(fetched_at=time.time())
            for it in data.get("Items", []):
                ud = it.get("UserData") or {}
                tid = (it.get("ProviderIds") or {}).get("Tmdb")
                lib.movies.append(LibraryMovie(
                    tmdb_id=int(tid) if tid and str(tid).isdigit() else None,
                    title=it.get("Name", ""), year=it.get("ProductionYear"), genres=it.get("Genres") or [],
                    added=(it.get("DateCreated") or "")[:10] or None, played=bool(ud.get("Played")),
                    last_played=(ud.get("LastPlayedDate") or "")[:10] or None, favourite=bool(ud.get("IsFavorite")),
                ))
            self._cache = lib
            return lib


@dataclass
class RadarrMovie:
    tmdb_id: int
    title: str
    year: int | None
    language: str | None
    added: str | None
    has_file: bool
    monitored: bool


class Radarr:
    """GET /api/v3/movie only. Optional: without a URL and key nothing counts as added."""

    def __init__(self, url: str | None, api_key: str | None, timeout: float = 30.0):
        self.enabled = bool(url and api_key)
        self._client = httpx.AsyncClient(
            base_url=(url or "http://localhost").rstrip("/"), timeout=timeout, headers={"X-Api-Key": api_key or ""}
        )
        self._cache: tuple[float, list[RadarrMovie]] | None = None
        self._lock = asyncio.Lock()

    async def movies(self, refresh: bool = False) -> list[RadarrMovie]:
        if not self.enabled:
            return []
        async with self._lock:
            if self._cache and not refresh and time.time() - self._cache[0] < CACHE_SECONDS:
                return self._cache[1]
            r = await self._client.get("/api/v3/movie")
            r.raise_for_status()
            out = [
                RadarrMovie(
                    tmdb_id=int(m["tmdbId"]), title=m.get("title", ""), year=m.get("year"),
                    language=(m.get("originalLanguage") or {}).get("name"), added=(m.get("added") or "")[:10] or None,
                    has_file=bool(m.get("hasFile")), monitored=bool(m.get("monitored")),
                )
                for m in r.json() if m.get("tmdbId")
            ]
            self._cache = (time.time(), out)
            return out


class Tmdb:
    BASE = "https://api.themoviedb.org/3"

    def __init__(self, api_key: str, timeout: float = 20.0, concurrency: int = 6):
        self._key = api_key
        self._client = httpx.AsyncClient(timeout=timeout, headers={"User-Agent": "movie-mcp/1 (homelab)"})
        self._sem = asyncio.Semaphore(concurrency)
        self._genres: dict[int, str] | None = None
        self._details: dict[int, dict] = {}

    async def _get(self, path: str, **params) -> dict:
        async with self._sem:
            r = await self._client.get(self.BASE + path, params={"api_key": self._key, **params})
        if r.status_code == 404:
            return {}
        r.raise_for_status()
        return r.json()

    async def genres(self) -> dict[int, str]:
        if self._genres is None:
            data = await self._get("/genre/movie/list")
            self._genres = {g["id"]: g["name"] for g in data.get("genres", [])}
        return self._genres

    async def genre_ids(self, names: list[str]) -> list[int]:
        by_name = {v.lower(): k for k, v in (await self.genres()).items()}
        return [by_name[n.lower()] for n in names if n.lower() in by_name]

    async def movie(self, tmdb_id: int) -> dict:
        """Details with credits, cached for the process lifetime (details rarely change)."""
        if tmdb_id not in self._details:
            self._details[tmdb_id] = await self._get(f"/movie/{tmdb_id}", append_to_response="credits")
        return self._details[tmdb_id]

    async def short(self, r: dict) -> dict:
        """The compact candidate shape shared by every tool."""
        g = await self.genres()
        names = [x["name"] for x in r["genres"]] if "genres" in r else [g[i] for i in r.get("genre_ids", []) if i in g]
        return {
            "tmdb_id": r["id"],
            "title": r.get("title") or r.get("original_title"),
            "original_title": r.get("original_title") if r.get("original_title") != r.get("title") else None,
            "year": int(r["release_date"][:4]) if r.get("release_date") else None,
            "release_date": r.get("release_date") or None,
            "language": r.get("original_language"),
            "genres": names,
            "rating": round(float(r.get("vote_average") or 0), 1),
            "votes": int(r.get("vote_count") or 0),
            "popularity": round(float(r.get("popularity") or 0), 1),
            "overview": (r.get("overview") or "")[:150] or None,
            "url": TMDB_MOVIE_URL.format(r["id"]),
        }

    async def recommendations(self, tmdb_id: int) -> list[dict]:
        data = await self._get(f"/movie/{tmdb_id}/recommendations")
        return data.get("results", [])

    async def discover(self, **params) -> list[dict]:
        data = await self._get("/discover/movie", include_adult="false", **params)
        return data.get("results", [])

    async def person_movies(self, person_id: int, as_director: bool) -> list[dict]:
        data = await self._get(f"/person/{person_id}/movie_credits")
        if as_director:
            return [m for m in data.get("crew", []) if m.get("job") == "Director"]
        return data.get("cast", [])

    async def search(self, query: str, year: int | None = None) -> list[dict]:
        params = {"query": query, "include_adult": "false"}
        if year:
            params["year"] = year
        data = await self._get("/search/movie", **params)
        return data.get("results", [])


def released(r: dict, today: date | None = None) -> bool:
    d = r.get("release_date")
    return bool(d) and d <= (today or date.today()).isoformat()
