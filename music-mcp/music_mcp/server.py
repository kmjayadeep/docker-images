"""music-mcp: MCP tools for the kagent music-curator agent, plus plain HTTP routes for the Telegram bot.

Environment:
  JELLYFIN_URL, JELLYFIN_API_KEY      required
  JELLYFIN_USER                       optional, Jellyfin user whose favourites count
  LASTFM_API_KEY                      optional; Last.fm sources are skipped without it
  YTM_LOCATION                        default IN (affects YouTube Music search ranking)
  DB_PATH                             default /data/music.db
  HOST, PORT                          default 0.0.0.0, 8000
  ALLOWED_HOSTS                       comma-separated Host headers accepted on /mcp (DNS-rebinding protection)
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections import Counter
from datetime import date, timedelta

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import JSONResponse

from .jellyfin import Jellyfin
from .names import artist_key, split_artists
from .sources import LastFm, YtMusic
from .store import ACTIONS, Store

log = logging.getLogger("music-mcp")

ALLOWED_HOSTS = [h.strip() for h in os.environ.get("ALLOWED_HOSTS", "localhost,127.0.0.1").split(",") if h.strip()]

mcp = FastMCP(
    "music-mcp",
    instructions=(
        "Tools for finding new music for one listener. Library tools describe what they already own; "
        "discovery tools return candidates with owned and already-suggested items removed; record_digest "
        "stores the final picks. Play counts are deliberately not available: the listener plays music on a "
        "separate player, so taste comes from recent additions, favourites and feedback."
    ),
    host=os.environ.get("HOST", "0.0.0.0"),
    port=int(os.environ.get("PORT", "8000")),
    stateless_http=True,
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=ALLOWED_HOSTS + [f"{h}:*" for h in ALLOWED_HOSTS],
    ),
)

_jf: Jellyfin | None = None
_lfm: LastFm | None = None
_ytm: YtMusic | None = None
_store: Store | None = None


def jellyfin() -> Jellyfin:
    global _jf
    if _jf is None:
        _jf = Jellyfin(os.environ["JELLYFIN_URL"], os.environ["JELLYFIN_API_KEY"], os.environ.get("JELLYFIN_USER"))
    return _jf


def lastfm() -> LastFm:
    global _lfm
    if _lfm is None:
        _lfm = LastFm(os.environ.get("LASTFM_API_KEY"))
    return _lfm


def ytm() -> YtMusic:
    global _ytm
    if _ytm is None:
        _ytm = YtMusic(location=os.environ.get("YTM_LOCATION", "IN"))
    return _ytm


def store() -> Store:
    global _store
    if _store is None:
        _store = Store(os.environ.get("DB_PATH", "/data/music.db"))
    return _store


async def _safe(coro, label: str, errors: list[str]):
    try:
        return await coro
    except Exception as e:  # one failing source must not fail the tool
        log.warning("%s failed: %s", label, e)
        errors.append(f"{label}: {type(e).__name__}")
        return None


async def _filter(candidates: list[dict], include_owned: bool = False) -> tuple[list[dict], dict]:
    """Drop owned, already suggested and disliked-artist candidates; merge duplicates across sources."""
    lib = await jellyfin().library()
    st = store()
    disliked = st.disliked_artist_keys()
    dropped = Counter()
    merged: dict[tuple[str, str], dict] = {}
    for c in candidates:
        artists = split_artists(c.get("artist") or "")
        akeys = {artist_key(a) for a in artists}
        if akeys & disliked:
            dropped["disliked_artist"] += 1
            continue
        if c["kind"] == "artist":
            owned = lib.owns_artist(c["artist"])
            if owned and not include_owned:
                dropped["owned"] += 1
                continue
            key = (artist_key(c["artist"]), "")
        else:
            if not include_owned and lib.owns_track(c["artist"], c["title"]):
                dropped["owned"] += 1
                continue
            if st.already_suggested(c["artist"], c["title"]):
                dropped["suggested_before"] += 1
                continue
            key = (artist_key(artists[0]) if artists else "", c["title"].lower())
            c["new_artist"] = not any(lib.owns_artist(a) for a in artists)
        if key in merged:
            prev = merged[key]
            prev["sources"] = sorted(set(prev["sources"]) | {c["source"]})
            prev["score"] = round(max(prev.get("score") or 0, c.get("score") or 0), 2)
            if not prev.get("url") or "youtube" not in (prev.get("url") or ""):
                prev["url"] = c.get("url") or prev.get("url")
            continue
        c["sources"] = [c.pop("source")]
        merged[key] = c
    out = sorted(merged.values(), key=lambda x: (-len(x["sources"]), -(x.get("score") or 0)))
    return out, dict(dropped)


# ---------------------------------------------------------------- library tools


@mcp.tool()
async def library_profile(top_artists: int = 40) -> dict:
    """Summary of the owned music library: size, the most represented album artists (for film music
    usually the composer) and credited artists (singers, lyricists), names merged across spellings,
    plus genres and decades. Use it first to understand the listener's taste."""
    lib = await jellyfin().library()
    genres = Counter(g for t in lib.tracks for g in t.genres)
    decades = Counter(f"{(t.year // 10) * 10}s" for t in lib.tracks if t.year)
    return {
        "tracks": len(lib.tracks),
        "albums": len(lib.albums),
        "artists": len(lib.artists),
        # Album artists are usually the composer for film soundtracks: the best taste anchor.
        "top_album_artists": [{"artist": a, "tracks": n} for a, n in lib.album_artist_tracks.most_common(top_artists)],
        "top_credited_artists": [{"artist": a, "tracks": n} for a, n in lib.artist_tracks.most_common(top_artists)],
        "genres": genres.most_common(15),
        "decades": sorted(decades.items()),
    }


@mcp.tool()
async def recent_additions(days: int = 90, limit: int = 25) -> dict:
    """Albums added to the library in the last `days` days, newest first. Recently added music is the
    strongest sign of current taste."""
    lib = await jellyfin().library()
    since = (date.today() - timedelta(days=days)).isoformat()
    albums = sorted((a for a in lib.albums if a.added >= since), key=lambda a: a.added, reverse=True)
    return {
        "since": since,
        "albums": [
            {"album": a.name, "artists": a.artists, "year": a.year, "added": a.added} for a in albums[:limit]
        ],
        "total": len(albums),
    }


@mcp.tool()
async def favourites(limit: int = 50) -> dict:
    """Tracks, albums and artists the listener marked as favourite in Jellyfin."""
    return {"favourites": await jellyfin().favourites(limit)}


@mcp.tool()
async def in_library(artist: str, title: str | None = None) -> dict:
    """Check whether an artist (and optionally a track title) is already owned. Spelling variants match."""
    lib = await jellyfin().library()
    canon = [lib.owns_artist(a) for a in split_artists(artist) or [artist]]
    out: dict = {"artist_owned": [c for c in canon if c], "artist_tracks": sum(lib.artist_tracks[c] for c in canon if c)}
    if title:
        t = lib.owns_track(artist, title)
        out["track_owned"] = bool(t)
        if t:
            out["match"] = {"title": t.title, "album": t.album, "artists": t.artists}
    return out


# ---------------------------------------------------------------- discovery tools


@mcp.tool()
async def similar_artists(artist: str, limit: int = 15, include_owned: bool = False) -> dict:
    """Artists similar to `artist` from YouTube Music and Last.fm. Artists already in the library are
    removed unless include_owned is true. Artists found by both sources are listed first."""
    errors: list[str] = []
    yt, lf = await asyncio.gather(
        _safe(ytm().related_artists(artist, limit), "ytmusic", errors),
        _safe(lastfm().similar_artists(artist, limit), "lastfm", errors),
    )
    found, dropped = await _filter((yt or []) + (lf or []), include_owned=include_owned)
    return {"seed": artist, "artists": found[:limit], "dropped": dropped, "errors": errors}


@mcp.tool()
async def related_tracks(artist: str, title: str, limit: int = 15) -> dict:
    """Tracks related to one seed song: the YouTube Music radio for that song plus Last.fm similar tracks.
    Owned tracks, earlier suggestions and disliked artists are removed. Best source for Indian film music."""
    errors: list[str] = []
    yt, lf = await asyncio.gather(
        _safe(ytm().related_tracks(artist, title, limit + 10), "ytmusic", errors),
        _safe(lastfm().similar_tracks(artist, title, limit), "lastfm", errors),
    )
    seed, related = yt if yt else (None, [])
    found, dropped = await _filter(related + (lf or []))
    return {
        "seed": seed or {"artist": artist, "title": title, "resolved": False},
        "tracks": found[:limit],
        "dropped": dropped,
        "errors": errors,
    }


@mcp.tool()
async def artist_top_tracks(artist: str, limit: int = 10) -> dict:
    """Most popular songs of an artist (YouTube Music, Last.fm as fallback), without owned or already
    suggested ones. Use it to pick entry points for an artist new to the listener."""
    errors: list[str] = []
    tracks = await _safe(ytm().top_tracks(artist, limit + 5), "ytmusic", errors) or []
    if not tracks:
        tracks = await _safe(lastfm().top_tracks(artist, limit + 5), "lastfm", errors) or []
    found, dropped = await _filter(tracks)
    return {"artist": artist, "tracks": found[:limit], "dropped": dropped, "errors": errors}


@mcp.tool()
async def new_releases(artists: list[str] | None = None, since_year: int | None = None, per_artist: int = 3) -> dict:
    """Latest albums and singles from the given artists, or from the 10 album artists (composers) with the
    most tracks in the library. Only releases from since_year (default: last year) onwards are returned."""
    lib = await jellyfin().library()
    if not artists:
        artists = [a for a, _ in lib.album_artist_tracks.most_common(10)]
    since_year = since_year or date.today().year - 1
    errors: list[str] = []
    results = await asyncio.gather(*(_safe(ytm().latest_releases(a, 8), f"ytmusic:{a}", errors) for a in artists[:12]))
    owned_albums = {a.name.lower() for a in lib.albums}
    out = []
    for a, rels in zip(artists, results):
        fresh = [
            r for r in rels or []
            if str(r.get("year") or "").isdigit() and int(r["year"]) >= since_year
            and (r.get("title") or "").lower() not in owned_albums
        ][:per_artist]
        if fresh:
            out.append({"artist": a, "releases": fresh})
    return {"since_year": since_year, "results": out, "errors": errors}


@mcp.tool()
async def search(query: str, kind: str = "songs", limit: int = 8) -> dict:
    """Search YouTube Music. kind is songs, albums or artists. Use it to resolve a suggestion to a
    preview link or to look for film soundtracks (for example "Malayalam movie songs 2026")."""
    if kind not in ("songs", "albums", "artists"):
        return {"error": "kind must be songs, albums or artists"}
    results = await ytm().search(query, kind, limit)
    for r in results:
        if r["kind"] == "track":
            r["owned"] = bool((await jellyfin().library()).owns_track(r["artist"], r["title"]))
    return {"query": query, "results": results}


# ---------------------------------------------------------------- memory tools


@mcp.tool()
async def suggestion_history(days: int = 180, limit: int = 100) -> dict:
    """Earlier suggestions with the listener's feedback (like, dislike, want, or null if unrated)."""
    return {"suggestions": store().history(days, limit)}


@mcp.tool()
async def feedback_summary() -> dict:
    """Artists the listener liked, wanted or disliked in earlier digests, with counts. Steer seeds
    towards liked and wanted artists; disliked artists are filtered out automatically."""
    return store().feedback_summary()


@mcp.tool()
async def record_digest(items: list[dict], note: str | None = None) -> dict:
    """Store this digest's final picks. Each item: {kind: "track"|"album", artist, title, album?, year?,
    reason, url?}. `reason` is one short line shown to the listener. Items already owned or suggested
    before are rejected; missing preview links are looked up. Call this once, then write the digest."""
    lib = await jellyfin().library()
    st = store()
    accepted, rejected = [], []
    for raw in items:
        it = {k: raw.get(k) for k in ("kind", "artist", "title", "album", "year", "reason", "url")}
        if it["kind"] not in ("track", "album") or not it["artist"] or not it["title"]:
            rejected.append({**it, "why": "needs kind track|album, artist and title"})
            continue
        if it["kind"] == "track" and lib.owns_track(it["artist"], it["title"]):
            rejected.append({**it, "why": "owned"})
            continue
        if st.already_suggested(it["artist"], it["title"]):
            rejected.append({**it, "why": "suggested before"})
            continue
        if not it["url"]:
            try:
                kind = "songs" if it["kind"] == "track" else "albums"
                hits = await ytm().search(f'{it["artist"]} {it["title"]}', kind, 1)
                it["url"] = hits[0]["url"] if hits else None
            except Exception as e:
                log.warning("link lookup failed: %s", e)
        accepted.append(it)
    if not accepted:
        return {"digest_id": None, "stored": [], "rejected": rejected}
    digest_id, stored = st.record_digest(accepted, note)
    return {"digest_id": digest_id, "stored": stored, "rejected": rejected}


# ---------------------------------------------------------------- HTTP routes for the bot


@mcp.custom_route("/healthz", methods=["GET"])
async def healthz(_: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


@mcp.custom_route("/digest", methods=["GET"])
async def get_digest(request: Request) -> JSONResponse:
    raw = request.query_params.get("id")
    d = store().digest(int(raw) if raw and raw.isdigit() else None)
    return JSONResponse(d or {"error": "not found"}, status_code=200 if d else 404)


@mcp.custom_route("/feedback", methods=["POST"])
async def post_feedback(request: Request) -> JSONResponse:
    try:
        body = await request.json()
        sid, action = int(body["id"]), str(body["action"])
    except Exception:
        return JSONResponse({"error": "body must be {id: int, action: like|dislike|want}"}, status_code=400)
    if action not in ACTIONS:
        return JSONResponse({"error": f"action must be one of {ACTIONS}"}, status_code=400)
    row = store().set_feedback(sid, action)
    return JSONResponse(row or {"error": "not found"}, status_code=200 if row else 404)


@mcp.custom_route("/wants", methods=["GET"])
async def get_wants(_: Request) -> JSONResponse:
    return JSONResponse({"wants": store().wants()})


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    store()  # create the schema before serving
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
