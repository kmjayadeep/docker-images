"""movie-mcp: MCP tools for the kagent movie-curator agent, plus plain HTTP routes for the Telegram bot.

Environment:
  TMDB_API_KEY                        required (TMDB API v3 key)
  JELLYFIN_URL, JELLYFIN_API_KEY      required
  JELLYFIN_USER                       Jellyfin user whose watch state counts (default: first user)
  RADARR_URL, RADARR_API_KEY          optional; movies already in Radarr are never suggested
  NEW_RELEASE_WINDOWS                 language:days pairs for new releases, default "ml:120,en:60,hi:90"
  DB_PATH                             default /data/movie.db
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

from .sources import Jellyfin, Radarr, Tmdb, released
from .store import ACTIONS, Store

log = logging.getLogger("movie-mcp")

OPEN_DIGEST_MINUTES = int(os.environ.get("OPEN_DIGEST_MINUTES", "40"))
ALLOWED_HOSTS = [h.strip() for h in os.environ.get("ALLOWED_HOSTS", "localhost,127.0.0.1").split(",") if h.strip()]


def _windows(raw: str) -> dict[str, int]:
    out = {}
    for part in raw.split(","):
        lang, _, days = part.strip().partition(":")
        if lang and days.isdigit():
            out[lang] = int(days)
    return out


NEW_RELEASE_WINDOWS = _windows(os.environ.get("NEW_RELEASE_WINDOWS", "ml:120,en:60,hi:90"))
# English releases are numerous; a vote floor keeps unknown straight-to-video titles out. Regional releases
# have few TMDB votes (0-10 for a new Malayalam film), so they are ranked by popularity without a floor.
NEW_RELEASE_MIN_VOTES = {"en": 30}

mcp = FastMCP(
    "movie-mcp",
    instructions=(
        "Tools for finding movies for one viewer, identified by TMDB id. Candidates come back with movies the "
        "viewer owns (Jellyfin), has already added (Radarr), was already suggested or disliked removed. "
        "record_digest stores the final picks."
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
_radarr: Radarr | None = None
_tmdb: Tmdb | None = None
_store: Store | None = None


def jellyfin() -> Jellyfin:
    global _jf
    if _jf is None:
        _jf = Jellyfin(os.environ["JELLYFIN_URL"], os.environ["JELLYFIN_API_KEY"], os.environ.get("JELLYFIN_USER"))
    return _jf


def radarr() -> Radarr:
    global _radarr
    if _radarr is None:
        _radarr = Radarr(os.environ.get("RADARR_URL"), os.environ.get("RADARR_API_KEY"))
    return _radarr


def tmdb() -> Tmdb:
    global _tmdb
    if _tmdb is None:
        _tmdb = Tmdb(os.environ["TMDB_API_KEY"])
    return _tmdb


def store() -> Store:
    global _store
    if _store is None:
        _store = Store(os.environ.get("DB_PATH", "/data/movie.db"))
    return _store


# ---------------------------------------------------------------- shared helpers


async def _state() -> dict[str, set[int]]:
    lib, rad = await asyncio.gather(jellyfin().library(), radarr().movies())
    st = store()
    return {
        "owned": lib.tmdb_ids,
        "radarr": {m.tmdb_id for m in rad},
        "suggested": st.suggested_ids(),
        "disliked": set(st.feedback_ids("dislike")),
    }


def _why_excluded(tmdb_id: int, state: dict[str, set[int]]) -> str | None:
    if tmdb_id in state["owned"]:
        return "owned (in Jellyfin)"
    if tmdb_id in state["radarr"]:
        return "already added to Radarr"
    if tmdb_id in state["disliked"]:
        return "disliked"
    if tmdb_id in state["suggested"]:
        return "suggested before"
    return None


async def _expand(seed_id: int) -> tuple[dict | None, list[tuple[dict, str]]]:
    """Neighbours of one movie as (raw TMDB result, how it was found).

    TMDB recommendations are fine for English films but return only English films for Malayalam and Hindi
    seeds (plan 0027 step 0), so non-English seeds expand through the same language and genres, ranked by
    votes, plus the director's and two leading actors' films in that language.
    """
    t = tmdb()
    m = await t.movie(seed_id)
    if not m:
        return None, []
    lang = m.get("original_language")
    found: list[tuple[dict, str]] = []
    if lang == "en":
        found += [(r, "TMDB recommendation") for r in await t.recommendations(seed_id)]
    if lang != "en" or len(found) < 5:
        gids = [g["id"] for g in m.get("genres", [])][:2]
        same = await t.discover(with_original_language=lang, with_genres="|".join(map(str, gids)),
                                sort_by="vote_count.desc", **{"vote_count.gte": 10})
        found += [(r, "same language and genre") for r in same]
        crew = (m.get("credits") or {}).get("crew", [])
        cast = (m.get("credits") or {}).get("cast", [])
        people = [(p, True) for p in crew if p.get("job") == "Director"][:1] + [(p, False) for p in cast[:2]]
        films = await asyncio.gather(*(t.person_movies(p["id"], d) for p, d in people), return_exceptions=True)
        for (p, director), rows in zip(people, films):
            if isinstance(rows, Exception):
                continue
            role = "director" if director else "actor"
            for r in rows:
                if r.get("original_language") == lang and r.get("vote_count", 0) >= 3:
                    found.append((r, f"{role} {p['name']}"))
    return m, [(r, via) for r, via in found if r.get("id") != seed_id and released(r)]


def _merge(rows: list[tuple[dict, str, str]], state: dict[str, set[int]]) -> dict[int, dict]:
    """Merge (raw, via, seed title) rows by TMDB id, dropping excluded movies."""
    merged: dict[int, dict] = {}
    for r, via, seed in rows:
        if _why_excluded(r["id"], state):
            continue
        c = merged.setdefault(r["id"], {"raw": r, "seeds": [], "via": []})
        if seed not in c["seeds"]:
            c["seeds"].append(seed)
        if via not in c["via"]:
            c["via"].append(via)
    return merged


def _rank(c: dict) -> tuple:
    r = c["raw"]
    return (len(c["seeds"]), min(r.get("vote_count", 0), 500) * (r.get("vote_average") or 0))


async def _default_seeds(n: int) -> list[int]:
    lib = await jellyfin().library()
    st = store()
    seeds: list[int] = []
    for tid in st.feedback_ids("like")[:3] + st.feedback_ids("want")[:2]:
        if tid not in seeds:
            seeds.append(tid)
    for m in lib.recently_watched(n * 2):
        if m.tmdb_id and m.tmdb_id not in seeds:
            seeds.append(m.tmdb_id)
    return seeds[:n]


# ---------------------------------------------------------------- MCP tools


@mcp.tool()
async def taste_profile() -> dict:
    """What the viewer watched and added recently, and language, genre and decade counts of their collection
    (Jellyfin library plus movies added to Radarr)."""
    lib, rad = await asyncio.gather(jellyfin().library(), radarr().movies())
    t = tmdb()
    rad_by_id = {m.tmdb_id: m for m in rad}
    langs = Counter(m.language for m in rad if m.language)
    decades = Counter(f"{(m.year // 10) * 10}s" for m in lib.movies if m.year)
    genres = Counter(g for m in lib.movies for g in m.genres)
    watched = lib.recently_watched(10)
    details = await asyncio.gather(*(t.movie(m.tmdb_id) for m in watched if m.tmdb_id), return_exceptions=True)
    lang_of = {d["id"]: d.get("original_language") for d in details if isinstance(d, dict) and d.get("id")}
    return {
        "library_movies": len(lib.movies),
        "watched_total": sum(1 for m in lib.movies if m.played),
        "recently_watched": [
            {"tmdb_id": m.tmdb_id, "title": m.title, "year": m.year, "language": lang_of.get(m.tmdb_id),
             "last_played": m.last_played} for m in watched],
        "recently_added_to_radarr": [
            {"tmdb_id": m.tmdb_id, "title": m.title, "year": m.year, "language": m.language, "added": m.added,
             "has_file": m.has_file} for m in sorted(rad, key=lambda m: m.added or "", reverse=True)[:10]],
        "radarr_languages": langs.most_common(),
        "library_genres": genres.most_common(10),
        "library_decades": sorted(decades.items()),
        "radarr_missing_monitored": sum(1 for m in rad_by_id.values() if m.monitored and not m.has_file),
    }


@mcp.tool()
async def digest_candidates(seeds: list[int] | None = None, max_seeds: int = 6, per_seed: int = 6,
                            per_language: int = 8) -> dict:
    """Everything needed for a weekly digest in ONE call.

    `by_seed`: neighbours of recently watched, liked and wanted movies (or of `seeds`, TMDB ids).
    `new_releases`: recent releases per language (NEW_RELEASE_WINDOWS), most popular first.
    Owned, already added to Radarr, already suggested and disliked movies are removed. A candidate found
    from several seeds lists all of them and ranks higher."""
    t = tmdb()
    state = await _state()
    seed_ids = seeds or await _default_seeds(max_seeds)
    expansions = await asyncio.gather(*(_expand(s) for s in seed_ids[:max_seeds]), return_exceptions=True)
    rows: list[tuple[dict, str, str]] = []
    seed_info = []
    for sid, ex in zip(seed_ids, expansions):
        if isinstance(ex, Exception) or not ex[0]:
            log.warning("seed %s failed: %s", sid, ex if isinstance(ex, Exception) else "not found")
            continue
        m, found = ex
        label = f'{m.get("title")} ({(m.get("release_date") or "")[:4]})'
        seed_info.append({"tmdb_id": sid, "title": label, "language": m.get("original_language")})
        rows += [(r, via, label) for r, via in found]
    merged = _merge(rows, state)
    by_seed: dict[str, list[dict]] = {}
    for c in sorted(merged.values(), key=_rank, reverse=True):
        seed = c["seeds"][0]
        if len(by_seed.setdefault(seed, [])) >= per_seed:
            continue
        by_seed[seed].append({**await t.short(c["raw"]), "found_via": c["via"][:3], "seeds": c["seeds"]})

    today = date.today()
    new: dict[str, list[dict]] = {}

    async def releases(lang: str, days: int) -> None:
        params = {
            "with_original_language": lang, "sort_by": "popularity.desc",
            "primary_release_date.gte": (today - timedelta(days=days)).isoformat(),
            "primary_release_date.lte": today.isoformat(),
        }
        if lang in NEW_RELEASE_MIN_VOTES:
            params["vote_count.gte"] = NEW_RELEASE_MIN_VOTES[lang]
        res = [r for r in await t.discover(**params) if not _why_excluded(r["id"], state)]
        new[lang] = [await t.short(r) for r in res[:per_language]]

    await asyncio.gather(*(releases(lang, days) for lang, days in NEW_RELEASE_WINDOWS.items()))
    return {"seeds": seed_info, "by_seed": by_seed, "new_releases": new,
            "release_windows_days": NEW_RELEASE_WINDOWS}


@mcp.tool()
async def similar(tmdb_id: int, limit: int = 12) -> dict:
    """Movies like one movie (TMDB id), with owned, added, suggested and disliked movies removed."""
    state = await _state()
    m, found = await _expand(tmdb_id)
    if not m:
        return {"error": f"no TMDB movie {tmdb_id}"}
    merged = _merge([(r, via, m.get("title", "")) for r, via in found], state)
    ranked = sorted(merged.values(), key=_rank, reverse=True)[:limit]
    t = tmdb()
    return {"seed": await t.short(m), "similar": [{**await t.short(c["raw"]), "found_via": c["via"][:3]} for c in ranked]}


@mcp.tool()
async def discover(language: str, since_days: int = 90, genres: list[str] | None = None,
                   sort: str = "popularity", min_votes: int = 0, limit: int = 15) -> dict:
    """Released movies in one original language (ISO 639-1: ml, en, hi, ta, ko...) from the last `since_days`
    days (0 = any time), optionally with any of `genres` (TMDB names such as Thriller, Comedy).
    sort: popularity | votes | rating."""
    t = tmdb()
    state = await _state()
    today = date.today()
    params = {"with_original_language": language, "primary_release_date.lte": today.isoformat(),
              "sort_by": {"votes": "vote_count.desc", "rating": "vote_average.desc"}.get(sort, "popularity.desc")}
    if since_days:
        params["primary_release_date.gte"] = (today - timedelta(days=since_days)).isoformat()
    if genres:
        params["with_genres"] = "|".join(map(str, await t.genre_ids(genres)))
    if min_votes or sort == "rating":
        params["vote_count.gte"] = max(min_votes, 10)
    res = [r for r in await t.discover(**params) if not _why_excluded(r["id"], state)]
    return {"movies": [await t.short(r) for r in res[:limit]]}


@mcp.tool()
async def search(query: str, year: int | None = None) -> dict:
    """Find movies by title. Each result says whether the viewer owns it, added it, or was suggested it."""
    t = tmdb()
    state = await _state()
    out = []
    for r in (await t.search(query, year))[:8]:
        out.append({**await t.short(r), "status": _why_excluded(r["id"], state)})
    return {"movies": out}


@mcp.tool()
async def movie_details(tmdb_id: int) -> dict:
    """One movie with director, main cast, runtime and whether the viewer has it."""
    t = tmdb()
    m = await t.movie(tmdb_id)
    if not m:
        return {"error": f"no TMDB movie {tmdb_id}"}
    credits = m.get("credits") or {}
    return {
        **await t.short(m),
        "runtime": m.get("runtime"),
        "director": [p["name"] for p in credits.get("crew", []) if p.get("job") == "Director"],
        "cast": [p["name"] for p in credits.get("cast", [])[:6]],
        "status": _why_excluded(tmdb_id, await _state()),
    }


@mcp.tool()
async def suggestion_history(days: int = 365, limit: int = 100) -> dict:
    """Movies suggested in earlier digests, newest first, with the viewer's feedback."""
    return {"suggestions": store().history(days, limit)}


@mcp.tool()
async def feedback_summary() -> dict:
    """Movies the viewer liked, wanted or disliked, and a genre score from that feedback. Liked and wanted
    movies are used as seeds automatically; disliked ones are never suggested again."""
    return store().feedback_summary()


@mcp.tool()
async def record_digest(items: list[dict], note: str | None = None) -> dict:
    """Store this digest's final picks, all of them in ONE call. Each item: {tmdb_id, reason}; `reason` is one
    short line shown to the viewer. Title, year and link come from TMDB. Owned, already added, already
    suggested, disliked and unreleased movies are rejected. Calls within the same run add to the same digest."""
    t = tmdb()
    state = await _state()
    st = store()
    accepted, rejected, seen = [], [], set()
    for raw in items:
        try:
            tid = int(raw.get("tmdb_id"))
        except (TypeError, ValueError):
            rejected.append({**raw, "why": "needs an integer tmdb_id"})
            continue
        why = _why_excluded(tid, state) or ("duplicate in this call" if tid in seen else None)
        m = None if why else await t.movie(tid)
        if not why and not m:
            why = "not found on TMDB"
        if not why and not released(m):
            why = "not released yet"
        if why:
            rejected.append({"tmdb_id": tid, "why": why})
            continue
        seen.add(tid)
        accepted.append({**await t.short(m), "reason": (raw.get("reason") or "").strip()[:160] or None})
    open_id = st.open_digest_id(OPEN_DIGEST_MINUTES)
    if not accepted:
        return {"digest_id": open_id, "stored": [], "rejected": rejected}
    digest_id, stored = st.record_digest(accepted, note, append_to=open_id)
    return {
        "digest_id": digest_id,
        "stored": [{"id": x["id"], "tmdb_id": x["tmdb_id"], "title": x["title"], "year": x["year"],
                    "language": x["language"], "url": x["url"]} for x in stored],
        "rejected": rejected,
        "digest_items_total": len(st.digest(digest_id)["items"]),
        "next": "Done. Do not call record_digest again; write the digest from all stored items.",
    }


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
    # httpx logs every request URL at INFO, and the Last.fm and TMDB keys travel as query parameters.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    store()
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
