import asyncio

import pytest

from movie_mcp import server
from movie_mcp.sources import Library, LibraryMovie, RadarrMovie, released
from movie_mcp.store import Store

OWNED, IN_RADARR, NEW, OTHER, FUTURE = 10, 20, 30, 40, 50

MOVIES = {
    NEW: {"id": NEW, "title": "Kishkindha Kaandam", "release_date": "2024-09-12", "original_language": "ml",
          "genres": [{"id": 9648, "name": "Mystery"}], "vote_average": 7.6, "vote_count": 67, "credits": {}},
    OTHER: {"id": OTHER, "title": "Tumbbad", "release_date": "2018-10-12", "original_language": "hi",
            "genres": [{"id": 27, "name": "Horror"}], "vote_average": 7.6, "vote_count": 374, "credits": {}},
    FUTURE: {"id": FUTURE, "title": "Later", "release_date": "2099-01-01", "original_language": "ml",
             "genres": [], "vote_average": 0, "vote_count": 0, "credits": {}},
}


class FakeJellyfin:
    async def library(self, refresh=False):
        return Library(movies=[LibraryMovie(OWNED, "Drishyam", 2013, ["Thriller"], "2026-01-01", True, "2026-11-01", False)])


class FakeRadarr:
    async def movies(self, refresh=False):
        return [RadarrMovie(IN_RADARR, "The Odyssey", 2026, "English", "2026-08-08", False, True)]


class FakeTmdb:
    async def movie(self, tmdb_id):
        return MOVIES.get(tmdb_id, {})

    async def genres(self):
        return {9648: "Mystery", 27: "Horror"}

    async def short(self, r):
        from movie_mcp.sources import Tmdb
        return await Tmdb.short(self, r)


@pytest.fixture
def env(tmp_path, monkeypatch):
    st = Store(str(tmp_path / "movie.db"))
    monkeypatch.setattr(server, "_store", st)
    monkeypatch.setattr(server, "_jf", FakeJellyfin())
    monkeypatch.setattr(server, "_radarr", FakeRadarr())
    monkeypatch.setattr(server, "_tmdb", FakeTmdb())
    return st


def run(coro):
    return asyncio.run(coro)


def test_record_digest_rejects_owned_added_unreleased_and_repeats(env):
    res = run(server.record_digest([
        {"tmdb_id": OWNED, "reason": "x"},
        {"tmdb_id": IN_RADARR, "reason": "x"},
        {"tmdb_id": FUTURE, "reason": "x"},
        {"tmdb_id": 999, "reason": "x"},
        {"tmdb_id": "abc"},
        {"tmdb_id": NEW, "reason": "because you watched Drishyam"},
        {"tmdb_id": NEW, "reason": "dup"},
    ]))
    assert [s["tmdb_id"] for s in res["stored"]] == [NEW]
    assert res["stored"][0]["url"] == f"https://www.themoviedb.org/movie/{NEW}"
    whys = {r.get("tmdb_id"): r["why"] for r in res["rejected"]}
    assert whys[OWNED].startswith("owned")
    assert whys[IN_RADARR] == "already added to Radarr"
    assert whys[FUTURE] == "not released yet"
    assert whys[999] == "not found on TMDB"
    assert any(r["why"] == "duplicate in this call" for r in res["rejected"])

    again = run(server.record_digest([{"tmdb_id": NEW, "reason": "x"}, {"tmdb_id": OTHER, "reason": "y"}]))
    assert [s["tmdb_id"] for s in again["stored"]] == [OTHER]
    assert again["digest_id"] == res["digest_id"]  # same run appends to the open digest
    assert again["digest_items_total"] == 2


def test_digest_feedback_and_dislike_exclusion(env):
    res = run(server.record_digest([{"tmdb_id": NEW, "reason": "r"}, {"tmdb_id": OTHER, "reason": "r"}]))
    d = env.digest()
    assert [i["kind"] for i in d["items"]] == ["movie", "movie"]
    assert d["items"][0]["search_url"] == d["items"][0]["url"]
    ids = [s["id"] for s in res["stored"]]
    env.set_feedback(ids[0], "dislike")
    env.set_feedback(ids[1], "want")
    assert env.feedback_ids("dislike") == [NEW]
    assert [w["tmdb_id"] for w in env.wants()] == [OTHER]
    summary = env.feedback_summary()
    assert summary["totals"]["rated"] == 2
    assert ("Horror", 1) in summary["genre_score"] and ("Mystery", -1) in summary["genre_score"]
    state = run(server._state())
    assert server._why_excluded(NEW, state) == "disliked"


def test_merge_and_rank_prefers_multi_seed():
    state = {"owned": {OWNED}, "radarr": set(), "suggested": set(), "disliked": set()}
    a = {"id": 1, "vote_count": 10, "vote_average": 6}
    b = {"id": 2, "vote_count": 400, "vote_average": 8}
    rows = [(a, "actor X", "Seed 1"), (a, "same language and genre", "Seed 2"), (b, "director Y", "Seed 1"),
            ({"id": OWNED}, "x", "Seed 1")]
    merged = server._merge(rows, state)
    assert set(merged) == {1, 2}
    assert merged[1]["seeds"] == ["Seed 1", "Seed 2"]
    assert max(merged.values(), key=server._rank)["raw"]["id"] == 1


def test_windows_and_released():
    assert server._windows("ml:120, en:60,bad,hi:x") == {"ml": 120, "en": 60}
    assert released({"release_date": "2020-01-01"}) and not released({"release_date": ""})
