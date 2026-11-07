import asyncio

import pytest

from music_mcp import server
from music_mcp.jellyfin import Album, Library, Track
from music_mcp.store import Store


def make_library() -> Library:
    lib = Library(fetched_at=1)
    rows = [
        ("Aaro Nenjil", ["Gowry Lekshmi"], "Godha", "Shaan Rahman"),
        ("Kun Faya Kun", ["A. R. Rahman", "Javed Ali", "Mohit Chauhan"], "Rockstar", "A.R. Rahman"),
        ("Aigiri Nandini", ["Brodha V"], "Aigiri Nandini", "Brodha V"),
        ("Jimikki Kammal", ["Vineeth Sreenivasan", "Renjith Unni"], "Velipadinte Pusthakam", "Shaan Rahman"),
    ]
    for title, artists, album, album_artist in rows:
        canon = [lib.artists.add(a) for a in artists]
        lib.artists.add(album_artist)
        for a in canon:
            lib.artist_tracks[a] += 1
        lib.album_artist_tracks[lib.artists.add(album_artist)] += 1
        lib.tracks.append(Track(title, canon, album, album_artist, 2017, [], "2026-10-01"))
    lib.albums.append(Album("1", "Godha", ["Shaan Rahman"], 2017, [], "2026-10-01"))
    return lib


class FakeJellyfin:
    def __init__(self, lib):
        self.lib = lib

    async def library(self, refresh=False):
        return self.lib


@pytest.fixture
def env(tmp_path, monkeypatch):
    st = Store(str(tmp_path / "music.db"))
    monkeypatch.setattr(server, "_store", st)
    monkeypatch.setattr(server, "_jf", FakeJellyfin(make_library()))
    return st


def run(coro):
    return asyncio.run(coro)


def test_owned_track_matches_across_spellings():
    lib = make_library()
    assert lib.owns_track("A.R. Rahman", 'Kun Faya Kun (From "Rockstar")')
    assert lib.owns_track("Shaan Rahman, Gowry Lekshmi", "Aaro Nenjil")
    assert not lib.owns_track("Sushin Shyam", "Armadham")
    assert lib.owns_artist("Gopi Sundar") is None
    assert lib.owns_artist("a r rahman") == "A. R. Rahman"


def test_filter_drops_owned_and_merges_sources(env):
    cands = [
        {"kind": "track", "artist": "Shaan Rahman, Gowry Lekshmi", "title": "Aaro Nenjil", "source": "ytmusic-radio", "score": 1},
        {"kind": "track", "artist": "Sushin Shyam, Pranavam Sasi", "title": "Armadham", "source": "ytmusic-radio", "score": 0.9,
         "url": "https://music.youtube.com/watch?v=x"},
        {"kind": "track", "artist": "Sushin Shyam", "title": "Armadham", "source": "lastfm", "score": 0.5, "url": "https://last.fm/x"},
        {"kind": "track", "artist": "Hesham Abdul Wahab", "title": "Onakka Munthiri", "source": "lastfm", "score": 0.95},
    ]
    found, dropped = run(server._filter(cands))
    assert dropped == {"owned": 1}
    assert [f["title"] for f in found] == ["Armadham", "Onakka Munthiri"]
    assert found[0]["sources"] == ["lastfm", "ytmusic-radio"]
    assert found[0]["url"].startswith("https://music.youtube.com")
    assert found[0]["new_artist"] is True


def test_filter_artists_drops_owned(env):
    cands = [
        {"kind": "artist", "artist": "A.R. Rahman", "source": "lastfm"},
        {"kind": "artist", "artist": "Sushin Shyam", "source": "ytmusic"},
    ]
    found, dropped = run(server._filter(cands))
    assert [f["artist"] for f in found] == ["Sushin Shyam"]
    assert dropped == {"owned": 1}


def test_record_digest_rejects_owned_and_repeats(env, monkeypatch):
    async def no_search(*a, **k):
        return []

    class FakeYt:
        search = staticmethod(no_search)

    monkeypatch.setattr(server, "_ytm", FakeYt())
    items = [
        {"kind": "track", "artist": "Sushin Shyam", "title": "Armadham", "reason": "because you added Godha",
         "url": "https://music.youtube.com/watch?v=x"},
        {"kind": "track", "artist": "A.R. Rahman", "title": "Kun Faya Kun", "reason": "owned"},
        {"kind": "album", "artist": "Hesham Abdul Wahab", "title": "Hridayam", "reason": "film soundtrack"},
    ]
    res = run(server.record_digest(items))
    assert res["digest_id"] == 1
    assert [s["title"] for s in res["stored"]] == ["Armadham", "Hridayam"]
    assert res["rejected"][0]["why"] == "owned"

    again = run(server.record_digest([{"kind": "track", "artist": "Sushin Shyam, Pranavam Sasi",
                                       "title": 'Armadham (From "Aavesham")', "reason": "x"}]))
    assert again["digest_id"] is None
    assert again["rejected"][0]["why"] == "suggested before"


def test_feedback_dislike_filters_artist(env):
    _, stored = env.record_digest([{"kind": "track", "artist": "Anirudh Ravichander", "title": "Monica", "reason": "r"}])
    assert env.set_feedback(stored[0]["id"], "dislike")["feedback"] == "dislike"
    found, dropped = run(server._filter([
        {"kind": "track", "artist": "Anirudh Ravichander, Sublahshini", "title": "Another Song", "source": "ytmusic"},
    ]))
    assert found == [] and dropped == {"disliked_artist": 1}
    summary = env.feedback_summary()
    assert summary["disliked_artists"] == [("Anirudh Ravichander", 1)]
    with pytest.raises(ValueError):
        env.set_feedback(stored[0]["id"], "love")


def test_digest_and_wants(env):
    did, stored = env.record_digest([{"kind": "album", "artist": "Justin Varghese", "title": "Thallumaala", "reason": "r"}])
    env.set_feedback(stored[0]["id"], "want")
    d = env.digest()
    assert d["id"] == did and d["items"][0]["feedback"] == "want"
    assert env.wants()[0]["title"] == "Thallumaala"


def test_profile_ranks_composers(env):
    p = run(server.library_profile())
    assert p["top_album_artists"][0] == {"artist": "Shaan Rahman", "tracks": 2}
