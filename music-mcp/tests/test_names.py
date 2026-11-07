from music_mcp.names import ArtistIndex, artist_key, same_artist, same_title, split_artists, title_key


def test_split_collaborations():
    assert split_artists("A. R. Rahman & Javed Akhtar") == ["A. R. Rahman", "Javed Akhtar"]
    assert split_artists("A. R. Rahman, Gulzar") == ["A. R. Rahman", "Gulzar"]
    assert split_artists("Raveendran; P. Jayachandran; Vayalar Sarathchandra Varma") == [
        "Raveendran", "P. Jayachandran", "Vayalar Sarathchandra Varma"]
    assert split_artists("Ed Sheeran feat. Arijit Singh") == ["Ed Sheeran", "Arijit Singh"]
    assert split_artists("Ankit Tiwari, Mithoon & Rabbi") == ["Ankit Tiwari", "Mithoon", "Rabbi"]
    assert split_artists("Brodha V") == ["Brodha V"]
    assert split_artists("") == []


def test_split_dedupes_spelling_variants():
    assert split_artists("A.R. Rahman, A. R. Rahman") == ["A.R. Rahman"]


def test_artist_variants_match():
    assert artist_key("A. R. Rahman") == artist_key("A.R. Rahman") == artist_key("a r rahman")
    assert same_artist("Gopi Sunder", "Gopi Sundar")
    assert same_artist("Ilaiyaraaja", "Ilayaraja")
    assert not same_artist("Shaan Rahman", "A. R. Rahman")
    assert not same_artist("Bijibal", "Gopi Sundar")


def test_title_suffixes_ignored():
    assert title_key('Onakka Munthiri (From "Hridayam")') == title_key("Onakka Munthiri")
    assert title_key("1. Aalilathaaliyumaay") == title_key("Aalilathaaliyumaay")
    assert title_key("Dhurandhar - Title Track") == title_key("Dhurandhar")
    assert same_title("Rathinamo (Female Version)", "Rathinamo")
    assert not same_title("Aaro Nenjil", "Aalolam")


def test_artist_index_canonicalises():
    idx = ArtistIndex()
    assert idx.add("A. R. Rahman") == "A. R. Rahman"
    assert idx.add("A.R. Rahman") == "A. R. Rahman"
    assert idx.add("Gopi Sunder") == "Gopi Sunder"
    assert idx.lookup("Gopi Sundar") == "Gopi Sunder"
    assert "Sushin Shyam" not in idx
    assert len(idx) == 2
