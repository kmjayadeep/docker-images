"""Name normalisation and fuzzy matching for artists and track titles.

The library was filled by hand, so the same person shows up as "A. R. Rahman", "A.R. Rahman" and
"A. R. Rahman & Javed Akhtar", and sources disagree on transliteration ("Gopi Sunder" / "Gopi Sundar",
"Ilaiyaraaja" / "Ilayaraja"). Everything that compares names goes through this module.
"""

from __future__ import annotations

import re
import unicodedata
from difflib import SequenceMatcher

# Separators between collaborating artists. Order matters: longer tokens first.
_SPLIT_RE = re.compile(
    r"\s*(?:,|;|/|\s&\s|\s\+\s|\sand\s|\sx\s|\sfeat\.?\s|\sft\.?\s|\sfeaturing\s)\s*",
    re.IGNORECASE,
)
# Bracketed suffixes on track titles: (From "Movie"), [Remastered], (Female Version), ...
_BRACKETS_RE = re.compile(r"\s*[\(\[].*?[\)\]]")
# " - From ...", " - Remastered 2011", " - Title Track"
_DASH_SUFFIX_RE = re.compile(r"\s+[-–—]\s+.*$")
_LEADING_TRACKNO_RE = re.compile(r"^\s*\d{1,3}\s*[\.\-]\s*")
_NON_WORD_RE = re.compile(r"[^\w\s]")
_SPACE_RE = re.compile(r"\s+")

ARTIST_MATCH_THRESHOLD = 0.88
TITLE_MATCH_THRESHOLD = 0.9


def fold(text: str) -> str:
    """Lowercase, strip accents and punctuation, collapse spaces. "A.R. Rahman" -> "a r rahman"."""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("‐", "-").lower()
    text = _NON_WORD_RE.sub(" ", text)
    return _SPACE_RE.sub(" ", text).strip()


def split_artists(text: str | None) -> list[str]:
    """Split a credit string into individual artists, keeping the original spelling."""
    if not text:
        return []
    parts = [p.strip() for p in _SPLIT_RE.split(text) if p and p.strip()]
    seen: set[str] = set()
    out: list[str] = []
    for part in parts:
        key = artist_key(part)
        if key and key not in seen:
            seen.add(key)
            out.append(part)
    return out


def artist_key(name: str) -> str:
    """Comparison key for one artist. Spaces are removed so "a r rahman" == "ar rahman"."""
    return fold(name).replace(" ", "")


def title_key(title: str) -> str:
    """Comparison key for a track or album title, without version and film suffixes."""
    t = _LEADING_TRACKNO_RE.sub("", title or "")
    t = _BRACKETS_RE.sub("", t)
    t = _DASH_SUFFIX_RE.sub("", t)
    return fold(t).replace(" ", "")


def similar(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


def same_artist(a: str, b: str) -> bool:
    return similar(artist_key(a), artist_key(b)) >= ARTIST_MATCH_THRESHOLD


def same_title(a: str, b: str) -> bool:
    return similar(title_key(a), title_key(b)) >= TITLE_MATCH_THRESHOLD


class ArtistIndex:
    """Canonical artist names with fuzzy lookup. The first spelling seen becomes canonical."""

    def __init__(self) -> None:
        self._keys: dict[str, str] = {}  # key -> canonical display name

    def add(self, name: str) -> str:
        found = self.lookup(name)
        if found:
            return found
        self._keys[artist_key(name)] = name
        return name

    def lookup(self, name: str) -> str | None:
        key = artist_key(name)
        if not key:
            return None
        if key in self._keys:
            return self._keys[key]
        best, best_score = None, 0.0
        for k, display in self._keys.items():
            score = similar(key, k)
            if score > best_score:
                best, best_score = display, score
        return best if best_score >= ARTIST_MATCH_THRESHOLD else None

    def __contains__(self, name: str) -> bool:
        return self.lookup(name) is not None

    def __len__(self) -> int:
        return len(self._keys)
