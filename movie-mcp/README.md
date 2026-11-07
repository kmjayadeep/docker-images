# movie-mcp

MCP server for the kagent `movie-curator` agent (homelab-k8s plan 0027). It finds movies for one viewer and keeps
digests and feedback in SQLite.

- **Jellyfin** (GET only): the movie library and watch state.
- **Radarr** (GET only, optional): movies already added; these are never suggested.
- **TMDB** API v3: the catalogue.
  - English seeds use TMDB recommendations.
  - Malayalam and Hindi seeds use same-language films with the same genres, plus the director's and lead actors' films. TMDB recommendations return only English films for them.

Movies are identified by TMDB id everywhere, so ownership checks are exact.

## Tools

`taste_profile`, `digest_candidates` (the one-call weekly fan-out), `similar`, `discover`, `search`,
`movie_details`, `suggestion_history`, `feedback_summary`, `record_digest` (takes `{tmdb_id, reason}` items; titles
and links come from TMDB).

## HTTP routes for the Telegram bot

`GET /healthz`, `GET /digest[?id=N]`, `POST /feedback {id, action: like|dislike|want}`, `GET /wants`.
Digest items carry `kind: movie` and `search_url` (the TMDB page) for the bot's title button.

## Environment

See the module docstring in `movie_mcp/server.py`. Keys come from the environment only. The `httpx` logger is
kept at WARNING because the TMDB key is a query parameter.

## Tests

```sh
pip install -r requirements.txt pytest && pytest tests
```
