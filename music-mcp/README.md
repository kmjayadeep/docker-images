# music-mcp

MCP server (streamable HTTP on `:8000/mcp`) for the kagent `music-curator` agent in homelab-k8s
(`clusters/titania/apps/kagent/`, plan 0026). It reads the Jellyfin music library, finds candidates on
YouTube Music (`ytmusicapi`, unauthenticated) and Last.fm, drops anything owned, already suggested or by a
disliked artist, and keeps digests and feedback in SQLite.

Read-only towards Jellyfin (GET only). The API keys stay in this process; tools never return them.

## Tools

`library_profile`, `recent_additions`, `favourites`, `in_library`, `similar_artists`, `related_tracks`,
`artist_top_tracks`, `new_releases`, `search`, `suggestion_history`, `feedback_summary`, `record_digest`.

## HTTP routes (for the Telegram bot, not MCP)

| Route | Purpose |
| --- | --- |
| `GET /healthz` | Probe |
| `GET /digest[?id=N]` | Latest (or given) digest with item ids, for rendering feedback buttons |
| `POST /feedback` `{id, action}` | `action` is `like`, `dislike` or `want` |
| `GET /wants` | The "want" queue for a later download pipeline |

These routes are unauthenticated; restrict callers with a NetworkPolicy.

## Configuration

| Variable | Default | |
| --- | --- | --- |
| `JELLYFIN_URL` | required | |
| `JELLYFIN_API_KEY` | required | |
| `JELLYFIN_USER` | unset | Jellyfin user whose favourites count |
| `LASTFM_API_KEY` | unset | Last.fm sources are skipped without it |
| `YTM_LOCATION` | `IN` | YouTube Music search region |
| `DB_PATH` | `/data/music.db` | |
| `ALLOWED_HOSTS` | `localhost,127.0.0.1` | Host headers accepted on `/mcp` (DNS-rebinding protection) |

## Development

```bash
uv run --with-requirements requirements.txt --with pytest python -m pytest -q tests
```
