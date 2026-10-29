# kagent-telegram-bot

Telegram front end and Alertmanager webhook for a kagent agent, talking to it over A2A.

| Env | Meaning |
| --- | --- |
| `TELEGRAM_BOT_TOKEN` | BotFather token (from a Secret) |
| `TELEGRAM_CHAT_ID` | Comma-separated allowed chat ids; the first one receives alert posts |
| `KAGENT_A2A_URL` | e.g. `http://kagent-controller.kagent.svc.cluster.local:8083/api/a2a/kagent/homelab-sre/` |
| `ALERT_DEDUP_SECONDS` | One investigation per Alertmanager group key per window (default 3600). A repeat notification whose alerts all started before the window is never investigated, even after a restart |
| `A2A_TIMEOUT_SECONDS` | Agent call timeout (default 600) |
| `MAX_CONCURRENT_INVESTIGATIONS` | Default 2 |
| `LISTEN_PORT` | HTTP port for `/alertmanager`, `/flux`, `/task` and `/healthz` (default 8080) |
| `FLUX_VERIFY_DELAY_SECONDS` | Wait before verifying a Flux change (default 120) |
| `FLUX_DEDUP_SECONDS` | One Flux verification per namespace per window (default 600) |
| `FLUX_POST_OK` | Also post verdicts that start with `OK` (default `false`: only `PROBLEM` verdicts and failed verifications are posted; OK is logged) |
| `GITHUB_APP_ID`, `GITHUB_APP_INSTALLATION_ID`, `GITHUB_APP_PRIVATE_KEY` | Enable the merge policy (same Secret as the GitHub MCP server) |
| `GITHUB_REPOS` | Comma-separated `owner/repo` list the merge policy watches |
| `AUTOMERGE_UPDATE_TYPES` | Renovate update types the bot may merge on its own (default `patch,digest,pin,pinDigest`) |
| `AUTOMERGE_BLOCK_LABELS` | Labels that always require the operator (default `critical-infra`) |
| `MERGE_POLICY_INTERVAL_SECONDS` | Policy pass interval (default 1800); a pass also runs after every gitops `/task` and on `/prs` |

Alertmanager receiver: `webhook_configs: [{url: http://<service>:8080/alertmanager, send_resolved: true}]`.
Flux events: route `alertname =~ "^Flux"` (from the notification-controller `alertmanager` Provider) to `webhook_configs: [{url: http://<service>:8080/flux, send_resolved: false}]`; the bot asks the agent for a short health verdict per namespace.
Scheduled tasks: `POST /task` with `{"agent": "<name in the same kagent namespace>", "prompt": "...", "title": "..."}`; the answer is posted to the alert chat. The `/task` endpoint takes `KAGENT_A2A_URL` with the agent segment replaced, so every agent in the namespace is reachable.
Unauthorized chats get a reply containing their chat id and nothing else.

Merge policy: a Renovate PR is merged by the bot only when every update type in its body table is in `AUTOMERGE_UPDATE_TYPES`, it carries none of `AUTOMERGE_BLOCK_LABELS`, at least one check run exists on its head and all are green, `mergeable_state` is `clean`, and a comment from the bot's own GitHub App containing `<!-- kagent-renovate-review -->` says `Verdict: merge`. Every other reviewed Renovate PR and every `kagent/*` PR whose checks are green or absent (docs-only) is posted once per head SHA with Merge and Skip buttons; the decision is recorded as a PR comment (`<!-- kagent-merge-asked sha=… -->`, `<!-- kagent-merge-skipped sha=… -->`), so a restart never asks twice. The merge happens on the button press from an allow-listed chat, after re-checking the PR. `/prs` runs a pass on demand.
