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

Alertmanager receiver: `webhook_configs: [{url: http://<service>:8080/alertmanager, send_resolved: true}]`.
Flux events: route `alertname =~ "^Flux"` (from the notification-controller `alertmanager` Provider) to `webhook_configs: [{url: http://<service>:8080/flux, send_resolved: false}]`; the bot asks the agent for a short health verdict per namespace.
Scheduled tasks: `POST /task` with `{"agent": "<name in the same kagent namespace>", "prompt": "...", "title": "..."}`; the answer is posted to the alert chat. The `/task` endpoint takes `KAGENT_A2A_URL` with the agent segment replaced, so every agent in the namespace is reachable.
Unauthorized chats get a reply containing their chat id and nothing else.
