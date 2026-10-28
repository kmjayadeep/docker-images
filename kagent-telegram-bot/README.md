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
| `LISTEN_PORT` | HTTP port for `/alertmanager` and `/healthz` (default 8080) |

Alertmanager receiver: `webhook_configs: [{url: http://<service>:8080/alertmanager, send_resolved: true}]`.
Unauthorized chats get a reply containing their chat id and nothing else.
