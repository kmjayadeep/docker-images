"""Telegram front end and Alertmanager webhook for a kagent agent (A2A).

Two flows share this process:

1. Telegram long polling: text messages from allow-listed chats are forwarded to the
   agent's A2A endpoint (one agent session per chat) and the answer is sent back.
2. Alertmanager webhook (POST /alertmanager): firing alerts produce an immediate short
   notification, then one agent investigation per alert group per ALERT_DEDUP_SECONDS,
   whose result is posted to the alert chat. Resolved alerts post a one-liner only.
   Repeat notifications of a group whose alerts all started more than ALERT_DEDUP_SECONDS
   ago are never investigated again, so the dedupe does not depend on process memory and
   a restart cannot re-trigger an investigation of a long-firing alert.
3. Flux change webhook (POST /flux): Alertmanager delivers Flux notification-controller
   events (alertname Flux*) here. After FLUX_VERIFY_DELAY_SECONDS the agent is asked for a
   short verification of the changed namespace; one check per namespace per
   FLUX_DEDUP_SECONDS. Nothing is posted for the alert itself or for resolved groups.
4. Scheduled tasks (POST /task, JSON {"agent", "prompt", "title"}): a CronJob asks any agent
   in the same kagent namespace a question and the answer is posted to the alert chat.

Configuration is by environment variables; the bot token is never logged.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone

from aiohttp import ClientSession, ClientTimeout, web

log = logging.getLogger("kagent-telegram-bot")

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
# Comma-separated chat ids allowed to talk to the agent. The first one receives alert posts.
ALLOWED_CHAT_IDS = [c.strip() for c in os.environ.get("TELEGRAM_CHAT_ID", "").split(",") if c.strip()]
ALERT_CHAT_ID = ALLOWED_CHAT_IDS[0] if ALLOWED_CHAT_IDS else None
A2A_URL = os.environ["KAGENT_A2A_URL"].rstrip("/") + "/"
# .../api/a2a/<namespace>/<agent>/ -> .../api/a2a/<namespace>/ so /task can address sibling agents.
A2A_BASE_URL = A2A_URL.rsplit("/", 2)[0] + "/"
AGENT_NAME_RE = re.compile(r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")
FLUX_VERIFY_DELAY_SECONDS = int(os.environ.get("FLUX_VERIFY_DELAY_SECONDS", "120"))
FLUX_DEDUP_SECONDS = int(os.environ.get("FLUX_DEDUP_SECONDS", "600"))
A2A_TIMEOUT = float(os.environ.get("A2A_TIMEOUT_SECONDS", "600"))
ALERT_DEDUP_SECONDS = int(os.environ.get("ALERT_DEDUP_SECONDS", "3600"))
MAX_CONCURRENT_INVESTIGATIONS = int(os.environ.get("MAX_CONCURRENT_INVESTIGATIONS", "2"))
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "8080"))
AGENT_NAME = os.environ.get("AGENT_DISPLAY_NAME", "homelab-sre")

TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
TG_MAX_LEN = 4000
SECRET_PATTERN = re.compile(r"(sk-[A-Za-z0-9_-]{8,}|glsa_[A-Za-z0-9_-]{8,}|Bearer\s+[A-Za-z0-9._-]{8,})")


def redact(text: str) -> str:
    return SECRET_PATTERN.sub("<redacted>", text)


def chunks(text: str, size: int = TG_MAX_LEN):
    for i in range(0, len(text), size):
        yield text[i : i + size]


def parse_rfc3339(value: str | None) -> float | None:
    """Alertmanager timestamps ("2026-10-28T07:48:21.526100123Z") as epoch seconds; None if unparseable."""
    if not value:
        return None
    try:
        m = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)?(Z|[+-]\d{2}:\d{2})$", value.strip())
        if not m:
            return None
        frac = (m.group(2) or "")[:7]  # fromisoformat accepts at most 6 fractional digits
        tz = "+00:00" if m.group(3) == "Z" else m.group(3)
        return datetime.fromisoformat(f"{m.group(1)}{frac}{tz}").timestamp()
    except ValueError:
        return None


class Bot:
    def __init__(self) -> None:
        self.http = ClientSession(timeout=ClientTimeout(total=A2A_TIMEOUT + 30))
        self.sessions: dict[str, str] = {}  # chat id -> A2A contextId
        self.last_investigation: dict[str, float] = {}  # alert group key -> epoch
        self.last_flux_check: dict[str, float] = {}  # namespace -> epoch
        self.sem = asyncio.Semaphore(MAX_CONCURRENT_INVESTIGATIONS)
        self.ready = False

    # ---------- Telegram ----------
    async def tg(self, method: str, **params):
        async with self.http.post(f"{TG_API}/{method}", json=params, timeout=ClientTimeout(total=70)) as r:
            data = await r.json()
            if not data.get("ok"):
                # Never include the URL (it carries the token) in logs.
                log.warning("telegram %s failed: %s", method, data.get("description"))
            return data

    async def send(self, chat_id: str, text: str) -> None:
        text = redact(text) or "(empty response)"
        for part in chunks(text):
            await self.tg("sendMessage", chat_id=chat_id, text=part, disable_web_page_preview=True)

    async def poll_telegram(self) -> None:
        offset = None
        # Drop anything that queued up while we were down.
        await self.tg("deleteWebhook", drop_pending_updates=True)
        self.ready = True
        while True:
            try:
                params = {"timeout": 50, "allowed_updates": ["message"]}
                if offset is not None:
                    params["offset"] = offset
                data = await self.tg("getUpdates", **params)
                for upd in data.get("result", []):
                    offset = upd["update_id"] + 1
                    msg = upd.get("message") or {}
                    if msg.get("text"):
                        asyncio.create_task(self.handle_message(msg))
            except asyncio.CancelledError:
                raise
            except Exception as e:  # network hiccups: back off and keep polling
                log.warning("getUpdates error: %s", type(e).__name__)
                await asyncio.sleep(5)

    async def handle_message(self, msg: dict) -> None:
        chat_id = str(msg["chat"]["id"])
        text = msg["text"].strip()
        if chat_id not in ALLOWED_CHAT_IDS:
            log.warning("unauthorized chat id %s (user %s)", chat_id, (msg.get("from") or {}).get("username"))
            await self.send(chat_id, f"Not authorized. This chat id is {chat_id}.")
            return
        if text.startswith("/start"):
            await self.send(
                chat_id,
                f"Connected to the {AGENT_NAME} agent. Ask about the cluster in plain language.\n"
                "/new starts a fresh session. Alerts are posted here automatically.",
            )
            return
        if text.startswith("/new"):
            self.sessions.pop(chat_id, None)
            await self.send(chat_id, "New session started.")
            return
        if text.startswith("/"):
            await self.send(chat_id, "Unknown command. Use /new or just ask a question.")
            return
        context_id = self.sessions.setdefault(chat_id, str(uuid.uuid4()))
        await self.tg("sendChatAction", chat_id=chat_id, action="typing")
        try:
            answer = await self.ask_agent(text, context_id)
        except Exception as e:
            log.exception("agent call failed for chat %s", chat_id)
            answer = f"The agent call failed: {type(e).__name__}: {e}"
        await self.send(chat_id, answer)

    # ---------- A2A ----------
    async def ask_agent(self, text: str, context_id: str, agent: str | None = None) -> str:
        url = f"{A2A_BASE_URL}{agent}/" if agent else A2A_URL
        payload = {
            "jsonrpc": "2.0",
            "id": str(uuid.uuid4()),
            "method": "message/send",
            "params": {
                "message": {
                    "role": "user",
                    "kind": "message",
                    "messageId": str(uuid.uuid4()),
                    "contextId": context_id,
                    "parts": [{"kind": "text", "text": text}],
                }
            },
        }
        async with self.http.post(url, json=payload, timeout=ClientTimeout(total=A2A_TIMEOUT)) as r:
            data = await r.json()
        if "error" in data:
            return f"Agent error: {json.dumps(data['error'])[:500]}"
        result = data.get("result", {})
        texts: list[str] = []
        for artifact in result.get("artifacts") or []:
            texts += [p.get("text", "") for p in artifact.get("parts", []) if p.get("kind") == "text"]
        if not texts:
            msg = (result.get("status") or {}).get("message") or {}
            texts = [p.get("text", "") for p in msg.get("parts", []) if p.get("kind") == "text"]
        state = (result.get("status") or {}).get("state")
        body = "\n".join(t for t in texts if t) or "The agent returned no text."
        return body if state in (None, "completed") else f"[task {state}]\n{body}"

    # ---------- Alertmanager ----------
    async def alertmanager(self, request: web.Request) -> web.Response:
        if not ALERT_CHAT_ID:
            return web.json_response({"error": "TELEGRAM_CHAT_ID not configured"}, status=503)
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "invalid json"}, status=400)
        status = payload.get("status", "firing")
        alerts = payload.get("alerts") or []
        group_key = payload.get("groupKey") or "nogroup"
        labels = payload.get("commonLabels") or (alerts[0].get("labels") if alerts else {}) or {}
        name = labels.get("alertname", "alert")
        severity = labels.get("severity", "")
        ns = labels.get("namespace", "")
        where = " ".join(x for x in (ns, labels.get("pod") or labels.get("node") or labels.get("instance") or "") if x)
        summary = ((payload.get("commonAnnotations") or {}).get("summary")
                   or (alerts[0].get("annotations", {}).get("summary") if alerts else "")
                   or (payload.get("commonAnnotations") or {}).get("description") or "")
        icon = "🔥" if status == "firing" else "✅"
        head = f"{icon} {status.upper()}: {name}" + (f" [{severity}]" if severity else "") + (f" ({where})" if where else "")
        if len(alerts) > 1:
            head += f" x{len(alerts)}"
        text = head + (f"\n{summary}" if summary else "")
        log.info("alertmanager %s %s group=%s alerts=%d", status, name, group_key, len(alerts))
        await self.send(ALERT_CHAT_ID, text)
        if status != "firing":
            return web.json_response({"ok": True, "investigated": False})
        now = time.time()
        # Alertmanager re-sends a firing group every repeat_interval. Only a group with an alert that started
        # inside the dedupe window is new; older repeats were investigated already (possibly before a restart).
        starts = [t for t in (parse_rfc3339(a.get("startsAt")) for a in alerts) if t is not None]
        if starts and now - max(starts) >= ALERT_DEDUP_SECONDS:
            log.info("skipping investigation of %s: newest alert started %.0fs ago", name, now - max(starts))
            return web.json_response({"ok": True, "investigated": False, "reason": "repeat of long-firing alert"})
        last = self.last_investigation.get(group_key, 0)
        if now - last < ALERT_DEDUP_SECONDS:
            return web.json_response({"ok": True, "investigated": False, "reason": "deduplicated"})
        self.last_investigation[group_key] = now
        asyncio.create_task(self.investigate(name, alerts, labels))
        return web.json_response({"ok": True, "investigated": True})

    async def investigate(self, name: str, alerts: list[dict], labels: dict) -> None:
        async with self.sem:
            lines = []
            for a in alerts[:5]:
                lines.append(json.dumps({"labels": a.get("labels", {}), "annotations": a.get("annotations", {}),
                                         "startsAt": a.get("startsAt")}, ensure_ascii=False))
            prompt = (
                f"Alertmanager fired '{name}' at {datetime.now(timezone.utc).isoformat(timespec='seconds')}. "
                "Investigate it with your method and keep the answer under 2500 characters: Summary, Evidence, "
                "Likely cause, Recommended change (Git path and field), Verification. Alerts:\n" + "\n".join(lines)
            )
            try:
                answer = await self.ask_agent(prompt, str(uuid.uuid4()))
            except Exception as e:
                log.exception("investigation failed for %s", name)
                answer = f"Investigation failed: {type(e).__name__}: {e}"
            await self.send(ALERT_CHAT_ID, f"🔎 {AGENT_NAME} on {name}:\n{answer}")

    # ---------- Flux change verification ----------
    async def flux(self, request: web.Request) -> web.Response:
        if not ALERT_CHAT_ID:
            return web.json_response({"error": "TELEGRAM_CHAT_ID not configured"}, status=503)
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "invalid json"}, status=400)
        if payload.get("status", "firing") != "firing":
            return web.json_response({"ok": True, "checked": False})
        alerts = payload.get("alerts") or []
        changes: dict[str, list[str]] = {}
        for a in alerts:
            labels = a.get("labels", {})
            ns = labels.get("namespace") or "unknown"
            summary = (a.get("annotations", {}).get("message") or a.get("annotations", {}).get("summary") or "")
            revision = labels.get("revision") or labels.get("kustomize.toolkit.fluxcd.io/revision") or ""
            line = f'{labels.get("kind", "?")}/{ns}/{labels.get("name", "?")} {labels.get("reason", "")}'
            if revision:
                line += f" revision={revision}"
            if summary:
                line += f": {summary[:300]}"
            changes.setdefault(ns, []).append(line)
        log.info("flux webhook: %d alerts in namespaces %s", len(alerts), sorted(changes))
        now = time.time()
        started = []
        for ns, lines in changes.items():
            if now - self.last_flux_check.get(ns, 0) < FLUX_DEDUP_SECONDS:
                continue
            self.last_flux_check[ns] = now
            started.append(ns)
            asyncio.create_task(self.verify_flux_change(ns, lines))
        return web.json_response({"ok": True, "checked": bool(started), "namespaces": started})

    async def verify_flux_change(self, ns: str, lines: list[str]) -> None:
        await asyncio.sleep(FLUX_VERIFY_DELAY_SECONDS)
        async with self.sem:
            prompt = (
                f"Flux applied a change in namespace '{ns}' about {FLUX_VERIFY_DELAY_SECONDS}s ago. Events:\n"
                + "\n".join(lines[:8])
                + "\n\nVerify that the affected workloads are healthy now: Flux objects Ready, pods Ready with no "
                "restarts since the change, no new firing alerts. Answer in at most 500 characters, plain text, no "
                "headers: start with OK or PROBLEM, then one line per finding. If everything is fine, one sentence "
                "is enough. Do not open a pull request."
            )
            try:
                answer = await self.ask_agent(prompt, str(uuid.uuid4()))
            except Exception as e:
                log.exception("flux verification failed for %s", ns)
                answer = f"Verification failed: {type(e).__name__}: {e}"
            await self.send(ALERT_CHAT_ID, f"🔁 Flux change in {ns}:\n{answer}")

    # ---------- Scheduled tasks ----------
    async def task(self, request: web.Request) -> web.Response:
        if not ALERT_CHAT_ID:
            return web.json_response({"error": "TELEGRAM_CHAT_ID not configured"}, status=503)
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "invalid json"}, status=400)
        prompt = str(payload.get("prompt") or "").strip()
        agent = str(payload.get("agent") or "").strip() or None
        title = str(payload.get("title") or "task").strip()[:80]
        if not prompt:
            return web.json_response({"error": "prompt required"}, status=400)
        if agent and not AGENT_NAME_RE.match(agent):
            return web.json_response({"error": "invalid agent name"}, status=400)
        log.info("task '%s' for agent %s", title, agent or AGENT_NAME)
        async with self.sem:
            try:
                answer = await self.ask_agent(prompt, str(uuid.uuid4()), agent=agent)
            except Exception as e:
                log.exception("task '%s' failed", title)
                answer = f"Task failed: {type(e).__name__}: {e}"
        await self.send(ALERT_CHAT_ID, f"🗓 {title}:\n{answer}")
        return web.json_response({"ok": True, "chars": len(answer)})

    async def healthz(self, request: web.Request) -> web.Response:
        return web.Response(text="ok" if self.ready else "starting", status=200 if self.ready else 503)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    bot = Bot()
    app = web.Application()
    app.router.add_post("/alertmanager", bot.alertmanager)
    app.router.add_post("/flux", bot.flux)
    app.router.add_post("/task", bot.task)
    app.router.add_get("/healthz", bot.healthz)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", LISTEN_PORT).start()
    log.info("listening on :%d, agent=%s, allowed chats=%d, alert chat configured=%s",
             LISTEN_PORT, A2A_URL, len(ALLOWED_CHAT_IDS), bool(ALERT_CHAT_ID))
    try:
        await bot.poll_telegram()
    finally:
        await runner.cleanup()
        await bot.http.close()


if __name__ == "__main__":
    asyncio.run(main())
