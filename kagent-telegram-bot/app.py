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
5. Merge policy (optional, needs GITHUB_APP_* env): a deterministic pass over open pull requests
   of GITHUB_REPOS, every MERGE_POLICY_INTERVAL_SECONDS and after a gitops task. Renovate PRs
   whose update types are all in AUTOMERGE_UPDATE_TYPES, that carry no AUTOMERGE_BLOCK_LABELS,
   whose checks are green and whose review comment from our own GitHub App says "Verdict: merge"
   are merged by the bot itself. Every other reviewed Renovate PR and every kagent/* PR with
   green checks is offered to the operator in Telegram with Merge and Skip buttons; the bot
   merges only on the button press from an allow-listed chat. No model is on this path.

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
# Merge policy (GitHub App credentials are the same Secret the GitHub MCP server uses).
GITHUB_APP_ID = os.environ.get("GITHUB_APP_ID", "")
GITHUB_APP_INSTALLATION_ID = os.environ.get("GITHUB_APP_INSTALLATION_ID", "")
GITHUB_APP_PRIVATE_KEY = os.environ.get("GITHUB_APP_PRIVATE_KEY", "")
GITHUB_REPOS = [r.strip() for r in os.environ.get("GITHUB_REPOS", "").split(",") if r.strip()]
AUTOMERGE_UPDATE_TYPES = {t.strip() for t in os.environ.get("AUTOMERGE_UPDATE_TYPES", "patch,digest,pin,pinDigest").split(",") if t.strip()}
AUTOMERGE_BLOCK_LABELS = {t.strip() for t in os.environ.get("AUTOMERGE_BLOCK_LABELS", "critical-infra").split(",") if t.strip()}
MERGE_POLICY_INTERVAL_SECONDS = int(os.environ.get("MERGE_POLICY_INTERVAL_SECONDS", "1800"))
MERGE_POLICY_ENABLED = bool(GITHUB_APP_ID and GITHUB_APP_INSTALLATION_ID and GITHUB_APP_PRIVATE_KEY and GITHUB_REPOS)
RENOVATE_LOGIN = os.environ.get("RENOVATE_LOGIN", "renovate[bot]")
AGENT_BRANCH_PREFIX = os.environ.get("AGENT_BRANCH_PREFIX", "kagent/")
REVIEW_MARKER = "<!-- kagent-renovate-review -->"
ASKED_MARKER = "<!-- kagent-merge-asked sha={sha} -->"
SKIPPED_MARKER = "<!-- kagent-merge-skipped sha={sha} -->"
GH_API = "https://api.github.com"
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


def parse_renovate_update_types(body: str) -> set[str]:
    """Update types from the table Renovate puts in every PR body (column 'Update')."""
    types: set[str] = set()
    col = None
    for line in body.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            if col is not None and types:
                break
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if col is None:
            if "Update" in cells:
                col = cells.index("Update")
            continue
        if cells and set("".join(cells)) <= set("-: "):
            continue
        if len(cells) > col and cells[col]:
            types.add(cells[col].strip("`* "))
    return types


class GitHubApp:
    """Minimal GitHub App client: short-lived installation tokens, no long-lived secret in memory
    beyond the private key from the environment. Never log tokens or the key."""

    def __init__(self, http: ClientSession) -> None:
        import jwt  # PyJWT[crypto]

        self._jwt = jwt
        self.http = http
        self._token = ""
        self._token_exp = 0.0
        self.login = ""

    def _app_jwt(self) -> str:
        now = int(time.time())
        return self._jwt.encode({"iat": now - 60, "exp": now + 540, "iss": GITHUB_APP_ID}, GITHUB_APP_PRIVATE_KEY, algorithm="RS256")

    async def _request(self, method: str, url: str, token: str, **kw):
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                   "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "kagent-telegram-bot"}
        async with self.http.request(method, url, headers=headers, timeout=ClientTimeout(total=30), **kw) as r:
            data = await r.json(content_type=None) if r.content_length != 0 else {}
            if r.status >= 400:
                raise RuntimeError(f"GitHub {method} {url.replace(GH_API, '')} -> {r.status}: {str(data.get('message') if isinstance(data, dict) else data)[:200]}")
            return data

    async def token(self) -> str:
        if time.time() < self._token_exp - 300 and self._token:
            return self._token
        app_jwt = self._app_jwt()
        if not self.login:
            app = await self._request("GET", f"{GH_API}/app", app_jwt)
            self.login = f"{app.get('slug', '')}[bot]"
        data = await self._request("POST", f"{GH_API}/app/installations/{GITHUB_APP_INSTALLATION_ID}/access_tokens", app_jwt)
        self._token = data["token"]
        exp = parse_rfc3339(data.get("expires_at")) or (time.time() + 3000)
        self._token_exp = exp
        return self._token

    async def api(self, method: str, path: str, **kw):
        return await self._request(method, f"{GH_API}{path}", await self.token(), **kw)


class Bot:
    def __init__(self) -> None:
        self.http = ClientSession(timeout=ClientTimeout(total=A2A_TIMEOUT + 30))
        self.sessions: dict[str, str] = {}  # chat id -> A2A contextId
        self.last_investigation: dict[str, float] = {}  # alert group key -> epoch
        self.last_flux_check: dict[str, float] = {}  # namespace -> epoch
        self.sem = asyncio.Semaphore(MAX_CONCURRENT_INVESTIGATIONS)
        self.ready = False
        self.gh = GitHubApp(self.http) if MERGE_POLICY_ENABLED else None
        self.policy_lock = asyncio.Lock()

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
                params = {"timeout": 50, "allowed_updates": ["message", "callback_query"]}
                if offset is not None:
                    params["offset"] = offset
                data = await self.tg("getUpdates", **params)
                for upd in data.get("result", []):
                    offset = upd["update_id"] + 1
                    msg = upd.get("message") or {}
                    if msg.get("text"):
                        asyncio.create_task(self.handle_message(msg))
                    if upd.get("callback_query"):
                        asyncio.create_task(self.handle_callback(upd["callback_query"]))
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
        if text.startswith("/prs"):
            if not self.gh:
                await self.send(chat_id, "Merge policy is not configured.")
                return
            summary = await self.merge_policy_pass(reason="/prs")
            await self.send(chat_id, summary or "No open pull request needs a decision.")
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
        if self.gh and agent and agent != AGENT_NAME:
            asyncio.create_task(self.merge_policy_pass(reason=f"task {title}"))
        return web.json_response({"ok": True, "chars": len(answer)})

    # ---------- Merge policy ----------
    async def merge_policy_loop(self) -> None:
        await asyncio.sleep(60)
        while True:
            try:
                await self.merge_policy_pass(reason="timer")
            except Exception:
                log.exception("merge policy pass failed")
            await asyncio.sleep(MERGE_POLICY_INTERVAL_SECONDS)

    async def merge_policy_pass(self, reason: str) -> str:
        """Evaluate every open PR once; returns a short human summary."""
        if not self.gh:
            return ""
        async with self.policy_lock:
            lines: list[str] = []
            for repo in GITHUB_REPOS:
                try:
                    prs = await self.gh.api("GET", f"/repos/{repo}/pulls?state=open&per_page=50")
                except Exception as e:
                    log.warning("merge policy: cannot list PRs of %s: %s", repo, type(e).__name__)
                    continue
                for pr in prs:
                    try:
                        line = await self.evaluate_pr(repo, pr)
                    except Exception as e:
                        log.warning("merge policy: PR %s#%s: %s", repo, pr.get("number"), e)
                        line = None
                    if line:
                        lines.append(line)
            log.info("merge policy pass (%s): %d decisions", reason, len(lines))
            return "\n".join(lines)

    async def pr_facts(self, repo: str, pr: dict) -> dict:
        number = pr["number"]
        sha = pr["head"]["sha"]
        full = await self.gh.api("GET", f"/repos/{repo}/pulls/{number}")
        comments = await self.gh.api("GET", f"/repos/{repo}/issues/{number}/comments?per_page=100")
        checks = await self.gh.api("GET", f"/repos/{repo}/commits/{sha}/check-runs?per_page=50")
        runs = checks.get("check_runs") or []
        checks_ok = bool(runs) and all(r.get("status") == "completed" and r.get("conclusion") in ("success", "neutral", "skipped") for r in runs)
        checks_pending = any(r.get("status") != "completed" for r in runs)
        own = self.gh.login
        verdict = ""
        asked = skipped = False
        for c in comments:
            body = c.get("body") or ""
            if (c.get("user") or {}).get("login") != own:
                continue
            if REVIEW_MARKER in body:
                m = re.search(r"Verdict:\**\s*\**\s*(merge|hold|needs human)", body, re.I)
                if m:
                    verdict = m.group(1).lower()
            if ASKED_MARKER.format(sha=sha) in body:
                asked = True
            if SKIPPED_MARKER.format(sha=sha) in body:
                skipped = True
        labels = {l.get("name") for l in pr.get("labels") or []}
        author = (pr.get("user") or {}).get("login") or ""
        return {
            "number": number, "sha": sha, "title": pr.get("title") or "", "url": pr.get("html_url") or "",
            "author": author, "renovate": author == RENOVATE_LOGIN, "agent": (pr["head"].get("ref") or "").startswith(AGENT_BRANCH_PREFIX),
            "draft": bool(pr.get("draft")), "mergeable_state": full.get("mergeable_state"), "labels": labels,
            "update_types": parse_renovate_update_types(pr.get("body") or "") if author == RENOVATE_LOGIN else set(),
            "checks_ok": checks_ok, "checks_pending": checks_pending, "verdict": verdict, "asked": asked, "skipped": skipped,
        }

    async def evaluate_pr(self, repo: str, pr: dict) -> str | None:
        f = await self.pr_facts(repo, pr)
        n = f["number"]
        if f["draft"] or f["asked"] or f["skipped"] or f["checks_pending"]:
            return None
        if f["mergeable_state"] not in ("clean",):
            return None  # unknown, dirty, behind or blocked: look again next pass
        auto = (f["renovate"] and f["verdict"] == "merge" and f["update_types"] and f["update_types"] <= AUTOMERGE_UPDATE_TYPES
                and not (f["labels"] & AUTOMERGE_BLOCK_LABELS) and f["checks_ok"])
        if auto:
            why = f"renovate {','.join(sorted(f['update_types']))}, checks green, review verdict merge"
            ok, detail = await self.merge_pr(repo, f, by=f"policy ({why})")
            text = (f"✅ Auto-merged #{n} {f['title']}\n{why}\n{f['url']}" if ok else f"⚠️ Auto-merge of #{n} failed: {detail}\n{f['url']}")
            await self.send(ALERT_CHAT_ID, text)
            return text.splitlines()[0]
        ask = (f["renovate"] and f["verdict"] in ("merge", "hold", "needs human")) or (f["agent"] and f["checks_ok"])
        if not ask:
            return None
        kind = "renovate " + ",".join(sorted(f["update_types"]) or ["?"]) if f["renovate"] else "agent PR"
        meta = f"{kind}, checks {'✅' if f['checks_ok'] else '—'}, verdict: {f['verdict'] or 'none'}"
        if f["labels"] & AUTOMERGE_BLOCK_LABELS:
            meta += ", " + ",".join(sorted(f["labels"] & AUTOMERGE_BLOCK_LABELS))
        text = f"🔀 #{n} {f['title']}\n{meta}\n{f['url']}"
        keyboard = {"inline_keyboard": [[{"text": f"Merge #{n}", "callback_data": f"m:{GITHUB_REPOS.index(repo)}:{n}:{f['sha'][:12]}"},
                                         {"text": "Skip", "callback_data": f"s:{GITHUB_REPOS.index(repo)}:{n}:{f['sha'][:12]}"}]]}
        await self.tg("sendMessage", chat_id=ALERT_CHAT_ID, text=redact(text), disable_web_page_preview=True, reply_markup=keyboard)
        await self.gh.api("POST", f"/repos/{repo}/issues/{n}/comments",
                          json={"body": f"{ASKED_MARKER.format(sha=f['sha'])}\nMerge decision requested from the operator in Telegram ({meta})."})
        return f"🔀 asked about #{n} ({meta})"

    async def merge_pr(self, repo: str, f: dict, by: str) -> tuple[bool, str]:
        try:
            await self.gh.api("PUT", f"/repos/{repo}/pulls/{f['number']}/merge",
                              json={"merge_method": "merge", "sha": f["sha"]})
        except Exception as e:
            log.warning("merge of %s#%s failed: %s", repo, f["number"], e)
            return False, str(e)[:200]
        log.info("merged %s#%s %s", repo, f["number"], by)
        try:
            await self.gh.api("DELETE", f"/repos/{repo}/git/refs/heads/{f['head_ref']}") if f.get("head_ref") else None
        except Exception:
            pass
        return True, ""

    async def handle_callback(self, cq: dict) -> None:
        cq_id = cq.get("id")
        chat_id = str(((cq.get("message") or {}).get("chat") or {}).get("id") or "")
        from_id = str((cq.get("from") or {}).get("id") or "")
        data = cq.get("data") or ""
        if chat_id not in ALLOWED_CHAT_IDS or not self.gh:
            await self.tg("answerCallbackQuery", callback_query_id=cq_id, text="Not authorized.")
            return
        try:
            action, repo_idx, number, sha12 = data.split(":")
            repo = GITHUB_REPOS[int(repo_idx)]
            number = int(number)
        except Exception:
            await self.tg("answerCallbackQuery", callback_query_id=cq_id, text="Bad button data.")
            return
        await self.tg("answerCallbackQuery", callback_query_id=cq_id, text="Working…")
        message_id = (cq.get("message") or {}).get("message_id")
        old_text = (cq.get("message") or {}).get("text") or ""
        log.info("callback %s on %s#%s from user %s", action, repo, number, from_id)
        try:
            pr = await self.gh.api("GET", f"/repos/{repo}/pulls/{number}")
            f = await self.pr_facts(repo, pr)
            f["head_ref"] = pr["head"].get("ref")
            if pr.get("state") != "open":
                outcome = f"ℹ️ #{number} is already {pr.get('state')}."
            elif not f["sha"].startswith(sha12):
                outcome = f"ℹ️ #{number} changed since this was asked; it will be offered again."
            elif action == "s":
                await self.gh.api("POST", f"/repos/{repo}/issues/{number}/comments",
                                  json={"body": f"{SKIPPED_MARKER.format(sha=f['sha'])}\nOperator skipped the merge in Telegram."})
                outcome = f"⏭ Skipped #{number}."
            elif action == "m":
                if f["mergeable_state"] != "clean" or (f["checks_pending"]):
                    outcome = f"⚠️ #{number} is not mergeable right now (state {f['mergeable_state']}); try again later."
                else:
                    ok, detail = await self.merge_pr(repo, f, by=f"operator (telegram user {from_id})")
                    outcome = f"✅ Merged #{number}." if ok else f"⚠️ Merge of #{number} failed: {detail}"
            else:
                outcome = "Unknown action."
        except Exception as e:
            log.exception("callback failed")
            outcome = f"⚠️ {type(e).__name__}: {e}"
        new_text = f"{old_text}\n{outcome}"[:TG_MAX_LEN]
        await self.tg("editMessageText", chat_id=chat_id, message_id=message_id, text=redact(new_text), disable_web_page_preview=True)

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
    log.info("listening on :%d, agent=%s, allowed chats=%d, alert chat configured=%s, merge policy=%s repos=%s",
             LISTEN_PORT, A2A_URL, len(ALLOWED_CHAT_IDS), bool(ALERT_CHAT_ID), MERGE_POLICY_ENABLED, GITHUB_REPOS)
    policy_task = asyncio.create_task(bot.merge_policy_loop()) if bot.gh else None
    try:
        await bot.poll_telegram()
    finally:
        if policy_task:
            policy_task.cancel()
        await runner.cleanup()
        await bot.http.close()


if __name__ == "__main__":
    asyncio.run(main())
