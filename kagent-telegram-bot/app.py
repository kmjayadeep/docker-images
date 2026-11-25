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
3. Flux failure webhook (POST /flux): Alertmanager delivers Flux notification-controller
   error events (alertname Flux*: reconciliation or health check failed, Helm install/upgrade
   failed). After FLUX_VERIFY_DELAY_SECONDS the agent investigates the namespace, at most once per
   FLUX_DEDUP_SECONDS; the same failure (commit ids ignored) only once per FLUX_REPEAT_SECONDS.
   Successful changes never reach the agent; an answer that starts with OK (the failure healed
   itself) is logged, not posted.
4. Scheduled tasks (POST /task, JSON {"agent", "prompt", "title"}): a CronJob asks any agent
   in the same kagent namespace a question and the answer is posted to the alert chat, unless it
   matches TASK_QUIET_PATTERN ("nothing new"). Failed agent runs on every unattended path (2-4) are
   posted once per AGENT_ERROR_NOTIFY_SECONDS; repeats of a firing alert group with no new alert
   once per ALERT_REPEAT_NOTIFY_SECONDS.
5. Merge policy (optional, needs GITHUB_APP_* env): a deterministic pass over open pull requests
   of GITHUB_REPOS, every MERGE_POLICY_INTERVAL_SECONDS and after a gitops task. Renovate PRs
   whose update types are all in AUTOMERGE_UPDATE_TYPES, that carry no AUTOMERGE_BLOCK_LABELS,
   whose checks are green and whose review comment from our own GitHub App says "Verdict: merge"
   are merged by the bot itself. Every other reviewed PR and every kagent/* PR with
   green checks is offered to the operator in Telegram with Merge and Skip buttons; the bot
   merges only on the button press from an allow-listed chat. No model is on this path.
   DEPLOY_ON_MERGE_REPOS (a merge deploys at once) are never merged automatically, are merged one
   at a time, and the deploy run of each merge commit is followed and reported. Pull requests from
   forks are reported without a merge button.
6. Curator digest feedback (optional, needs MUSIC_MCP_URL and/or MOVIE_MCP_URL): after an answer
   from MUSIC_AGENT or MOVIE_AGENT (a weekly /task digest, or a /music or /movie question) the bot
   reads the digest that run stored in that agent's MCP server (GET /digest) and posts one 👍/👎/⬇
   button row per item. A press is sent to the server (POST /feedback); nothing goes through a
   model. The title button opens the item's page (movies: TMDB) or a Qobuz search (music).
   /music and /movie talk to their agent in a separate session per chat.
7. Chat UX: the commands are registered with Telegram (setMyCommands, scoped to the allowed chats) so
   they show up in the / menu. /home, /money, /music and /movie switch the chat to that agent: later
   plain messages go there until /sre switches back. Sent without a question, they also post quick
   question buttons. The typing indicator is kept up while an agent works. Agent answers are sent as
   Telegram HTML converted from their Markdown (plain text if Telegram rejects it). Replying to a bot
   message asks the agent that wrote it (by its emoji; anything else goes to the SRE agent) and quotes
   the replied-to message so the agent knows what "this" is.

Configuration is by environment variables; the bot token is never logged.
"""

from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import re
import time
import uuid
from urllib.parse import quote
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
# The same failure (same objects and messages, commit ids ignored) is investigated again only after
# FLUX_REPEAT_SECONDS; it is forgotten (treated as healed) when no event arrives for FLUX_FORGET_SECONDS.
FLUX_REPEAT_SECONDS = int(os.environ.get("FLUX_REPEAT_SECONDS", "86400"))
FLUX_FORGET_SECONDS = int(os.environ.get("FLUX_FORGET_SECONDS", "3600"))
# Post OK answers too (the failure healed before the agent looked)? Default: log only.
FLUX_POST_OK = os.environ.get("FLUX_POST_OK", "false").lower() in ("1", "true", "yes")
# Merge policy (GitHub App credentials are the same Secret the GitHub MCP server uses).
GITHUB_APP_ID = os.environ.get("GITHUB_APP_ID", "")
GITHUB_APP_INSTALLATION_ID = os.environ.get("GITHUB_APP_INSTALLATION_ID", "")
GITHUB_APP_PRIVATE_KEY = os.environ.get("GITHUB_APP_PRIVATE_KEY", "")
GITHUB_REPOS = [r.strip() for r in os.environ.get("GITHUB_REPOS", "").split(",") if r.strip()]
AUTOMERGE_UPDATE_TYPES = {t.strip() for t in os.environ.get("AUTOMERGE_UPDATE_TYPES", "patch,digest,pin,pinDigest").split(",") if t.strip()}
AUTOMERGE_BLOCK_LABELS = {t.strip() for t in os.environ.get("AUTOMERGE_BLOCK_LABELS", "critical-infra").split(",") if t.strip()}
# Per-repository merge behaviour, both subsets of GITHUB_REPOS.
# SQUASH_MERGE_REPOS: squash instead of a merge commit (rulesets that require linear history).
# DEPLOY_ON_MERGE_REPOS: a merge to the default branch deploys at once through a GitHub Actions run on the
# merge commit. Never merged automatically, no merge while a run is still going, and the run's result is posted.
SQUASH_MERGE_REPOS = {r.strip() for r in os.environ.get("SQUASH_MERGE_REPOS", "").split(",") if r.strip()}
DEPLOY_ON_MERGE_REPOS = {r.strip() for r in os.environ.get("DEPLOY_ON_MERGE_REPOS", "").split(",") if r.strip()}
DEPLOY_WATCH_TIMEOUT_SECONDS = int(os.environ.get("DEPLOY_WATCH_TIMEOUT_SECONDS", "1800"))
MUSIC_MCP_URL = os.environ.get("MUSIC_MCP_URL", "").rstrip("/")
MUSIC_AGENT = os.environ.get("MUSIC_AGENT", "music-curator")
MOVIE_MCP_URL = os.environ.get("MOVIE_MCP_URL", "").rstrip("/")
MOVIE_AGENT = os.environ.get("MOVIE_AGENT", "movie-curator")
# Empty disables /money. It asks the agent directly and has no digest or feedback buttons.
FINANCE_AGENT = os.environ.get("FINANCE_AGENT", "")
# Empty disables /home (Home Assistant agent). Like /money: a direct question, no digest buttons.
HOME_AGENT = os.environ.get("HOME_AGENT", "")
DIGEST_ACTIONS = {"l": ("like", "👍"), "d": ("dislike", "👎"), "w": ("want", "⬇")}
# Qobuz store whose search the title button opens, as in https://www.qobuz.com/ch-fr/search/tracks/...
# Empty keeps the title button as a plain label.
QOBUZ_STORE = os.environ.get("QOBUZ_STORE", "ch-fr").strip("/")
# Curator agents whose answers get digest feedback buttons. The prefix is the callback-data namespace;
# "f" stays music so buttons posted before movies existed keep working.
CURATORS = [c for c in (
    {"prefix": "f", "kind": "music", "command": "/music", "emoji": "🎵", "name": "music curator",
     "agent": MUSIC_AGENT, "url": MUSIC_MCP_URL, "want": "want it in the library",
     "examples": ["/music more like Vidyasagar's 2000s songs", "/music new Malayalam film albums this month"],
     "quick": [("🎧 Something new", "Suggest a few songs I have not heard, based on what I liked."),
               ("💿 New albums", "Any new Malayalam film albums this month?")]},
    {"prefix": "v", "kind": "movie", "command": "/movie", "emoji": "🎬", "name": "movie curator",
     "agent": MOVIE_AGENT, "url": MOVIE_MCP_URL, "want": "want to watch",
     "examples": ["/movie something like Bulbbul", "/movie new Malayalam thrillers this month"],
     "quick": [("🍿 Tonight", "Suggest one movie for tonight from what I liked."),
               ("🆕 New releases", "Any new Malayalam movies worth watching this month?")]},
    {"prefix": "m", "kind": "money", "command": "/money", "emoji": "💰", "name": "finance agent",
     "agent": FINANCE_AGENT, "url": "", "digest": False, "want": "",
     "examples": ["/money what is my net worth", "/money what did my last trip cost"],
     "quick": [("📊 Net worth", "What is my net worth?"),
               ("🧾 This month", "How much have I spent this month, by category?")]},
    {"prefix": "h", "kind": "home", "command": "/home", "emoji": "🏠", "name": "home agent",
     "agent": HOME_AGENT, "url": "", "digest": False, "want": "",
     "examples": ["/home is the living room light on", "/home set the wake time to 06:30"],
     "quick": [("💡 Light", "Is the living room light on?"),
               ("⚡ Power", "How much power are the homelab sockets drawing now, and how much energy today?"),
               ("⏰ Wake time", "What is the weekday wake time?"),
               ("🌤 Weather", "What is the weather today?")]},
) if c["url"] or (c["agent"] and not c.get("digest", True))]
CURATOR_BY_PREFIX = {c["prefix"]: c for c in CURATORS}
# Only curators with digests own a callback namespace: "m" is also the merge button's ("m:<repo>:<n>:<sha>").
DIGEST_BY_PREFIX = {c["prefix"]: c for c in CURATORS if c.get("digest", True)}
SRE_QUICK = [("🩺 Health", "Is anything in the cluster unhealthy right now? Short answer."),
             ("🔥 Alerts", "Which alerts are firing right now?")]
CURATOR_BY_AGENT = {c["agent"]: c for c in CURATORS}
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
# A repeat notification of a firing group with no new alert is posted at most once per window.
ALERT_REPEAT_NOTIFY_SECONDS = int(os.environ.get("ALERT_REPEAT_NOTIFY_SECONDS", "86400"))
# Unattended runs (alert, Flux, scheduled task) whose agent call failed: one post per window, the rest logged.
AGENT_ERROR_NOTIFY_SECONDS = int(os.environ.get("AGENT_ERROR_NOTIFY_SECONDS", "3600"))
# Scheduled-task answers matching this (case-insensitive, whole answer) are logged, not posted.
TASK_QUIET_PATTERN = re.compile(os.environ.get("TASK_QUIET_PATTERN", r"\W*nothing new\W*"), re.IGNORECASE)
FAILURE_PREFIXES = ("[task failed]", "[task rejected]", "[task canceled]", "agent error:",
                    "investigation failed:", "task failed:")
COMMIT_ID = re.compile(r"\b(?:sha1:|sha256:)?[0-9a-f]{7,64}\b")
MAX_CONCURRENT_INVESTIGATIONS = int(os.environ.get("MAX_CONCURRENT_INVESTIGATIONS", "2"))
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "8080"))
AGENT_NAME = os.environ.get("AGENT_DISPLAY_NAME", "homelab-sre")

TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
TG_MAX_LEN = 4000
# Also JWTs (Home Assistant long-lived tokens).
SECRET_PATTERN = re.compile(r"(sk-[A-Za-z0-9_-]{8,}|glsa_[A-Za-z0-9_-]{8,}|Bearer\s+[A-Za-z0-9._-]{8,}|eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,})")


def redact(text: str) -> str:
    return SECRET_PATTERN.sub("<redacted>", text)


def chunks(text: str, size: int = TG_MAX_LEN):
    for i in range(0, len(text), size):
        yield text[i : i + size]


def text_chunks(text: str, size: int = TG_MAX_LEN - 500):
    """Split on line boundaries (never inside a code block when avoidable) so each chunk converts on its own."""
    out, cur = [], ""
    for line in text.splitlines(keepends=True):
        if len(cur) + len(line) > size and cur:
            out.append(cur)
            cur = ""
        while len(line) > size:
            out.append(line[:size])
            line = line[size:]
        cur += line
    if cur:
        out.append(cur)
    # Keep code fences balanced per chunk.
    for i, c in enumerate(out):
        if c.count("```") % 2:
            out[i] = c + "\n```"
            if i + 1 < len(out):
                out[i + 1] = "```\n" + out[i + 1]
    return out


_INLINE = [
    (re.compile(r"`([^`\n]+)`"), r"<code>\1</code>"),
    (re.compile(r"\[([^\]\n]+)\]\((https?://[^)\s]+)\)"), r'<a href="\2">\1</a>'),
    (re.compile(r"\*\*(?=\S)(.+?)(?<=\S)\*\*"), r"<b>\1</b>"),
    (re.compile(r"__(?=\S)(.+?)(?<=\S)__"), r"<b>\1</b>"),
    (re.compile(r"(?<![\w*])\*(?=[^\s*])([^*\n]+?)(?<=[^\s*])\*(?![\w*])"), r"<i>\1</i>"),
    (re.compile(r"(?<![\w_])_(?=[^\s_])([^_\n]+?)(?<=[^\s_])_(?![\w_])"), r"<i>\1</i>"),
    (re.compile(r"~~(?=\S)(.+?)(?<=\S)~~"), r"<s>\1</s>"),
]


def _inline(line: str) -> str:
    # Protect inline code from the other rules.
    parts = re.split(r"(`[^`\n]+`)", line)
    out = []
    for part in parts:
        if part.startswith("`") and part.endswith("`") and len(part) > 1:
            out.append(f"<code>{part[1:-1]}</code>")
            continue
        for pattern, repl in _INLINE[1:]:
            part = pattern.sub(repl, part)
        out.append(part)
    return "".join(out)


def md_to_html(text: str) -> str:
    """The Markdown agents write (headings, bold, italics, code, links, lists, tables) as Telegram HTML."""
    out, in_code, code = [], False, []
    for raw in html.escape(text, quote=False).split("\n"):
        if raw.strip().startswith("```"):
            if in_code:
                out.append("<pre>" + "\n".join(code) + "</pre>")
                code, in_code = [], False
            else:
                in_code = True
            continue
        if in_code:
            code.append(raw)
            continue
        line = raw.rstrip()
        if m := re.match(r"^\s{0,3}#{1,6}\s+(.*)$", line):
            line = f"<b>{_inline(m.group(1).strip('# '))}</b>"
        elif re.match(r"^\s*([-*_])(\s*\1){2,}\s*$", line):
            line = "──────"
        elif re.match(r"^\s*\|?\s*:?-{2,}", line) and set(line.strip()) <= set("|-: "):
            continue  # table separator row
        elif line.lstrip().startswith("|") and line.rstrip().endswith("|"):
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            line = " · ".join(_inline(c) for c in cells)
        else:
            if m := re.match(r"^(\s*)[-*+]\s+(.*)$", line):
                line = f"{m.group(1)}• {m.group(2)}"
            elif m := re.match(r"^(\s*)&gt;\s?(.*)$", line):
                line = f"{m.group(1)}▎ {m.group(2)}"
            line = _inline(line)
        out.append(line)
    if in_code:
        out.append("<pre>" + "\n".join(code) + "</pre>")
    return "\n".join(out)


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
        self.curator_sessions: dict[tuple[str, str], str] = {}  # (curator prefix, chat id) -> A2A contextId
        self.modes: dict[str, str] = {}  # chat id -> curator prefix that plain messages go to (absent: SRE)
        self.last_investigation: dict[str, float] = {}  # alert group key -> epoch
        self.last_flux_check: dict[str, float] = {}  # namespace -> epoch
        self.flux_seen: dict[str, tuple[str, float, float]] = {}  # namespace -> (fingerprint, investigated, last seen)
        self.alert_posted: dict[str, tuple[frozenset, float]] = {}  # group key -> (firing fingerprints, posted)
        self.last_agent_error_post = 0.0
        self.suppressed_agent_errors = 0
        self.sem = asyncio.Semaphore(MAX_CONCURRENT_INVESTIGATIONS)
        self.ready = False
        self.gh = GitHubApp(self.http) if MERGE_POLICY_ENABLED else None
        self.policy_lock = asyncio.Lock()

    # ---------- Telegram ----------
    async def tg(self, method: str, _attempts: int = 4, _quiet: bool = False, **params):
        """Call the Bot API. Never raises: network errors become {"ok": False}. Transient failures
        (network, 429, 5xx) are retried with backoff, honouring Telegram's retry_after."""
        delay = 2.0
        data: dict = {}
        for attempt in range(_attempts):
            try:
                async with self.http.post(f"{TG_API}/{method}", json=params, timeout=ClientTimeout(total=70)) as r:
                    data = await r.json(content_type=None)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                data = {"ok": False, "error_code": 0, "description": type(e).__name__}
            if data.get("ok"):
                return data
            code = data.get("error_code") or 0
            retry_after = (data.get("parameters") or {}).get("retry_after")
            if not (code == 0 or code == 429 or code >= 500) or attempt == _attempts - 1:
                break
            await asyncio.sleep(retry_after or delay)
            delay = min(delay * 2, 30)
        if not _quiet:
            # Never include the URL (it carries the token) in logs.
            log.warning("telegram %s failed: %s", method, data.get("description"))
        return data

    async def send(self, chat_id: str, text: str, markdown: bool = False) -> None:
        text = redact(text) or "(empty response)"
        if not markdown:
            for part in chunks(text):
                await self.tg("sendMessage", chat_id=chat_id, text=part, disable_web_page_preview=True)
            return
        # Agent answers: Markdown rendered as Telegram HTML, falling back to the raw text if Telegram rejects it.
        for part in text_chunks(text):
            data = await self.tg("sendMessage", chat_id=chat_id, text=md_to_html(part), parse_mode="HTML",
                                 disable_web_page_preview=True)
            if not data.get("ok"):
                for raw in chunks(part):
                    await self.tg("sendMessage", chat_id=chat_id, text=raw, disable_web_page_preview=True)

    @staticmethod
    def is_failure(answer: str) -> bool:
        return answer.lstrip().lower().startswith(FAILURE_PREFIXES)

    async def post_unattended(self, header: str, answer: str) -> None:
        """Post the result of a run nobody is waiting for. Agent failures (model down, provider out of
        credit) repeat on every alert and task, so only one per AGENT_ERROR_NOTIFY_SECONDS is posted."""
        if self.is_failure(answer):
            now = time.time()
            if now - self.last_agent_error_post < AGENT_ERROR_NOTIFY_SECONDS:
                self.suppressed_agent_errors += 1
                log.info("agent failure not posted (%s): %s", header.splitlines()[0], answer[:200].replace("\n", " "))
                return
            self.last_agent_error_post = now
            if self.suppressed_agent_errors:
                answer += (f"\n\n({self.suppressed_agent_errors} more failed runs since the last notice; "
                           f"next notice in {AGENT_ERROR_NOTIFY_SECONDS // 60} min at the earliest)")
                self.suppressed_agent_errors = 0
        await self.send(ALERT_CHAT_ID, f"{header}\n{answer}", markdown=True)

    async def with_typing(self, chat_id: str, coro):
        """Await coro while showing "typing…" (Telegram clears it after about 5 s, so repeat it)."""
        async def keep_typing():
            while True:
                try:
                    await self.tg("sendChatAction", chat_id=chat_id, action="typing")
                except Exception:
                    pass
                await asyncio.sleep(4.5)
        typing = asyncio.create_task(keep_typing())
        try:
            return await coro
        finally:
            typing.cancel()

    def command_menu(self) -> list[dict]:
        cmds = [{"command": "sre", "description": f"Talk to {AGENT_NAME} (cluster questions; the default)"}]
        cmds += [{"command": c["command"][1:], "description": f"{c['emoji']} Talk to the {c['name']}"} for c in CURATORS]
        if self.gh:
            cmds.append({"command": "prs", "description": "Pull requests waiting for a merge decision"})
        cmds += [{"command": "new", "description": "Start fresh sessions (forget the conversation)"},
                 {"command": "help", "description": "What this bot can do"}]
        return cmds

    async def register_commands(self) -> None:
        """Show the commands in Telegram's / menu, only in the allowed chats."""
        cmds = self.command_menu()
        for chat_id in ALLOWED_CHAT_IDS:
            try:
                await self.tg("setMyCommands", commands=cmds, scope={"type": "chat", "chat_id": int(chat_id)})
            except Exception as e:
                log.warning("setMyCommands failed: %s", type(e).__name__)

    def help_text(self, chat_id: str) -> str:
        cur = CURATOR_BY_PREFIX.get(self.modes.get(chat_id, ""))
        now = f"{cur['emoji']} the {cur['name']}" if cur else f"{AGENT_NAME}"
        lines = [f"Plain messages go to {now} right now. Alerts are posted here automatically.", "",
                 f"/sre switches back to {AGENT_NAME} (cluster questions)."]
        lines += [f"{c['command']} switches to the {c['name']}; add a question to ask it at once, "
                  f"e.g. {c['examples'][0]}" for c in CURATORS]
        if self.gh:
            lines.append("/prs lists pull requests waiting for a merge decision.")
        lines.append("/new forgets the conversation and starts fresh sessions.")
        return "\n".join(lines)

    @staticmethod
    def quick_keyboard(prefix: str, quick: list[tuple[str, str]]) -> dict:
        buttons = [{"text": label, "callback_data": f"q:{prefix}:{i}"} for i, (label, _) in enumerate(quick)]
        return {"inline_keyboard": [buttons[i:i + 2] for i in range(0, len(buttons), 2)]}

    async def poll_telegram(self) -> None:
        offset = None
        failures = 0
        await self.register_commands()
        # Drop anything that queued up while we were down.
        await self.tg("deleteWebhook", drop_pending_updates=True)
        self.ready = True
        while True:
            try:
                params = {"timeout": 50, "allowed_updates": ["message", "callback_query"]}
                if offset is not None:
                    params["offset"] = offset
                data = await self.tg("getUpdates", _attempts=1, _quiet=True, **params)
                if not data.get("ok"):
                    # Telegram's 502s, 429s and 409s come back as answers, not exceptions: back off, or the
                    # loop spins and Telegram rate-limits the bot.
                    failures += 1
                    if failures == 1 or failures % 20 == 0:
                        log.warning("getUpdates failed (%d in a row): %s", failures, data.get("description"))
                    retry_after = (data.get("parameters") or {}).get("retry_after")
                    await asyncio.sleep(retry_after or min(5 * 2 ** min(failures - 1, 4), 60))
                    continue
                if failures:
                    log.info("getUpdates recovered after %d failures", failures)
                    failures = 0
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
        command, _, rest = text.partition(" ")
        command = command.split("@")[0].lower() if command.startswith("/") else ""
        if command in ("/start", "/help"):
            await self.send(chat_id, self.help_text(chat_id))
            return
        if command == "/sre":
            self.modes.pop(chat_id, None)
            if rest.strip():
                await self.ask_sre(chat_id, rest.strip())
            else:
                await self.tg("sendMessage", chat_id=chat_id, text=f"Plain messages now go to {AGENT_NAME}.",
                              reply_markup=self.quick_keyboard("sre", SRE_QUICK))
            return
        if command == "/new":
            self.sessions.pop(chat_id, None)
            for key in [k for k in self.curator_sessions if k[1] == chat_id]:
                self.curator_sessions.pop(key)
            await self.send(chat_id, "New session started.")
            return
        for cur in CURATORS:
            if command == cur["command"]:
                self.modes[chat_id] = cur["prefix"]
                await self.handle_curator(cur, chat_id, rest.strip())
                return
        if command == "/prs":
            if not self.gh:
                await self.send(chat_id, "Merge policy is not configured.")
                return
            summary = await self.merge_policy_pass(reason="/prs")
            await self.send(chat_id, summary or "No open pull request needs a decision.")
            return
        if command:
            await self.send(chat_id, "Unknown command. /help lists them.")
            return
        replied = msg.get("reply_to_message") or {}
        if replied.get("text") and (replied.get("from") or {}).get("is_bot"):
            # A reply goes to the agent that wrote the message (by its emoji), with the message quoted.
            quoted = replied["text"].strip()
            cur = next((c for c in CURATORS if quoted.startswith(c["emoji"])), None)
            question = f"I am replying to this earlier message:\n<<<\n{quoted[:2000]}\n>>>\n\n{text}"
            if cur:
                await self.handle_curator(cur, chat_id, question)
            else:
                await self.ask_sre(chat_id, question)
            return
        cur = CURATOR_BY_PREFIX.get(self.modes.get(chat_id, ""))
        if cur:
            await self.handle_curator(cur, chat_id, text)
            return
        await self.ask_sre(chat_id, text)

    async def ask_sre(self, chat_id: str, text: str) -> None:
        context_id = self.sessions.setdefault(chat_id, str(uuid.uuid4()))
        try:
            answer = await self.with_typing(chat_id, self.ask_agent(text, context_id))
        except Exception as e:
            log.exception("agent call failed for chat %s", chat_id)
            answer = f"The agent call failed: {type(e).__name__}: {e}"
        await self.send(chat_id, answer, markdown=True)

    # ---------- Curator digests (music, movies) ----------
    async def handle_curator(self, cur: dict, chat_id: str, question: str) -> None:
        if not question:
            text = (f"{cur['emoji']} Plain messages now go to the {cur['name']}; /sre switches back. "
                    f"Ask anything, for example: {cur['examples'][-1].split(' ', 1)[1]}")
            await self.tg("sendMessage", chat_id=chat_id, text=text,
                          reply_markup=self.quick_keyboard(cur["prefix"], cur.get("quick") or []))
            return
        context_id = self.curator_sessions.setdefault((cur["prefix"], chat_id), str(uuid.uuid4()))
        started = datetime.now(timezone.utc)
        try:
            answer = await self.with_typing(chat_id, self.ask_agent(question, context_id, agent=cur["agent"]))
        except Exception as e:
            log.exception("%s call failed for chat %s", cur["agent"], chat_id)
            answer = f"The {cur['name']} call failed: {type(e).__name__}: {e}"
        await self.send(chat_id, f"{cur['emoji']} {answer}", markdown=True)
        await self.post_digest_buttons(cur, chat_id, started)

    async def post_digest_buttons(self, cur: dict, chat_id: str, since: datetime) -> None:
        """Post feedback buttons for the digest stored during this run, if there is one."""
        if not cur.get("digest", True):
            return
        try:
            async with self.http.get(f"{cur['url']}/digest", timeout=ClientTimeout(total=15)) as r:
                if r.status != 200:
                    return
                digest = await r.json()
        except Exception as e:
            log.warning("%s digest fetch failed: %s", cur["agent"], type(e).__name__)
            return
        items = [i for i in digest.get("items") or [] if i.get("id")]
        created = parse_rfc3339(digest.get("created_at"))
        if not items or created is None or created < since.timestamp() - 60:
            return  # nothing stored in this run
        rows = [self.digest_row(cur, i) for i in items[:30]]
        await self.tg("sendMessage", chat_id=chat_id, disable_web_page_preview=True,
                      text=f"Rate {cur['kind']} digest {digest['id']}: 👍 like, 👎 not for me, ⬇ {cur['want']}.",
                      reply_markup={"inline_keyboard": rows})

    @staticmethod
    def qobuz_search_url(item: dict) -> str | None:
        """Qobuz store search for a music digest item: short title plus first artist for tracks, title for albums."""
        title = re.split(r"\s*[(\[]", item.get("title") or "")[0].strip()
        if not QOBUZ_STORE or not title:
            return None
        if item.get("kind") == "album":
            section, query = "albums", title
        else:
            artist = re.split(r"\s*(?:,|&| feat\.? | ft\.? )\s*", item.get("artist") or "")[0].strip()
            section, query = "tracks", f"{title} {artist}".strip()
        return f"https://www.qobuz.com/{QOBUZ_STORE}/search/{section}/{quote(query, safe='')}"

    @staticmethod
    def digest_row(cur: dict, item: dict, chosen: str | None = None) -> list[dict]:
        p, sid = cur["prefix"], item["id"]
        short = re.split(r"\s*[(\[]", item.get("title") or "")[0].strip() or item.get("title", "")
        label = f"{sid}. {short}"[:24]
        chosen = chosen or {"like": "l", "dislike": "d", "want": "w"}.get(item.get("feedback") or "")
        # The title cell opens the item's page (movies: TMDB) or a Qobuz search (music), else it is a label.
        url = item.get("search_url") or (Bot.qobuz_search_url(item) if p == "f" else None)
        row = [{"text": f"🔎 {label}", "url": url} if url else {"text": label, "callback_data": f"{p}:{sid}:n"}]
        for code, (_, emoji) in DIGEST_ACTIONS.items():
            row.append({"text": f"✓{emoji}" if code == chosen else emoji, "callback_data": f"{p}:{sid}:{code}"})
        return row

    async def handle_digest_callback(self, cur: dict, cq: dict, chat_id: str, data: str) -> None:
        cq_id = cq.get("id")
        try:
            p, sid, code = data.split(":")
            sid = int(sid)
        except Exception:
            await self.tg("answerCallbackQuery", callback_query_id=cq_id, text="Bad button data.")
            return
        if code == "n":  # the title cell is a label, not an action
            await self.tg("answerCallbackQuery", callback_query_id=cq_id, text="Use 👍, 👎 or ⬇ on this row.")
            return
        if code not in DIGEST_ACTIONS:
            await self.tg("answerCallbackQuery", callback_query_id=cq_id, text="Unknown action.")
            return
        action, emoji = DIGEST_ACTIONS[code]
        try:
            async with self.http.post(f"{cur['url']}/feedback", json={"id": sid, "action": action},
                                      timeout=ClientTimeout(total=15)) as r:
                ok = r.status == 200
                body = await r.json()
        except Exception as e:
            log.warning("%s feedback failed: %s", cur["agent"], type(e).__name__)
            ok, body = False, {}
        if not ok:
            await self.tg("answerCallbackQuery", callback_query_id=cq_id, text="Could not save that, try again later.")
            return
        log.info("%s feedback %s on suggestion %s", cur["kind"], action, sid)
        await self.tg("answerCallbackQuery", callback_query_id=cq_id, text=f"{emoji} saved for {body.get('title', sid)}")
        # Tick the pressed button in place.
        message = cq.get("message") or {}
        keyboard = (message.get("reply_markup") or {}).get("inline_keyboard") or []
        new_rows = []
        for row in keyboard:
            if any(str(b.get("callback_data", "")).startswith(f"{p}:{sid}:") for b in row):
                row = [row[0]] + self.digest_row(cur, {"id": sid}, chosen=code)[1:]  # keep the title cell as posted
            new_rows.append(row)
        await self.tg("editMessageReplyMarkup", chat_id=chat_id, message_id=message.get("message_id"),
                      reply_markup={"inline_keyboard": new_rows})

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
        now = time.time()
        if status != "firing":
            self.alert_posted.pop(group_key, None)
            await self.send(ALERT_CHAT_ID, text)
            return web.json_response({"ok": True, "investigated": False})
        # Alertmanager repeats a firing group every repeat_interval. Post only when an alert is new to the
        # group, or as a reminder once per ALERT_REPEAT_NOTIFY_SECONDS.
        firing = frozenset(a.get("fingerprint") or json.dumps(a.get("labels", {}), sort_keys=True)
                           for a in alerts if a.get("status", "firing") == "firing")
        prev = self.alert_posted.get(group_key)
        if prev and firing <= prev[0] and now - prev[1] < ALERT_REPEAT_NOTIFY_SECONDS:
            log.info("repeat of %s not posted (last posted %.0fs ago)", name, now - prev[1])
        else:
            self.alert_posted[group_key] = (firing, now)
            await self.send(ALERT_CHAT_ID, text)
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
            await self.post_unattended(f"🔎 {AGENT_NAME} on {name}:", answer)

    # ---------- Flux failure investigation ----------
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
            # Flux re-sends a persistent failure every reconcile interval, often with a new revision.
            fingerprint = "\n".join(sorted(COMMIT_ID.sub("<rev>", line) for line in lines))
            seen = self.flux_seen.get(ns)
            if seen and now - seen[2] > FLUX_FORGET_SECONDS:
                seen = None  # no event for a while: it healed, a new event is a new failure
            if seen and seen[0] == fingerprint and now - seen[1] < FLUX_REPEAT_SECONDS:
                self.flux_seen[ns] = (fingerprint, seen[1], now)
                log.info("flux failure in %s unchanged since %.0fs ago, not investigated again", ns, now - seen[1])
                continue
            if now - self.last_flux_check.get(ns, 0) < FLUX_DEDUP_SECONDS:
                continue
            self.last_flux_check[ns] = now
            self.flux_seen[ns] = (fingerprint, now, now)
            started.append(ns)
            asyncio.create_task(self.verify_flux_change(ns, lines))
        return web.json_response({"ok": True, "checked": bool(started), "namespaces": started})

    async def verify_flux_change(self, ns: str, lines: list[str]) -> None:
        await asyncio.sleep(FLUX_VERIFY_DELAY_SECONDS)
        async with self.sem:
            prompt = (
                f"Flux reported a failure in namespace '{ns}' about {FLUX_VERIFY_DELAY_SECONDS}s ago. Events:\n"
                + "\n".join(lines[:8])
                + "\n\nCheck whether it is still failing. If the Flux objects are Ready again and the workloads are "
                "healthy, answer with one sentence starting with OK. Otherwise start with PROBLEM and give, in at most "
                "600 characters of plain text without headers: what is failing, the cause from the evidence, and the "
                "Git change that fixes it. If that change is concrete, hand it to homelab-gitops as usual and end "
                "with the PR URL."
            )
            try:
                answer = await self.ask_agent(prompt, str(uuid.uuid4()))
            except Exception as e:
                log.exception("flux failure investigation failed for %s", ns)
                answer = f"Investigation failed: {type(e).__name__}: {e}"
            if answer.lstrip().upper().startswith("OK") and not FLUX_POST_OK:
                log.info("flux failure in %s healed (not posted): %s", ns, answer[:200].replace("\n", " "))
                return
            await self.post_unattended(f"🔁 Flux failure in {ns}:", answer)

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
        started = datetime.now(timezone.utc)
        async with self.sem:
            try:
                answer = await self.ask_agent(prompt, str(uuid.uuid4()), agent=agent)
            except Exception as e:
                log.exception("task '%s' failed", title)
                answer = f"Task failed: {type(e).__name__}: {e}"
        if TASK_QUIET_PATTERN.fullmatch(answer.strip()):
            log.info("task '%s' answer not posted: %s", title, answer.strip()[:200])
        else:
            await self.post_unattended(f"🗓 {title}:", answer)
        failed = self.is_failure(answer)
        if agent in CURATOR_BY_AGENT and not failed:
            await self.post_digest_buttons(CURATOR_BY_AGENT[agent], ALERT_CHAT_ID, started)
        if self.gh and agent and agent != AGENT_NAME:
            asyncio.create_task(self.merge_policy_pass(reason=f"task {title}"))
        # 200 even when the agent failed: the failure was reported (or rate-limited) above, and a failed
        # CronJob would only add a KubeJobFailed alert for the same outage.
        return web.json_response({"ok": not failed, "chars": len(answer)})

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
        # Docs-only PRs have no check runs (the validation workflow runs on clusters/** only): that is fine for
        # asking the operator, but automatic merges require at least one green check.
        checks_ok = all(r.get("status") == "completed" and r.get("conclusion") in ("success", "neutral", "skipped") for r in runs)
        has_checks = bool(runs)
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
            "fork": ((pr["head"].get("repo") or {}).get("full_name") or "") != repo, "base": (pr.get("base") or {}).get("ref") or "main",
            "draft": bool(pr.get("draft")), "mergeable_state": full.get("mergeable_state"), "labels": labels,
            "update_types": parse_renovate_update_types(pr.get("body") or "") if author == RENOVATE_LOGIN else set(),
            "checks_ok": checks_ok, "has_checks": has_checks, "checks_pending": checks_pending, "verdict": verdict,
            "asked": asked, "skipped": skipped,
        }

    async def evaluate_pr(self, repo: str, pr: dict) -> str | None:
        f = await self.pr_facts(repo, pr)
        n = f["number"]
        if f["draft"] or f["asked"] or f["skipped"] or f["checks_pending"]:
            return None
        if f["mergeable_state"] not in ("clean",):
            return None  # unknown, dirty, behind or blocked: look again next pass
        auto = (f["renovate"] and f["verdict"] == "merge" and f["update_types"] and f["update_types"] <= AUTOMERGE_UPDATE_TYPES
                and not (f["labels"] & AUTOMERGE_BLOCK_LABELS) and f["has_checks"] and f["checks_ok"]
                and repo not in DEPLOY_ON_MERGE_REPOS)
        name = repo.split("/")[-1]
        if auto:
            why = f"renovate {','.join(sorted(f['update_types']))}, checks green, review verdict merge"
            ok, detail = await self.merge_pr(repo, f, by=f"policy ({why})")
            text = (f"✅ Auto-merged {name}#{n} {f['title']}\n{why}\n{f['url']}" if ok else f"⚠️ Auto-merge of {name}#{n} failed: {detail}\n{f['url']}")
            await self.send(ALERT_CHAT_ID, text)
            return text.splitlines()[0]
        # A verdict exists only where the review agent was asked to review: Renovate PRs everywhere, and every
        # open PR in repositories whose scheduled review covers them.
        ask = bool(f["verdict"]) or (f["agent"] and f["checks_ok"])
        if not ask:
            return None
        if f["renovate"]:
            kind = "renovate " + ",".join(sorted(f["update_types"]) or ["?"])
        elif f["agent"]:
            kind = "agent PR"
        else:
            kind = f"PR by {f['author']}" + (" from a fork" if f["fork"] else "")
        meta = f"{kind}, checks {('✅' if f['checks_ok'] else '❌') if f['has_checks'] else 'none'}, verdict: {f['verdict'] or 'none'}"
        if f["labels"] & AUTOMERGE_BLOCK_LABELS:
            meta += ", " + ",".join(sorted(f["labels"] & AUTOMERGE_BLOCK_LABELS))
        if repo in DEPLOY_ON_MERGE_REPOS:
            meta += ", merge deploys at once"
        text = f"🔀 {name}#{n} {f['title']}\n{meta}\n{f['url']}"
        if f["fork"]:
            # Code from outside the account is never one tap away from main: report it and leave the decision to GitHub.
            await self.send(ALERT_CHAT_ID, f"{text}\nOutside pull request: no merge button, decide on GitHub.")
            await self.gh.api("POST", f"/repos/{repo}/issues/{n}/comments",
                              json={"body": f"{ASKED_MARKER.format(sha=f['sha'])}\nReported to the operator in Telegram without a merge button ({meta})."})
            return f"👀 reported {name}#{n} ({meta})"
        keyboard = {"inline_keyboard": [[{"text": f"Merge #{n}", "callback_data": f"m:{GITHUB_REPOS.index(repo)}:{n}:{f['sha'][:12]}"},
                                         {"text": "Skip", "callback_data": f"s:{GITHUB_REPOS.index(repo)}:{n}:{f['sha'][:12]}"}]]}
        await self.tg("sendMessage", chat_id=ALERT_CHAT_ID, text=redact(text), disable_web_page_preview=True, reply_markup=keyboard)
        await self.gh.api("POST", f"/repos/{repo}/issues/{n}/comments",
                          json={"body": f"{ASKED_MARKER.format(sha=f['sha'])}\nMerge decision requested from the operator in Telegram ({meta})."})
        return f"🔀 asked about {name}#{n} ({meta})"

    async def merge_pr(self, repo: str, f: dict, by: str) -> tuple[bool, str]:
        try:
            merged = await self.gh.api("PUT", f"/repos/{repo}/pulls/{f['number']}/merge",
                                       json={"merge_method": "squash" if repo in SQUASH_MERGE_REPOS else "merge", "sha": f["sha"]})
        except Exception as e:
            log.warning("merge of %s#%s failed: %s", repo, f["number"], e)
            return False, str(e)[:200]
        log.info("merged %s#%s %s", repo, f["number"], by)
        if repo in DEPLOY_ON_MERGE_REPOS and isinstance(merged, dict) and merged.get("sha"):
            asyncio.create_task(self.watch_deploy(repo, f["number"], merged["sha"]))
        try:
            await self.gh.api("DELETE", f"/repos/{repo}/git/refs/heads/{f['head_ref']}") if f.get("head_ref") else None
        except Exception:
            pass
        return True, ""

    async def deploy_in_progress(self, repo: str, branch: str) -> bool:
        """True while a workflow run on the branch has not finished. Unreadable runs (the App lacks the Actions
        read permission) count as idle: the merge still needs the operator's button press."""
        try:
            data = await self.gh.api("GET", f"/repos/{repo}/actions/runs?branch={branch}&per_page=5")
        except Exception as e:
            log.warning("deploy check: cannot list runs of %s: %s", repo, e)
            return False
        return any(r.get("status") != "completed" for r in data.get("workflow_runs") or [])

    async def watch_deploy(self, repo: str, number: int, sha: str) -> None:
        """Follow the workflow runs of a merge commit and post the outcome. No model involved."""
        name = repo.split("/")[-1]
        deadline = time.time() + DEPLOY_WATCH_TIMEOUT_SECONDS
        started = time.time()
        runs: list[dict] = []
        try:
            while time.time() < deadline:
                await asyncio.sleep(20)
                data = await self.gh.api("GET", f"/repos/{repo}/actions/runs?head_sha={sha}&per_page=10")
                runs = data.get("workflow_runs") or []
                if runs and all(r.get("status") == "completed" for r in runs):
                    break
                if not runs and time.time() - started > 300:
                    await self.send(ALERT_CHAT_ID, f"ℹ️ {name}#{number} merged ({sha[:7]}), but no deploy run started within 5 minutes.")
                    return
            else:
                url = runs[0].get("html_url") if runs else ""
                await self.send(ALERT_CHAT_ID, f"⚠️ Deploy of {name}#{number} ({sha[:7]}) is still running after {DEPLOY_WATCH_TIMEOUT_SECONDS // 60} minutes.\n{url}")
                return
        except Exception as e:
            log.warning("deploy watch of %s %s failed: %s", repo, sha[:7], e)
            await self.send(ALERT_CHAT_ID, f"⚠️ {name}#{number} merged ({sha[:7]}), but its deploy run cannot be read: {str(e)[:150]}")
            return
        failed = [r for r in runs if r.get("conclusion") not in ("success", "skipped", "neutral")]
        if failed:
            await self.send(ALERT_CHAT_ID, f"❌ Deploy of {name}#{number} ({sha[:7]}) failed: {failed[0].get('name')} {failed[0].get('conclusion')}\n{failed[0].get('html_url')}\nNothing was rolled back.")
        else:
            await self.send(ALERT_CHAT_ID, f"✅ Deploy of {name}#{number} ({sha[:7]}) succeeded.")

    async def handle_quick_callback(self, cq: dict, chat_id: str, data: str) -> None:
        """A quick question button: ask its question as if typed, in that agent's session."""
        try:
            _, prefix, idx = data.split(":")
            cur = CURATOR_BY_PREFIX.get(prefix)
            label, question = (cur.get("quick") or [] if cur else SRE_QUICK)[int(idx)]
        except Exception:
            await self.tg("answerCallbackQuery", callback_query_id=cq.get("id"), text="Unknown button.")
            return
        await self.tg("answerCallbackQuery", callback_query_id=cq.get("id"), text=label)
        await self.send(chat_id, f"› {question}")
        if cur:
            self.modes[chat_id] = prefix
            await self.handle_curator(cur, chat_id, question)
        else:
            self.modes.pop(chat_id, None)
            await self.ask_sre(chat_id, question)

    async def handle_callback(self, cq: dict) -> None:
        cq_id = cq.get("id")
        chat_id = str(((cq.get("message") or {}).get("chat") or {}).get("id") or "")
        from_id = str((cq.get("from") or {}).get("id") or "")
        data = cq.get("data") or ""
        if chat_id in ALLOWED_CHAT_IDS and data.startswith("q:"):
            await self.handle_quick_callback(cq, chat_id, data)
            return
        if chat_id in ALLOWED_CHAT_IDS and data[:2] in {f"{p}:" for p in DIGEST_BY_PREFIX}:
            await self.handle_digest_callback(DIGEST_BY_PREFIX[data[0]], cq, chat_id, data)
            return
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
                elif f["fork"]:
                    outcome = f"⚠️ #{number} comes from a fork; merge it on GitHub."
                elif repo in DEPLOY_ON_MERGE_REPOS and await self.deploy_in_progress(repo, f["base"]):
                    outcome = f"⏳ A deploy of {repo.split('/')[-1]} is still running; press Merge again when it has finished."
                else:
                    ok, detail = await self.merge_pr(repo, f, by=f"operator (telegram user {from_id})")
                    outcome = f"✅ Merged #{number}." if ok else f"⚠️ Merge of #{number} failed: {detail}"
                    if ok and repo in DEPLOY_ON_MERGE_REPOS:
                        outcome += " Deploy started; the result follows."
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
