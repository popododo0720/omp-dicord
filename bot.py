#!/usr/bin/env python3
"""omp Discord bridge.

Discord message -> `omp --mode rpc` prompt -> streamed reply back to Discord.

One persistent omp RPC session is kept per Discord channel, so conversations
carry context. Only whitelisted user IDs may drive the agent.

Auto-mode UX (no approval prompts, single session per channel):
  - Image attachments are forwarded to the agent (remote screenshot debugging).
  - A message sent WHILE a turn is streaming STEERS that turn (끼어들기), it does
    not queue a second turn.
  - Prefix commands: /stop /new /compact /model /think /stats /help.
  - Long replies are attached as a file instead of being split across messages.
  - Status line shows tools, subagents, elapsed, model, and context usage.
  - Long turns ping the author on completion so you can step away.
  - Deleting a still-queued prompt cancels it (delete-to-dequeue).
"""
from __future__ import annotations

import asyncio
import io
import json
import os
import re
import time
from dataclasses import dataclass, field

import discord

# ---------------------------------------------------------------------------
# Config (from environment / .env)
# ---------------------------------------------------------------------------
TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "").strip()
ALLOWED_USER_IDS = {
    int(x) for x in os.environ.get("ALLOWED_USER_IDS", "").replace(" ", "").split(",") if x
}
# Channels where the bot replies to ALL messages (no mention needed).
OMP_CHANNEL_IDS = {
    int(x) for x in os.environ.get("OMP_CHANNEL_IDS", "").replace(" ", "").split(",") if x
}
OMP_BIN = os.environ.get("OMP_BIN", "/root/.bun/bin/omp")
OMP_CWD = os.environ.get("OMP_CWD", os.path.expanduser("~"))
# Extra CLI args for omp (e.g. "--yolo" to auto-approve). Space separated.
OMP_ARGS = os.environ.get("OMP_ARGS", "--yolo").split()
# Seconds of silence from the agent before we assume the turn is done.
IDLE_TIMEOUT = float(os.environ.get("OMP_IDLE_TIMEOUT", "300"))
# Seconds to wait for the RPC "ready" handshake on cold start (contention-safe).
READY_TIMEOUT = float(os.environ.get("OMP_READY_TIMEOUT", "90"))
# Turns longer than this (s) ping the author on completion so they can step away.
PING_AFTER = float(os.environ.get("OMP_PING_AFTER", "90"))
# Replies longer than this (chars) are attached as a .md file, not paginated.
FILE_THRESHOLD = int(os.environ.get("OMP_FILE_THRESHOLD", "3600"))
# Skip image attachments larger than this (bytes) to avoid huge base64 frames.
MAX_IMAGE_BYTES = int(os.environ.get("OMP_MAX_IMAGE_BYTES", str(12 * 1024 * 1024)))
# Per-channel session storage: lets sessions survive a bot restart via
# `--session-dir <root>/<channel_id> --continue` (context resume).
SESSION_ROOT = os.environ.get(
    "OMP_SESSION_ROOT", os.path.expanduser("~/.omp/agent/discord-sessions")
)
DISCORD_MAX = 1900  # leave headroom under Discord's 2000-char limit

THINK_LEVELS = ("off", "minimal", "low", "medium", "high", "xhigh")


# ---------------------------------------------------------------------------
# omp RPC session — one subprocess per channel, kept alive for context.
# ---------------------------------------------------------------------------
@dataclass
class OmpSession:
    proc: asyncio.subprocess.Process
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    busy: bool = False          # True while a prompt turn is actively streaming
    _rid: int = 0

    @classmethod
    async def start(cls, channel_id: int) -> "OmpSession":
        # Isolate each channel's session in its own dir and --continue it, so a
        # bot restart resumes that channel's conversation instead of losing it.
        sess_dir = os.path.join(SESSION_ROOT, str(channel_id))
        os.makedirs(sess_dir, exist_ok=True)
        proc = await asyncio.create_subprocess_exec(
            OMP_BIN, "--mode", "rpc", *OMP_ARGS,
            "--session-dir", sess_dir, "--continue",
            cwd=OMP_CWD,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            # omp can emit a single JSON frame far bigger than asyncio's default
            # 64KB readline buffer (big tool results / large messages). Without a
            # raised limit, readline() throws LimitOverrunError and kills the turn.
            limit=16 * 1024 * 1024,
        )
        self = cls(proc=proc)
        await self._await_ready()
        # Subscribe to subagent progress so we can relay sub-work to the channel.
        try:
            self._write({"type": "set_subagent_subscription", "level": "progress"})
            await self.proc.stdin.drain()  # type: ignore[union-attr]
            await self._drain()
        except Exception as exc:  # noqa: BLE001
            print(f"[omp-bot] subagent subscribe failed: {exc}")
        return self

    def _next_id(self) -> str:
        self._rid += 1
        return f"bot_{self._rid}"

    async def _readline(self, timeout: float) -> dict | None:
        assert self.proc.stdout
        try:
            raw = await asyncio.wait_for(self.proc.stdout.readline(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        except (ValueError, asyncio.LimitOverrunError) as exc:
            # A single frame exceeded even the raised buffer. The oversized data
            # stays in the buffer (would re-raise forever), so end the turn
            # cleanly instead of looping or crashing the generator.
            print(f"[omp-bot] oversized RPC frame, ending turn: {exc}")
            return None
        if not raw:
            return None
        try:
            return json.loads(raw.decode("utf-8").strip())
        except json.JSONDecodeError:
            return {}

    async def _await_ready(self) -> None:
        # Consume frames until the {"type":"ready"} handshake.
        deadline = time.monotonic() + READY_TIMEOUT
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            frame = await self._readline(timeout=max(1.0, remaining))
            if frame and frame.get("type") == "ready":
                return
        raise RuntimeError("omp rpc did not emit ready frame")

    async def _drain(self) -> int:
        """Discard any frames left in the pipe from a previously-interrupted
        turn, so the next prompt does not read stale output (desync guard).
        Returns the number of frames dropped."""
        dropped = 0
        while True:
            frame = await self._readline(timeout=0.05)
            if frame is None:
                return dropped
            dropped += 1

    def _write(self, obj: dict) -> None:
        assert self.proc.stdin
        self.proc.stdin.write((json.dumps(obj) + "\n").encode("utf-8"))

    def steer(self, message: str) -> None:
        """Inject a message into the CURRENTLY streaming turn (끼어들기).
        Safe to call from another coroutine while prompt() holds the lock:
        it only writes to stdin, it does not read."""
        self._write({"type": "steer", "message": message})

    def abort(self) -> None:
        """Abort the current turn. The prompt() reader then sees the stream end."""
        self._write({"type": "abort"})

    async def request(self, cmd: dict, timeout: float = 30.0) -> dict:
        """Send a control command and wait for its matching response.
        Only usable when idle (acquires the turn lock). Returns {} on timeout."""
        async with self.lock:
            await self._drain()
            rid = self._next_id()
            self._write({**cmd, "id": rid})
            await self.proc.stdin.drain()  # type: ignore[union-attr]
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                frame = await self._readline(timeout=max(1.0, deadline - time.monotonic()))
                if frame and frame.get("type") == "response" and frame.get("id") == rid:
                    return frame
            return {}

    async def prompt(self, message: str, images: list[dict] | None = None):
        """Send a prompt and yield streamed (kind, payload) events until the turn
        ends. Yields: ('accepted',''), ('delta',text), ('tool',name),
        ('subagent',dict), ('timeout',secs). Returns when the turn ends."""
        async with self.lock:
            self.busy = True
            try:
                # Drop stale frames from a previously-interrupted turn (desync guard).
                await self._drain()
                cmd: dict = {"type": "prompt", "message": message}
                if images:
                    cmd["images"] = images
                self._write(cmd)
                await self.proc.stdin.drain()  # type: ignore[union-attr]
                # Past this point the turn is handed to omp and WILL run, so it can
                # no longer be pulled from the queue (delete-to-dequeue only works
                # before this). Signal the consumer to mark the job as started.
                yield ("accepted", "")
                while True:
                    frame = await self._readline(timeout=IDLE_TIMEOUT)
                    if frame is None:
                        # No output for IDLE_TIMEOUT: surface it, don't die quietly.
                        yield ("timeout", str(int(IDLE_TIMEOUT)))
                        return
                    ftype = frame.get("type")
                    if ftype == "message_update":
                        ev = frame.get("assistantMessageEvent") or {}
                        if ev.get("type") == "text_delta":
                            yield ("delta", ev.get("delta", ""))
                    elif ftype == "tool_execution_start":
                        name = (frame.get("toolName") or frame.get("tool") or "tool")
                        yield ("tool", str(name))
                    elif ftype == "subagent_lifecycle":
                        yield ("subagent", frame.get("payload") or {})
                    elif ftype == "agent_end":
                        return
            finally:
                self.busy = False

    def alive(self) -> bool:
        return self.proc.returncode is None

    async def close(self) -> None:
        if self.proc.stdin and not self.proc.stdin.is_closing():
            self.proc.stdin.close()
        try:
            await asyncio.wait_for(self.proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            self.proc.kill()


# ---------------------------------------------------------------------------
# Discord client
# ---------------------------------------------------------------------------
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

_sessions: dict[int, OmpSession] = {}
# Per-channel spawn locks so a slow cold start on one channel never blocks
# other channels (the old single global lock caused head-of-line blocking:
# a 90s ready-handshake froze get_session for every channel at once).
_spawn_locks: dict[int, asyncio.Lock] = {}
_spawn_locks_guard = asyncio.Lock()


@dataclass
class Job:
    """One in-flight user message. `started` flips true once the prompt is
    handed to omp; before that the job is still queued and can be dequeued by
    deleting the Discord message."""
    task: asyncio.Task
    placeholder: discord.Message
    started: bool = False


# message_id -> Job, for delete-to-dequeue.
_jobs: dict[int, Job] = {}


def _spawn_lock(channel_id: int) -> asyncio.Lock:
    lock = _spawn_locks.get(channel_id)
    if lock is None:
        lock = asyncio.Lock()
        _spawn_locks[channel_id] = lock
    return lock


async def get_session(channel_id: int) -> OmpSession:
    # Fast path: a live session already exists.
    sess = _sessions.get(channel_id)
    if sess is not None and sess.alive():
        return sess
    # Slow path: serialize spawns per-channel so two near-simultaneous messages
    # in the same channel don't each launch an omp process.
    async with _spawn_locks_guard:
        lock = _spawn_lock(channel_id)
    async with lock:
        sess = _sessions.get(channel_id)
        if sess is not None and sess.alive():
            return sess
        last_exc: Exception | None = None
        for attempt in range(2):  # cold start can lose the ready race under load
            try:
                sess = await OmpSession.start(channel_id)
                _sessions[channel_id] = sess
                return sess
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                print(f"[omp-bot] session start failed (attempt {attempt + 1}/2): {exc}")
        raise last_exc  # type: ignore[misc]


def _chunk(text: str, limit: int = DISCORD_MAX) -> list[str]:
    out, buf = [], ""
    for line in text.split("\n"):
        while len(line) > limit:
            if buf:
                out.append(buf)
                buf = ""
            out.append(line[:limit])
            line = line[limit:]
        if len(buf) + len(line) + 1 > limit:
            out.append(buf)
            buf = line
        else:
            buf = f"{buf}\n{line}" if buf else line
    if buf:
        out.append(buf)
    return out or ["(빈 응답)"]


async def _collect_images(msg: discord.Message) -> list[dict]:
    """Download image attachments and return RPC ImageContent items
    ({data: base64, mimeType: str})."""
    import base64 as _b64
    out: list[dict] = []
    for att in msg.attachments:
        ctype = (att.content_type or "").lower()
        if not ctype.startswith("image/"):
            continue
        if att.size and att.size > MAX_IMAGE_BYTES:
            print(f"[omp-bot] skip oversized image {att.filename} ({att.size}B)")
            continue
        try:
            raw = await att.read()
            out.append({"data": _b64.b64encode(raw).decode("ascii"), "mimeType": ctype})
        except Exception as exc:  # noqa: BLE001
            print(f"[omp-bot] image read failed {att.filename}: {exc}")
    return out


_prewarmed = False


@client.event
async def on_ready() -> None:
    global _prewarmed
    guilds = ", ".join(g.name for g in client.guilds) or "(공유 서버 없음!)"
    print(f"[omp-bot] logged in as {client.user} | whitelist={ALLOWED_USER_IDS}")
    print(f"[omp-bot] guilds: {guilds}")
    # Pre-warm bound-channel sessions so the first real message doesn't eat the
    # cold-start cost (which under load blew past the ready timeout and stalled).
    if not _prewarmed and OMP_CHANNEL_IDS:
        _prewarmed = True
        for cid in OMP_CHANNEL_IDS:
            try:
                await get_session(cid)
                print(f"[omp-bot] pre-warmed session for channel {cid}")
            except Exception as exc:  # noqa: BLE001
                print(f"[omp-bot] pre-warm failed for channel {cid}: {exc}")


@client.event
async def on_raw_message_delete(payload: discord.RawMessageDeleteEvent) -> None:
    # Delete-to-dequeue: if the user deletes their prompt while it's still
    # waiting in the queue (not yet handed to omp), cancel it and drop the
    # placeholder. If it already started running, we can't un-run it.
    job = _jobs.get(payload.message_id)
    if job is None:
        return
    if job.started:
        print(f"[omp-bot] delete ignored (already running): msg={payload.message_id}")
        return
    job.task.cancel()
    _jobs.pop(payload.message_id, None)
    try:
        await job.placeholder.delete()
    except discord.HTTPException:
        pass
    print(f"[omp-bot] dequeued by delete: msg={payload.message_id}")


# ---------------------------------------------------------------------------
# Prefix commands (/stop /new /compact /model /think /stats /help). Also accept
# the "!" prefix. Discord shows "/" as a slash-command hint but with no real app
# command registered the raw text still arrives here, so we parse it ourselves.
# ---------------------------------------------------------------------------
async def handle_command(msg: discord.Message, sess: OmpSession, raw: str) -> bool:
    m = re.match(r"^[/!](\w+)\s*(.*)$", raw, re.DOTALL)
    if not m:
        return False
    cmd, arg = m.group(1).lower(), m.group(2).strip()

    if cmd in ("stop", "abort", "cancel"):
        if sess.busy:
            sess.abort()
            try:
                await sess.proc.stdin.drain()  # type: ignore[union-attr]
            except Exception:  # noqa: BLE001
                pass
            await msg.reply("⏹ 현재 턴을 중단했어요.", mention_author=False)
        else:
            await msg.reply("실행 중인 작업이 없어요.", mention_author=False)
        return True

    if cmd == "help":
        await msg.reply(
            "**명령어**\n"
            "`/stop` 현재 턴 중단 · `/new` 새 세션(컨텍스트 초기화) · `/compact` 컨텍스트 압축\n"
            "`/model [이름]` 모델 보기/변경 · `/think [레벨]` 사고량(off·minimal·low·medium·high·xhigh)\n"
            "`/stats` 상태(모델·컨텍스트%) · `/help` 도움말\n"
            "그 외 메시지는 프롬프트. 스트리밍 중 새 메시지는 **끼어들기(steer)**, 이미지 첨부 지원.",
            mention_author=False,
        )
        return True

    # The rest mutate session state — refuse mid-turn to avoid corrupting the run.
    if sess.busy and cmd in ("new", "compact", "model", "think", "thinking", "stats", "state", "status"):
        await msg.reply("실행 중이에요. 먼저 `/stop` 후 다시 시도하세요.", mention_author=False)
        return True

    if cmd == "new":
        r = await sess.request({"type": "new_session"})
        ok = r.get("success")
        await msg.reply("🆕 새 세션 시작 (컨텍스트 초기화됨)." if ok else "새 세션 실패.",
                        mention_author=False)
        return True

    if cmd == "compact":
        async with msg.channel.typing():
            r = await sess.request({"type": "compact"}, timeout=180.0)
        await msg.reply("🗜 컨텍스트를 압축했어요." if r.get("success") else "압축 실패/타임아웃.",
                        mention_author=False)
        return True

    if cmd in ("think", "thinking"):
        if not arg:
            r = await sess.request({"type": "cycle_thinking_level"})
            lvl = (r.get("data") or {}).get("thinkingLevel") or (r.get("data") or {}).get("level")
            await msg.reply(f"🧠 사고량: **{lvl or '변경됨'}**", mention_author=False)
            return True
        lvl = arg.lower()
        if lvl not in THINK_LEVELS:
            await msg.reply(f"레벨은 {', '.join(THINK_LEVELS)} 중 하나.", mention_author=False)
            return True
        r = await sess.request({"type": "set_thinking_level", "level": lvl})
        await msg.reply(f"🧠 사고량 → **{lvl}**" if r.get("success") else "변경 실패.",
                        mention_author=False)
        return True

    if cmd == "model":
        avail = await sess.request({"type": "get_available_models"})
        models = (avail.get("data") or {}).get("models") or (avail.get("data") or {}).get("available") or []
        state = await sess.request({"type": "get_state"})
        cur = (state.get("data") or {}).get("model") or {}
        cur_s = f"{cur.get('provider','?')}/{cur.get('id','?')}"
        if not arg:
            names = []
            for md in models[:40]:
                if isinstance(md, dict):
                    names.append(f"{md.get('provider','?')}/{md.get('id') or md.get('modelId','?')}")
                else:
                    names.append(str(md))
            listing = "\n".join(f"• {n}" for n in names) or "(목록 없음)"
            await msg.reply(f"현재: **{cur_s}**\n사용 가능:\n{listing}\n\n변경: `/model <이름일부>`",
                            mention_author=False)
            return True
        # match by substring against provider/id
        target = None
        for md in models:
            if not isinstance(md, dict):
                continue
            mid = md.get("id") or md.get("modelId") or ""
            prov = md.get("provider") or ""
            if arg.lower() in f"{prov}/{mid}".lower():
                target = (prov, mid)
                break
        if not target:
            await msg.reply(f"'{arg}' 매칭 모델 없음. `/model` 로 목록 확인.", mention_author=False)
            return True
        r = await sess.request({"type": "set_model", "provider": target[0], "modelId": target[1]})
        await msg.reply(f"🤖 모델 → **{target[0]}/{target[1]}**" if r.get("success") else "모델 변경 실패.",
                        mention_author=False)
        return True

    if cmd in ("stats", "state", "status"):
        r = await sess.request({"type": "get_state"})
        d = r.get("data") or {}
        model = d.get("model") or {}
        cu = d.get("contextUsage") or {}
        pct = cu.get("percent")
        pct_s = f"{pct*100:.0f}%" if isinstance(pct, (int, float)) and pct <= 1 else (f"{pct:.0f}%" if isinstance(pct, (int, float)) else "?")
        await msg.reply(
            f"**상태**\n모델 `{model.get('provider','?')}/{model.get('id','?')}` · "
            f"사고량 `{d.get('thinkingLevel','?')}`\n"
            f"컨텍스트 {cu.get('tokens','?')}/{cu.get('contextWindow','?')} ({pct_s}) · "
            f"메시지 {d.get('messageCount','?')} · 큐 {d.get('queuedMessageCount', 0)}",
            mention_author=False,
        )
        return True

    return False


@client.event
async def on_message(msg: discord.Message) -> None:
    ch = type(msg.channel).__name__
    print(f"[omp-bot] RX: author={msg.author}({msg.author.id}) ch={ch} "
          f"chan_id={msg.channel.id} len={len(msg.content)} "
          f"content={msg.content!r} atts={len(msg.attachments)} mentions={[m.id for m in msg.mentions]}")
    if msg.author.bot:
        return
    # Respond in DMs, when mentioned (user OR the bot's own role), or in a bound channel.
    is_dm = isinstance(msg.channel, discord.DMChannel)
    user_mentioned = client.user in msg.mentions if client.user else False
    bot_role_ids = {r.id for r in getattr(msg.guild.me, "roles", [])} if msg.guild and msg.guild.me else set()
    role_mentioned = any(r.id in bot_role_ids for r in msg.role_mentions)
    in_bound_channel = msg.channel.id in OMP_CHANNEL_IDS
    if not (is_dm or user_mentioned or role_mentioned or in_bound_channel):
        return
    # Whitelist gate — the agent can touch real infra, so this is mandatory.
    if msg.author.id not in ALLOWED_USER_IDS:
        await msg.reply("⛔ 허가되지 않은 사용자입니다.", mention_author=False)
        return

    text = re.sub(r"<@[&!]?\d+>", "", msg.content)  # strip user & role mention tokens
    text = text.strip()
    images = await _collect_images(msg)
    if not text and not images:
        return

    # Establish the session first (needed for both commands and steering).
    try:
        sess = await get_session(msg.channel.id)
    except Exception as exc:  # noqa: BLE001
        await msg.reply(f"❌ omp 세션 시작 실패: {exc}", mention_author=False)
        return

    # Prefix commands.
    if text.startswith(("/", "!")):
        try:
            if await handle_command(msg, sess, text):
                return
        except Exception as exc:  # noqa: BLE001
            await msg.reply(f"명령 처리 실패: {exc}", mention_author=False)
            return

    # If a turn is already streaming, this message STEERS it (끼어들기) instead of
    # starting a second turn. The steered content appears in the ongoing reply.
    if sess.busy:
        try:
            sess.steer(text if text else "(이미지 첨부됨)")
            await sess.proc.stdin.drain()  # type: ignore[union-attr]
            await msg.add_reaction("🎯")
            print(f"[omp-bot] steered running turn on channel {msg.channel.id}")
        except Exception as exc:  # noqa: BLE001
            await msg.reply(f"끼어들기 실패: {exc}", mention_author=False)
        return

    started = time.monotonic()
    tools: list[str] = []
    subagents: list[str] = []
    collected = ""

    def status_line(done: bool, extra: str = "") -> str:
        elapsed = time.monotonic() - started
        parts = [f"⏱ {elapsed:.1f}s"]
        if tools:
            uniq = list(dict.fromkeys(tools))
            parts.append("🔧 " + ", ".join(uniq[-5:]))
        if subagents:
            uniq = list(dict.fromkeys(subagents))
            parts.append("🤖 " + ", ".join(uniq[-4:]))
        if extra:
            parts.append(extra)
        parts.append("omp")
        prefix = ""
        if not done:
            prefix = "🤔 생각 중…  ·  " if not collected else "✍️ 작성 중…  ·  "
        return "-# " + prefix + "  ·  ".join(parts)

    def render_stream() -> str:
        body = collected[:DISCORD_MAX] if collected else "…"
        if collected:
            body += "▌"
        return body + "\n" + status_line(done=False)

    placeholder = await msg.reply(content=render_stream(), mention_author=False)
    # Register this job so deleting the Discord message can dequeue it while it
    # is still waiting for the session lock (before omp receives the prompt).
    _cur = asyncio.current_task()
    job = Job(task=_cur, placeholder=placeholder) if _cur is not None else None
    if job is not None:
        _jobs[msg.id] = job

    last_edit = 0.0
    min_interval = 1.0          # base edit throttle (Discord rate limit ~5/5s per channel)
    pending = False             # unflushed content exists

    def at_boundary(s: str) -> bool:
        return s.endswith(("\n", ". ", "! ", "? ", "다.", "요.", "죠.", "…", ".", "!", "?"))

    async def flush() -> None:
        nonlocal last_edit, pending, min_interval
        try:
            await placeholder.edit(content=render_stream())
            last_edit = time.monotonic()
            pending = False
        except discord.HTTPException as exc:
            if getattr(exc, "status", None) == 429:
                min_interval = min(min_interval * 1.5, 4.0)

    timed_out = False
    async with msg.channel.typing():
        async for kind, payload in sess.prompt(text, images):
            if kind == "accepted":
                if job is not None:
                    job.started = True
                continue
            if kind == "delta":
                collected += payload
                pending = True
            elif kind == "tool":
                tools.append(payload)
                pending = True
            elif kind == "subagent":
                name = "subagent"
                if isinstance(payload, dict):
                    name = str(payload.get("agentType") or payload.get("agent")
                               or payload.get("name") or "subagent")
                subagents.append(name)
                pending = True
            elif kind == "timeout":
                timed_out = True
                print(f"[omp-bot] idle timeout ({payload}s) on channel {msg.channel.id}")
                break
            now = time.monotonic()
            if pending and now - last_edit >= min_interval:
                if at_boundary(collected) or now - last_edit >= min_interval * 2:
                    await flush()

    _jobs.pop(msg.id, None)
    elapsed = time.monotonic() - started

    # Best-effort context usage for the footer (idle now, lock is free).
    ctx_extra = ""
    try:
        st = await sess.request({"type": "get_state"}, timeout=8.0)
        d = st.get("data") or {}
        cu = d.get("contextUsage") or {}
        pct = cu.get("percent")
        if isinstance(pct, (int, float)):
            ctx_extra = f"🧠 {pct*100:.0f}%" if pct <= 1 else f"🧠 {pct:.0f}%"
        model = d.get("model") or {}
        if model.get("id"):
            ctx_extra = (ctx_extra + " · " if ctx_extra else "") + str(model.get("id"))
    except Exception:  # noqa: BLE001
        pass

    if timed_out:
        mins = int(IDLE_TIMEOUT // 60)
        note = f"⌛ {mins}분 동안 응답이 없어 이번 턴을 종료했어요. 다시 말 걸어주세요."
        collected = f"{collected}\n\n{note}" if collected else note
    collected = collected or "(응답 없음)"

    footer = "\n" + status_line(done=True, extra=ctx_extra)

    # Long replies: attach as a file instead of paginating across many messages.
    if len(collected) > FILE_THRESHOLD:
        preview = collected[:600].rstrip()
        buf = io.BytesIO(collected.encode("utf-8"))
        file = discord.File(buf, filename="omp-response.md")
        head = f"{preview}\n\n… (전체 {len(collected):,}자 첨부)" + footer
        try:
            await placeholder.edit(content=head[:2000])
            await msg.channel.send(file=file)
        except discord.HTTPException as exc:
            print(f"[omp-bot] file send failed: {exc}")
            await placeholder.edit(content=collected[:2000])
    else:
        pages = _chunk(collected, DISCORD_MAX)
        if len(pages[-1]) + len(footer) <= 2000:
            pages[-1] += footer
            footer_msg = None
        else:
            footer_msg = status_line(done=True, extra=ctx_extra)
        await placeholder.edit(content=pages[0])
        for extra in pages[1:]:
            await msg.channel.send(extra)
        if footer_msg:
            await msg.channel.send(footer_msg)

    # Ping the author for long turns so they can step away and get notified.
    if elapsed >= PING_AFTER and not timed_out:
        try:
            await msg.channel.send(f"<@{msg.author.id}> ✅ 완료 ({elapsed:.0f}s)")
        except discord.HTTPException:
            pass


def main() -> None:
    if not TOKEN:
        raise SystemExit("DISCORD_BOT_TOKEN 이 설정되지 않았습니다 (.env 확인)")
    if not ALLOWED_USER_IDS:
        raise SystemExit("ALLOWED_USER_IDS 가 비어 있습니다 (.env 확인)")
    client.run(TOKEN)


if __name__ == "__main__":
    main()
