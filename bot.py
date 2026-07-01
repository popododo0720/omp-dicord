#!/usr/bin/env python3
"""omp Discord bridge.

Discord message -> `omp --mode rpc` prompt -> streamed reply back to Discord.

One persistent omp RPC session is kept per Discord channel, so conversations
carry context. Only whitelisted user IDs may drive the agent.
"""
from __future__ import annotations

import asyncio
import json
import os
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
# Per-channel session storage: lets sessions survive a bot restart via
# `--session-dir <root>/<channel_id> --continue` (context resume).
SESSION_ROOT = os.environ.get(
    "OMP_SESSION_ROOT", os.path.expanduser("~/.omp/agent/discord-sessions")
)
DISCORD_MAX = 1900  # leave headroom under Discord's 2000-char limit


# ---------------------------------------------------------------------------
# omp RPC session — one subprocess per channel, kept alive for context.
# ---------------------------------------------------------------------------
@dataclass
class OmpSession:
    proc: asyncio.subprocess.Process
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

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
        )
        self = cls(proc=proc)
        await self._await_ready()
        return self

    async def _readline(self, timeout: float) -> dict | None:
        assert self.proc.stdout
        try:
            raw = await asyncio.wait_for(self.proc.stdout.readline(), timeout=timeout)
        except asyncio.TimeoutError:
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

    async def prompt(self, message: str):
        """Send a prompt and yield streamed text chunks until the turn ends.

        Returns naturally when the turn ends so the async-for in the consumer
        completes and the lock is released. (A sentinel + break left the
        generator suspended holding the lock -> next prompt deadlocked.)
        """
        async with self.lock:
            # Drop any stale frames from a previously-interrupted turn so this
            # prompt's stream can't get contaminated by the last one (desync).
            await self._drain()
            self._write({"type": "prompt", "message": message})
            await self.proc.stdin.drain()  # type: ignore[union-attr]
            while True:
                frame = await self._readline(timeout=IDLE_TIMEOUT)
                if frame is None:
                    # No output for IDLE_TIMEOUT: surface it instead of dying quietly.
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
                elif ftype == "agent_end":
                    return

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
async def on_message(msg: discord.Message) -> None:
    ch = type(msg.channel).__name__
    print(f"[omp-bot] RX: author={msg.author}({msg.author.id}) ch={ch} "
          f"chan_id={msg.channel.id} len={len(msg.content)} "
          f"content={msg.content!r} mentions={[m.id for m in msg.mentions]}")
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

    import re as _re
    text = _re.sub(r"<@[&!]?\d+>", "", msg.content)  # strip user & role mention tokens
    text = text.strip()
    if not text:
        return

    started = time.monotonic()
    tools: list[str] = []
    collected = ""

    def status_line(done: bool) -> str:
        elapsed = time.monotonic() - started
        parts = [f"⏱ {elapsed:.1f}s"]
        if tools:
            uniq = list(dict.fromkeys(tools))  # de-dup, keep order
            parts.append("🔧 " + ", ".join(uniq[-5:]))
        parts.append("omp")
        prefix = ""
        if not done:
            prefix = "🤔 생각 중…  ·  " if not collected else "✍️ 작성 중…  ·  "
        # Discord subtext (-#): small grey status line, full chat width.
        return "-# " + prefix + "  ·  ".join(parts)

    def render_stream() -> str:
        body = collected[:DISCORD_MAX] if collected else "…"
        if collected:
            body += "▌"  # streaming cursor
        return body + "\n" + status_line(done=False)

    placeholder = await msg.reply(content=render_stream(), mention_author=False)
    try:
        sess = await get_session(msg.channel.id)
    except Exception as exc:  # noqa: BLE001
        await placeholder.edit(content=f"❌ omp 세션 시작 실패: {exc}")
        return

    # If the session is mid-turn, this prompt will queue behind it (prompt()
    # awaits the per-session lock). Tell the user instead of showing a frozen
    # "생각 중" that never advances.
    if sess.lock.locked():
        try:
            await placeholder.edit(
                content="⏳ 앞 작업을 처리하는 중입니다. 끝나는 대로 이어서 답할게요…\n"
                        + status_line(done=False)
            )
        except discord.HTTPException:
            pass

    last_edit = 0.0
    min_interval = 1.0          # base edit throttle (Discord rate limit ~5/5s per channel)
    pending = False             # unflushed content exists

    def at_boundary(s: str) -> bool:
        # flush on sentence / line boundaries for natural chunks
        return s.endswith(("\n", ". ", "! ", "? ", "다.", "요.", "죠.", "…", ".", "!", "?"))

    async def flush() -> None:
        nonlocal last_edit, pending, min_interval
        try:
            await placeholder.edit(content=render_stream())
            last_edit = time.monotonic()
            pending = False
        except discord.HTTPException as exc:
            # 429 rate-limited -> back off so we stop hammering
            if getattr(exc, "status", None) == 429:
                min_interval = min(min_interval * 1.5, 4.0)

    timed_out = False
    async with msg.channel.typing():
        async for kind, payload in sess.prompt(text):
            if kind == "delta":
                collected += payload
                pending = True
            elif kind == "tool":
                tools.append(payload)
                pending = True
            elif kind == "timeout":
                timed_out = True
                print(f"[omp-bot] idle timeout ({payload}s) with no output on channel {msg.channel.id}")
                break
            now = time.monotonic()
            if pending and now - last_edit >= min_interval:
                # prefer flushing at a natural boundary once past the throttle
                if at_boundary(collected) or now - last_edit >= min_interval * 2:
                    await flush()

    # Final render as full-width plain text, paginated across messages.
    if timed_out:
        mins = int(IDLE_TIMEOUT // 60)
        note = f"⌛ {mins}분 동안 응답이 없어 이번 턴을 종료했어요. 다시 말 걸어주세요."
        collected = f"{collected}\n\n{note}" if collected else note
    collected = collected or "(응답 없음)"
    pages = _chunk(collected, DISCORD_MAX)
    footer = "\n" + status_line(done=True)
    if len(pages[-1]) + len(footer) <= 2000:
        pages[-1] += footer
        footer_msg = None
    else:
        footer_msg = status_line(done=True)
    await placeholder.edit(content=pages[0])
    for extra in pages[1:]:
        await msg.channel.send(extra)
    if footer_msg:
        await msg.channel.send(footer_msg)


def main() -> None:
    if not TOKEN:
        raise SystemExit("DISCORD_BOT_TOKEN 이 설정되지 않았습니다 (.env 확인)")
    if not ALLOWED_USER_IDS:
        raise SystemExit("ALLOWED_USER_IDS 가 비어 있습니다 (.env 확인)")
    client.run(TOKEN)


if __name__ == "__main__":
    main()
