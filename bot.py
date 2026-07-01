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
DISCORD_MAX = 1900  # leave headroom under Discord's 2000-char limit


# ---------------------------------------------------------------------------
# omp RPC session — one subprocess per channel, kept alive for context.
# ---------------------------------------------------------------------------
@dataclass
class OmpSession:
    proc: asyncio.subprocess.Process
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    @classmethod
    async def start(cls) -> "OmpSession":
        proc = await asyncio.create_subprocess_exec(
            OMP_BIN, "--mode", "rpc", *OMP_ARGS,
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
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            frame = await self._readline(timeout=30)
            if frame and frame.get("type") == "ready":
                return
        raise RuntimeError("omp rpc did not emit ready frame")

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
            self._write({"type": "prompt", "message": message})
            await self.proc.stdin.drain()  # type: ignore[union-attr]
            while True:
                frame = await self._readline(timeout=IDLE_TIMEOUT)
                if frame is None:
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
_sessions_lock = asyncio.Lock()


async def get_session(channel_id: int) -> OmpSession:
    async with _sessions_lock:
        sess = _sessions.get(channel_id)
        if sess is None or not sess.alive():
            sess = await OmpSession.start()
            _sessions[channel_id] = sess
        return sess


def _chunk(text: str) -> list[str]:
    out, buf = [], ""
    for line in text.split("\n"):
        while len(line) > DISCORD_MAX:
            out.append(line[:DISCORD_MAX])
            line = line[DISCORD_MAX:]
        if len(buf) + len(line) + 1 > DISCORD_MAX:
            out.append(buf)
            buf = line
        else:
            buf = f"{buf}\n{line}" if buf else line
    if buf:
        out.append(buf)
    return out or ["(빈 응답)"]


@client.event
async def on_ready() -> None:
    guilds = ", ".join(g.name for g in client.guilds) or "(공유 서버 없음!)"
    print(f"[omp-bot] logged in as {client.user} | whitelist={ALLOWED_USER_IDS}")
    print(f"[omp-bot] guilds: {guilds}")


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

    def build_embed(body: str, *, done: bool) -> discord.Embed:
        color = 0x2ECC71 if done else 0x5865F2  # green when done, blurple while working
        shown = body[:4000] if body else "…"
        if not done and body:
            shown = (shown[:3999] + "▌")  # streaming cursor
        emb = discord.Embed(description=shown, color=color)
        elapsed = time.monotonic() - started
        parts = [f"⏱ {elapsed:.1f}s"]
        if tools:
            uniq = list(dict.fromkeys(tools))  # de-dup, keep order
            parts.append("🔧 " + ", ".join(uniq[-5:]))
        parts.append("omp")
        emb.set_footer(text="  ·  ".join(parts))
        if not done:
            emb.set_author(name="🤔 생각 중…" if not body else "✍️ 작성 중…")
        return emb

    tools: list[str] = []
    placeholder = await msg.reply(embed=build_embed("", done=False), mention_author=False)
    try:
        sess = await get_session(msg.channel.id)
    except Exception as exc:  # noqa: BLE001
        await placeholder.edit(embed=build_embed(f"❌ omp 세션 시작 실패: {exc}", done=True))
        return

    collected = ""
    last_edit = 0.0
    min_interval = 1.0          # base edit throttle (Discord rate limit ~5/5s per channel)
    pending = False             # unflushed content exists

    def at_boundary(s: str) -> bool:
        # flush on sentence / line boundaries for natural chunks
        return s.endswith(("\n", ". ", "! ", "? ", "다.", "요.", "죠.", "…", ".", "!", "?"))

    async def flush(done: bool = False) -> None:
        nonlocal last_edit, pending, min_interval
        try:
            await placeholder.edit(embed=build_embed(collected or "…", done=done))
            last_edit = time.monotonic()
            pending = False
        except discord.HTTPException as exc:
            # 429 rate-limited -> back off so we stop hammering
            if getattr(exc, "status", None) == 429:
                min_interval = min(min_interval * 1.5, 4.0)

    async with msg.channel.typing():
        async for kind, payload in sess.prompt(text):
            if kind == "delta":
                collected += payload
                pending = True
            elif kind == "tool":
                tools.append(payload)
                pending = True
            now = time.monotonic()
            if pending and now - last_edit >= min_interval:
                # prefer flushing at a natural boundary once past the throttle
                if at_boundary(collected) or now - last_edit >= min_interval * 2:
                    await flush(done=False)

    # Final render. Long answers: first 4000 chars in the embed, overflow as follow-up messages.
    collected = collected or "(응답 없음)"
    await flush(done=True)
    if len(collected) > 4000:
        for extra in _chunk(collected[4000:]):
            await msg.channel.send(extra)


def main() -> None:
    if not TOKEN:
        raise SystemExit("DISCORD_BOT_TOKEN 이 설정되지 않았습니다 (.env 확인)")
    if not ALLOWED_USER_IDS:
        raise SystemExit("ALLOWED_USER_IDS 가 비어 있습니다 (.env 확인)")
    client.run(TOKEN)


if __name__ == "__main__":
    main()
