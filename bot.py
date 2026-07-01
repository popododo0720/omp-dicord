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
        """Send a prompt and yield streamed text chunks until the turn ends."""
        async with self.lock:
            self._write({"type": "prompt", "message": message})
            await self.proc.stdin.drain()  # type: ignore[union-attr]
            while True:
                frame = await self._readline(timeout=IDLE_TIMEOUT)
                if frame is None:
                    yield ("__end__", "")
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
                    yield ("__end__", "")
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
    print(f"[omp-bot] logged in as {client.user} | whitelist={ALLOWED_USER_IDS}")


@client.event
async def on_message(msg: discord.Message) -> None:
    if msg.author.bot:
        return
    # Respond in DMs, or when mentioned in a guild channel.
    is_dm = isinstance(msg.channel, discord.DMChannel)
    mentioned = client.user in msg.mentions if client.user else False
    if not (is_dm or mentioned):
        return
    # Whitelist gate — the agent can touch real infra, so this is mandatory.
    if msg.author.id not in ALLOWED_USER_IDS:
        await msg.reply("⛔ 허가되지 않은 사용자입니다.", mention_author=False)
        return

    text = msg.content
    if client.user:
        text = text.replace(f"<@{client.user.id}>", "").replace(f"<@!{client.user.id}>", "")
    text = text.strip()
    if not text:
        return

    placeholder = await msg.reply("⏳ 처리 중…", mention_author=False)
    try:
        sess = await get_session(msg.channel.id)
    except Exception as exc:  # noqa: BLE001
        await placeholder.edit(content=f"❌ omp 세션 시작 실패: {exc}")
        return

    collected = ""
    tools: list[str] = []
    last_edit = 0.0
    async with msg.channel.typing():
        async for kind, payload in sess.prompt(text):
            if kind == "delta":
                collected += payload
            elif kind == "tool":
                tools.append(payload)
            elif kind == "__end__":
                break
            now = time.monotonic()
            if now - last_edit > 1.2 and collected:
                last_edit = now
                head = _chunk(collected)[0]
                suffix = f"\n\n🔧 {', '.join(tools[-3:])}" if tools else ""
                try:
                    await placeholder.edit(content=head[:DISCORD_MAX] + suffix[:80])
                except discord.HTTPException:
                    pass

    chunks = _chunk(collected or "(응답 없음)")
    await placeholder.edit(content=chunks[0])
    for extra in chunks[1:]:
        await msg.channel.send(extra)


def main() -> None:
    if not TOKEN:
        raise SystemExit("DISCORD_BOT_TOKEN 이 설정되지 않았습니다 (.env 확인)")
    if not ALLOWED_USER_IDS:
        raise SystemExit("ALLOWED_USER_IDS 가 비어 있습니다 (.env 확인)")
    client.run(TOKEN)


if __name__ == "__main__":
    main()
