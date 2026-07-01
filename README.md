# omp-dicord

A Discord bot to drive the [omp](https://omp.sh) agent remotely.

## Install

```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
cp .env.example .env   # fill in the values below
```

## .env

Required:

- `DISCORD_BOT_TOKEN` — [Developer Portal](https://discord.com/developers/applications) → Bot → Reset Token (**enable MESSAGE CONTENT INTENT**)
- `ALLOWED_USER_IDS` — enable Discord Developer Mode → right-click your profile → Copy ID (numeric)

Optional:

- `OMP_CHANNEL_IDS` — channels where the bot replies to every message (no mention needed). Right-click a channel → Copy ID.

## Run

```bash
./run.sh
```

DM the bot, mention it in a server (`@bot <command>`), or post in a bound channel.

## Run as a service (recommended)

The bot must run as a persistent service. A background `&` job dies with the
shell, and the unit bakes in the `HOME`/`PATH` env omp needs (otherwise omp
can't find its config and RPC fails with "did not emit ready frame").

```bash
cp deploy/omp-discord-bot.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now omp-discord-bot
systemctl status omp-discord-bot
```

Logs: `journalctl -u omp-discord-bot -f` or `tail -f bot.log`.
