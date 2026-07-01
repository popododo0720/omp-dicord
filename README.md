# omp-dicord

A Discord bot to drive the [omp](https://omp.sh) agent remotely.

## Install

```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
cp .env.example .env   # fill in the 2 values below
```

## .env (just 2 values)

- `DISCORD_BOT_TOKEN` — [Developer Portal](https://discord.com/developers/applications) → Bot → Reset Token (**enable MESSAGE CONTENT INTENT**)
- `ALLOWED_USER_IDS` — enable Discord Developer Mode → right-click your profile → Copy ID

## Run

```bash
./run.sh
```

DM the bot, or mention it in a server: `@bot <command>`.
