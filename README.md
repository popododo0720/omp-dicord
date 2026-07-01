# omp-dicord

Discord에서 [omp](https://omp.sh) 에이전트를 원격 조종하는 봇.

## 설치

```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
cp .env.example .env   # 아래 2개 값 입력
```

## .env (2개만)

- `DISCORD_BOT_TOKEN` — [Developer Portal](https://discord.com/developers/applications) → Bot → Reset Token (**MESSAGE CONTENT INTENT 켜기**)
- `ALLOWED_USER_IDS` — Discord 개발자 모드 ON → 내 프로필 우클릭 → ID 복사

## 실행

```bash
./run.sh
```

DM으로 메시지, 또는 서버에서 `@봇 명령`.
