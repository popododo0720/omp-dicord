# omp-dicord

Discord ↔ [oh-my-pi (omp)](https://omp.sh) 브리지 봇.

Discord 메시지를 `omp --mode rpc` 세션으로 넘기고, 스트리밍 응답을 다시
Discord로 돌려줍니다. 폰/데스크톱 Discord에서 코딩 에이전트를 원격 조종.

```
Discord DM/멘션  ──►  bot.py  ──►  omp --mode rpc  ──►  실제 작업(SSH/파일/코드)
                 ◄──          ◄── agent 이벤트 스트림 ◄──
```

## 네가 줘야 하는 것 (2개)

`.env` 파일에 딱 2개만 채우면 끝:

| 값 | 어디서 |
|---|---|
| `DISCORD_BOT_TOKEN` | [Discord Developer Portal](https://discord.com/developers/applications) → New Application → **Bot** → Reset Token. **MESSAGE CONTENT INTENT 켜기** 필수 |
| `ALLOWED_USER_IDS` | Discord 설정 → 고급 → 개발자 모드 ON → 내 프로필 우클릭 → ID 복사 |

## 설치

```bash
cd /root/omp-discord-bot
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
cp .env.example .env
# .env 에 위 2개 값 입력
```

## 봇 초대 (서버에 넣거나 DM용)

Developer Portal → OAuth2 → URL Generator:
- Scopes: `bot`
- Bot Permissions: `Send Messages`, `Read Message History`
- 생성된 URL로 봇을 서버에 초대. (DM만 쓸 거면 같은 서버에 있기만 하면 됨)

## 실행

```bash
./run.sh
```

또는 상시 구동 (systemd 등):

```bash
./.venv/bin/python bot.py   # .env 를 환경에 로드한 상태에서
```

## 사용

- **DM**: 봇에게 바로 메시지
- **서버 채널**: `@봇 216 상태 점검해` 처럼 멘션

같은 채널은 하나의 omp 세션을 공유하므로 대화 맥락이 이어집니다.

## 보안

- `ALLOWED_USER_IDS` 화이트리스트에 있는 사용자만 실행 가능. 그 외는 거부.
- 봇은 `omp`를 통해 실제 인프라(SSH/파일/명령)를 만질 수 있음 → **토큰과 ID를 반드시 관리**.
- 읽기전용으로 제한하려면 `.env` 의 `OMP_ARGS` 를 조정.

## 설정 (선택)

| 환경변수 | 기본 | 설명 |
|---|---|---|
| `OMP_BIN` | `/root/.bun/bin/omp` | omp 실행 파일 |
| `OMP_CWD` | `/root` | 에이전트 작업 디렉토리 |
| `OMP_ARGS` | `--yolo` | omp 실행 인자 |
| `OMP_IDLE_TIMEOUT` | `300` | 무응답 타임아웃(초) |
