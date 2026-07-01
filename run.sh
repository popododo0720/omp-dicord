#!/bin/bash
# omp Discord 봇 실행 래퍼
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -f .env ]; then
  echo "❌ .env 없음. cp .env.example .env 후 값 채우기"
  exit 1
fi

# .env 로드
set -a
# shellcheck disable=SC1091
source .env
set +a

exec ./.venv/bin/python bot.py
