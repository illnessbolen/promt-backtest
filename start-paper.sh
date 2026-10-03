#!/usr/bin/env bash
# Запуск стратегии @bosona внутри updown в paper-режиме (реальных ордеров нет) одной командой:
#   ./start-paper.sh            -> меню
#   ./start-paper.sh paper      -> сразу paper с записью тиков
#   ./start-paper.sh <команда>  -> любая команда `python -m bosona`, например: updown-grid data/paper/ticks
# При первом запуске создаёт .venv и ставит зависимости. Папку updown ищет рядом с этой
# (updown, updown-*, illnessbolen/updown) или берёт из переменной UPDOWN_PATH.
set -euo pipefail
cd "$(dirname "$0")"

PY="${PYTHON:-}"
if [ -z "$PY" ]; then
  for c in python3.13 python3.12 python3.11 python3 python; do
    if command -v "$c" >/dev/null 2>&1 && \
       "$c" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' 2>/dev/null; then
      PY="$c"; break
    fi
  done
fi
if [ -z "$PY" ]; then
  echo "Нужен Python 3.11 или новее (python3 --version). Установите его с python.org и запустите снова." >&2
  exit 1
fi

if [ ! -x .venv/bin/python ]; then
  echo "[setup] создаю виртуальное окружение .venv ($("$PY" --version))"
  if ! "$PY" -m venv .venv; then
    echo "Не удалось создать venv. На Debian/Ubuntu: sudo apt install python3-venv" >&2
    rm -rf .venv
    exit 1
  fi
fi
VPY=.venv/bin/python

if [ ! -f .venv/.installed ] || [ pyproject.toml -nt .venv/.installed ]; then
  echo "[setup] устанавливаю зависимости (один раз, несколько минут)"
  "$VPY" -m pip install --upgrade pip >/dev/null
  "$VPY" -m pip install -e ".[updown]"
  touch .venv/.installed
fi

if [ -z "${UPDOWN_PATH:-}" ] || [ ! -f "$UPDOWN_PATH/latarb/__init__.py" ]; then
  UPDOWN_PATH=""
  for d in ../updown ../updown-* ../illnessbolen/updown; do
    if [ -f "$d/latarb/__init__.py" ]; then
      UPDOWN_PATH="$(cd "$d" && pwd)"; break
    fi
  done
fi
if [ -z "$UPDOWN_PATH" ]; then
  echo "Не нашёл папку updown. Скачайте updown (github.com/illnessbolen/updown) и положите её рядом" >&2
  echo "с этой папкой, например ../updown, или укажите путь: UPDOWN_PATH=/путь/к/updown ./start-paper.sh" >&2
  exit 1
fi
export UPDOWN_PATH
echo "[setup] updown: $UPDOWN_PATH"

if [ $# -eq 0 ]; then
  cat <<'MENU'

  Стратегия @bosona внутри updown, paper-режим (реальных ордеров нет) — что запустить?
    1) check   проверка: 5 минут paper без записи
    2) paper   paper с записью тиков; работает, пока не остановите (Ctrl+C)
    3) grid    сравнить настройки правил на записанных тиках (через несколько дней записи)
    4) test    прогнать тесты
    0) выход
  Пауза: создать файл STOP в этой папке (touch STOP) — заявки снимутся; удалить файл — продолжит.

MENU
  read -r -p "Выбор: " choice
  case "$choice" in
    1) set -- check ;;
    2) set -- paper ;;
    3) set -- grid ;;
    4) set -- test ;;
    *) exit 0 ;;
  esac
fi

case "$1" in
  check) exec "$VPY" -m bosona updown-paper --profile conservative --duration 300 ;;
  paper) exec "$VPY" -m bosona updown-paper --profile conservative --record ;;
  grid)  exec "$VPY" -m bosona updown-grid data/paper/ticks --profile conservative ;;
  test)
    "$VPY" -m pip install -q -e ".[dev,updown]"
    exec "$VPY" -m pytest -q ;;
  *) exec "$VPY" -m bosona "$@" ;;
esac
