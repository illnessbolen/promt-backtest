#!/usr/bin/env bash
# Запуск стратегии @bosona внутри updown в paper-режиме (реальных ордеров нет) одной командой:
#   ./start-paper.sh            -> меню
#   ./start-paper.sh paper      -> сразу paper с записью тиков
#   ./start-paper.sh where      -> показать, какая папка updown используется
#   ./start-paper.sh <команда>  -> любая команда `python -m bosona`, например: updown-grid data/paper/ticks
# При первом запуске создаёт .venv и ставит зависимости. Папка updown: переменная UPDOWN_PATH, запомненная
# в .updown_path, поиск рядом с этой папкой и в ~/Downloads; иначе скрипт спросит путь (папку можно
# перетащить в окно терминала).
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

UD=""
probe() {   # папка $1 или папка updown* прямо в ней (распаковка ZIP часто даёт папку в папке)
  [ -n "$UD" ] && return 0
  if [ -f "$1/latarb/__init__.py" ]; then UD="$(cd "$1" && pwd)"; return 0; fi
  for e in "$1"/updown*; do
    if [ -f "$e/latarb/__init__.py" ]; then UD="$(cd "$e" && pwd)"; return 0; fi
  done
  return 0
}
if [ -n "${UPDOWN_PATH:-}" ]; then probe "$UPDOWN_PATH"; fi
if [ -z "$UD" ] && [ -f .updown_path ]; then probe "$(head -n 1 .updown_path)"; fi
if [ -z "$UD" ]; then
  for d in ./updown* ../updown* ../../updown* ../illnessbolen/updown "$HOME"/Downloads/updown*; do
    probe "$d"
  done
  while [ -z "$UD" ]; do
    echo
    echo "Не нашёл папку updown. Перетащите папку updown (в ней bot.py и папка latarb) в это окно и нажмите Enter."
    read -r -p "Папка updown: " answer || exit 1
    answer="$(printf '%s' "$answer" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e "s/^[\"']//" -e "s/[\"']\$//")"
    answer="${answer//\\ / }"          # терминал macOS экранирует пробелы в перетащенном пути
    if [ -z "$answer" ]; then
      echo "Папка не указана. Скачайте updown (github.com/illnessbolen/updown), распакуйте и запустите снова." >&2
      exit 1
    fi
    probe "$answer"
    if [ -z "$UD" ]; then echo "В «$answer» нет папки latarb — это не папка updown, попробуйте ещё раз."; fi
  done
  printf '%s\n' "$UD" > .updown_path
fi
export UPDOWN_PATH="$UD"
echo "[setup] updown: $UPDOWN_PATH"
if [ "${1:-}" = "where" ]; then exit 0; fi

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
