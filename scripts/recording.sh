#!/usr/bin/env bash
# Laptop runner for the recorders, used until the cloud VM takes over. Elsewhere use
# `docker compose up -d` (same services, with automatic restarts); containers can't keep a
# Mac awake, so the laptop keeps this script and `caffeinate`.
#
#   scripts/recording.sh start    start Redis, the writer and every recorder, detached
#   scripts/recording.sh stop     stop them cleanly (SIGTERM: each finishes its current step)
#   scripts/recording.sh status   show what is running, Redis queue state and recent logs
#
# Processes run under `caffeinate -s`, which blocks system sleep while on AC power. On
# battery, or with the lid closed on battery, macOS sleeps anyway (2026-11-11: only 91 of
# ~723 overnight minutes recorded). Keep the laptop plugged in with the lid open. Ticks
# missed while asleep are skipped and logged, never stored under the wrong time.
set -euo pipefail
cd "$(dirname "$0")/.."

RUN_DIR=run
LOG_DIR=logs
SERVICES=(writer deribit kalshi polymarket reference settlements merger)   # add venues here as their recorders land
mkdir -p "$RUN_DIR" "$LOG_DIR"

cmd_for() {
  case "$1" in
    writer) echo "uv run ppm writer" ;;
    merger) echo "uv run ppm merge --loop" ;;
    *)      echo "uv run ppm record $1" ;;
  esac
}

is_running() { [[ -f "$RUN_DIR/$1.pid" ]] && kill -0 "$(cat "$RUN_DIR/$1.pid")" 2>/dev/null; }

start() {
  docker compose up -d --wait redis
  for svc in "${SERVICES[@]}"; do
    if is_running "$svc"; then echo "$svc already running (pid $(cat "$RUN_DIR/$svc.pid"))"; continue; fi
    # nohup + background: keeps running after this terminal (or Claude session) closes.
    nohup caffeinate -s $(cmd_for "$svc") >> "$LOG_DIR/$svc.log" 2>&1 &
    echo $! > "$RUN_DIR/$svc.pid"
    echo "started $svc (pid $!)"
  done
}

stop() {
  for svc in "${SERVICES[@]}"; do
    if is_running "$svc"; then
      pid=$(cat "$RUN_DIR/$svc.pid")
      # caffeinate passes SIGTERM on to nothing, so signal its child (the python process).
      pkill -TERM -P "$pid" 2>/dev/null || true
      kill -TERM "$pid" 2>/dev/null || true
      # Wait for a clean exit (the writer may be mid blocking read, up to ~5 s), so a
      # restart never runs two copies at once.
      for _ in $(seq 30); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
      echo "stopped $svc"
    fi
    rm -f "$RUN_DIR/$svc.pid"
  done
}

status() {
  for svc in "${SERVICES[@]}"; do
    if is_running "$svc"; then echo "RUNNING  $svc (pid $(cat "$RUN_DIR/$svc.pid"))"; else echo "STOPPED  $svc"; fi
  done
  echo "--- redis"
  docker compose exec -T redis sh -c 'for s in $(redis-cli --scan --pattern "ppm:*"); do echo "$s len=$(redis-cli XLEN $s)"; redis-cli XINFO GROUPS $s | paste - - | grep -E "pending|lag"; done' 2>/dev/null || echo "redis not running"
  echo "--- last log lines"
  for svc in "${SERVICES[@]}"; do tail -n 2 "$LOG_DIR/$svc.log" 2>/dev/null | grep -v "HTTP Request" | sed "s/^/[$svc] /"; done
}

case "${1:-}" in
  start) start ;; stop) stop ;; status) status ;;
  *) echo "usage: $0 {start|stop|status}"; exit 2 ;;
esac
