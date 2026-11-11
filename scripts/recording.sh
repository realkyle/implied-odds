#!/usr/bin/env bash
# Interim laptop runner for the Phase 0 recorders (replaced by docker compose in Stage 1.5).
#
#   scripts/recording.sh start    start Redis, the writer and every recorder, detached
#   scripts/recording.sh stop     stop them cleanly (SIGTERM: each finishes its current step)
#   scripts/recording.sh status   show what is running, Redis queue state and recent logs
#
# Processes run under `caffeinate -i` so the Mac doesn't idle-sleep while recording.
# Closing the lid still sleeps the machine; ticks missed then are skipped and logged.
set -euo pipefail
cd "$(dirname "$0")/.."

RUN_DIR=run
LOG_DIR=logs
SERVICES=(writer deribit)   # add venues here as their recorders land
mkdir -p "$RUN_DIR" "$LOG_DIR"

cmd_for() {
  case "$1" in
    writer) echo "uv run ppm writer" ;;
    *)      echo "uv run ppm record $1" ;;
  esac
}

is_running() { [[ -f "$RUN_DIR/$1.pid" ]] && kill -0 "$(cat "$RUN_DIR/$1.pid")" 2>/dev/null; }

start() {
  docker compose up -d --wait redis
  for svc in "${SERVICES[@]}"; do
    if is_running "$svc"; then echo "$svc already running (pid $(cat "$RUN_DIR/$svc.pid"))"; continue; fi
    # nohup + background: keeps running after this terminal (or Claude session) closes.
    nohup caffeinate -i $(cmd_for "$svc") >> "$LOG_DIR/$svc.log" 2>&1 &
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
