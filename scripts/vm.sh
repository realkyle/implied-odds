#!/usr/bin/env bash
# Control the recording VM from the laptop, over SSH. Every command the VM runs is here,
# so the VM-only settings below are applied the same way every time.
#
#   scripts/vm.sh bootstrap              once: g++ and uv on the VM, clone the repo
#   scripts/vm.sh deploy [REF]           run commit REF (default origin/main) on the VM
#   scripts/vm.sh deploy --branch REF    allow a pushed commit that isn't on main yet
#   scripts/vm.sh status                 deployed commit, services, memory, disk, Redis
#   scripts/vm.sh logs SERVICE [ARGS]    docker compose logs on the VM (e.g. --tail 50)
#   scripts/vm.sh faulttest [ARGS]       `ppm faulttest` on the VM, next to the live stack
#   scripts/vm.sh ssh [CMD]              shell (or one command) on the VM
#   aws iam create-access-key … | scripts/vm.sh backup-key
#                                        install the S3 backup key (JSON on stdin) on the VM
#   scripts/vm.sh backup-check           prove the VM's key can add but never overwrite/delete
#
# Why a script: if PPM_HOST_DATA were forgotten, Compose would silently write the live
# data into the checkout; if the project name were left to default, it would come from
# the directory name and the Redis volume would change name with it.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
KEY=${PPM_VM_KEY:-$HOME/.ssh/ppm_lightsail}
REPO_URL=https://github.com/realkyle/implied-odds.git # public: no credentials on the VM
UV_VERSION=0.13.7                                     # same uv as the Dockerfile

die() {
  echo "vm.sh: $*" >&2
  exit 1
}

tf_output() { terraform -chdir="$ROOT/infra/terraform" output -raw "$1"; }
vm_ip() { tf_output vm_ip; }

on_vm() { ssh -i "$KEY" -o BatchMode=yes "ubuntu@$(vm_ip)" "$@"; }

# Every VM-side script starts with this: strict mode, paths, and `live`, the only way
# this file runs Compose for the live stack (pinned project name, VM data directories).
PRELUDE=$(
  cat <<'VM'
set -euo pipefail
APP=/srv/ppm/app
BACKUP_KEY=/srv/ppm/secrets/aws-backup
export PATH="$HOME/.local/bin:$PATH"
live() {
  cd "$APP"
  # The backup service runs only once both its key (`vm.sh backup-key`) and the bucket
  # name (written by `vm.sh deploy`) exist. Otherwise it would exit at start and
  # crash-loop under `restart: unless-stopped` without ever reaching its alerting.
  local profiles=""
  [ -f "$BACKUP_KEY" ] && [ -s /srv/ppm/backup-bucket ] && profiles=backup
  COMPOSE_PROJECT_NAME=ppm PPM_HOST_DATA=/srv/ppm/data PPM_HOST_SPOOL=/srv/ppm/spool \
    COMPOSE_PROFILES=$profiles PPM_BACKUP_CREDENTIALS=$BACKUP_KEY \
    PPM_BACKUP_BUCKET=$(cat /srv/ppm/backup-bucket 2>/dev/null || true) \
    docker compose "$@"
}
VM
)

# run_vm ARGS... <<'VM' body VM: run prelude + body (stdin) on the VM with ARGS as $1...
# `bash -s` reads the script itself from stdin, so any VM command that could read stdin
# (docker compose exec, apt, git) gets </dev/null, or it would swallow the rest.
# Bodies are quoted heredocs, so nothing in them is expanded on the laptop. SSH joins the
# arguments with spaces and the VM's shell splits them again, so only plain tokens are
# allowed (checked here), never anything with spaces or shell characters.
run_vm() {
  local a
  for a in "$@"; do
    [[ $a =~ ^[A-Za-z0-9._:/=+@-]+$ ]] || die "argument not allowed: '$a'"
  done
  { printf '%s\n' "$PRELUDE"; cat; } | on_vm bash -s -- "$@"
}

cmd_bootstrap() {
  run_vm "$REPO_URL" "$UV_VERSION" <<'VM'
repo_url=$1 uv_version=$2
# g++ builds ppm_core for the host-side faulttest venv (cmake/ninja come from PyPI).
if ! command -v g++ >/dev/null; then
  sudo apt-get -o DPkg::Lock::Timeout=600 update -q </dev/null
  sudo DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=600 \
    install -y -q --no-install-recommends g++ </dev/null
fi
if [ "$(uv --version 2>/dev/null | cut -d' ' -f2)" != "$uv_version" ]; then
  # Official installer, pinned to one version; installs to ~/.local/bin only.
  curl -LsSf "https://astral.sh/uv/$uv_version/install.sh" | UV_NO_MODIFY_PATH=1 sh
fi
[ -d "$APP/.git" ] || git clone --quiet "$repo_url" "$APP" </dev/null
echo "g++ $(g++ -dumpversion), $(uv --version), checkout $(git -C "$APP" rev-parse --short HEAD)"
VM
}

cmd_deploy() {
  local allow_branch=0
  if [ "${1:-}" = "--branch" ]; then
    allow_branch=1
    shift
  fi
  local ref=${1:-origin/main} sha
  # --prune: a branch deleted on GitHub (after a squash-merge) must not still count as pushed.
  git -C "$ROOT" fetch --quiet --prune origin
  sha=$(git -C "$ROOT" rev-parse --verify --quiet "$ref^{commit}") || die "unknown ref: $ref"
  if git -C "$ROOT" merge-base --is-ancestor "$sha" origin/main; then
    :
  elif [ "$allow_branch" = 1 ]; then
    [ -n "$(git -C "$ROOT" branch -r --contains "$sha")" ] ||
      die "$sha is not pushed to GitHub; the VM fetches from there"
  else
    die "$sha is not on origin/main (use --branch for a pushed, unmerged commit)"
  fi
  echo "deploying $sha"
  run_vm "$sha" "$(tf_output backup_bucket)" <<'VM'
sha=$1
printf '%s\n' "$2" >/srv/ppm/backup-bucket # not a secret; read by live()
cd "$APP"
git fetch --quiet origin '+refs/heads/*:refs/remotes/origin/*' </dev/null
git checkout --quiet --detach "$sha"
start=$(date +%s)
live up -d --build --wait --remove-orphans
echo "$(date -u +%FT%TZ) $sha build+start $(($(date +%s) - start)) s" | tee -a /srv/ppm/deploys.log
live ps --format 'table {{.Service}}\t{{.Status}}'
VM
}

cmd_status() {
  run_vm <<'VM'
echo "--- deployed: $(git -C "$APP" rev-parse --short HEAD)  ($(tail -n1 /srv/ppm/deploys.log 2>/dev/null || true))"
live ps --format 'table {{.Service}}\t{{.Status}}'
echo "--- memory (MB)"
free -m
echo "--- disk"
df -h /srv/ppm | tail -n1
echo "--- redis"
live exec -T redis sh -c '
  redis-cli INFO memory | grep -E "^used_memory_human|^used_memory_peak_human"
  for s in $(redis-cli --scan --pattern "ppm:*"); do
    echo "$s len=$(redis-cli XLEN "$s") bytes=$(redis-cli MEMORY USAGE "$s" SAMPLES 0)"
  done' </dev/null
echo "--- monitor (last 5 alert/gap lines)"
live logs --no-color --no-log-prefix monitor | grep -E "ALERT|GAP|RECOVERED" | tail -n5 || true
VM
}

cmd_logs() {
  [ $# -ge 1 ] || die "usage: vm.sh logs SERVICE [ARGS]"
  run_vm "$@" <<'VM'
live logs --no-color "$@"
VM
}

cmd_faulttest() {
  # The daily merge runs from 02:07 UTC and reads a whole day of files; the test takes
  # ~30 min and shares the 2 GB of RAM, so it must neither start nor run into that.
  local hhmm
  hhmm=$(date -u +%H%M)
  if ((10#$hhmm >= 125 && 10#$hhmm < 330)); then # 10#: "0830" is decimal, not octal
    die "refusing at $hhmm UTC: too close to the 02:07 daily merge (allowed outside 01:25-03:30)"
  fi
  run_vm "$@" <<'VM'
cd "$APP"
# The harness runs on the host: it drives the docker CLI and reads the run's Parquet.
# uv-managed Python 3.11: same as the image, and it ships the headers ppm_core needs
# (Ubuntu's system Python 3.12 has none).
export UV_PYTHON=3.11 UV_PYTHON_PREFERENCE=only-managed
uv sync --quiet --no-dev --frozen
# --no-sync below: `uv run` would otherwise re-sync and add the dev group back.
out=.faulttest/vm-$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$out"
# Detached (nohup, output to a file): if this SSH session drops, the test carries on and
# its log stays in $out/faulttest.log; this session only follows it.
nohup uv run --no-sync ppm faulttest "$@" >"$out/faulttest.log" 2>&1 </dev/null &
test_pid=$!
# Memory samplers, alive exactly as long as the test: whole-VM RAM and swap in use every
# 10 s, per container every 60 s.
(while kill -0 "$test_pid" 2>/dev/null; do
  free -m | awk -v t="$(date -u +%T)" '/^Mem:/{m=$3} /^Swap:/{s=$3} END{print t, m, s}'
  sleep 10
done >"$out/memory.tsv" 2>/dev/null) &
(while kill -0 "$test_pid" 2>/dev/null; do
  date -u +%T
  docker stats --no-stream --format '{{.Name}} {{.MemUsage}}'
  sleep 60
done >"$out/docker-stats.log" 2>/dev/null) &
# Samplers write only to files (stdout and stderr), so SSH doesn't wait for them to exit.
tail -n +1 -f --pid="$test_pid" "$out/faulttest.log" || true # a dropped session ends only tail
code=0
wait "$test_pid" || code=$?
awk 'BEGIN{m=0; s=0} {if ($2 > m) m = $2; if ($3 > s) s = $3}
     END{print "peak RAM used " m " MB, peak swap " s " MB (" NR " samples, every 10 s)"}' \
  "$out/memory.tsv" | tee "$out/peak.txt"
echo "memory samples in $APP/$out"
exit "$code"
VM
}

# `vm.sh backup-key`, laptop side: AWS's create-access-key JSON (stdin) -> an AWS
# credentials file (stdout). The key ID (not secret) goes to stderr for the summary.
TO_CREDENTIALS_FILE='
import json, sys
key = json.load(sys.stdin)["AccessKey"]
print(key["AccessKeyId"], file=sys.stderr)
print("[default]")
print("aws_access_key_id = " + key["AccessKeyId"])
print("aws_secret_access_key = " + key["SecretAccessKey"])
'

# VM side: store the credentials file from stdin atomically (temp file, then rename),
# readable only by uid 1000, outside the data directory so it can never be backed up.
INSTALL_KEY=$(
  cat <<'VM'
set -eu
umask 077
d=/srv/ppm/secrets
mkdir -p "$d"
chmod 700 "$d"
cat >"$d/.aws-backup.tmp"
if [ ! -s "$d/.aws-backup.tmp" ]; then
  rm -f "$d/.aws-backup.tmp"
  exit 1
fi
chmod 400 "$d/.aws-backup.tmp"
mv -f "$d/.aws-backup.tmp" "$d/aws-backup"
stat -c '%n mode %a uid %u' "$d" "$d/aws-backup"
VM
)

cmd_backup_key() {
  # The secret only ever travels through pipes: never on screen, in shell history, or on
  # the laptop's disk.
  [ ! -t 0 ] || die "pipe the key in: aws iam create-access-key --user-name ppm-backup \
--profile ppm --output json | scripts/vm.sh backup-key"
  local id_file key_id
  id_file=$(mktemp)
  # -I: isolated mode (ignores PYTHON* env vars and modules in the current directory).
  if ! python3 -I -c "$TO_CREDENTIALS_FILE" 2>"$id_file" | on_vm "$INSTALL_KEY"; then
    key_id=$(head -c 200 "$id_file")
    rm -f "$id_file"
    die "install failed. If a key was created, delete it: aws iam delete-access-key \
--user-name ppm-backup --profile ppm --access-key-id <id>  (stderr: $key_id)"
  fi
  key_id=$(cat "$id_file")
  rm -f "$id_file"
  echo "installed access key $key_id on the VM (secret not shown). Next: scripts/vm.sh deploy"
}

cmd_backup_check() {
  run_vm <<'VM'
[ -f "$BACKUP_KEY" ] || { echo "no backup key on the VM: run vm.sh backup-key first" >&2; exit 1; }
live run --rm --no-deps -T backup backup --check-permissions </dev/null
VM
}

cmd=${1:-}
[ $# -gt 0 ] && shift
case "$cmd" in
  bootstrap) cmd_bootstrap ;;
  deploy) cmd_deploy "$@" ;;
  status) cmd_status ;;
  logs) cmd_logs "$@" ;;
  faulttest) cmd_faulttest "$@" ;;
  backup-key) cmd_backup_key ;;
  backup-check) cmd_backup_check ;;
  ssh) on_vm "$@" ;;
  *)
    sed -n '2,15p' "$0" >&2
    exit 2
    ;;
esac
