#!/bin/sh
# First-boot setup for the ppm recording VM (Lightsail runs this once, as root).
#
# Installs Docker, adds swap, and creates the data directories. The app itself is
# deployed separately (Stage 2b: git clone + docker compose up). Output goes to
# /var/log/ppm-bootstrap.log; /var/lib/ppm-bootstrap.done marks success.
#
# POSIX sh only, no bash features: Lightsail prepends its own `#!/bin/sh` launch script
# to this one, so our shebang becomes a comment and the whole file runs under dash.
# (First version used `set -o pipefail`; dash rejected it and nothing ran. 2026-11-13.)
set -eux
exec >/var/log/ppm-bootstrap.log 2>&1

export DEBIAN_FRONTEND=noninteractive

timedatectl set-timezone UTC

# 2 GB swap: building the C++ extension inside `docker compose build` can exceed 2 GB RAM
# briefly; swap turns an out-of-memory kill into a slower build.
if [ ! -f /swapfile ]; then
  fallocate -l 2G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >>/etc/fstab
fi

# Security updates install automatically every day.
# DPkg::Lock::Timeout: on first boot Ubuntu's own apt-daily/unattended-upgrades jobs can
# hold the package lock; wait up to 10 min for it instead of failing (set -e would abort
# the whole bootstrap, and it never re-runs on this VM).
apt-get -o DPkg::Lock::Timeout=600 update
apt-get -o DPkg::Lock::Timeout=600 install -y ca-certificates curl unattended-upgrades
cat >/etc/apt/apt.conf.d/20auto-upgrades <<'EOF'
APT::Periodic::Update-Package-Lists "1";
APT::Periodic::Unattended-Upgrade "1";
EOF

# Docker Engine + Compose plugin from Docker's own apt repository (Ubuntu's docker.io
# package lags behind and has no compose v2 plugin). The repository's signing key is
# pinned via signed-by, so apt only accepts packages signed by Docker.
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
# shellcheck source=/dev/null  # exists only on the VM; provides VERSION_CODENAME
. /etc/os-release
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" \
  >/etc/apt/sources.list.d/docker.list
apt-get -o DPkg::Lock::Timeout=600 update
apt-get -o DPkg::Lock::Timeout=600 install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
systemctl enable --now docker

# Let the login user run docker without sudo.
usermod -aG docker ubuntu

# Data and spool directories, owned by uid 1000: the containers run as uid 1000
# (Dockerfile), and so does the `ubuntu` login user on Lightsail's Ubuntu image.
install -d -o 1000 -g 1000 -m 0755 /srv/ppm /srv/ppm/data /srv/ppm/spool

# Lightsail's prepended part adds its SSH CA (used by browser SSH, our fallback when the
# owner's IP changes) to sshd_config, then runs `service sshd restart`, which fails on
# Ubuntu 24.04 because the unit is called `ssh`. Restart it ourselves so the CA loads.
systemctl restart ssh

touch /var/lib/ppm-bootstrap.done
