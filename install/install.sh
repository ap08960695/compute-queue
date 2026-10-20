#!/usr/bin/env bash
# Install the compute-queue Node Agent and register this machine with the Coordinator.
# Usage: install.sh [--gpu-lock PATH] <coordinator_url> <invite_code> <node_name>
# --gpu-lock defaults to /tmp/compute-queue-gpu.lock on Linux nodes that have a GPU.
set -euo pipefail

usage() { echo "usage: $0 [--gpu-lock PATH] <coordinator_url> <invite_code> <node_name>" >&2; exit 2; }
lock=; pos=()
while [ $# -gt 0 ]; do
  case $1 in
    --gpu-lock) [ $# -ge 2 ] || usage; lock=$2; shift 2 ;;
    --gpu-lock=*) lock=${1#--gpu-lock=}; shift ;;
    *) pos+=("$1"); shift ;;
  esac
done
[ ${#pos[@]} -eq 3 ] || usage
url=${pos[0]%/}; code=${pos[1]}; name=${pos[2]}
[ -z "$lock" ] || [[ $lock =~ ^/[^[:space:]]+$ ]] || { echo "bad gpu lock path: $lock" >&2; exit 2; }

[[ $url =~ ^https?://[^[:space:]]+$ ]] || { echo "bad coordinator url: $url" >&2; exit 2; }
[[ $name =~ ^[A-Za-z0-9._-]+$ ]] || { echo "bad node name: $name" >&2; exit 2; }

py=$(command -v python3) || { echo "python3 not found" >&2; exit 1; }
"$py" -c 'import sys; sys.exit(sys.version_info < (3, 11))' \
  || { echo "python3 >= 3.11 required" >&2; exit 1; }

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
src=$here/../cq/agent.py
[ -f "$src" ] || { echo "missing $src" >&2; exit 1; }

agent_dir=$HOME/.local/share/compute-queue/agent
conf_dir=$HOME/.config/compute-queue
umask 077
mkdir -p "$agent_dir" "$conf_dir"
chmod 700 "$conf_dir"
install -m 755 "$src" "$agent_dir/agent.py"

case $(uname -s) in
  Linux)  os=linux ;;
  Darwin) os=darwin ;;
  *) echo "unsupported OS: $(uname -s)" >&2; exit 1 ;;
esac

hw=$("$py" "$agent_dir/agent.py" --hw)

if [ -z "$lock" ] && [ "$os" = linux ] \
   && "$py" -c 'import json, sys; sys.exit(not json.loads(sys.argv[1])["gpus"])' "$hw"; then
  lock=/tmp/compute-queue-gpu.lock
fi

# Register and write agent.json (mode 0600). The token only ever travels through this
# python process; it is not put on any command line.
CQ_URL=$url CQ_CODE=$code CQ_NODE=$name CQ_OS=$os CQ_HW=$hw CQ_GPU_LOCK=$lock CQ_CONF=$conf_dir/agent.json \
"$py" - <<'PY'
import json, os, urllib.request
e = os.environ
body = json.dumps({"name": e["CQ_NODE"], "code": e["CQ_CODE"], "hw": json.loads(e["CQ_HW"])}).encode()
req = urllib.request.Request(e["CQ_URL"] + "/nodes/register", data=body,
                             headers={"Content-Type": "application/json"})
with urllib.request.urlopen(req, timeout=15) as r:
    token = json.load(r)["token"]
cfg = {"coordinator": e["CQ_URL"], "token": token, "node": e["CQ_NODE"], "os": e["CQ_OS"]}
if e["CQ_GPU_LOCK"]:
    cfg["gpu_locks"] = [e["CQ_GPU_LOCK"]]
tmp = e["CQ_CONF"] + ".tmp"
fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w") as f:
    json.dump(cfg, f)
os.replace(tmp, e["CQ_CONF"])
PY
chmod 600 "$conf_dir/agent.json"

if [ "$os" = linux ]; then
  unit_dir=$HOME/.config/systemd/user
  mkdir -p "$unit_dir"
  cat > "$unit_dir/cq-agent.service" <<EOF
[Unit]
Description=compute-queue Node Agent
After=network-online.target

[Service]
ExecStart=$py $agent_dir/agent.py
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
EOF
  systemctl --user daemon-reload
  systemctl --user enable --now cq-agent.service
  echo "note: run 'loginctl enable-linger $(id -un)' so the agent keeps running when logged out"
else
  plist=$HOME/Library/LaunchAgents/com.compute-queue.agent.plist
  mkdir -p "$HOME/Library/LaunchAgents"
  cat > "$plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.compute-queue.agent</string>
  <key>ProgramArguments</key>
  <array><string>$py</string><string>$agent_dir/agent.py</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>$agent_dir/agent.log</string>
  <key>StandardErrorPath</key><string>$agent_dir/agent.log</string>
</dict>
</plist>
EOF
  launchctl bootout "gui/$(id -u)/com.compute-queue.agent" 2>/dev/null || true
  launchctl bootstrap "gui/$(id -u)" "$plist"
fi

echo "registered $name as pending; ask the Owner to run: cq node approve $name"
