#!/usr/bin/env bash
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

readonly BE_DIR="/home/disk1/sr/be"
readonly LOCAL_BE_BINARY="${LOCAL_BE_BINARY:-/home/disk3/liang/starrocks/output/be/lib/starrocks_be}"
readonly SSH_USER="sr"
readonly SSH_PASSWORD="${SSH_PASSWORD:-sr@test}"
readonly FE_HOST="${FE_HOST:-172.26.95.250}"
readonly FE_QUERY_PORT="${FE_QUERY_PORT:-9030}"
readonly FE_MYSQL_USER="${FE_MYSQL_USER:-root}"
readonly SSH_OPTS=(
    -o StrictHostKeyChecking=no
    -o UserKnownHostsFile=/dev/null
    -o ConnectTimeout=8
    -o NumberOfPasswordPrompts=1
)
readonly BE_HOSTS=(
    "172.26.95.251"
    "172.26.95.252"
    "172.26.80.1"
)

usage() {
    cat <<'EOF'
Usage:
  tools/be_cluster_ctl.sh start
  tools/be_cluster_ctl.sh stop
  tools/be_cluster_ctl.sh restart
  tools/be_cluster_ctl.sh status
  tools/be_cluster_ctl.sh deploy

Actions:
  start    Start all three BE nodes
  stop     Stop all three BE nodes
  restart  Stop then start all three BE nodes
  status   Show remote BE process status
  deploy   Copy local starrocks_be to all remote BE lib directories

Password:
  Default password is built in.
  Override with: SSH_PASSWORD='your_password' tools/be_cluster_ctl.sh start

Binary:
  Default local binary: /home/disk3/liang/starrocks/output/be/lib/starrocks_be
  Override with: LOCAL_BE_BINARY='/path/to/starrocks_be' tools/be_cluster_ctl.sh deploy

FE readiness:
  The start action optionally checks `SHOW BACKENDS` using:
    FE_HOST=${FE_HOST}
    FE_QUERY_PORT=${FE_QUERY_PORT}
    FE_MYSQL_USER=${FE_MYSQL_USER}
EOF
}

run_with_password() {
    if [[ "${USE_EXPECT:-0}" == "1" ]] && command -v expect >/dev/null 2>&1; then
        env SSH_PASSWORD="${SSH_PASSWORD}" expect -c '
            set timeout 30
            set password $env(SSH_PASSWORD)

            spawn -noecho {*}$argv
            expect {
                -re "(?i)continue connecting" {
                    send "yes\r"
                    exp_continue
                }
                -re "(?i)(password|passphrase).*:" {
                    send "$password\r"
                    exp_continue
                }
                eof
                timeout {
                    exit 124
                }
            }

            catch wait result
            exit [lindex $result 3]
        ' -- "$@"
        return
    fi

    env SSH_PASSWORD="${SSH_PASSWORD}" python3 - "$@" <<'PY'
import os
import pty
import select
import subprocess
import sys

password = os.environ["SSH_PASSWORD"] + "\n"
argv = sys.argv[1:]

master_fd, slave_fd = pty.openpty()
proc = subprocess.Popen(argv, stdin=slave_fd, stdout=slave_fd, stderr=slave_fd, close_fds=True)
os.close(slave_fd)

sent_password = False
buffer = ""

try:
    while True:
        if proc.poll() is not None:
            break

        ready, _, _ = select.select([master_fd], [], [], 0.2)
        if master_fd not in ready:
            continue

        try:
            data = os.read(master_fd, 4096)
        except OSError:
            break

        if not data:
            break

        text = data.decode(errors="ignore")
        sys.stdout.write(text)
        sys.stdout.flush()
        buffer = (buffer + text)[-4096:]
        lower = buffer.lower()

        if "continue connecting" in lower:
            os.write(master_fd, b"yes\n")
            buffer = ""
            continue

        if ("password" in lower or "passphrase" in lower) and not sent_password:
            os.write(master_fd, password.encode())
            sent_password = True
            buffer = ""

    while True:
        try:
            data = os.read(master_fd, 4096)
        except OSError:
            break
        if not data:
            break
        sys.stdout.write(data.decode(errors="ignore"))
        sys.stdout.flush()
finally:
    os.close(master_fd)

sys.exit(proc.wait())
PY
}

run_remote() {
    local host="$1"
    local cmd="$2"
    run_with_password ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "${cmd}"
}

copy_to_remote() {
    local host="$1"
    local local_path="$2"
    local remote_path="$3"
    run_with_password scp "${SSH_OPTS[@]}" "${local_path}" "${SSH_USER}@${host}:${remote_path}"
}

run_parallel() {
    local action="$1"
    shift

    local pids=()
    local hosts=("$@")
    local failed=0

    for host in "${hosts[@]}"; do
        {
            echo "[${host}] ${action}"
            run_remote "${host}" "cd \"${BE_DIR}\" && ${action}"
            echo "[${host}] done"
        } &
        pids+=("$!")
    done

    for pid in "${pids[@]}"; do
        if ! wait "${pid}"; then
            failed=1
        fi
    done

    return "${failed}"
}

start_all() {
    local start_action
    start_action="if command -v setsid >/dev/null 2>&1; then setsid -f bin/start_be.sh >/tmp/starrocks_be_start.log 2>&1 </dev/null; else nohup bin/start_be.sh >/tmp/starrocks_be_start.log 2>&1 </dev/null & fi"
    run_parallel "${start_action}" "${BE_HOSTS[@]}"
}

check_remote_be_process() {
    local host="$1"
    run_remote "${host}" "ps -ef | awk '/starrocks_be|be\\.conf/ && !/awk/ {found=1} END {exit(found ? 0 : 1)}'" \
        >/dev/null
}

wait_for_remote_processes() {
    local attempts="${1:-15}"
    local sleep_seconds="${2:-1}"
    local failed=0

    for ((attempt = 1; attempt <= attempts; attempt++)); do
        failed=0
        for host in "${BE_HOSTS[@]}"; do
            if ! check_remote_be_process "${host}"; then
                failed=1
                break
            fi
        done
        if [[ "${failed}" -eq 0 ]]; then
            echo "All BE processes are up."
            return 0
        fi
        sleep "${sleep_seconds}"
    done

    echo "Timed out waiting for BE processes to start." >&2
    return 1
}

check_backends_alive_via_fe() {
    if ! command -v mysql >/dev/null 2>&1; then
        return 2
    fi

    local output
    if ! output=$(mysql -h"${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_MYSQL_USER}" --connect-timeout=2 \
        --batch --raw --skip-column-names -e "SHOW BACKENDS" 2>/dev/null); then
        return 2
    fi

    BACKENDS_OUTPUT="${output}" python3 - "${BE_HOSTS[@]}" <<'PY'
import sys
import os

expected = set(sys.argv[1:])
alive = set()
for line in os.environ.get("BACKENDS_OUTPUT", "").splitlines():
    fields = line.rstrip("\n").split("\t")
    if len(fields) <= 8:
        continue
    host = fields[1]
    is_alive = fields[8].lower() == "true"
    if host in expected and is_alive:
        alive.add(host)

sys.exit(0 if alive == expected else 1)
PY
}

wait_for_backends_alive_via_fe() {
    local attempts="${1:-12}"
    local sleep_seconds="${2:-1}"

    for ((attempt = 1; attempt <= attempts; attempt++)); do
        if check_backends_alive_via_fe; then
            echo "SHOW BACKENDS reports all BEs alive."
            return 0
        fi
        sleep "${sleep_seconds}"
    done

    echo "SHOW BACKENDS did not report all BEs alive within the short wait window." >&2
    return 1
}

stop_all() {
    run_parallel "bin/stop_be.sh" "${BE_HOSTS[@]}"
}

status_all() {
    local failed=0
    for host in "${BE_HOSTS[@]}"; do
        echo "[${host}]"
        if ! run_remote "${host}" \
            "ps -ef | awk '/starrocks_be|be\\.conf/ && !/awk/ {print}'"; then
            failed=1
        fi
        echo
    done
    return "${failed}"
}

deploy_binary() {
    local failed=0
    local pids=()

    if [[ ! -f "${LOCAL_BE_BINARY}" ]]; then
        echo "Local binary not found: ${LOCAL_BE_BINARY}" >&2
        return 1
    fi

    for host in "${BE_HOSTS[@]}"; do
        {
            echo "[${host}] deploy ${LOCAL_BE_BINARY}"
            run_remote "${host}" "mkdir -p \"${BE_DIR}/lib\""
            copy_to_remote "${host}" "${LOCAL_BE_BINARY}" "${BE_DIR}/lib/starrocks_be"
            echo "[${host}] done"
        } &
        pids+=("$!")
    done

    for pid in "${pids[@]}"; do
        if ! wait "${pid}"; then
            failed=1
        fi
    done

    return "${failed}"
}

main() {
    if [[ $# -ne 1 ]]; then
        usage
        exit 1
    fi

    case "$1" in
        start)
            start_all
            wait_for_remote_processes
            wait_for_backends_alive_via_fe || true
            ;;
        stop)
            stop_all
            ;;
        restart)
            stop_all
            echo "Waiting 3 seconds before start..."
            sleep 3
            start_all
            ;;
        status)
            status_all
            ;;
        deploy)
            deploy_binary
            ;;
        -h|--help|help)
            usage
            ;;
        *)
            usage
            exit 1
            ;;
    esac
}

main "$@"
