#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
FE_DIR="$ROOT_DIR/output/fe"
FE_START="$FE_DIR/bin/start_fe.sh"

if [[ ! -x "$FE_START" ]]; then
    echo "[ERROR] FE start script not found or not executable: $FE_START" >&2
    echo "[HINT] Build FE first: ./build.sh --fe" >&2
    exit 1
fi

FE_CONF="$FE_DIR/conf/fe.conf"
if [[ ! -f "$FE_CONF" ]]; then
    echo "[ERROR] FE config not found: $FE_CONF" >&2
    exit 1
fi

conf_has_key() {
    local key="$1"
    local pattern="^[[:space:]]*#?[[:space:]]*${key}[[:space:]]*="

    if command -v rg >/dev/null 2>&1; then
        rg -q "$pattern" "$FE_CONF"
    else
        grep -Eq "$pattern" "$FE_CONF"
    fi
}

set_conf_value() {
    local key="$1"
    local value="$2"

    if conf_has_key "$key"; then
        sed -i '' -E "s|^[[:space:]]*#?[[:space:]]*${key}[[:space:]]*=.*|${key} = ${value}|" "$FE_CONF"
    else
        echo "${key} = ${value}" >> "$FE_CONF"
    fi
}

set_conf_value "priority_networks" "10.10.10.0/24;192.168.0.0/16"
set_conf_value "default_replication_num" "1"
set_conf_value "bdbje_reset_election_group" "true"

cd "$FE_DIR"
"$FE_START" "$@"
