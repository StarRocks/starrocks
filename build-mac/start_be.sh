#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
BE_DIR="$ROOT_DIR/output/be"
BE_BIN="$BE_DIR/lib/starrocks_be"

if [[ ! -x "$BE_BIN" ]]; then
    echo "[ERROR] BE binary not found or not executable: $BE_BIN" >&2
    echo "[HINT] Build BE first: ./build-mac/build_be.sh" >&2
    exit 1
fi

export ASAN_OPTIONS="detect_container_overflow=0"
export STARROCKS_HOME="$BE_DIR"
export PID_DIR="$BE_DIR/bin"
export UDF_RUNTIME_DIR="$BE_DIR/lib"

BE_CONF="$BE_DIR/conf/be.conf"
if [[ ! -f "$BE_CONF" ]]; then
    echo "[ERROR] BE config not found: $BE_CONF" >&2
    exit 1
fi

conf_has_key() {
    local key="$1"
    local pattern="^[[:space:]]*#?[[:space:]]*${key}[[:space:]]*="

    if command -v rg >/dev/null 2>&1; then
        rg -q "$pattern" "$BE_CONF"
    else
        grep -Eq "$pattern" "$BE_CONF"
    fi
}

set_conf_value() {
    local key="$1"
    local value="$2"

    if conf_has_key "$key"; then
        sed -i '' -E "s|^[[:space:]]*#?[[:space:]]*${key}[[:space:]]*=.*|${key} = ${value}|" "$BE_CONF"
    else
        echo "${key} = ${value}" >> "$BE_CONF"
    fi
}

set_conf_value "priority_networks" "10.10.10.0/24;192.168.0.0/16"
set_conf_value "datacache_enable" "false"
set_conf_value "enable_system_metrics" "false"
set_conf_value "enable_table_metrics" "false"
set_conf_value "enable_collect_table_metrics" "false"
set_conf_value "sys_log_verbose_modules" "*"

mkdir -p "$BE_DIR/log"

"$BE_BIN" &

PID=$!
echo "[INFO] StarRocks BE started (pid: $PID)"
