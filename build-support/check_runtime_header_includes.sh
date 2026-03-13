#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BE_ROOT="${REPO_ROOT}/be"
RUNTIME_STATE_ALLOWLIST="${REPO_ROOT}/build-support/runtime_state_header_include_allowlist.txt"
EXEC_ENV_ALLOWLIST="${REPO_ROOT}/build-support/exec_env_header_include_allowlist.txt"

check_allowlist() {
    local include_pattern="$1"
    local allowlist_file="$2"
    local label="$3"

    if [[ ! -f "${allowlist_file}" ]]; then
        echo "allowlist file not found: ${allowlist_file}" >&2
        exit 2
    fi

    local current_file
    local allowlist_tmp_file
    current_file="$(mktemp)"
    allowlist_tmp_file="$(mktemp)"
    trap 'rm -f "${current_file}" "${allowlist_tmp_file}"' RETURN

    if command -v rg >/dev/null 2>&1; then
        rg -l "${include_pattern}" "${BE_ROOT}/src" "${BE_ROOT}/test" --glob '*.{h,hh,hpp,inl}' \
            | sed "s#^${BE_ROOT}/##" \
            | sort -u > "${current_file}"
    else
        find "${BE_ROOT}/src" "${BE_ROOT}/test" -type f \
            \( -name '*.h' -o -name '*.hh' -o -name '*.hpp' -o -name '*.inl' \) -print0 \
            | while IFS= read -r -d '' file; do
                if grep -Eq "${include_pattern}" "${file}"; then
                    printf '%s\n' "${file}"
                fi
            done \
            | sed "s#^${BE_ROOT}/##" \
            | sort -u > "${current_file}"
    fi

    awk '
        /^[[:space:]]*#/ {next}
        /^[[:space:]]*$/ {next}
        {print}
    ' "${allowlist_file}" | sort -u > "${allowlist_tmp_file}"

    local extra_headers
    extra_headers="$(comm -23 "${current_file}" "${allowlist_tmp_file}")"
    local file_count
    file_count="$(wc -l < "${current_file}" | tr -d '[:space:]')"

    if [[ -n "${extra_headers}" ]]; then
        echo "Found headers that directly include ${label} outside the allowlist:" >&2
        echo "${extra_headers}" >&2
        exit 1
    fi

    echo "OK: all direct header includes of ${label} are within allowlist (${file_count} files)."
}

check_allowlist '#include[[:space:]]*("runtime/runtime_state.h"|<runtime/runtime_state.h>)' \
    "${RUNTIME_STATE_ALLOWLIST}" "runtime/runtime_state.h"
check_allowlist '#include[[:space:]]*("runtime/exec_env.h"|<runtime/exec_env.h>)' \
    "${EXEC_ENV_ALLOWLIST}" "runtime/exec_env.h"
