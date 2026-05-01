#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BE_SRC="${REPO_ROOT}/be/src"

check_allowlist() {
    local label="$1"
    local include_pattern="$2"
    local allowlist_file="$3"

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
        rg -l "${include_pattern}" "${BE_SRC}" --glob '*.{h,hpp,cc,cpp}' \
            | sed "s#^${REPO_ROOT}/##" \
            | sort -u > "${current_file}"
    else
        find "${BE_SRC}" -type f \
            \( -name '*.h' -o -name '*.hpp' -o -name '*.cc' -o -name '*.cpp' \) -print0 \
            | while IFS= read -r -d '' file; do
                if grep -Eq "${include_pattern}" "${file}"; then
                    printf '%s\n' "${file}"
                fi
            done \
            | sed "s#^${REPO_ROOT}/##" \
            | sort -u > "${current_file}"
    fi

    awk '
        /^[[:space:]]*#/ {next}
        /^[[:space:]]*$/ {next}
        {print}
    ' "${allowlist_file}" | sort -u > "${allowlist_tmp_file}"

    local extra_headers
    local stale_headers
    extra_headers="$(comm -23 "${current_file}" "${allowlist_tmp_file}")"
    stale_headers="$(comm -13 "${current_file}" "${allowlist_tmp_file}")"

    if [[ -n "${extra_headers}" ]]; then
        echo "Found new production includes of ${label} outside the shrink-only allowlist:" >&2
        echo "${extra_headers}" >&2
        echo "Remove the dependency or add it only after explicit architecture review." >&2
        exit 1
    fi

    if [[ -n "${stale_headers}" ]]; then
        echo "Found stale ${label} allowlist entries. Remove fixed debt from ${allowlist_file}:" >&2
        echo "${stale_headers}" >&2
        exit 1
    fi

    local file_count
    file_count="$(wc -l < "${current_file}" | tr -d '[:space:]')"
    echo "OK: ${label} production include allowlist matches current tree (${file_count} files)."
}

check_allowlist \
    "runtime/starrocks_metrics.h" \
    '#include[[:space:]]*[<"]runtime/starrocks_metrics\.h[>"]' \
    "${REPO_ROOT}/build-support/starrocks_metrics_include_allowlist.txt"

check_allowlist \
    "util/global_metrics_registry.h" \
    '#include[[:space:]]*[<"]util/global_metrics_registry\.h[>"]' \
    "${REPO_ROOT}/build-support/global_metrics_registry_include_allowlist.txt"
