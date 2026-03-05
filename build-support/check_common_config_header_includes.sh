#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BE_ROOT="${REPO_ROOT}/be"
ALLOWLIST_FILE="${1:-${REPO_ROOT}/build-support/config_header_include_allowlist.txt}"

if [[ ! -f "${ALLOWLIST_FILE}" ]]; then
    echo "allowlist file not found: ${ALLOWLIST_FILE}" >&2
    exit 2
fi

CURRENT_FILE="$(mktemp)"
ALLOWLIST_TMP_FILE="$(mktemp)"
trap 'rm -f "${CURRENT_FILE}" "${ALLOWLIST_TMP_FILE}"' EXIT

INCLUDE_PATTERN='#include[[:space:]]*("common/config.h"|<common/config.h>)'

if command -v rg >/dev/null 2>&1; then
    rg -l "${INCLUDE_PATTERN}" "${BE_ROOT}/src" --glob '*.{h,hpp}' \
        | sed "s#^${BE_ROOT}/##" \
        | sort -u > "${CURRENT_FILE}"
else
    # Some CI images do not have ripgrep preinstalled.
    find "${BE_ROOT}/src" -type f \( -name '*.h' -o -name '*.hpp' \) -print0 \
        | while IFS= read -r -d '' file; do
            if grep -Eq "${INCLUDE_PATTERN}" "${file}"; then
                printf '%s\n' "${file}"
            fi
        done \
        | sed "s#^${BE_ROOT}/##" \
        | sort -u > "${CURRENT_FILE}"
fi

awk '
    /^[[:space:]]*#/ {next}
    /^[[:space:]]*$/ {next}
    {print}
' "${ALLOWLIST_FILE}" | sort -u > "${ALLOWLIST_TMP_FILE}"

EXTRA_HEADERS="$(comm -23 "${CURRENT_FILE}" "${ALLOWLIST_TMP_FILE}")"
HEADER_COUNT="$(wc -l < "${CURRENT_FILE}" | tr -d '[:space:]')"

if [[ -n "${EXTRA_HEADERS}" ]]; then
    echo "Found headers that newly include common/config.h outside the allowlist:" >&2
    echo "${EXTRA_HEADERS}" >&2
    exit 1
fi

echo "OK: all direct header includes of common/config.h are within allowlist (${HEADER_COUNT} headers)."
