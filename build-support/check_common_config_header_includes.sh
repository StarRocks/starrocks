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

rg -n '#include "common/config.h"|#include <common/config.h>' "${BE_ROOT}/src" --glob '*.{h,hpp}' \
    | awk -F: '{print $1}' \
    | sed "s#^${BE_ROOT}/##" \
    | sort -u > "${CURRENT_FILE}"

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
