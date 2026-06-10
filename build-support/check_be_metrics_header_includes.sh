#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BE_SRC="${REPO_ROOT}/be/src"

check_absent_pattern() {
    local label="$1"
    local pattern="$2"

    local matches=""
    if command -v rg >/dev/null 2>&1; then
        local rg_status
        set +e
        matches="$(rg -n "${pattern}" "${BE_SRC}" --glob '*.{h,hpp,cc,cpp}')"
        rg_status=$?
        set -e
        if [[ "${rg_status}" != "0" && "${rg_status}" != "1" ]]; then
            echo "${matches}" >&2
            exit "${rg_status}"
        fi
    else
        matches="$(
            find "${BE_SRC}" -type f \
                \( -name '*.h' -o -name '*.hpp' -o -name '*.cc' -o -name '*.cpp' \) -print0 \
                | while IFS= read -r -d '' file; do
                    if grep -Eq "${pattern}" "${file}"; then
                        grep -En "${pattern}" "${file}" | sed "s#^#${file}:#"
                    fi
                done
        )"
    fi

    if [[ -n "${matches}" ]]; then
        echo "Found forbidden production use of ${label}:" >&2
        echo "${matches}" | sed "s#${REPO_ROOT}/##" >&2
        exit 1
    fi

    echo "OK: no production use of ${label}."
}

check_absent_pattern \
    "runtime/starrocks_metrics.h" \
    '#include[[:space:]]*[<"]runtime/starrocks_metrics\.h[>"]'

check_absent_pattern \
    "util/global_metrics_registry.h" \
    '#include[[:space:]]*[<"]util/global_metrics_registry\.h[>"]'

check_absent_pattern \
    "GlobalMetricsRegistry facade" \
    '(^|[^[:alnum:]_])GlobalMetricsRegistry([^[:alnum:]_]|$)'
