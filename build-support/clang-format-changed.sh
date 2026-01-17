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

##############################################################
# This script runs clang-format only on C++ files changed
# since origin/main. If origin/main is unavailable, it formats
# all C++ files in be/src and be/test.
##############################################################

set -eo pipefail

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

export STARROCKS_HOME=`cd "${ROOT}/.."; pwd`

CLANG_FORMAT=${CLANG_FORMAT_BINARY:=$(which clang-format)}
EXCLUDES_FILE="${STARROCKS_HOME}/build-support/excludes"
SOURCE_DIRS=("${STARROCKS_HOME}/be/src" "${STARROCKS_HOME}/be/test")

if ! git -C "${STARROCKS_HOME}" rev-parse --verify origin/main >/dev/null 2>&1; then
    echo "origin/main not found; formatting all C++ files."
    python3 "${STARROCKS_HOME}/build-support/run_clang_format.py" --clang_format_binary="${CLANG_FORMAT}" --fix \
        --source_dirs="${SOURCE_DIRS[0]}","${SOURCE_DIRS[1]}" \
        --exclude_globs="${EXCLUDES_FILE}"
    exit 0
fi

changed_files=$(
    {
        git -C "${STARROCKS_HOME}" diff --name-only --diff-filter=ACMR origin/main...HEAD
        git -C "${STARROCKS_HOME}" diff --name-only --diff-filter=ACMR --cached -- be
        git -C "${STARROCKS_HOME}" diff --name-only --diff-filter=ACMR -- be
    } | sort -u
)
if [[ -z "${changed_files}" ]]; then
    echo "No files changed since origin/main."
    exit 0
fi

tmpfile=$(mktemp)
trap 'rm -f "${tmpfile}"' EXIT

python3 "${STARROCKS_HOME}/build-support/format_changed_files.py" \
    --repo_root "${STARROCKS_HOME}" \
    --exclude_globs "${EXCLUDES_FILE}" \
    --source_dirs "${SOURCE_DIRS[0]}","${SOURCE_DIRS[1]}" \
    --null < <(printf '%s\n' "${changed_files}") > "${tmpfile}"

if [[ ! -s "${tmpfile}" ]]; then
    echo "No changed C++ files to format since origin/main."
    exit 0
fi

echo "C++ files to format:"
while IFS= read -r -d '' filename; do
    echo "${filename}"
done < "${tmpfile}"

xargs -0 -n 16 "${CLANG_FORMAT}" -style=file -i < "${tmpfile}"
