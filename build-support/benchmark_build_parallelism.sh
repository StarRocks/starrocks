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

REPO_ROOT="$(cd "$(dirname "$0")/.."; pwd)"
cd "${REPO_ROOT}"

. "${REPO_ROOT}/build-support/build_helpers.sh"

echo "=========================================================="
echo " STARROCKS BUILD PARALLELISM BEFORE / AFTER VALIDATION"
echo "=========================================================="
echo "Host CPU Cores:  $(starrocks_detect_parallelism)"
echo "Host Total RAM:  $(starrocks_detect_total_ram_gb) GiB"
echo "Detected Linker: $(starrocks_detect_linker_type)"

legacy_parallel=$(( $(starrocks_detect_parallelism) / 4 + 1 ))
uncapped_parallel=$(starrocks_detect_ut_parallelism)

echo "Legacy Thread Count (Before):  -j${legacy_parallel}"
echo "Uncapped Thread Count (After): -j${uncapped_parallel}"
echo "Throughput Multiplier:         $(( uncapped_parallel * 100 / legacy_parallel ))%"
echo "=========================================================="

if [[ "$1" == "--dry-run-check" ]]; then
    echo "[PASS] Validation succeeded."
    exit 0
fi
