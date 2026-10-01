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

prefix="${1:?usage: test-proj.sh <thirdparty-installed-prefix>}"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

cat >"$tmp_dir/proj_smoke.cpp" <<'CPP'
#include <proj.h>

#include <cmath>
#include <cstdio>

int main() {
    PJ_CONTEXT* context = proj_context_create();
    if (context == nullptr) return 1;
    proj_context_set_enable_network(context, 0);
    PJ* raw = proj_create_crs_to_crs(context, "EPSG:4326", "EPSG:3857", nullptr);
    if (raw == nullptr) return 2;
    PJ* transform = proj_normalize_for_visualization(context, raw);
    proj_destroy(raw);
    if (transform == nullptr) return 3;
    PJ_COORD result = proj_trans(transform, PJ_FWD, proj_coord(10.0, 20.0, 0.0, 0.0));
    bool valid = std::isfinite(result.xy.x) && std::isfinite(result.xy.y) &&
                 std::abs(result.xy.x - 1113194.90793274) < 0.001 &&
                 std::abs(result.xy.y - 2273030.92698769) < 0.001;
    proj_destroy(transform);
    proj_context_destroy(context);
    if (!valid) return 4;
    std::puts("PROJ embedded CRS database and EPSG:4326 -> EPSG:3857: PASS");
    return 0;
}
CPP

system_libs=(-pthread -lm)
if [[ "$(uname -s)" != Darwin ]]; then
    system_libs+=(-ldl)
fi
cxx="${CXX:-g++}"
"$cxx" -std=c++17 -I"$prefix/include" "$tmp_dir/proj_smoke.cpp" \
    "$prefix/lib/libproj.a" "$prefix/lib/libsqlite3.a" "${system_libs[@]}" \
    -o "$tmp_dir/proj_smoke"
# Rocky9 can use a newer compiler than its system libstdc++. Run the smoke
# test against the runtime paired with the compiler that linked it.
runtime_libstdcpp="$("$cxx" -print-file-name=libstdc++.so.6)"
if [[ -f "$runtime_libstdcpp" ]]; then
    export LD_LIBRARY_PATH="$(dirname "$runtime_libstdcpp"):${LD_LIBRARY_PATH:-}"
fi
PROJ_DATA="$tmp_dir/no-external-proj-data" PROJ_NETWORK=OFF "$tmp_dir/proj_smoke"
