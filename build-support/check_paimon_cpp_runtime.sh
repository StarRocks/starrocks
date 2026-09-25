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

usage() {
    echo "usage: $(basename "$0") <paimon-runtime-dir> <x86_64|aarch64>" >&2
    exit 2
}

[[ $# -eq 2 ]] || usage

runtime_dir=$1
expected_arch=$2
shim=${runtime_dir}/libstarrocks_paimon.so

case ${expected_arch} in
    x86_64 | amd64)
        expected_arch=x86_64
        machine_pattern='Advanced Micro Devices X86-64'
        require_lumina=1
        ;;
    aarch64 | arm64)
        expected_arch=aarch64
        machine_pattern='AArch64'
        require_lumina=0
        ;;
    *)
        echo "unsupported Paimon runtime architecture: ${expected_arch}" >&2
        exit 2
        ;;
esac

for command in readelf ldd; do
    command -v "${command}" >/dev/null 2>&1 || {
        echo "${command} is required to validate the Paimon runtime package" >&2
        exit 2
    }
done

[[ -d ${runtime_dir} ]] || {
    echo "Paimon runtime directory does not exist: ${runtime_dir}" >&2
    exit 1
}
[[ -f ${shim} ]] || {
    echo "Paimon runtime shim does not exist: ${shim}" >&2
    exit 1
}

dynamic_section() {
    readelf -d "$1"
}

needed_entries() {
    dynamic_section "$1" | sed -n 's/.*Shared library: \[\([^]]*\)\].*/\1/p'
}

require_needed() {
    local elf=$1
    local library=$2
    if ! needed_entries "${elf}" | grep -Fxq "${library}"; then
        echo "${elf} does not declare required DT_NEEDED entry ${library}" >&2
        exit 1
    fi
}

check_elf() {
    local elf=$1
    local header
    local absolute_needed

    header=$(readelf -h "${elf}")
    if ! grep -Fq "${machine_pattern}" <<<"${header}"; then
        echo "${elf} is not a ${expected_arch} ELF binary" >&2
        exit 1
    fi

    absolute_needed=$(needed_entries "${elf}" | grep '/' || true)
    if [[ -n ${absolute_needed} ]]; then
        echo "${elf} contains absolute DT_NEEDED entries:" >&2
        echo "${absolute_needed}" >&2
        exit 1
    fi
}

shopt -s nullglob
runtime_elfs=("${runtime_dir}"/libstarrocks_paimon.so "${runtime_dir}"/libpaimon*.so* "${runtime_dir}"/liblumina.so*)
for elf in "${runtime_elfs[@]}"; do
    [[ -L ${elf} ]] && continue
    check_elf "${elf}"
done

shim_dynamic=$(dynamic_section "${shim}")
if ! grep -Eq '\((RPATH|RUNPATH)\)' <<<"${shim_dynamic}" || ! grep -Fq '$ORIGIN' <<<"${shim_dynamic}"; then
    echo "${shim} must carry an \$ORIGIN RPATH or RUNPATH" >&2
    exit 1
fi

require_needed "${shim}" libpaimon.so
require_needed "${shim}" libpaimon_global_index.so

if (( require_lumina == 1 )); then
    lumina_plugin=${runtime_dir}/libpaimon_lumina_index.so
    lumina_runtime=${runtime_dir}/liblumina.so
    [[ -f ${lumina_plugin} ]] || {
        echo "x86-64 Paimon runtime is missing ${lumina_plugin}" >&2
        exit 1
    }
    [[ -f ${lumina_runtime} ]] || {
        echo "x86-64 Paimon runtime is missing ${lumina_runtime}" >&2
        exit 1
    }
    require_needed "${shim}" libpaimon_lumina_index.so
    require_needed "${shim}" liblumina.so
    require_needed "${lumina_plugin}" liblumina.so
else
    arm_lumina_files=("${runtime_dir}"/libpaimon_lumina_index.so* "${runtime_dir}"/liblumina.so*)
    if (( ${#arm_lumina_files[@]} > 0 )); then
        echo "aarch64 Paimon runtime must not package x86-only Lumina binaries" >&2
        printf '  %s\n' "${arm_lumina_files[@]}" >&2
        exit 1
    fi
    if needed_entries "${shim}" | grep -Eq '^lib(paimon_lumina_index|lumina)\.so'; then
        echo "aarch64 Paimon shim must not depend on x86-only Lumina binaries" >&2
        exit 1
    fi
fi

while IFS= read -r bundled_needed; do
    case ${bundled_needed} in
        libpaimon*.so* | liblumina.so*)
            [[ -e ${runtime_dir}/${bundled_needed} ]] || {
                echo "${shim} depends on missing bundled library ${bundled_needed}" >&2
                exit 1
            }
            ;;
    esac
done < <(needed_entries "${shim}")

relocated_dir=$(mktemp -d "${TMPDIR:-/tmp}/starrocks-paimon-runtime.XXXXXX")
trap 'rm -rf "${relocated_dir}"' EXIT
cp -a "${runtime_dir}/." "${relocated_dir}/"

ldd_output=$(env -u LD_LIBRARY_PATH ldd "${relocated_dir}/libstarrocks_paimon.so" 2>&1) || {
    echo "relocated Paimon runtime dependency resolution failed:" >&2
    echo "${ldd_output}" >&2
    exit 1
}
if grep -Fq 'not found' <<<"${ldd_output}"; then
    echo "relocated Paimon runtime has unresolved dependencies:" >&2
    echo "${ldd_output}" >&2
    exit 1
fi
while IFS= read -r bundled_needed; do
    case ${bundled_needed} in
        libpaimon*.so* | liblumina.so*)
            if ! grep -Fq "${relocated_dir}/${bundled_needed}" <<<"${ldd_output}"; then
                echo "relocated ${bundled_needed} did not resolve from ${relocated_dir}:" >&2
                echo "${ldd_output}" >&2
                exit 1
            fi
            ;;
    esac
done < <(needed_entries "${shim}")

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
"${script_dir}/check_glibc_abi.sh" "${runtime_dir}"

echo "Paimon runtime package check passed for ${expected_arch}: ${runtime_dir}"
