#!/usr/bin/env bash
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [[ -n "${STARROCKS_BUILD_HELPERS_LOADED:-}" ]]; then
    return 0
fi
export STARROCKS_BUILD_HELPERS_LOADED=1

starrocks_is_darwin() {
    [[ "${OSTYPE:-}" == darwin* ]]
}

starrocks_detect_parallelism() {
    local cpu_count=""

    if starrocks_is_darwin; then
        cpu_count="$(sysctl -n hw.ncpu 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
    else
        cpu_count="$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
    fi

    if [[ -z "${cpu_count}" || ! "${cpu_count}" =~ ^[0-9]+$ || "${cpu_count}" -lt 1 ]]; then
        cpu_count=1
    fi

    if ! starrocks_is_darwin; then
        local cgroup_root="${STARROCKS_CGROUP_ROOT:-/sys/fs/cgroup}"
        local proc_cgroup="${STARROCKS_PROC_SELF_CGROUP:-/proc/self/cgroup}"
        local v2_subpath=""
        local v1_cpu_subpath=""
        if [[ -r "${proc_cgroup}" ]]; then
            local cg_id="" cg_ctrl="" cg_path=""
            while IFS=: read -r cg_id cg_ctrl cg_path || [[ -n "${cg_id}" ]]; do
                cg_path="${cg_path% (deleted)}"
                while [[ "${cg_path}" == */ ]]; do
                    cg_path="${cg_path%/}"
                done
                if [[ -n "${cg_path}" && "${cg_path}" != /* ]]; then
                    cg_path="/${cg_path}"
                fi
                if [[ "${cg_id}" == "0" && -z "${cg_ctrl}" ]]; then
                    v2_subpath="${cg_path}"
                elif [[ ",${cg_ctrl}," == *,cpu,* ]]; then
                    v1_cpu_subpath="${cg_path}"
                fi
            done < "${proc_cgroup}"
        fi

        local cgroup_cores=0

        # 1. cgroup v2: walk leaf-to-root cpu.max ("quota period", e.g. "200000 100000" or "max 100000")
        local cur_subpath="${v2_subpath}"
        while true; do
            local cpu_max_file="${cgroup_root}${cur_subpath}/cpu.max"
            local cpu_max_content=""
            if [[ -z "${cur_subpath}" ]]; then
                cpu_max_content="$(cat "${cpu_max_file}" 2>/dev/null || true)"
            elif [[ -r "${cpu_max_file}" ]]; then
                cpu_max_content="$(cat "${cpu_max_file}" 2>/dev/null || true)"
            fi
            if [[ -n "${cpu_max_content}" ]]; then
                local cfs_quota="" cfs_period=""
                read -r cfs_quota cfs_period <<< "${cpu_max_content}"
                if [[ -n "${cfs_quota}" && "${cfs_quota}" =~ ^[0-9]+$ && "${cfs_quota}" -gt 0 && \
                      -n "${cfs_period}" && "${cfs_period}" =~ ^[0-9]+$ && "${cfs_period}" -gt 0 ]]; then
                    local level_cores=$(( cfs_quota / cfs_period ))
                    [[ "${level_cores}" -lt 1 ]] && level_cores=1
                    if [[ "${cgroup_cores}" -le 0 || "${level_cores}" -lt "${cgroup_cores}" ]]; then
                        cgroup_cores="${level_cores}"
                    fi
                fi
            fi
            [[ -z "${cur_subpath}" ]] && break
            cur_subpath="${cur_subpath%/*}"
        done

        # 2. cgroup v1: walk leaf-to-root cpu/cpu.cfs_quota_us and cpu/cpu.cfs_period_us (or cpu,cpuacct / cpuacct,cpu)
        cur_subpath="${v1_cpu_subpath}"
        while true; do
            local ctrl_dir=""
            local found_readable_v1_quota=0
            for ctrl_dir in "${cgroup_root}/cpu" "${cgroup_root}/cpu,cpuacct" "${cgroup_root}/cpuacct,cpu"; do
                local v1_quota="" v1_period=""
                if [[ -r "${ctrl_dir}${cur_subpath}/cpu.cfs_quota_us" ]]; then
                    found_readable_v1_quota=1
                    v1_quota="$(cat "${ctrl_dir}${cur_subpath}/cpu.cfs_quota_us" 2>/dev/null || true)"
                    if [[ -r "${ctrl_dir}${cur_subpath}/cpu.cfs_period_us" ]]; then
                        v1_period="$(cat "${ctrl_dir}${cur_subpath}/cpu.cfs_period_us" 2>/dev/null || echo 100000)"
                    else
                        v1_period=100000
                    fi
                fi
                if [[ -n "${v1_quota}" && "${v1_quota}" =~ ^[0-9]+$ && "${v1_quota}" -gt 0 && \
                      -n "${v1_period}" && "${v1_period}" =~ ^[0-9]+$ && "${v1_period}" -gt 0 ]]; then
                    local v1_level_cores=$(( v1_quota / v1_period ))
                    [[ "${v1_level_cores}" -lt 1 ]] && v1_level_cores=1
                    if [[ "${cgroup_cores}" -le 0 || "${v1_level_cores}" -lt "${cgroup_cores}" ]]; then
                        cgroup_cores="${v1_level_cores}"
                    fi
                fi
            done
            if [[ -z "${cur_subpath}" && "${found_readable_v1_quota}" -eq 0 ]]; then
                local v1_root_quota="" v1_root_period=""
                v1_root_quota="$(cat "${cgroup_root}/cpu/cpu.cfs_quota_us" 2>/dev/null || cat "${cgroup_root}/cpu,cpuacct/cpu.cfs_quota_us" 2>/dev/null || cat "${cgroup_root}/cpuacct,cpu/cpu.cfs_quota_us" 2>/dev/null || true)"
                if [[ -n "${v1_root_quota}" && "${v1_root_quota}" =~ ^[0-9]+$ && "${v1_root_quota}" -gt 0 ]]; then
                    v1_root_period="$(cat "${cgroup_root}/cpu/cpu.cfs_period_us" 2>/dev/null || cat "${cgroup_root}/cpu,cpuacct/cpu.cfs_period_us" 2>/dev/null || cat "${cgroup_root}/cpuacct,cpu/cpu.cfs_period_us" 2>/dev/null || echo 100000)"
                    if [[ -n "${v1_root_period}" && "${v1_root_period}" =~ ^[0-9]+$ && "${v1_root_period}" -gt 0 ]]; then
                        local v1_root_cores=$(( v1_root_quota / v1_root_period ))
                        [[ "${v1_root_cores}" -lt 1 ]] && v1_root_cores=1
                        if [[ "${cgroup_cores}" -le 0 || "${v1_root_cores}" -lt "${cgroup_cores}" ]]; then
                            cgroup_cores="${v1_root_cores}"
                        fi
                    fi
                fi
            fi
            [[ -z "${cur_subpath}" ]] && break
            cur_subpath="${cur_subpath%/*}"
        done

        if [[ "${cgroup_cores}" -gt 0 && "${cgroup_cores}" -lt "${cpu_count}" ]]; then
            cpu_count="${cgroup_cores}"
        fi
    fi

    echo "${cpu_count}"
}

starrocks_detect_total_ram_gb() {
    local ram_kb=""
    if starrocks_is_darwin; then
        local ram_bytes
        ram_bytes="$(sysctl -n hw.memsize 2>/dev/null || true)"
        if [[ -n "${ram_bytes}" && "${ram_bytes}" =~ ^[0-9]+$ && "${ram_bytes}" -gt 0 ]]; then
            local ram_gb=$(( ram_bytes / 1024 / 1024 / 1024 ))
            [[ "${ram_gb}" -lt 1 ]] && ram_gb=1
            echo "${ram_gb}"
            return 0
        fi
    else
        local ram_gb_host=0
        if [[ -r /proc/meminfo ]]; then
            ram_kb="$(awk '/^MemTotal:/ {print $2; exit}' /proc/meminfo 2>/dev/null || true)"
            if [[ -n "${ram_kb}" && "${ram_kb}" =~ ^[0-9]+$ && "${ram_kb}" -gt 0 ]]; then
                ram_gb_host=$(( ram_kb / 1024 / 1024 ))
            fi
        fi

        local cgroup_root="${STARROCKS_CGROUP_ROOT:-/sys/fs/cgroup}"
        local proc_cgroup="${STARROCKS_PROC_SELF_CGROUP:-/proc/self/cgroup}"
        local v2_subpath=""
        local v1_mem_subpath=""
        if [[ -r "${proc_cgroup}" ]]; then
            local cg_id="" cg_ctrl="" cg_path=""
            while IFS=: read -r cg_id cg_ctrl cg_path || [[ -n "${cg_id}" ]]; do
                cg_path="${cg_path% (deleted)}"
                while [[ "${cg_path}" == */ ]]; do
                    cg_path="${cg_path%/}"
                done
                if [[ -n "${cg_path}" && "${cg_path}" != /* ]]; then
                    cg_path="/${cg_path}"
                fi
                if [[ "${cg_id}" == "0" && -z "${cg_ctrl}" ]]; then
                    v2_subpath="${cg_path}"
                elif [[ ",${cg_ctrl}," == *,memory,* ]]; then
                    v1_mem_subpath="${cg_path}"
                fi
            done < "${proc_cgroup}"
        fi

        local best_cgroup_gb=0
        local cur_subpath=""
        local cgroup_bytes=""

        cur_subpath="${v2_subpath}"
        while [[ -n "${cur_subpath}" ]]; do
            if [[ -r "${cgroup_root}${cur_subpath}/memory.max" ]]; then
                cgroup_bytes="$(cat "${cgroup_root}${cur_subpath}/memory.max" 2>/dev/null || true)"
                if [[ -n "${cgroup_bytes}" && "${cgroup_bytes}" =~ ^[0-9]+$ && "${cgroup_bytes}" -gt 0 ]]; then
                    local v2_gb=$(( cgroup_bytes / 1024 / 1024 / 1024 ))
                    if [[ "${v2_gb}" -le 1048576 ]]; then
                        [[ "${v2_gb}" -lt 1 ]] && v2_gb=1
                        if [[ "${best_cgroup_gb}" -le 0 || "${v2_gb}" -lt "${best_cgroup_gb}" ]]; then
                            best_cgroup_gb="${v2_gb}"
                        fi
                    fi
                fi
            fi
            cur_subpath="${cur_subpath%/*}"
        done

        cur_subpath="${v1_mem_subpath}"
        while [[ -n "${cur_subpath}" ]]; do
            if [[ -r "${cgroup_root}/memory${cur_subpath}/memory.limit_in_bytes" ]]; then
                cgroup_bytes="$(cat "${cgroup_root}/memory${cur_subpath}/memory.limit_in_bytes" 2>/dev/null || true)"
                if [[ -n "${cgroup_bytes}" && "${cgroup_bytes}" =~ ^[0-9]+$ && "${cgroup_bytes}" -gt 0 ]]; then
                    local v1_gb=$(( cgroup_bytes / 1024 / 1024 / 1024 ))
                    if [[ "${v1_gb}" -le 1048576 ]]; then
                        [[ "${v1_gb}" -lt 1 ]] && v1_gb=1
                        if [[ "${best_cgroup_gb}" -le 0 || "${v1_gb}" -lt "${best_cgroup_gb}" ]]; then
                            best_cgroup_gb="${v1_gb}"
                        fi
                    fi
                fi
            fi
            cur_subpath="${cur_subpath%/*}"
        done

        cgroup_bytes="$(cat "${cgroup_root}/memory.max" 2>/dev/null || cat "${cgroup_root}/memory/memory.limit_in_bytes" 2>/dev/null || true)"
        if [[ -n "${cgroup_bytes}" && "${cgroup_bytes}" =~ ^[0-9]+$ && "${cgroup_bytes}" -gt 0 ]]; then
            local root_cgroup_gb=$(( cgroup_bytes / 1024 / 1024 / 1024 ))
            # Disregard cgroup v1 unbounded sentinel limits (e.g. >= 1 PiB / 1048576 GiB)
            if [[ "${root_cgroup_gb}" -le 1048576 ]]; then
                [[ "${root_cgroup_gb}" -lt 1 ]] && root_cgroup_gb=1
                if [[ "${best_cgroup_gb}" -le 0 || "${root_cgroup_gb}" -lt "${best_cgroup_gb}" ]]; then
                    best_cgroup_gb="${root_cgroup_gb}"
                fi
            fi
        fi
        if [[ -r "${cgroup_root}/memory/memory.limit_in_bytes" ]]; then
            local root_v1_bytes=""
            root_v1_bytes="$(cat "${cgroup_root}/memory/memory.limit_in_bytes" 2>/dev/null || true)"
            if [[ -n "${root_v1_bytes}" && "${root_v1_bytes}" =~ ^[0-9]+$ && "${root_v1_bytes}" -gt 0 ]]; then
                local root_v1_gb=$(( root_v1_bytes / 1024 / 1024 / 1024 ))
                if [[ "${root_v1_gb}" -le 1048576 ]]; then
                    [[ "${root_v1_gb}" -lt 1 ]] && root_v1_gb=1
                    if [[ "${best_cgroup_gb}" -le 0 || "${root_v1_gb}" -lt "${best_cgroup_gb}" ]]; then
                        best_cgroup_gb="${root_v1_gb}"
                    fi
                fi
            fi
        fi

        if [[ "${best_cgroup_gb}" -gt 0 ]]; then
            if [[ "${ram_gb_host}" -eq 0 || "${best_cgroup_gb}" -lt "${ram_gb_host}" ]]; then
                echo "${best_cgroup_gb}"
                return 0
            fi
        fi

        if [[ "${ram_gb_host}" -gt 0 ]]; then
            echo "${ram_gb_host}"
            return 0
        fi
    fi
    echo 0
}

starrocks_classify_linker_banner() {
    local banner_text="${1:-}"
    local strip_wl_echo="${2:-0}"
    if [[ "${strip_wl_echo}" -eq 1 && -n "${banner_text}" ]]; then
        # Strip collect2's echoed linker invocation command line (which contains --version
        # and full directory paths that could otherwise false-match directory names).
        local filtered_banner=""
        filtered_banner="$(printf '%s\n' "${banner_text}" | grep -v -- '--version' || true)"
        if [[ -n "${filtered_banner}" ]]; then
            banner_text="${filtered_banner}"
        fi
    fi
    case "${banner_text}" in
        *LLD*|*mold*|*GNU\ gold*)
            echo "modern"
            return 0
            ;;
        *GNU\ ld*)
            echo "legacy"
            return 0
            ;;
        *lld*|*gold*)
            echo "modern"
            return 0
            ;;
    esac
    return 1
}

starrocks_classify_linker_spec() {
    local spec="${1:-}"
    local cxx_compiler_bin="${2:-}"
    shift 2 || true
    local driver_flags=("$@")

    local ld_bin=""
    if [[ "${spec}" == */* ]]; then
        if command -v "${spec}" >/dev/null 2>&1; then
            ld_bin="${spec}"
        fi
    elif [[ -n "${spec}" ]]; then
        local compiler_ld=""
        if [[ -n "${cxx_compiler_bin}" ]]; then
            compiler_ld="$("${cxx_compiler_bin}" "${driver_flags[@]+"${driver_flags[@]}"}" -print-prog-name="ld.${spec}" 2>/dev/null || true)"
            if [[ -z "${compiler_ld}" || "${compiler_ld}" == "ld.${spec}" ]] && [[ "${#driver_flags[@]}" -gt 0 ]]; then
                compiler_ld="$("${cxx_compiler_bin}" -print-prog-name="ld.${spec}" 2>/dev/null || true)"
            fi
            if [[ -n "${compiler_ld}" && "${compiler_ld}" != "ld.${spec}" && "${compiler_ld##*/}" == "ld.${spec}" ]] && command -v "${compiler_ld}" >/dev/null 2>&1; then
                ld_bin="${compiler_ld}"
            else
                compiler_ld="$("${cxx_compiler_bin}" "${driver_flags[@]+"${driver_flags[@]}"}" -print-prog-name="${spec}" 2>/dev/null || true)"
                if [[ -z "${compiler_ld}" || "${compiler_ld}" == "${spec}" ]] && [[ "${#driver_flags[@]}" -gt 0 ]]; then
                    compiler_ld="$("${cxx_compiler_bin}" -print-prog-name="${spec}" 2>/dev/null || true)"
                fi
                if [[ -n "${compiler_ld}" && "${compiler_ld}" != "${spec}" && "${compiler_ld##*/}" == "${spec}" ]] && command -v "${compiler_ld}" >/dev/null 2>&1; then
                    ld_bin="${compiler_ld}"
                fi
            fi
        fi
        if [[ -z "${ld_bin}" ]]; then
            if command -v "ld.${spec}" >/dev/null 2>&1; then
                ld_bin="ld.${spec}"
            elif command -v "${spec}" >/dev/null 2>&1; then
                ld_bin="${spec}"
            fi
        fi
    fi

    if [[ -n "${ld_bin}" ]]; then
        local ld_ver=""
        ld_ver="$("${ld_bin}" -v 2>&1 || "${ld_bin}" --version 2>&1 || true)"
        local classified=""
        if classified="$(starrocks_classify_linker_banner "${ld_ver}" 0)"; then
            echo "${classified}"
            return 0
        fi
    fi

    local spec_base="${spec##*/}"
    case "${spec_base}" in
        *bfd*)
            echo "legacy"
            return 0
            ;;
        *lld*|*mold*|*gold*)
            echo "modern"
            return 0
            ;;
    esac
    echo "legacy"
    return 0
}

starrocks_detect_linker_type() {
    local build_dir="${1:-${CMAKE_BUILD_DIR:-}}"
    local explicit_linker="${STARROCKS_LINKER:-}"

    if [[ -z "${explicit_linker}" ]] && starrocks_is_darwin; then
        echo "modern"
        return 0
    fi

    # Stage 1: Resolve active C++ compiler binary and mandatory compiler arguments
    # from CMakeCache.txt, CMakeFiles/*/CMake{C,CXX}Compiler.cmake, paired CC+CXX,
    # STARROCKS_GCC_HOME, STARROCKS_LLVM_HOME, or default g++.
    local cached_c=""
    local cached_cxx=""
    local cached_cxx_arg1=""
    local cached_build_type=""
    local cached_config_cxx_flags=""
    local cached_cxx_link_flags=""
    local cached_exe_linker_flags=""
    local cached_config_exe_linker_flags=""
    local has_cached_exe_linker_flags=0
    local has_cmake_compiler_override=0
    if [[ -n "${build_dir}" ]]; then
        if [[ -r "${build_dir}/CMakeCache.txt" ]]; then
            cached_c="$(sed -n -E 's/^CMAKE_C_COMPILER(:[^=]*)?=(.*)$/\2/p' "${build_dir}/CMakeCache.txt" | tail -n 1)"
            cached_cxx="$(sed -n -E 's/^CMAKE_CXX_COMPILER(:[^=]*)?=(.*)$/\2/p' "${build_dir}/CMakeCache.txt" | tail -n 1)"
            cached_cxx_arg1="$(sed -n -E 's/^CMAKE_CXX_COMPILER_ARG1(:[^=]*)?=(.*)$/\2/p' "${build_dir}/CMakeCache.txt" | tail -n 1)"
            cached_build_type="$(sed -n -E 's/^CMAKE_BUILD_TYPE(:[^=]*)?=(.*)$/\2/p' "${build_dir}/CMakeCache.txt" | tail -n 1)"
            cached_cxx_link_flags="$(sed -n -E 's/^CMAKE_CXX_LINK_FLAGS(:[^=]*)?=(.*)$/\2/p' "${build_dir}/CMakeCache.txt" | tail -n 1)"
            if grep -q -E '^CMAKE_EXE_LINKER_FLAGS(:[^=]*)?=' "${build_dir}/CMakeCache.txt" 2>/dev/null; then
                has_cached_exe_linker_flags=1
                cached_exe_linker_flags="$(sed -n -E 's/^CMAKE_EXE_LINKER_FLAGS(:[^=]*)?=(.*)$/\2/p' "${build_dir}/CMakeCache.txt" | tail -n 1)"
            fi
            local active_config=""
            if [[ -n "${cached_build_type}" ]]; then
                active_config="${cached_build_type}"
            elif [[ -n "${CMAKE_BUILD_TYPE:-${BUILD_TYPE:-}}" ]]; then
                active_config="${CMAKE_BUILD_TYPE:-${BUILD_TYPE:-}}"
            elif [[ "${build_dir%/}" =~ (^|/|_)build_([A-Za-z0-9_]+)$ ]]; then
                active_config="${BASH_REMATCH[2]}"
            else
                active_config="ASAN"
            fi
            active_config="$(printf '%s' "${active_config}" | tr '[:lower:]' '[:upper:]')"
            if [[ "${active_config}" =~ ^[A-Z0-9_]+$ ]]; then
                if grep -q -E "^CMAKE_CXX_FLAGS_${active_config}(:[^=]*)?=" "${build_dir}/CMakeCache.txt" 2>/dev/null; then
                    cached_config_cxx_flags="$(sed -n -E "s/^CMAKE_CXX_FLAGS_${active_config}(:[^=]*)?=(.*)$/\2/p" "${build_dir}/CMakeCache.txt" | tail -n 1)"
                fi
                if grep -q -E "^CMAKE_EXE_LINKER_FLAGS_${active_config}(:[^=]*)?=" "${build_dir}/CMakeCache.txt" 2>/dev/null; then
                    cached_config_exe_linker_flags="$(sed -n -E "s/^CMAKE_EXE_LINKER_FLAGS_${active_config}(:[^=]*)?=(.*)$/\2/p" "${build_dir}/CMakeCache.txt" | tail -n 1)"
                fi
            fi
        fi
        local cmake_compiler_file=""
        for cmake_compiler_file in "${build_dir}"/CMakeFiles/*/CMakeCCompiler.cmake; do
            if [[ -r "${cmake_compiler_file}" ]]; then
                if [[ -z "${cached_c}" ]]; then
                    cached_c="$(sed -n -E 's/^[[:space:]]*set\(CMAKE_C_COMPILER[[:space:]]+"([^"]+)".*\).*$/\1/p' "${cmake_compiler_file}" | tail -n 1)"
                fi
                break
            fi
        done
        for cmake_compiler_file in "${build_dir}"/CMakeFiles/*/CMakeCXXCompiler.cmake; do
            if [[ -r "${cmake_compiler_file}" ]]; then
                if [[ -z "${cached_cxx}" ]]; then
                    cached_cxx="$(sed -n -E 's/^[[:space:]]*set\(CMAKE_CXX_COMPILER[[:space:]]+"([^"]+)".*\).*$/\1/p' "${cmake_compiler_file}" | tail -n 1)"
                fi
                if [[ -z "${cached_cxx_arg1}" ]]; then
                    cached_cxx_arg1="$(sed -n -E 's/^[[:space:]]*set\(CMAKE_CXX_COMPILER_ARG1[[:space:]]+"([^"]+)".*\).*$/\1/p' "${cmake_compiler_file}" | tail -n 1)"
                fi
                break
            fi
        done
        if [[ -n "${cached_c}" || -n "${cached_cxx}" ]]; then
            has_cmake_compiler_override=1
        fi
    fi

    local raw_cxx_compiler=""
    local raw_cxx_arg1=""
    if [[ "${has_cmake_compiler_override}" -eq 1 ]]; then
        raw_cxx_compiler="${cached_cxx:-${CXX:-g++}}"
        raw_cxx_arg1="${cached_cxx_arg1}"
    elif [[ -n "${CC:-}" && -n "${CXX:-}" ]]; then
        raw_cxx_compiler="${CXX}"
    elif [[ -n "${STARROCKS_GCC_HOME:-}" ]]; then
        if [[ -x "${STARROCKS_GCC_HOME}/bin/g++" ]]; then
            raw_cxx_compiler="${STARROCKS_GCC_HOME}/bin/g++"
        else
            raw_cxx_compiler="g++"
        fi
    elif [[ -n "${STARROCKS_LLVM_HOME:-}" ]]; then
        if [[ -x "${STARROCKS_LLVM_HOME}/bin/clang++" ]]; then
            raw_cxx_compiler="${STARROCKS_LLVM_HOME}/bin/clang++"
        else
            raw_cxx_compiler="g++"
        fi
    else
        raw_cxx_compiler="g++"
    fi

    local cxx_compiler=""
    local cxx_compiler_args=()
    if [[ "${raw_cxx_compiler}" != *";"* ]] && command -v "${raw_cxx_compiler}" >/dev/null 2>&1; then
        cxx_compiler="${raw_cxx_compiler}"
    else
        local semi_parts=()
        IFS=';' read -r -a semi_parts <<< "${raw_cxx_compiler}"
        local part_idx=0
        local part=""
        for part in "${semi_parts[@]+"${semi_parts[@]}"}"; do
            [[ -z "${part}" ]] && continue
            if [[ "${part_idx}" -eq 0 ]]; then
                if command -v "${part}" >/dev/null 2>&1; then
                    cxx_compiler="${part}"
                else
                    local ws_tokens=()
                    read -r -a ws_tokens <<< "${part}"
                    if [[ "${#ws_tokens[@]}" -gt 0 ]]; then
                        cxx_compiler="${ws_tokens[0]}"
                        if [[ "${#ws_tokens[@]}" -gt 1 ]]; then
                            cxx_compiler_args+=("${ws_tokens[@]:1}")
                        fi
                    fi
                fi
            else
                local extra_tokens=()
                read -r -a extra_tokens <<< "${part}"
                if [[ "${#extra_tokens[@]}" -gt 0 ]]; then
                    cxx_compiler_args+=("${extra_tokens[@]}")
                fi
            fi
            part_idx=$((part_idx + 1))
        done
    fi
    cxx_compiler="${cxx_compiler:-g++}"
    if [[ -n "${raw_cxx_arg1}" ]]; then
        local arg1_tokens=()
        read -r -a arg1_tokens <<< "${raw_cxx_arg1}"
        if [[ "${#arg1_tokens[@]}" -gt 0 ]]; then
            cxx_compiler_args+=("${arg1_tokens[@]}")
        fi
    fi
    local cxx_compiler_spec="${cxx_compiler} ${cxx_compiler_args[*]+"${cxx_compiler_args[*]}"}"

    # Stage 2: Determine effective STARROCKS_LINKER (explicit or non-aarch64 glibc<2.29 + compiler>14.0.0 gold fallback)
    local effective_starrocks_linker="${explicit_linker}"
    if [[ -z "${effective_starrocks_linker}" && "$(uname -m)" != "aarch64" ]]; then
        local cxx_version=""
        cxx_version="$("${cxx_compiler}" "${cxx_compiler_args[@]+"${cxx_compiler_args[@]}"}" -dumpfullversion -dumpversion 2>/dev/null || "${cxx_compiler}" "${cxx_compiler_args[@]+"${cxx_compiler_args[@]}"}" -dumpversion 2>/dev/null || true)"
        if [[ -z "${cxx_version}" && "${has_cmake_compiler_override}" -eq 1 ]]; then
            local cmake_cxx_file=""
            for cmake_cxx_file in "${build_dir}"/CMakeFiles/*/CMakeCXXCompiler.cmake; do
                if [[ -r "${cmake_cxx_file}" ]]; then
                    cxx_version="$(sed -n -E 's/^[[:space:]]*set\(CMAKE_CXX_COMPILER_VERSION[[:space:]]+"([^"]+)".*\).*$/\1/p' "${cmake_cxx_file}" | tail -n 1)"
                    [[ -n "${cxx_version}" ]] && break
                fi
            done
        fi
        cxx_version="${cxx_version:-0}"
        local cxx_major cxx_minor cxx_patch
        IFS='.' read -r cxx_major cxx_minor cxx_patch <<< "${cxx_version}"
        cxx_major="${cxx_major%%[^0-9]*}"
        cxx_minor="${cxx_minor%%[^0-9]*}"
        cxx_patch="${cxx_patch%%[^0-9]*}"
        cxx_major="${cxx_major:-0}"
        cxx_minor="${cxx_minor:-0}"
        cxx_patch="${cxx_patch:-0}"
        if [[ "${cxx_major}" -gt 14 ]] || [[ "${cxx_major}" -eq 14 && ( "${cxx_minor}" -gt 0 || "${cxx_patch}" -gt 0 ) ]]; then
            local glibc_version
            glibc_version="$(ldd --version 2>/dev/null | awk 'NR==1 {print $NF; exit}' || true)"
            if [[ -n "${glibc_version}" ]]; then
                local major="${glibc_version%%.*}"
                local minor="${glibc_version#*.}"
                minor="${minor%%.*}"
                if [[ "${major}" -lt 2 ]] || [[ "${major}" -eq 2 && "${minor}" -lt 29 ]]; then
                    effective_starrocks_linker="gold"
                fi
            fi
        fi
    fi

    # Stage 3: Assemble surviving driver and linker flags in CMake's exact
    # <CMAKE_CXX_COMPILER> <CMAKE_CXX_COMPILER_ARG1> <FLAGS> <CMAKE_CXX_LINK_FLAGS> <LINK_FLAGS> order.
    local base_exe_linker_flags=""
    if [[ "${has_cached_exe_linker_flags}" -eq 1 ]]; then
        base_exe_linker_flags="${cached_exe_linker_flags}"
    else
        base_exe_linker_flags="${LDFLAGS:-}"
    fi
    local gcc_toolchain_flag=""
    if [[ "${cxx_compiler_spec}" == *clang* && -n "${STARROCKS_GCC_HOME:-}" ]]; then
        gcc_toolchain_flag="--gcc-toolchain=${STARROCKS_GCC_HOME}"
    fi
    local combined_flags="${STARROCKS_CXX_COMMON_FLAGS:-} ${gcc_toolchain_flag} ${cached_config_cxx_flags} ${cached_cxx_link_flags} ${base_exe_linker_flags} ${STARROCKS_CXX_LINKER_FLAGS:-} ${cached_config_exe_linker_flags}"
    local extra_flag_tokens=()
    read -r -a extra_flag_tokens <<< "${combined_flags}"
    local all_driver_tokens=()
    if [[ "${#cxx_compiler_args[@]}" -gt 0 ]]; then
        all_driver_tokens+=("${cxx_compiler_args[@]}")
    fi
    if [[ "${#extra_flag_tokens[@]}" -gt 0 ]]; then
        all_driver_tokens+=("${extra_flag_tokens[@]}")
    fi

    local last_ld_path=""
    local last_selector_val=""
    local flag_token=""
    for flag_token in "${all_driver_tokens[@]+"${all_driver_tokens[@]}"}"; do
        case "${flag_token}" in
            -fuse-ld=*)
                last_selector_val="${flag_token#-fuse-ld=}"
                ;;
            --ld-path=*)
                last_ld_path="${flag_token#--ld-path=}"
                last_selector_val="${flag_token#--ld-path=}"
                ;;
        esac
    done
    if [[ -n "${effective_starrocks_linker}" ]]; then
        last_selector_val="${effective_starrocks_linker}"
    fi
    local selected_linker=""
    if [[ "${cxx_compiler_spec}" == *clang* && -n "${last_ld_path}" ]]; then
        selected_linker="${last_ld_path}"
    else
        selected_linker="${last_selector_val}"
    fi
    if [[ -n "${selected_linker}" ]]; then
        starrocks_classify_linker_spec "${selected_linker}" "${cxx_compiler}" "${all_driver_tokens[@]+"${all_driver_tokens[@]}"}"
        return 0
    fi

    # Stage 4: Probe the active compiler's resolved linker (-Wl,--version and -print-prog-name=ld),
    # forwarding all surviving driver flags (including CMAKE_CXX_COMPILER_ARG1, -B<dir>, -B <dir>,
    # --prefix=<dir>, --gcc-toolchain=<dir>), then fall back to compiler+args, bare compiler, and system ld.
    if ! ( declare -F ld >/dev/null 2>&1 && [[ "${cxx_compiler}" == "g++" && "${#all_driver_tokens[@]}" -eq 0 ]] && ! declare -F g++ >/dev/null 2>&1 ); then
        local compiler_wl_version=""
        local classified=""
        compiler_wl_version="$("${cxx_compiler}" "${all_driver_tokens[@]+"${all_driver_tokens[@]}"}" -Wl,--version 2>&1 || true)"
        if classified="$(starrocks_classify_linker_banner "${compiler_wl_version}" 1)"; then
            echo "${classified}"
            return 0
        fi

        local compiler_ld=""
        compiler_ld="$("${cxx_compiler}" "${all_driver_tokens[@]+"${all_driver_tokens[@]}"}" -print-prog-name=ld 2>/dev/null || true)"
        if [[ -n "${compiler_ld}" && "${compiler_ld}" != "ld" ]] && command -v "${compiler_ld}" >/dev/null 2>&1; then
            local compiler_ld_version=""
            compiler_ld_version="$("${compiler_ld}" -v 2>&1 || "${compiler_ld}" --version 2>&1 || true)"
            if classified="$(starrocks_classify_linker_banner "${compiler_ld_version}" 0)"; then
                echo "${classified}"
                return 0
            fi
        fi

        if [[ "${#cxx_compiler_args[@]}" -gt 0 && "${#extra_flag_tokens[@]}" -gt 0 ]]; then
            compiler_wl_version="$("${cxx_compiler}" "${cxx_compiler_args[@]}" -Wl,--version 2>&1 || true)"
            if classified="$(starrocks_classify_linker_banner "${compiler_wl_version}" 1)"; then
                echo "${classified}"
                return 0
            fi

            compiler_ld="$("${cxx_compiler}" "${cxx_compiler_args[@]}" -print-prog-name=ld 2>/dev/null || true)"
            if [[ -n "${compiler_ld}" && "${compiler_ld}" != "ld" ]] && command -v "${compiler_ld}" >/dev/null 2>&1; then
                local args_compiler_ld_version=""
                args_compiler_ld_version="$("${compiler_ld}" -v 2>&1 || "${compiler_ld}" --version 2>&1 || true)"
                if classified="$(starrocks_classify_linker_banner "${args_compiler_ld_version}" 0)"; then
                    echo "${classified}"
                    return 0
                fi
            fi
        fi

        if [[ "${#all_driver_tokens[@]}" -gt 0 ]]; then
            compiler_wl_version="$("${cxx_compiler}" -Wl,--version 2>&1 || true)"
            if classified="$(starrocks_classify_linker_banner "${compiler_wl_version}" 1)"; then
                echo "${classified}"
                return 0
            fi

            compiler_ld="$("${cxx_compiler}" -print-prog-name=ld 2>/dev/null || true)"
            if [[ -n "${compiler_ld}" && "${compiler_ld}" != "ld" ]] && command -v "${compiler_ld}" >/dev/null 2>&1; then
                local bare_compiler_ld_version=""
                bare_compiler_ld_version="$("${compiler_ld}" -v 2>&1 || "${compiler_ld}" --version 2>&1 || true)"
                if classified="$(starrocks_classify_linker_banner "${bare_compiler_ld_version}" 0)"; then
                    echo "${classified}"
                    return 0
                fi
            fi
        fi
    fi

    local ld_version_output=""
    local classified_ld=""
    if command -v ld >/dev/null 2>&1; then
        ld_version_output="$(ld -v 2>&1 || true)"
        if classified_ld="$(starrocks_classify_linker_banner "${ld_version_output}" 0)"; then
            echo "${classified_ld}"
            return 0
        fi
    fi

    echo "legacy"
}

starrocks_detect_ut_parallelism() {
    # 1. Caller-supplied environment variable takes precedence
    if [[ -n "${PARALLEL:-}" && "${PARALLEL}" =~ ^[0-9]+$ && "${PARALLEL}" -gt 0 ]]; then
        echo "${PARALLEL}"
        return 0
    fi

    # 2. Darwin host defaults to 100% core parallelism
    if starrocks_is_darwin; then
        starrocks_detect_parallelism
        return 0
    fi

    local cpus
    cpus="$(starrocks_detect_parallelism)"
    if [[ -z "${cpus}" || ! "${cpus}" =~ ^[0-9]+$ || "${cpus}" -lt 1 ]]; then
        cpus=1
    fi

    local linker_type="${1:-}"
    [[ -z "${linker_type}" ]] && linker_type="$(starrocks_detect_linker_type)"

    # 3. Legacy GNU BFD: preserve conservative 25% CPU throttle to avoid linker OOM
    if [[ "${linker_type}" == "legacy" ]]; then
        local legacy_parallel=$(( cpus / 4 + 1 ))
        echo "${legacy_parallel}"
        return 0
    fi

    # 4. Modern linkers (LLD, mold, gold): scale up to CPU count, capped by RAM availability
    # Peak template instantiation and LLD link uses ~1.5 - 2.0 GiB per concurrent job.
    local ram_gb="${2:-}"
    [[ -z "${ram_gb}" ]] && ram_gb="$(starrocks_detect_total_ram_gb)"
    local target_parallel="${cpus}"

    # If RAM is unreadable or unresolved, fall back to conservative legacy ratio
    if [[ -z "${ram_gb}" || ! "${ram_gb}" =~ ^[0-9]+$ || "${ram_gb}" -le 0 ]]; then
        local safe_parallel=$(( cpus / 4 + 1 ))
        if [[ "${safe_parallel}" -lt 1 ]]; then
            safe_parallel=1
        fi
        echo "${safe_parallel}"
        return 0
    fi

    local ram_safe_jobs=$(( ram_gb / 2 ))
    if [[ "${ram_safe_jobs}" -lt 1 ]]; then
        ram_safe_jobs=1
    fi
    if [[ "${target_parallel}" -gt "${ram_safe_jobs}" ]]; then
        target_parallel="${ram_safe_jobs}"
    fi

    if [[ "${target_parallel}" -lt 1 ]]; then
        target_parallel=1
    fi

    echo "${target_parallel}"
}

starrocks_default_ut_thin_archive() {
    if starrocks_is_darwin; then
        echo "OFF"
    else
        echo "ON"
    fi
}

starrocks_collect_test_binaries() {
    local test_dir="$1"
    local module_pattern="$2"

    find "${test_dir}" -type f -perm -111 -name "*test" \
        | grep -v starrocks_test \
        | grep -v starrocks_dw_test \
        | grep -v bench_test \
        | grep -v builtin_functions_fuzzy_test \
        | grep -i -e "${module_pattern}" || true
}

starrocks_resolve_java_home() {
    local java_home_candidate="$1"

    if [[ -z "${java_home_candidate}" ]]; then
        return 1
    fi
    if [[ -f "${java_home_candidate}/Contents/Home/include/jni.h" ]]; then
        echo "${java_home_candidate}/Contents/Home"
        return 0
    fi
    if [[ -f "${java_home_candidate}/libexec/openjdk.jdk/Contents/Home/include/jni.h" ]]; then
        echo "${java_home_candidate}/libexec/openjdk.jdk/Contents/Home"
        return 0
    fi
    if [[ -f "${java_home_candidate}/include/jni.h" ]]; then
        echo "${java_home_candidate}"
        return 0
    fi
    echo "${java_home_candidate}"
}

starrocks_setup_darwin_build_env() {
    local bundled_java_home=""

    export STARROCKS_ENV_QUIET=1
    . "${STARROCKS_HOME}/build-support/darwin_build_env.sh"

    unset STARROCKS_GCC_HOME

    MVN_CMD=${CUSTOM_MVN:-mvn}
    export MVN_CMD
    CMAKE_CMD=${CUSTOM_CMAKE:-cmake}
    export CMAKE_CMD
    BUILD_SYSTEM=ninja
    export BUILD_SYSTEM

    bundled_java_home="${STARROCKS_THIRDPARTY}/installed/open_jdk"
    if [[ -n "${JAVA_HOME:-}" ]]; then
        export JAVA_HOME="$(starrocks_resolve_java_home "${JAVA_HOME}")"
    elif [[ -d "${bundled_java_home}" ]]; then
        export JAVA_HOME="$(starrocks_resolve_java_home "${bundled_java_home}")"
    fi
}

starrocks_reset_stale_darwin_cmake_cache() {
    local build_dir="$1"
    local cache_path=""
    local cached_tool_paths=""
    local tool_path=""

    if ! starrocks_is_darwin; then
        return 0
    fi

    cache_path="${build_dir}/CMakeCache.txt"
    if [[ ! -f "${cache_path}" ]]; then
        return 0
    fi

    cached_tool_paths="$(
        sed -n -E \
            's/^(CMAKE_AR|CMAKE_RANLIB|CMAKE_C_COMPILER_AR|CMAKE_C_COMPILER_RANLIB|CMAKE_CXX_COMPILER_AR|CMAKE_CXX_COMPILER_RANLIB):FILEPATH=(.*)$/\2/p' \
            "${cache_path}"
    )"

    while IFS= read -r tool_path; do
        if [[ -n "${tool_path}" && ! -e "${tool_path}" ]]; then
            echo "[INFO] Reset stale CMake cache in ${build_dir} because ${tool_path} is missing"
            rm -rf "${build_dir}"
            mkdir -p "${build_dir}"
            return 0
        fi
    done <<< "${cached_tool_paths}"
}

starrocks_validate_darwin_thirdparty() {
    local tp_root="${STARROCKS_THIRDPARTY}"
    local tp_installed="${tp_root}/installed"

    if [[ ! -d "${tp_installed}" ]]; then
        if [[ -d "${tp_root}/bin" || -d "${tp_root}/lib" || -d "${tp_root}/lib64" ]]; then
            echo "Error: STARROCKS_THIRDPARTY must point to the thirdparty root, not the installed directory: ${tp_root}"
        else
            echo "Error: macOS BE build requires STARROCKS_THIRDPARTY to point to a prepared Darwin thirdparty root."
            echo "Expected directory: ${tp_installed}"
        fi
        exit 1
    fi

    local required_paths=(
        "${tp_installed}/bin/protoc"
        "${tp_installed}/bin/thrift"
    )
    local required_path=""
    for required_path in "${required_paths[@]}"; do
        if [[ ! -e "${required_path}" ]]; then
            echo "Error: missing macOS thirdparty artifact: ${required_path}"
            exit 1
        fi
    done

    # libgrpc.a / libgrpc++.a: be/cmake_modules/ThirdParty.cmake does
    # find_package(gRPC CONFIG REQUIRED) against ${tp_installed}/lib/cmake/grpc.
    # Failing here gives a clear rebuild-thirdparty error up front.
    local required_libs=(
        "libprotobuf.a"
        "librocksdb.a"
        "libglog.a"
        "libbrpc.a"
        "libgrpc.a"
        "libgrpc++.a"
    )
    local required_lib=""
    for required_lib in "${required_libs[@]}"; do
        if [[ ! -e "${tp_installed}/lib/${required_lib}" && ! -e "${tp_installed}/lib64/${required_lib}" ]]; then
            echo "Error: missing macOS thirdparty artifact: ${required_lib} under ${tp_installed}/lib or ${tp_installed}/lib64"
            exit 1
        fi
    done

    # gRPC is built from source as a static archive (libgrpc.a / libgrpc++.a)
    # and consumed via ${tp_installed}/lib/cmake/grpc/gRPCConfig.cmake, matching
    # the Linux flow. Only list dylibs that Homebrew still provides.
    local required_darwin_dylibs=(
        "libkrb5support.dylib"
        "libkrb5.dylib"
        "libcom_err.dylib"
        "libk5crypto.dylib"
        "libgssapi_krb5.dylib"
        "libsasl2.dylib"
    )
    if [[ "${STARROCKS_USE_NIX_DEPS:-0}" != "1" ]]; then
        required_darwin_dylibs+=("libxml2.dylib")
    fi
    local required_darwin_dylib=""
    for required_darwin_dylib in "${required_darwin_dylibs[@]}"; do
        if [[ ! -e "${tp_installed}/lib/${required_darwin_dylib}" &&
              ! -e "${tp_installed}/lib64/${required_darwin_dylib}" ]]; then
            echo "Error: missing macOS thirdparty dylib: ${required_darwin_dylib} under ${tp_installed}/lib or ${tp_installed}/lib64"
            exit 1
        fi
    done

    local jemalloc_shared_lib=""
    jemalloc_shared_lib=$(find "${tp_installed}/jemalloc/lib-shared" -maxdepth 1 -name 'libjemalloc*.dylib' -print -quit 2>/dev/null)
    if [[ -z "${jemalloc_shared_lib}" ]]; then
        echo "Error: missing macOS thirdparty jemalloc dylib under ${tp_installed}/jemalloc/lib-shared"
        exit 1
    fi

    local bundled_java_home="$(starrocks_resolve_java_home "${tp_installed}/open_jdk")"
    local resolved_java_home=""
    if [[ -n "${JAVA_HOME:-}" ]]; then
        resolved_java_home="$(starrocks_resolve_java_home "${JAVA_HOME}")"
    fi
    local bundled_jni_header="${bundled_java_home}/include/jni.h"
    local java_home_jni_header=""
    if [[ -n "${resolved_java_home}" ]]; then
        java_home_jni_header="${resolved_java_home}/include/jni.h"
    fi
    if [[ ! -f "${bundled_jni_header}" && ! -f "${java_home_jni_header}" ]]; then
        echo "Error: missing JNI headers. Expected ${bundled_jni_header} or ${resolved_java_home}/include/jni.h"
        exit 1
    fi

    local bundled_libjvm=""
    local java_home_libjvm=""
    if [[ -d "${bundled_java_home}/lib" ]]; then
        bundled_libjvm=$(find "${bundled_java_home}/lib" -name 'libjvm.dylib' -print -quit 2>/dev/null)
    fi
    if [[ -n "${resolved_java_home}" && -d "${resolved_java_home}/lib" ]]; then
        java_home_libjvm=$(find "${resolved_java_home}/lib" -name 'libjvm.dylib' -print -quit 2>/dev/null)
    fi
    if [[ -z "${bundled_libjvm}" && -z "${java_home_libjvm}" ]]; then
        echo "Error: missing libjvm.dylib under ${bundled_java_home} and ${resolved_java_home:-${JAVA_HOME:-JAVA_HOME}}"
        exit 1
    fi
}

starrocks_resolve_getopt_bin() {
    local gnu_getopt_prefix=""
    local getopt_candidate=""
    local getopt_test_status=0

    if ! starrocks_is_darwin; then
        echo "getopt"
        return 0
    fi

    getopt_candidate="$(command -v getopt 2>/dev/null || true)"
    if [[ -n "${getopt_candidate}" ]]; then
        # util-linux/enhanced getopt returns 4 for -T. BSD getopt does not.
        if "${getopt_candidate}" -T >/dev/null 2>&1; then
            getopt_test_status=0
        else
            getopt_test_status=$?
        fi
        if [[ "${getopt_test_status}" -eq 4 ]]; then
            echo "${getopt_candidate}"
            return 0
        fi
    fi

    if command -v brew >/dev/null 2>&1; then
        gnu_getopt_prefix="$(brew --prefix gnu-getopt 2>/dev/null)"
        if [[ -n "${gnu_getopt_prefix}" ]] && [[ -x "${gnu_getopt_prefix}/bin/getopt" ]]; then
            echo "${gnu_getopt_prefix}/bin/getopt"
            return 0
        fi
        echo "gnu-getopt is required on macOS. Please install it with 'brew install gnu-getopt'." >&2
        return 1
    fi

    echo "Homebrew is required on macOS to install gnu-getopt. Please install Homebrew first." >&2
    return 1
}

starrocks_collect_host_dylib_dependencies() {
    local scan_path="$1"

    if [[ ! -e "${scan_path}" ]]; then
        return 0
    fi

    otool -L "${scan_path}" 2>/dev/null | awk 'NR > 1 {print $1}' | while IFS= read -r dep_path; do
        case "${dep_path}" in
            ""|/usr/lib/*|/System/Library/*|@rpath/*|@loader_path/*|@executable_path/*)
                continue
                ;;
            /*)
                printf '%s\n' "${dep_path}"
                ;;
        esac
    done
}

starrocks_append_unbundled_host_dylib_dependencies() {
    local scan_path="$1"
    local packaged_basename_file="$2"
    local output_file="$3"
    local dep_path=""
    local dep_basename=""

    while IFS= read -r dep_path; do
        dep_basename="$(basename "${dep_path}")"
        if [[ -n "${dep_basename}" ]] && grep -Fxq "${dep_basename}" "${packaged_basename_file}"; then
            continue
        fi
        printf '%s\n' "${dep_path}" >> "${output_file}"
    done < <(starrocks_collect_host_dylib_dependencies "${scan_path}")
}

starrocks_write_output_be_host_dylib_manifest() {
    local output_be_dir="$1"
    local manifest_path="${output_be_dir}/HOST_DYLIB_DEPENDENCIES.txt"
    local tmp_manifest=""
    local packaged_basename_file=""
    local packaged_dylib=""

    tmp_manifest="$(mktemp "${TMPDIR:-/tmp}/starrocks-host-dylibs.XXXXXX")" || {
        echo "Error: failed to create temporary file for macOS host dylib dependency manifest"
        exit 1
    }
    packaged_basename_file="$(mktemp "${TMPDIR:-/tmp}/starrocks-packaged-dylibs.XXXXXX")" || {
        rm -f "${tmp_manifest}"
        echo "Error: failed to create temporary file for packaged macOS dylib names"
        exit 1
    }

    find "${output_be_dir}/lib" -type f \( -name '*.dylib' -o -name '*.dylib.*' \) -exec basename {} \; | sort -u > "${packaged_basename_file}"

    starrocks_append_unbundled_host_dylib_dependencies "${output_be_dir}/lib/starrocks_be" "${packaged_basename_file}" "${tmp_manifest}"

    while IFS= read -r -d '' packaged_dylib; do
        starrocks_append_unbundled_host_dylib_dependencies "${packaged_dylib}" "${packaged_basename_file}" "${tmp_manifest}"
    done < <(find "${output_be_dir}/lib" -type f \( -name '*.dylib' -o -name '*.dylib.*' \) -print0)

    {
        echo "# macOS development note for output/be"
        echo "# This package is not self-contained."
        echo "# It depends on host dynamic libraries not bundled in output/be."
        echo "# Rebuild on the target machine or install compatible packages if any path is missing."
        echo
        if [[ -s "${tmp_manifest}" ]]; then
            sort -u "${tmp_manifest}"
        else
            echo "# No non-system absolute host dylib dependencies were detected."
        fi
    } > "${manifest_path}"

    rm -f "${tmp_manifest}"
    rm -f "${packaged_basename_file}"
}
