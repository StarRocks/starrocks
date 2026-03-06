#!/usr/bin/env bash
set -eo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "${ROOT}"

MODE=""
BASE_VERSION=""
BRANCH=""
BUILD_TYPE="${BUILD_TYPE:-Release}"
J_PARALLEL=""
ADD_COMPILE_OPTIONS=""

usage() {
    cat <<'USAGE'
Usage:
  build-support/clang-tidy.sh --mode <changed|full> [options]
  build-support/clang-tidy.sh run-changed <base_version> <branch> <build_type> <j_parallel> [add_compile_options]
  build-support/clang-tidy.sh run-full <branch> <build_type> <j_parallel> [add_compile_options]

Options:
  --mode <changed|full>            clang-tidy mode
  --base-version <sha>             required when mode=changed
  --branch <name>                  branch name for STARROCKS_VERSION (default: current branch or main)
  --build-type <Release|Debug|Asan>
  -j <num>                         parallel level for build.sh
  --add-compile-options <string>   extra options passed to build.sh (e.g. "--use-staros")
  -h, --help                       show this help
USAGE
}

get_default_branch() {
    local current_branch
    current_branch=$(git symbolic-ref -q --short HEAD 2>/dev/null || true)
    if [[ -n "${current_branch}" ]]; then
        echo "${current_branch}"
    else
        echo "main"
    fi
}

get_default_parallel() {
    if [[ "$OSTYPE" == darwin* ]]; then
        sysctl -n hw.ncpu
    else
        echo $(( $(nproc) / 4 + 1 ))
    fi
}

get_changed_be_files_for_tidy() {
    local base_version="$1"
    local changed_paths changed_be_files changed_be_headers header stem ext candidate

    changed_paths=$(
        {
            git diff --name-only --diff-filter=d "${base_version}" HEAD || true
            git diff --name-only --diff-filter=d HEAD || true
        } | sed '/^$/d' | sort -u
    )

    changed_be_files=$(printf '%s\n' "${changed_paths}" | grep -E '^be/.*\.(c|cc|cpp|cxx)$' || true)
    changed_be_headers=$(printf '%s\n' "${changed_paths}" | grep -E '^be/.*\.(h|hh|hpp|hxx)$' || true)

    for header in ${changed_be_headers}; do
        stem=${header%.*}
        for ext in c cc cpp cxx; do
            candidate="${stem}.${ext}"
            if [[ -f "${candidate}" ]]; then
                changed_be_files+=$'\n'"${candidate}"
            fi
        done
    done

    echo "${changed_be_files}" | sed '/^$/d' | sort -u
}

run_build_for_tidy() {
    local mode="$1"
    local -a extra_args mode_args

    extra_args=()
    if [[ -n "${ADD_COMPILE_OPTIONS}" ]]; then
        # shellcheck disable=SC2206
        extra_args=(${ADD_COMPILE_OPTIONS})
    fi

    if [[ "${mode}" == "changed" ]]; then
        mode_args=(--clean --configure-only --without-pch)
    else
        mode_args=(--clean --with-clang-tidy)
    fi

    if [[ "${STARROCKS_GCC_HOME}" == "/opt/gcc/usr" ]]; then
        export LD_LIBRARY_PATH=/opt/gcc/usr/lib64/:${LD_LIBRARY_PATH}
        CC=centos-clang CXX=centos-clang++ CXXFLAGS=--gcc-toolchain="${STARROCKS_GCC_HOME}" \
            STARROCKS_VERSION="${BRANCH}" BUILD_TYPE="${BUILD_TYPE}" \
            ./build.sh --be -j "${J_PARALLEL}" "${mode_args[@]}" "${extra_args[@]}"
    else
        CC=clang CXX=clang++ STARROCKS_VERSION="${BRANCH}" BUILD_TYPE="${BUILD_TYPE}" \
            ./build.sh --be -j "${J_PARALLEL}" "${mode_args[@]}" "${extra_args[@]}"
    fi
}

run_changed_clang_tidy() {
    local changed_be_files build_dir file clang_tidy_rc

    if [[ -z "${BASE_VERSION}" ]]; then
        echo "::error::base_version is required in changed mode"
        exit 1
    fi

    changed_be_files=$(get_changed_be_files_for_tidy "${BASE_VERSION}")
    if [[ -z "${changed_be_files}" ]]; then
        echo "No changed BE source files found for clang-tidy. Skip."
        exit 0
    fi

    echo "Clang-tidy changed files:"
    echo "${changed_be_files}"

    run_build_for_tidy "changed"

    build_dir="be/build_${BUILD_TYPE}"
    if [[ ! -f "${build_dir}/compile_commands.json" ]]; then
        echo "compile_commands.json is missing in ${build_dir}"
        exit 1
    fi

    clang_tidy_rc=0
    while IFS= read -r file; do
        [[ -z "${file}" ]] && continue
        if [[ ! -f "${file}" ]]; then
            echo "[clang-tidy] skip missing file: ${file}"
            continue
        fi
        echo "[clang-tidy] ${file}"
        clang-tidy -p "${build_dir}" "${file}" || clang_tidy_rc=1
    done <<< "${changed_be_files}"

    exit "${clang_tidy_rc}"
}

run_full_clang_tidy() {
    run_build_for_tidy "full"
}

parse_args() {
    if [[ $# -eq 0 ]]; then
        usage
        exit 1
    fi

    case "$1" in
        run-changed)
            MODE="changed"
            BASE_VERSION="${2:-}"
            BRANCH="${3:-}"
            BUILD_TYPE="${4:-${BUILD_TYPE}}"
            J_PARALLEL="${5:-}"
            ADD_COMPILE_OPTIONS="${6:-}"
            return
            ;;
        run-full)
            MODE="full"
            BRANCH="${2:-}"
            BUILD_TYPE="${3:-${BUILD_TYPE}}"
            J_PARALLEL="${4:-}"
            ADD_COMPILE_OPTIONS="${5:-}"
            return
            ;;
    esac

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --mode)
                MODE="$2"
                shift 2
                ;;
            --base-version)
                BASE_VERSION="$2"
                shift 2
                ;;
            --branch)
                BRANCH="$2"
                shift 2
                ;;
            --build-type)
                BUILD_TYPE="$2"
                shift 2
                ;;
            -j)
                J_PARALLEL="$2"
                shift 2
                ;;
            --add-compile-options)
                ADD_COMPILE_OPTIONS="$2"
                shift 2
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                echo "::error::unknown argument: $1"
                usage
                exit 1
                ;;
        esac
    done
}

parse_args "$@"

if [[ -z "${MODE}" ]]; then
    echo "::error::--mode is required"
    usage
    exit 1
fi

if [[ -z "${BRANCH}" ]]; then
    BRANCH=$(get_default_branch)
fi

if [[ -z "${J_PARALLEL}" ]]; then
    J_PARALLEL=$(get_default_parallel)
fi

case "${MODE}" in
    changed)
        run_changed_clang_tidy
        ;;
    full)
        run_full_clang_tidy
        ;;
    *)
        echo "::error::unsupported mode: ${MODE}"
        usage
        exit 1
        ;;
esac
