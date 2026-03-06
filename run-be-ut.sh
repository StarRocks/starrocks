#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -eo pipefail

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

export STARROCKS_HOME=${ROOT}

. ${STARROCKS_HOME}/env.sh

PARALLEL=$[$(nproc)/4+1]

# Check args
usage() {
  echo "
Usage: $0 <options>
  Optional options:
     --test TEST_NAME               run specific test
     --gtest_filter GTEST_FILTER    run test cases with gtest filters
     --dry-run                      dry-run unit tests
     --clean                        clean old unit tests before run
     --with-gcov                    enable to build with gcov
     --with-dynamic                 enable to build with dynamic libs
     --with-aws                     enable to test aws
     --with-bench                   enable to build with benchmark
     --excluding-test-suit          don't run cases of specific suit
     --module                       module to run uts
     --build-target TARGET          only build the specified target (e.g. base_test)
     --enable-shared-data           enable to build with shared-data feature support
     --without-starcache            build without starcache library
     --use-staros                   DEPRECATED. an alias of --enable-shared-data option
     --without-debug-symbol-split   split debug symbol out of the test binary to accelerate the speed
                                    of loading binary into memory and start execution.
     -j                             build parallel

  Eg.
    $0                              run all unit tests
    $0 --test CompactionUtilsTest   run compaction test
    $0 --dry-run                    dry-run unit tests
    $0 --clean                      clean old unit tests before run
    $0 --help                       display usage
    $0 --gtest_filter CompactionUtilsTest*:TabletUpdatesTest*   run the two test suites: CompactionUtilsTest and TabletUpdatesTest
  "
  exit 1
}

# Append negative cases to existing $TEST_NAME
# refer to https://github.com/google/googletest/blob/main/docs/advanced.md#running-a-subset-of-the-tests
# for detailed explaination of `--gtest_filter`
append_negative_case() {
    local exclude_case=$1
    case $TEST_NAME in
      *-*)
        # already has negative cases, just append the cases to the end
        TEST_NAME=${TEST_NAME}:$exclude_case
        ;;
      *)
        # doesn't have negative cases, start the negative session
        TEST_NAME=${TEST_NAME}-$exclude_case
        ;;
    esac
}

# -l run and -l gtest_filter only used for compatibility
OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'test:' \
  -l 'dry-run' \
  -l 'clean' \
  -l 'with-gcov' \
  -l 'with-dynamic' \
  -l 'module:' \
  -l 'with-aws' \
  -l 'with-bench' \
  -l 'excluding-test-suit:' \
  -l 'use-staros' \
  -l 'enable-shared-data' \
  -l 'build-target:' \
  -l 'without-starcache' \
  -l 'without-java-ext' \
  -l 'without-debug-symbol-split' \
  -l 'without-java-ext' \
  -o 'j:' \
  -l 'help' \
  -l 'run' \
  -l 'gtest_filter:' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

CLEAN=0
DRY_RUN=0
TEST_NAME=*
TEST_MODULE=".*"
EXCLUDING_TEST_SUIT=
HELP=0
WITH_AWS=OFF
USE_STAROS=OFF
WITH_GCOV=OFF
WITH_STARCACHE=ON
WITH_BRPC_KEEPALIVE=OFF
WITH_DEBUG_SYMBOL_SPLIT=ON
BUILD_JAVA_EXT=ON
BUILD_TARGET=
if [[ -z ${WITH_DYNAMIC} ]]; then
    WITH_DYNAMIC=OFF
fi
while true; do
    case "$1" in
        --clean) CLEAN=1 ; shift ;;
        --dry-run) DRY_RUN=1 ; shift ;;
        --run) shift ;; # Option only for compatibility
        --test) TEST_NAME=${2}* ; shift 2;;
        --gtest_filter) TEST_NAME=$2 ; shift 2;;
        --module) TEST_MODULE=$2; shift 2;;
        --help) HELP=1 ; shift ;;
        --with-aws) WITH_AWS=ON; shift ;;
        --with-gcov) WITH_GCOV=ON; shift ;;
        --with-dynamic) WITH_DYNAMIC=ON; shift ;;
        --without-starcache) WITH_STARCACHE=OFF; shift ;;
        --excluding-test-suit) EXCLUDING_TEST_SUIT=$2; shift 2;;
        --enable-shared-data|--use-staros) USE_STAROS=ON; shift ;;
        --build-target) BUILD_TARGET=$2; shift 2;;
        --without-debug-symbol-split) WITH_DEBUG_SYMBOL_SPLIT=OFF; shift ;;
        --without-java-ext) BUILD_JAVA_EXT=OFF; shift ;;
        -j) PARALLEL=$2; shift 2 ;;
        --) shift ;  break ;;
        *) echo "Internal error" ; exit 1 ;;
    esac
done

if [[ "${BUILD_TYPE}" == "ASAN" && "${WITH_GCOV}" == "ON" ]]; then
    echo "Error: ASAN and gcov cannot be enabled at the same time. Please disable one of them."
    exit 1
fi

if [ ${HELP} -eq 1 ]; then
    usage
    exit 0
fi

CMAKE_BUILD_TYPE=${BUILD_TYPE:-ASAN}
CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
if [[ -z ${USE_SSE4_2} ]]; then
    USE_SSE4_2=ON
fi
if [[ -z ${USE_BMI_2} ]]; then
    USE_BMI_2=ON
fi
if [[ -z ${USE_AVX2} ]]; then
    USE_AVX2=ON
fi
if [[ -z ${USE_AVX512} ]]; then
    # Disable it by default
    USE_AVX512=OFF
fi
echo "Build Backend UT"

if [ -z $CMAKE_BUILD_PREFIX ]; then
    CMAKE_BUILD_PREFIX=${STARROCKS_HOME}/be
fi

CMAKE_BUILD_DIR=${CMAKE_BUILD_PREFIX}/ut_build_${CMAKE_BUILD_TYPE}
if [ ${CLEAN} -eq 1 ]; then
    rm ${CMAKE_BUILD_DIR} -rf
    rm ${STARROCKS_HOME}/be/output/ -rf
fi

if [ ! -d ${CMAKE_BUILD_DIR} ]; then
    mkdir -p ${CMAKE_BUILD_DIR}
fi

source ${STARROCKS_HOME}/bin/common.sh

cd ${CMAKE_BUILD_DIR}
if [ "${USE_STAROS}" == "ON"  ]; then
  if [ -z "$STARLET_INSTALL_DIR" ] ; then
    # assume starlet_thirdparty is installed to ${STARROCKS_THIRDPARTY}/installed/starlet/
    STARLET_INSTALL_DIR=${STARROCKS_THIRDPARTY}/installed/starlet
  fi
  export STARLET_INSTALL_DIR
fi

# Build Java Extensions
if [ ${BUILD_JAVA_EXT} = "ON" ]; then
    echo "Build Java Extensions"
    cd ${STARROCKS_HOME}/java-extensions
    if [ ${CLEAN} -eq 1 ]; then
        ${MVN_CMD} clean
    fi
    ${MVN_CMD} $addon_mvn_opts package -DskipTests -T ${PARALLEL}
    cd ${STARROCKS_HOME}
else
    echo "Skip Building Java Extensions"
fi

if [[ -z ${CCACHE} ]] && [[ -x "$(command -v ccache)" ]]; then
    CCACHE=ccache
    export CCACHE_SLOPPINESS="pch_defines,time_macros"
fi


cd ${CMAKE_BUILD_DIR}
${CMAKE_CMD}  -G "${CMAKE_GENERATOR}" \
            -DSTARROCKS_THIRDPARTY=${STARROCKS_THIRDPARTY}\
            -DSTARROCKS_HOME=${STARROCKS_HOME} \
            -DCMAKE_CXX_COMPILER_LAUNCHER=$CCACHE \
            -DMAKE_TEST=ON -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
            -DUSE_AVX2=$USE_AVX2 -DUSE_AVX512=$USE_AVX512 -DUSE_SSE4_2=$USE_SSE4_2 -DUSE_BMI_2=$USE_BMI_2\
            -DUSE_STAROS=${USE_STAROS} \
            -DSTARLET_INSTALL_DIR=${STARLET_INSTALL_DIR}          \
            -DWITH_GCOV=${WITH_GCOV} \
            -DWITH_STARCACHE=${WITH_STARCACHE} \
            -DWITH_BRPC_KEEPALIVE=${WITH_BRPC_KEEPALIVE} \
            -DSTARROCKS_JIT_ENABLE=ON \
            -DWITH_RELATIVE_SRC_PATH=OFF \
            -DENABLE_MULTI_DYNAMIC_LIBS=${WITH_DYNAMIC} \
            -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
            ${STARROCKS_HOME}/be

if [[ -n "$BUILD_TARGET" ]]; then
    ${BUILD_SYSTEM} -j${PARALLEL} ${BUILD_TARGET}
else
    ${BUILD_SYSTEM} -j${PARALLEL}
fi

cd ${STARROCKS_HOME}
export STARROCKS_TEST_BINARY_BASE_DIR=${CMAKE_BUILD_DIR}
export STARROCKS_TEST_BINARY_DIR=${STARROCKS_TEST_BINARY_BASE_DIR}/test

split_debug_symbol() {
    local bin="$1"
    local symbol="${bin}.debuginfo"
    echo -n "[INFO] Split $(basename "$bin") debug symbol to $(basename "$symbol") ..."
    objcopy --only-keep-debug "$bin" "$symbol"
    strip --strip-debug "$bin"
    objcopy --add-gnu-debuglink="$symbol" "$bin"
}

if [ "x$WITH_DEBUG_SYMBOL_SPLIT" = "xON" ] ; then
    if [ "x$WITH_DEBUG_SO_SYMBOL_SPLIT" = "xON" ] ; then
        find "${STARROCKS_TEST_BINARY_BASE_DIR}" -type f -name "*.so*" ! -name "*.debuginfo" | while read -r so; do
            split_debug_symbol "$so"
        done
    fi
    if [ -f "${STARROCKS_TEST_BINARY_BASE_DIR}/test/starrocks_test" ]; then
        split_debug_symbol ${STARROCKS_TEST_BINARY_BASE_DIR}/test/starrocks_test
    fi
    if [ -f "${STARROCKS_TEST_BINARY_BASE_DIR}/test/starrocks_dw_test" ]; then
        split_debug_symbol ${STARROCKS_TEST_BINARY_BASE_DIR}/test/starrocks_dw_test
    fi
fi

echo "*********************************"
echo "  Starting to Run BE Unit Tests  "
echo "*********************************"

export TERM=xterm
export UDF_RUNTIME_DIR=${STARROCKS_HOME}/lib/udf-runtime
export LOG_DIR=${STARROCKS_HOME}/log
export LSAN_OPTIONS=suppressions=${STARROCKS_HOME}/conf/asan_suppressions.conf
for i in `sed 's/ //g' $STARROCKS_HOME/conf/be_test.conf | grep -E "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*="`; do
    eval "export $i";
done

mkdir -p $LOG_DIR
mkdir -p ${UDF_RUNTIME_DIR}
rm -f ${UDF_RUNTIME_DIR}/*

export LD_LIBRARY_PATH=${STARROCKS_THIRDPARTY}/installed/jemalloc/lib-shared/:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=${STARROCKS_THIRDPARTY}/installed/lib64/:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=${STARROCKS_THIRDPARTY}/installed/llvm/lib/:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$(find ${CMAKE_BUILD_DIR} -type f -name "*.so" -exec dirname {} \; | sort -u | tr '\n' ':' | sed 's/:$//'):$LD_LIBRARY_PATH

# ====================== configure JAVA/JVM ====================
# NOTE: JAVA_HOME must be configed if using hdfs scan, like hive external table
# this is only for starting be
jvm_arch="amd64"
if [[ "${MACHINE_TYPE}" == "aarch64" ]]; then
    jvm_arch="aarch64"
fi

if [ "$JAVA_HOME" = "" ]; then
    export LD_LIBRARY_PATH=$STARROCKS_HOME/lib/jvm/$jvm_arch/server:$STARROCKS_HOME/lib/jvm/$jvm_arch:$LD_LIBRARY_PATH
else
    java_version=$(jdk_version)
    if [[ $java_version -gt 8 ]]; then
        export LD_LIBRARY_PATH=$JAVA_HOME/lib/server:$JAVA_HOME/lib:$LD_LIBRARY_PATH
    # JAVA_HOME is jdk
    elif [[ -d "$JAVA_HOME/jre"  ]]; then
        export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/$jvm_arch/server:$JAVA_HOME/jre/lib/$jvm_arch:$LD_LIBRARY_PATH
    # JAVA_HOME is jre
    else
        export LD_LIBRARY_PATH=$JAVA_HOME/lib/$jvm_arch/server:$JAVA_HOME/lib/$jvm_arch:$LD_LIBRARY_PATH
    fi
fi

if [[ -n "$STARROCKS_GCC_HOME" ]] ; then
    # add gcc lib64 into LD_LIBRARY_PATH because of dynamic link libstdc++ and libgcc
    export LD_LIBRARY_PATH=$(dirname $($STARROCKS_GCC_HOME/bin/g++ -print-file-name=libstdc++.so)):$LD_LIBRARY_PATH
fi

RUN_UT_HADOOP_COMMON_HOME=${STARROCKS_HOME}/java-extensions/hadoop-lib/target/hadoop-lib
if [[ -d ${RUN_UT_HADOOP_COMMON_HOME} ]] ; then
    export HADOOP_CLASSPATH=${RUN_UT_HADOOP_COMMON_HOME}/*:${RUN_UT_HADOOP_COMMON_HOME}/lib/*:
    # get rid of StackOverflowError on the process reaper thread, which has a small stack size.
    # https://bugs.openjdk.org/browse/JDK-8153057
    export LIBHDFS_OPTS="$LIBHDFS_OPTS -Djdk.lang.processReaperUseDefaultStackSize=true"
else
    # exclude HdfsFileSystemTest related test case if no hadoop env found
    echo "[INFO] Can't find available HADOOP common lib, disable HdfsFileSystemTest related test!"
    append_negative_case "HdfsFileSystemTest*"
fi
# HADOOP_CLASSPATH defined in $STARROCKS_HOME/conf/hadoop_env.sh
# put $STARROCKS_HOME/conf ahead of $HADOOP_CLASSPATH so that custom config can replace the config in $HADOOP_CLASSPATH
export CLASSPATH=$STARROCKS_HOME/conf:$HADOOP_CLASSPATH:$CLASSPATH
export CLASSPATH=${STARROCKS_HOME}/java-extensions/udf-extensions/target/*:$CLASSPATH
export CLASSPATH=${STARROCKS_HOME}/java-extensions/java-utils/target/*:$CLASSPATH

# ===========================================================

export ASAN_OPTIONS="abort_on_error=1:disable_coredump=0:unmap_shadow_on_exit=1:detect_stack_use_after_return=1"

if [ $WITH_AWS = "OFF" ]; then
    append_negative_case "*S3*"
fi

if [ -n "$EXCLUDING_TEST_SUIT" ]; then
    excluding_test_suit=$EXCLUDING_TEST_SUIT
    excluding_test_suit_array=("${excluding_test_suit//|/ }")
    for element in ${excluding_test_suit_array[*]}; do
        append_negative_case "*.${element}_*"
    done
fi

# prepare util test_data
if [ -d ${STARROCKS_TEST_BINARY_DIR}/util/test_data ]; then
    rm -rf ${STARROCKS_TEST_BINARY_DIR}/util/test_data
fi
cp -r ${STARROCKS_HOME}/be/test/util/test_data ${STARROCKS_TEST_BINARY_DIR}/util/

test_files=`find ${STARROCKS_TEST_BINARY_DIR} -type f -perm -111 -name "*test" \
    | grep -v starrocks_test \
    | grep -v starrocks_dw_test \
    | grep -v bench_test \
    | grep -v builtin_functions_fuzzy_test \
    | grep -e "$TEST_MODULE" `

echo "[INFO] gtest_filter: $TEST_NAME"

# Helpers below only apply to the two monolithic BE UT binaries:
# `starrocks_test` and `starrocks_dw_test`.
is_positive_integer() {
    [[ "$1" =~ ^[0-9]+$ ]] && (( $1 > 0 ))
}

run_test_module_parallel() {
    local target="$1"

    if [ -x "${GTEST_PARALLEL}" ]; then
        ${GTEST_PARALLEL} ${STARROCKS_TEST_BINARY_DIR}/${target} \
            --gtest_filter=${TEST_NAME} \
            --serialize_test_cases ${GTEST_PARALLEL_OPTIONS}
    else
        ${STARROCKS_TEST_BINARY_DIR}/${target} $GTEST_OPTIONS --gtest_filter=${TEST_NAME}
    fi
}

resolve_be_ut_batch_jobs() {
    local jobs="${SR_BE_UT_BATCH_JOBS:-}"
    local parallel_options="${GTEST_PARALLEL_OPTIONS:-}"

    if [[ -z "$jobs" && "$parallel_options" =~ (^|[[:space:]])--workers=([0-9]+)($|[[:space:]]) ]]; then
        jobs="${BASH_REMATCH[2]}"
    elif [[ -z "$jobs" && "$parallel_options" =~ (^|[[:space:]])--workers[[:space:]]+([0-9]+)($|[[:space:]]) ]]; then
        jobs="${BASH_REMATCH[2]}"
    elif [[ -z "$jobs" ]]; then
        jobs="$(nproc)"
    fi

    if ! is_positive_integer "$jobs"; then
        echo "Error: invalid BE UT batch jobs: ${jobs}" >&2
        return 1
    fi

    echo "$jobs"
}

discover_concrete_gtests() {
    local target="$1"
    local output_file="$2"

    # Use the checked-in helper so the parsing logic is versioned and testable.
    python3 "${STARROCKS_HOME}/build-support/starrocks_test_batcher.py" discover \
        --binary "${STARROCKS_TEST_BINARY_DIR}/${target}" \
        --gtest-filter "${TEST_NAME}" \
        --output "${output_file}"
}

plan_test_module_batches() {
    local tests_file="$1"
    local manifest_dir="$2"
    local jobs="$3"
    local factor="$4"
    local max_filter_bytes="$5"

    python3 "${STARROCKS_HOME}/build-support/starrocks_test_batcher.py" plan \
        --tests-file "${tests_file}" \
        --manifest-dir "${manifest_dir}" \
        --jobs "${jobs}" \
        --factor "${factor}" \
        --max-filter-bytes "${max_filter_bytes}"
}

run_test_module_batches() {
    local target="$1"
    local manifest_root="${STARROCKS_TEST_BINARY_BASE_DIR}/test_batch_manifest"
    local manifest_dir="${manifest_root}/${target}"
    local log_dir="${LOG_DIR}/${target}_batches"
    local tests_file="${manifest_root}/${target}.all_tests.txt"
    local batch_jobs
    local batch_factor="${SR_BE_UT_BATCH_FACTOR:-4}"
    local max_filter_bytes="${SR_BE_UT_BATCH_MAX_FILTER_BYTES:-32768}"
    local total_tests
    local manifest_count

    if ! is_positive_integer "$batch_factor"; then
        echo "Error: invalid BE UT batch factor: ${batch_factor}" >&2
        return 1
    fi
    if ! is_positive_integer "$max_filter_bytes"; then
        echo "Error: invalid BE UT batch max filter bytes: ${max_filter_bytes}" >&2
        return 1
    fi

    # Batch mode reduces repeated process/bootstrap cost by running many tests
    # per monolithic test process instead of one process per concrete gtest.
    batch_jobs="$(resolve_be_ut_batch_jobs)" || return 1

    mkdir -p "$manifest_root"
    if ! discover_concrete_gtests "$target" "$tests_file"; then
        return 1
    fi

    total_tests="$(wc -l < "$tests_file")"
    if [[ -z "$total_tests" || "$total_tests" -eq 0 ]]; then
        echo "[INFO] ${target} batching found zero concrete tests, falling back to direct execution"
        ${STARROCKS_TEST_BINARY_DIR}/${target} $GTEST_OPTIONS --gtest_filter=${TEST_NAME}
        return $?
    fi

    if ! plan_test_module_batches "$tests_file" "$manifest_dir" "$batch_jobs" "$batch_factor" "$max_filter_bytes"; then
        return 1
    fi

    mkdir -p "$log_dir"
    rm -f "${log_dir}"/batch_*.log

    local -a manifests=()
    local manifest
    while IFS= read -r manifest; do
        manifests+=("$manifest")
    done < <(find "$manifest_dir" -maxdepth 1 -type f -name 'batch_*.txt' | sort)

    manifest_count="${#manifests[@]}"
    if (( manifest_count == 0 )); then
        echo "[INFO] ${target} batch planner produced no manifests, falling back to direct execution"
        ${STARROCKS_TEST_BINARY_DIR}/${target} $GTEST_OPTIONS --gtest_filter=${TEST_NAME}
        return $?
    fi

    echo "[INFO] ${target} mode: batch"
    echo "[INFO] ${target} batching ${total_tests} tests into ${manifest_count} batches with ${batch_jobs} concurrent jobs"

    local -a active_pids=()
    local -a failed_batch_ids=()
    local -a failed_log_paths=()
    local -a failed_manifests=()
    local -A pid_to_batch_id=()
    local -A pid_to_log_path=()
    local -A pid_to_manifest=()
    local overall_status=0
    local batch_id
    local log_file
    local joined_filter
    local batch_test_count
    local filter_bytes
    local pid
    local finished_pid
    local wait_status
    local i

    for manifest in "${manifests[@]}"; do
        # Keep a fixed number of batch processes in flight so startup-heavy
        # tests still run concurrently without exploding process count.
        while (( ${#active_pids[@]} >= batch_jobs )); do
            if wait -n -p finished_pid "${active_pids[@]}"; then
                wait_status=0
            else
                wait_status=$?
            fi

            if (( wait_status != 0 )); then
                overall_status=1
                failed_batch_ids+=("${pid_to_batch_id[$finished_pid]}")
                failed_log_paths+=("${pid_to_log_path[$finished_pid]}")
                failed_manifests+=("${pid_to_manifest[$finished_pid]}")
            fi

            local -a remaining_pids=()
            for pid in "${active_pids[@]}"; do
                if [[ "$pid" != "$finished_pid" ]]; then
                    remaining_pids+=("$pid")
                fi
            done
            active_pids=("${remaining_pids[@]}")
            unset 'pid_to_batch_id[$finished_pid]' 'pid_to_log_path[$finished_pid]' 'pid_to_manifest[$finished_pid]'
        done

        batch_id="${manifest##*/}"
        batch_id="${batch_id%.txt}"
        log_file="${log_dir}/${batch_id}.log"
        joined_filter="$(paste -sd: "$manifest")"
        batch_test_count="$(wc -l < "$manifest")"
        filter_bytes="${#joined_filter}"

        echo "[INFO] Run ${target} ${batch_id}: tests=${batch_test_count} filter_bytes=${filter_bytes} log=${log_file}"

        (
            ${STARROCKS_TEST_BINARY_DIR}/${target} $GTEST_OPTIONS --gtest_filter="${joined_filter}"
        ) >"${log_file}" 2>&1 &
        pid=$!
        active_pids+=("$pid")
        pid_to_batch_id["$pid"]="$batch_id"
        pid_to_log_path["$pid"]="$log_file"
        pid_to_manifest["$pid"]="$manifest"
    done

    while (( ${#active_pids[@]} > 0 )); do
        if wait -n -p finished_pid "${active_pids[@]}"; then
            wait_status=0
        else
            wait_status=$?
        fi

        if (( wait_status != 0 )); then
            overall_status=1
            failed_batch_ids+=("${pid_to_batch_id[$finished_pid]}")
            failed_log_paths+=("${pid_to_log_path[$finished_pid]}")
            failed_manifests+=("${pid_to_manifest[$finished_pid]}")
        fi

        local -a remaining_pids=()
        for pid in "${active_pids[@]}"; do
            if [[ "$pid" != "$finished_pid" ]]; then
                remaining_pids+=("$pid")
            fi
        done
        active_pids=("${remaining_pids[@]}")
        unset 'pid_to_batch_id[$finished_pid]' 'pid_to_log_path[$finished_pid]' 'pid_to_manifest[$finished_pid]'
    done

    if (( overall_status != 0 )); then
        echo "[ERROR] ${target} batch execution failed"
        echo "[ERROR] Failed batch ids: ${failed_batch_ids[*]}"
        echo "[ERROR] Failed batch logs:"
        for log_file in "${failed_log_paths[@]}"; do
            echo "  ${log_file}"
        done
        for i in "${!failed_manifests[@]}"; do
            echo "[ERROR] First 10 tests from ${failed_batch_ids[$i]}:"
            head -n 10 "${failed_manifests[$i]}"
        done
    fi

    return "$overall_status"
}

run_test_module() {
    TARGET=$1
    local ut_mode="${SR_BE_UT_MODE:-batch}"

    # `run_test_module` is only used by the two monolithic BE UT binaries, so
    # apply the same batch/parallel mode switch to both of them.
    if [[ $TEST_MODULE == '.*'  || $TEST_MODULE == $TARGET ]]; then
        echo "Run test: ${STARROCKS_TEST_BINARY_DIR}/$TARGET"
        if [ ${DRY_RUN} -eq 0 ]; then
            case "$ut_mode" in
                batch)
                    run_test_module_batches "$TARGET"
                    ;;
                parallel)
                    run_test_module_parallel "$TARGET"
                    ;;
                *)
                    echo "Error: invalid SR_BE_UT_MODE: ${ut_mode}" >&2
                    return 1
                    ;;
            esac
        fi
    fi
}

run_test_module starrocks_test
run_test_module starrocks_dw_test

for test_bin in $test_files
do
    echo "Run test: $test_bin"
    if [ ${DRY_RUN} -eq 0 ]; then
        file_name=${test_bin##*/}
        if [ -z $RUN_FILE ] || [ $file_name == $RUN_FILE ]; then
            $test_bin $GTEST_OPTIONS --gtest_filter=${TEST_NAME}
        fi
    fi
done
