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

# Check args
usage() {
  echo "
Usage: $0 <options>
  Optional options:
     --test [TEST_NAME]         run specific test
     --filter [TEST_NAME]       skip run specific test,
                                multiple tests separated by commas and enclosed in quotation marks
     --dry-run                  dry-run unit tests
     --coverage                 run coverage statistic tasks
     --dumpcase [PATH]          run dump case and save to path
     --enable-profiler [0|1]    enable/disable async-profiler for performance profiling
     --jacoco-diff [0|1]        enable/disable incremental JaCoCo coverage based on git diff (default: 1)
     -j [N]                     build parallel, default is ${FE_UT_PARALLEL:-4}

  Eg.
    $0                                            run all unit tests
    $0 --test com.starrocks.utframe.Demo          run demo test
    $0 --filter com.starrocks.utframe.Demo#Test0  skip run demo test0
    $0 --dry-run                                  dry-run unit tests
    $0 --coverage                                 run coverage statistic tasks
    $0 --dumpcase /home/disk1/                    run dump case and save to path
    $0 --enable-profiler 1                        run tests with async-profiler enabled
    $0 --enable-profiler 0                        run tests with async-profiler disabled
    $0 --jacoco-diff 1                            enable incremental JaCoCo coverage (default)
    $0 --jacoco-diff 0                            disable incremental JaCoCo coverage
    $0 -j16 --test com.starrocks.utframe.Demo     run demo test with 16 parallel threads
  "
  exit 1
}

# -l run only used for compatibility
OPTS=$(getopt \
  -n $0 \
  -o 'j:' \
  -l 'test:' \
  -l 'filter:' \
  -l 'dry-run' \
  -l 'coverage' \
  -l 'dumpcase' \
  -l 'enable-profiler:' \
  -l 'jacoco-diff:' \
  -l 'help' \
  -l 'run' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

HELP=0
DRY_RUN=0
RUN_SPECIFIED_TEST=0
TEST_NAME=*
FILTER_TEST=""
COVERAGE=0
DUMPCASE=0
ENABLE_PROFILER=0
JACOCO_DIFF=1
PARALLEL=${FE_UT_PARALLEL:-4}
while true; do
    case "$1" in
        --coverage) COVERAGE=1 ; shift ;;
        --test) RUN_SPECIFIED_TEST=1; TEST_NAME=$2; shift 2;;
        --filter) FILTER_TEST=$2; shift 2;;
        --run) shift ;; # only used for compatibility
        --dumpcase) DUMPCASE=1; shift ;;
        --dry-run) DRY_RUN=1 ; shift ;;
        --enable-profiler) ENABLE_PROFILER=$2; shift 2;;
        --jacoco-diff) JACOCO_DIFF=$2; shift 2;;
        --help) HELP=1 ; shift ;;
        -j) PARALLEL=$2; shift 2 ;;
        --) shift ;  break ;;
        *) echo "Internal error" ; exit 1 ;;
    esac
done

if [ ${HELP} -eq 1 ]; then
    usage
    exit 0
fi

echo "*********************************"
echo "  Starting to Run FE Unit Tests  "
echo "*********************************"

cd ${STARROCKS_HOME}/fe/
mkdir -p build/compile

# Set FE_UT_PARALLEL if -j parameter is provided
export FE_UT_PARALLEL=$PARALLEL
echo "Unit test parallel is: $FE_UT_PARALLEL"

# Set JaCoCo configuration based on jacoco-diff parameter
if [ "${JACOCO_DIFF}" = "1" ]; then
    export JACOCO_BASE_BRANCH=main
    export JACOCO_DIFF_ENABLED=true
    echo "JaCoCo incremental coverage is enabled (base branch: $JACOCO_BASE_BRANCH)"
else
    export JACOCO_DIFF_ENABLED=false
    echo "JaCoCo incremental coverage is disabled (full coverage)"
fi

# Set ASYNC_PROFILER_ENABLED based on --enable-profiler parameter and platform detection
if [ "${ENABLE_PROFILER}" = "1" ]; then
    # Check if we're on Linux platform
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Check if async profiler library exists
        ASYNC_PROFILER_LIB="${STARROCKS_HOME}/build-support/libasyncProfiler.so"
        if [ -f "$ASYNC_PROFILER_LIB" ]; then
            export ASYNC_PROFILER_ENABLED=true
            echo "Async-profiler is enabled (Linux platform, library found)"
        else
            export ASYNC_PROFILER_ENABLED=false
            echo "Async-profiler is disabled (Linux platform, but library not found: $ASYNC_PROFILER_LIB)"
        fi
    else
        export ASYNC_PROFILER_ENABLED=false
        echo "Async-profiler is disabled (non-Linux platform: $OSTYPE)"
    fi
else
    export ASYNC_PROFILER_ENABLED=false
    echo "Async-profiler is disabled (explicitly disabled)"
fi

if [ -d "./mocked" ]; then
    rm -r ./mocked
fi

if [ -d "./ut_ports" ]; then
    rm -r ./ut_ports
fi

mkdir ut_ports

if [[ ${DUMPCASE} -ne 1 ]]; then
    DUMP_FILTER_TEST="com.starrocks.sql.dump.QueryDumpRegressionTest,com.starrocks.sql.dump.QueryDumpCaseRewriter"

    if [[ $FILTER_TEST != "" ]];then
        FILTER_TEST="${FILTER_TEST},${DUMP_FILTER_TEST}"
    else
        FILTER_TEST="${DUMP_FILTER_TEST}"
    fi

    FILTER_TEST=`echo $FILTER_TEST | sed -E 's/([^,]+)/!\1/g'`
    TEST_NAME="$TEST_NAME,$FILTER_TEST"
fi

# Generate JaCoCo includes configuration before running tests
FE_CORE_DIR="${STARROCKS_HOME}/fe/fe-core"
JACOCO_INCLUDES_DIR="${FE_CORE_DIR}/target"
mkdir -p "${JACOCO_INCLUDES_DIR}"

if [ "${JACOCO_DIFF_ENABLED}" = "true" ]; then
    JACOCO_BASE_BRANCH=${JACOCO_BASE_BRANCH:-HEAD~1}
    echo "Generating JaCoCo incremental coverage includes (base branch: $JACOCO_BASE_BRANCH)"
    if [ -f "${FE_CORE_DIR}/scripts/generate-jacoco-includes.sh" ]; then
        bash "${FE_CORE_DIR}/scripts/generate-jacoco-includes.sh" \
            "${JACOCO_BASE_BRANCH}" \
            "${JACOCO_INCLUDES_DIR}/jacoco-includes.txt" \
            "${JACOCO_INCLUDES_DIR}/jacoco-includes.properties"
    else
        echo "Warning: generate-jacoco-includes.sh not found, using full coverage"
        echo "jacoco.includes=com/starrocks/**" > "${JACOCO_INCLUDES_DIR}/jacoco-includes.properties"
    fi
else
    echo "Generating JaCoCo full coverage includes"
    echo "jacoco.includes=com/starrocks/**" > "${JACOCO_INCLUDES_DIR}/jacoco-includes.properties"
fi

# Extract jacoco.includes value from properties file and set as environment variable
if [ -f "${JACOCO_INCLUDES_DIR}/jacoco-includes.properties" ]; then
    # Extract the value after 'jacoco.includes=' line
    JACOCO_INCLUDES_VALUE=$(grep "^jacoco.includes=" "${JACOCO_INCLUDES_DIR}/jacoco-includes.properties" | cut -d'=' -f2-)
    if [ -n "$JACOCO_INCLUDES_VALUE" ]; then
        export JACOCO_INCLUDES="$JACOCO_INCLUDES_VALUE"
        echo "Set JACOCO_INCLUDES=${JACOCO_INCLUDES}"
    else
        export JACOCO_INCLUDES="com/starrocks/**"
        echo "Failed to extract jacoco.includes, using default: ${JACOCO_INCLUDES}"
    fi
else
    export JACOCO_INCLUDES="com/starrocks/**"
    echo "jacoco-includes.properties not found, using default: ${JACOCO_INCLUDES}"
fi

if [ ${COVERAGE} -eq 1 ]; then
    echo "Run coverage statistic tasks"
    ant cover-test
elif [ ${DUMPCASE} -eq 1 ]; then
    ${MVN_CMD} test -DfailIfNoTests=false -DtrimStackTrace=false -D test=com.starrocks.sql.dump.QueryDumpRegressionTest -D dumpJsonConfig=$1
else
    if [ $DRY_RUN -eq 0 ]; then
        if [ ${RUN_SPECIFIED_TEST} -eq 1 ]; then
            echo "Run test: $TEST_NAME"
        else
            echo "Run All Frontend Unittests"
        fi

        # set trimStackTrace to false to show full stack when debugging specified class or case
        ${MVN_CMD} verify -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false -DtrimStackTrace=false -D test="$TEST_NAME" -T $PARALLEL
    fi
fi
