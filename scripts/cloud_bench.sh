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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PROJECT="${PROJECT:-oranged-experimentation}"
ZONE="${ZONE:-us-central1-a}"

ARM_INSTANCE="${ARM_INSTANCE:-bench-simd-arm-c4a}"
ARM_MACHINE="${ARM_MACHINE:-c4a-standard-16}"
ARM_IMAGE_FAMILY="${ARM_IMAGE_FAMILY:-ubuntu-2404-lts-arm64}"

X86_INSTANCE="${X86_INSTANCE:-bench-simd-x86-c4}"
X86_MACHINE="${X86_MACHINE:-c4-standard-16}"
X86_IMAGE_FAMILY="${X86_IMAGE_FAMILY:-ubuntu-2404-lts-amd64}"

RESULTS_DIR="${REPO_ROOT}/docs/superpowers/specs/bench_results"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

instance_exists() {
    local name="$1"
    gcloud compute instances describe "${name}" --project="${PROJECT}" --zone="${ZONE}" >/dev/null 2>&1
}

provision_instance() {
    local name="$1"
    local machine="$2"
    local family="$3"

    if instance_exists "${name}"; then
        log "Instance ${name} already exists."
        local status
        status="$(gcloud compute instances describe "${name}" --project="${PROJECT}" --zone="${ZONE}" --format='value(status)')"
        if [[ "${status}" != "RUNNING" ]]; then
            log "Starting instance ${name} (status: ${status})..."
            gcloud compute instances start "${name}" --project="${PROJECT}" --zone="${ZONE}"
        fi
        return 0
    fi

    log "Provisioning ${name} (${machine}, ${family}) in ${PROJECT}/${ZONE}..."
    gcloud compute instances create "${name}" \
        --project="${PROJECT}" \
        --zone="${ZONE}" \
        --machine-type="${machine}" \
        --image-family="${family}" \
        --image-project="ubuntu-os-cloud" \
        --boot-disk-size="100GB"
}

provision_all() {
    log "Provisioning ARM64 testbed: ${ARM_INSTANCE}..."
    provision_instance "${ARM_INSTANCE}" "${ARM_MACHINE}" "${ARM_IMAGE_FAMILY}"
    log "Provisioning x86_64 testbed: ${X86_INSTANCE}..."
    provision_instance "${X86_INSTANCE}" "${X86_MACHINE}" "${X86_IMAGE_FAMILY}"
}

wait_for_ssh() {
    local name="$1"
    log "Waiting for SSH to become ready on ${name}..."
    local attempts=0
    while ! gcloud compute ssh "${name}" --project="${PROJECT}" --zone="${ZONE}" --command="echo ready" >/dev/null 2>&1; do
        sleep 2
        attempts=$((attempts + 1))
        if [[ ${attempts} -gt 60 ]]; then
            echo "Timeout waiting for SSH on ${name}" >&2
            return 1
        fi
    done
    log "SSH is ready on ${name}."
}

deploy_and_build() {
    local name="$1"
    wait_for_ssh "${name}"

    log "Setting up dependencies and workspace on ${name}..."
    gcloud compute ssh "${name}" --project="${PROJECT}" --zone="${ZONE}" --command="
        sudo apt-get update -qq && \
        sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq build-essential libbenchmark-dev libjemalloc-dev
        mkdir -p ~/bench/be/src/base/simd ~/bench/be/src/bench
    "

    log "Copying benchmark sources to ${name}..."
    gcloud compute scp --project="${PROJECT}" --zone="${ZONE}" \
        "${REPO_ROOT}/be/src/base/simd/gather.h" \
        "${name}:~/bench/be/src/base/simd/gather.h"

    gcloud compute scp --project="${PROJECT}" --zone="${ZONE}" \
        "${REPO_ROOT}/be/src/bench/gather_bench.cpp" \
        "${name}:~/bench/be/src/bench/gather_bench.cpp"

    log "Compiling gather_bench on ${name}..."
    gcloud compute ssh "${name}" --project="${PROJECT}" --zone="${ZONE}" --command="
        cd ~/bench && \
        g++ -O3 -march=native -std=c++17 -I. -Ibe/src \
            be/src/bench/gather_bench.cpp \
            -lbenchmark -lpthread -ljemalloc \
            -o ~/bench/gather_bench
    "
    log "Build successful on ${name}."
}

run_bench() {
    local name="$1"
    local arch_label="$2"
    local filter="${3:-.*4096.*}"
    local reps="${4:-10}"

    mkdir -p "${RESULTS_DIR}"
    local out_txt="${RESULTS_DIR}/${arch_label}_4096.txt"
    local out_json="${RESULTS_DIR}/${arch_label}_4096.json"

    log "Running pinned benchmark on ${name} (${arch_label}) on core 2, filter='${filter}', reps=${reps}..."
    gcloud compute ssh "${name}" --project="${PROJECT}" --zone="${ZONE}" --command="
        cd ~/bench && \
        taskset -c 2 ./gather_bench \
            --benchmark_filter='${filter}' \
            --benchmark_repetitions=${reps} \
            --benchmark_report_aggregates_only=true \
            --benchmark_out_format=json \
            --benchmark_out=gather_bench.json
    " | tee "${out_txt}"

    log "Fetching JSON results from ${name}..."
    gcloud compute scp --project="${PROJECT}" --zone="${ZONE}" \
        "${name}:~/bench/gather_bench.json" \
        "${out_json}"

    log "Results saved to ${out_txt} and ${out_json}."
}

teardown() {
    log "Tearing down instances..."
    if instance_exists "${ARM_INSTANCE}"; then
        gcloud compute instances delete "${ARM_INSTANCE}" --project="${PROJECT}" --zone="${ZONE}" --quiet
    fi
    if instance_exists "${X86_INSTANCE}"; then
        gcloud compute instances delete "${X86_INSTANCE}" --project="${PROJECT}" --zone="${ZONE}" --quiet
    fi
    log "Teardown complete."
}

usage() {
    cat <<EOF
Usage: $0 {provision|deploy|run|all|teardown} [options]

Commands:
  provision       Create ARM64 (c4a-standard-16) and x86 (c4-standard-16) instances
  deploy          Copy sources and build gather_bench on both instances
  run [filter]    Run benchmark on both instances (default filter: .*4096.*)
  run-arm         Run benchmark on ARM instance only
  run-x86         Run benchmark on x86 instance only
  all             Provision, deploy, and run benchmarks
  teardown        Delete benchmark instances
EOF
    exit 1
}

cmd="${1:-}"
shift || true

case "${cmd}" in
    provision)
        provision_all
        ;;
    deploy)
        log "Deploying to ARM instance..."
        deploy_and_build "${ARM_INSTANCE}"
        log "Deploying to x86 instance..."
        deploy_and_build "${X86_INSTANCE}"
        ;;
    run)
        filter="${1:-.*4096.*}"
        reps="${2:-10}"
        log "Running benchmarks on ARM64..."
        run_bench "${ARM_INSTANCE}" "c4a_arm64_axion" "${filter}" "${reps}"
        log "Running benchmarks on x86_64..."
        run_bench "${X86_INSTANCE}" "c4_x86_emerald_rapids" "${filter}" "${reps}"
        ;;
    run-arm)
        filter="${1:-.*4096.*}"
        reps="${2:-10}"
        run_bench "${ARM_INSTANCE}" "c4a_arm64_axion" "${filter}" "${reps}"
        ;;
    run-x86)
        filter="${1:-.*4096.*}"
        reps="${2:-10}"
        run_bench "${X86_INSTANCE}" "c4_x86_emerald_rapids" "${filter}" "${reps}"
        ;;
    all)
        provision_all
        deploy_and_build "${ARM_INSTANCE}"
        deploy_and_build "${X86_INSTANCE}"
        run_bench "${ARM_INSTANCE}" "c4a_arm64_axion" ".*4096.*" 10
        run_bench "${X86_INSTANCE}" "c4_x86_emerald_rapids" ".*4096.*" 10
        ;;
    teardown)
        teardown
        ;;
    *)
        usage
        ;;
esac
