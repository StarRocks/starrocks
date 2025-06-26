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
# Docker-based build script for StarRocks
# This script uses the official StarRocks dev-env Docker image
# to build the project in a consistent Ubuntu environment.
#
# Usage:
#    ./build-in-docker.sh --help
# Examples:
#    ./build-in-docker.sh                                    # build all (FE + BE)
#    ./build-in-docker.sh --fe                               # build Frontend only
#    ./build-in-docker.sh --be                               # build Backend only
#    ./build-in-docker.sh --fe --clean                       # clean and build Frontend
#    ./build-in-docker.sh --fe --be --clean                  # clean and build both
#    ./build-in-docker.sh --shell                            # open interactive shell
#    ./build-in-docker.sh --test                             # run tests
##############################################################

set -euo pipefail

# Configuration
DOCKER_IMAGE="${STARROCKS_DEV_ENV_IMAGE:-starrocks/dev-env-ubuntu:latest}"
CONTAINER_NAME="starrocks-build-$(whoami)-$(id -u)-$(date +%s)"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_ARGS=""
INTERACTIVE_SHELL=false
RUN_TESTS=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Help function
usage() {
    cat << EOF
Docker-based build script for StarRocks

Usage: $0 [DOCKER_OPTIONS] [BUILD_OPTIONS]

DOCKER-SPECIFIC OPTIONS:
    --shell                 Open interactive shell in dev environment
    --test                  Run tests after building
    --image IMAGE           Use specific Docker image (default: $DOCKER_IMAGE)
    --help, -h              Show this help message

BUILD OPTIONS (passed through to build.sh):
    All options supported by build.sh are automatically passed through, including:
    --fe                    Build Frontend only
    --be                    Build Backend only
    --spark-dpp             Build Spark DPP application
    --hive-udf              Build Hive UDF
    --clean                 Clean before building
    --enable-shared-data    Build Backend with shared-data feature support
    --with-gcov             Build Backend with gcov
    --without-gcov          Build Backend without gcov (default)
    --with-bench            Build Backend with bench
    --with-clang-tidy       Build Backend with clang-tidy
    --without-java-ext      Build Backend without java-extensions
    --without-starcache     Build Backend without starcache library
    --without-tenann        Build without vector index tenann library
    --without-avx2          Build Backend without avx2 instruction
    --disable-java-check-style  Disable Java checkstyle checks
    -j N                    Build with N parallel jobs
    ... and any other build.sh options

EXAMPLES:
    $0                                          # Build all components
    $0 --fe --clean                             # Clean and build Frontend
    $0 --be --with-gcov                         # Build Backend with gcov
    $0 --shell                                  # Open interactive shell
    $0 --test                                   # Build and run tests
    $0 --image starrocks/dev-env-ubuntu:latest  # Use different image
    $0 --be --new-future-option                 # Any new build.sh option works automatically

ENVIRONMENT VARIABLES:
    STARROCKS_DEV_ENV_IMAGE    Docker image to use (default: $DOCKER_IMAGE)
    DOCKER_BUILD_OPTS          Additional Docker run options

NOTE: This script automatically passes through all unrecognized options to build.sh,
      so new build.sh options work without updating this script.

EOF
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            # Docker-specific options (handled by this script)
            --shell)
                INTERACTIVE_SHELL=true
                shift
                ;;
            --test)
                RUN_TESTS=true
                shift
                ;;
            --image)
                DOCKER_IMAGE="$2"
                shift 2
                ;;
            --help|-h)
                usage
                exit 0
                ;;
            # Options with arguments (pass through to build.sh)
            -j)
                BUILD_ARGS="$BUILD_ARGS $1 $2"
                shift 2
                ;;
            # All other options (pass through to build.sh)
            *)
                BUILD_ARGS="$BUILD_ARGS $1"
                shift
                ;;
        esac
    done
}

# Check if Docker is available
check_docker() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        exit 1
    fi

    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running or not accessible"
        exit 1
    fi
}

# Pull Docker image if needed
pull_image() {
    log_info "Checking Docker image: $DOCKER_IMAGE"

    if ! docker image inspect "$DOCKER_IMAGE" &> /dev/null; then
        log_info "Pulling Docker image: $DOCKER_IMAGE"
        if ! docker pull "$DOCKER_IMAGE"; then
            log_error "Failed to pull Docker image: $DOCKER_IMAGE"
            exit 1
        fi
    else
        log_info "Docker image already available: $DOCKER_IMAGE"
    fi
}

# Setup Docker run command
setup_docker_run() {
    # Base Docker run options
    DOCKER_RUN_OPTS=(
        --rm
        --name "$CONTAINER_NAME"
        --volume "$REPO_ROOT:/workspace"
        --volume "$HOME/.m2:/tmp/.m2"
        --workdir /workspace
        --user "$(id -u):$(id -g)"
        --env "HOME=/tmp"
        --env "STARROCKS_HOME=/workspace"
        --env "STARROCKS_THIRDPARTY=/var/local/thirdparty"
        --env "MAVEN_OPTS=-Dmaven.repo.local=/tmp/.m2/repository"
    )

    # Add any additional Docker options from environment
    if [[ -n "${DOCKER_BUILD_OPTS:-}" ]]; then
        read -ra ADDITIONAL_OPTS <<< "$DOCKER_BUILD_OPTS"
        DOCKER_RUN_OPTS+=("${ADDITIONAL_OPTS[@]}")
    fi

    # Interactive mode for shell
    if [[ "$INTERACTIVE_SHELL" == "true" ]]; then
        DOCKER_RUN_OPTS+=(--interactive --tty)
    fi
}

# Run interactive shell
run_shell() {
    log_info "Starting interactive shell in StarRocks dev environment"
    log_info "Container: $CONTAINER_NAME"
    log_info "Image: $DOCKER_IMAGE"
    log_info "Workspace: /workspace (mounted from $REPO_ROOT)"

    docker run "${DOCKER_RUN_OPTS[@]}" "$DOCKER_IMAGE" /bin/bash
}

# Check if protobuf version mismatch exists
check_protobuf_version_mismatch() {
    log_info "Checking for protobuf version compatibility..."

    # Check if generated protobuf files exist
    if [[ ! -d "gensrc/build/gen_cpp" ]] || [[ -z "$(find gensrc/build/gen_cpp -name "*.pb.cc" 2>/dev/null)" ]]; then
        log_info "No generated protobuf files found - clean generation needed"
        return 0  # Need to clean/generate
    fi

    # Get protobuf version from Docker container
    local container_protoc_version
    container_protoc_version=$(docker run --rm "${DOCKER_RUN_OPTS[@]}" "$DOCKER_IMAGE" bash -c "/var/local/thirdparty/installed/bin/protoc --version 2>/dev/null | cut -d' ' -f2" 2>/dev/null || echo "unknown")

    # Try to detect version mismatch by checking for common error patterns in a test compilation
    local test_result
    test_result=$(docker run --rm "${DOCKER_RUN_OPTS[@]}" "$DOCKER_IMAGE" bash -c "
        cd /workspace &&
        echo '#include \"gensrc/build/gen_cpp/data.pb.h\"' > /tmp/test_protobuf.cpp &&
        echo 'int main() { return 0; }' >> /tmp/test_protobuf.cpp &&
        g++ -I/var/local/thirdparty/installed/include -I. -c /tmp/test_protobuf.cpp -o /tmp/test_protobuf.o 2>&1 || echo 'PROTOBUF_MISMATCH'
    " 2>/dev/null)

    if [[ "$test_result" == *"PROTOBUF_MISMATCH"* ]] || [[ "$test_result" == *"incompatible version"* ]] || [[ "$test_result" == *"generated_message_tctable_decl.h"* ]]; then
        log_warning "Protobuf version mismatch detected (container: $container_protoc_version)"
        log_warning "Generated files were created with different protobuf version"
        return 0  # Need to clean
    fi

    log_success "Protobuf version compatibility check passed (container: $container_protoc_version)"
    return 1  # No need to clean
}

# Clean generated files only when necessary
clean_generated_files_if_needed() {
    if check_protobuf_version_mismatch; then
        log_info "Cleaning generated files due to protobuf version mismatch..."
        docker run "${DOCKER_RUN_OPTS[@]}" "$DOCKER_IMAGE" bash -c "make -C gensrc clean || true"
        docker run "${DOCKER_RUN_OPTS[@]}" "$DOCKER_IMAGE" bash -c "rm -rf fe/fe-core/target/generated-sources || true"
        log_success "Generated files cleaned - will regenerate with correct protobuf version"
    else
        log_info "Skipping gensrc clean - no protobuf version mismatch detected"
    fi
}

# Run build
run_build() {
    local build_cmd="./build.sh"

    if [[ -n "$BUILD_ARGS" ]]; then
        build_cmd="$build_cmd$BUILD_ARGS"
    fi

    log_info "Starting build in Docker container"
    log_info "Container: $CONTAINER_NAME"
    log_info "Image: $DOCKER_IMAGE"
    log_info "Build command: $build_cmd"

    # Smart cleaning - only clean when protobuf version mismatch detected
    clean_generated_files_if_needed

    # Run the build
    if docker run "${DOCKER_RUN_OPTS[@]}" "$DOCKER_IMAGE" bash -c "$build_cmd"; then
        log_success "Build completed successfully!"

        # Show output directory contents
        if [[ -d "$REPO_ROOT/output" ]]; then
            log_info "Build artifacts in output directory:"
            ls -la "$REPO_ROOT/output/"
        fi

        return 0
    else
        log_error "Build failed!"
        return 1
    fi
}

# Run tests
run_tests() {
    log_info "Running tests in Docker container"

    # Determine which tests to run based on build arguments
    local run_fe_tests=false
    local run_be_tests=false

    if [[ "$BUILD_ARGS" == *"--fe"* ]] && [[ "$BUILD_ARGS" != *"--be"* ]]; then
        run_fe_tests=true
    elif [[ "$BUILD_ARGS" == *"--be"* ]] && [[ "$BUILD_ARGS" != *"--fe"* ]]; then
        run_be_tests=true
    else
        # Default: run both if no specific component specified
        run_fe_tests=true
        run_be_tests=true
    fi

    if [[ "$run_fe_tests" == "true" ]]; then
        log_info "Running Frontend tests..."
        docker run "${DOCKER_RUN_OPTS[@]}" "$DOCKER_IMAGE" bash -c "./run-fe-ut.sh || echo 'FE tests completed with some failures'"
    fi

    if [[ "$run_be_tests" == "true" ]]; then
        log_info "Running Backend tests..."
        docker run "${DOCKER_RUN_OPTS[@]}" "$DOCKER_IMAGE" bash -c "./run-be-ut.sh || echo 'BE tests completed with some failures'"
    fi
}

# Cleanup function
cleanup() {
    if docker ps -q -f name="$CONTAINER_NAME" &> /dev/null; then
        log_info "Cleaning up container: $CONTAINER_NAME"
        docker stop "$CONTAINER_NAME" &> /dev/null || true
    fi
}

# Main function
main() {
    # Set up cleanup trap
    trap cleanup EXIT

    # Parse arguments
    parse_args "$@"

    # Check prerequisites
    check_docker
    pull_image
    setup_docker_run

    # Execute requested action
    if [[ "$INTERACTIVE_SHELL" == "true" ]]; then
        run_shell
    elif [[ "$RUN_TESTS" == "true" ]]; then
        run_build && run_tests
    else
        # Default build args if none specified
        if [[ -z "$BUILD_ARGS" ]]; then
            BUILD_ARGS=" --fe --be"
        fi
        run_build
    fi
}

# Run main function
main "$@"
