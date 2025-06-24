#!/usr/bin/env bash
# Simple wrapper for common Docker development tasks

set -euo pipefail

DOCKER_IMAGE="${STARROCKS_DEV_ENV_IMAGE:-starrocks/dev-env-ubuntu:latest}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

usage() {
    cat << EOF
Simple Docker development wrapper for StarRocks

Usage: $0 <command> [additional_options]

COMMANDS:
    shell           Open interactive shell in dev environment
    build-fe        Build Frontend only
    build-be        Build Backend only
    build-all       Build both Frontend and Backend
    clean-build     Clean and build all
    test-fe         Run Frontend tests
    test-be         Run Backend tests
    test-all        Run all tests
    build           Pass through any build.sh options directly

EXAMPLES:
    $0 shell                        # Open development shell
    $0 build-fe                     # Build Frontend
    $0 clean-build                  # Clean and build everything
    $0 test-fe                      # Run Frontend tests
    $0 build --be --with-gcov       # Pass through custom build options
    $0 build --fe --new-option      # Any new build.sh option works

EOF
}

case "${1:-}" in
    shell)
        log_info "Opening development shell..."
        ./build-in-docker.sh --shell
        ;;
    build-fe)
        log_info "Building Frontend..."
        ./build-in-docker.sh --fe
        ;;
    build-be)
        log_info "Building Backend..."
        ./build-in-docker.sh --be
        ;;
    build-all)
        log_info "Building all components..."
        ./build-in-docker.sh --fe --be
        ;;
    clean-build)
        log_info "Clean building all components..."
        ./build-in-docker.sh --fe --be --clean
        ;;
    test-fe)
        log_info "Running Frontend tests..."
        docker run --rm -v "$REPO_ROOT:/workspace" -w /workspace "$DOCKER_IMAGE" ./run-fe-ut.sh
        ;;
    test-be)
        log_info "Running Backend tests..."
        docker run --rm -v "$REPO_ROOT:/workspace" -w /workspace "$DOCKER_IMAGE" ./run-be-ut.sh
        ;;
    test-all)
        log_info "Running all tests..."
        ./build-in-docker.sh --test
        ;;
    build)
        # Pass through all remaining arguments to build-in-docker.sh
        shift
        ./build-in-docker.sh "$@"
        ;;
    *)
        usage
        exit 1
        ;;
esac
