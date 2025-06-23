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

Usage: $0 <command>

COMMANDS:
    shell           Open interactive shell in dev environment
    build-fe        Build Frontend only
    build-be        Build Backend only
    build-all       Build both Frontend and Backend
    clean-build     Clean and build all
    test-fe         Run Frontend tests
    test-be         Run Backend tests
    test-all        Run all tests

EXAMPLES:
    $0 shell           # Open development shell
    $0 build-fe         # Build Frontend
    $0 clean-build      # Clean and build everything
    $0 test-fe          # Run Frontend tests

EOF
}

case "${1:-}" in
    shell)
        log_info "Opening development shell..."
        ./docker-build.sh --shell
        ;;
    build-fe)
        log_info "Building Frontend..."
        ./docker-build.sh --fe
        ;;
    build-be)
        log_info "Building Backend..."
        ./docker-build.sh --be
        ;;
    build-all)
        log_info "Building all components..."
        ./docker-build.sh --fe --be
        ;;
    clean-build)
        log_info "Clean building all components..."
        ./docker-build.sh --fe --be --clean
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
        ./docker-build.sh --test
        ;;
    *)
        usage
        exit 1
        ;;
esac
