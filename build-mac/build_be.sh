#!/usr/bin/env bash

# StarRocks BE Builder for macOS ARM64
# This script provides a one-click solution to build StarRocks BE on macOS

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="${BUILD_DIR:-$SCRIPT_DIR/build}"
CMAKE_FILE="$SCRIPT_DIR/CMakeLists.txt"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

usage() {
    cat << EOF
StarRocks BE Builder for macOS ARM64

Usage: $(basename "$0") [OPTIONS]

Options:
    -h, --help              Show this help message
    --clean                 Remove build directory before configuring
    --full                  Build all targets (equivalent to 'ninja')
    --no-install            Skip 'ninja install' (binary stays in build tree)
    --release               Build in Release mode (default)
    --debug                 Build in Debug mode for development
    --asan                  Build with AddressSanitizer (Debug mode)
    --thirdparty-only       Only build third-party dependencies
    --skip-thirdparty       Skip third-party dependency check
    --skip-codegen          Skip code generation step (gensrc)
    --parallel N            Set parallel jobs (default: $(sysctl -n hw.ncpu))
    --verbose               Enable verbose build output

Environment Variables:
    STARROCKS_HOME          StarRocks root directory (default: $ROOT_DIR)
    STARROCKS_THIRDPARTY    Third-party directory (default: \$STARROCKS_HOME/thirdparty)
    BUILD_DIR               Build directory (default: $BUILD_DIR)

Examples:
    $(basename "$0")                           # Standard release build (incremental)
    $(basename "$0") --clean                   # Clean release build (full rebuild)
    $(basename "$0") --debug --asan           # Debug build with AddressSanitizer
    $(basename "$0") --thirdparty-only        # Only build dependencies
    $(basename "$0") --skip-codegen           # Skip code generation (fastest incremental)
    $(basename "$0") --skip-thirdparty --skip-codegen # Skip deps and codegen (fastest)
EOF
}

# Parse command line arguments
CLEAN_BUILD=0
FULL_BUILD=0
DO_INSTALL=1
BUILD_TYPE="Release"
USE_SANITIZER_FLAG=""
THIRDPARTY_ONLY=0
SKIP_THIRDPARTY=0
SKIP_CODEGEN=0
PARALLEL_JOBS=$(sysctl -n hw.ncpu)
VERBOSE_FLAG=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        --clean)
            CLEAN_BUILD=1
            shift
            ;;
        --full)
            FULL_BUILD=1
            shift
            ;;
        --no-install)
            DO_INSTALL=0
            shift
            ;;
        --release)
            BUILD_TYPE="Release"
            shift
            ;;
        --debug)
            BUILD_TYPE="Debug"
            shift
            ;;
        --asan)
            BUILD_TYPE="Debug"
            USE_SANITIZER_FLAG="-DUSE_SANITIZER=ON"
            shift
            ;;
        --thirdparty-only)
            THIRDPARTY_ONLY=1
            shift
            ;;
        --skip-thirdparty)
            SKIP_THIRDPARTY=1
            shift
            ;;
        --skip-codegen)
            SKIP_CODEGEN=1
            shift
            ;;
        --parallel)
            PARALLEL_JOBS="$2"
            shift 2
            ;;
        --verbose)
            VERBOSE_FLAG="-v"
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

log_info "=== StarRocks BE Builder for macOS ARM64 ==="
log_info "Root directory: $ROOT_DIR"
log_info "Build directory: $BUILD_DIR"
log_info "Build type: $BUILD_TYPE"
log_info "Parallel jobs: $PARALLEL_JOBS"

# ============================================================================
# ENVIRONMENT SETUP
# ============================================================================
log_info "Setting up build environment..."

# Load environment variables
ENV_SCRIPT="$SCRIPT_DIR/env_macos.sh"
if [[ -f "$ENV_SCRIPT" ]]; then
    export STARROCKS_ENV_QUIET=1  # Don't show env summary
    set +u  # Temporarily allow unset variables
    # shellcheck source=/dev/null
    source "$ENV_SCRIPT"
    set -u
    log_success "Environment loaded"
else
    log_error "Environment script not found: $ENV_SCRIPT"
    exit 1
fi

# Validate environment
if [[ "$(uname -s)" != "Darwin" ]]; then
    log_error "This script is only for macOS"
    exit 1
fi

if [[ "$(uname -m)" != "arm64" ]]; then
    log_error "This script is only for Apple Silicon (ARM64)"
    exit 1
fi

# Set default STARROCKS_THIRDPARTY if not set
if [[ -z "${STARROCKS_THIRDPARTY:-}" ]]; then
    export STARROCKS_THIRDPARTY="$ROOT_DIR/thirdparty"
fi

log_info "Third-party directory: $STARROCKS_THIRDPARTY"

# ============================================================================
# THIRD-PARTY DEPENDENCIES
# ============================================================================
if [[ $THIRDPARTY_ONLY -eq 1 ]]; then
    log_info "Building third-party dependencies only..."
    "$SCRIPT_DIR/build_thirdparty.sh" --parallel "$PARALLEL_JOBS"
    log_success "Third-party dependencies built successfully"
    exit 0
fi

if [[ $SKIP_THIRDPARTY -eq 0 ]]; then
    log_info "Checking third-party dependencies..."

    # Required libraries
    required_libs=(
        "libgflags.a"
        "libglog.a"
        "libprotobuf.a"
        "libleveldb.a"
        "libbrpc.a"
        "librocksdb.a"
        "libvelocypack.a"
        "libbitshuffle.a"
        # vectorscan (hyperscan ABI)
        "libhs.a"
        "libhs_runtime.a"
    )

    missing_libs=()
    install_dir="$STARROCKS_THIRDPARTY/installed"

    for lib in "${required_libs[@]}"; do
        if [[ ! -f "$install_dir/lib/$lib" ]]; then
            missing_libs+=("$lib")
        fi
    done

    if [[ ${#missing_libs[@]} -gt 0 ]]; then
        log_warn "Missing third-party libraries: ${missing_libs[*]}"
        log_info "Building third-party dependencies..."
        "$SCRIPT_DIR/build_thirdparty.sh" --parallel "$PARALLEL_JOBS"
    else
        log_success "All third-party dependencies found"
    fi
fi

# ============================================================================
# CODE GENERATION
# ============================================================================
if [[ $SKIP_CODEGEN -eq 0 ]]; then
    log_info "Generating Thrift and Protobuf code..."

    # Check for required tools
    for tool in protoc thrift; do
        if ! command -v $tool &>/dev/null; then
            # Check in thirdparty
            if [[ -f "$STARROCKS_THIRDPARTY/installed/bin/$tool" ]]; then
                export PATH="$STARROCKS_THIRDPARTY/installed/bin:$PATH"
            else
                log_error "Can't find command tool '$tool'!"
                exit 1
            fi
        fi
    done

# Generate code from gensrc
cd "$ROOT_DIR/gensrc"
if [[ $CLEAN_BUILD -eq 1 ]]; then
    log_info "Cleaning generated code..."
    make clean
fi

# Check if generated code needs to be rebuilt
GENSRC_TARGET_DIR="$ROOT_DIR/gensrc/build/gen_cpp"
GENSRC_NEEDS_BUILD=0

# Check if generated files exist
if [[ ! -d "$GENSRC_TARGET_DIR" ]] || [[ -z "$(ls -A "$GENSRC_TARGET_DIR" 2>/dev/null)" ]]; then
    GENSRC_NEEDS_BUILD=1
else
    # Check if any .proto or .thrift files are newer than generated files
    newest_generated=$(find "$GENSRC_TARGET_DIR" -type f -name "*.cc" -o -name "*.h" | xargs stat -f "%m" 2>/dev/null | sort -n | tail -1)
    newest_source=$(find "$ROOT_DIR/gensrc" -name "*.proto" -o -name "*.thrift" | xargs stat -f "%m" 2>/dev/null | sort -n | tail -1)

    if [[ -n "$newest_source" ]] && [[ -n "$newest_generated" ]] && [[ $newest_source -gt $newest_generated ]]; then
        GENSRC_NEEDS_BUILD=1
    fi
fi

if [[ $GENSRC_NEEDS_BUILD -eq 1 ]]; then
    log_info "Building generated code (incremental - source files changed)..."
    make

    if [[ $? -ne 0 ]]; then
        log_error "Generated code build failed"
        exit 1
    fi
    log_success "Generated code built successfully"
else
    log_info "Generated code is up to date, skipping build"
fi

# Copy generated files to BE source directory (only if needed)
BE_GEN_DIR="$ROOT_DIR/be/src/gen_cpp/build"
BE_OPCODE_DIR="$ROOT_DIR/be/src/gen_cpp/opcode"

# Always create target directories
mkdir -p "$BE_GEN_DIR"
mkdir -p "$BE_OPCODE_DIR"

if [[ $GENSRC_NEEDS_BUILD -eq 1 ]] || [[ $CLEAN_BUILD -eq 1 ]]; then
    log_info "Copying generated files to BE source directory..."
    cp -r "$ROOT_DIR/gensrc/build/gen_cpp/"* "$BE_GEN_DIR/"
    cp -r "$ROOT_DIR/gensrc/build/opcode/"* "$BE_OPCODE_DIR/" 2>/dev/null || true
    log_success "Generated files copied"
else
    log_info "Generated files are up to date, skipping copy"
fi

else
    log_info "Skipping code generation step (--skip-codegen specified)"
fi

cd "$BUILD_DIR"

# ============================================================================
# CMAKE CONFIGURATION
# ============================================================================
log_info "Configuring CMake build system..."

# Clean build directory if requested
if [[ $CLEAN_BUILD -eq 1 ]]; then
    log_info "Cleaning build directory..."
    rm -rf "$BUILD_DIR"
fi

# Create build directory
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# Prepare CMake flags
CMAKE_FLAGS=(
    "-G" "Ninja"
    "-DCMAKE_BUILD_TYPE=$BUILD_TYPE"
    "-DCMAKE_EXPORT_COMPILE_COMMANDS=ON"
    "-DCMAKE_INSTALL_PREFIX=${ROOT_DIR}/be/output"
)

if [[ -n "$USE_SANITIZER_FLAG" ]]; then
    CMAKE_FLAGS+=("$USE_SANITIZER_FLAG")
fi

# Run CMake configuration
log_info "Running CMake configuration..."
cmake "${CMAKE_FLAGS[@]}" "$CMAKE_FILE"

if [[ $? -ne 0 ]]; then
    log_error "CMake configuration failed"
    exit 1
fi

log_success "CMake configuration completed"

# ============================================================================
# BUILD EXECUTION
# ============================================================================
log_info "Starting build process..."

# Determine build targets
if [[ $FULL_BUILD -eq 1 ]]; then
    BUILD_TARGETS=""  # Build everything
else
    BUILD_TARGETS="starrocks_be"  # Just the main target
fi

# Build with Ninja
NINJA_FLAGS=("-j" "$PARALLEL_JOBS")
if [[ -n "$VERBOSE_FLAG" ]]; then
    NINJA_FLAGS+=("$VERBOSE_FLAG")
fi

if [[ -n "$BUILD_TARGETS" ]]; then
    ninja "${NINJA_FLAGS[@]}" $BUILD_TARGETS
else
    ninja "${NINJA_FLAGS[@]}"
fi

if [[ $? -ne 0 ]]; then
    log_error "Build failed"
    exit 1
fi

log_success "Build completed successfully"

# ============================================================================
# INSTALLATION
# ============================================================================
if [[ $DO_INSTALL -eq 1 ]]; then
    log_info "Installing binary..."
    ninja install

    if [[ $? -ne 0 ]]; then
        log_error "Installation failed"
        exit 1
    fi

    # Verify installation
    BE_BINARY="${ROOT_DIR}/be/output/lib/starrocks_be"
    if [[ -f "$BE_BINARY" ]]; then
        log_success "StarRocks BE installed: $BE_BINARY"

        # Show binary info
        BINARY_SIZE=$(stat -f%z "$BE_BINARY" 2>/dev/null || echo "unknown")
        if [[ "$BINARY_SIZE" != "unknown" ]]; then
            BINARY_SIZE_MB=$((BINARY_SIZE / 1024 / 1024))
            log_info "Binary size: ${BINARY_SIZE_MB}MB"
        fi

        # Check if it's properly linked
        if otool -L "$BE_BINARY" >/dev/null 2>&1; then
            log_info "Binary is properly linked for macOS"
        else
            log_warn "Binary linking check failed"
        fi
    else
        log_error "Binary not found after installation: $BE_BINARY"
        exit 1
    fi
else
    log_info "Skipping installation (--no-install specified)"
    log_info "Binary location: $BUILD_DIR/src/service/starrocks_be"
fi

# ============================================================================
# BUILD SUMMARY
# ============================================================================
BUILD_TIME=$(date)
log_success "=== Build Summary ==="
echo "Build Type:      $BUILD_TYPE"
echo "Parallel Jobs:   $PARALLEL_JOBS"
echo "Build Directory: $BUILD_DIR"
echo "Completed:       $BUILD_TIME"

if [[ $DO_INSTALL -eq 1 ]]; then
    echo "Installation:    ${ROOT_DIR}/be/output/lib/starrocks_be"
    echo ""
    echo "To run StarRocks BE:"
    echo "  cd ${ROOT_DIR}/be/output/lib"
    echo "  ./starrocks_be --help"
fi

log_success "StarRocks BE build completed successfully!"
