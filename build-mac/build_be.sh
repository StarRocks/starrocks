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
BUILD_TYPE="ASAN"
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

    # Check for required tools and use thirdparty versions
    export PATH="$STARROCKS_THIRDPARTY/installed/bin:$PATH"

    # Verify tools are available
    for tool in protoc thrift; do
        if ! command -v $tool &>/dev/null; then
            log_error "Can't find command tool '$tool' in $STARROCKS_THIRDPARTY/installed/bin/"
            exit 1
        fi
    done

    log_info "Using protoc: $(which protoc)"
    log_info "Using thrift: $(which thrift)"

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

    # Ensure the gensrc make uses the correct tools from thirdparty
    export STARROCKS_THIRDPARTY="$STARROCKS_THIRDPARTY"
    export PATH="$STARROCKS_THIRDPARTY/installed/bin:$PATH"

    # Run make with explicit environment
    make

    if [[ $? -ne 0 ]]; then
        log_error "Generated code build failed"
        exit 1
    fi

    # Verify that both protobuf and thrift files were generated
    PROTO_FILES=$(find "$ROOT_DIR/gensrc/build/gen_cpp" -name "*.pb.cc" -o -name "*.pb.h" | wc -l)
    THRIFT_FILES=$(find "$ROOT_DIR/gensrc/build/gen_cpp" -name "*_types.cc" -o -name "*_types.h" -o -name "*_service.cc" -o -name "*_service.h" | wc -l)

    log_info "Generated $PROTO_FILES protobuf files and $THRIFT_FILES thrift files"

    if [[ $PROTO_FILES -eq 0 ]]; then
        log_warn "No protobuf files were generated"
    fi

    if [[ $THRIFT_FILES -eq 0 ]]; then
        log_warn "No thrift files were generated - checking thrift generation..."
        # Try to generate thrift files explicitly
        cd "$ROOT_DIR/gensrc/thrift"
        export STARROCKS_THIRDPARTY="$STARROCKS_THIRDPARTY"
        export PATH="$STARROCKS_THIRDPARTY/installed/bin:$PATH"
        make
        # Return to gensrc directory
        cd "$ROOT_DIR/gensrc"
    fi

    log_success "Generated code built successfully"
else
    log_info "Generated code is up to date, skipping build"
fi

# Copy generated files to BE source directory (only if needed)
# Note: Files should be copied to be/src/gen_cpp for direct inclusion
BE_GEN_CPP_DIR="$ROOT_DIR/be/src/gen_cpp"
BE_GEN_BUILD_DIR="$BE_GEN_CPP_DIR/build"
BE_GEN_OPCODE_DIR="$BE_GEN_CPP_DIR/opcode"

# Always create target directories
mkdir -p "$BE_GEN_BUILD_DIR"
mkdir -p "$BE_GEN_OPCODE_DIR"

if [[ $GENSRC_NEEDS_BUILD -eq 1 ]] || [[ $CLEAN_BUILD -eq 1 ]]; then
    log_info "Copying generated files to BE source directory..."

    # Copy protobuf and thrift generated files to be/src/gen_cpp/build/
    # Use -f flag to force overwrite and avoid hanging on identical files
    cp -rf "$ROOT_DIR/gensrc/build/gen_cpp/"* "$BE_GEN_BUILD_DIR/" 2>/dev/null || true

    # Copy opcode files to be/src/gen_cpp/opcode/
    cp -rf "$ROOT_DIR/gensrc/build/opcode/"* "$BE_GEN_OPCODE_DIR/" 2>/dev/null || true

    # Also copy to the nested build/gen_cpp structure for compatibility
    # This is needed for some include paths in the source code
    BE_NESTED_GEN_DIR="$BE_GEN_BUILD_DIR/gen_cpp"
    mkdir -p "$BE_NESTED_GEN_DIR"
    # Copy from gensrc directly to avoid copying from a directory we just copied to
    cp -rf "$ROOT_DIR/gensrc/build/gen_cpp/"* "$BE_NESTED_GEN_DIR/" 2>/dev/null || true

    # Verify files were copied
    COPIED_PROTO_FILES=$(find "$BE_GEN_BUILD_DIR" -name "*.pb.cc" -o -name "*.pb.h" | wc -l)
    COPIED_THRIFT_FILES=$(find "$BE_GEN_BUILD_DIR" -name "*_types.cc" -o -name "*_types.h" -o -name "*_service.cc" -o -name "*_service.h" | wc -l)
    COPIED_OPCODE_FILES=$(find "$BE_GEN_OPCODE_DIR" -name "*.inc" | wc -l)

    log_info "Copied $COPIED_PROTO_FILES protobuf files, $COPIED_THRIFT_FILES thrift files, and $COPIED_OPCODE_FILES opcode files"

    if [[ $COPIED_PROTO_FILES -eq 0 ]]; then
        log_warn "No protobuf files were copied to BE source directory"
    fi

    if [[ $COPIED_THRIFT_FILES -eq 0 ]]; then
        log_warn "No thrift files were copied to BE source directory"
    fi

    log_success "Generated files copied to BE source directory"
else
    log_info "Generated files are up to date, skipping copy"
fi

else
    log_info "Skipping code generation step (--skip-codegen specified)"
fi

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
log_info "CMake command: cmake ${CMAKE_FLAGS[*]} $CMAKE_FILE"

# Show environment variables for debugging
log_info "Build environment variables:"
log_info "  CC: ${CC:-not set}"
log_info "  CXX: ${CXX:-not set}"
log_info "  CMAKE_PREFIX_PATH: ${CMAKE_PREFIX_PATH:-not set}"
log_info "  PKG_CONFIG_PATH: ${PKG_CONFIG_PATH:-not set}"
log_info "  OPENSSL_ROOT_DIR: ${OPENSSL_ROOT_DIR:-not set}"
log_info "  STARROCKS_THIRDPARTY: ${STARROCKS_THIRDPARTY:-not set}"

cmake "${CMAKE_FLAGS[@]}" "$CMAKE_FILE"
CMAKE_RESULT=$?

log_info "CMake configuration completed with exit code: $CMAKE_RESULT"

if [[ $CMAKE_RESULT -ne 0 ]]; then
    log_error "CMake configuration failed with exit code: $CMAKE_RESULT"
    log_error "CMake flags: ${CMAKE_FLAGS[*]}"
    log_error "CMake file: $CMAKE_FILE"
    exit 1
fi

log_success "CMake configuration completed"

# Show what targets were configured
log_info "Configured targets:"
ninja -t targets 2>/dev/null | grep -E "(starrocks|install)" || log_warn "Could not find starrocks or install targets"

# ============================================================================
# BUILD EXECUTION
# ============================================================================
log_info "Starting build process..."

# Determine build targets
if [[ $FULL_BUILD -eq 1 ]]; then
    BUILD_TARGETS=""  # Build everything
    log_info "Building all targets (FULL_BUILD)"
else
    BUILD_TARGETS="starrocks_be"  # Just the main target
    log_info "Building target: $BUILD_TARGETS"
fi

# Build with Ninja
NINJA_FLAGS=("-j" "$PARALLEL_JOBS")
if [[ -n "$VERBOSE_FLAG" ]]; then
    NINJA_FLAGS+=("$VERBOSE_FLAG")
fi

log_info "Ninja flags: ${NINJA_FLAGS[*]}"
log_info "Build directory: $BUILD_DIR"

# Check what targets are available
log_info "Available ninja targets:"
ninja -t targets 2>/dev/null | head -20 || log_warn "Could not list ninja targets"

# Start build with detailed logging
log_info "Starting ninja build..."
if [[ -n "$BUILD_TARGETS" ]]; then
    log_info "Executing: ninja ${NINJA_FLAGS[*]} $BUILD_TARGETS"
    ninja "${NINJA_FLAGS[@]}" $BUILD_TARGETS
    BUILD_RESULT=$?
else
    log_info "Executing: ninja ${NINJA_FLAGS[*]}"
    ninja "${NINJA_FLAGS[@]}"
    BUILD_RESULT=$?
fi

log_info "Build command completed with exit code: $BUILD_RESULT"

if [[ $BUILD_RESULT -ne 0 ]]; then
    log_error "Build failed with exit code: $BUILD_RESULT"

    # Show build errors
    log_error "Build errors summary:"
    ninja -t errors 2>/dev/null || log_warn "Could not retrieve build errors"

    # Show what failed to build
    log_error "Failed targets:"
    ninja -t targets failed 2>/dev/null || log_warn "Could not list failed targets"

    exit 1
fi

log_success "Build completed successfully"

# Check if starrocks_be was built
BUILD_BINARY="$BUILD_DIR/src/service/starrocks_be"
if [[ -f "$BUILD_BINARY" ]]; then
    log_success "StarRocks BE binary found in build directory: $BUILD_BINARY"
    BINARY_SIZE=$(stat -f%z "$BUILD_BINARY" 2>/dev/null || echo "unknown")
    if [[ "$BINARY_SIZE" != "unknown" ]]; then
        BINARY_SIZE_MB=$((BINARY_SIZE / 1024 / 1024))
        log_info "Build binary size: ${BINARY_SIZE_MB}MB"
    fi
else
    log_warn "StarRocks BE binary not found in build directory: $BUILD_BINARY"
    log_info "Looking for alternative locations..."
    find "$BUILD_DIR" -name "starrocks_be" -type f 2>/dev/null | while read -r file; do
        log_info "Found: $file"
    done
fi

# ============================================================================
# INSTALLATION
# ============================================================================
if [[ $DO_INSTALL -eq 1 ]]; then
    log_info "Starting installation process..."

    # Check if install target exists
    log_info "Checking install target..."
    ninja -t targets | grep -E "(install|starrocks_be)" || log_warn "Install or starrocks_be target not found in ninja targets"

    log_info "Executing: ninja install"
    ninja install
    INSTALL_RESULT=$?

    log_info "Install command completed with exit code: $INSTALL_RESULT"

    if [[ $INSTALL_RESULT -ne 0 ]]; then
        log_error "Installation failed with exit code: $INSTALL_RESULT"

        # Show install errors
        log_error "Installation errors summary:"
        ninja -t errors 2>/dev/null || log_warn "Could not retrieve installation errors"

        exit 1
    fi

    log_success "Installation command completed successfully"

    # Verify installation
    BE_BINARY="${ROOT_DIR}/be/output/lib/starrocks_be"
    log_info "Verifying installation at: $BE_BINARY"

    if [[ -f "$BE_BINARY" ]]; then
        log_success "StarRocks BE installed: $BE_BINARY"

        # Show binary info
        BINARY_SIZE=$(stat -f%z "$BE_BINARY" 2>/dev/null || echo "unknown")
        if [[ "$BINARY_SIZE" != "unknown" ]]; then
            BINARY_SIZE_MB=$((BINARY_SIZE / 1024 / 1024))
            log_info "Installed binary size: ${BINARY_SIZE_MB}MB"
        fi

        # Check if it's executable and properly linked
        if [[ -x "$BE_BINARY" ]]; then
            log_success "Binary is executable"
        else
            log_warn "Binary is not executable"
        fi

        if otool -L "$BE_BINARY" >/dev/null 2>&1; then
            log_info "Binary is properly linked for macOS"
            log_info "Binary dependencies:"
            otool -L "$BE_BINARY" | head -10
        else
            log_warn "Binary linking check failed"
        fi

        # Check if it can run --help
        log_info "Testing binary execution..."
        if timeout 10 "$BE_BINARY" --help >/dev/null 2>&1; then
            log_success "Binary can execute successfully"
        else
            log_warn "Binary execution test failed"
        fi
    else
        log_error "Binary not found after installation: $BE_BINARY"
        log_info "Checking be/output/lib directory contents:"
        ls -la "${ROOT_DIR}/be/output/lib/" 2>/dev/null || log_warn "Could not list lib directory"

        log_info "Checking be/output directory structure:"
        find "${ROOT_DIR}/be/output" -name "*starrocks*" -type f 2>/dev/null | while read -r file; do
            log_info "Found starrocks file: $file"
        done || log_warn "No starrocks files found in output directory"

        exit 1
    fi
else
    log_info "Skipping installation (--no-install specified)"
    log_info "Binary would be located at: $BUILD_DIR/src/service/starrocks_be"

    # Even if skipping install, check if binary exists in build directory
    BUILD_BINARY="$BUILD_DIR/src/service/starrocks_be"
    if [[ -f "$BUILD_BINARY" ]]; then
        log_success "Build binary exists: $BUILD_BINARY"
        BINARY_SIZE=$(stat -f%z "$BUILD_BINARY" 2>/dev/null || echo "unknown")
        if [[ "$BINARY_SIZE" != "unknown" ]]; then
            BINARY_SIZE_MB=$((BINARY_SIZE / 1024 / 1024))
            log_info "Build binary size: ${BINARY_SIZE_MB}MB"
        fi
    else
        log_warn "Build binary not found: $BUILD_BINARY"
    fi
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
