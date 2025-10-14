#!/bin/bash
# StarRocks BE macOS Environment Configuration
# This file sets up all environment variables required for building StarRocks BE on macOS ARM64

# Prevent multiple sourcing
if [[ -n "${STARROCKS_MACOS_ENV_LOADED:-}" ]]; then
    return 0
fi
export STARROCKS_MACOS_ENV_LOADED=1

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[ENV]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[ENV-WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ENV-ERROR]${NC} $1"
}

# ============================================================================
# BASIC PATHS
# ============================================================================
export STARROCKS_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export STARROCKS_BE_HOME="$STARROCKS_HOME/be"
export STARROCKS_THIRDPARTY="$STARROCKS_HOME/thirdparty"
export STARROCKS_BUILD_MAC="$STARROCKS_HOME/build-mac"

log_info "StarRocks home: $STARROCKS_HOME"

# ============================================================================
# SYSTEM VALIDATION
# ============================================================================
if [[ "$(uname -s)" != "Darwin" ]]; then
    log_error "This environment is only for macOS"
    return 1
fi

if [[ "$(uname -m)" != "arm64" ]]; then
    log_error "This environment is only for Apple Silicon (ARM64)"
    return 1
fi

# Check Homebrew
if ! command -v brew >/dev/null 2>&1; then
    log_error "Homebrew is not installed. Please install it from https://brew.sh"
    return 1
fi

# ============================================================================
# HOMEBREW CONFIGURATION
# ============================================================================
export HOMEBREW_PREFIX="/opt/homebrew"
export HOMEBREW_NO_AUTO_UPDATE=1  # Speed up builds

# Add Homebrew to PATH if not already there
if [[ ":$PATH:" != *":$HOMEBREW_PREFIX/bin:"* ]]; then
    export PATH="$HOMEBREW_PREFIX/bin:$HOMEBREW_PREFIX/sbin:$PATH"
fi

# ============================================================================
# COMPILER CONFIGURATION
# ============================================================================
# Use Homebrew LLVM for consistent C++17 support
export LLVM_HOME="$HOMEBREW_PREFIX/opt/llvm"

if [[ ! -d "$LLVM_HOME" ]]; then
    log_error "LLVM not found at $LLVM_HOME. Please run: brew install llvm"
    return 1
fi

export CC="$LLVM_HOME/bin/clang"
export CXX="$LLVM_HOME/bin/clang++"
export AR="$LLVM_HOME/bin/llvm-ar"
export RANLIB="$LLVM_HOME/bin/llvm-ranlib"
export STRIP="$LLVM_HOME/bin/llvm-strip"

# StarRocks-specific compiler environment variables
export STARROCKS_GCC_HOME="$LLVM_HOME"
export STARROCKS_LLVM_HOME="$LLVM_HOME"

# ============================================================================
# MACOS SDK CONFIGURATION
# ============================================================================
export SDKROOT="$(xcrun --show-sdk-path)"
export MACOSX_DEPLOYMENT_TARGET="15.0"

if [[ ! -d "$SDKROOT" ]]; then
    log_error "macOS SDK not found. Please install Xcode Command Line Tools"
    return 1
fi

# ============================================================================
# TARGET ARCHITECTURE
# ============================================================================
export CMAKE_BUILD_TARGET_ARCH="aarch64"
export CMAKE_OSX_ARCHITECTURES="arm64"

# ============================================================================
# COMPILER FLAGS
# ============================================================================
# Base flags for ARM64 optimization
export ARM64_FLAGS="-march=armv8-a"
export OPT_FLAGS="-O3 -DNDEBUG"

# C flags
export CFLAGS="${ARM64_FLAGS} ${OPT_FLAGS}"

# C++ flags - use libc++ (not libstdc++)
export CXXFLAGS="${ARM64_FLAGS} ${OPT_FLAGS} -stdlib=libc++ -std=gnu++20"

# Add Homebrew include/lib paths
export CPPFLAGS="-I${HOMEBREW_PREFIX}/include ${CPPFLAGS:-}"
export LDFLAGS="-L${HOMEBREW_PREFIX}/lib ${LDFLAGS:-}"

# ============================================================================
# BUILD OPTIMIZATION
# ============================================================================
# ccache for faster rebuilds
export CCACHE_DIR="$HOME/.ccache"
export CCACHE_MAXSIZE="50G"
export USE_CCACHE=1

# Parallel builds
export PARALLEL="$(sysctl -n hw.ncpu)"
export MAKEFLAGS="-j${PARALLEL}"

log_info "Parallel jobs: $PARALLEL"

# ============================================================================
# THIRD-PARTY LIBRARIES CONFIGURATION
# ============================================================================

# OpenSSL (keg-only in Homebrew)
export OPENSSL_ROOT_DIR="$HOMEBREW_PREFIX/opt/openssl@3"
if [[ -d "$OPENSSL_ROOT_DIR" ]]; then
    export PKG_CONFIG_PATH="$OPENSSL_ROOT_DIR/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
    export CPPFLAGS="-I$OPENSSL_ROOT_DIR/include $CPPFLAGS"
    export LDFLAGS="-L$OPENSSL_ROOT_DIR/lib $LDFLAGS"
else
    log_warn "OpenSSL not found at $OPENSSL_ROOT_DIR. Please run: brew install openssl@3"
fi

# cURL (keg-only in Homebrew)
export CURL_ROOT="$HOMEBREW_PREFIX/opt/curl"
if [[ -d "$CURL_ROOT" ]]; then
    export PKG_CONFIG_PATH="$CURL_ROOT/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
    export CPPFLAGS="-I$CURL_ROOT/include $CPPFLAGS"
    export LDFLAGS="-L$CURL_ROOT/lib $LDFLAGS"
fi

# ICU4C (keg-only in Homebrew)
export ICU_ROOT="$HOMEBREW_PREFIX/opt/icu4c"
if [[ -d "$ICU_ROOT" ]]; then
    export PKG_CONFIG_PATH="$ICU_ROOT/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
    export CPPFLAGS="-I$ICU_ROOT/include $CPPFLAGS"
    export LDFLAGS="-L$ICU_ROOT/lib $LDFLAGS"
fi

# Third-party install directory
export THIRDPARTY_INSTALL_DIR="$STARROCKS_THIRDPARTY/installed"
if [[ -d "$THIRDPARTY_INSTALL_DIR" ]]; then
    export PKG_CONFIG_PATH="$THIRDPARTY_INSTALL_DIR/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
    export CPPFLAGS="-I$THIRDPARTY_INSTALL_DIR/include $CPPFLAGS"
    export LDFLAGS="-L$THIRDPARTY_INSTALL_DIR/lib $LDFLAGS"
    export PATH="$THIRDPARTY_INSTALL_DIR/bin:$PATH"
fi

# ============================================================================
# FEATURE CONFIGURATION
# ============================================================================
# Disable incompatible features
export DISABLE_SSE=1
export DISABLE_AVX=1
export DISABLE_BREAKPAD=1
export DISABLE_STARCACHE=1

# ============================================================================
# CMAKE CONFIGURATION
# ============================================================================
# Force static linking for better compatibility
export CMAKE_FIND_LIBRARY_SUFFIXES=".a"
export BUILD_SHARED_LIBS=OFF

# CMake generator
export CMAKE_GENERATOR="Ninja"

# CMake build type
export CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE:-Release}"

# ============================================================================
# RUNTIME ENVIRONMENT (for running StarRocks BE)
# ============================================================================
export ASAN_OPTIONS="halt_on_error=1:abort_on_error=1:replace_str=0:replace_intrin=0:detect_odr_violation=0:fast_unwind_on_malloc=0:intercept_strlen=0:intercept_strchr=0:intercept_strndup=0"
export MallocNanoZone=0
export STARROCKS_HOME_RUNTIME="$STARROCKS_BE_HOME/output"
export PID_DIR="$STARROCKS_HOME_RUNTIME/bin"
export UDF_RUNTIME_DIR="$STARROCKS_HOME_RUNTIME/lib"

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================
# Function to verify environment
verify_env() {
    log_info "Verifying macOS build environment..."

    local errors=0

    # Check compilers
    if [[ ! -x "$CC" ]]; then
        log_error "C compiler not found: $CC"
        ((errors++))
    fi

    if [[ ! -x "$CXX" ]]; then
        log_error "C++ compiler not found: $CXX"
        ((errors++))
    fi

    # Check essential tools
    local required_tools=("cmake" "ninja" "git")
    for tool in "${required_tools[@]}"; do
        if ! command -v "$tool" >/dev/null 2>&1; then
            log_error "Required tool not found: $tool"
            ((errors++))
        fi
    done

    # Check key libraries
    if [[ ! -d "$OPENSSL_ROOT_DIR" ]]; then
        log_error "OpenSSL not found. Please run: brew install openssl@3"
        ((errors++))
    fi

    if [[ $errors -gt 0 ]]; then
        log_error "$errors error(s) found in environment"
        return 1
    fi

    log_info "Environment verification passed"
    return 0
}

# Function to show environment summary
show_env() {
    cat << EOF

================================================
  StarRocks macOS Build Environment
================================================
OS:                 $(uname -s) $(uname -r)
Architecture:       $(uname -m)
Xcode:              $(xcode-select -p 2>/dev/null || echo "Not found")
Homebrew:           $HOMEBREW_PREFIX

Compilers:
  CC:               $CC
  CXX:              $CXX
  AR:               $AR
  RANLIB:           $RANLIB

Paths:
  StarRocks:        $STARROCKS_HOME
  Third-party:      $STARROCKS_THIRDPARTY
  Build-mac:        $STARROCKS_BUILD_MAC

Build Configuration:
  Build Type:       $CMAKE_BUILD_TYPE
  Parallel Jobs:    $PARALLEL
  Generator:        $CMAKE_GENERATOR

Key Libraries:
  OpenSSL:          $OPENSSL_ROOT_DIR
  LLVM:             $LLVM_HOME

================================================
EOF
}

# Show environment if not sourced quietly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]] || [[ "${STARROCKS_ENV_QUIET:-}" != "1" ]]; then
    show_env
fi

log_info "macOS build environment loaded successfully"