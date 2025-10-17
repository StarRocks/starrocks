#!/usr/bin/env bash

# StarRocks macOS Third-party Libraries Builder
#
# This script builds all third-party dependencies required for StarRocks BE on macOS ARM64
#
# Usage: ./build_thirdparty.sh [--clean] [--homebrew-only] [--source-only]

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
THIRDPARTY_DIR="${ROOT_DIR}/thirdparty"
INSTALL_DIR="${THIRDPARTY_DIR}/installed"

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
    echo "Usage: $(basename "$0") [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help           Show this help message"
    echo "  --clean              Clean all build directories before building"
    echo "  --homebrew-only      Only install Homebrew dependencies"
    echo "  --source-only        Only build source dependencies (skip Homebrew)"
    echo "  --parallel N         Set parallel jobs (default: $(sysctl -n hw.ncpu))"
    echo ""
    echo "Environment Variables:"
    echo "  STARROCKS_THIRDPARTY    Third-party directory (default: $THIRDPARTY_DIR)"
}

# Parse command line arguments
CLEAN_BUILD=0
HOMEBREW_ONLY=0
SOURCE_ONLY=0
PARALLEL_JOBS=$(sysctl -n hw.ncpu)

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
        --homebrew-only)
            HOMEBREW_ONLY=1
            shift
            ;;
        --source-only)
            SOURCE_ONLY=1
            shift
            ;;
        --parallel)
            if [[ -z "$2" || "$2" =~ ^- ]]; then
                log_error "--parallel requires a numeric value"
                usage
                exit 1
            fi
            if ! [[ "$2" =~ ^[0-9]+$ ]]; then
                log_error "--parallel argument must be a positive integer, got: $2"
                usage
                exit 1
            fi
            PARALLEL_JOBS="$2"
            shift 2
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate environment
if [[ "$(uname -s)" != "Darwin" ]]; then
    log_error "This script is only for macOS"
    exit 1
fi

if [[ "$(uname -m)" != "arm64" ]]; then
    log_error "This script is only for Apple Silicon (ARM64)"
    exit 1
fi

# Check if Homebrew is installed
if ! command -v brew >/dev/null 2>&1; then
    log_error "Homebrew is not installed. Please install it from https://brew.sh"
    exit 1
fi

# Setup environment variables
export HOMEBREW_PREFIX="/opt/homebrew"
export CC="$HOMEBREW_PREFIX/opt/llvm/bin/clang"
export CXX="$HOMEBREW_PREFIX/opt/llvm/bin/clang++"
export AR="$HOMEBREW_PREFIX/opt/llvm/bin/llvm-ar"
export RANLIB="$HOMEBREW_PREFIX/opt/llvm/bin/llvm-ranlib"
export OPENSSL_ROOT_DIR="$HOMEBREW_PREFIX/opt/openssl@3"
export PKG_CONFIG_PATH="$OPENSSL_ROOT_DIR/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
export CFLAGS="-march=armv8-a -O3"
export CXXFLAGS="-march=armv8-a -O3 -stdlib=libc++"
export LDFLAGS="-L$OPENSSL_ROOT_DIR/lib"
export CPPFLAGS="-I$OPENSSL_ROOT_DIR/include"
# Add Homebrew bin directories to PATH for Bison and other tools
export PATH="$HOMEBREW_PREFIX/opt/bison/bin:$HOMEBREW_PREFIX/bin:$PATH"

log_info "Starting StarRocks macOS third-party build"
log_info "Root directory: $ROOT_DIR"
log_info "Third-party directory: $THIRDPARTY_DIR"
log_info "Install directory: $INSTALL_DIR"
log_info "Parallel jobs: $PARALLEL_JOBS"

# Create directories
mkdir -p "$THIRDPARTY_DIR" "$INSTALL_DIR"/{lib,lib64,bin,include}

# Clean if requested
if [[ $CLEAN_BUILD -eq 1 ]]; then
    log_info "Cleaning build directories..."
    rm -rf "$THIRDPARTY_DIR"/build
    rm -rf "$INSTALL_DIR"
    mkdir -p "$INSTALL_DIR"/{lib,lib64,bin,include}
fi

# ============================================================================
# PHASE 1: HOMEBREW DEPENDENCIES
# ============================================================================
install_homebrew_deps() {
    log_info "Installing Homebrew dependencies..."

    local homebrew_deps=(
        # Build tools
        "llvm"
        "gnu-getopt"
        "autoconf"
        "automake"
        "libtool"
        "cmake"
        "ninja"
        "ccache"
        "bison"

        # SSL and crypto
        "openssl@3"

        # Memory management
        "jemalloc"

        # Compression libraries
        "snappy"
        "zstd"
        "lz4"
        "bzip2"

        # Other libraries
        "icu4c"
        "xsimd"
    )

    for dep in "${homebrew_deps[@]}"; do
        if brew list "$dep" >/dev/null 2>&1; then
            log_success "$dep already installed"
        else
            log_info "Installing $dep..."
            brew install "$dep"
        fi
    done

    log_success "All Homebrew dependencies installed"
}

# ============================================================================
# PHASE 2: SOURCE DEPENDENCIES
# ============================================================================

# Version definitions from thirdparty/vars.sh
GFLAGS_VERSION="2.2.2"
GLOG_VERSION="0.7.1"
PROTOBUF_VERSION="3.14.0"
THRIFT_VERSION="0.20.0"
LEVELDB_VERSION="1.20"
BRPC_VERSION="1.9.0"
ROCKSDB_VERSION="6.22.1"
BITSHUFFLE_VERSION="0.5.1"
VECTORSCAN_VERSION="5.4.12"
VELOCYPACK_VERSION="XYZ1.0"
ASYNC_PROFILER_VERSION="4.1"

# Thrift
THRIFT_DOWNLOAD="http://archive.apache.org/dist/thrift/0.20.0/thrift-0.20.0.tar.gz"
THRIFT_NAME="thrift-0.20.0.tar.gz"
THRIFT_SOURCE="thrift-0.20.0"
THRIFT_MD5SUM="aadebde599e1f5235acd3c730721b873"

# datasketches-cpp
DATASKETCHES_VERSION="4.0.0"
DATASKETCHES_DOWNLOAD="https://github.com/apache/datasketches-cpp/archive/refs/tags/${DATASKETCHES_VERSION}.tar.gz"
DATASKETCHES_NAME="datasketches-cpp-${DATASKETCHES_VERSION}.tar.gz"
DATASKETCHES_SOURCE="datasketches-cpp-${DATASKETCHES_VERSION}"

# RYU (build from source; pinned commit snapshot)
RYU_DOWNLOAD="https://github.com/ulfjack/ryu/archive/aa31ca9361d21b1a00ee054aac49c87d07e74abc.zip"
RYU_NAME="ryu-aa31ca9361d21b1a00ee054aac49c87d07e74abc.zip"
RYU_SOURCE="ryu-aa31ca9361d21b1a00ee054aac49c87d07e74abc"

# icu
ICU_DOWNLOAD="https://github.com/unicode-org/icu/releases/download/release-76-1/icu4c-76_1-src.zip"
ICU_NAME="icu4c-76_1-src.zip"
ICU_SOURCE="icu"
ICU_MD5SUM="f5f5c827d94af8445766c7023aca7f6b"

# libdivide
LIBDIVIDE_DOWNLOAD="https://github.com/ridiculousfish/libdivide/archive/refs/tags/v5.2.0.tar.gz"
LIBDIVIDE_NAME="libdivide-v5.2.0.tar.gz"
LIBDIVIDE_SOURCE="libdivide-5.2.0"
LIBDIVIDE_MD5SUM="4ba77777192c295d6de2b86d88f3239a"

# CROARINGBITMAP
CROARINGBITMAP_DOWNLOAD="https://github.com/RoaringBitmap/CRoaring/archive/refs/tags/v4.2.1.tar.gz"
CROARINGBITMAP_NAME=CRoaring-4.2.1.tar.gz
CROARINGBITMAP_SOURCE=CRoaring-4.2.1
CROARINGBITMAP_MD5SUM="00667266a60709978368cf867fb3a3aa"

# curl
CURL_DOWNLOAD="https://curl.se/download/curl-8.16.0.tar.gz"
CURL_NAME=curl-8.16.0.tar.gz
CURL_SOURCE=curl-8.16.0
CURL_MD5SUM="533e8a3b1228d5945a6a512537bea4c7"

# simdutf
SIMDUTF_DOWNLOAD="https://github.com/simdutf/simdutf/archive/refs/tags/v5.2.8.tar.gz"
SIMDUTF_NAME="simdutf-5.2.8.tar.gz"
SIMDUTF_SOURCE="simdutf-5.2.8"
SIMDUTF_MD5SUM="731c78ab5a10c6073942dc93d5c4b04c"

# async-profiler
ASYNC_PROFILER_DOWNLOAD="https://github.com/async-profiler/async-profiler/releases/download/v4.1/async-profiler-4.1-macos.zip"
ASYNC_PROFILER_NAME="async-profiler-4.1-macos.zip"
ASYNC_PROFILER_SOURCE="async-profiler-4.1-macos"


download_source() {
    local name="$1"
    local version="$2"
    local primary_url="$3"
    local filename="$4"
    shift 4

    local src_dir="$THIRDPARTY_DIR/src"
    mkdir -p "$src_dir"

    if [[ -f "$src_dir/$filename" ]]; then
        log_success "$name source already downloaded"
        return 0
    fi

    log_info "Downloading $name $version..."

    # Build URL list: primary + any fallbacks passed as extra args
    local urls=("$primary_url")
    if [[ $# -gt 0 ]]; then
        while [[ $# -gt 0 ]]; do
            urls+=("$1")
            shift
        done
    fi

    local success=0
    local tried_list=()
    for u in "${urls[@]}"; do
        if [[ -z "$u" ]]; then
            continue
        fi
        log_info "Trying URL: $u"
        tried_list+=("$u")
        if curl -fL -o "$src_dir/$filename" "$u" 2>/dev/null; then
            success=1
            break
        fi
    done

    if [[ $success -ne 1 ]]; then
        log_error "Failed to download $name $version. Tried: ${tried_list[*]}"
        return 1
    fi
}

build_gflags() {
    # Check if already built
    if [[ -f "$INSTALL_DIR/lib/libgflags.a" && -f "$INSTALL_DIR/include/gflags/gflags.h" ]]; then
        log_success "gflags already built, skipping"
        return 0
    fi

    log_info "Building gflags $GFLAGS_VERSION..."

    local src_dir="$THIRDPARTY_DIR/src"
    local build_dir="$THIRDPARTY_DIR/build/gflags"

    download_source "gflags" "$GFLAGS_VERSION" \
        "https://github.com/gflags/gflags/archive/v$GFLAGS_VERSION.tar.gz" \
        "gflags-$GFLAGS_VERSION.tar.gz"

    mkdir -p "$build_dir"
    cd "$build_dir"

    if [[ ! -d "gflags-$GFLAGS_VERSION" ]]; then
        tar -xzf "$src_dir/gflags-$GFLAGS_VERSION.tar.gz"
    fi

    cd "gflags-$GFLAGS_VERSION"
    mkdir -p build && cd build

    cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="$INSTALL_DIR" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DGFLAGS_BUILD_TESTING=OFF \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5

    make -j"$PARALLEL_JOBS"
    make install

    log_success "gflags built successfully"
}

build_glog() {
    # Check if already built
    if [[ -f "$INSTALL_DIR/lib/libglog.a" && -f "$INSTALL_DIR/include/glog/logging.h" ]]; then
        log_success "glog already built, skipping"
        return 0
    fi

    log_info "Building glog $GLOG_VERSION..."

    local src_dir="$THIRDPARTY_DIR/src"
    local build_dir="$THIRDPARTY_DIR/build/glog"
    local PATCHED_MARK="patched_mark"

    download_source "glog" "$GLOG_VERSION" \
        "https://github.com/google/glog/archive/v$GLOG_VERSION.tar.gz" \
        "glog-$GLOG_VERSION.tar.gz"

    mkdir -p "$build_dir"
    cd "$build_dir"

    if [[ ! -d "glog-$GLOG_VERSION" ]]; then
        tar -xzf "$src_dir/glog-$GLOG_VERSION.tar.gz"
    fi

    cd "glog-$GLOG_VERSION"

    # Apply glog-0.7.1 patches
    if [[ ! -f "$PATCHED_MARK" && "$GLOG_VERSION" == "0.7.1" ]]; then
        log_info "Applying glog-0.7.1 patches..."
        patch -p1 < "$THIRDPARTY_DIR/patches/glog-0.7.1.patch"
        patch -p1 < "$THIRDPARTY_DIR/patches/glog-0.7.1-add-handler-after-output-log.patch"
        patch -p1 < "$THIRDPARTY_DIR/patches/glog-0.7.1-lwp.patch"
        touch "$PATCHED_MARK"
        log_success "glog patches applied successfully"
    fi

    mkdir -p build && cd build

    cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="$INSTALL_DIR" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DWITH_GFLAGS=ON \
        -DGFLAGS_NAMESPACE=gflags \
        -DWITH_GTEST=OFF \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5

    make -j"$PARALLEL_JOBS"
    make install

    log_success "glog built successfully"
}

generate_hash_memory_shim() {
    local src_root="$THIRDPARTY_DIR/build/protobuf/protobuf-$PROTOBUF_VERSION"
    local shim_dir="$src_root/src/google/protobuf/internal"
    local shim_file="$shim_dir/hash_memory_impl.cc"

    # 1. Ensure target directory exists
    mkdir -p "$shim_dir"

    # 2. Write shim (overwrite if file already exists)
    cat > "$shim_file" <<'EOF'
#ifndef _LIBCPP_HAS_NO_HASH_MEMORY
#define _LIBCPP_HAS_NO_HASH_MEMORY 1
#endif
#include <cstddef>
namespace std { namespace __1 {
[[gnu::pure]] size_t
__hash_memory(const void* __ptr, size_t __size) noexcept
{
    // Compatible with old libc++ implementation, protobuf only needs the symbol to exist
    size_t h = 0;
    const unsigned char* p = static_cast<const unsigned char*>(__ptr);
    for (size_t i = 0; i < __size; ++i)
        h = h * 31 + p[i];
    return h;
}
} }
EOF

    log_success "shim created at $shim_file"
}

build_protobuf() {
    # Check if already built
    if [[ -f "$INSTALL_DIR/lib/libprotobuf.a" && -f "$INSTALL_DIR/bin/protoc" ]]; then
        local protoc_version=$("$INSTALL_DIR/bin/protoc" --version 2>/dev/null | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' || echo "")
        if [[ "$protoc_version" == "$PROTOBUF_VERSION" ]]; then
            log_success "protobuf already built, skipping"
            return 0
        fi
    fi

    log_info "Building protobuf $PROTOBUF_VERSION..."

    local src_dir="$THIRDPARTY_DIR/src"
    local build_dir="$THIRDPARTY_DIR/build/protobuf"

    download_source "protobuf" "$PROTOBUF_VERSION" \
        "https://github.com/protocolbuffers/protobuf/archive/v$PROTOBUF_VERSION.tar.gz" \
        "protobuf-$PROTOBUF_VERSION.tar.gz"

    mkdir -p "$build_dir"
    cd "$build_dir"

    if [[ ! -d "protobuf-$PROTOBUF_VERSION" ]]; then
        tar -xzf "$src_dir/protobuf-$PROTOBUF_VERSION.tar.gz"
    fi

    cd "protobuf-$PROTOBUF_VERSION"

    # Build using autotools (standard for protobuf 3.14.0)
    ./autogen.sh

    generate_hash_memory_shim

    # Compile the hash memory shim separately and add it to the build
    local shim_obj="$build_dir/hash_memory_shim.o"
    $CXX $CXXFLAGS -c src/google/protobuf/internal/hash_memory_impl.cc -o "$shim_obj"

    ./configure \
        --prefix="$INSTALL_DIR" \
        --disable-shared \
        --enable-static \
        --with-pic \
        --disable-tests \
        --disable-examples \
        --with-zlib \
        --with-zlib-include="$OPENSSL_ROOT_DIR/include" \
        --with-zlib-libpath="$OPENSSL_ROOT_DIR/lib" \
        CC="$CC" \
        CXX="$CXX" \
        CFLAGS="$CFLAGS" \
        CXXFLAGS="$CXXFLAGS" \
        LDFLAGS="$LDFLAGS $shim_obj"

    make -j"$PARALLEL_JOBS" LDFLAGS="$LDFLAGS $shim_obj"
    make install

    # Fix the protobuf library to include our shim
    if [[ -f "$INSTALL_DIR/lib/libprotobuf.a" ]]; then
        # Create a combined libprotobuf with the shim
        cd "$INSTALL_DIR/lib"
        cp libprotobuf.a libprotobuf.a.backup
        ar rcs libprotobuf.a "$shim_obj"
        ranlib libprotobuf.a 2>/dev/null || true
        rm -f libprotobuf.a.backup
    fi

    # Verify protoc
    "$INSTALL_DIR/bin/protoc" --version

    log_success "protobuf built successfully"
}

build_thrift() {
    # Check if already built
    if [[ -f "$INSTALL_DIR/lib/libthrift.a" && -f "$INSTALL_DIR/lib/libthriftnb.a" && -f "$INSTALL_DIR/include/thrift/Thrift.h" ]]; then
        log_success "thrift already built, skipping"
        return 0
    fi

    log_info "Building thrift $THRIFT_VERSION..."

    local src_dir="$THIRDPARTY_DIR/src"
    local build_dir="$THIRDPARTY_DIR/build/thrift"

    download_source "thrift" "$THRIFT_VERSION" \
        "$THRIFT_DOWNLOAD" \
        "$THRIFT_NAME"

    mkdir -p "$build_dir"
    cd "$build_dir"

    if [[ ! -d "$THRIFT_SOURCE" ]]; then
        tar -xzf "$src_dir/$THRIFT_NAME"
    fi

    cd "$THRIFT_SOURCE"

    # Run bootstrap if configure doesn't exist
    if [[ ! -f configure ]]; then
        ./bootstrap.sh
    fi

    # Generate hash memory shim for macOS libc++ compatibility
    local shim_src="$build_dir/hash_memory_impl.cc"
    cat > "$shim_src" <<'EOF'
#ifndef _LIBCPP_HAS_NO_HASH_MEMORY
#define _LIBCPP_HAS_NO_HASH_MEMORY 1
#endif
#include <cstddef>
namespace std { namespace __1 {
[[gnu::pure]] size_t
__hash_memory(const void* __ptr, size_t __size) noexcept
{
    // Compatible with old libc++ implementation
    size_t h = 0;
    const unsigned char* p = static_cast<const unsigned char*>(__ptr);
    for (size_t i = 0; i < __size; ++i)
        h = h * 31 + p[i];
    return h;
}
} }
EOF

    local shim_obj="$build_dir/hash_memory_shim.o"
    $CXX $CXXFLAGS -c "$shim_src" -o "$shim_obj"

    # Configure for macOS ARM64 with static libraries
    ./configure LDFLAGS="-L${INSTALL_DIR}/lib -L${OPENSSL_ROOT_DIR}/lib" \
        LIBS="-lssl -lcrypto -ldl $shim_obj" \
        CPPFLAGS="-DTHRIFT_STATIC_DEFINE $CPPFLAGS" \
        --prefix="$INSTALL_DIR" \
        --docdir="$INSTALL_DIR/doc" \
        --enable-static \
        --disable-shared \
        --disable-tests \
        --disable-tutorial \
        --without-qt4 \
        --without-qt5 \
        --without-csharp \
        --without-erlang \
        --without-nodejs \
        --without-lua \
        --without-perl \
        --without-php \
        --without-php_extension \
        --without-dart \
        --without-ruby \
        --without-haskell \
        --without-go \
        --without-haxe \
        --without-d \
        --without-python \
        --without-java \
        --without-rs \
        --with-cpp \
        --with-libevent="$INSTALL_DIR" \
        --with-boost="$HOMEBREW_PREFIX" \
        CXXFLAGS="$CXXFLAGS -DUSE_BOOST_THREAD"

    # Fix naming issue for macOS
    if [[ -f compiler/cpp/thrifty.hh ]]; then
        mv compiler/cpp/thrifty.hh compiler/cpp/thrifty.h
    fi

    make -j"$PARALLEL_JOBS" CXXFLAGS="$CXXFLAGS -Wno-error" CFLAGS="$CFLAGS -Wno-error"
    make install || echo "make install failed, manually installing binaries"

    # Manual installation if make install fails
    if [[ -f "compiler/cpp/thrift" ]]; then
        cp compiler/cpp/thrift "$INSTALL_DIR/bin/"
        log_success "Manually installed thrift binary"
    fi

    # Copy any library files that were built
    if [[ -f "lib/cpp/.libs/libthrift.a" ]]; then
        cp lib/cpp/.libs/libthrift.a "$INSTALL_DIR/lib/"
        log_success "Manually installed libthrift.a"
    fi

    if [[ -f "lib/cpp/.libs/libthriftnb.a" ]]; then
        cp lib/cpp/.libs/libthriftnb.a "$INSTALL_DIR/lib/"
        log_success "Manually installed libthriftnb.a"
    fi

    # Verify libraries were created
    if [[ ! -f "$INSTALL_DIR/lib/libthrift.a" ]]; then
        log_error "libthrift.a not found after build"
        return 1
    fi

    if [[ ! -f "$INSTALL_DIR/lib/libthriftnb.a" ]]; then
        log_error "libthriftnb.a not found after build"
        return 1
    fi

    log_success "thrift built successfully"
}

build_leveldb() {
    # Check if already built
    if [[ -f "$INSTALL_DIR/lib/libleveldb.a" && -f "$INSTALL_DIR/include/leveldb/db.h" ]]; then
        log_success "leveldb already built, skipping"
        return 0
    fi

    log_info "Building leveldb $LEVELDB_VERSION..."

    local src_dir="$THIRDPARTY_DIR/src"
    local build_dir="$THIRDPARTY_DIR/build/leveldb"

    download_source "leveldb" "$LEVELDB_VERSION" \
        "https://github.com/google/leveldb/archive/v$LEVELDB_VERSION.tar.gz" \
        "leveldb-$LEVELDB_VERSION.tar.gz"

    mkdir -p "$build_dir"
    cd "$build_dir"

    if [[ ! -d "leveldb-$LEVELDB_VERSION" ]]; then
        tar -xzf "$src_dir/leveldb-$LEVELDB_VERSION.tar.gz"
    fi

    cd "leveldb-$LEVELDB_VERSION"

    # Apply patch to disable SSE on non-x86 architectures (arm64)
    # Be robust: only apply if not already applied; never reverse interactively.
    local patch_file="$SCRIPT_DIR/patch/leveldb_build_detect_platform.patch"
    if [[ -f "$patch_file" ]]; then
        # Detect whether our marker exists; if not, attempt a forward-only patch.
        if ! grep -q "Non-x86 architecture detected" build_detect_platform 2>/dev/null; then
            log_info "Applying leveldb build_detect_platform patch..."
            if patch -p1 --forward --batch < "$patch_file" >/dev/null; then
                log_success "leveldb patch applied successfully"
            else
                log_warn "leveldb patch could not be applied (maybe already applied). Proceeding."
            fi
        else
            log_success "leveldb patch already applied, skipping"
        fi
    else
        log_warn "leveldb patch not found at $patch_file"
    fi

    # Build using Makefile (leveldb 1.20 doesn't use CMake)
    # Force-disable SSE flags to avoid x86 intrinsics on arm64 even if detection misfires.
    # Use system compiler to avoid toolchain mismatches.
    CC=cc CXX=c++ make -j"$PARALLEL_JOBS" PLATFORM_SSEFLAGS="" out-static/libleveldb.a

    # Install manually
    mkdir -p "$INSTALL_DIR/include/leveldb"
    mkdir -p "$INSTALL_DIR/lib"

    # Copy headers
    cp -r include/leveldb/* "$INSTALL_DIR/include/leveldb/"

    # Copy the static library from the correct location
    if [[ -f "out-static/libleveldb.a" ]]; then
        cp out-static/libleveldb.a "$INSTALL_DIR/lib/"
    else
        log_error "leveldb static library not found in out-static/"
        return 1
    fi

    log_success "leveldb built successfully"
}

build_brpc() {
    # Check if already built
    if [[ -f "$INSTALL_DIR/lib/libbrpc.a" && -f "$INSTALL_DIR/include/brpc/server.h" ]]; then
        log_success "brpc already built, skipping"
        return 0
    fi

    log_info "Building brpc $BRPC_VERSION..."

    local src_dir="$THIRDPARTY_DIR/src"
    local build_dir="$THIRDPARTY_DIR/build/brpc"
    local PATCHED_MARK="patched_mark"

    download_source "brpc" "$BRPC_VERSION" \
        "https://github.com/apache/brpc/archive/$BRPC_VERSION.tar.gz" \
        "brpc-$BRPC_VERSION.tar.gz"

    mkdir -p "$build_dir"
    cd "$build_dir"

    if [[ ! -d "brpc-$BRPC_VERSION" ]]; then
        tar -xzf "$src_dir/brpc-$BRPC_VERSION.tar.gz"
    fi

    cd "brpc-$BRPC_VERSION"

    # Apply brpc-1.9.0 patches
    if [[ ! -f "$PATCHED_MARK" && "$BRPC_VERSION" == "1.9.0" ]]; then
        log_info "Applying brpc-1.9.0 patches..."
        # Apply config_brpc.sh patch
        if [[ -f "$THIRDPARTY_DIR/patches/brpc-1.9.0.patch" ]]; then
            patch -p1 < "$THIRDPARTY_DIR/patches/brpc-1.9.0.patch" || true
            log_info "Patched config_brpc.sh"
        fi
        
        # Force C++17 in CMakeLists.txt using sed for robustness
        log_info "Forcing C++17 in CMakeLists.txt..."
        sed -i '' 's/set(CMAKE_CXX_STANDARD_REQUIRED ON)/set(CMAKE_CXX_STANDARD_REQUIRED ON)\n\n# Force C++17 for glog 0.7.1 compatibility on macOS\nset(CMAKE_CXX_STANDARD 17)\nset(CMAKE_CXX_STANDARD_REQUIRED ON)/' CMakeLists.txt
        
        touch "$PATCHED_MARK"
        log_success "brpc patches applied successfully"
    fi

    mkdir -p build && cd build

    # Use our compiled protobuf-3.14.0 and glog-0.7.1
    export PKG_CONFIG_PATH="$INSTALL_DIR/lib/pkgconfig:$PKG_CONFIG_PATH"
    export PROTOBUF_ROOT="$INSTALL_DIR"

    cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="$INSTALL_DIR" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DWITH_GLOG=ON \
        -DBRPC_WITH_GLOG=ON \
        -DWITH_THRIFT=OFF \
        -DProtobuf_DIR="$INSTALL_DIR/lib/cmake/protobuf" \
        -Dglog_DIR="$INSTALL_DIR/lib/cmake/glog" \
        -DGLOG_INCLUDE_PATH="$INSTALL_DIR/include" \
        -DGLOG_LIB="$INSTALL_DIR/lib/libglog.a" \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5

    make -j"$PARALLEL_JOBS"
    make install

    # Verify brpc library
    ls -la "$INSTALL_DIR/lib/libbrpc.a"

    log_success "brpc built successfully"
}

build_rocksdb() {
    # Check if already built
    if [[ -f "$INSTALL_DIR/lib/librocksdb.a" && -f "$INSTALL_DIR/include/rocksdb/db.h" ]]; then
        log_success "rocksdb already built, skipping"
        return 0
    fi

    log_info "Building rocksdb $ROCKSDB_VERSION..."

    local src_dir="$THIRDPARTY_DIR/src"
    local build_dir="$THIRDPARTY_DIR/build/rocksdb"

    download_source "rocksdb" "$ROCKSDB_VERSION" \
        "https://github.com/facebook/rocksdb/archive/v$ROCKSDB_VERSION.tar.gz" \
        "rocksdb-$ROCKSDB_VERSION.tar.gz"

    mkdir -p "$build_dir"
    cd "$build_dir"

    if [[ ! -d "rocksdb-$ROCKSDB_VERSION" ]]; then
        tar -xzf "$src_dir/rocksdb-$ROCKSDB_VERSION.tar.gz"
    fi

    cd "rocksdb-$ROCKSDB_VERSION"

    # Apply RocksDB metadata header patch for macOS libc++ compatibility
    local patch_file="$THIRDPARTY_DIR/patches/rocksdb-6.22.1-metadata-header.patch"
    if [[ -f "$patch_file" ]]; then
        # Check if patch is already applied by looking for our marker comment
        if ! grep -q "The metadata that describes a SST file" include/rocksdb/metadata.h 2>/dev/null; then
            log_info "Applying RocksDB metadata header patch..."
            if patch -p1 --forward --batch < "$patch_file" >/dev/null; then
                log_success "RocksDB metadata patch applied successfully"
            else
                log_warn "RocksDB metadata patch could not be applied (maybe already applied). Proceeding."
            fi
        else
            log_success "RocksDB metadata patch already applied, skipping"
        fi
    else
        log_warn "RocksDB metadata patch not found at $patch_file"
    fi

    mkdir -p build && cd build

    cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="$INSTALL_DIR" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DROCKSDB_BUILD_SHARED=OFF \
        -DCMAKE_CXX_FLAGS="${CXXFLAGS} -D_LIBCPP_HAS_NO_HASH_MEMORY=1 -Wno-error" \
        -DCMAKE_C_FLAGS="${CFLAGS} -Wno-error" \
        -DFAIL_ON_WARNINGS=OFF \
        -DWITH_ALL_TESTS=OFF \
        -DWITH_CORE_TOOLS=OFF \
        -DWITH_BENCHMARK_TOOLS=OFF \
        -DBUILD_TESTING=OFF \
        -DWITH_SNAPPY=ON \
        -DWITH_ZSTD=ON \
        -DWITH_TESTS=OFF \
        -DWITH_BENCHMARKS=OFF \
        -DWITH_TOOLS=OFF \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5

    make -j"$PARALLEL_JOBS" rocksdb
    make install

    log_success "rocksdb built successfully"
}

build_velocypack() {
    # Check if already built
    if [[ -f "$INSTALL_DIR/lib/libvelocypack.a" && -f "$INSTALL_DIR/include/velocypack/vpack.h" ]]; then
        log_success "velocypack already built, skipping"
        return 0
    fi

    log_info "Building velocypack $VELOCYPACK_VERSION..."

    local src_dir="$THIRDPARTY_DIR/src"
    local build_dir="$THIRDPARTY_DIR/build/velocypack"

    download_source "velocypack" "$VELOCYPACK_VERSION" \
        "https://github.com/arangodb/velocypack/archive/refs/tags/$VELOCYPACK_VERSION.tar.gz" \
        "velocypack-$VELOCYPACK_VERSION.tar.gz"

    mkdir -p "$build_dir"
    cd "$build_dir"

    if [[ ! -d "velocypack-$VELOCYPACK_VERSION" ]]; then
        tar -xzf "$src_dir/velocypack-$VELOCYPACK_VERSION.tar.gz"
    fi

    cd "velocypack-$VELOCYPACK_VERSION"
    mkdir -p build && cd build

    cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="$INSTALL_DIR" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DCMAKE_CXX_FLAGS="${CXXFLAGS} -D_LIBCPP_HAS_NO_HASH_MEMORY=1" \
        -DBuildVelocyPackExamples=OFF \
        -DBuildTools=OFF \
        -DBuildBench=OFF \
        -DBuildTests=OFF \
        -DBuildLargeTests=OFF \
        -DBuildAsmTest=OFF \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5

    make -j"$PARALLEL_JOBS"
    make install

    log_success "velocypack built successfully"
}

build_bitshuffle() {
    # Check if already built
    if [[ -f "$INSTALL_DIR/lib/libbitshuffle.a" && -f "$INSTALL_DIR/include/bitshuffle/bitshuffle.h" ]]; then
        log_success "bitshuffle already built, skipping"
        return 0
    fi

    log_info "Building bitshuffle $BITSHUFFLE_VERSION..."

    local src_dir="$THIRDPARTY_DIR/src"
    local build_dir="$THIRDPARTY_DIR/build/bitshuffle"

    download_source "bitshuffle" "$BITSHUFFLE_VERSION" \
        "https://github.com/kiyo-masui/bitshuffle/archive/$BITSHUFFLE_VERSION.tar.gz" \
        "bitshuffle-$BITSHUFFLE_VERSION.tar.gz" \
        "https://github.com/kiyo-masui/bitshuffle/archive/refs/tags/$BITSHUFFLE_VERSION.tar.gz" \
        "https://github.com/kiyo-masui/bitshuffle/archive/v$BITSHUFFLE_VERSION.tar.gz" \
        "https://github.com/kiyo-masui/bitshuffle/archive/refs/tags/v$BITSHUFFLE_VERSION.tar.gz"

    mkdir -p "$build_dir"
    cd "$build_dir"

    if [[ ! -d "bitshuffle-$BITSHUFFLE_VERSION" ]]; then
        tar -xzf "$src_dir/bitshuffle-$BITSHUFFLE_VERSION.tar.gz"
    fi

    cd "bitshuffle-$BITSHUFFLE_VERSION"

    # Build static library manually; include bundled LZ4 headers and sources
    local BSHUF_CFLAGS="$CFLAGS -I./src -I./lz4"
    "$CC" $BSHUF_CFLAGS -c src/bitshuffle.c -o bitshuffle.o
    "$CC" $BSHUF_CFLAGS -c src/bitshuffle_core.c -o bitshuffle_core.o
    "$CC" $BSHUF_CFLAGS -c src/iochain.c -o iochain.o

    "$AR" rcs libbitshuffle.a *.o

    # Install
    mkdir -p "$INSTALL_DIR"/{lib,include/bitshuffle}
    cp libbitshuffle.a "$INSTALL_DIR/lib/"
    cp src/*.h "$INSTALL_DIR/include/bitshuffle/"

    log_success "bitshuffle built successfully"
}

# datasketches (header-only install)
build_datasketches() {
    # Check if already installed (pick one representative header)
    if [[ -d "$INSTALL_DIR/include/datasketches" && -f "$INSTALL_DIR/include/datasketches/hll.hpp" ]]; then
        log_success "datasketches already installed, skipping"
        return 0
    fi

    log_info "Installing datasketches ${DATASKETCHES_VERSION} headers..."

    local src_dir="$THIRDPARTY_DIR/src"
    local build_dir="$THIRDPARTY_DIR/build/datasketches"

    download_source "datasketches-cpp" "$DATASKETCHES_VERSION" \
        "$DATASKETCHES_DOWNLOAD" \
        "$DATASKETCHES_NAME"

    mkdir -p "$build_dir"
    cd "$build_dir"

    if [[ ! -d "$DATASKETCHES_SOURCE" ]]; then
        tar -xzf "$src_dir/$DATASKETCHES_NAME"
    fi

    # Copy public headers into a flat include/datasketches directory, matching Linux build layout
    mkdir -p "$INSTALL_DIR/include/datasketches"
    cp -r "$DATASKETCHES_SOURCE"/common/include/* "$INSTALL_DIR/include/datasketches/" || true
    cp -r "$DATASKETCHES_SOURCE"/cpc/include/* "$INSTALL_DIR/include/datasketches/" || true
    cp -r "$DATASKETCHES_SOURCE"/fi/include/* "$INSTALL_DIR/include/datasketches/" || true
    cp -r "$DATASKETCHES_SOURCE"/hll/include/* "$INSTALL_DIR/include/datasketches/" || true
    cp -r "$DATASKETCHES_SOURCE"/kll/include/* "$INSTALL_DIR/include/datasketches/" || true
    cp -r "$DATASKETCHES_SOURCE"/quantiles/include/* "$INSTALL_DIR/include/datasketches/" || true
    cp -r "$DATASKETCHES_SOURCE"/req/include/* "$INSTALL_DIR/include/datasketches/" || true
    cp -r "$DATASKETCHES_SOURCE"/sampling/include/* "$INSTALL_DIR/include/datasketches/" || true
    cp -r "$DATASKETCHES_SOURCE"/theta/include/* "$INSTALL_DIR/include/datasketches/" || true
    cp -r "$DATASKETCHES_SOURCE"/tuple/include/* "$INSTALL_DIR/include/datasketches/" || true

    log_success "datasketches headers installed"
}

# async-profiler distribution copy
build_async_profiler() {
    if [[ -d "$INSTALL_DIR/async-profiler/bin" && -d "$INSTALL_DIR/async-profiler/lib" ]]; then
        log_success "async-profiler already installed, skipping"
        return 0
    fi

    log_info "Installing async-profiler ${ASYNC_PROFILER_VERSION} for macOS..."

    local src_dir="$THIRDPARTY_DIR/src"
    local build_dir="$THIRDPARTY_DIR/build/async-profiler"

    download_source "async-profiler" "$ASYNC_PROFILER_VERSION" \
        "$ASYNC_PROFILER_DOWNLOAD" \
        "$ASYNC_PROFILER_NAME"

    mkdir -p "$build_dir"
    cd "$build_dir"

    if [[ ! -d "$ASYNC_PROFILER_SOURCE" ]]; then
        unzip -q "$src_dir/$ASYNC_PROFILER_NAME"
    fi

    mkdir -p "$INSTALL_DIR/async-profiler"
    rm -rf "$INSTALL_DIR/async-profiler/bin" "$INSTALL_DIR/async-profiler/lib"
    cp -R "$ASYNC_PROFILER_SOURCE/bin" "$INSTALL_DIR/async-profiler/"
    cp -R "$ASYNC_PROFILER_SOURCE/lib" "$INSTALL_DIR/async-profiler/"

    log_success "async-profiler installed successfully"
}

# Build ryu from source and install into $INSTALL_DIR
build_ryu() {
    # Check if already built
    if [[ -f "$INSTALL_DIR/lib/libryu.a" && -f "$INSTALL_DIR/include/ryu/ryu.h" ]]; then
        log_success "ryu already built, skipping"
        return 0
    fi

    log_info "Building ryu from source..."

    local src_dir="$THIRDPARTY_DIR/src"
    local build_dir="$THIRDPARTY_DIR/build/ryu"

    download_source "ryu" "$RYU_SOURCE" "$RYU_DOWNLOAD" "$RYU_NAME"

    mkdir -p "$build_dir"
    cd "$build_dir"

    if [[ ! -d "$RYU_SOURCE" ]]; then
        unzip -q "$src_dir/$RYU_NAME"
    fi

    cd "$RYU_SOURCE"

    # Apply patch if exists (use shared thirdparty patch when available)
    local patch_file="$THIRDPARTY_DIR/patches/ryu.patch"
    if [[ -f "$patch_file" ]]; then
        log_info "Applying ryu patch..."
        patch -p1 < "$patch_file"
    fi

    # Build and install
    cd ryu
    make -j"$PARALLEL_JOBS"
    make install DESTDIR="$INSTALL_DIR"

    # Ensure headers path matches <ryu/ryu.h>
    mkdir -p "$INSTALL_DIR/include/ryu"
    if [[ -f "$INSTALL_DIR/include/ryu.h" ]]; then
        mv -f "$INSTALL_DIR/include/ryu.h" "$INSTALL_DIR/include/ryu/ryu.h"
    elif [[ -f "ryu.h" ]]; then
        cp -f "ryu.h" "$INSTALL_DIR/include/ryu/ryu.h"
    fi

    # Ensure lib64 compatibility copy
    if [[ -f "$INSTALL_DIR/lib/libryu.a" ]]; then
        mkdir -p "$INSTALL_DIR/lib64"
        cp -f "$INSTALL_DIR/lib/libryu.a" "$INSTALL_DIR/lib64/libryu.a"
    fi

    log_success "ryu built successfully"
}

build_libdivide() {
    # Check if already built
    if [[ -f "$INSTALL_DIR/include/libdivide.h" ]]; then
        log_success "libdivide already built, skipping"
        return 0
    fi

    log_info "Building libdivide..."

    local src_dir="$THIRDPARTY_DIR/src"
    local build_dir="$THIRDPARTY_DIR/build/libdivide"

    download_source "libdivide" "v5.2.0" \
        "$LIBDIVIDE_DOWNLOAD" \
        "$LIBDIVIDE_NAME"

    mkdir -p "$build_dir"
    cd "$build_dir"

    if [[ ! -d "$LIBDIVIDE_SOURCE" ]]; then
        tar -xzf "$src_dir/$LIBDIVIDE_NAME"
    fi

    cd "$LIBDIVIDE_SOURCE"

    # libdivide is header-only, just copy the header
    cp libdivide.h "$INSTALL_DIR/include/"

    log_success "libdivide built successfully"
}

build_icu() {
    # Check if already built
    if [[ -f "$INSTALL_DIR/lib/libicuuc.a" && -f "$INSTALL_DIR/include/unicode/ucasemap.h" ]]; then
        log_success "icu already built, skipping"
        return 0
    fi

    log_info "Building icu..."

    local src_dir="$THIRDPARTY_DIR/src"
    local build_dir="$THIRDPARTY_DIR/build/icu"

    download_source "icu" "76-1" \
        "$ICU_DOWNLOAD" \
        "$ICU_NAME"

    mkdir -p "$build_dir"
    cd "$build_dir"

    if [[ ! -d "$ICU_SOURCE" ]]; then
        unzip -q "$src_dir/$ICU_NAME"
    fi

    cd "$ICU_SOURCE/source"

    # Fix line endings for shell scripts
    sed -i '' 's/\r$//' ./runConfigureICU
    sed -i '' 's/\r$//' ./config.*
    sed -i '' 's/\r$//' ./configure
    sed -i '' 's/\r$//' ./mkinstalldirs

    # Clear compile flags to use ICU defaults
    unset CPPFLAGS
    unset CXXFLAGS
    unset CFLAGS

    # Use a subshell to prevent environment variable leakage
    (
        export CFLAGS="-O3 -fno-omit-frame-pointer -fPIC"
        export CXXFLAGS="-O3 -fno-omit-frame-pointer -fPIC"
        ./runConfigureICU macOS --prefix="$INSTALL_DIR" --enable-static --disable-shared
        make -j"$PARALLEL_JOBS"
        make install
    )

    log_success "icu built successfully"
}


detect_boost_version() {
    # Detect Boost version from Homebrew installation
    local boost_version
    boost_version=$(brew list --versions boost 2>/dev/null | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1)

    if [[ -z "$boost_version" ]]; then
        log_error "Boost not found via Homebrew. Please install with: brew install boost"
        return 1
    fi

    echo "$boost_version"
}

build_vectorscan() {
    # Check if already built
    if [[ -f "$INSTALL_DIR/lib/libhs.a" && -f "$INSTALL_DIR/include/hs/hs.h" ]]; then
        log_success "vectorscan already built, skipping"
        return 0
    fi

    log_info "Building vectorscan $VECTORSCAN_VERSION..."

    local src_dir="$THIRDPARTY_DIR/src"
    local build_dir="$THIRDPARTY_DIR/build/vectorscan"

    # Download and extract vectorscan source if needed
    download_source "vectorscan" "$VECTORSCAN_VERSION" \
        "https://github.com/VectorCamp/vectorscan/archive/refs/tags/vectorscan/$VECTORSCAN_VERSION.tar.gz" \
        "vectorscan-$VECTORSCAN_VERSION.tar.gz" \
        "https://github.com/VectorCamp/vectorscan/archive/vectorscan-$VECTORSCAN_VERSION.tar.gz"

    mkdir -p "$build_dir"
    cd "$build_dir"

    if [[ ! -d "vectorscan-vectorscan-$VECTORSCAN_VERSION" ]]; then
        tar -xzf "$src_dir/vectorscan-$VECTORSCAN_VERSION.tar.gz"
    fi

    cd "vectorscan-vectorscan-$VECTORSCAN_VERSION"

    # Clean and rebuild
    rm -rf build
    mkdir -p build && cd build

    # Dynamically detect Boost version
    local boost_version
    boost_version=$(detect_boost_version)
    if [[ $? -ne 0 ]]; then
        return 1
    fi

    # Set boost path for homebrew with detected version
    export BOOST_ROOT="/opt/homebrew"
    export Boost_DIR="/opt/homebrew/lib/cmake/Boost-$boost_version"

    log_info "Using Boost version $boost_version"

    cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="$INSTALL_DIR" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DFAT_RUNTIME=OFF \
        -DBUILD_STATIC_LIBS=ON \
        -DBUILD_EXAMPLES=OFF \
        -DBUILD_TOOLS=OFF \
        -DBUILD_BENCHMARKS=OFF \
        -DBUILD_UNIT=OFF \
        -DBUILD_TESTS=OFF \
        -DBUILD_UNIT_TESTS=OFF \
        -DENABLE_UNIT_TESTS=OFF \
        -DCORRECT_PCRE_VERSION=OFF \
        -DCMAKE_C_FLAGS="${CFLAGS} -Wno-error" \
        -DCMAKE_CXX_FLAGS="${CXXFLAGS} -D_LIBCPP_HAS_NO_HASH_MEMORY=1 -Wno-error" \
        -DBOOST_ROOT="/opt/homebrew" \
        -DBoost_DIR="/opt/homebrew/lib/cmake/Boost-$boost_version" \
        -DBoost_NO_SYSTEM_PATHS=ON \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5

    # Build only the main library components, skip all tests
    make -j"$PARALLEL_JOBS" hs hs_compile hs_runtime
    # Install the libraries that were built
    make install

    # Inject libc++ hash memory shim into libhs.a for macOS arm64
    if [[ -f "$INSTALL_DIR/lib/libhs.a" ]]; then
        local shim_src="$build_dir/hash_memory_impl.cc"
        cat > "$shim_src" <<'EOF'
#ifndef _LIBCPP_HAS_NO_HASH_MEMORY
#define _LIBCPP_HAS_NO_HASH_MEMORY 1
#endif
#include <cstddef>
namespace std { namespace __1 {
[[gnu::pure]] size_t
__hash_memory(const void* __ptr, size_t __size) noexcept
{
    size_t h = 0;
    const unsigned char* p = static_cast<const unsigned char*>(__ptr);
    for (size_t i = 0; i < __size; ++i)
        h = h * 31 + p[i];
    return h;
}
} }
EOF
        local shim_obj="$build_dir/hash_memory_shim.o"
        $CXX $CXXFLAGS -c "$shim_src" -o "$shim_obj"
        ( cd "$INSTALL_DIR/lib" && ar rcs libhs.a "$shim_obj" && ranlib libhs.a 2>/dev/null || true )
        log_success "Injected hash_memory shim into libhs.a"
    fi

    log_success "vectorscan built successfully"
}

build_curl() {
    # Check if already built
    if [[ -f "$INSTALL_DIR/lib/libcurl.a" && -f "$INSTALL_DIR/include/curl/curl.h" ]]; then
        log_success "curl already built, skipping"
        return 0
    fi

    log_info "Building curl $CURL_SOURCE..."

    local src_dir="$THIRDPARTY_DIR/src"
    local build_dir="$THIRDPARTY_DIR/build/curl"

    download_source "curl" "$CURL_SOURCE" \
        "$CURL_DOWNLOAD" \
        "$CURL_NAME"

    mkdir -p "$build_dir"
    cd "$build_dir"

    if [[ ! -d "$CURL_SOURCE" ]]; then
        tar -xzf "$src_dir/$CURL_NAME"
    fi

    cd "$CURL_SOURCE"

    # Configure curl with minimal dependencies to avoid complex linking issues
    # Use similar configuration as Linux build but adapted for macOS
    LDFLAGS="-L${INSTALL_DIR}/lib -L${OPENSSL_ROOT_DIR}/lib" LIBS="-lssl -lcrypto -ldl" \
    ./configure --prefix="$INSTALL_DIR" --disable-shared --enable-static \
    --without-librtmp --with-ssl="${OPENSSL_ROOT_DIR}" --without-libidn2 --without-libgsasl \
    --disable-ldap --enable-ipv6 --without-brotli \
    --disable-ftp --disable-ftps --disable-file --disable-dict --disable-telnet \
    --disable-tftp --disable-pop3 --disable-pop3s --disable-imap --disable-imaps \
    --disable-smb --disable-smtp --disable-smtps --disable-gopher --disable-mqtt \
    --disable-rtsp --disable-rtsp --disable-ldap --disable-ldaps --disable-unix-sockets \
    --without-zstd --without-brotli --without-libidn --without-libssh2 \
    --without-nghttp2 --without-nghttp3 --without-ngtcp2 \
    --without-libpsl \
    --with-pic --enable-optimize --disable-debug --enable-http

    make -j"$PARALLEL_JOBS"
    make install

    # Verify installation
    if [[ ! -f "$INSTALL_DIR/lib/libcurl.a" ]]; then
        log_error "curl static library not found after build"
        return 1
    fi

    if [[ ! -f "$INSTALL_DIR/include/curl/curl.h" ]]; then
        log_error "curl headers not found after build"
        return 1
    fi

    log_success "curl built successfully"
}

build_simdutf() {
    # Check if already built
    if [[ -f "$INSTALL_DIR/lib/libsimdutf.a" && -f "$INSTALL_DIR/include/simdutf.h" ]]; then
        log_success "simdutf already built, skipping"
        return 0
    fi

    log_info "Building simdutf $SIMDUTF_SOURCE..."

    local src_dir="$THIRDPARTY_DIR/src"
    local build_dir="$THIRDPARTY_DIR/build/simdutf"

    download_source "simdutf" "$SIMDUTF_SOURCE" \
        "$SIMDUTF_DOWNLOAD" \
        "$SIMDUTF_NAME"

    mkdir -p "$build_dir"
    cd "$build_dir"

    if [[ ! -d "$SIMDUTF_SOURCE" ]]; then
        tar -xzf "$src_dir/$SIMDUTF_NAME"
    fi

    cd "$SIMDUTF_SOURCE"
    mkdir -p build && cd build

    cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="$INSTALL_DIR" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DSIMDUTF_TESTS=OFF \
        -DSIMDUTF_TOOLS=OFF \
        -DSIMDUTF_ICONV=OFF \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_SHARED_LIBS=OFF

    make -j"$PARALLEL_JOBS"
    make install

    log_success "simdutf built successfully"
}

build_croaringbitmap() {
    # Check if already built
    if [[ -f "$INSTALL_DIR/lib/libroaring.a" && -f "$INSTALL_DIR/include/roaring/roaring.h" ]]; then
        log_success "croaringbitmap already built, skipping"
        return 0
    fi

    log_info "Building croaringbitmap..."

    local src_dir="$THIRDPARTY_DIR/src"
    local build_dir="$THIRDPARTY_DIR/build/croaringbitmap"

    download_source "croaringbitmap" "v4.2.1" \
        "$CROARINGBITMAP_DOWNLOAD" \
        "$CROARINGBITMAP_NAME"

    mkdir -p "$build_dir"
    cd "$build_dir"

    if [[ ! -d "$CROARINGBITMAP_SOURCE" ]]; then
        tar -xzf "$src_dir/$CROARINGBITMAP_NAME"
    fi

    cd "$CROARINGBITMAP_SOURCE"

    # Pre-download CPM.cmake to avoid network issues during build
    local cpm_dir="$CROARINGBITMAP_SOURCE/cmake"
    mkdir -p "$cpm_dir"
    if [[ ! -f "$cpm_dir/CPM.cmake" ]]; then
        log_info "Downloading CPM.cmake for offline build..."
        if curl -fL -o "$cpm_dir/CPM.cmake" "https://github.com/cpm-cmake/CPM.cmake/releases/download/v0.38.6/CPM.cmake" 2>/dev/null; then
            log_success "CPM.cmake downloaded successfully"
        else
            log_warn "Failed to download CPM.cmake, will try to build without it"
        fi
    fi

    mkdir -p build && cd build

    # Configure AVX support for macOS ARM64
    local FORCE_AVX=FALSE
    # avx2 is not supported by aarch64
    if [[ "$(uname -m)" != "arm64" ]]; then
        # Only enable AVX on non-ARM64 architectures
        FORCE_AVX=ON
    fi

    # Try to build with CPM first, fallback to simple build if it fails
    if ! cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="$INSTALL_DIR" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DROARING_BUILD_STATIC=ON \
        -DENABLE_ROARING_TESTS=OFF \
        -DROARING_DISABLE_NATIVE=ON \
        -DFORCE_AVX=$FORCE_AVX \
        -DROARING_DISABLE_AVX512=ON \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_C_FLAGS="${CFLAGS} -Wno-error" \
        -DCMAKE_CXX_FLAGS="${CXXFLAGS} -D_LIBCPP_HAS_NO_HASH_MEMORY=1 -Wno-error" \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5; then

        log_warn "CMake configuration failed, trying simple build without CPM..."

        # Fallback: Simple manual build without CPM
        cd ..

        # Create a simple build without external dependencies
        if [[ -f "src/roaring.c" && -f "include/roaring/roaring.h" ]]; then
            log_info "Building roaring bitmap manually..."

            # Debug: Show directory structure
            log_info "Source directory structure:"
            ls -la src/ 2>/dev/null || log_warn "src directory not found"
            ls -la include/ 2>/dev/null || log_warn "include directory not found"

            # Build directly from source
            mkdir -p "$INSTALL_DIR/include/roaring"
            mkdir -p "$INSTALL_DIR/lib"

            # Copy headers
            cp -r include/roaring/* "$INSTALL_DIR/include/roaring/"

            # Compile the static library manually with proper include paths
            local object_files=()
            local include_flags="-I./include -I./src"

            # List source files to be compiled
            log_info "Compiling source files:"
            for src_file in src/*.c; do
                if [[ -f "$src_file" ]]; then
                    log_info "  Found source file: $src_file"
                    local obj_file="$build_dir/$(basename "$src_file" .c).o"
                    log_info "  Compiling: $src_file -> $obj_file"
                    if "$CC" $CFLAGS $include_flags -c "$src_file" -o "$obj_file" 2>&1; then
                        object_files+=("$obj_file")
                        log_info "  ✓ Successfully compiled $src_file"
                    else
                        log_warn "  ✗ Failed to compile $src_file"
                    fi
                fi
            done

            # Also compile any additional source files in subdirectories
            for src_file in src/*/*.c; do
                if [[ -f "$src_file" ]]; then
                    log_info "  Found source file: $src_file"
                    local obj_file="$build_dir/$(basename "$src_file" .c).o"
                    log_info "  Compiling: $src_file -> $obj_file"
                    if "$CC" $CFLAGS $include_flags -c "$src_file" -o "$obj_file" 2>&1; then
                        object_files+=("$obj_file")
                        log_info "  ✓ Successfully compiled $src_file"
                    else
                        log_warn "  ✗ Failed to compile $src_file"
                    fi
                fi
            done

            log_info "Total object files compiled: ${#object_files[@]}"

            # Create the static library
            if [[ ${#object_files[@]} -gt 0 ]]; then
                log_info "Creating static library with ${#object_files[@]} object files..."
                "${AR:-ar}" rcs "$INSTALL_DIR/lib/libroaring.a" "${object_files[@]}"
                "${RANLIB:-ranlib}" "$INSTALL_DIR/lib/libroaring.a" 2>/dev/null || true
                log_info "Static library created: $INSTALL_DIR/lib/libroaring.a"
                ls -la "$INSTALL_DIR/lib/libroaring.a" 2>/dev/null || log_error "Library file not found"
            else
                log_error "No object files were compiled successfully"
                return 1
            fi

            # Inject libc++ hash memory shim for macOS compatibility
            local shim_src="$build_dir/hash_memory_impl.cc"
            cat > "$shim_src" <<'EOF'
#ifndef _LIBCPP_HAS_NO_HASH_MEMORY
#define _LIBCPP_HAS_NO_HASH_MEMORY 1
#endif
#include <cstddef>
namespace std { namespace __1 {
[[gnu::pure]] size_t
__hash_memory(const void* __ptr, size_t __size) noexcept
{
    size_t h = 0;
    const unsigned char* p = static_cast<const unsigned char*>(__ptr);
    for (size_t i = 0; i < __size; ++i)
        h = h * 31 + p[i];
    return h;
}
} }
EOF
            local shim_obj="$build_dir/hash_memory_shim.o"
            $CXX $CXXFLAGS -c "$shim_src" -o "$shim_obj"
            ( cd "$INSTALL_DIR/lib" && ar rcs libroaring.a "$shim_obj" && ranlib libroaring.a 2>/dev/null || true )
            log_success "Injected hash_memory shim into libroaring.a"

            # Verify installation
            if [[ -f "$INSTALL_DIR/lib/libroaring.a" && -f "$INSTALL_DIR/include/roaring/roaring.h" ]]; then
                log_success "Manual build completed successfully"
            else
                log_error "Manual build verification failed"
                return 1
            fi

        else
            log_error "Cannot find required source files for manual build"
            log_info "Looking for: src/roaring.c and include/roaring/roaring.h"
            ls -la src/roaring.c 2>/dev/null || log_info "src/roaring.c not found"
            ls -la include/roaring/roaring.h 2>/dev/null || log_info "include/roaring/roaring.h not found"
            return 1
        fi
    else
        # CMake succeeded, continue with normal build
        make -j"$PARALLEL_JOBS"
        make install

        # Inject libc++ hash memory shim for macOS compatibility if needed
        if [[ -f "$INSTALL_DIR/lib/libroaring.a" ]]; then
            local shim_src="$build_dir/hash_memory_impl.cc"
            cat > "$shim_src" <<'EOF'
#ifndef _LIBCPP_HAS_NO_HASH_MEMORY
#define _LIBCPP_HAS_NO_HASH_MEMORY 1
#endif
#include <cstddef>
namespace std { namespace __1 {
[[gnu::pure]] size_t
__hash_memory(const void* __ptr, size_t __size) noexcept
{
    size_t h = 0;
    const unsigned char* p = static_cast<const unsigned char*>(__ptr);
    for (size_t i = 0; i < __size; ++i)
        h = h * 31 + p[i];
    return h;
}
} }
EOF
            local shim_obj="$build_dir/hash_memory_shim.o"
            $CXX $CXXFLAGS -c "$shim_src" -o "$shim_obj"
            ( cd "$INSTALL_DIR/lib" && ar rcs libroaring.a "$shim_obj" && ranlib libroaring.a 2>/dev/null || true )
            log_success "Injected hash_memory shim into libroaring.a"
        fi
    fi

    log_success "croaringbitmap built successfully"
}

build_source_deps() {
    log_info "Building source dependencies..."

    # Layer 1: Basic libraries (no dependencies)
    build_gflags
    build_glog
    build_protobuf
    build_thrift
    build_leveldb
    build_datasketches
    build_ryu
    build_libdivide
    build_icu
    build_croaringbitmap
    build_curl
    build_simdutf
    build_async_profiler

    # Layer 2: Libraries that depend on Layer 1
    build_brpc

    # Layer 3: Storage and format libraries
    build_rocksdb
    build_velocypack
    build_bitshuffle
    build_vectorscan

    # Create lib64 symlinks for compatibility
    ln -sf ../lib/libgflags.a "$INSTALL_DIR/lib64/libgflags.a" 2>/dev/null || true
    ln -sf ../lib/libglog.a "$INSTALL_DIR/lib64/libglog.a" 2>/dev/null || true
    ln -sf ../lib/libprotobuf.a "$INSTALL_DIR/lib64/libprotobuf.a" 2>/dev/null || true
    ln -sf ../lib/libleveldb.a "$INSTALL_DIR/lib64/libleveldb.a" 2>/dev/null || true
    ln -sf ../lib/libbrpc.a "$INSTALL_DIR/lib64/libbrpc.a" 2>/dev/null || true
    ln -sf ../lib/librocksdb.a "$INSTALL_DIR/lib64/librocksdb.a" 2>/dev/null || true
    ln -sf ../lib/libhs.a "$INSTALL_DIR/lib64/libhs.a" 2>/dev/null || true
    ln -sf ../lib/libryu.a "$INSTALL_DIR/lib64/libryu.a" 2>/dev/null || true
    ln -sf ../lib/libicuuc.a "$INSTALL_DIR/lib64/libicuuc.a" 2>/dev/null || true
    ln -sf ../lib/libicui18n.a "$INSTALL_DIR/lib64/libicui18n.a" 2>/dev/null || true
    ln -sf ../lib/libroaring.a "$INSTALL_DIR/lib64/libroaring.a" 2>/dev/null || true
    ln -sf ../lib/libcurl.a "$INSTALL_DIR/lib64/libcurl.a" 2>/dev/null || true

    log_success "All source dependencies built successfully"
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================
main() {
    if [[ $SOURCE_ONLY -eq 0 ]]; then
        install_homebrew_deps
    fi

    if [[ $HOMEBREW_ONLY -eq 0 ]]; then
        build_source_deps
    fi

    # Verification
    log_info "Verifying build results..."

    local required_libs=(
        "libgflags.a"
        "libglog.a"
        "libprotobuf.a"
        "libleveldb.a"
        "libbrpc.a"
        "librocksdb.a"
        "libvelocypack.a"
        "libbitshuffle.a"
        "libhs.a"
        "libhs_runtime.a"
        "libryu.a"
        "libicuuc.a"
        "libicui18n.a"
        "libroaring.a"
    )

    local missing_libs=()
    for lib in "${required_libs[@]}"; do
        if [[ ! -f "$INSTALL_DIR/lib/$lib" ]]; then
            missing_libs+=("$lib")
        fi
    done

    if [[ ${#missing_libs[@]} -gt 0 ]]; then
        log_error "Missing libraries: ${missing_libs[*]}"
        exit 1
    fi

    log_success "All third-party dependencies built successfully!"
    log_info "Install directory: $INSTALL_DIR"
    log_info "Libraries: $(ls -1 "$INSTALL_DIR/lib"/*.a | wc -l) static libraries built"
    log_info "Headers: $(find "$INSTALL_DIR/include" -name "*.h" | wc -l) header files installed"
}

# Run main function
main "$@"
