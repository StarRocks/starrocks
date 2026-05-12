#!/usr/bin/env bash
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eo pipefail

# build.sh sources env_macos.sh before calling us (directly or via the auto-
# trigger when thirdparty is not yet built), which exports a batch of CMake
# behavior-control vars intended for the BE build only. They leak into
# thirdparty subprocesses and break bundled cmake logic
# (e.g. velocypack's TargetArch.cmake rejects CMAKE_OSX_ARCHITECTURES=arm64
# with "Invalid OS X arch name: arm64"). Unset at entry so running via
# build.sh matches running this script standalone.
unset CMAKE_OSX_ARCHITECTURES CMAKE_BUILD_TARGET_ARCH \
      CMAKE_GENERATOR CMAKE_FIND_LIBRARY_SUFFIXES \
      BUILD_SHARED_LIBS CMAKE_BUILD_TYPE MAKEFLAGS

curdir="$(cd -- "$(dirname -- "$0")" >/dev/null 2>&1 && pwd)"

export STARROCKS_HOME="${STARROCKS_HOME:-$curdir/..}"
export TP_DIR="${TP_DIR:-$curdir}"
export STARROCKS_TP_VARS_OVERRIDE="${TP_DIR}/vars-darwin-aarch64.sh"

if [[ -f "${STARROCKS_HOME}/custom_env.sh" ]]; then
    . "${STARROCKS_HOME}/custom_env.sh"
fi

if [[ ! -f "${TP_DIR}/vars.sh" ]]; then
    echo "vars.sh is missing"
    exit 1
fi
if [[ ! -f "${TP_DIR}/package-manifest.sh" ]]; then
    echo "package-manifest.sh is missing"
    exit 1
fi

. "${TP_DIR}/vars.sh"
. "${TP_DIR}/package-manifest.sh"
starrocks_set_default_packages "${MACHINE_TYPE}"

usage() {
    cat <<EOF
Usage: $0 [options...] [packages...]

  Description:
    Build thirdparty dependencies for StarRocks on macOS Darwin. If no packages
    are specified, all packages in the repo-root default order will be built.

  Optional options:
    -j<num>                Build with <num> parallel jobs (can also use -j <num>)
    --clean                Clean extracted source before building
    --continue <package>   Continue building from specified package
    -h, --help             Show this help message
EOF
}

append_flags() {
    local base="$1"
    local extra="$2"

    if [[ -z "${base}" ]]; then
        echo "${extra}"
    elif [[ -z "${extra}" ]]; then
        echo "${base}"
    else
        echo "${base} ${extra}"
    fi
}

portable_strip_crlf() {
    local file
    for file in "$@"; do
        [[ -e "${file}" ]] || continue
        perl -pi -e 's/\r$//' "${file}"
    done
}

safe_remove_glob() {
    local pattern
    local path

    for pattern in "$@"; do
        for path in ${pattern}; do
            [[ -e "${path}" ]] || continue
            rm -f "${path}"
        done
    done
}

move_matching_files() {
    local dest="$1"
    local pattern
    local path

    mkdir -p "${dest}"
    shift
    for pattern in "$@"; do
        for path in ${pattern}; do
            [[ -e "${path}" ]] || continue
            mv "${path}" "${dest}/"
        done
    done
}

write_hash_memory_shim() {
    local shim_file="$1"

    mkdir -p "$(dirname "${shim_file}")"
    cat > "${shim_file}" <<'EOF'
#ifndef _LIBCPP_HAS_NO_HASH_MEMORY
#define _LIBCPP_HAS_NO_HASH_MEMORY 1
#endif
#include <cstddef>
namespace std { namespace __1 {
[[gnu::pure]] size_t
__hash_memory(const void* __ptr, size_t __size) noexcept {
    size_t h = 0;
    const unsigned char* p = static_cast<const unsigned char*>(__ptr);
    for (size_t i = 0; i < __size; ++i) {
        h = h * 31 + p[i];
    }
    return h;
}
} }
EOF
}

ensure_macos_libtool_wrapper() {
    local wrapper_dir="${TP_DIR}/build/libtool-wrapper"
    local wrapper_path="${wrapper_dir}/libtool"

    mkdir -p "${wrapper_dir}"
    cat > "${wrapper_path}" <<'EOF'
#!/usr/bin/env bash
if [[ "$1" == "-V" ]]; then
    /usr/bin/libtool -V 2>&1
    exit $?
fi
exec /usr/bin/libtool "$@"
EOF
    chmod +x "${wrapper_path}"
    echo "${wrapper_path}"
}

append_object_to_archive() {
    local archive="$1"
    local object_file="$2"

    "${AR}" rcs "${archive}" "${object_file}"
    "${RANLIB}" "${archive}" >/dev/null 2>&1 || true
}

contains_cpu_feature() {
    local feature="$1"

    if [[ "${feature}" == "avx" || "${feature}" == "avx2" || "${feature}" == "avx512" ]]; then
        sysctl -a 2>/dev/null | grep -qi "machdep.cpu.features:.*${feature}"
        return $?
    fi
    return 1
}

check_if_source_exist() {
    if [[ ! -d "${TP_SOURCE_DIR}/$1" ]]; then
        echo "Source directory ${TP_SOURCE_DIR}/$1 does not exist"
        exit 1
    fi
}

clean_sources() {
    local archive
    local source_var
    local source

    for archive in ${TP_ARCHIVES}; do
        source_var="${archive}_SOURCE"
        source="${!source_var}"
        [[ -n "${source}" ]] || continue
        rm -rf "${TP_SOURCE_DIR}/${source}"
    done
}

ensure_formula() {
    local formula="$1"
    if ! brew list --formula "${formula}" >/dev/null 2>&1; then
        brew install "${formula}"
    fi
}

formula_prefix() {
    brew --prefix "$1"
}

link_if_missing() {
    local src="$1"
    local dst="$2"

    [[ -e "${src}" || -L "${src}" ]] || return 0
    if [[ -e "${dst}" || -L "${dst}" ]]; then
        return 0
    fi
    mkdir -p "$(dirname "${dst}")"
    ln -s "${src}" "${dst}"
}

link_children_if_missing() {
    local src_dir="$1"
    local dst_dir="$2"
    local path
    local dst_path

    [[ -d "${src_dir}" ]] || return 0
    mkdir -p "${dst_dir}"
    for path in "${src_dir}"/*; do
        [[ -e "${path}" || -L "${path}" ]] || continue
        dst_path="${dst_dir}/$(basename "${path}")"
        if [[ -d "${path}" ]] && [[ -d "${dst_path}" ]] && [[ ! -L "${dst_path}" ]]; then
            link_children_if_missing "${path}" "${dst_path}"
        else
            link_if_missing "${path}" "${dst_path}"
        fi
    done
}

link_matching_if_missing() {
    local dest_dir="$1"
    shift
    local pattern
    local path

    mkdir -p "${dest_dir}"
    for pattern in "$@"; do
        for path in ${pattern}; do
            [[ -e "${path}" || -L "${path}" ]] || continue
            link_if_missing "${path}" "${dest_dir}/$(basename "${path}")"
        done
    done
}

link_formula_metadata() {
    local prefix="$1"
    link_children_if_missing "${prefix}/lib/pkgconfig" "${TP_INSTALL_DIR}/lib/pkgconfig"
    link_children_if_missing "${prefix}/lib/cmake" "${TP_INSTALL_DIR}/lib/cmake"
    link_children_if_missing "${prefix}/share" "${TP_INSTALL_DIR}/share"
}

sync_lib64_links() {
    local lib
    local dst
    mkdir -p "${TP_INSTALL_DIR}/lib64"
    for lib in "${TP_INSTALL_DIR}"/lib/*; do
        [[ -e "${lib}" || -L "${lib}" ]] || continue
        case "$(basename "${lib}")" in
            *.a|*.dylib|*.so|*.so.*)
                dst="${TP_INSTALL_DIR}/lib64/$(basename "${lib}")"
                if [[ ! -e "${dst}" && ! -L "${dst}" ]]; then
                    ln -s "../lib/$(basename "${lib}")" "${dst}"
                fi
                ;;
        esac
    done
}

setup_build_environment() {
    local base_formula

    for base_formula in coreutils gnu-tar wget gnu-getopt autoconf automake libtool cmake ninja bison pkg-config llvm; do
        ensure_formula "${base_formula}"
    done

    export HOMEBREW_PREFIX="${HOMEBREW_PREFIX:-$(brew --prefix)}"
    export STARROCKS_LLVM_HOME="${STARROCKS_LLVM_HOME:-$(brew --prefix llvm)}"
    export PATH="${HOMEBREW_PREFIX}/opt/coreutils/libexec/gnubin:${HOMEBREW_PREFIX}/opt/gnu-tar/libexec/gnubin:${HOMEBREW_PREFIX}/opt/bison/bin:${HOMEBREW_PREFIX}/opt/gnu-getopt/bin:${STARROCKS_LLVM_HOME}/bin:${HOMEBREW_PREFIX}/bin:${PATH}"
    export SDKROOT="${SDKROOT:-$(xcrun --show-sdk-path)}"
    export MACOSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET:-15.0}"
    export CC="${CC:-${STARROCKS_LLVM_HOME}/bin/clang}"
    export CXX="${CXX:-${STARROCKS_LLVM_HOME}/bin/clang++}"
    export CPP="${CPP:-${CC} -E}"
    export AR="${AR:-${STARROCKS_LLVM_HOME}/bin/llvm-ar}"
    export RANLIB="${RANLIB:-${STARROCKS_LLVM_HOME}/bin/llvm-ranlib}"
    export OBJCOPY="${OBJCOPY:-${STARROCKS_LLVM_HOME}/bin/llvm-objcopy}"
    export NM_BIN="${NM_BIN:-$(command -v nm)}"
    export LD_BIN="${LD_BIN:-$(command -v ld)}"
    export CMAKE_CMD="${CMAKE_CMD:-cmake}"
    if command -v ninja >/dev/null 2>&1; then
        export CMAKE_GENERATOR="${CMAKE_GENERATOR:-Ninja}"
        export BUILD_SYSTEM="${BUILD_SYSTEM:-ninja}"
    else
        export CMAKE_GENERATOR="${CMAKE_GENERATOR:-Unix Makefiles}"
        export BUILD_SYSTEM="${BUILD_SYSTEM:-make}"
    fi

    export FILE_PREFIX_MAP_OPTION="-ffile-prefix-map=${TP_SOURCE_DIR}=. -ffile-prefix-map=${TP_INSTALL_DIR}=."
    export GLOBAL_CPPFLAGS="-I${TP_INCLUDE_DIR}"
    export GLOBAL_CFLAGS="-O3 -fno-omit-frame-pointer -std=gnu17 -fPIC -g ${FILE_PREFIX_MAP_OPTION}"
    export GLOBAL_CXXFLAGS="-O3 -fno-omit-frame-pointer -fPIC -g -stdlib=libc++ ${FILE_PREFIX_MAP_OPTION}"
    export CPPFLAGS="${GLOBAL_CPPFLAGS}"
    export CFLAGS="${GLOBAL_CFLAGS}"
    export CXXFLAGS="${GLOBAL_CXXFLAGS}"
    export STATIC_CXX_LDFLAGS=""
    export DL_LIB=""
    export RT_LIB=""
    export BUILD_DIR="starrocks_build"
    export THIRD_PARTY_BUILD_WITH_AVX2=OFF
}

restore_compile_flags() {
    export CPPFLAGS="${GLOBAL_CPPFLAGS}"
    export CFLAGS="${GLOBAL_CFLAGS}"
    export CXXFLAGS="${GLOBAL_CXXFLAGS}"
    unset LDFLAGS
}

resolve_java_home() {
    local java_home_candidate="$1"

    if [[ -z "${java_home_candidate}" ]]; then
        return 1
    fi
    if [[ -f "${java_home_candidate}/libexec/openjdk.jdk/Contents/Home/include/jni.h" ]]; then
        echo "${java_home_candidate}/libexec/openjdk.jdk/Contents/Home"
        return 0
    fi
    if [[ -f "${java_home_candidate}/include/jni.h" ]]; then
        echo "${java_home_candidate}"
        return 0
    fi
    echo "${java_home_candidate}"
}

ensure_java_home() {
    local java_home_candidate="${JAVA_HOME:-}"

    if [[ -z "${java_home_candidate}" ]]; then
        java_home_candidate="$(/usr/libexec/java_home -v 17 2>/dev/null || true)"
    fi
    if [[ -z "${java_home_candidate}" ]]; then
        ensure_formula openjdk@17
        java_home_candidate="$(brew --prefix openjdk@17)"
    fi
    JAVA_HOME="$(resolve_java_home "${java_home_candidate}")"
    export JAVA_HOME
    export PATH="${JAVA_HOME}/bin:${PATH}"
}

detect_java_jni_platform_include() {
    local java_home_root="$1"
    local candidate

    for candidate in darwin linux; do
        if [[ -f "${java_home_root}/include/${candidate}/jni_md.h" ]]; then
            printf '%s\n' "${candidate}"
            return 0
        fi
    done

    for candidate in "${java_home_root}"/include/*; do
        [[ -d "${candidate}" ]] || continue
        if [[ -f "${candidate}/jni_md.h" ]]; then
            basename "${candidate}"
            return 0
        fi
    done

    echo "Unable to locate jni_md.h under ${java_home_root}/include" >&2
    return 1
}

ensure_hadoop_libhdfs_makefile_uses_platform_include() {
    local makefile="$1"

    if ! grep -Fq 'JNI_PLATFORM_INCLUDE ?= linux' "${makefile}"; then
        perl -0pi -e 's@CC\s+:=\s+gcc@JNI_PLATFORM_INCLUDE ?= linux\n\nCC      := gcc@' "${makefile}"
    fi

    perl -0pi -e 's@-I\$\(JAVA_HOME\)/include/linux@-I\$(JAVA_HOME)/include/\$(JNI_PLATFORM_INCLUDE)@g' "${makefile}"

    if ! grep -Fq -- '-I$(JAVA_HOME)/include/$(JNI_PLATFORM_INCLUDE)' "${makefile}"; then
        echo "Failed to update ${makefile} to use JNI_PLATFORM_INCLUDE" >&2
        return 1
    fi
}

restore_env_var() {
    local name="$1"
    local value="$2"

    if [[ -n "${value}" ]]; then
        export "${name}=${value}"
    else
        unset "${name}"
    fi
}

find_boost_cmake_dir() {
    local root="$1"
    local candidate

    for candidate in "${root}"/lib/cmake/Boost-*; do
        [[ -d "${candidate}" ]] || continue
        echo "${candidate}"
        return 0
    done
    return 1
}

build_gflags() {
    if [[ -f "${TP_INSTALL_DIR}/lib/libgflags.a" && -f "${TP_INCLUDE_DIR}/gflags/gflags.h" ]]; then
        return 0
    fi

    check_if_source_exist "${GFLAGS_SOURCE}"
    cd "${TP_SOURCE_DIR}/${GFLAGS_SOURCE}"
    rm -rf cmake_build
    mkdir -p cmake_build
    cd cmake_build

    "${CMAKE_CMD}" .. \
        -G "${CMAKE_GENERATOR}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DGFLAGS_BUILD_TESTING=OFF \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    "${CMAKE_CMD}" --build . -j "${PARALLEL}"
    "${CMAKE_CMD}" --install .
    sync_lib64_links
}

build_glog() {
    if [[ -f "${TP_INSTALL_DIR}/lib/libglog.a" && -f "${TP_INCLUDE_DIR}/glog/logging.h" ]]; then
        return 0
    fi

    check_if_source_exist "${GLOG_SOURCE}"
    cd "${TP_SOURCE_DIR}/${GLOG_SOURCE}"
    rm -rf cmake_build
    mkdir -p cmake_build
    cd cmake_build

    "${CMAKE_CMD}" .. \
        -G "${CMAKE_GENERATOR}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_PREFIX_PATH="${TP_INSTALL_DIR}" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DWITH_GFLAGS=ON \
        -DGFLAGS_NAMESPACE=gflags \
        -DWITH_GTEST=OFF \
        -Dgflags_DIR="${TP_INSTALL_DIR}/lib/cmake/gflags" \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    "${CMAKE_CMD}" --build . -j "${PARALLEL}"
    "${CMAKE_CMD}" --install .
    sync_lib64_links
}

build_boost() {
    if [[ -f "${TP_INSTALL_DIR}/lib/libboost_system.a" && -f "${TP_INCLUDE_DIR}/boost/version.hpp" ]]; then
        return 0
    fi

    check_if_source_exist "${BOOST_SOURCE}"
    cd "${TP_SOURCE_DIR}/${BOOST_SOURCE}"

    if [[ ! -x "./b2" ]]; then
        PATH="/usr/bin:${PATH}" CC=/usr/bin/clang CXX=/usr/bin/clang++ \
            ./bootstrap.sh --prefix="${TP_INSTALL_DIR}" --with-toolset=clang --without-libraries=python
    fi

    ./b2 \
        toolset=clang \
        link=static \
        runtime-link=static \
        threading=multi \
        variant=release \
        cxxflags="-std=c++11 -fPIC -O3 -I${TP_INCLUDE_DIR} -stdlib=libc++ -D_LIBCPP_HAS_NO_HASH_MEMORY=1" \
        linkflags="-L${TP_INSTALL_DIR}/lib -stdlib=libc++" \
        --prefix="${TP_INSTALL_DIR}" \
        --layout=system \
        --without-test \
        --without-mpi \
        --without-graph \
        --without-graph_parallel \
        --without-python \
        --without-contract \
        -j"${PARALLEL}" \
        install
    sync_lib64_links
}

build_protobuf() {
    if [[ -f "${TP_INSTALL_DIR}/lib/libprotobuf.a" && -f "${TP_INSTALL_DIR}/bin/protoc" ]]; then
        return 0
    fi

    check_if_source_exist "${PROTOBUF_SOURCE}"
    cd "${TP_SOURCE_DIR}/${PROTOBUF_SOURCE}"
    if [[ -f Makefile ]]; then
        make distclean >/dev/null 2>&1 || true
    fi

    ./autogen.sh

    mkdir -p "${TP_DIR}/build/protobuf"
    local shim_src="${TP_DIR}/build/protobuf/hash_memory_impl.cc"
    local shim_obj="${TP_DIR}/build/protobuf/hash_memory_shim.o"
    write_hash_memory_shim "${shim_src}"
    "${CXX}" ${CXXFLAGS} -c "${shim_src}" -o "${shim_obj}"

    local protobuf_ldflags
    protobuf_ldflags="$(append_flags "${LDFLAGS:-}" "${shim_obj}")"

    ./configure \
        --prefix="${TP_INSTALL_DIR}" \
        --disable-shared \
        --enable-static \
        --with-pic \
        --disable-tests \
        --disable-examples \
        --with-zlib \
        --with-zlib-include="${TP_INCLUDE_DIR}" \
        --with-zlib-libpath="${TP_INSTALL_DIR}/lib" \
        CC="${CC}" \
        CXX="${CXX}" \
        CPPFLAGS="${CPPFLAGS}" \
        CFLAGS="${CFLAGS}" \
        CXXFLAGS="${CXXFLAGS}" \
        LDFLAGS="${protobuf_ldflags}"
    make -j"${PARALLEL}" LDFLAGS="${protobuf_ldflags}"
    make install

    append_object_to_archive "${TP_INSTALL_DIR}/lib/libprotobuf.a" "${shim_obj}"
    sync_lib64_links
}

build_thrift() {
    if [[ -f "${TP_INSTALL_DIR}/lib/libthrift.a" && -f "${TP_INSTALL_DIR}/lib/libthriftnb.a" && -f "${TP_INCLUDE_DIR}/thrift/Thrift.h" ]]; then
        return 0
    fi

    ensure_formula libevent

    check_if_source_exist "${THRIFT_SOURCE}"
    cd "${TP_SOURCE_DIR}/${THRIFT_SOURCE}"
    if [[ ! -f configure ]]; then
        ./bootstrap.sh
    fi
    if [[ -f Makefile ]]; then
        make distclean >/dev/null 2>&1 || true
    fi

    mkdir -p "${TP_DIR}/build/thrift"
    local shim_src="${TP_DIR}/build/thrift/hash_memory_impl.cc"
    local shim_obj="${TP_DIR}/build/thrift/hash_memory_shim.o"
    local libevent_prefix
    libevent_prefix="$(formula_prefix libevent)"
    write_hash_memory_shim "${shim_src}"
    "${CXX}" ${CXXFLAGS} -c "${shim_src}" -o "${shim_obj}"

    ./configure \
        LDFLAGS="-L${TP_INSTALL_DIR}/lib -L${TP_INSTALL_DIR}/lib64 -L${libevent_prefix}/lib" \
        LIBS="-lssl -lcrypto ${shim_obj}" \
        CPPFLAGS="-DTHRIFT_STATIC_DEFINE -I${TP_INCLUDE_DIR} -I${libevent_prefix}/include" \
        --prefix="${TP_INSTALL_DIR}" \
        --docdir="${TP_INSTALL_DIR}/doc" \
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
        --without-swift \
        --without-cl \
        --with-cpp \
        --with-libevent="${libevent_prefix}" \
        --with-boost="${TP_INSTALL_DIR}" \
        --with-openssl="${TP_INSTALL_DIR}"

    if [[ -f compiler/cpp/thrifty.hh ]]; then
        mv compiler/cpp/thrifty.hh compiler/cpp/thrifty.h
    fi

    make -j"${PARALLEL}" CXXFLAGS="$(append_flags "${CXXFLAGS}" "-Wno-error")" CFLAGS="$(append_flags "${CFLAGS}" "-Wno-error")"
    make install || true

    if [[ -f compiler/cpp/thrift ]]; then
        mkdir -p "${TP_INSTALL_DIR}/bin"
        cp compiler/cpp/thrift "${TP_INSTALL_DIR}/bin/"
    fi
    if [[ -f lib/cpp/.libs/libthrift.a ]]; then
        cp lib/cpp/.libs/libthrift.a "${TP_INSTALL_DIR}/lib/"
    fi
    if [[ -f lib/cpp/.libs/libthriftnb.a ]]; then
        cp lib/cpp/.libs/libthriftnb.a "${TP_INSTALL_DIR}/lib/"
    fi

    if [[ ! -f "${TP_INSTALL_DIR}/lib/libthrift.a" || ! -f "${TP_INSTALL_DIR}/lib/libthriftnb.a" ]]; then
        echo "Failed to build thrift"
        exit 1
    fi
    sync_lib64_links
}

build_leveldb() {
    if [[ -f "${TP_INSTALL_DIR}/lib/libleveldb.a" && -f "${TP_INCLUDE_DIR}/leveldb/db.h" ]]; then
        return 0
    fi

    check_if_source_exist "${LEVELDB_SOURCE}"
    cd "${TP_SOURCE_DIR}/${LEVELDB_SOURCE}"

    local patch_file="${TP_PATCH_DIR}/leveldb_build_detect_platform.patch"
    if [[ -f "${patch_file}" ]] && ! grep -q "Non-x86 architecture detected" build_detect_platform 2>/dev/null; then
        patch -p1 --forward --batch < "${patch_file}" >/dev/null || true
    fi

    CC=cc CXX=c++ make -j"${PARALLEL}" PLATFORM_SSEFLAGS="" out-static/libleveldb.a

    mkdir -p "${TP_INSTALL_DIR}/lib" "${TP_INCLUDE_DIR}/leveldb"
    cp -R include/leveldb/* "${TP_INCLUDE_DIR}/leveldb/"
    cp out-static/libleveldb.a "${TP_INSTALL_DIR}/lib/"
    sync_lib64_links
}

build_brpc() {
    if [[ -f "${TP_INSTALL_DIR}/lib/libbrpc.a" && -f "${TP_INCLUDE_DIR}/brpc/server.h" ]]; then
        return 0
    fi

    check_if_source_exist "${BRPC_SOURCE}"
    cd "${TP_SOURCE_DIR}/${BRPC_SOURCE}"
    if ! grep -q "Force C++17 for glog 0.7.1 compatibility on macOS" CMakeLists.txt; then
        perl -0pi -e 's/set\(CMAKE_CXX_STANDARD_REQUIRED ON\)/set(CMAKE_CXX_STANDARD_REQUIRED ON)\n\n# Force C++17 for glog 0.7.1 compatibility on macOS\nset(CMAKE_CXX_STANDARD 17)\nset(CMAKE_CXX_STANDARD_REQUIRED ON)/' CMakeLists.txt
    fi

    local old_path="${PATH}"
    local old_pkg_config_path="${PKG_CONFIG_PATH:-}"
    local old_protobuf_root="${PROTOBUF_ROOT:-}"
    local old_cplus_include_path="${CPLUS_INCLUDE_PATH:-}"
    local old_c_include_path="${C_INCLUDE_PATH:-}"
    export PATH="${TP_INSTALL_DIR}/bin:${PATH}"
    export PKG_CONFIG_PATH="${TP_INSTALL_DIR}/lib/pkgconfig:${TP_INSTALL_DIR}/lib64/pkgconfig${old_pkg_config_path:+:${old_pkg_config_path}}"
    export PROTOBUF_ROOT="${TP_INSTALL_DIR}"
    export CPLUS_INCLUDE_PATH="${TP_INCLUDE_DIR}${old_cplus_include_path:+:${old_cplus_include_path}}"
    export C_INCLUDE_PATH="${TP_INCLUDE_DIR}${old_c_include_path:+:${old_c_include_path}}"

    rm -rf cmake_build
    mkdir -p cmake_build
    cd cmake_build

    local brpc_cxx_flags="-isystem ${TP_INCLUDE_DIR}"
    local brpc_c_flags="-isystem ${TP_INCLUDE_DIR}"
    brpc_cxx_flags="$(append_flags "${brpc_cxx_flags}" "${CXXFLAGS}")"
    brpc_c_flags="$(append_flags "${brpc_c_flags}" "${CFLAGS}")"

    "${CMAKE_CMD}" .. \
        -G "${CMAKE_GENERATOR}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_PREFIX_PATH="${TP_INSTALL_DIR}" \
        -DCMAKE_CXX_FLAGS="${brpc_cxx_flags}" \
        -DCMAKE_C_FLAGS="${brpc_c_flags}" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DWITH_GLOG=ON \
        -DBRPC_WITH_GLOG=ON \
        -DWITH_THRIFT=OFF \
        -Dgflags_DIR="${TP_INSTALL_DIR}/lib/cmake/gflags" \
        -DProtobuf_DIR="${TP_INSTALL_DIR}/lib/cmake/protobuf" \
        -Dglog_DIR="${TP_INSTALL_DIR}/lib/cmake/glog" \
        -DGLOG_INCLUDE_PATH="${TP_INCLUDE_DIR}" \
        -DGLOG_LIB="${TP_INSTALL_DIR}/lib/libglog.a" \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    "${CMAKE_CMD}" --build . -j "${PARALLEL}"
    "${CMAKE_CMD}" --install .

    restore_env_var PATH "${old_path}"
    restore_env_var PKG_CONFIG_PATH "${old_pkg_config_path}"
    restore_env_var PROTOBUF_ROOT "${old_protobuf_root}"
    restore_env_var CPLUS_INCLUDE_PATH "${old_cplus_include_path}"
    restore_env_var C_INCLUDE_PATH "${old_c_include_path}"
    sync_lib64_links
}

build_rocksdb() {
    if [[ -f "${TP_INSTALL_DIR}/lib/librocksdb.a" && -f "${TP_INCLUDE_DIR}/rocksdb/db.h" ]]; then
        return 0
    fi

    ensure_formula zstd

    check_if_source_exist "${ROCKSDB_SOURCE}"
    cd "${TP_SOURCE_DIR}/${ROCKSDB_SOURCE}"
    rm -rf cmake_build
    mkdir -p cmake_build
    cd cmake_build

    "${CMAKE_CMD}" .. \
        -G "${CMAKE_GENERATOR}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DROCKSDB_BUILD_SHARED=OFF \
        -DCMAKE_CXX_FLAGS="$(append_flags "${CXXFLAGS}" "-D_LIBCPP_HAS_NO_HASH_MEMORY=1 -Wno-error")" \
        -DCMAKE_C_FLAGS="$(append_flags "${CFLAGS}" "-Wno-error")" \
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
    "${CMAKE_CMD}" --build . -j "${PARALLEL}" --target rocksdb
    "${CMAKE_CMD}" --install .
    sync_lib64_links
}

build_vpack() {
    if [[ -f "${TP_INSTALL_DIR}/lib/libvelocypack.a" && -f "${TP_INCLUDE_DIR}/velocypack/vpack.h" ]]; then
        return 0
    fi

    check_if_source_exist "${VPACK_SOURCE}"
    cd "${TP_SOURCE_DIR}/${VPACK_SOURCE}"
    rm -rf cmake_build
    mkdir -p cmake_build
    cd cmake_build

    "${CMAKE_CMD}" .. \
        -G "${CMAKE_GENERATOR}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DCMAKE_CXX_FLAGS="$(append_flags "${CXXFLAGS}" "-D_LIBCPP_HAS_NO_HASH_MEMORY=1")" \
        -DBuildVelocyPackExamples=OFF \
        -DBuildTools=OFF \
        -DBuildBench=OFF \
        -DBuildTests=OFF \
        -DBuildLargeTests=OFF \
        -DBuildAsmTest=OFF \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    "${CMAKE_CMD}" --build . -j "${PARALLEL}"
    "${CMAKE_CMD}" --install .
    sync_lib64_links
}

build_bitshuffle() {
    if [[ -f "${TP_INSTALL_DIR}/lib/libbitshuffle.a" && -f "${TP_INCLUDE_DIR}/bitshuffle/bitshuffle.h" ]]; then
        return 0
    fi

    check_if_source_exist "${BITSHUFFLE_SOURCE}"
    cd "${TP_SOURCE_DIR}/${BITSHUFFLE_SOURCE}"

    local machine_type
    machine_type="$(uname -m)"
    local arches="default"
    if [[ "${machine_type}" == "arm64" ]]; then
        arches="default neon"
    fi

    local to_link=""
    local arch
    for arch in ${arches}; do
        local arch_flag=""
        if [[ "${arch}" == "neon" ]]; then
            arch_flag="-march=armv8-a+crc"
        fi

        local tmp_obj="bitshuffle_${arch}_tmp.o"
        local dst_obj="bitshuffle_${arch}.o"
        local bshuf_cflags="-I./src -I./lz4 -I${TP_INCLUDE_DIR}/lz4 -std=c99 -O3 -DNDEBUG -fPIC"

        "${CC}" ${bshuf_cflags} ${arch_flag} -c src/bitshuffle_core.c -o bitshuffle_core.o
        "${CC}" ${bshuf_cflags} ${arch_flag} -c src/bitshuffle.c -o bitshuffle.o
        "${CC}" ${bshuf_cflags} ${arch_flag} -c src/iochain.c -o iochain.o
        "${LD_BIN}" -r -o "${tmp_obj}" bitshuffle_core.o bitshuffle.o iochain.o

        if [[ "${arch}" == "neon" ]]; then
            "${NM_BIN}" -gU "${tmp_obj}" | awk '{print $3, $3"_neon"}' | grep -v '^$' > renames.txt
            "${OBJCOPY}" --redefine-syms=renames.txt "${tmp_obj}" "${dst_obj}"
        else
            mv "${tmp_obj}" "${dst_obj}"
        fi

        to_link="${to_link} ${dst_obj}"
    done

    rm -f libbitshuffle.a
    "${AR}" rcs libbitshuffle.a ${to_link}
    mkdir -p "${TP_INSTALL_DIR}/lib" "${TP_INCLUDE_DIR}/bitshuffle"
    cp libbitshuffle.a "${TP_INSTALL_DIR}/lib/"
    cp src/*.h "${TP_INCLUDE_DIR}/bitshuffle/"
    sync_lib64_links
}

build_datasketches() {
    if [[ -d "${TP_INCLUDE_DIR}/datasketches" && -f "${TP_INCLUDE_DIR}/datasketches/hll.hpp" ]]; then
        return 0
    fi

    check_if_source_exist "${DATASKETCHES_SOURCE}"
    mkdir -p "${TP_INCLUDE_DIR}/datasketches"
    cp -R "${TP_SOURCE_DIR}/${DATASKETCHES_SOURCE}/common/include/"* "${TP_INCLUDE_DIR}/datasketches/" || true
    cp -R "${TP_SOURCE_DIR}/${DATASKETCHES_SOURCE}/cpc/include/"* "${TP_INCLUDE_DIR}/datasketches/" || true
    cp -R "${TP_SOURCE_DIR}/${DATASKETCHES_SOURCE}/fi/include/"* "${TP_INCLUDE_DIR}/datasketches/" || true
    cp -R "${TP_SOURCE_DIR}/${DATASKETCHES_SOURCE}/hll/include/"* "${TP_INCLUDE_DIR}/datasketches/" || true
    cp -R "${TP_SOURCE_DIR}/${DATASKETCHES_SOURCE}/kll/include/"* "${TP_INCLUDE_DIR}/datasketches/" || true
    cp -R "${TP_SOURCE_DIR}/${DATASKETCHES_SOURCE}/quantiles/include/"* "${TP_INCLUDE_DIR}/datasketches/" || true
    cp -R "${TP_SOURCE_DIR}/${DATASKETCHES_SOURCE}/req/include/"* "${TP_INCLUDE_DIR}/datasketches/" || true
    cp -R "${TP_SOURCE_DIR}/${DATASKETCHES_SOURCE}/sampling/include/"* "${TP_INCLUDE_DIR}/datasketches/" || true
    cp -R "${TP_SOURCE_DIR}/${DATASKETCHES_SOURCE}/theta/include/"* "${TP_INCLUDE_DIR}/datasketches/" || true
    cp -R "${TP_SOURCE_DIR}/${DATASKETCHES_SOURCE}/tuple/include/"* "${TP_INCLUDE_DIR}/datasketches/" || true
}

build_ryu() {
    if [[ -f "${TP_INSTALL_DIR}/lib/libryu.a" && -f "${TP_INCLUDE_DIR}/ryu/ryu.h" ]]; then
        return 0
    fi

    check_if_source_exist "${RYU_SOURCE}"
    cd "${TP_SOURCE_DIR}/${RYU_SOURCE}/ryu"
    make clean >/dev/null 2>&1 || true
    make -j"${PARALLEL}" CC="${CC}" AR="${AR}" CFLAGS="${CFLAGS}"

    mkdir -p "${TP_INSTALL_DIR}/lib" "${TP_INCLUDE_DIR}/ryu"
    cp libryu.a "${TP_INSTALL_DIR}/lib/"
    cp *.h "${TP_INCLUDE_DIR}/ryu/"
    sync_lib64_links
}

build_libdivide() {
    if [[ -f "${TP_INCLUDE_DIR}/libdivide.h" ]]; then
        return 0
    fi

    check_if_source_exist "${LIBDIVIDE_SOURCE}"
    cp "${TP_SOURCE_DIR}/${LIBDIVIDE_SOURCE}/libdivide.h" "${TP_INCLUDE_DIR}/"
}

build_icu() {
    if [[ -f "${TP_INSTALL_DIR}/lib/libicuuc.a" && -f "${TP_INCLUDE_DIR}/unicode/ucasemap.h" ]]; then
        return 0
    fi

    check_if_source_exist "${ICU_SOURCE}"
    cd "${TP_SOURCE_DIR}/${ICU_SOURCE}/source"
    portable_strip_crlf ./runConfigureICU ./config.* ./configure ./mkinstalldirs

    (
        unset CPPFLAGS
        unset CXXFLAGS
        unset CFLAGS
        export CFLAGS="-O3 -fno-omit-frame-pointer -fPIC"
        export CXXFLAGS="-O3 -fno-omit-frame-pointer -fPIC"
        ./runConfigureICU macOS --prefix="${TP_INSTALL_DIR}" --enable-static --disable-shared
        make -j"${PARALLEL}"
        make install
    )
    sync_lib64_links
}

build_hyperscan() {
    if [[ -f "${TP_INSTALL_DIR}/lib/libhs.a" && -f "${TP_INCLUDE_DIR}/hs/hs.h" ]]; then
        return 0
    fi

    check_if_source_exist "${HYPERSCAN_SOURCE}"
    cd "${TP_SOURCE_DIR}/${HYPERSCAN_SOURCE}"
    rm -rf cmake_build
    mkdir -p cmake_build
    cd cmake_build

    local boost_root="${TP_INSTALL_DIR}"
    local boost_cmake_dir=""
    boost_cmake_dir="$(find_boost_cmake_dir "${boost_root}" || true)"
    if [[ -z "${boost_cmake_dir}" ]]; then
        ensure_formula boost
        boost_root="$(formula_prefix boost)"
        boost_cmake_dir="$(find_boost_cmake_dir "${boost_root}")"
    fi

    "${CMAKE_CMD}" .. \
        -G "${CMAKE_GENERATOR}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib \
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
        -DCMAKE_C_FLAGS="$(append_flags "${CFLAGS}" "-Wno-error")" \
        -DCMAKE_CXX_FLAGS="$(append_flags "${CXXFLAGS}" "-D_LIBCPP_HAS_NO_HASH_MEMORY=1 -Wno-error")" \
        -DBOOST_ROOT="${boost_root}" \
        -DBoost_ROOT="${boost_root}" \
        -DBoost_DIR="${boost_cmake_dir}" \
        -DBoost_NO_SYSTEM_PATHS=ON \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    "${CMAKE_CMD}" --build . -j "${PARALLEL}" --target hs
    "${CMAKE_CMD}" --build . -j "${PARALLEL}" --target hs_compile
    "${CMAKE_CMD}" --build . -j "${PARALLEL}" --target hs_runtime
    "${CMAKE_CMD}" --install .

    if [[ -f "${TP_INSTALL_DIR}/lib/libhs.a" ]]; then
        local shim_src="${TP_DIR}/build/hyperscan/hash_memory_impl.cc"
        local shim_obj="${TP_DIR}/build/hyperscan/hash_memory_shim.o"
        mkdir -p "${TP_DIR}/build/hyperscan"
        write_hash_memory_shim "${shim_src}"
        "${CXX}" ${CXXFLAGS} -c "${shim_src}" -o "${shim_obj}"
        append_object_to_archive "${TP_INSTALL_DIR}/lib/libhs.a" "${shim_obj}"
    fi
    sync_lib64_links
}

build_curl() {
    if [[ -f "${TP_INSTALL_DIR}/lib/libcurl.a" && -f "${TP_INCLUDE_DIR}/curl/curl.h" ]]; then
        return 0
    fi

    check_if_source_exist "${CURL_SOURCE}"
    cd "${TP_SOURCE_DIR}/${CURL_SOURCE}"
    if [[ -f Makefile ]]; then
        make distclean >/dev/null 2>&1 || true
    fi

    LDFLAGS="-L${TP_INSTALL_DIR}/lib -L${TP_INSTALL_DIR}/lib64" LIBS="-lssl -lcrypto" \
        ./configure \
        --prefix="${TP_INSTALL_DIR}" \
        --disable-shared \
        --enable-static \
        --without-librtmp \
        --with-ssl="${TP_INSTALL_DIR}" \
        --without-libidn2 \
        --without-libgsasl \
        --disable-ldap \
        --enable-ipv6 \
        --without-brotli \
        --disable-ftp \
        --disable-ftps \
        --disable-file \
        --disable-dict \
        --disable-telnet \
        --disable-tftp \
        --disable-pop3 \
        --disable-pop3s \
        --disable-imap \
        --disable-imaps \
        --disable-smb \
        --disable-smtp \
        --disable-smtps \
        --disable-gopher \
        --disable-mqtt \
        --disable-rtsp \
        --disable-ldaps \
        --disable-unix-sockets \
        --without-zstd \
        --without-libidn \
        --without-libssh2 \
        --without-nghttp2 \
        --without-nghttp3 \
        --without-ngtcp2 \
        --without-libpsl \
        --with-pic \
        --enable-optimize \
        --disable-debug \
        --enable-http

    make -j"${PARALLEL}"
    make install
    sync_lib64_links
}

build_simdutf() {
    if [[ -f "${TP_INSTALL_DIR}/lib/libsimdutf.a" && -f "${TP_INCLUDE_DIR}/simdutf.h" ]]; then
        return 0
    fi

    check_if_source_exist "${SIMDUTF_SOURCE}"
    cd "${TP_SOURCE_DIR}/${SIMDUTF_SOURCE}"
    rm -rf cmake_build
    mkdir -p cmake_build
    cd cmake_build

    "${CMAKE_CMD}" .. \
        -G "${CMAKE_GENERATOR}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DSIMDUTF_TESTS=OFF \
        -DSIMDUTF_TOOLS=OFF \
        -DSIMDUTF_ICONV=OFF \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    "${CMAKE_CMD}" --build . -j "${PARALLEL}"
    "${CMAKE_CMD}" --install .
    sync_lib64_links
}

build_fmt() {
    if [[ -f "${TP_INSTALL_DIR}/lib/libfmt.a" && -f "${TP_INCLUDE_DIR}/fmt/format.h" ]]; then
        return 0
    fi

    check_if_source_exist "${FMT_SOURCE}"
    cd "${TP_SOURCE_DIR}/${FMT_SOURCE}"
    rm -rf cmake_build
    mkdir -p cmake_build
    cd cmake_build

    "${CMAKE_CMD}" .. \
        -G "${CMAKE_GENERATOR}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DFMT_TEST=OFF \
        -DFMT_DOC=OFF \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    "${CMAKE_CMD}" --build . -j "${PARALLEL}"
    "${CMAKE_CMD}" --install .
    sync_lib64_links
}

build_cctz() {
    if [[ -f "${TP_INSTALL_DIR}/lib/libcctz.a" && -f "${TP_INCLUDE_DIR}/cctz/civil_time.h" ]]; then
        return 0
    fi

    check_if_source_exist "${CCTZ_SOURCE}"
    cd "${TP_SOURCE_DIR}/${CCTZ_SOURCE}"

    local obj_files=()
    local src
    for src in src/*.cc; do
        local obj
        obj="$(basename "${src}" .cc).o"
        case "${obj}" in
            time_tool.o|cctz_benchmark.o|*_test.o)
                continue
                ;;
        esac
        "${CXX}" ${CXXFLAGS} -Wall -Iinclude -std=c++11 -c -o "${obj}" "${src}"
        obj_files+=("${obj}")
    done

    "${AR}" rcs libcctz.a "${obj_files[@]}"
    mkdir -p "${TP_INSTALL_DIR}/lib" "${TP_INCLUDE_DIR}/cctz"
    cp libcctz.a "${TP_INSTALL_DIR}/lib/"
    cp include/cctz/*.h "${TP_INCLUDE_DIR}/cctz/"
    sync_lib64_links
}

build_croaringbitmap() {
    if [[ -f "${TP_INSTALL_DIR}/lib/libroaring.a" && -f "${TP_INCLUDE_DIR}/roaring/roaring.h" ]]; then
        return 0
    fi

    check_if_source_exist "${CROARINGBITMAP_SOURCE}"
    cd "${TP_SOURCE_DIR}/${CROARINGBITMAP_SOURCE}"
    mkdir -p cmake
    if [[ ! -f cmake/CPM.cmake ]]; then
        curl -fL -o cmake/CPM.cmake "https://github.com/cpm-cmake/CPM.cmake/releases/download/v0.38.6/CPM.cmake" >/dev/null 2>&1 || true
    fi

    rm -rf cmake_build
    mkdir -p cmake_build
    cd cmake_build

    local force_avx=FALSE
    if [[ "$(uname -m)" != "arm64" ]]; then
        force_avx=ON
    fi

    if ! "${CMAKE_CMD}" .. \
        -G "${CMAKE_GENERATOR}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DROARING_BUILD_STATIC=ON \
        -DENABLE_ROARING_TESTS=OFF \
        -DROARING_DISABLE_NATIVE=ON \
        -DFORCE_AVX="${force_avx}" \
        -DROARING_DISABLE_AVX512=ON \
        -DCMAKE_C_FLAGS="$(append_flags "${CFLAGS}" "-Wno-error")" \
        -DCMAKE_CXX_FLAGS="$(append_flags "${CXXFLAGS}" "-D_LIBCPP_HAS_NO_HASH_MEMORY=1 -Wno-error")" \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5; then
        cd ..
        mkdir -p "${TP_DIR}/build/croaringbitmap"
        local object_files=()
        local include_flags="-I./include -I./src"
        local src_file
        for src_file in src/*.c src/*/*.c; do
            [[ -f "${src_file}" ]] || continue
            local obj_file="${TP_DIR}/build/croaringbitmap/$(basename "${src_file}" .c).o"
            "${CC}" ${CFLAGS} ${include_flags} -c "${src_file}" -o "${obj_file}"
            object_files+=("${obj_file}")
        done
        if [[ "${#object_files[@]}" -eq 0 ]]; then
            echo "Failed to compile croaringbitmap"
            exit 1
        fi

        mkdir -p "${TP_INSTALL_DIR}/lib" "${TP_INCLUDE_DIR}/roaring"
        cp -R include/roaring/* "${TP_INCLUDE_DIR}/roaring/"
        if [[ -f cpp/roaring.hh ]]; then
            cp cpp/roaring.hh "${TP_INCLUDE_DIR}/roaring/"
        fi
        "${AR}" rcs "${TP_INSTALL_DIR}/lib/libroaring.a" "${object_files[@]}"
        "${RANLIB}" "${TP_INSTALL_DIR}/lib/libroaring.a" >/dev/null 2>&1 || true
    else
        "${CMAKE_CMD}" --build . -j "${PARALLEL}"
        "${CMAKE_CMD}" --install .
        if [[ -f ../cpp/roaring.hh ]]; then
            cp ../cpp/roaring.hh "${TP_INCLUDE_DIR}/roaring/"
        fi
    fi

    local shim_src="${TP_DIR}/build/croaringbitmap/hash_memory_impl.cc"
    local shim_obj="${TP_DIR}/build/croaringbitmap/hash_memory_shim.o"
    mkdir -p "${TP_DIR}/build/croaringbitmap"
    write_hash_memory_shim "${shim_src}"
    "${CXX}" ${CXXFLAGS} -c "${shim_src}" -o "${shim_obj}"
    append_object_to_archive "${TP_INSTALL_DIR}/lib/libroaring.a" "${shim_obj}"
    sync_lib64_links
}

unsupported_package() {
    local package="$1"
    case "${package}" in
        starcache)
            echo "UNSUPPORTED[darwin] starcache: only Linux prebuilt archives are available" >&2
            ;;
        tenann)
            echo "UNSUPPORTED[darwin] tenann: only Linux prebuilt archives are available" >&2
            ;;
        pprof)
            echo "UNSUPPORTED[darwin] pprof: only Linux binaries are available" >&2
            ;;
        breakpad)
            echo "UNSUPPORTED[darwin] breakpad: no Darwin package implementation is provided" >&2
            ;;
        *)
            echo "UNSUPPORTED[darwin] ${package}: no Darwin package implementation is provided" >&2
            ;;
    esac
}

is_darwin_unsupported_package() {
    local package="$1"
    local unsupported

    for unsupported in ${DARWIN_UNSUPPORTED_PACKAGES:-}; do
        if [[ "${package}" == "${unsupported}" ]]; then
            return 0
        fi
    done
    [[ "${package}" == "breakpad" ]]
}

validate_requested_package() {
    local package="$1"

    if is_darwin_unsupported_package "${package}"; then
        unsupported_package "${package}"
        echo "Darwin build cannot satisfy requested package '${package}'." >&2
        exit 1
    fi
}

build_formula_gtest() {
    ensure_formula googletest
    local prefix
    prefix="$(formula_prefix googletest)"
    link_children_if_missing "${prefix}/include" "${TP_INCLUDE_DIR}"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/libgtest*.a" "${prefix}/lib/libgmock*.a" "${prefix}/lib/libgtest*.dylib" "${prefix}/lib/libgmock*.dylib"
    sync_lib64_links
}

build_formula_rapidjson() {
    ensure_formula rapidjson
    local prefix
    prefix="$(formula_prefix rapidjson)"
    link_if_missing "${prefix}/include/rapidjson" "${TP_INCLUDE_DIR}/rapidjson"
    link_formula_metadata "${prefix}"
}

build_formula_libevent() {
    ensure_formula libevent
    local prefix
    prefix="$(formula_prefix libevent)"
    link_if_missing "${prefix}/include/event2" "${TP_INCLUDE_DIR}/event2"
    link_matching_if_missing "${TP_INCLUDE_DIR}" \
        "${prefix}/include/event.h" \
        "${prefix}/include/evdns.h" \
        "${prefix}/include/evhttp.h" \
        "${prefix}/include/evrpc.h" \
        "${prefix}/include/evutil.h"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/libevent*.a" "${prefix}/lib/libevent*.dylib"
    link_formula_metadata "${prefix}"
    sync_lib64_links
}

build_formula_zlib() {
    ensure_formula zlib
    local prefix
    prefix="$(formula_prefix zlib)"
    link_if_missing "${prefix}/include/zlib.h" "${TP_INCLUDE_DIR}/zlib.h"
    link_if_missing "${prefix}/include/zconf.h" "${TP_INCLUDE_DIR}/zconf.h"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/libz.a" "${prefix}/lib/libz*.dylib"
    sync_lib64_links
}

build_formula_lz4() {
    ensure_formula lz4
    local prefix
    prefix="$(formula_prefix lz4)"
    mkdir -p "${TP_INCLUDE_DIR}/lz4"
    link_matching_if_missing "${TP_INCLUDE_DIR}/lz4" "${prefix}/include/lz4*.h"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/liblz4.a" "${prefix}/lib/liblz4*.dylib"
    sync_lib64_links
}

build_formula_zstd() {
    ensure_formula zstd
    local prefix
    prefix="$(formula_prefix zstd)"
    mkdir -p "${TP_INCLUDE_DIR}/zstd"
    link_matching_if_missing "${TP_INCLUDE_DIR}" "${prefix}/include/zstd*.h"
    link_matching_if_missing "${TP_INCLUDE_DIR}/zstd" "${prefix}/include/zstd*.h"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/libzstd.a" "${prefix}/lib/libzstd*.dylib"
    sync_lib64_links
}

build_formula_lzo2() {
    ensure_formula lzo
    local prefix
    prefix="$(formula_prefix lzo)"
    link_children_if_missing "${prefix}/include/lzo" "${TP_INCLUDE_DIR}/lzo"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/liblzo2.a" "${prefix}/lib/liblzo2*.dylib"
    sync_lib64_links
}

build_formula_bzip() {
    ensure_formula bzip2
    local prefix
    prefix="$(formula_prefix bzip2)"
    link_if_missing "${prefix}/include/bzlib.h" "${TP_INCLUDE_DIR}/bzlib.h"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/libbz2.a" "${prefix}/lib/libbz2*.dylib"
    sync_lib64_links
}

build_formula_openssl() {
    ensure_formula openssl@3
    local prefix
    prefix="$(formula_prefix openssl@3)"
    link_if_missing "${prefix}/include/openssl" "${TP_INCLUDE_DIR}/openssl"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/libssl.a" "${prefix}/lib/libcrypto.a" "${prefix}/lib/libssl*.dylib" "${prefix}/lib/libcrypto*.dylib"
    link_formula_metadata "${prefix}"
    sync_lib64_links
}

build_formula_simdjson() {
    ensure_formula simdjson
    local prefix
    prefix="$(formula_prefix simdjson)"
    link_if_missing "${prefix}/include/simdjson.h" "${TP_INCLUDE_DIR}/simdjson.h"
    link_if_missing "${prefix}/lib/libsimdjson_static.a" "${TP_INSTALL_DIR}/lib/libsimdjson.a"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/libsimdjson*.dylib"
    link_formula_metadata "${prefix}"
    sync_lib64_links
}

build_formula_snappy() {
    ensure_formula snappy
    local prefix
    prefix="$(formula_prefix snappy)"
    mkdir -p "${TP_INCLUDE_DIR}/snappy"
    link_matching_if_missing "${TP_INCLUDE_DIR}" "${prefix}/include/snappy*.h"
    link_matching_if_missing "${TP_INCLUDE_DIR}/snappy" "${prefix}/include/snappy*.h"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/libsnappy.a" "${prefix}/lib/libsnappy*.dylib"
    sync_lib64_links
}

build_formula_gperftools() {
    ensure_formula gperftools
    local prefix
    prefix="$(formula_prefix gperftools)"
    link_if_missing "${prefix}" "${TP_INSTALL_DIR}/gperftools"
    link_children_if_missing "${prefix}/include/gperftools" "${TP_INCLUDE_DIR}/gperftools"
}

build_formula_kerberos() {
    ensure_formula krb5
    local prefix
    prefix="$(formula_prefix krb5)"
    link_if_missing "${prefix}/include/gssapi" "${TP_INCLUDE_DIR}/gssapi"
    link_if_missing "${prefix}/include/krb5.h" "${TP_INCLUDE_DIR}/krb5.h"
    link_if_missing "${prefix}/include/com_err.h" "${TP_INCLUDE_DIR}/com_err.h"
    link_if_missing "${prefix}/include/profile.h" "${TP_INCLUDE_DIR}/profile.h"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/"*.a "${prefix}/lib/"*.dylib
    link_formula_metadata "${prefix}"
    sync_lib64_links
}

build_formula_sasl() {
    ensure_formula cyrus-sasl
    local prefix
    prefix="$(formula_prefix cyrus-sasl)"
    link_if_missing "${prefix}/include/sasl" "${TP_INCLUDE_DIR}/sasl"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/libsasl2.a" "${prefix}/lib/libsasl2*.dylib"
    link_formula_metadata "${prefix}"
    sync_lib64_links
}

# Build abseil from TP source (pinned 20220623). Homebrew's abseil ABI drifts
# across releases and newer ones are too new for gRPC 1.43; source build
# keeps the whole TP chain consistent.
build_absl() {
    check_if_source_exist "${ABSL_SOURCE}"

    # Drop stale symlinks from a prior build_formula_absl run — cmake --install
    # would follow them into Homebrew's Cellar (corrupt or EACCES).
    rm -rf "${TP_INCLUDE_DIR}/absl"
    rm -rf "${TP_INSTALL_DIR}/lib/cmake/absl"
    safe_remove_glob \
        "${TP_INSTALL_DIR}/lib/libabsl_"*.a \
        "${TP_INSTALL_DIR}/lib/libabsl_"*.dylib \
        "${TP_INSTALL_DIR}/lib64/libabsl_"*.a \
        "${TP_INSTALL_DIR}/lib64/libabsl_"*.dylib \
        "${TP_INSTALL_DIR}/lib/pkgconfig/absl_"*.pc

    cd "${TP_SOURCE_DIR}/${ABSL_SOURCE}"

    # Narrow the APPLE+Clang branch to AppleClang only — Homebrew LLVM clang
    # does not implement Apple Clang's -Xarch_* driver and fails on arm64.
    local patch_file="${TP_PATCH_DIR}/abseil-cpp-20220623.0-apple-clang-only.patch"
    local copts_file="absl/copts/AbseilConfigureCopts.cmake"
    if [[ -f "${patch_file}" ]] && grep -q "MATCHES \\[\\[Clang\\]\\]" "${copts_file}" 2>/dev/null; then
        patch -p1 --forward --batch < "${patch_file}" >/dev/null || true
    fi

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"
    rm -rf CMakeCache.txt CMakeFiles/
    "${CMAKE_CMD}" -G "${CMAKE_GENERATOR}" .. \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_CXX_STANDARD=17 \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    "${BUILD_SYSTEM}" -j "${PARALLEL}" install
    sync_lib64_links
}

# Build gRPC 1.43 from TP source with all providers pinned to TP deps
# (protobuf 3.14, abseil 20220623, re2, openssl). The Homebrew symlink
# approach would drag in protobuf 33.x and collide with arrow-flight targets.
build_grpc() {
    check_if_source_exist "${GRPC_SOURCE}"

    # Drop stale symlinks from a prior build_formula_grpc run (see build_absl).
    rm -rf \
        "${TP_INCLUDE_DIR}/grpc" \
        "${TP_INCLUDE_DIR}/grpc++" \
        "${TP_INCLUDE_DIR}/grpcpp"
    rm -rf \
        "${TP_INSTALL_DIR}/lib/cmake/grpc" \
        "${TP_INSTALL_DIR}/lib/cmake/gRPC"
    safe_remove_glob \
        "${TP_INSTALL_DIR}/lib/libgrpc"*.a \
        "${TP_INSTALL_DIR}/lib/libgrpc"*.dylib \
        "${TP_INSTALL_DIR}/lib/libgpr"*.a \
        "${TP_INSTALL_DIR}/lib/libgpr"*.dylib \
        "${TP_INSTALL_DIR}/lib/libaddress_sorting"*.a \
        "${TP_INSTALL_DIR}/lib/libaddress_sorting"*.dylib \
        "${TP_INSTALL_DIR}/lib/libupb"*.a \
        "${TP_INSTALL_DIR}/lib/libupb"*.dylib \
        "${TP_INSTALL_DIR}/lib/libutf8_range"*.a \
        "${TP_INSTALL_DIR}/lib/libutf8_range"*.dylib \
        "${TP_INSTALL_DIR}/lib64/libgrpc"*.a \
        "${TP_INSTALL_DIR}/lib64/libgrpc"*.dylib \
        "${TP_INSTALL_DIR}/lib64/libgpr"*.a \
        "${TP_INSTALL_DIR}/lib64/libgpr"*.dylib \
        "${TP_INSTALL_DIR}/lib/pkgconfig/grpc"*.pc \
        "${TP_INSTALL_DIR}/lib/pkgconfig/gpr"*.pc \
        "${TP_INSTALL_DIR}/bin/grpc_cpp_plugin" \
        "${TP_INSTALL_DIR}/bin/grpc_"* \
        "${TP_INSTALL_DIR}/bin/protoc-gen-grpc"*

    cd "${TP_SOURCE_DIR}/${GRPC_SOURCE}"
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"
    rm -rf CMakeCache.txt CMakeFiles/
    "${CMAKE_CMD}" -G "${CMAKE_GENERATOR}" .. \
        -DCMAKE_PREFIX_PATH="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DgRPC_INSTALL=ON \
        -DgRPC_BUILD_TESTS=OFF \
        -DgRPC_BUILD_CSHARP_EXT=OFF \
        -DgRPC_BUILD_GRPC_RUBY_PLUGIN=OFF \
        -DgRPC_BUILD_GRPC_PYTHON_PLUGIN=OFF \
        -DgRPC_BUILD_GRPC_PHP_PLUGIN=OFF \
        -DgRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN=OFF \
        -DgRPC_BUILD_GRPC_NODE_PLUGIN=OFF \
        -DgRPC_BUILD_GRPC_CSHARP_PLUGIN=OFF \
        -DgRPC_BACKWARDS_COMPATIBILITY_MODE=OFF \
        -DgRPC_SSL_PROVIDER=package \
        -DOPENSSL_ROOT_DIR="${TP_INSTALL_DIR}" \
        -DOPENSSL_USE_STATIC_LIBS=TRUE \
        -DgRPC_ZLIB_PROVIDER=package \
        -DZLIB_LIBRARY_RELEASE="${TP_INSTALL_DIR}/lib/libz.a" \
        -DgRPC_ABSL_PROVIDER=package \
        -Dabsl_DIR="${TP_INSTALL_DIR}/lib/cmake/absl" \
        -DgRPC_PROTOBUF_PROVIDER=package \
        -DgRPC_RE2_PROVIDER=package \
        -DRE2_INCLUDE_DIR="${TP_INSTALL_DIR}/include" \
        -DRE2_LIBRARY="${TP_INSTALL_DIR}/lib/libre2.a" \
        -DgRPC_CARES_PROVIDER=module \
        -DCARES_ROOT_DIR="${TP_SOURCE_DIR}/${CARES_SOURCE}/" \
        -DCMAKE_CXX_STANDARD=17 \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    "${BUILD_SYSTEM}" -j "${PARALLEL}" install
    sync_lib64_links
}

build_formula_flatbuffers() {
    ensure_formula flatbuffers
    local prefix
    prefix="$(formula_prefix flatbuffers)"
    link_if_missing "${prefix}/include/flatbuffers" "${TP_INCLUDE_DIR}/flatbuffers"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/libflatbuffers.a" "${prefix}/lib/libflatbuffers"*.dylib
    link_matching_if_missing "${TP_INSTALL_DIR}/bin" "${prefix}/bin/flatc"
    link_formula_metadata "${prefix}"
    sync_lib64_links
}

build_formula_brotli() {
    ensure_formula brotli
    local prefix
    prefix="$(formula_prefix brotli)"
    link_children_if_missing "${prefix}/include/brotli" "${TP_INCLUDE_DIR}/brotli"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/libbrotli"*.a "${prefix}/lib/libbrotli"*.dylib
    sync_lib64_links
}

build_arrow() {
    # libarrow_flight_sql.a as short-circuit sentinel: it is installed last,
    # so its presence implies libarrow_flight.a + libarrow.a; a pre-Flight
    # install or a run interrupted mid-way falls through to a full rebuild.
    if [[ -f "${TP_INSTALL_DIR}/lib/libarrow.a" && -f "${TP_INSTALL_DIR}/lib/libparquet.a" \
          && -f "${TP_INSTALL_DIR}/lib/libarrow_flight_sql.a" && -f "${TP_INCLUDE_DIR}/arrow/api.h" ]]; then
        return 0
    fi

    check_if_source_exist "${ARROW_SOURCE}"
    safe_remove_glob \
        "${TP_INSTALL_DIR}/lib/libarrow"* \
        "${TP_INSTALL_DIR}/lib/libparquet"* \
        "${TP_INSTALL_DIR}/lib/libgandiva"* \
        "${TP_INSTALL_DIR}/lib64/libarrow"* \
        "${TP_INSTALL_DIR}/lib64/libparquet"* \
        "${TP_INSTALL_DIR}/lib64/libgandiva"* \
        "${TP_INSTALL_DIR}/lib/pkgconfig/arrow"*.pc \
        "${TP_INSTALL_DIR}/lib/pkgconfig/parquet"*.pc
    rm -rf \
        "${TP_INCLUDE_DIR}"/arrow* \
        "${TP_INCLUDE_DIR}/parquet" \
        "${TP_INCLUDE_DIR}/gandiva" \
        "${TP_INSTALL_DIR}/lib/cmake"/Arrow* \
        "${TP_INSTALL_DIR}/lib/cmake"/Parquet* \
        "${TP_INSTALL_DIR}/lib/cmake"/Gandiva*

    cd "${TP_SOURCE_DIR}/${ARROW_SOURCE}/cpp"
    rm -rf release
    mkdir -p release
    cd release

    export ARROW_BROTLI_URL="${TP_SOURCE_DIR}/${BROTLI_NAME}"
    export ARROW_GLOG_URL="${TP_SOURCE_DIR}/${GLOG_NAME}"
    export ARROW_LZ4_URL="${TP_SOURCE_DIR}/${LZ4_NAME}"
    export ARROW_SNAPPY_URL="${TP_SOURCE_DIR}/${SNAPPY_NAME}"
    export ARROW_ZLIB_URL="${TP_SOURCE_DIR}/${ZLIB_NAME}"
    export ARROW_FLATBUFFERS_URL="${TP_SOURCE_DIR}/${FLATBUFFERS_NAME}"
    export ARROW_ZSTD_URL="${TP_SOURCE_DIR}/${ZSTD_NAME}"
    export ARROW_THRIFT_URL="${TP_SOURCE_DIR}/${THRIFT_NAME}"

    # Pin pkg-config to TP's protobuf to keep arrow-flight's static-link probe
    # from picking up Homebrew's v33.x. Saved/restored so it does not leak.
    local old_pkg_config_path="${PKG_CONFIG_PATH:-}"
    export PKG_CONFIG_PATH="${TP_INSTALL_DIR}/lib/pkgconfig${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}"

    # Arrow 19's FindProtobufAlt tries find_package(protobuf CONFIG) then
    # find_package(Protobuf). CMAKE_DISABLE_FIND_PACKAGE_<name> is case-
    # sensitive: the lowercase _protobuf guard (set below) blocks only the
    # CONFIG probe, which is where a stray protobuf-config.cmake would leak.
    # The capital-P module fallback is intentionally kept and pinned to TP
    # via the Protobuf_LIBRARY / _INCLUDE_DIR / _PROTOC_EXECUTABLE hints.
    # Adding _Protobuf (capital) would break Flight configure.

    local formula
    for formula in rapidjson zlib lz4 snappy zstd brotli flatbuffers; do
        ensure_formula "${formula}"
    done

    local rapidjson_prefix
    local zlib_prefix
    local lz4_prefix
    local snappy_prefix
    local brotli_prefix
    local flatbuffers_prefix
    rapidjson_prefix="$(formula_prefix rapidjson)"
    zlib_prefix="$(formula_prefix zlib)"
    lz4_prefix="$(formula_prefix lz4)"
    snappy_prefix="$(formula_prefix snappy)"
    brotli_prefix="$(formula_prefix brotli)"
    flatbuffers_prefix="$(formula_prefix flatbuffers)"

    local arrow_simd_level="DEFAULT"
    local arrow_runtime_simd_level="SSE4_2"
    if [[ "${THIRD_PARTY_BUILD_WITH_AVX2}" != "OFF" ]]; then
        arrow_simd_level="AVX2"
        arrow_runtime_simd_level="AVX2"
    fi

    local arrow_prefix_path="${TP_INSTALL_DIR};${flatbuffers_prefix};${snappy_prefix};${lz4_prefix};${brotli_prefix};${zlib_prefix};${rapidjson_prefix}"
    local libtool_wrapper
    libtool_wrapper="$(ensure_macos_libtool_wrapper)"

    "${CMAKE_CMD}" .. \
        -G "${CMAKE_GENERATOR}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_LIBTOOL="${libtool_wrapper}" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DARROW_BUILD_STATIC=ON \
        -DARROW_BUILD_SHARED=OFF \
        -DARROW_BUILD_TESTS=OFF \
        -DARROW_BUILD_EXAMPLES=OFF \
        -DARROW_BUILD_INTEGRATION=OFF \
        -DARROW_BUILD_UTILITIES=OFF \
        -DARROW_BUILD_BENCHMARKS=OFF \
        -DARROW_GANDIVA=OFF \
        -DARROW_PARQUET=ON \
        -DARROW_JSON=ON \
        -DARROW_IPC=ON \
        -DARROW_USE_GLOG=OFF \
        -DARROW_WITH_BROTLI=ON \
        -DARROW_WITH_LZ4=ON \
        -DARROW_WITH_SNAPPY=ON \
        -DARROW_WITH_ZLIB=ON \
        -DARROW_WITH_ZSTD=ON \
        -DARROW_WITH_UTF8PROC=OFF \
        -DARROW_WITH_RE2=OFF \
        -DARROW_JEMALLOC=OFF \
        -DARROW_MIMALLOC=OFF \
        -DARROW_SIMD_LEVEL="${arrow_simd_level}" \
        -DARROW_RUNTIME_SIMD_LEVEL="${arrow_runtime_simd_level}" \
        -DARROW_GFLAGS_USE_SHARED=OFF \
        -DJEMALLOC_HOME="${TP_INSTALL_DIR}/jemalloc" \
        -Dzstd_SOURCE=BUNDLED \
        -DRapidJSON_ROOT="${rapidjson_prefix}" \
        -DARROW_SNAPPY_USE_SHARED=OFF \
        -DZLIB_ROOT="${zlib_prefix}" \
        -DLZ4_INCLUDE_DIR="${lz4_prefix}/include" \
        -DARROW_LZ4_USE_SHARED=OFF \
        -DBROTLI_ROOT="${brotli_prefix}" \
        -DARROW_BROTLI_USE_SHARED=OFF \
        -Dgflags_ROOT="${TP_INSTALL_DIR}" \
        -DSnappy_ROOT="${snappy_prefix}" \
        -DGLOG_ROOT="${TP_INSTALL_DIR}" \
        -DLZ4_ROOT="${lz4_prefix}" \
        -DBoost_DIR="${TP_INSTALL_DIR}" \
        -DBoost_ROOT="${TP_INSTALL_DIR}" \
        -DARROW_BOOST_USE_SHARED=OFF \
        -DBoost_NO_BOOST_CMAKE=ON \
        -DARROW_FLIGHT=ON \
        -DARROW_FLIGHT_SQL=ON \
        -DCMAKE_IGNORE_PREFIX_PATH="/opt/homebrew/anaconda3" \
        -DCMAKE_DISABLE_FIND_PACKAGE_protobuf=ON \
        -DProtobuf_PROTOC_EXECUTABLE="${TP_INSTALL_DIR}/bin/protoc" \
        -DProtobuf_INCLUDE_DIR="${TP_INSTALL_DIR}/include" \
        -DProtobuf_LIBRARY="${TP_INSTALL_DIR}/lib/libprotobuf.a" \
        -DProtobuf_PROTOC_LIBRARY="${TP_INSTALL_DIR}/lib/libprotoc.a" \
        -DProtobuf_LITE_LIBRARY="${TP_INSTALL_DIR}/lib/libprotobuf-lite.a" \
        -DProtobuf_USE_STATIC_LIBS=ON \
        -Dflatbuffers_ROOT="${flatbuffers_prefix}" \
        -DCMAKE_PREFIX_PATH="${arrow_prefix_path}" \
        -DThrift_ROOT="${TP_INSTALL_DIR}" \
        -Dthrift_SOURCE=SYSTEM \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    "${CMAKE_CMD}" --build . -j "${PARALLEL}"
    "${CMAKE_CMD}" --install .

    # Hoist Arrow's bundled zstd — zstd is not in Darwin's package-manifest.sh,
    # so this is TP's only source (matches Linux build-thirdparty.sh).
    if [ -f ./zstd_ep-install/lib64/libzstd.a ]; then
        cp -rf ./zstd_ep-install/lib64/libzstd.a "${TP_INSTALL_DIR}/lib/libzstd.a"
    else
        cp -rf ./zstd_ep-install/lib/libzstd.a "${TP_INSTALL_DIR}/lib/libzstd.a"
    fi
    mkdir -p "${TP_INSTALL_DIR}/include/zstd"
    cp ./zstd_ep-install/include/* "${TP_INSTALL_DIR}/include/zstd"

    restore_env_var PKG_CONFIG_PATH "${old_pkg_config_path}"

    sync_lib64_links
}

build_formula_librdkafka() {
    ensure_formula librdkafka
    local prefix
    prefix="$(formula_prefix librdkafka)"
    link_if_missing "${prefix}/include/librdkafka" "${TP_INCLUDE_DIR}/librdkafka"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/librdkafka.a" "${prefix}/lib/librdkafka++.a" "${prefix}/lib/librdkafka"*.dylib "${prefix}/lib/librdkafka++"*.dylib
    link_formula_metadata "${prefix}"
    sync_lib64_links
}

build_formula_pulsar() {
    ensure_formula libpulsar
    local prefix
    prefix="$(formula_prefix libpulsar)"
    link_if_missing "${prefix}/include/pulsar" "${TP_INCLUDE_DIR}/pulsar"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/libpulsar.a" "${prefix}/lib/libpulsar"*.dylib
    link_formula_metadata "${prefix}"
    sync_lib64_links
}

build_formula_fmt_shared() {
    ensure_formula fmt
    local prefix
    prefix="$(formula_prefix fmt)"
    link_children_if_missing "${prefix}/include/fmt" "${TP_INCLUDE_DIR}/fmt"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/libfmt"*.dylib
    link_formula_metadata "${prefix}"
    sync_lib64_links
}

build_formula_aws_cpp_sdk() {
    ensure_formula aws-sdk-cpp
    ensure_formula aws-crt-cpp
    local prefix
    local crt_prefix
    prefix="$(formula_prefix aws-sdk-cpp)"
    crt_prefix="$(formula_prefix aws-crt-cpp)"
    if [[ -L "${TP_INCLUDE_DIR}/aws" ]]; then
        rm -f "${TP_INCLUDE_DIR}/aws"
    fi
    mkdir -p "${TP_INCLUDE_DIR}/aws"
    link_children_if_missing "${prefix}/include/aws" "${TP_INCLUDE_DIR}/aws"
    link_children_if_missing "${crt_prefix}/include/aws" "${TP_INCLUDE_DIR}/aws"
    link_children_if_missing "${HOMEBREW_PREFIX}/include/aws" "${TP_INCLUDE_DIR}/aws"
    if [[ -L "${TP_INCLUDE_DIR}/smithy" ]]; then
        rm -f "${TP_INCLUDE_DIR}/smithy"
    fi
    mkdir -p "${TP_INCLUDE_DIR}/smithy"
    link_children_if_missing "${prefix}/include/smithy" "${TP_INCLUDE_DIR}/smithy"
    link_children_if_missing "${HOMEBREW_PREFIX}/include/smithy" "${TP_INCLUDE_DIR}/smithy"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/libaws"*.a "${prefix}/lib/libaws"*.dylib
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${crt_prefix}/lib/libaws-crt-cpp"*.a "${crt_prefix}/lib/libaws-crt-cpp"*.dylib
    link_formula_metadata "${prefix}"
    link_formula_metadata "${crt_prefix}"
    sync_lib64_links
}

build_formula_opentelemetry() {
    ensure_formula opentelemetry-cpp
    local prefix
    prefix="$(formula_prefix opentelemetry-cpp)"
    link_children_if_missing "${prefix}/include" "${TP_INCLUDE_DIR}"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/libopentelemetry"*.a "${prefix}/lib/libopentelemetry"*.dylib
    link_formula_metadata "${prefix}"
    sync_lib64_links
}

build_formula_fast_float() {
    ensure_formula fast_float
    local prefix
    prefix="$(formula_prefix fast_float)"
    link_if_missing "${prefix}/include/fast_float" "${TP_INCLUDE_DIR}/fast_float"
}

build_formula_streamvbyte() {
    ensure_formula streamvbyte
    local prefix
    prefix="$(formula_prefix streamvbyte)"
    link_children_if_missing "${prefix}/include" "${TP_INCLUDE_DIR}"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/libstreamvbyte.a" "${prefix}/lib/libstreamvbyte"*.dylib
    link_if_missing "${prefix}/lib/libstreamvbyte.a" "${TP_INSTALL_DIR}/lib/libstreamvbyte_static.a"
    sync_lib64_links
}

build_formula_jansson() {
    ensure_formula jansson
    local prefix
    prefix="$(formula_prefix jansson)"
    link_if_missing "${prefix}/include/jansson.h" "${TP_INCLUDE_DIR}/jansson.h"
    link_if_missing "${prefix}/include/jansson_config.h" "${TP_INCLUDE_DIR}/jansson_config.h"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/libjansson.a" "${prefix}/lib/libjansson"*.dylib
    link_formula_metadata "${prefix}"
    sync_lib64_links
}

clean_poco_install_artifacts() {
    rm -rf "${TP_INCLUDE_DIR}/Poco" \
        "${TP_INSTALL_DIR}/lib/cmake/Poco" \
        "${TP_INSTALL_DIR}/share/cmake/Poco"
    safe_remove_glob "${TP_INSTALL_DIR}/lib/libPoco"* "${TP_INSTALL_DIR}/lib64/libPoco"*
}

remove_stale_homebrew_poco_symlinks() {
    find "${TP_INSTALL_DIR}/lib" "${TP_INSTALL_DIR}/lib64" \
        -maxdepth 1 \
        -type l \
        -name 'libPoco*' \
        -lname "${HOMEBREW_PREFIX%/}/*" \
        -exec rm -f {} +
}

build_poco() {
    remove_stale_homebrew_poco_symlinks
    if [[ -f "${TP_INSTALL_DIR}/lib/libPocoNet.a" &&
          -f "${TP_INSTALL_DIR}/lib/libPocoNetSSL.a" &&
          -f "${TP_INCLUDE_DIR}/Poco/Net/HTTPResponse.h" ]]; then
        return 0
    fi

    check_if_source_exist "${POCO_SOURCE}"
    cd "${TP_SOURCE_DIR}/${POCO_SOURCE}"
    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}"
    clean_poco_install_artifacts
    cd "${BUILD_DIR}"

    "${CMAKE_CMD}" .. \
        -G "${CMAKE_GENERATOR}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DPOCO_UNBUNDLED=ON \
        -DOPENSSL_ROOT_DIR="${TP_INSTALL_DIR}" \
        -DZLIB_ROOT="${TP_INSTALL_DIR}" \
        -DPCRE2_ROOT_DIR="${HOMEBREW_PREFIX}/opt/pcre2" \
        -DENABLE_XML=OFF \
        -DENABLE_JSON=OFF \
        -DENABLE_NET=ON \
        -DENABLE_NETSSL=ON \
        -DENABLE_CRYPTO=OFF \
        -DENABLE_JWT=OFF \
        -DENABLE_DATA=OFF \
        -DENABLE_DATA_SQLITE=OFF \
        -DENABLE_DATA_MYSQL=OFF \
        -DENABLE_DATA_POSTGRESQL=OFF \
        -DENABLE_DATA_ODBC=OFF \
        -DENABLE_MONGODB=OFF \
        -DENABLE_REDIS=OFF \
        -DENABLE_UTIL=OFF \
        -DENABLE_ZIP=OFF \
        -DENABLE_APACHECONNECTOR=OFF \
        -DENABLE_ENCODINGS=OFF \
        -DENABLE_PAGECOMPILER=OFF \
        -DENABLE_PAGECOMPILER_FILE2PAGE=OFF \
        -DENABLE_ACTIVERECORD=OFF \
        -DENABLE_ACTIVERECORD_COMPILER=OFF \
        -DENABLE_PROMETHEUS=OFF \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    "${CMAKE_CMD}" --build . -j "${PARALLEL}"
    "${CMAKE_CMD}" --install .
    sync_lib64_links
}

build_formula_xsimd() {
    ensure_formula xsimd
    local prefix
    prefix="$(formula_prefix xsimd)"
    link_if_missing "${prefix}/include/xsimd" "${TP_INCLUDE_DIR}/xsimd"
    link_formula_metadata "${prefix}"
}

build_formula_libxml2() {
    ensure_formula libxml2
    local prefix
    prefix="$(formula_prefix libxml2)"
    link_if_missing "${prefix}/include/libxml2" "${TP_INCLUDE_DIR}/libxml2"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/libxml2.a" "${prefix}/lib/libxml2"*.dylib
    link_formula_metadata "${prefix}"
    sync_lib64_links
}

build_formula_azure() {
    check_if_source_exist "${AZURE_SOURCE}"
    cd "${TP_SOURCE_DIR}/${AZURE_SOURCE}"

    export AZURE_SDK_DISABLE_AUTO_VCPKG=true
    export PKG_CONFIG_LIBDIR="${TP_INSTALL_DIR}/lib/pkgconfig:${TP_INSTALL_DIR}/lib64/pkgconfig"

    "${CMAKE_CMD}" -G "${CMAKE_GENERATOR}" \
        -S . -B build \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_PREFIX_PATH="${TP_INSTALL_DIR}" \
        -DBUILD_SHARED_LIBS=OFF \
        -DBUILD_TESTING=OFF \
        -DBUILD_SAMPLES=OFF \
        -DDISABLE_AZURE_CORE_OPENTELEMETRY=ON \
        -DWARNINGS_AS_ERRORS=OFF \
        -DCURL_INCLUDE_DIR="${TP_INSTALL_DIR}/include" \
        -DCURL_LIBRARY="${TP_INSTALL_DIR}/lib/libcurl.a" \
        -DOPENSSL_ROOT_DIR="${TP_INSTALL_DIR}" \
        -DOPENSSL_USE_STATIC_LIBS=TRUE \
        -DLibXml2_ROOT="${TP_INSTALL_DIR}" \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    "${CMAKE_CMD}" --build build -j "${PARALLEL}"
    "${CMAKE_CMD}" --install build

    unset AZURE_SDK_DISABLE_AUTO_VCPKG
    unset PKG_CONFIG_LIBDIR
    sync_lib64_links
}

build_formula_xxhash() {
    ensure_formula xxhash
    local prefix
    prefix="$(formula_prefix xxhash)"
    link_if_missing "${prefix}/include/xxhash.h" "${TP_INCLUDE_DIR}/xxhash.h"
    link_matching_if_missing "${TP_INSTALL_DIR}/lib" "${prefix}/lib/libxxhash.a" "${prefix}/lib/libxxhash"*.dylib
    link_formula_metadata "${prefix}"
    sync_lib64_links
}

build_formula_ragel() {
    ensure_formula ragel
    local prefix
    prefix="$(formula_prefix ragel)"
    link_matching_if_missing "${TP_INSTALL_DIR}/bin" "${prefix}/bin/ragel"
}

build_formula_llvm() {
    ensure_formula llvm
    local prefix
    prefix="$(formula_prefix llvm)"
    link_if_missing "${prefix}" "${TP_INSTALL_DIR}/llvm"
}

build_re2() {
    check_if_source_exist "${RE2_SOURCE}"
    cd "${TP_SOURCE_DIR}/${RE2_SOURCE}"
    rm -rf CMakeCache.txt CMakeFiles/ build.ninja
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"
    rm -rf CMakeCache.txt CMakeFiles/
    "${CMAKE_CMD}" -S .. -B . -G "${CMAKE_GENERATOR}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_SHARED_LIBS=0 \
        -DRE2_BUILD_TESTING=OFF \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    "${BUILD_SYSTEM}" -j"${PARALLEL}" install
    sync_lib64_links
}

build_s2() {
    check_if_source_exist "${S2_SOURCE}"
    cd "${TP_SOURCE_DIR}/${S2_SOURCE}"
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"
    rm -rf CMakeCache.txt CMakeFiles/
    "${CMAKE_CMD}" -G "${CMAKE_GENERATOR}" \
        -DBUILD_SHARED_LIBS=OFF \
        -DBUILD_EXAMPLES=OFF \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INCLUDE_PATH="${TP_INSTALL_DIR}/include" \
        -DCMAKE_CXX_STANDARD=17 \
        -DGFLAGS_ROOT_DIR="${TP_INSTALL_DIR}/include" \
        -DWITH_GFLAGS=ON \
        -DGLOG_ROOT_DIR="${TP_INSTALL_DIR}/include" \
        -DWITH_GLOG=ON \
        -DCMAKE_LIBRARY_PATH="${TP_INSTALL_DIR}/lib;${TP_INSTALL_DIR}/lib64" \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5 ..
    "${BUILD_SYSTEM}" -j"${PARALLEL}"
    "${BUILD_SYSTEM}" install
    sync_lib64_links
}

build_hadoop_src() {
    local libhdfs_dir
    local jni_platform_include

    ensure_java_home
    check_if_source_exist "${HADOOPSRC_SOURCE}"
    libhdfs_dir="${TP_SOURCE_DIR}/${HADOOPSRC_SOURCE}/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs"
    jni_platform_include="$(detect_java_jni_platform_include "${JAVA_HOME}")"
    ensure_hadoop_libhdfs_makefile_uses_platform_include "${libhdfs_dir}/Makefile"

    cd "${libhdfs_dir}"
    make clean >/dev/null 2>&1 || true
    make JNI_PLATFORM_INCLUDE="${jni_platform_include}"
    mkdir -p "${TP_INSTALL_DIR}/include/hdfs"
    cp "${TP_SOURCE_DIR}/${HADOOPSRC_SOURCE}/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/include/hdfs/hdfs.h" "${TP_INSTALL_DIR}/include/hdfs/"
    cp "${TP_SOURCE_DIR}/${HADOOPSRC_SOURCE}/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/libhdfs.a" "${TP_INSTALL_DIR}/lib/"
    sync_lib64_links
}

build_jdk() {
    check_if_source_exist "${JDK_SOURCE}"
    rm -rf "${TP_INSTALL_DIR}/open_jdk"
    cp -R "${TP_SOURCE_DIR}/${JDK_SOURCE}" "${TP_INSTALL_DIR}/open_jdk"
}

build_mariadb() {
    local old_generator="${CMAKE_GENERATOR}"
    local old_system="${BUILD_SYSTEM}"

    check_if_source_exist "${MARIADB_SOURCE}"
    cd "${TP_SOURCE_DIR}/${MARIADB_SOURCE}"
    mkdir -p build
    cd build
    rm -rf CMakeCache.txt CMakeFiles

    export CMAKE_GENERATOR="Unix Makefiles"
    export BUILD_SYSTEM="make"
    "${CMAKE_CMD}" .. -G "${CMAKE_GENERATOR}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DWITH_UNIT_TESTS=OFF \
        -DWITH_EXTERNAL_ZLIB=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DOPENSSL_ROOT_DIR="${TP_INSTALL_DIR}" \
        -DOPENSSL_USE_STATIC_LIBS=TRUE \
        -DZLIB_INCLUDE_DIR="${TP_INSTALL_DIR}/include" \
        -DZLIB_LIBRARY="${TP_INSTALL_DIR}/lib/libz.a" \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    ${BUILD_SYSTEM} -j"${PARALLEL}" mariadbclient
    mkdir -p "${TP_INSTALL_DIR}/lib/mariadb"
    cp libmariadb/libmariadbclient.a "${TP_INSTALL_DIR}/lib/mariadb/"
    cd include
    ${BUILD_SYSTEM} install

    export CMAKE_GENERATOR="${old_generator}"
    export BUILD_SYSTEM="${old_system}"
}

build_benchmark() {
    check_if_source_exist "${BENCHMARK_SOURCE}"
    cd "${TP_SOURCE_DIR}/${BENCHMARK_SOURCE}"
    if grep -q 'add_cxx_compiler_flag(-Wthread-safety)' CMakeLists.txt; then
        perl -0pi -e 's/\n(\s*)add_cxx_compiler_flag\(-Wthread-safety\)\n/\n$1# Disabled on macOS: Clang thread-safety analysis rejects benchmark mutex wrappers.\n/' CMakeLists.txt
    fi
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"
    rm -rf CMakeCache.txt CMakeFiles/
    "${CMAKE_CMD}" .. \
        -DBENCHMARK_DOWNLOAD_DEPENDENCIES=OFF \
        -DBENCHMARK_ENABLE_TESTING=OFF \
        -DBENCHMARK_ENABLE_GTEST_TESTS=OFF \
        -DBENCHMARK_INSTALL_DOCS=OFF \
        -DBENCHMARK_INSTALL_TOOLS=OFF \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib64 \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    "${BUILD_SYSTEM}" -j"${PARALLEL}"
    "${BUILD_SYSTEM}" install
    sync_lib64_links
}

build_jemalloc() {
    check_if_source_exist "${JEMALLOC_SOURCE}"
    cd "${TP_SOURCE_DIR}/${JEMALLOC_SOURCE}"
    local addition_opts=" --with-lg-page=16"
    CFLAGS="-O3 -fno-omit-frame-pointer -fPIC -g" \
        ./configure --prefix="${TP_INSTALL_DIR}/jemalloc" --with-jemalloc-prefix=je --enable-prof --disable-cxx --disable-libdl ${addition_opts}
    make -j"${PARALLEL}"
    make install
    mkdir -p "${TP_INSTALL_DIR}/jemalloc/lib-shared" "${TP_INSTALL_DIR}/jemalloc/lib-static"
    move_matching_files "${TP_INSTALL_DIR}/jemalloc/lib-shared" "${TP_INSTALL_DIR}/jemalloc/lib/"*.dylib* "${TP_INSTALL_DIR}/jemalloc/lib/"*.so*
    move_matching_files "${TP_INSTALL_DIR}/jemalloc/lib-static" "${TP_INSTALL_DIR}/jemalloc/lib/"*.a
    if [[ -f "${TP_INSTALL_DIR}/jemalloc/lib-shared/libjemalloc.2.dylib" ]]; then
        ln -sfn ../lib-shared/libjemalloc.2.dylib "${TP_INSTALL_DIR}/jemalloc/lib/libjemalloc.2.dylib"
        ln -sfn libjemalloc.2.dylib "${TP_INSTALL_DIR}/jemalloc/lib/libjemalloc.dylib"
    fi

    CFLAGS="-O3 -fno-omit-frame-pointer -fPIC -g" \
        ./configure --prefix="${TP_INSTALL_DIR}/jemalloc-debug" --with-jemalloc-prefix=je --enable-prof --disable-static --enable-debug --enable-fill --disable-cxx --disable-libdl ${addition_opts}
    make -j"${PARALLEL}"
    make install
}

build_avro_c() {
    check_if_source_exist "${AVRO_SOURCE}"
    cd "${TP_SOURCE_DIR}/${AVRO_SOURCE}/lang/c"
    mkdir -p build
    cd build
    "${CMAKE_CMD}" .. \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib64 \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    "${BUILD_SYSTEM}" -j"${PARALLEL}"
    "${BUILD_SYSTEM}" install
    safe_remove_glob "${TP_INSTALL_DIR}/lib64/libavro.so*" "${TP_INSTALL_DIR}/lib64/libavro"*.dylib*
}

build_avro_cpp() {
    check_if_source_exist "${AVRO_SOURCE}"
    cd "${TP_SOURCE_DIR}/${AVRO_SOURCE}/lang/c++"
    mkdir -p build
    cd build
    rm -rf CMakeCache.txt CMakeFiles/
    "${CMAKE_CMD}" .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DBOOST_ROOT="${TP_INSTALL_DIR}" \
        -DBoost_USE_STATIC_RUNTIME=ON \
        -DCMAKE_PREFIX_PATH="${TP_INSTALL_DIR}" \
        -DSNAPPY_INCLUDE_DIR="${TP_INSTALL_DIR}/include" \
        -DSNAPPY_LIBRARIES="${TP_INSTALL_DIR}/lib" \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    LIBRARY_PATH="${TP_INSTALL_DIR}/lib64:${LIBRARY_PATH:-}" "${BUILD_SYSTEM}" -j"${PARALLEL}" avrocpp_s
    cp libavrocpp_s.a "${TP_INSTALL_DIR}/lib64/"
    rm -rf "${TP_INSTALL_DIR}/include/avrocpp"
    cp -R ../include/avro "${TP_INSTALL_DIR}/include/avrocpp"
}

build_serdes() {
    check_if_source_exist "${SERDES_SOURCE}"
    cd "${TP_SOURCE_DIR}/${SERDES_SOURCE}"
    export LIBS="-lpthread -lcurl -ljansson -lrdkafka -lrdkafka++ -lavro -lssl -lcrypto"
    ./configure \
        --prefix="${TP_INSTALL_DIR}" \
        --libdir="${TP_INSTALL_DIR}/lib" \
        --CFLAGS="-I ${TP_INSTALL_DIR}/include" \
        --CXXFLAGS="-I ${TP_INSTALL_DIR}/include" \
        --LDFLAGS="-L ${TP_INSTALL_DIR}/lib -L ${TP_INSTALL_DIR}/lib64" \
        --enable-static \
        --disable-shared
    make -C src -j"${PARALLEL}" libserdes.a
    mkdir -p "${TP_INSTALL_DIR}/lib" "${TP_INSTALL_DIR}/include/libserdes"
    cp src/libserdes.a "${TP_INSTALL_DIR}/lib/"
    cp src/serdes.h src/serdes-common.h src/serdes-avro.h "${TP_INSTALL_DIR}/include/libserdes/"
    "${OBJCOPY}" --localize-symbol=cnd_timedwait "${TP_INSTALL_DIR}/lib/libserdes.a"
    "${OBJCOPY}" --localize-symbol=cnd_timedwait_ms "${TP_INSTALL_DIR}/lib/libserdes.a"
    "${OBJCOPY}" --localize-symbol=thrd_is_current "${TP_INSTALL_DIR}/lib/libserdes.a"
    unset LIBS
    restore_compile_flags
}

build_fiu() {
    check_if_source_exist "${FIU_SOURCE}"
    cd "${TP_SOURCE_DIR}/${FIU_SOURCE}"
    make -C libfiu -j"${PARALLEL}" libfiu.a
    mkdir -p "${TP_INSTALL_DIR}/include/fiu"
    cp libfiu/fiu.h libfiu/fiu-control.h libfiu/fiu-local.h "${TP_INSTALL_DIR}/include/fiu/"
    cp libfiu/libfiu.a "${TP_INSTALL_DIR}/lib/"
    sync_lib64_links
}

build_clucene() {
    check_if_source_exist "${CLUCENE_SOURCE}"
    cd "${TP_SOURCE_DIR}/${CLUCENE_SOURCE}"
    if grep -q -- '-std=c++17' CMakeLists.txt; then
        perl -0pi -e 's/-std=c\+\+17/-std=c++14 -D_LIBCPP_ENABLE_CXX17_REMOVED_UNARY_BINARY_FUNCTION -Dstat64=stat -Dfstat64=fstat/' CMakeLists.txt
    fi
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"
    rm -rf CMakeCache.txt CMakeFiles/
    "${CMAKE_CMD}" -G "${CMAKE_GENERATOR}" \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DCMAKE_INSTALL_LIBDIR=lib64 \
        -DBUILD_STATIC_LIBRARIES=ON \
        -DBUILD_SHARED_LIBRARIES=OFF \
        -DENABLE_TESTS=OFF \
        -DENABLE_COMPILE_TESTS=OFF \
        -DBOOST_ROOT="${TP_INSTALL_DIR}" \
        -DZLIB_ROOT="${TP_INSTALL_DIR}" \
        -DCMAKE_CXX_FLAGS="-g -fno-omit-frame-pointer -Wno-narrowing ${FILE_PREFIX_MAP_OPTION}" \
        -DUSE_STAT64=0 \
        -DCMAKE_BUILD_TYPE=Release \
        -DUSE_AVX2=OFF \
        -DBUILD_CONTRIBS_LIB=ON \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5 ..
    "${BUILD_SYSTEM}" -j"${PARALLEL}"
    "${BUILD_SYSTEM}" install
}

build_flamegraph() {
    check_if_source_exist "${FLAMEGRAPH_SOURCE}"
    mkdir -p "${TP_INSTALL_DIR}/flamegraph"
    cp -R "${TP_SOURCE_DIR}/${FLAMEGRAPH_SOURCE}/stackcollapse-perf.pl" "${TP_INSTALL_DIR}/flamegraph/"
    cp -R "${TP_SOURCE_DIR}/${FLAMEGRAPH_SOURCE}/stackcollapse-go.pl" "${TP_INSTALL_DIR}/flamegraph/"
    cp -R "${TP_SOURCE_DIR}/${FLAMEGRAPH_SOURCE}/flamegraph.pl" "${TP_INSTALL_DIR}/flamegraph/"
    chmod +x "${TP_INSTALL_DIR}/flamegraph/"*.pl
}

build_benchgen() {
    check_if_source_exist "${BENCHGEN_SOURCE}"
    cd "${TP_SOURCE_DIR}/${BENCHGEN_SOURCE}"
    "${CMAKE_CMD}" -G "${CMAKE_GENERATOR}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DBENCHGEN_ARROW_PREFIX="${TP_INSTALL_DIR}" \
        -S . -B build
    "${CMAKE_CMD}" --build build -j "${PARALLEL}"
    "${CMAKE_CMD}" --install build
}

build_aliyun_jindosdk() {
    check_if_source_exist "${JINDOSDK_SOURCE}"
    mkdir -p "${TP_INSTALL_DIR}/jindosdk"
    cp -R "${TP_SOURCE_DIR}/${JINDOSDK_SOURCE}/lib/"*.jar "${TP_INSTALL_DIR}/jindosdk/"
}

build_gcs_connector() {
    check_if_source_exist "${GCS_CONNECTOR_SOURCE}"
    mkdir -p "${TP_INSTALL_DIR}/gcs_connector"
    cp -R "${TP_SOURCE_DIR}/${GCS_CONNECTOR_SOURCE}/"*.jar "${TP_INSTALL_DIR}/gcs_connector/"
}

PARALLEL="${PARALLEL:-$default_parallel}"
HELP=0
CLEAN=0
CONTINUE=0
start_package=""
packages=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -j)
            PARALLEL="$2"
            shift 2
            ;;
        -j*)
            PARALLEL="${1#-j}"
            shift
            ;;
        -h|--help)
            HELP=1
            shift
            ;;
        --clean)
            CLEAN=1
            shift
            ;;
        --continue)
            CONTINUE=1
            start_package="$2"
            shift 2
            ;;
        --)
            shift
            while [[ $# -gt 0 ]]; do
                packages+=("$1")
                shift
            done
            ;;
        *)
            packages+=("$1")
            shift
            ;;
    esac
done

if [[ "${HELP}" -eq 1 ]]; then
    usage
    exit 0
fi

if [[ "$(uname -s)" != "Darwin" ]]; then
    echo "This script only supports Darwin"
    exit 1
fi
if [[ "${MACHINE_TYPE}" != "aarch64" ]]; then
    echo "This script only supports macOS arm64"
    exit 1
fi
if ! command -v brew >/dev/null 2>&1; then
    echo "Homebrew is required on macOS"
    exit 1
fi
if [[ "${CONTINUE}" -eq 1 ]] && ([[ -z "${start_package}" ]] || [[ "${#packages[@]}" -ne 0 ]]); then
    usage
    exit 1
fi
if [[ "${CONTINUE}" -eq 1 ]]; then
    validate_requested_package "${start_package}"
fi

if [[ "${#packages[@]}" -eq 0 ]]; then
    packages=("${STARROCKS_THIRDPARTY_ALL_PACKAGES[@]}")
else
    for package in "${packages[@]}"; do
        validate_requested_package "${package}"
    done
fi

setup_build_environment
ensure_java_home

if [[ "${CLEAN}" -eq 1 ]]; then
    clean_sources
    rm -rf "${TP_INSTALL_DIR}" "${TP_DIR}/build"
fi
mkdir -p "${TP_INSTALL_DIR}/lib" "${TP_INSTALL_DIR}/lib64" "${TP_INSTALL_DIR}/include" "${TP_INSTALL_DIR}/bin"

"${TP_DIR}/download-thirdparty.sh"

PACKAGE_FOUND=0
for package in "${packages[@]}"; do
    if [[ "${package}" == "${start_package}" ]]; then
        PACKAGE_FOUND=1
    fi
    if [[ "${CONTINUE}" -eq 1 ]] && [[ "${PACKAGE_FOUND}" -eq 0 ]]; then
        continue
    fi

    case "${package}" in
        boost)
            build_boost
            ;;
        protobuf)
            build_protobuf
            ;;
        gflags)
            build_gflags
            ;;
        glog)
            build_glog
            ;;
        thrift)
            build_thrift
            ;;
        leveldb)
            build_leveldb
            ;;
        brpc)
            build_brpc
            ;;
        rocksdb)
            build_rocksdb
            ;;
        bitshuffle)
            build_bitshuffle
            ;;
        croaringbitmap)
            build_croaringbitmap
            ;;
        cctz)
            build_cctz
            ;;
        fmt)
            build_fmt
            ;;
        ryu)
            build_ryu
            ;;
        datasketches)
            build_datasketches
            ;;
        simdutf)
            build_simdutf
            ;;
        curl)
            build_curl
            ;;
        icu)
            build_icu
            ;;
        libdivide)
            build_libdivide
            ;;
        vpack)
            build_vpack
            ;;
        hyperscan)
            build_hyperscan
            ;;
        libevent)
            build_formula_libevent
            ;;
        zlib)
            build_formula_zlib
            ;;
        zstd)
            build_formula_zstd
            ;;
        lz4)
            build_formula_lz4
            ;;
        lzo2)
            build_formula_lzo2
            ;;
        bzip)
            build_formula_bzip
            ;;
        openssl)
            build_formula_openssl
            ;;
        gtest)
            build_formula_gtest
            ;;
        rapidjson)
            build_formula_rapidjson
            ;;
        simdjson)
            build_formula_simdjson
            ;;
        snappy)
            build_formula_snappy
            ;;
        gperftools)
            build_formula_gperftools
            ;;
        re2)
            build_re2
            ;;
        kerberos)
            build_formula_kerberos
            ;;
        sasl)
            build_formula_sasl
            ;;
        absl)
            build_absl
            ;;
        grpc)
            build_grpc
            ;;
        flatbuffers)
            build_formula_flatbuffers
            ;;
        jemalloc)
            build_jemalloc
            ;;
        brotli)
            build_formula_brotli
            ;;
        arrow)
            build_arrow
            ;;
        librdkafka)
            build_formula_librdkafka
            ;;
        pulsar)
            build_formula_pulsar
            ;;
        s2)
            build_s2
            ;;
        fmt_shared)
            build_formula_fmt_shared
            ;;
        hadoop_src)
            build_hadoop_src
            ;;
        jdk)
            build_jdk
            ;;
        ragel)
            build_formula_ragel
            ;;
        mariadb)
            build_mariadb
            ;;
        aliyun_jindosdk)
            build_aliyun_jindosdk
            ;;
        gcs_connector)
            build_gcs_connector
            ;;
        aws_cpp_sdk)
            build_formula_aws_cpp_sdk
            ;;
        opentelemetry)
            build_formula_opentelemetry
            ;;
        benchmark)
            build_benchmark
            ;;
        fast_float)
            build_formula_fast_float
            ;;
        clucene)
            build_clucene
            ;;
        starcache|tenann|pprof|breakpad)
            unsupported_package "${package}"
            exit 1
            ;;
        streamvbyte)
            build_formula_streamvbyte
            ;;
        jansson)
            build_formula_jansson
            ;;
        avro_c)
            build_avro_c
            ;;
        avro_cpp)
            build_avro_cpp
            ;;
        serdes)
            build_serdes
            ;;
        fiu)
            build_fiu
            ;;
        llvm)
            build_formula_llvm
            ;;
        poco)
            build_poco
            ;;
        xsimd)
            build_formula_xsimd
            ;;
        libxml2)
            build_formula_libxml2
            ;;
        azure)
            build_formula_azure
            ;;
        flamegraph)
            build_flamegraph
            ;;
        xxhash)
            build_formula_xxhash
            ;;
        benchgen)
            build_benchgen
            ;;
        libdeflate)
            ensure_formula libdeflate
            prefix="$(formula_prefix libdeflate)"
            link_if_missing "${prefix}/include/libdeflate.h" "${TP_INCLUDE_DIR}/libdeflate.h"
            link_matching_if_missing "${TP_INSTALL_DIR}/lib64" "${prefix}/lib/libdeflate.a" "${prefix}/lib/libdeflate"*.dylib
            ;;
        *)
            echo "Unknown package: ${package}"
            exit 1
            ;;
    esac
done

sync_lib64_links

echo "Finished to build all thirdparties on Darwin"
