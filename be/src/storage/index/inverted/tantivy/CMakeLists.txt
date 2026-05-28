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

# tantivy FFI binding — drives `cargo build` on the in-tree binding crate
# and exposes the resulting static library as the IMPORTED CMake target
# `tantivy_binding` so the rest of BE can link against it.
#
# CI image (olap-devops/cicd) bakes the Rust toolchain (rustup + 1.88.0)
# and the cargo dependency cache (/var/local/thirdparty/installed/cargo-registry,
# symlinked to /root/.cargo/registry) into the image. So `cargo build` here
# never needs to touch the network.
#
# Build behavior (driven by Ninja/Make mtime comparison on OUTPUT vs DEPENDS):
#   - First BE build: libtantivy_binding.a does not exist, cargo runs.
#     Output lands in tantivy-binding/target/release/ (cargo's default dir,
#     which is outside be/build_${TYPE}/ and therefore survives a regular
#     `rm -rf be/build_*`).
#   - Rebuild with no Rust source change: .a is newer than every input,
#     Ninja skips the cargo command entirely. Zero overhead.
#   - Rebuild after a Rust source change: Ninja sees the source is newer
#     than .a and re-runs cargo. Cargo's own incremental compilation only
#     rebuilds the changed crate(s).
#   - `./build.sh --clean --be`: build.sh additionally deletes
#     tantivy-binding/target/, forcing a full cargo rebuild next time.

set(TANTIVY_BINDING_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tantivy-binding)
# Persistent cargo target dir (gitignored). Must be outside be/build_*.
set(TANTIVY_CARGO_TARGET_DIR ${TANTIVY_BINDING_DIR}/target)
set(TANTIVY_BINDING_LIB ${TANTIVY_CARGO_TARGET_DIR}/release/libtantivy_binding.a)
set(TANTIVY_BINDING_HEADER ${TANTIVY_BINDING_DIR}/include/tantivy_binding.h)

# Track every Rust source / build input so the build system re-runs cargo
# whenever any of them change. CONFIGURE_DEPENDS makes the glob re-evaluate
# at build time so newly added .rs files don't require a manual cmake
# re-run. Cargo handles its own incremental compilation on top of this.
file(GLOB_RECURSE TANTIVY_BINDING_SOURCES
    CONFIGURE_DEPENDS
    ${TANTIVY_BINDING_DIR}/Cargo.toml
    ${TANTIVY_BINDING_DIR}/Cargo.lock
    ${TANTIVY_BINDING_DIR}/build.rs
    ${TANTIVY_BINDING_DIR}/cbindgen.toml
    ${TANTIVY_BINDING_DIR}/rust-toolchain.toml
    ${TANTIVY_BINDING_DIR}/src/*.rs
    ${TANTIVY_BINDING_DIR}/src/**/*.rs)

# Single rule: cargo runs iff any DEPENDS file is newer than the OUTPUT .a.
# Ninja/Make's mtime check supplies the "skip when nothing changed" behavior;
# we no longer need a configure-time EXISTS short-circuit (which had the bug
# of skipping cargo even when sources had changed).
add_custom_command(
    OUTPUT ${TANTIVY_BINDING_LIB} ${TANTIVY_BINDING_HEADER}
    COMMAND cargo build --release --frozen --manifest-path ${TANTIVY_BINDING_DIR}/Cargo.toml
    DEPENDS ${TANTIVY_BINDING_SOURCES}
    WORKING_DIRECTORY ${TANTIVY_BINDING_DIR}
    COMMENT "Building tantivy-binding (cargo, release profile)"
    VERBATIM)

add_custom_target(build_tantivy_binding ALL
    DEPENDS ${TANTIVY_BINDING_LIB} ${TANTIVY_BINDING_HEADER})

# Pre-create the include/ directory so CMake's IMPORTED target validation at
# configure time does not fail when the cbindgen-generated header has not
# been produced yet (first build, or after a clean). The actual header is
# materialized by cargo at build time; we only need the directory to exist
# for CMake's INTERFACE_INCLUDE_DIRECTORIES check.
file(MAKE_DIRECTORY ${TANTIVY_BINDING_DIR}/include)

# Expose the static library as an IMPORTED target so the rest of BE can simply
# `target_link_libraries(... tantivy_binding)`. GLOBAL is required because
# this target is referenced from the root be/CMakeLists.txt link group.
add_library(tantivy_binding STATIC IMPORTED GLOBAL)
add_dependencies(tantivy_binding build_tantivy_binding)
set_target_properties(tantivy_binding PROPERTIES
    IMPORTED_LOCATION ${TANTIVY_BINDING_LIB}
    INTERFACE_INCLUDE_DIRECTORIES ${TANTIVY_BINDING_DIR}/include)
