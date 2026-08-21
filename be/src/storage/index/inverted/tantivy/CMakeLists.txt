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
set(TANTIVY_IK_PATCH
    ${CMAKE_CURRENT_SOURCE_DIR}/patches/ik-rs-0.7.0-starrocks-home.patch)

# ik-rs 0.7 loads ik.yml and its dictionaries from CARGO_MANIFEST_DIR at
# runtime. That directory belongs to the build image and is not present after
# starrocks_be is copied to another host. Locate the locked crate in Cargo's
# registry, create an isolated patched copy, and make it resolve resources from
# $STARROCKS_HOME/lib/tantivy-packages instead. Never modify the shared Cargo
# cache. Prefer CARGO_HOME when the build environment provides it; otherwise
# derive Cargo's home from the cargo executable location.
find_program(TANTIVY_CARGO_EXECUTABLE cargo REQUIRED)
if (DEFINED ENV{CARGO_HOME} AND NOT "$ENV{CARGO_HOME}" STREQUAL "")
    set(TANTIVY_CARGO_HOME $ENV{CARGO_HOME})
else()
    get_filename_component(TANTIVY_CARGO_BIN_DIR ${TANTIVY_CARGO_EXECUTABLE} DIRECTORY)
    get_filename_component(TANTIVY_CARGO_HOME ${TANTIVY_CARGO_BIN_DIR} DIRECTORY)
endif()
file(GLOB TANTIVY_IK_SOURCE_CANDIDATES
    LIST_DIRECTORIES true
    ${TANTIVY_CARGO_HOME}/registry/src/*/ik-rs-0.7.0)
list(SORT TANTIVY_IK_SOURCE_CANDIDATES)
list(LENGTH TANTIVY_IK_SOURCE_CANDIDATES TANTIVY_IK_SOURCE_COUNT)
if (TANTIVY_IK_SOURCE_COUNT EQUAL 0)
    message(FATAL_ERROR
        "ik-rs 0.7.0 is missing from ${TANTIVY_CARGO_HOME}/registry/src. "
        "The build image must provide the locked Cargo dependency cache.")
endif()
list(GET TANTIVY_IK_SOURCE_CANDIDATES 0 TANTIVY_IK_SOURCE_DIR)
foreach (TANTIVY_IK_REQUIRED_PATH
         ik.yml
         dict/main2012.dic
         dict/quantifier.dic
         dict/stopword.dic
         dict/ext_dict/ext.dic
         dict/ext_stop_word/stop.dic
         LICENSE)
    if (NOT EXISTS ${TANTIVY_IK_SOURCE_DIR}/${TANTIVY_IK_REQUIRED_PATH})
        message(FATAL_ERROR
            "Required ik-rs runtime resource is missing: "
            "${TANTIVY_IK_SOURCE_DIR}/${TANTIVY_IK_REQUIRED_PATH}")
    endif()
endforeach()

set(TANTIVY_IK_PATCHED_DIR
    ${TANTIVY_CARGO_TARGET_DIR}/patched-deps/ik-rs-0.7.0)
file(REMOVE_RECURSE ${TANTIVY_IK_PATCHED_DIR})
file(MAKE_DIRECTORY ${TANTIVY_IK_PATCHED_DIR})
file(COPY ${TANTIVY_IK_SOURCE_DIR}/ DESTINATION ${TANTIVY_IK_PATCHED_DIR})
execute_process(
    COMMAND patch -p1 --input ${TANTIVY_IK_PATCH}
    WORKING_DIRECTORY ${TANTIVY_IK_PATCHED_DIR}
    RESULT_VARIABLE TANTIVY_IK_PATCH_RESULT
    OUTPUT_VARIABLE TANTIVY_IK_PATCH_OUTPUT
    ERROR_VARIABLE TANTIVY_IK_PATCH_ERROR)
if (NOT TANTIVY_IK_PATCH_RESULT EQUAL 0)
    message(FATAL_ERROR
        "Failed to patch ik-rs 0.7.0: ${TANTIVY_IK_PATCH_OUTPUT} "
        "${TANTIVY_IK_PATCH_ERROR}")
endif()
set(TANTIVY_IK_CARGO_PATCH_CONFIG
    "patch.crates-io.ik-rs.path=\"${TANTIVY_IK_PATCHED_DIR}\"")

# Stage the runtime resources in be/output as part of every BE/UT build. This
# target intentionally refreshes the directory on every build so an
# independently cleaned be/output never leaves a stale or incomplete bundle.
set(TANTIVY_IK_OUTPUT_DIR ${OUTPUT_DIR}/lib/tantivy-packages)
file(GLOB_RECURSE TANTIVY_IK_RESOURCE_FILES
    CONFIGURE_DEPENDS
    ${TANTIVY_IK_SOURCE_DIR}/ik.yml
    ${TANTIVY_IK_SOURCE_DIR}/dict/*
    ${TANTIVY_IK_SOURCE_DIR}/LICENSE)
add_custom_target(stage_tantivy_ik_resources ALL
    COMMAND ${CMAKE_COMMAND} -E rm -rf ${TANTIVY_IK_OUTPUT_DIR}/dict
    COMMAND ${CMAKE_COMMAND} -E make_directory ${TANTIVY_IK_OUTPUT_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${TANTIVY_IK_SOURCE_DIR}/ik.yml
            ${TANTIVY_IK_OUTPUT_DIR}/ik.yml
    COMMAND ${CMAKE_COMMAND} -E copy ${TANTIVY_IK_SOURCE_DIR}/LICENSE
            ${TANTIVY_IK_OUTPUT_DIR}/LICENSE
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${TANTIVY_IK_SOURCE_DIR}/dict
            ${TANTIVY_IK_OUTPUT_DIR}/dict
    DEPENDS ${TANTIVY_IK_RESOURCE_FILES}
    COMMENT "Staging Tantivy IK runtime resources"
    VERBATIM)

# Track every Rust source / build input so the build system re-runs cargo
# whenever any of them change. CONFIGURE_DEPENDS makes the glob re-evaluate
# at build time so newly added .rs files don't require a manual cmake
# re-run. Cargo handles its own incremental compilation on top of this.
file(GLOB_RECURSE TANTIVY_BINDING_SOURCES
    CONFIGURE_DEPENDS
    ${TANTIVY_BINDING_DIR}/.cargo/config.toml
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
    COMMAND cargo build --release --frozen
            --config ${TANTIVY_IK_CARGO_PATCH_CONFIG}
            --manifest-path ${TANTIVY_BINDING_DIR}/Cargo.toml
    DEPENDS ${TANTIVY_BINDING_SOURCES} ${TANTIVY_IK_PATCH}
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

install(FILES ${TANTIVY_IK_SOURCE_DIR}/ik.yml ${TANTIVY_IK_SOURCE_DIR}/LICENSE
        DESTINATION ${OUTPUT_DIR}/lib/tantivy-packages)
install(DIRECTORY ${TANTIVY_IK_SOURCE_DIR}/dict
        DESTINATION ${OUTPUT_DIR}/lib/tantivy-packages)
