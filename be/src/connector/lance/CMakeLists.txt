# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0.

find_package(Python3 REQUIRED COMPONENTS Interpreter)

set(LANCE_RUST_SOURCE "${CMAKE_CURRENT_SOURCE_DIR}/rust")
set(LANCE_RUST_TARGET "${CMAKE_BINARY_DIR}/lance-rust")
set(LANCE_RUST_LIBRARY "${LANCE_RUST_TARGET}/release/${CMAKE_SHARED_LIBRARY_PREFIX}starrocks_lance${CMAKE_SHARED_LIBRARY_SUFFIX}")
set(LANCE_RUST_TEST_ARGS)
if (MAKE_TEST)
    list(APPEND LANCE_RUST_TEST_ARGS --test)
endif()
add_custom_command(
    OUTPUT "${LANCE_RUST_TARGET}/build.stamp"
    BYPRODUCTS "${LANCE_RUST_LIBRARY}"
    COMMAND ${Python3_EXECUTABLE} "${STARROCKS_HOME}/build-support/build_lance.py"
            --source "${LANCE_RUST_SOURCE}" --target "${LANCE_RUST_TARGET}"
            --protoc "${THIRDPARTY_DIR}/bin/protoc" ${LANCE_RUST_TEST_ARGS}
    COMMAND ${CMAKE_COMMAND} -E touch "${LANCE_RUST_TARGET}/build.stamp"
    DEPENDS rust/Cargo.toml rust/Cargo.lock rust/src/lib.rs rust/src/storage.rs rust/src/tests.rs
            "${STARROCKS_HOME}/build-support/build_lance.py"
    COMMENT "Build Lance Rust SDK and Arrow C interface"
    VERBATIM)
add_custom_target(lance_rust_build ALL DEPENDS "${LANCE_RUST_TARGET}/build.stamp")
add_library(lance_rust SHARED IMPORTED GLOBAL)
set_target_properties(lance_rust PROPERTIES IMPORTED_LOCATION "${LANCE_RUST_LIBRARY}" IMPORTED_NO_SONAME TRUE)
add_dependencies(lance_rust lance_rust_build)
# build.sh installs this into be/lib even when Java extensions are disabled.
install(FILES "${LANCE_RUST_LIBRARY}" DESTINATION "${OUTPUT_DIR}/lib")

if (UNIX AND NOT APPLE)
    target_link_options(lance_rust INTERFACE "LINKER:-rpath,$ORIGIN")
endif()
