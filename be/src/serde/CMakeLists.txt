# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/serde")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/serde")

add_library(Serde STATIC
        column_array_serde.cpp
        protobuf_serde.cpp
        )
