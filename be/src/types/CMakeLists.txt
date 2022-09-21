# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/types")

add_library(Types STATIC
    bitmap_value.cpp
    date_value.cpp
    hll.cpp
    timestamp_value.cpp
    ipv4_value.cpp
)
