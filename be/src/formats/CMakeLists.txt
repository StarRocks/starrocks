# This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/converter")

add_library(Formats STATIC
        csv/array_converter.cpp
        csv/binary_converter.cpp
        csv/boolean_converter.cpp
        csv/converter.cpp
        csv/date_converter.cpp
        csv/datetime_converter.cpp
        csv/decimalv2_converter.cpp
        csv/decimalv3_converter.cpp
        csv/float_converter.cpp
        csv/numeric_converter.cpp
        csv/nullable_converter.cpp
        )

add_subdirectory(orc/apache-orc)
