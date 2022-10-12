# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/column")

add_library(Column STATIC
        array_column.cpp
        chunk.cpp
        column.cpp
        column_helper.cpp
        column_viewer.cpp
        const_column.cpp
        datum.cpp
        datum_convert.cpp
        datum_tuple.cpp
        field.cpp
        fixed_length_column_base.cpp
        fixed_length_column.cpp
        nullable_column.cpp
        schema.cpp
        binary_column.cpp
        object_column.cpp
        decimalv3_column.cpp
        column_visitor.cpp
        column_visitor_mutable.cpp
        json_column.cpp
        map_column.cpp
        )
