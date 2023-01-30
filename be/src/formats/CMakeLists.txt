# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/formats")

add_library(Formats STATIC
        csv/array_converter.cpp
        csv/array_reader.cpp
        csv/binary_converter.cpp
        csv/boolean_converter.cpp
        csv/converter.cpp
        csv/csv_reader.cpp
        csv/date_converter.cpp
        csv/datetime_converter.cpp
        csv/decimalv2_converter.cpp
        csv/decimalv3_converter.cpp
        csv/float_converter.cpp
        csv/json_converter.cpp
        csv/numeric_converter.cpp
        csv/nullable_converter.cpp
        json/nullable_column.cpp
        json/numeric_column.cpp
        json/binary_column.cpp
        orc/orc_chunk_reader.cpp
        orc/orc_mapping.cpp
        parquet/column_chunk_reader.cpp
        parquet/column_converter.cpp
        parquet/column_reader.cpp
        parquet/encoding.cpp
        parquet/level_codec.cpp
        parquet/page_reader.cpp
        parquet/schema.cpp
        parquet/stored_column_reader.cpp
        parquet/utils.cpp
        parquet/metadata.cpp
        parquet/group_reader.cpp
        parquet/file_reader.cpp
        )

add_subdirectory(orc/apache-orc)