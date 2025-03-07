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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/formats")

add_library(Formats STATIC
        csv/array_converter.cpp
        csv/array_reader.cpp
        csv/boolean_converter.cpp
        csv/converter.cpp
        csv/csv_reader.cpp
        csv/csv_file_writer.cpp
        csv/date_converter.cpp
        csv/datetime_converter.cpp
        csv/decimalv2_converter.cpp
        csv/decimalv3_converter.cpp
        csv/float_converter.cpp
        csv/json_converter.cpp
        csv/map_converter.cpp
        csv/numeric_converter.cpp
        csv/nullable_converter.cpp
        csv/default_value_converter.cpp
        csv/string_converter.cpp
        csv/varbinary_converter.cpp
        json/nullable_column.cpp
        json/numeric_column.cpp
        json/binary_column.cpp
        json/struct_column.cpp
        json/map_column.cpp
        avro/nullable_column.cpp
        avro/numeric_column.cpp
        avro/binary_column.cpp
        orc/orc_chunk_reader.cpp
        orc/orc_file_writer.cpp
        orc/orc_input_stream.cpp
        orc/orc_mapping.cpp
        orc/orc_memory_pool.cpp
        orc/orc_min_max_decoder.cpp
        orc/orc_schema_builder.cpp
        orc/utils.cpp
        orc/column_reader.cpp
        orc/memory_stream/MemoryInputStream.cc
        orc/memory_stream/MemoryOutputStream.cc
        parquet/arrow_memory_pool.cpp
        parquet/column_chunk_reader.cpp
        parquet/column_converter.cpp
        parquet/column_reader.cpp
        parquet/column_reader_factory.cpp
        parquet/scalar_column_reader.cpp
        parquet/complex_column_reader.cpp
        parquet/encoding.cpp
        parquet/level_codec.cpp
        parquet/page_reader.cpp
        parquet/parquet_file_writer.cpp
        parquet/schema.cpp
        parquet/stored_column_reader.cpp
        parquet/stored_column_reader_with_index.cpp
        parquet/utils.cpp
        parquet/metadata.cpp
        parquet/meta_helper.cpp
        parquet/group_reader.cpp
        parquet/file_reader.cpp
        parquet/file_writer.cpp
        parquet/chunk_writer.cpp
        parquet/level_builder.cpp
        parquet/column_chunk_writer.cpp
        parquet/column_read_order_ctx.cpp
        parquet/statistics_helper.cpp
        parquet/zone_map_filter_evaluator.h
        disk_range.hpp
        )

add_subdirectory(orc/apache-orc)
