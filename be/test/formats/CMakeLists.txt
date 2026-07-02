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

set(FORMAT_CORE_TEST_FILES
    ../base/cxa_throw_wrap.cpp
    disk_range_test.cpp
    io/async_flush_output_stream_test.cpp
    io/formatted_output_stream_file_test.cpp
)

set(FORMAT_CSV_TEST_FILES
    csv/array_converter_test.cpp
    csv/boolean_converter_test.cpp
    csv/csv_file_writer_test.cpp
    csv/csv_reader_test.cpp
    csv/date_converter_test.cpp
    csv/datetime_converter_test.cpp
    csv/decimalv2_converter_test.cpp
    csv/default_value_converter_test.cpp
    csv/float_converter_test.cpp
    csv/json_converter_test.cpp
    csv/map_converter_test.cpp
    csv/nullable_converter_test.cpp
    csv/numeric_converter_test.cpp
    csv/string_converter_test.cpp
    csv/varbinary_converter_test.cpp
)

set(FORMAT_JSON_TEST_FILES
    json/binary_column_test.cpp
    json/map_column_test.cpp
    json/nullable_column_test.cpp
    json/numeric_column_test.cpp
    json/struct_column_test.cpp
)

set(FORMAT_AVRO_TEST_FILES
    avro/binary_column_test.cpp
    avro/nullable_column_test.cpp
    avro/numeric_column_test.cpp
    avro/cpp/avro_reader_test.cpp
    avro/cpp/binary_column_reader_test.cpp
    avro/cpp/complex_column_reader_test.cpp
    avro/cpp/date_column_reader_test.cpp
    avro/cpp/numeric_column_reader_test.cpp
    avro/cpp/utils_test.cpp
)

set(FORMAT_ORC_TEST_FILES
    orc/orc_column_reader_test.cpp
    orc/orc_emit_rejected_rows_test.cpp
    orc/orc_lazy_load_test.cpp
    orc/utils_test.cpp
)

set(FORMAT_TEST_LINK_LIBS
    FormatOrc
    FormatAvro
    FormatJson
    FormatCsv
    FormatCore
    ExprCore
    Runtime
    FileSystem
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    Gutil
    StarRocksGen
    ${STARROCKS_DEPENDENCIES}
    ${STARROCKS_STATIC_LINK_LIBS}
    ${WL_LINK_DYNAMIC} ${STARROCKS_SYSTEM_LINK_LIBS} ${WRAP_LINKER_FLAGS}
    ${WL_START_GROUP}
    gmock
    gtest
    ${WL_END_GROUP}
)

add_library(starrocks_format_core_test_objs OBJECT ${FORMAT_CORE_TEST_FILES})
target_link_libraries(starrocks_format_core_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_format_core_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")

add_library(starrocks_format_csv_test_objs OBJECT ${FORMAT_CSV_TEST_FILES})
target_link_libraries(starrocks_format_csv_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_format_csv_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")

add_library(starrocks_format_json_test_objs OBJECT ${FORMAT_JSON_TEST_FILES})
target_link_libraries(starrocks_format_json_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_format_json_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")

add_library(starrocks_format_avro_test_objs OBJECT ${FORMAT_AVRO_TEST_FILES})
target_link_libraries(starrocks_format_avro_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_format_avro_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")

add_library(starrocks_format_orc_test_objs OBJECT ${FORMAT_ORC_TEST_FILES})
target_link_libraries(starrocks_format_orc_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_format_orc_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")

add_executable(format_test
    format_test_main.cpp
    $<TARGET_OBJECTS:starrocks_format_core_test_objs>
    $<TARGET_OBJECTS:starrocks_format_csv_test_objs>
    $<TARGET_OBJECTS:starrocks_format_json_test_objs>
    $<TARGET_OBJECTS:starrocks_format_avro_test_objs>
    $<TARGET_OBJECTS:starrocks_format_orc_test_objs>
)
target_link_libraries(format_test ${FORMAT_TEST_LINK_LIBS})
