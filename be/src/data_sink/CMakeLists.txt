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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/data_sink")

ADD_BE_LIB(DataSinkFile
    file/parquet_builder.cpp
    file/plain_text_builder.cpp
)

target_link_libraries(DataSinkFile PUBLIC
    FileSystem
    ChunkCore
    ColumnCore
    Common
)

target_link_libraries(DataSinkFile PRIVATE
    FormatCore
    FormatCsv
    FormatParquet
    Expr
    Gutil
)

ADD_BE_LIB(DataSinkResult
    result/arrow_result_writer.cpp
    result/buffer_control_result_writer.cpp
    result/customized_result_writer.cpp
    result/file_result_writer.cpp
    result/http_result_writer.cpp
    result/memory_scratch_sink.cpp
    result/metadata_result_writer.cpp
    result/mysql_result_writer.cpp
    result/result_sink.cpp
    result/statistic_result_writer.cpp
    result/variable_result_writer.cpp
)

target_link_libraries(DataSinkResult PUBLIC
    ComputeEnv
    ExecPrimitive
    FormatParquet
    Runtime
    FileSystem
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    StarRocksGen
)

target_link_libraries(DataSinkResult PRIVATE
    DataSinkFile
    FormatCore
    FormatCsv
    Expr
    Platform
    Gutil
)

ADD_BE_LIB(DataSinkExchange
    exchange/data_stream_sender.cpp
    exchange/multi_cast_data_stream_sink.cpp
)

target_link_libraries(DataSinkExchange PUBLIC
    ExecPrimitive
    Runtime
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    StarRocksGen
)

target_link_libraries(DataSinkExchange PRIVATE
    ComputeEnv
    Expr
    Gutil
)

ADD_BE_LIB(DataSinkExternal
    external/blackhole_table_sink.cpp
    external/export_sink.cpp
    external/hive_table_sink.cpp
    external/mysql_table_sink.cpp
    external/mysql_table_writer.cpp
    external/schema_table_sink.cpp
    external/table_function_table_sink.cpp
)

target_link_libraries(DataSinkExternal PUBLIC
    ExecPrimitive
    FormatCore
    FormatCsv
    Runtime
    FileSystem
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    StarRocksGen
)

target_link_libraries(DataSinkExternal PRIVATE
    DataSinkFile
    StoragePrimitive
    Expr
    Platform
    Gutil
)

ADD_BE_LIB(DataSinkDictionaryCache
    dictionary_cache/dictionary_cache_sink.cpp
)

target_link_libraries(DataSinkDictionaryCache PUBLIC
    ExecPrimitive
    Runtime
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    StarRocksGen
)

target_link_libraries(DataSinkDictionaryCache PRIVATE
    Gutil
)

ADD_BE_LIB(DataSinkTablet
    tablet/multi_olap_table_sink.cpp
    tablet/range_router.cpp
    tablet/range_tablet_sink_sender.cpp
    tablet/olap_table_sink.cpp
    tablet/tablet_sink_colocate_sender.cpp
    tablet/tablet_sink_index_channel.cpp
    tablet/tablet_sink_sender.cpp
)

target_link_libraries(DataSinkTablet PUBLIC
    StoragePrimitive
    ExecPrimitive
    Runtime
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    StarRocksGen
)

target_link_libraries(DataSinkTablet PRIVATE
    Storage
    ComputeEnv
    Expr
    Platform
    Gutil
)
