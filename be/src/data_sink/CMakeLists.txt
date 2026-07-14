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
