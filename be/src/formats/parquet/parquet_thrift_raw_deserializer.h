// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <string>

#include "gen_cpp/parquet_types.h"

namespace starrocks::parquet {

// These raw entry points are compiled into StarRocksGen so parquet thrift
// protocol construction and generated tparquet::*::read() execution stay in
// the same DSO in --with-dynamic mode.
//
// The API intentionally avoids Status/Common/Base types. StarRocksGen sits
// below those layers, so callers provide an optional std::string to receive
// the error message and convert it to Status in a higher-level wrapper.
struct ParquetThriftLimits {
    int32_t max_message_size;
    int32_t max_frame_size;
    int32_t max_recursion_depth;
};

bool deserialize_parquet_file_metadata_raw(const uint8_t* buf, uint32_t* len, const ParquetThriftLimits& limits,
                                           tparquet::FileMetaData* metadata, std::string* err);

bool deserialize_parquet_page_header_raw(const uint8_t* buf, uint32_t* len, const ParquetThriftLimits& limits,
                                         tparquet::PageHeader* header, std::string* err);

bool deserialize_parquet_column_index_raw(const uint8_t* buf, uint32_t* len, const ParquetThriftLimits& limits,
                                          tparquet::ColumnIndex* column_index, std::string* err);

bool deserialize_parquet_offset_index_raw(const uint8_t* buf, uint32_t* len, const ParquetThriftLimits& limits,
                                          tparquet::OffsetIndex* offset_index, std::string* err);

bool deserialize_parquet_bloom_filter_header_raw(const uint8_t* buf, uint32_t* len, const ParquetThriftLimits& limits,
                                                 tparquet::BloomFilterHeader* header, std::string* err);

} // namespace starrocks::parquet
