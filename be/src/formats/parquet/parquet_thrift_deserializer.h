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

#include <string>

#include "common/status.h"
#include "formats/parquet/parquet_thrift_raw_deserializer.h"

namespace starrocks::parquet {
namespace detail {

inline Status parquet_thrift_status(bool ok, const std::string& err) {
    if (ok) {
        return Status::OK();
    }
    return Status::InternalError(err.empty() ? "Unknown exception" : err);
}

} // namespace detail

// This wrapper is the public parquet-facing API. It keeps Status handling in
// the parquet layer while delegating the actual thrift protocol construction
// and generated tparquet::*::read() call into StarRocksGen via the raw bridge.
//
// That split preserves the layering constraint: StarRocksGen does not depend on
// Common/Base, but parquet callers still get the normal Status interface.

inline Status deserialize_parquet_file_metadata(const uint8_t* buf, uint32_t* len, tparquet::FileMetaData* metadata) {
    std::string err;
    return detail::parquet_thrift_status(deserialize_parquet_file_metadata_raw(buf, len, metadata, &err), err);
}

inline Status deserialize_parquet_page_header(const uint8_t* buf, uint32_t* len, tparquet::PageHeader* header) {
    std::string err;
    return detail::parquet_thrift_status(deserialize_parquet_page_header_raw(buf, len, header, &err), err);
}

inline Status deserialize_parquet_column_index(const uint8_t* buf, uint32_t* len,
                                               tparquet::ColumnIndex* column_index) {
    std::string err;
    return detail::parquet_thrift_status(deserialize_parquet_column_index_raw(buf, len, column_index, &err), err);
}

inline Status deserialize_parquet_offset_index(const uint8_t* buf, uint32_t* len,
                                               tparquet::OffsetIndex* offset_index) {
    std::string err;
    return detail::parquet_thrift_status(deserialize_parquet_offset_index_raw(buf, len, offset_index, &err), err);
}

inline Status deserialize_parquet_bloom_filter_header(const uint8_t* buf, uint32_t* len,
                                                      tparquet::BloomFilterHeader* header) {
    std::string err;
    return detail::parquet_thrift_status(deserialize_parquet_bloom_filter_header_raw(buf, len, header, &err), err);
}

} // namespace starrocks::parquet
