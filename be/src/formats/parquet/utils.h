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

#include <cstddef>

#include "base/container/raw_container.h"
#include "gen_cpp/parquet_types.h"
#include "gen_cpp/types.pb.h"
#include "types/type_descriptor.h"

namespace parquet {
class FileMetaData;
} // namespace parquet

namespace starrocks {
class Column;
}

namespace starrocks::parquet {

enum ColumnContentType { VALUE, DICT_CODE };

enum ColumnIOType { INVALID = 0, PAGE_INDEX = 1, PAGES = 2, BLOOM_FILTER = 4 };
enum CacheType { META, PAGE };

using ColumnIOTypeFlags = int32_t;

class ParquetUtils {
public:
    static CompressionTypePB convert_compression_codec(tparquet::CompressionCodec::type parquet_codec);

    static int decimal_precision_to_byte_count(int precision);

    static std::vector<int64_t> collect_split_offsets(const ::parquet::FileMetaData& meta_data);

    static int64_t get_column_start_offset(const tparquet::ColumnMetaData& column);

    static int64_t get_row_group_start_offset(const tparquet::RowGroup& row_group);

    static int64_t get_row_group_end_offset(const tparquet::RowGroup& row_group);

    static std::string get_file_cache_key(CacheType type, const std::string& filename, int64_t modification_time,
                                          uint64_t file_size);

    // Resolve constant/nullable wrappers and return the non-null underlying data column + row index.
    // Returns false if column is null at `row` or input/output args are invalid.
    static bool get_non_null_data_column_and_row(const Column* column, size_t row, const Column** out_column,
                                                 size_t* out_row);

    // Returns true when the column has at least one non-null value in [0, num_rows).
    static bool has_non_null_value(const Column* column, size_t num_rows);

    // Returns true when the column is binary (after unwrap) and has at least one non-null value in [0, num_rows).
    static bool has_non_null_binary_value(const Column* column, size_t num_rows);

private:
    inline static const std::vector<std::string> cache_key_prefix{"ft", "pg"};
};

struct NullInfos {
    // The number of nulls contained in the null vector.
    size_t num_nulls{};
    // The number of Runs in null info. (null[i] ! = null[i + 1]) This is an estimated value.
    size_t num_ranges{};

    uint8_t* nulls_data() { return _is_nulls.data(); }
    const uint8_t* nulls_data() const { return _is_nulls.data(); }

    // This function only reserves space but does no actual initialization.
    void reset_with_capacity(size_t num_rows) {
        _is_nulls.resize(num_rows);
        num_nulls = 0;
        num_ranges = 0;
    }

private:
    raw::RawVector<uint8_t> _is_nulls;
};

} // namespace starrocks::parquet
