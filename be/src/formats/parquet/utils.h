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

#include <algorithm>
#include <cstddef>
#include <cstdint>

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

struct ParquetField;

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

// Infer typed descriptor for variant typed_value Parquet field (scalar/array/map/struct).
// Falls back to TYPE_VARIANT when the source type cannot be represented safely.
// This function always returns a concrete descriptor and never TYPE_UNKNOWN.
TypeDescriptor variant_typed_desc_from_parquet_field(const ParquetField* field);

// Returns true when the Parquet schema annotates this column as an unsigned integer:
// a modern logical INTEGER with isSigned=false, or a legacy UINT_* converted type.
// Used both to zero-extend unsigned INT32 values on load and to derive the unsigned
// statistics sort order so signed-ordered legacy footer stats are not trusted.
bool is_unsigned_integer(const tparquet::SchemaElement& schema_element);

// GEO values have undefined sort order, even when read through an ordinary binary/string slot.
inline bool has_geo_annotation(const tparquet::SchemaElement& schema_element) {
    return schema_element.__isset.logicalType &&
           (schema_element.logicalType.__isset.GEOMETRY || schema_element.logicalType.__isset.GEOGRAPHY);
}

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

// Counts the nulls and the null/non-null runs of `n` null flags (0/1) into `infos`. The sums go per block of at
// most 255 rows into uint8 accumulators, so the compiler keeps the reductions in byte lanes.
inline void count_nulls_and_ranges(const uint8_t* __restrict is_nulls, size_t n, NullInfos* infos) {
    constexpr size_t kBlock = 240;
    size_t num_nulls = 0;
    size_t num_transitions = 0;
    for (size_t begin = 0; begin < n; begin += kBlock) {
        size_t end = std::min(begin + kBlock, n);
        uint8_t block_nulls = 0;
        for (size_t i = begin; i < end; ++i) {
            block_nulls += is_nulls[i];
        }
        uint8_t block_transitions = 0;
        for (size_t i = std::max<size_t>(begin, 1); i < end; ++i) {
            block_transitions += is_nulls[i] ^ is_nulls[i - 1];
        }
        num_nulls += block_nulls;
        num_transitions += block_transitions;
    }
    infos->num_nulls = num_nulls;
    infos->num_ranges = num_transitions + 1;
}

// Turns `n` definition levels into the null flags of `infos` (levels[i] < max_def_level), and counts the
// nulls and the null/non-null runs. The flags are written in one pass and counted in a second; the previous
// per-row loop (size_t counters, reading is_nulls[i - 1] back) was ~5x slower on gcc/x86 and clang/arm, see
// be/src/bench/parquet_def_level_bench.cpp.
inline void assign_nulls_from_def_levels(const int16_t* __restrict levels, int16_t max_def_level, size_t n,
                                         NullInfos* infos) {
    uint8_t* __restrict is_nulls = infos->nulls_data();
    for (size_t i = 0; i < n; ++i) {
        is_nulls[i] = levels[i] < max_def_level;
    }
    count_nulls_and_ranges(is_nulls, n, infos);
}

} // namespace starrocks::parquet
