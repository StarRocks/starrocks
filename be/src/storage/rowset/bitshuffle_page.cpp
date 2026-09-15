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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/bitshuffle_page.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/rowset/bitshuffle_page.h"

#include <fmt/format.h>

#include <algorithm>

#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "gutil/casts.h"
#include "gutil/compiler_util.h"
#include "gutil/port.h"
#include "gutil/strings/substitute.h"
#include "storage_primitive/rowid_types.h"
#include "types/date_value.h"
#include "types/timestamp_value.h"

namespace starrocks {

std::string bitshuffle_error_msg(int64_t err) {
    switch (err) {
    case -1:
        return "Failed to allocate memory";
    case -11:
        return "Missing SSE";
    case -12:
        return "Missing AVX";
    case -80:
        return "Input size not a multiple of 8";
    case -81:
        return "block_size not multiple of 8";
    case -91:
        return "Decompression error, wrong number of bytes processed";
    default:
        return strings::Substitute("Error internal to compression routine with error code $0", err);
    }
}

template <LogicalType Type>
Status BitShufflePageDecoder<Type>::read_by_rowids(const ordinal_t first_ordinal_in_page, const rowid_t* rowids,
                                                   size_t* count, Column* column) {
    DCHECK(_parsed);
    if (PREDICT_FALSE(*count == 0)) {
        return Status::OK();
    }
    size_t total = *count;
    size_t read_count = 0;

    if (UNLIKELY(column->is_constant())) {
        return Status::NotSupported("BitShufflePageDecoder::read_by_rowids does not support ConstColumn destination");
    }

    Column* data_col = nullptr;
    NullableColumn* nullable_col = nullptr;
    if (column->is_nullable()) {
        column->materialized_nullable();
        nullable_col = down_cast<NullableColumn*>(column);
        data_col = nullable_col->data_column_raw_ptr();
    } else {
        data_col = column;
    }

    uint8_t* raw_dst = nullptr;
    size_t orig_size = 0;
    if constexpr (!std::is_same_v<CppType, bool>) {
        if (data_col != nullptr) {
            if constexpr (Type == TYPE_DATE) {
                if (auto* date_col = dynamic_cast<FixedLengthColumnBase<DateValue>*>(data_col)) {
                    orig_size = date_col->size();
                    date_col->resize_uninitialized(orig_size + total);
                    raw_dst = date_col->mutable_raw_data() + orig_size * sizeof(DateValue);
                }
            } else if constexpr (Type == TYPE_DATETIME) {
                if (auto* dt_col = dynamic_cast<FixedLengthColumnBase<TimestampValue>*>(data_col)) {
                    orig_size = dt_col->size();
                    dt_col->resize_uninitialized(orig_size + total);
                    raw_dst = dt_col->mutable_raw_data() + orig_size * sizeof(TimestampValue);
                }
            }

            if (raw_dst == nullptr) {
                if (auto* fc = dynamic_cast<FixedLengthColumnBase<CppType>*>(data_col)) {
                    orig_size = fc->size();
                    fc->resize_uninitialized(orig_size + total);
                    raw_dst = fc->mutable_raw_data() + orig_size * sizeof(CppType);
                }
            }
        }

        if (raw_dst != nullptr) {
            CppType* dst = reinterpret_cast<CppType*>(raw_dst);

            constexpr size_t kPrefetchDist = 16;
            for (size_t i = 0; i < total; i++) {
                ordinal_t ord = rowids[i] - first_ordinal_in_page;
                if (UNLIKELY(ord >= _num_elements)) {
                    break;
                }
                if (i + kPrefetchDist < total) {
                    ordinal_t pre_ord = rowids[i + kPrefetchDist] - first_ordinal_in_page;
                    if (pre_ord < _num_elements) {
                        __builtin_prefetch(get_data(pre_ord * SIZE_OF_TYPE), 0, 3);
                    }
                }
                dst[read_count++] = *reinterpret_cast<const CppType*>(get_data(ord * SIZE_OF_TYPE));
            }

            if (read_count < total) {
                data_col->resize(orig_size + read_count);
            }
            if (nullable_col != nullptr && read_count > 0) {
                nullable_col->null_column_data().insert(nullable_col->null_column_data().end(), read_count, 0);
            }
        }
    }

    if (raw_dst == nullptr) {
        // Fallback path: small-buffer optimization avoiding heap allocation.
        // Cap stack buffer at <= 4KB across all CppType widths (e.g. 128 for int256_t, 1024 for int32_t).
        constexpr size_t kStackBatch = std::max<size_t>(1, 4096 / sizeof(CppType));
        CppType stack_buf[kStackBatch];
        std::unique_ptr<CppType[]> heap_buf;
        CppType* data =
                (total <= kStackBatch) ? stack_buf : (heap_buf = std::unique_ptr<CppType[]>(new CppType[total])).get();

        for (size_t i = 0; i < total; i++) {
            ordinal_t ord = rowids[i] - first_ordinal_in_page;
            if (UNLIKELY(ord >= _num_elements)) {
                break;
            }
            data[read_count++] = *reinterpret_cast<const CppType*>(get_data(ord * SIZE_OF_TYPE));
        }

        if (read_count > 0) {
            size_t nappend = column->append_numbers(data, SIZE_OF_TYPE * read_count);
            if (UNLIKELY(nappend != read_count)) {
                return Status::InternalError(
                        fmt::format("append_numbers failed, expected rows[{}], actual rows[{}]", read_count, nappend));
            }
        }
    }
    *count = read_count;
    return Status::OK();
}

#define DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(Type)                                            \
    template Status BitShufflePageDecoder<Type>::read_by_rowids(const ordinal_t first_ordinal_in_page, \
                                                                const rowid_t* rowids, size_t* count, Column* column)

DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_TINYINT);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_SMALLINT);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_INT);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_BIGINT);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_LARGEINT);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_FLOAT);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_DOUBLE);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_BOOLEAN);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_DATE);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_DATETIME);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_DECIMAL32);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_DECIMAL64);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_DECIMAL128);

DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_UNSIGNED_TINYINT);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_UNSIGNED_SMALLINT);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_UNSIGNED_INT);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_UNSIGNED_BIGINT);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_DATE_V1);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_DATETIME_V1);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_DECIMAL);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_DECIMALV2);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_DECIMAL256);
DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS(TYPE_INT256);

#undef DEFINE_BITSHUFFLE_PAGE_DECODER_READ_BY_ROWIDS

} // namespace starrocks
