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

#include "formats/parquet/column_chunk_writer.h"

#include <parquet/arrow/writer.h>

#include "formats/parquet/chunk_writer.h"
#include "formats/parquet/level_builder.h"
#include "gutil/casts.h"

namespace starrocks::parquet {

ColumnChunkWriter::ColumnChunkWriter(::parquet::ColumnWriter* column_writer) : _column_writer(column_writer) {}

#define WRITE_BATCH_CASE(ParquetType)                                                                           \
    case ParquetType: {                                                                                         \
        auto typed_column_writer =                                                                              \
                down_cast<::parquet::TypedColumnWriter<::parquet::PhysicalType<ParquetType>>*>(_column_writer); \
        auto values = reinterpret_cast<::parquet::type_traits<ParquetType>::value_type*>(result.values);        \
        if (result.null_bitset == nullptr) {                                                                    \
            typed_column_writer->WriteBatch(result.num_levels, result.def_levels, result.rep_levels, values);   \
        } else {                                                                                                \
            typed_column_writer->WriteBatchSpaced(result.num_levels, result.def_levels, result.rep_levels,      \
                                                  result.null_bitset, 0, values);                               \
        }                                                                                                       \
        return;                                                                                                 \
    }

void ColumnChunkWriter::write(const LevelBuilderResult& result) {
    switch (_column_writer->type()) {
        WRITE_BATCH_CASE(::parquet::Type::BOOLEAN);
        WRITE_BATCH_CASE(::parquet::Type::INT32);
        WRITE_BATCH_CASE(::parquet::Type::INT64);
        WRITE_BATCH_CASE(::parquet::Type::INT96);
        WRITE_BATCH_CASE(::parquet::Type::FLOAT);
        WRITE_BATCH_CASE(::parquet::Type::DOUBLE);
        WRITE_BATCH_CASE(::parquet::Type::BYTE_ARRAY);
        WRITE_BATCH_CASE(::parquet::Type::FIXED_LEN_BYTE_ARRAY);
    default: {
    }
    }
}

#define ESTIMATED_BUFFERED_VALUE_BYTES(ParquetType)                                                             \
    case ParquetType: {                                                                                         \
        auto typed_column_writer =                                                                              \
                down_cast<::parquet::TypedColumnWriter<::parquet::PhysicalType<ParquetType>>*>(_column_writer); \
        return typed_column_writer->EstimatedBufferedValueBytes();                                              \
    }

int64_t ColumnChunkWriter::estimated_buffered_value_bytes() {
    switch (_column_writer->type()) {
        ESTIMATED_BUFFERED_VALUE_BYTES(::parquet::Type::BOOLEAN);
        ESTIMATED_BUFFERED_VALUE_BYTES(::parquet::Type::INT32);
        ESTIMATED_BUFFERED_VALUE_BYTES(::parquet::Type::INT64);
        ESTIMATED_BUFFERED_VALUE_BYTES(::parquet::Type::INT96);
        ESTIMATED_BUFFERED_VALUE_BYTES(::parquet::Type::FLOAT);
        ESTIMATED_BUFFERED_VALUE_BYTES(::parquet::Type::DOUBLE);
        ESTIMATED_BUFFERED_VALUE_BYTES(::parquet::Type::BYTE_ARRAY);
        ESTIMATED_BUFFERED_VALUE_BYTES(::parquet::Type::FIXED_LEN_BYTE_ARRAY);
    default: {
        return 0;
    }
    }
}

} // namespace starrocks::parquet
