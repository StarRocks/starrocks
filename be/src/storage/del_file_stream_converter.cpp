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

#include "storage/del_file_stream_converter.h"

#include "column/chunk.h"
#include "column/column.h"
#include "column/schema.h"
#include "gutil/strings/substitute.h"
#include "serde/column_array_serde.h"
#include "storage/primary_key_encoder.h"
#include "storage/tablet_schema.h"
#include "storage/types.h"
#include "types/logical_type_infra.h"

namespace starrocks {

namespace {

bool is_fixed_length_non_string_pk_type(LogicalType type) {
    switch (type) {
#define M(LT) case LT:
        APPLY_FOR_ALL_PK_SUPPORT_FIXED_TYPE(M)
#undef M
        return true;
    default:
        // TYPE_VARCHAR and other types fall through; they are binary-compatible across V1/V2.
        return false;
    }
}

} // namespace

bool is_single_fixed_length_non_string_primary_key(const Schema& pkey_schema) {
    return pkey_schema.num_fields() == 1 && is_fixed_length_non_string_pk_type(pkey_schema.field(0)->type()->type());
}

bool is_single_fixed_length_non_string_primary_key(const TabletSchema& tablet_schema) {
    return tablet_schema.num_key_columns() == 1 && is_fixed_length_non_string_pk_type(tablet_schema.column(0).type());
}

bool requires_v1_to_v2_del_transcode(PrimaryKeyEncodingType source_encoding, PrimaryKeyEncodingType target_encoding,
                                     const Schema& pkey_schema) {
    return source_encoding == PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1 &&
           target_encoding == PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2 &&
           is_single_fixed_length_non_string_primary_key(pkey_schema);
}

DelFileStreamConverter::DelFileStreamConverter(std::string_view input_file_name, uint64_t input_file_size,
                                               std::unique_ptr<WritableFile> output_file, SchemaPtr pkey_schema,
                                               PrimaryKeyEncodingType source_pk_encoding,
                                               PrimaryKeyEncodingType target_pk_encoding)
        : FileStreamConverter(input_file_name, input_file_size, std::move(output_file)),
          _pkey_schema(std::move(pkey_schema)),
          _source_pk_encoding(source_pk_encoding),
          _target_pk_encoding(target_pk_encoding) {
    DCHECK(_pkey_schema != nullptr);
    DCHECK(_source_pk_encoding == PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1);
    DCHECK(_target_pk_encoding == PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
}

Status DelFileStreamConverter::append(const void* data, size_t size) {
    if (size == 0) {
        return Status::OK();
    }
    // Overflow-safe bound check against the declared input size.
    const uint64_t buffered = _input_buffer.size();
    if (buffered > _input_file_size || size > _input_file_size - buffered) {
        LOG(WARNING) << "del file stream overflow, file: " << _input_file_name
                     << ", declared_size: " << _input_file_size << ", already_buffered: " << buffered
                     << ", incoming: " << size;
        return Status::Corruption("del file append exceeds declared input_file_size");
    }
    _input_buffer.append(static_cast<const char*>(data), size);
    return Status::OK();
}

Status DelFileStreamConverter::close() {
    if (_input_buffer.size() != _input_file_size) {
        LOG(WARNING) << "del file short read, file: " << _input_file_name << ", declared_size: " << _input_file_size
                     << ", actual: " << _input_buffer.size();
        return Status::Corruption("del file short read");
    }

    MutableColumnPtr src_col;
    RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(*_pkey_schema, &src_col, _source_pk_encoding));

    const uint8_t* const input_begin = reinterpret_cast<const uint8_t*>(_input_buffer.data());
    const uint8_t* const input_end = input_begin + _input_buffer.size();
    ASSIGN_OR_RETURN(const uint8_t* consumed_end,
                     serde::ColumnArraySerde::deserialize(input_begin, input_end, src_col.get()));
    if (consumed_end != input_end) {
        LOG(WARNING) << "del file deserialize did not consume full buffer, file: " << _input_file_name
                     << ", consumed: " << (consumed_end - input_begin) << ", total: " << _input_buffer.size();
        return Status::Corruption("del file deserialize did not consume full buffer");
    }
    const size_t row_count = src_col->size();

    // PrimaryKeyEncoder::encode reads typed raw_data from the source column and writes
    // V2-encoded slices (sign-flipped big-endian for signed integers; see
    // primary_key_encoder.h encode_integral) into the BinaryColumn destination.
    MutableColumnPtr dst_col;
    RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(*_pkey_schema, &dst_col, _target_pk_encoding));

    Columns pk_columns;
    pk_columns.emplace_back(std::move(src_col));
    Chunk pk_chunk(std::move(pk_columns), _pkey_schema);
    PrimaryKeyEncoder::encode(*_pkey_schema, pk_chunk, /*offset=*/0, /*len=*/row_count, dst_col.get(),
                              _target_pk_encoding);

    const int64_t output_capacity = serde::ColumnArraySerde::max_serialized_size(*dst_col);
    if (output_capacity <= 0) {
        return Status::InternalError("ColumnArraySerde::max_serialized_size returned 0 for target column");
    }
    std::string output_bytes;
    output_bytes.resize(static_cast<size_t>(output_capacity));
    auto* const output_begin = reinterpret_cast<uint8_t*>(output_bytes.data());
    ASSIGN_OR_RETURN(uint8_t * output_end, serde::ColumnArraySerde::serialize(*dst_col, output_begin));
    output_bytes.resize(static_cast<size_t>(output_end - output_begin));

    // Skip the parent class size invariant: V1 and V2 layouts have different sizes by design.
    RETURN_IF_ERROR(_output_file->append(Slice(output_bytes)));
    _final_output_file_size = _output_file->size();

    LOG(INFO) << "Del file V1->V2 transcode complete, file: " << _input_file_name << ", rows: " << row_count
              << ", input_bytes: " << _input_file_size << ", output_bytes: " << _final_output_file_size;

    return _output_file->close();
}

} // namespace starrocks
