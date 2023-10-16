// Copyright 2021 - present StarRocks, Inc.All rights reserved.
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

#include "storage/row_store_encoder_simple.h"

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/status.h"
#include "gutil/endian.h"
#include "storage/chunk_helper.h"
#include "storage/olap_common.h"
#include "storage/primary_key_encoder.h"
#include "storage/row_store_encoder_util.h"
#include "storage/tablet_schema.h"
#include "types/date_value.hpp"

namespace starrocks {

Status RowStoreEncoderSimple::encode_columns_to_full_row_column(const Schema& schema, const Columns& columns,
                                                                BinaryColumn& dest) {
    RETURN_IF_ERROR(is_supported(schema));
    // columns only include values used, idx is offset, (value index - key end index)
    size_t num_rows = columns[0]->size();
    size_t num_value_cols = columns.size();
    int num_key_cols = schema.num_key_fields();

    std::vector<int32_t> offsets;
    std::string header;
    std::string null_bitmap_str;
    std::string offset_str;
    std::string buff;
    std::string value_buff;
    BitmapValue null_bitmap;
    dest.reserve(dest.size() + num_rows);

    for (size_t i = 0; i < num_rows; i++) {
        header.clear();
        null_bitmap.clear();
        null_bitmap_str.clear();
        offset_str.clear();
        buff.clear();
        offsets.clear();
        // header
        encode_header(num_value_cols, &header);
        // bitset + offset+ value
        for (int j = 0; j < num_value_cols; j++) {
            value_buff.clear();
            size_t idx = j + num_key_cols;
            if (columns[j]->get(i).is_null()) {
                if (schema.field(idx)->is_nullable()) {
                    null_bitmap.add(j);
                } else {
                    return Status::InternalError("null value in non-null filed, check value correct.");
                }
            } else {
                //TODO(jkj) check varialbe field length
                auto value_buffer_start = reinterpret_cast<uint8_t*>(&value_buff[0]);
                uint32_t len = columns[j]->serialize(i, value_buffer_start);
                buff.append(reinterpret_cast<const char*>(value_buffer_start), len);
                offsets.emplace_back(len);
            }
        }
        encode_null_bitmap(null_bitmap, &null_bitmap_str);
        encode_offset(&offset_str, &offsets);
        std::stringstream ss;
        ss << header << null_bitmap_str << offset_str << buff;
        dest.append(ss.str());
    }

    return Status::OK();
}

Status RowStoreEncoderSimple::encode_chunk_to_full_row_column(const Schema& schema, const Chunk& chunk,
                                                              BinaryColumn* dest) {
    // chunk have all columns, include key columns
    // schema also have all columns, include key columns
    Columns columns;
    size_t num_key_fields = schema.num_key_fields();
    if (chunk.columns().size() - num_key_fields == 0) {
        return Status::NotSupported("only have key columns using column with row");
    }
    for (int i = 0; i < chunk.columns().size() - num_key_fields; i++) {
        columns.emplace_back(chunk.columns()[i + num_key_fields]);
    }
    return encode_columns_to_full_row_column(schema, columns, *dest);
}

Status RowStoreEncoderSimple::decode_columns_from_full_row_column(const Schema& schema,
                                                                  const BinaryColumn& full_row_column,
                                                                  const std::vector<uint32_t>& read_column_ids,
                                                                  std::vector<std::unique_ptr<Column>>* pdest) {
    auto& dest = *pdest;
    int num_rows = full_row_column.size();
    for (size_t i = 0; i < num_rows; i++) {
        Slice s = full_row_column.get_slice(i);
        int32_t version = RowStoreEncoderType::SIMPLE;

        size_t num_cols = schema.num_fields();
        size_t num_key_cols = schema.num_key_fields();
        int32_t num_value_cols = num_cols - num_key_cols;

        // header 8 bytes
        decode_header(&s, &version, num_value_cols);
        BitmapValue null_bitmap;
        // null bitset, only value column
        decode_null_bitmap(&s, null_bitmap);
        // offsets  4 bytes,
        std::vector<int32_t> offsets;
        decode_offset(&s, &offsets, num_value_cols);

        //value
        uint32_t cur_read_idx = 0;
        Column* dest_column = nullptr;
        for (uint j = num_key_cols; j <= read_column_ids.back(); j++) {
            size_t idx = j - num_key_cols;
            if (read_column_ids[cur_read_idx] == j) {
                dest_column = dest[cur_read_idx].get();
                if (null_bitmap.contains(idx)) {
                    dest_column->append_nulls(1);
                    cur_read_idx++;
                    continue;
                }
            } else {
                // skip not read fields
                s.remove_prefix(offsets[idx]);
                continue;
            }
            // cur_read_idx must in offsets size
            assert(cur_read_idx < offsets.size());
            auto s_offset = reinterpret_cast<const uint8_t*>(s.data);
            int32_t col_length = offsets[idx];
            Slice slice(s.data, col_length);
            dest_column->deserialize_and_append(s_offset);
            s.remove_prefix(col_length);
            cur_read_idx++;
        }
    }
    return Status::OK();
}

// encode bitmap<column_num>
void RowStoreEncoderSimple::encode_null_bitmap(BitmapValue& null_bitmap, std::string* dest) {
    size_t len = null_bitmap.getSizeInBytes();
    encode_integral<size_t>(len, dest);
    std::string bitmap_value;
    bitmap_value.reserve(len);
    char* bitmap_value_offset = &bitmap_value[0];
    null_bitmap.write(bitmap_value_offset);
    dest->append(bitmap_value_offset, len);
}

// decode bitma<column_num>
void RowStoreEncoderSimple::decode_null_bitmap(Slice* src, BitmapValue& null_bitmap) {
    // current use slice seperator, it's easy, read slice function has already existed
    std::string dest;
    size_t len = 0;
    decode_integral<size_t>(src, &len);
    Slice null_bitmap_slice(src->get_data(), len);
    null_bitmap.deserialize(null_bitmap_slice.get_data());
    src->remove_prefix(len);
}

// encode version
void RowStoreEncoderSimple::encode_header(int32_t col_length, std::string* dest) {
    encode_integral<int32_t>(ROW_STORE_VERSION, dest);
    encode_integral<int32_t>(col_length, dest);
}

// decode version
void RowStoreEncoderSimple::decode_header(Slice* src, int32_t* version, int32_t& num_value_cols) {
    decode_integral<int32_t>(src, version);
    decode_integral<int32_t>(src, &num_value_cols);
}

// encode offset
void RowStoreEncoderSimple::encode_offset(std::string* dest, std::vector<int32_t>* offsets) {
    for (auto& offset : *offsets) {
        encode_integral<int32_t>(offset, dest);
    }
}

// decode offset
void RowStoreEncoderSimple::decode_offset(Slice* src, std::vector<int32_t>* offsets, const int32_t& num_value_cols) {
    int32_t offset = 0;
    for (int i = 0; i < num_value_cols; i++) {
        decode_integral<int32_t>(src, &offset);
        offsets->emplace_back(offset);
    }
}
} // namespace starrocks