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

#include "column/field.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "storage/row_store_encoder.h"
#include "util/slice.h"

namespace starrocks {

class RowStoreEncoderSimple : public RowStoreEncoder {
public:
    ~RowStoreEncoderSimple() {}

public:
    Status encode_chunk_to_full_row_column(const Schema& schema, const Chunk& chunk,
                                           BinaryColumn* dest_column) override;
    Status encode_columns_to_full_row_column(const Schema& schema, const Columns& columns, BinaryColumn& dest) override;
    Status decode_columns_from_full_row_column(const Schema& schema, const BinaryColumn& full_row_column,
                                               const std::vector<uint32_t>& read_column_ids,
                                               std::vector<std::unique_ptr<Column>>* dest) override;

private: // -- for simple and length
    void encode_null_bitmap(BitmapValue& null_bitmap, std::string* dest);
    void decode_null_bitmap(Slice* src, BitmapValue& null_bitmap);
    void encode_header(int32_t col_length, std::string* dest);
    void decode_header(Slice* src, int32_t* version, int32_t& num_value_cols);
    void encode_offset(std::string* dest, std::vector<int32_t>* offsets);
    void decode_offset(Slice* src, std::vector<int32_t>* offsets, const int32_t& num_value_cols);
};
} // namespace starrocks