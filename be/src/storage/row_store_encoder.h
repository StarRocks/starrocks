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

namespace starrocks {

enum RowStoreEncoderType {
    SIMPLE = 0,
    LENGTH = 1,
    EXTERNAL = 2,
};

#define ROW_STORE_VERSION 0

typedef size_t (*EncodeLengthOp)(const void*, int, std::string*, int32_t);
std::unique_ptr<Schema> create_binary_schema();

class RowStoreEncoder {
public:
    virtual ~RowStoreEncoder() = default;

    virtual Status encode_chunk_to_full_row_column(const Schema& schema, const Chunk& chunk,
                                                   BinaryColumn* dest_column) = 0;
    // columns only contain value column, exclude key columns
    virtual Status encode_columns_to_full_row_column(const Schema& schema, const Columns& columns,
                                                     BinaryColumn& dest) = 0;
    virtual Status decode_columns_from_full_row_column(const Schema& schema, const BinaryColumn& full_row_column,
                                                       const std::vector<uint32_t>& read_column_ids,
                                                       MutableColumns* dest) = 0;
    Status is_supported(const Schema& schema);

protected:
    // simple encode not support complex type
    bool is_field_supported(const Field& f);
};

} // namespace starrocks
