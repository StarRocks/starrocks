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

// op
#define RS_DEL_OP 0
#define RS_PUT_OP 1
// only use 56bit version
#define VER_MASK 0xFFFFFFFFFFFFFF
#define OP_MASK 0xFF
// key format:
// | raw_key | version 56bit(reserse) | OP 8bit | raw_key length 32bit |

inline int64_t encode_version(const int8_t op, const int64_t version) {
    return ((VER_MASK - (version & VER_MASK)) << 8) | ((int64_t)op & OP_MASK);
}

inline void decode_version(const int64_t raw_version, int8_t& op, int64_t& version) {
    version = VER_MASK - ((raw_version >> 8) & VER_MASK);
    op = (int8_t)(raw_version & OP_MASK);
}

class RowStoreEncoder {
public:
    static void chunk_to_keys(const Schema& schema, const Chunk& chunk, size_t offset, size_t len,
                              std::vector<std::string>& keys);
    static void chunk_to_values(const Schema& schema, const Chunk& chunk, size_t offset, size_t len,
                                std::vector<std::string>& values);
    static void encode_chunk_to_full_row_column(const Schema& schema, const Chunk& chunk, BinaryColumn* dest_column);
    static void extract_columns_from_full_row_column(const Schema& schema, const BinaryColumn& full_row_column,
                                                     const std::vector<uint32_t>& read_column_ids,
                                                     std::vector<std::unique_ptr<Column>>& dest);
    static void encode_columns_to_full_row_column(const Schema& schema, const std::vector<Column*>& columns,
                                                  BinaryColumn& dest);
    static Status kvs_to_chunk(const std::vector<std::string>& keys, const std::vector<std::string>& values,
                               const Schema& schema, Chunk* dest);
    static void combine_key_with_ver(std::string& key, const int8_t op, const int64_t version);
    static Status split_key_with_ver(const std::string& ckey, std::string& key, int8_t& op, int64_t& version);
    static void pk_column_to_keys(Schema& pkey_schema, Column* column, std::vector<std::string>& keys);
};

} // namespace starrocks