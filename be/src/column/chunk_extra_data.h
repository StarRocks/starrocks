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

#include "column/chunk.h"
#include "serde/column_array_serde.h"

namespace starrocks {
class ChunkExtraColumnsData;
using ChunkExtraColumnsDataPtr = std::shared_ptr<ChunkExtraColumnsData>;

struct ChunkExtraColumnsMeta {
    TypeDescriptor type;
    bool is_null;
    bool is_const;
};

/*
 * `ChunkExtraColumnsData` is a specific implementation which includes extra 
 * columns besides the schema, and it supports common methods like Chunk. eg, 
 * In Stream MV scenes, the hidden `_op_` column can be added in the ChunkExtraData.
 */
class ChunkExtraColumnsData final : public ChunkExtraData {
public:
    ChunkExtraColumnsData(std::vector<ChunkExtraColumnsMeta> extra_metas, Columns columns)
            : _data_metas(std::move(extra_metas)), _columns(std::move(columns)) {}
    ~ChunkExtraColumnsData() override = default;

    std::vector<ChunkExtraColumnsMeta> chunk_data_metas() const { return _data_metas; }

    Columns columns() const { return _columns; }
    size_t num_rows() const { return _columns.empty() ? 0 : _columns[0]->size(); }

    void filter(const Buffer<uint8_t>& selection) const;
    void filter_range(const Buffer<uint8_t>& selection, size_t from, size_t to) const;

    ChunkExtraColumnsDataPtr clone_empty(size_t size) const;

    void append(const ChunkExtraColumnsData& src, size_t offset, size_t count);
    void append_selective(const ChunkExtraColumnsData& src, const uint32_t* indexes, uint32_t from, uint32_t size);

    size_t memory_usage() const;
    size_t bytes_usage(size_t from, size_t size) const;

    int64_t max_serialized_size(const int encode_level = 0);
    uint8_t* serialize(uint8_t* buff, bool sorted = false, const int encode_level = 0);
    const uint8_t* deserialize(const uint8_t* buff, bool sorted = false, const int encode_level = 0);

    static ChunkExtraColumnsData* as_raw(const ChunkExtraDataPtr& extra_data);

private:
    std::vector<ChunkExtraColumnsMeta> _data_metas;
    Columns _columns;
};
} // namespace starrocks