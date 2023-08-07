

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

#include "column/chunk_extra_data.h"

namespace starrocks {
void ChunkExtraColumnsData::filter(const Buffer<uint8_t>& selection) const {
    for (auto& col : _columns) {
        col->filter(selection);
    }
}

void ChunkExtraColumnsData::filter_range(const Buffer<uint8_t>& selection, size_t from, size_t to) const {
    for (auto& col : _columns) {
        col->filter_range(selection, from, to);
    }
}

ChunkExtraColumnsDataPtr ChunkExtraColumnsData::clone_empty(size_t size) const {
    Columns columns(_columns.size());
    for (size_t i = 0; i < _columns.size(); i++) {
        columns[i] = _columns[i]->clone_empty();
        columns[i]->reserve(size);
    }
    auto extra_data_metas = _data_metas;
    return std::make_shared<ChunkExtraColumnsData>(extra_data_metas, columns);
}

void ChunkExtraColumnsData::append(const ChunkExtraColumnsData& src, size_t offset, size_t count) {
    auto src_columns = src.columns();
    DCHECK_EQ(src_columns.size(), _columns.size());
    for (size_t i = 0; i < _columns.size(); ++i) {
        _columns[i]->append(*src_columns[i], offset, count);
    }
}

void ChunkExtraColumnsData::append_selective(const ChunkExtraColumnsData& src, const uint32_t* indexes, uint32_t from,
                                             uint32_t size) {
    auto src_columns = src.columns();
    DCHECK_EQ(src_columns.size(), _columns.size());
    for (size_t i = 0; i < _columns.size(); ++i) {
        _columns[i]->append_selective(*src_columns[i], indexes, from, size);
    }
}

size_t ChunkExtraColumnsData::memory_usage() const {
    size_t memory_usage = 0;
    for (const auto& column : _columns) {
        memory_usage += column->memory_usage();
    }
    return memory_usage;
}

size_t ChunkExtraColumnsData::bytes_usage(size_t from, size_t size) const {
    size_t bytes_usage = 0;
    for (const auto& column : _columns) {
        bytes_usage += column->byte_size(from, size);
    }
    return bytes_usage;
}

int64_t ChunkExtraColumnsData::max_serialized_size(const int encode_level) {
    DCHECK_EQ(encode_level, 0);
    int64_t serialized_size = 0;
    for (auto& column : _columns) {
        serialized_size += serde::ColumnArraySerde::max_serialized_size(*column, 0);
    }
    return serialized_size;
}

uint8_t* ChunkExtraColumnsData::serialize(uint8_t* buff, bool sorted, const int encode_level) {
    DCHECK_EQ(encode_level, 0);
    for (auto& column : _columns) {
        buff = serde::ColumnArraySerde::serialize(*column, buff, sorted, encode_level);
    }
    return buff;
}

const uint8_t* ChunkExtraColumnsData::deserialize(const uint8_t* buff, bool sorted, const int encode_level) {
    DCHECK_EQ(encode_level, 0);
    for (auto& column : _columns) {
        buff = serde::ColumnArraySerde::deserialize(buff, column.get(), sorted, encode_level);
    }
    return buff;
}

ChunkExtraColumnsData* ChunkExtraColumnsData::as_raw(const ChunkExtraDataPtr& extra_data) {
    return extra_data ? down_cast<ChunkExtraColumnsData*>(extra_data.get()) : nullptr;
}

} // namespace starrocks