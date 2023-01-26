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

#include <memory>
#include <vector>

#include "column/schema.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "segment_options.h"
#include "storage/chunk_iterator.h"
#include "storage/convert_helper.h"
#include "storage/tablet_schema.h"
#include "util/runtime_profile.h"

namespace starrocks {

class SegmentChunkIteratorAdapter final : public ChunkIterator {
public:
    // |schema| is the output fields.
    explicit SegmentChunkIteratorAdapter(const TabletSchema& tablet_schema, const std::vector<LogicalType>& new_types,
                                         const Schema& out_schema, int chunk_size);

    ~SegmentChunkIteratorAdapter() override = default;

    Status prepare(const SegmentReadOptions& options);

    void close() override;

    const Schema& in_schema() const { return _in_schema; }
    const SegmentReadOptions& in_read_options() const { return _in_read_options; };

    void set_iterator(std::shared_ptr<ChunkIterator> iterator) { _inner_iter = std::move(iterator); }

    Status init_encoded_schema(ColumnIdToGlobalDictMap& dict_maps) override {
        _inner_iter->init_encoded_schema(dict_maps);
        ChunkIterator::init_encoded_schema(dict_maps);
        return Status::OK();
    }
    Status init_output_schema(const std::unordered_set<uint32_t>& unused_output_column_ids) override {
        _inner_iter->init_output_schema(unused_output_column_ids);
        ChunkIterator::init_output_schema(unused_output_column_ids);
        return Status::OK();
    }

protected:
    Status do_get_next(Chunk* chunk) override;
    Status do_get_next(Chunk* chunk, std::vector<uint32_t>* rowid) override;

    const TabletSchema& _tablet_schema;
    const std::vector<LogicalType>& _new_types;

    Schema _in_schema;
    SegmentReadOptions _in_read_options;
    ObjectPool _obj_pool;

    std::shared_ptr<ChunkIterator> _inner_iter;

    ChunkConverter _converter;
    int64_t _convert_time;
    RuntimeProfile::Counter* _convert_timer;

    ChunkPtr _in_chunk;
};

} // namespace starrocks
