// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

namespace starrocks::vectorized {

class SegmentChunkIteratorAdapter final : public ChunkIterator {
public:
    // |schema| is the output fields.
    explicit SegmentChunkIteratorAdapter(const TabletSchema& tablet_schema, const std::vector<FieldType>& new_types,
                                         const Schema& out_schema, int chunk_size);

    ~SegmentChunkIteratorAdapter() override = default;

    Status prepare(const SegmentReadOptions& options);

    void close() override;

    const Schema& in_schema() const { return _in_schema; }
    const SegmentReadOptions& in_read_options() const { return _in_read_options; };

    void set_iterator(std::shared_ptr<ChunkIterator> iterator) { _inner_iter = std::move(iterator); }

    virtual Status init_encoded_schema(ColumnIdToGlobalDictMap& dict_maps) {
        _inner_iter->init_encoded_schema(dict_maps);
        ChunkIterator::init_encoded_schema(dict_maps);
        return Status::OK();
    }
    virtual Status init_output_schema(const std::unordered_set<uint32_t>& unused_output_column_ids) override {
        _inner_iter->init_output_schema(unused_output_column_ids);
        ChunkIterator::init_output_schema(unused_output_column_ids);
        return Status::OK();
    }

protected:
    Status do_get_next(Chunk* chunk) override;
    Status do_get_next(Chunk* chunk, vector<uint32_t>* rowid) override;

    const TabletSchema& _tablet_schema;
    const std::vector<FieldType>& _new_types;

    Schema _in_schema;
    SegmentReadOptions _in_read_options;
    ObjectPool _obj_pool;

    std::shared_ptr<ChunkIterator> _inner_iter;

    ChunkConverter _converter;
    int64_t _convert_time;
    RuntimeProfile::Counter* _convert_timer;

    ChunkPtr _in_chunk;
};

} // namespace starrocks::vectorized
