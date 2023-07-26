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

#include "storage/chunk_iterator.h"

namespace starrocks {

class TimedChunkIterator final : public ChunkIterator {
public:
    TimedChunkIterator(ChunkIteratorPtr iter, RuntimeProfile::Counter* counter)
            : ChunkIterator(iter->schema(), iter->chunk_size()), _iter(std::move(iter)), _counter(counter) {}

    ~TimedChunkIterator() override = default;

    void close() override {
        COUNTER_UPDATE(_counter, _cost);
        _iter->close();
        _iter.reset();
    }

    size_t merged_rows() const override { return _iter->merged_rows(); }

    Status init_encoded_schema(ColumnIdToGlobalDictMap& dict_maps) override {
        ChunkIterator::init_encoded_schema(dict_maps);
        _iter->init_encoded_schema(dict_maps);
        return Status::OK();
    }

private:
    Status do_get_next(Chunk* chunk) override {
        SCOPED_RAW_TIMER(&_cost);
        return _iter->get_next(chunk);
    }

    Status do_get_next(Chunk* chunk, std::vector<uint32_t>* rowid) override {
        SCOPED_RAW_TIMER(&_cost);
        return _iter->get_next(chunk, rowid);
    }

    Status do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) override {
        SCOPED_RAW_TIMER(&_cost);
        return _iter->get_next(chunk, source_masks);
    }

    ChunkIteratorPtr _iter;
    int64_t _cost{0};
    RuntimeProfile::Counter* _counter;
};

ChunkIteratorPtr timed_chunk_iterator(const ChunkIteratorPtr& iter, RuntimeProfile::Counter* counter) {
    return std::make_shared<TimedChunkIterator>(iter, counter);
}

} // namespace starrocks
