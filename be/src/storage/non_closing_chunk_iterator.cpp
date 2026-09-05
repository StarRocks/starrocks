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

#include "storage/non_closing_chunk_iterator.h"

#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

namespace starrocks {

class NonClosingChunkIterator final : public ChunkIterator {
public:
    explicit NonClosingChunkIterator(ChunkIteratorPtr child)
            : ChunkIterator(child->schema(), child->chunk_size()), _child(std::move(child)) {}

    void close() override {}

    size_t merged_rows() const override { return _child->merged_rows(); }

    Status init_encoded_schema(ColumnIdToGlobalDictMap& dict_maps) override {
        RETURN_IF_ERROR(ChunkIterator::init_encoded_schema(dict_maps));
        return _child->init_encoded_schema(dict_maps);
    }

    Status init_output_schema(const std::unordered_set<uint32_t>& unused_output_column_ids) override {
        RETURN_IF_ERROR(ChunkIterator::init_output_schema(unused_output_column_ids));
        return _child->init_output_schema(unused_output_column_ids);
    }

protected:
    Status do_get_next(Chunk* chunk) override { return _child->get_next(chunk); }
    Status do_get_next(Chunk* chunk, std::vector<uint32_t>* rowid) override { return _child->get_next(chunk, rowid); }
    Status do_get_next(Chunk* chunk, std::vector<uint64_t>* rssid_rowids) override {
        return _child->get_next(chunk, rssid_rowids);
    }

private:
    ChunkIteratorPtr _child;
};

ChunkIteratorPtr new_non_closing_chunk_iterator(const ChunkIteratorPtr& child) {
    return std::make_shared<NonClosingChunkIterator>(child);
}

} // namespace starrocks
