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

#include "storage/primitive/union_iterator.h"

#include <memory>

#include "column/chunk.h"

namespace starrocks {

class UnionIterator final : public ChunkIterator {
public:
    explicit UnionIterator(std::vector<ChunkIteratorPtr> children)
            : ChunkIterator(children[0]->schema(), children[0]->chunk_size()), _children(std::move(children)) {
#ifndef NDEBUG
        for (auto& iter : _children) {
            const Schema& child_schema = iter->schema();
            CHECK_EQ(_schema.num_fields(), child_schema.num_fields());
            for (int i = 0; i < _schema.num_fields(); i++) {
                CHECK_EQ(_schema.field(i)->to_string(), child_schema.field(i)->to_string());
            }
        }
#endif
    }

    ~UnionIterator() override = default;

    void close() override;

    size_t merged_rows() const override { return _merged_rows; }

    Status init_encoded_schema(ColumnIdToGlobalDictMap& dict_maps) override {
        RETURN_IF_ERROR(ChunkIterator::init_encoded_schema(dict_maps));
        for (auto& child : _children) {
            RETURN_IF_ERROR(child->init_encoded_schema(dict_maps));
        }
        return Status::OK();
    }

    Status init_output_schema(const std::unordered_set<uint32_t>& unused_output_column_ids) override {
        RETURN_IF_ERROR(ChunkIterator::init_output_schema(unused_output_column_ids));
        for (auto& child : _children) {
            RETURN_IF_ERROR(child->init_output_schema(unused_output_column_ids));
        }
        return Status::OK();
    }

protected:
    Status do_get_next(Chunk* chunk) override;
    Status do_get_next(Chunk* chunk, std::vector<uint32_t>* rowid) override;
    Status do_get_next(Chunk* chunk, std::vector<uint64_t>* rssid_rowids) override;
    // Union Iterator will read data in order of segment and we don't need to record the read segment record
    // Add this function for compatibility
    Status do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) override { return do_get_next(chunk); }

private:
    std::vector<ChunkIteratorPtr> _children;
    size_t _cur_idx = 0;
    size_t _merged_rows = 0;
    // Scratch chunk used to coalesce sparse child output (see do_get_next).
    ChunkUniquePtr _coalesce_tmp;
};

inline Status UnionIterator::do_get_next(Chunk* chunk) {
    // Coalesce rows across the non-overlapping child segments into a full chunk
    // before returning. A child segment iterator asserts the output chunk is
    // empty and builds into it, so we pull each child into a scratch chunk and
    // swap (zero-copy) when the output is still empty, only paying an append
    // copy when topping a partial chunk up from a later segment.
    //
    // Without this, a segment that yields only a handful of rows -- e.g. a
    // scattered secondary-index readback where each base segment matches a few
    // rowids -- emits a tiny partial chunk, forcing one downstream
    // (aggregation/scheduling) invocation per segment. On sparse scans this
    // collapses thousands of tiny chunks into a few full ones; on dense scans
    // the first child already fills the chunk so it stays a single swap.
    const auto target_rows = static_cast<size_t>(chunk_size());
    while (chunk->num_rows() < target_rows && _cur_idx < _children.size()) {
        if (_coalesce_tmp == nullptr) {
            _coalesce_tmp = chunk->clone_empty();
        }
        _coalesce_tmp->reset();
        Status res = _children[_cur_idx]->get_next(_coalesce_tmp.get());
        if (res.is_end_of_file()) {
            _merged_rows += _children[_cur_idx]->merged_rows();
            _children[_cur_idx]->close();
            _children[_cur_idx].reset();
            _cur_idx++;
            continue;
        }
        if (!res.ok()) {
            return res;
        }
        if (chunk->num_rows() == 0) {
            chunk->swap_chunk(*_coalesce_tmp);
        } else {
            chunk->append(*_coalesce_tmp);
        }
    }
    if (chunk->num_rows() > 0) {
        return Status::OK();
    }
    return Status::EndOfFile("End of union iterator");
}

inline Status UnionIterator::do_get_next(Chunk* chunk, std::vector<uint32_t>* rowid) {
    while (_cur_idx < _children.size()) {
        Status res = _children[_cur_idx]->get_next(chunk, rowid);
        if (res.is_end_of_file()) {
            _merged_rows += _children[_cur_idx]->merged_rows();
            _children[_cur_idx]->close();
            _children[_cur_idx].reset();
            _cur_idx++;
            continue;
        }
        return res;
    }
    return Status::EndOfFile("End of union iterator");
}

inline Status UnionIterator::do_get_next(Chunk* chunk, std::vector<uint64_t>* rssid_rowids) {
    while (_cur_idx < _children.size()) {
        Status res = _children[_cur_idx]->get_next(chunk, rssid_rowids);
        if (res.is_end_of_file()) {
            _merged_rows += _children[_cur_idx]->merged_rows();
            _children[_cur_idx]->close();
            _children[_cur_idx].reset();
            _cur_idx++;
            continue;
        }
        return res;
    }
    return Status::EndOfFile("End of union iterator");
}

inline void UnionIterator::close() {
    for (auto& ptr : _children) {
        if (ptr != nullptr) {
            ptr->close();
        }
    }
    _children.clear();
}

ChunkIteratorPtr new_union_iterator(std::vector<ChunkIteratorPtr> children) {
    DCHECK(!children.empty());
    if (children.size() == 1) {
        return children[0];
    }
    return std::make_shared<UnionIterator>(std::move(children));
}

} // namespace starrocks
