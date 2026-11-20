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

#include "storage_primitive/union_iterator.h"

#include <algorithm>
#include <memory>

#include "column/chunk.h"

namespace starrocks {

class UnionIterator final : public ChunkIterator {
public:
    explicit UnionIterator(std::vector<ChunkIteratorPtr> children, size_t read_ahead_children = 0,
                           size_t read_ahead_blocks = 0)
            : ChunkIterator(children[0]->schema(), children[0]->chunk_size()),
              _children(std::move(children)),
              _read_ahead_children(read_ahead_children),
              _read_ahead_budget(read_ahead_blocks) {
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

    // A union read ahead as a whole (nested in another union) reads ahead its current child.
    Status start_read_ahead(bool* started) override {
        if (_cur_idx >= _children.size()) {
            *started = true;
            return Status::OK();
        }
        return _children[_cur_idx]->start_read_ahead(started);
    }
    size_t read_ahead_blocks() const override {
        return _cur_idx < _children.size() ? _children[_cur_idx]->read_ahead_blocks() : 0;
    }
    bool read_ahead_done() const override {
        return _cur_idx >= _children.size() ||
               (_cur_idx + 1 == _children.size() && _children[_cur_idx]->read_ahead_done());
    }

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
    // Starts the read-ahead of the children after the current one, see new_union_iterator().
    void _read_ahead();

    std::vector<ChunkIteratorPtr> _children;
    size_t _cur_idx = 0;
    size_t _merged_rows = 0;
    size_t _read_ahead_children;
    const size_t _read_ahead_budget;
    // One past the last child whose read-ahead has started.
    size_t _read_ahead_end = 0;
};

inline void UnionIterator::_read_ahead() {
    if (_read_ahead_children == 0 || _cur_idx >= _children.size()) {
        return;
    }
    // A child starts only once every child before it has asked for everything it will read, and only while
    // the blocks they all hold ahead stay under the budget.
    _read_ahead_end = std::max(_read_ahead_end, _cur_idx + 1);
    size_t held = 0;
    for (size_t i = _cur_idx; i < _read_ahead_end; ++i) {
        if (!_children[i]->read_ahead_done()) {
            return;
        }
        held += _children[i]->read_ahead_blocks();
    }
    while (_read_ahead_end < _children.size() && _read_ahead_end <= _cur_idx + _read_ahead_children &&
           held < _read_ahead_budget) {
        auto& child = _children[_read_ahead_end++];
        bool started = false;
        Status st = child->start_read_ahead(&started);
        if (!st.ok() || !started) {
            // The child reports the error again when its turn comes; without a read-ahead of its own,
            // there is nothing to gain from preparing the others early.
            _read_ahead_children = 0;
            return;
        }
        held += child->read_ahead_blocks();
        if (!child->read_ahead_done()) {
            return;
        }
    }
}

inline Status UnionIterator::do_get_next(Chunk* chunk) {
    while (_cur_idx < _children.size()) {
        Status res = _children[_cur_idx]->get_next(chunk);
        if (res.ok()) {
            _read_ahead();
        }
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

inline Status UnionIterator::do_get_next(Chunk* chunk, std::vector<uint32_t>* rowid) {
    while (_cur_idx < _children.size()) {
        Status res = _children[_cur_idx]->get_next(chunk, rowid);
        if (res.ok()) {
            _read_ahead();
        }
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
        if (res.ok()) {
            _read_ahead();
        }
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

ChunkIteratorPtr new_union_iterator(std::vector<ChunkIteratorPtr> children, size_t read_ahead_children,
                                    size_t read_ahead_blocks) {
    DCHECK(!children.empty());
    if (children.size() == 1) {
        return std::move(children[0]);
    }
    return std::make_shared<UnionIterator>(std::move(children), read_ahead_children, read_ahead_blocks);
}

ChunkIteratorPtr new_union_iterator(std::vector<ChunkIteratorPtr> children) {
    DCHECK(!children.empty());
    if (children.size() == 1) {
        return children[0];
    }
    return std::make_shared<UnionIterator>(std::move(children));
}

} // namespace starrocks
