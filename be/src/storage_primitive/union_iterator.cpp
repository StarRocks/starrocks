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
#include "common/config_rowset_fwd.h"

namespace starrocks {

class UnionIterator final : public ChunkIterator {
public:
    explicit UnionIterator(std::vector<ChunkIteratorPtr> children)
            : ChunkIterator(children[0]->schema(), children[0]->chunk_size()),
              _children(std::move(children)),
              _prepared(_children.size(), false),
              _prepare_statuses(_children.size(), Status::OK()) {
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
    Status _prepare_window();

    std::vector<ChunkIteratorPtr> _children;
    std::vector<bool> _prepared;
    std::vector<Status> _prepare_statuses;
    size_t _cur_idx = 0;
    size_t _prepared_until = 0;
    size_t _merged_rows = 0;
};

inline Status UnionIterator::_prepare_window() {
    if (config::segment_iterator_lookahead <= 0 || _cur_idx >= _children.size()) {
        return Status::OK();
    }
    const size_t window = static_cast<size_t>(std::clamp(config::segment_iterator_lookahead, 2, 4));

    auto prepare_child = [&](size_t idx) -> const Status& {
        if (!_prepared[idx]) {
            _prepare_statuses[idx] = _children[idx]->prepare_for_read();
            _prepared[idx] = true;
        }
        return _prepare_statuses[idx];
    };

    // A future child's error is stored and surfaced only when that child becomes current,
    // after valid rows from the preceding children have been returned.
    RETURN_IF_ERROR(prepare_child(_cur_idx));

    const size_t end = std::min(_children.size(), _cur_idx + window);
    _prepared_until = std::max(_prepared_until, _cur_idx + 1);
    while (_prepared_until < end) {
        (void)prepare_child(_prepared_until);
        ++_prepared_until;
    }
    return Status::OK();
}

inline Status UnionIterator::do_get_next(Chunk* chunk) {
    while (_cur_idx < _children.size()) {
        Status prepared = _prepare_window();
        if (prepared.is_end_of_file()) {
            _merged_rows += _children[_cur_idx]->merged_rows();
            _children[_cur_idx]->close();
            _children[_cur_idx].reset();
            ++_cur_idx;
            continue;
        }
        RETURN_IF_ERROR(prepared);
        Status res = _children[_cur_idx]->get_next(chunk);
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
        Status prepared = _prepare_window();
        if (prepared.is_end_of_file()) {
            _merged_rows += _children[_cur_idx]->merged_rows();
            _children[_cur_idx]->close();
            _children[_cur_idx].reset();
            ++_cur_idx;
            continue;
        }
        RETURN_IF_ERROR(prepared);
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
        Status prepared = _prepare_window();
        if (prepared.is_end_of_file()) {
            _merged_rows += _children[_cur_idx]->merged_rows();
            _children[_cur_idx]->close();
            _children[_cur_idx].reset();
            ++_cur_idx;
            continue;
        }
        RETURN_IF_ERROR(prepared);
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
