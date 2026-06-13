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

#include "storage/primitive/projection_iterator.h"

#include <unordered_set>

#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "runtime/chunk_helper.h"

namespace starrocks {

class ProjectionIterator final : public ChunkIterator {
public:
    ProjectionIterator(Schema schema, ChunkIteratorPtr child)
            : ChunkIterator(std::move(schema), child->chunk_size()), _child(std::move(child)) {
        build_index_map(this->_schema, _child->schema());
    }

    void close() override;

    size_t merged_rows() const override { return _child->merged_rows(); }

    Status init_encoded_schema(ColumnIdToGlobalDictMap& dict_maps) override {
        RETURN_IF_ERROR(ChunkIterator::init_encoded_schema(dict_maps));
        return _child->init_encoded_schema(dict_maps);
    }

    Status init_output_schema(const std::unordered_set<uint32_t>& unused_output_column_ids) override {
        RETURN_IF_ERROR(ChunkIterator::init_output_schema(unused_output_column_ids));
        RETURN_IF_ERROR(_child->init_output_schema(unused_output_column_ids));
        _index_map.clear();
        build_index_map(this->_output_schema, _child->output_schema());
        return Status::OK();
    }

protected:
    Status do_get_next(Chunk* chunk) override;

private:
    void build_index_map(const Schema& output, const Schema& input);

    ChunkIteratorPtr _child;
    // mapping from index of column in output chunk to index of column in input chunk.
    std::vector<size_t> _index_map;
    ChunkPtr _chunk;
};

void ProjectionIterator::build_index_map(const Schema& output, const Schema& input) {
    DCHECK_LE(output.num_fields(), input.num_fields());

    std::unordered_map<ColumnId, size_t> input_indexes;
    for (size_t i = 0; i < input.num_fields(); i++) {
        input_indexes[input.field(i)->id()] = i;
    }

    _index_map.resize(output.num_fields());
    for (size_t i = 0; i < output.num_fields(); i++) {
        DCHECK(input_indexes.count(output.field(i)->id()) > 0);
        _index_map[i] = input_indexes[output.field(i)->id()];
    }
}

Status ProjectionIterator::do_get_next(Chunk* chunk) {
    if (_chunk == nullptr) {
        DCHECK_GT(_child->output_schema().num_fields(), 0);
        ASSIGN_OR_RETURN(_chunk, RuntimeChunkHelper::new_chunk_checked(_child->output_schema(), _chunk_size));
    }
    _chunk->reset();
    Status st = _child->get_next(_chunk.get());
    if (st.ok()) {
        Columns& input_columns = _chunk->columns();
        for (size_t i = 0; i < _index_map.size(); i++) {
            chunk->get_column_by_index(i).swap(input_columns[_index_map[i]]);
        }
        // The child may append columns at runtime beyond its static output schema -- the
        // vector distance column appended by SegmentIterator::_read. _index_map is built
        // from the static schemas, so those columns are not copied above and would be lost.
        // Re-attach them (by their slot id, falling back to field id) so downstream slot/name
        // lookups in the scan find them.
        const size_t static_n = _child->output_schema().num_fields();
        for (size_t k = static_n; k < _chunk->num_columns(); k++) {
            const FieldPtr& f = _chunk->schema()->field(k);
            SlotId sid = -1;
            for (const auto& [s, idx] : _chunk->get_slot_id_to_index_map()) {
                if (idx == k) {
                    sid = s;
                    break;
                }
            }
            if (chunk->is_cid_exist(f->id())) {
                // Output chunk reused across calls: swap the data into the existing column.
                // Swap (not move) leaves a valid column in _chunk for the next reset().
                size_t existing = chunk->get_column_id_to_index_map().at(f->id());
                chunk->get_column_by_index(existing).swap(input_columns[k]);
            } else {
                // Fresh output column: move the filled column in (so row counts stay
                // consistent for check_or_die), then leave an empty clone in _chunk so the
                // next _chunk->reset() does not dereference a moved-from (null) ColumnPtr.
                ColumnPtr empty = input_columns[k]->clone_empty();
                if (sid >= 0) {
                    chunk->append_vector_column(std::move(input_columns[k]), f, sid);
                } else {
                    chunk->append_column(std::move(input_columns[k]), f);
                }
                input_columns[k] = std::move(empty);
            }
        }
    }
#ifndef NDEBUG
    if (st.ok()) {
        CHECK(chunk->num_rows() > 0);
    }
#endif
    return st;
}

void ProjectionIterator::close() {
    if (_chunk != nullptr) {
        _chunk.reset();
    }
    if (_child != nullptr) {
        _child->close();
        _child.reset();
    }
}

ChunkIteratorPtr new_projection_iterator(const Schema& schema, const ChunkIteratorPtr& child) {
    return std::make_shared<ProjectionIterator>(schema, child);
}

} // namespace starrocks
