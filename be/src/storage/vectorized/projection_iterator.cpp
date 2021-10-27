// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/projection_iterator.h"

#include <unordered_set>

#include "column/chunk.h"
#include "glog/logging.h"
#include "runtime/current_mem_tracker.h"
#include "storage/vectorized/chunk_helper.h"

namespace starrocks::vectorized {

class ProjectionIterator final : public ChunkIterator {
public:
    ProjectionIterator(Schema schema, ChunkIteratorPtr child)
            : ChunkIterator(std::move(schema), child->chunk_size()), _child(std::move(child)), _output_schema(_schema) {
        build_index_map(this->_schema, _child->schema());
    }

    void close() override;

    size_t merged_rows() const override { return _child->merged_rows(); }

    virtual Status init_encoded_schema(ColumnIdToGlobalDictMap& dict_maps) override {
        ChunkIterator::init_encoded_schema(dict_maps);
        return _child->init_encoded_schema(dict_maps);
    }

protected:
    Status do_get_next(Chunk* chunk) override;

private:
    void build_index_map(const Schema& output, const Schema& input);

    ChunkIteratorPtr _child;
    Schema _output_schema;
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
        DCHECK_GT(_child->encoded_schema().num_fields(), 0);
        _chunk = ChunkHelper::new_chunk(_child->encoded_schema(), _chunk_size);
        CurrentMemTracker::consume(_chunk->memory_usage());
    }
    _chunk->reset();
    Status st = _child->get_next(_chunk.get());
    if (st.ok()) {
        Columns& input_columns = _chunk->columns();
        for (size_t i = 0; i < _index_map.size(); i++) {
            chunk->get_column_by_index(i).swap(input_columns[_index_map[i]]);
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
        CurrentMemTracker::release(_chunk->memory_usage());
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

} // namespace starrocks::vectorized
