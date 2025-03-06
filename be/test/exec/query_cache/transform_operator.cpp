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

#include "exec/query_cache/transform_operator.h"

#include <utility>
namespace starrocks {
MapOperator::MapOperator(pipeline::OperatorFactory* factory, int driver_sequence, MapFunc map_func)
        : pipeline::Operator(factory, factory->id(), factory->get_raw_name(), factory->plan_node_id(), false,
                             driver_sequence),
          _map_func(std::move(map_func)) {}
bool MapOperator::has_output() const {
    return _cur_chunk != nullptr;
}

bool MapOperator::need_input() const {
    return !_is_finished && _cur_chunk == nullptr;
}

bool MapOperator::is_finished() const {
    return _is_finished && _cur_chunk == nullptr;
}

Status MapOperator::set_finishing(starrocks::RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

Status MapOperator::set_finished(starrocks::RuntimeState* state) {
    _is_finished = true;
    _cur_chunk = nullptr;
    return Status::OK();
}

Status MapOperator::reset_state(RuntimeState* state, const std::vector<ChunkPtr>& chunks) {
    _is_finished = false;
    _cur_chunk = nullptr;
    return Status::OK();
}

StatusOr<ChunkPtr> MapOperator::pull_chunk(starrocks::RuntimeState* state) {
    DCHECK(_cur_chunk != nullptr);
    return std::move(_cur_chunk);
}

Status MapOperator::push_chunk(starrocks::RuntimeState* state, const ChunkPtr& chunk) {
    DCHECK(_cur_chunk == nullptr);
    DCHECK(chunk != nullptr && !chunk->is_empty());
    DCHECK(chunk->num_columns() == 1);
    auto column = chunk->get_column_by_slot_id(SlotId(1));
    auto* col = dynamic_cast<DoubleColumn*>(column.get());
    DCHECK(col != nullptr);
    auto num_rows = col->size();
    auto& data = col->get_data();
    auto new_column = DoubleColumn::create();
    new_column->resize(num_rows);
    auto& new_data = dynamic_cast<DoubleColumn*>(new_column.get())->get_data();
    for (auto i = 0; i < num_rows; ++i) {
        new_data[i] = _map_func(data[i]);
    }
    auto new_chunk = std::make_shared<Chunk>();
    new_chunk->append_column(std::move(new_column), SlotId(1));
    _cur_chunk = std::move(new_chunk);
    return Status::OK();
}

MapOperatorFactory::MapOperatorFactory(int operator_id, MapFunc map_func)
        : pipeline::OperatorFactory(operator_id, "map", operator_id), _map_func(std::move(map_func)) {}

pipeline::OperatorPtr MapOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<MapOperator>(this, driver_sequence, _map_func);
}

Reducer::Reducer(double init_value, ReduceFunc reduce_func, size_t output_num_rows)
        : _init_value(init_value), _reduce_func(std::move(reduce_func)), _output_num_rows(output_num_rows) {}

ReducerFactory::ReducerFactory(double init_value, ReduceFunc reduce_func, size_t output_num_rows)
        : _init_value(init_value), _reduce_func(std::move(reduce_func)), _output_num_rows(output_num_rows) {}

ReducerPtr ReducerFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    if (_reducers.empty()) {
        _reducers.resize(degree_of_parallelism);
    }
    if (_reducers[driver_sequence] != nullptr) {
        return _reducers[driver_sequence];
    }
    _reducers[driver_sequence] = std::make_shared<Reducer>(_init_value, _reduce_func, _output_num_rows);
    return _reducers[driver_sequence];
}

ReduceSinkOperator::ReduceSinkOperator(pipeline::OperatorFactory* factory, int32_t driver_sequence,
                                       starrocks::ReducerRawPtr reducer)
        : pipeline::Operator(factory, factory->id(), "reduce_sink", factory->id(), false, driver_sequence),
          _result(reducer->init_value()),
          _reducer(reducer) {}
bool ReduceSinkOperator::is_finished() const {
    return _reducer->is_sink_finished();
}
bool ReduceSinkOperator::need_input() const {
    return !is_finished();
}

Status ReduceSinkOperator::push_chunk(starrocks::RuntimeState* state, const ChunkPtr& chunk) {
    DCHECK(chunk != nullptr && !chunk->is_empty());
    auto column = chunk->get_column_by_slot_id(SlotId(1));
    const auto* col = dynamic_cast<const DoubleColumn*>(column.get());
    DCHECK(col != nullptr);
    const auto& data = col->get_data();
    const auto num_rows = data.size();
    auto reduce_func = _reducer->reduce_func();
    for (auto i = 0; i < num_rows; ++i) {
        _result = reduce_func(_result, data[i]);
    }
    return Status::OK();
}

Status ReduceSinkOperator::set_finishing(starrocks::RuntimeState* state) {
    _reducer->set_result(_result);
    _reducer->set_sink_finished();
    return Status::OK();
}

Status ReduceSinkOperator::reset_state(RuntimeState* state, const std::vector<ChunkPtr>& chunks) {
    _result = _reducer->init_value();
    return _reducer->reset_state(state, chunks, this);
}

Status Reducer::reset_state(RuntimeState* state, const Chunks& chunks, pipeline::Operator* op) {
    _sink_is_finished = false;
    _source_is_finished = false;
    for (const auto& chunk : chunks) {
        RETURN_IF_ERROR(op->push_chunk(state, chunk));
    }
    return Status::OK();
}

ReduceSinkOperatorFactory::ReduceSinkOperatorFactory(int32_t id, ReducerFactoryRawPtr reducer_factory)
        : pipeline::OperatorFactory(id, "reduce_sink", id), _reducer_factory(reducer_factory) {}

pipeline::OperatorPtr ReduceSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto reducer = _reducer_factory->create(degree_of_parallelism, driver_sequence);
    return std::make_shared<ReduceSinkOperator>(this, driver_sequence, reducer.get());
}

ReduceSourceOperator::ReduceSourceOperator(pipeline::OperatorFactory* factory, int32_t driver_sequence,
                                           starrocks::ReducerPtr reducer)
        : pipeline::SourceOperator(factory, factory->id(), factory->get_raw_name(), factory->id(), false,
                                   driver_sequence),
          _reducer(std::move(reducer)) {}

bool ReduceSourceOperator::is_finished() const {
    return _reducer->is_source_finished();
}

bool ReduceSourceOperator::has_output() const {
    return _reducer->is_sink_finished() && !_reducer->is_source_finished();
}

Status ReduceSourceOperator::set_finishing(starrocks::RuntimeState* state) {
    _reducer->set_source_finished();
    return Status::OK();
}

Status ReduceSourceOperator::set_finished(starrocks::RuntimeState* state) {
    _reducer->set_source_finished();
    _reducer->set_sink_finished();
    return Status::OK();
}

Status ReduceSourceOperator::reset_state(RuntimeState* state, const std::vector<ChunkPtr>& chunks) {
    _current_output_num_rows = 0;
    return Status::OK();
}

StatusOr<ChunkPtr> ReduceSourceOperator::pull_chunk(starrocks::RuntimeState* state) {
    const auto output_num_rows_total = _reducer->output_num_rows();
    static constexpr auto CHUNK_SIZE = 4;
    const auto num_rows = std::min<size_t>(CHUNK_SIZE, (output_num_rows_total - _current_output_num_rows));
    _current_output_num_rows += num_rows;
    auto column = DoubleColumn::create();
    column->resize(num_rows);
    auto& data = dynamic_cast<DoubleColumn*>(column.get())->get_data();
    data.assign(num_rows, _reducer->result());
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(column), SlotId(1));
    if (_current_output_num_rows == _reducer->output_num_rows()) {
        _reducer->set_source_finished();
    }
    return chunk;
}

ReduceSourceOperatorFactory::ReduceSourceOperatorFactory(int32_t id, starrocks::ReducerFactoryPtr reducer_factory)
        : pipeline::SourceOperatorFactory(id, "reduce_source", id), _reducer_factory(std::move(reducer_factory)) {}

pipeline::OperatorPtr ReduceSourceOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto reducer = _reducer_factory->create(degree_of_parallelism, driver_sequence);
    return std::make_shared<ReduceSourceOperator>(this, driver_sequence, reducer);
}

} // namespace starrocks
