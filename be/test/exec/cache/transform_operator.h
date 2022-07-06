// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once
#include <any>

#include "column/chunk.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks::vectorized {
using MapFunc = std::function<double(double)>;
using ReduceFunc = std::function<double(double, double)>;

class MapOperator final : public pipeline::Operator {
public:
    MapOperator(pipeline::OperatorFactory* factory, int driver_sequence, MapFunc map_func);
    ~MapOperator() = default;
    Status push_chunk(starrocks::RuntimeState* state, const vectorized::ChunkPtr& chunk) override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(starrocks::RuntimeState* state) override;
    bool is_finished() const override;
    bool has_output() const override;
    bool need_input() const override;
    Status set_finished(starrocks::RuntimeState* state) override;
    Status set_finishing(starrocks::RuntimeState* state) override;
    Status reset_state(std::vector<ChunkPtr>&& chunks) override;

private:
    MapFunc _map_func;
    bool _is_finished = false;
    ChunkPtr _cur_chunk;
};

class MapOperatorFactory final : public pipeline::OperatorFactory {
public:
    explicit MapOperatorFactory(int operator_id, std::function<double(double)> map_func);
    ~MapOperatorFactory() = default;
    pipeline::OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    MapFunc _map_func;
};
class Reducer;
using ReducerRawPtr = Reducer*;
using ReducerPtr = std::shared_ptr<Reducer>;
using Reducers = std::vector<ReducerPtr>;

class Reducer final : public pipeline::ContextWithDependency {
public:
    Reducer(double init_value, ReduceFunc reduce_func, size_t output_num_rows);
    ~Reducer() = default;
    void close(starrocks::RuntimeState* state) override{};
    Status reset_state() {
        _sink_is_finished = false;
        _source_is_finished = false;
        return Status::OK();
    }

    void set_result(double result) { _result = result; }
    double result() const { return _result; }
    size_t output_num_rows() const { return _output_num_rows; }
    void set_sink_finished() { _sink_is_finished = true; }
    void set_source_finished() { _source_is_finished = true; }
    bool is_sink_finished() const { return _sink_is_finished; }
    bool is_source_finished() const { return _source_is_finished; }
    double init_value() const { return _init_value; }
    ReduceFunc reduce_func() { return _reduce_func; }

private:
    const double _init_value;
    double _result;
    ReduceFunc _reduce_func;
    const size_t _output_num_rows;
    bool _sink_is_finished = false;
    bool _source_is_finished = false;
};
class ReducerFactory;
using ReducerFactoryRawPtr = ReducerFactory*;
using ReducerFactoryPtr = std::shared_ptr<ReducerFactory>;

class ReducerFactory final {
public:
    explicit ReducerFactory(double init_value, ReduceFunc reduce_func, size_t output_num_rows);
    ReducerPtr create(int32_t degree_of_parallelism, int32_t driver_sequence);

private:
    const double _init_value;
    ReduceFunc _reduce_func;
    const size_t _output_num_rows;
    Reducers _reducers;
};

class ReduceSinkOperator final : public pipeline::Operator {
public:
    ReduceSinkOperator(pipeline::OperatorFactory* factory, int32_t driver_sequence, ReducerRawPtr reducer);
    ~ReduceSinkOperator() = default;
    bool has_output() const override { return false; }
    bool need_input() const override;
    bool is_finished() const override;
    Status push_chunk(starrocks::RuntimeState* state, const vectorized::ChunkPtr& chunk) override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(starrocks::RuntimeState* state) override {
        return Status::NotSupported("Not implement");
    }
    Status reset_state(std::vector<ChunkPtr>&& chunks) override;
    Status set_finishing(starrocks::RuntimeState* state) override;

private:
    double _result;
    ReducerRawPtr _reducer;
};
class ReduceSinkOperatorFactory final : public pipeline::OperatorFactory {
public:
    ReduceSinkOperatorFactory(int32_t id, ReducerFactoryRawPtr reducer_factory);
    pipeline::OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    ReducerFactoryRawPtr _reducer_factory;
};

class ReduceSourceOperator final : public pipeline::SourceOperator {
public:
    ReduceSourceOperator(pipeline::OperatorFactory* factory, int32_t driver_sequence, ReducerPtr reducer);
    ~ReduceSourceOperator() = default;
    bool has_output() const override;
    bool need_input() const override { return false; }
    bool is_finished() const override;
    Status push_chunk(starrocks::RuntimeState* state, const vectorized::ChunkPtr& chunk) override {
        return Status::NotSupported("Not implement");
    }
    StatusOr<vectorized::ChunkPtr> pull_chunk(starrocks::RuntimeState* state) override;
    Status reset_state(std::vector<ChunkPtr>&& chunks) override;
    Status set_finishing(starrocks::RuntimeState* state) override;
    Status set_finished(starrocks::RuntimeState* state) override;

private:
    size_t _current_output_num_rows = 0;
    ReducerPtr _reducer;
};

class ReduceSourceOperatorFactory final : public pipeline::SourceOperatorFactory {
public:
    ReduceSourceOperatorFactory(int32_t id, ReducerFactoryPtr reducer_factory);
    ~ReduceSourceOperatorFactory() = default;
    pipeline::OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    ReducerFactoryPtr _reducer_factory;
};

} // namespace starrocks::vectorized