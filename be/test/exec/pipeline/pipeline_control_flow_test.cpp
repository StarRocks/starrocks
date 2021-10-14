// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "pipeline_test_base.h"
#include "util/thrift_util.h"

namespace starrocks::pipeline {

struct Counter {
    size_t pull_chunk_num;
    size_t push_chunk_num;
};

using CounterPtr = std::shared_ptr<Counter>;

void assert_counter(CounterPtr counter, size_t expected_pull_chunk_num, size_t expected_push_chunk_num) {
    ASSERT_EQ(expected_pull_chunk_num, counter->pull_chunk_num);
    ASSERT_EQ(expected_push_chunk_num, counter->push_chunk_num);
}

class TestSourceOperator : public SourceOperator {
public:
    TestSourceOperator(int32_t id, int32_t plan_node_id, size_t chunk_num, size_t chunk_size, CounterPtr counter)
            : SourceOperator(id, "test_source", plan_node_id), _counter(counter) {
        for (size_t i = 0; i < chunk_num; ++i) {
            _chunks.push_back(std::move(PipelineTestBase::_create_and_fill_chunk(chunk_size)));
        }
    }
    ~TestSourceOperator() override = default;

    bool has_output() const override { return _index < _chunks.size(); }
    bool is_finished() const override { return !has_output(); }
    void finish(RuntimeState* state) override {}

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    CounterPtr _counter;
    std::vector<vectorized::ChunkPtr> _chunks;
    size_t _index = 0;
};

Status TestSourceOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _counter->push_chunk_num++;
    return Status::InternalError("Shouldn't push chunk to source operator");
}

StatusOr<vectorized::ChunkPtr> TestSourceOperator::pull_chunk(RuntimeState* state) {
    _counter->pull_chunk_num++;
    return _chunks[_index++];
}

class TestSourceOperatorFactory final : public SourceOperatorFactory {
public:
    TestSourceOperatorFactory(int32_t id, int32_t plan_node_id, size_t chunk_num, size_t chunk_size, CounterPtr counter)
            : SourceOperatorFactory(id, "test_source", plan_node_id),
              _chunk_num(chunk_num),
              _chunk_size(chunk_size),
              _counter(counter) {}

    ~TestSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestSourceOperator>(_id, _plan_node_id, _chunk_num, _chunk_size, _counter);
    }

private:
    size_t _chunk_num;
    size_t _chunk_size;
    CounterPtr _counter;
};

class TestNormalOperator : public Operator {
public:
    TestNormalOperator(int32_t id, int32_t plan_node_id, CounterPtr counter)
            : Operator(id, "test_normal", plan_node_id), _counter(counter) {}
    ~TestNormalOperator() override = default;

    bool need_input() const override { return true; }
    bool has_output() const override { return _chunk != nullptr; }
    bool is_finished() const override { return _is_finished && !has_output(); }
    void finish(RuntimeState* state) override { _is_finished = true; }

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    CounterPtr _counter;
    bool _is_finished;
    ChunkPtr _chunk = nullptr;
};

Status TestNormalOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _counter->push_chunk_num++;
    _chunk = chunk;
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> TestNormalOperator::pull_chunk(RuntimeState* state) {
    _counter->pull_chunk_num++;
    ChunkPtr chunk = _chunk;
    _chunk = nullptr;
    return chunk;
}

class TestNormalOperatorFactory final : public OperatorFactory {
public:
    TestNormalOperatorFactory(int32_t id, int32_t plan_node_id, CounterPtr counter)
            : OperatorFactory(id, "test_normal", plan_node_id), _counter(counter) {}

    ~TestNormalOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestNormalOperator>(_id, _plan_node_id, _counter);
    }

private:
    CounterPtr _counter;
};

class TestSinkOperator : public Operator {
public:
    TestSinkOperator(int32_t id, int32_t plan_node_id, CounterPtr counter)
            : Operator(id, "test_sink", plan_node_id), _counter(counter) {}
    ~TestSinkOperator() override = default;

    bool need_input() const override { return true; }
    bool has_output() const override { return _chunk != nullptr; }
    bool is_finished() const override { return _is_finished; }
    void finish(RuntimeState* state) override { _is_finished = true; }

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    CounterPtr _counter;
    bool _is_finished;
    ChunkPtr _chunk = nullptr;
};

Status TestSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _counter->push_chunk_num++;
    _chunk = chunk;
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> TestSinkOperator::pull_chunk(RuntimeState* state) {
    _counter->pull_chunk_num++;
    return Status::InternalError("Shouldn't pull chunk to sink operator");
}

class TestSinkOperatorFactory final : public OperatorFactory {
public:
    TestSinkOperatorFactory(int32_t id, int32_t plan_node_id, CounterPtr counter)
            : OperatorFactory(id, "test_sink", plan_node_id), _counter(counter) {}

    ~TestSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestSinkOperator>(_id, _plan_node_id, _counter);
    }

private:
    CounterPtr _counter;
};
class TestPipelineControlFlow : public PipelineTestBase {};

TEST_F(TestPipelineControlFlow, test_two_operatories) {
    CounterPtr sourceCounter = std::make_shared<Counter>();
    CounterPtr sinkCounter = std::make_shared<Counter>();

    _pipeline_builder = [=]() {
        _pipelines.clear();

        OpFactories op_factories;
        op_factories.push_back(std::make_shared<TestSourceOperatorFactory>(1, 1, 1, 1, sourceCounter));
        op_factories.push_back(std::make_shared<TestSinkOperatorFactory>(2, 2, sinkCounter));

        _pipelines.push_back(std::make_shared<Pipeline>(1, op_factories));
    };

    start_test();

    ASSERT_TRUE(_fragment_future.wait_for(std::chrono::seconds(3)) == std::future_status::ready);
    assert_counter(sourceCounter, 1, 0);
    assert_counter(sinkCounter, 0, 1);
}

TEST_F(TestPipelineControlFlow, test_three_operatories) {
    CounterPtr sourceCounter = std::make_shared<Counter>();
    CounterPtr normalCounter = std::make_shared<Counter>();
    CounterPtr sinkCounter = std::make_shared<Counter>();

    _pipeline_builder = [=]() {
        _pipelines.clear();

        OpFactories op_factories;
        op_factories.push_back(std::make_shared<TestSourceOperatorFactory>(1, 1, 1, 1, sourceCounter));
        op_factories.push_back(std::make_shared<TestNormalOperatorFactory>(2, 2, normalCounter));
        op_factories.push_back(std::make_shared<TestSinkOperatorFactory>(3, 3, sinkCounter));

        _pipelines.push_back(std::make_shared<Pipeline>(1, op_factories));
    };

    start_test();

    ASSERT_TRUE(_fragment_future.wait_for(std::chrono::seconds(3)) == std::future_status::ready);
    assert_counter(sourceCounter, 1, 0);
    assert_counter(normalCounter, 1, 1);
    assert_counter(sinkCounter, 0, 1);
}
} // namespace starrocks::pipeline
