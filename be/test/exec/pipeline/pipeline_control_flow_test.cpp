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

#include <mutex>
#include <random>
#include <utility>

#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_builder.h"
#include "pipeline_test_base.h"
#include "util/defer_op.h"
#include "util/thrift_util.h"

#define ASSERT_COUNTER_LIFETIME(counter, dop)       \
    do {                                            \
        ASSERT_EQ(dop, counter->prepare_times());   \
        ASSERT_EQ(dop, counter->finishing_times()); \
        ASSERT_EQ(dop, counter->finished_times());  \
        ASSERT_EQ(dop, counter->closed_times());    \
        ASSERT_EQ(0, counter->wrong_times());       \
    } while (false)

#define ASSERT_COUNTER_CHUNK_NUM(counter, dop, expected_push_chunk_num, expected_pull_chunk_num) \
    do {                                                                                         \
        ASSERT_COUNTER_LIFETIME(counter, dop);                                                   \
        ASSERT_EQ(expected_push_chunk_num, counter->push_chunk_num());                           \
        ASSERT_EQ(expected_pull_chunk_num, counter->pull_chunk_num());                           \
    } while (false)

#define ASSERT_COUNTER_CHUNK_ROW_NUM(counter, dop, expected_push_chunk_row_num, expected_pull_chunk_row_num) \
    do {                                                                                                     \
        ASSERT_COUNTER_LIFETIME(counter, dop);                                                               \
        ASSERT_EQ(expected_push_chunk_row_num, counter->push_chunk_row_num());                               \
        ASSERT_EQ(expected_pull_chunk_row_num, counter->pull_chunk_row_num());                               \
    } while (false)

namespace starrocks::pipeline {

class Counter {
public:
    void process_prepare() {
        std::lock_guard<std::mutex> l(_mutex);
        _prepare_times++;
    }

    void process_push(const ChunkPtr& chunk) {
        std::lock_guard<std::mutex> l(_mutex);
        ++_push_chunk_num;
        _push_chunk_row_num += chunk->num_rows();
    }

    void process_pull(const ChunkPtr& chunk) {
        std::lock_guard<std::mutex> l(_mutex);
        ++_pull_chunk_num;
        _pull_chunk_row_num += chunk->num_rows();
    }

    void process_finishing() {
        std::lock_guard<std::mutex> l(_mutex);
        _finishing_times++;
    }

    void process_finished() {
        std::lock_guard<std::mutex> l(_mutex);
        _finished_times++;
    }

    void process_closed() {
        std::lock_guard<std::mutex> l(_mutex);
        _closed_times++;
    }

    void process_wrong() {
        std::lock_guard<std::mutex> l(_mutex);
        _wrong_state_times++;
    }

    size_t prepare_times() {
        std::lock_guard<std::mutex> l(_mutex);
        return _prepare_times;
    }

    size_t finishing_times() {
        std::lock_guard<std::mutex> l(_mutex);
        return _finishing_times;
    }

    size_t finished_times() {
        std::lock_guard<std::mutex> l(_mutex);
        return _finished_times;
    }

    size_t closed_times() {
        std::lock_guard<std::mutex> l(_mutex);
        return _closed_times;
    }

    size_t push_chunk_num() {
        std::lock_guard<std::mutex> l(_mutex);
        return _push_chunk_num;
    }

    size_t pull_chunk_num() {
        std::lock_guard<std::mutex> l(_mutex);
        return _pull_chunk_num;
    }

    size_t push_chunk_row_num() {
        std::lock_guard<std::mutex> l(_mutex);
        return _push_chunk_row_num;
    }

    size_t pull_chunk_row_num() {
        std::lock_guard<std::mutex> l(_mutex);
        return _pull_chunk_row_num;
    }

    size_t wrong_times() {
        std::lock_guard<std::mutex> l(_mutex);
        return _wrong_state_times;
    }

private:
    std::mutex _mutex;
    size_t _push_chunk_num = 0;
    size_t _pull_chunk_num = 0;
    size_t _push_chunk_row_num = 0;
    size_t _pull_chunk_row_num = 0;
    size_t _prepare_times = 0;
    size_t _finishing_times = 0;
    size_t _finished_times = 0;
    size_t _closed_times = 0;
    size_t _wrong_state_times = 0;
};

using CounterPtr = std::shared_ptr<Counter>;

class TestOperator : public Operator {
public:
    TestOperator(OperatorFactory* factory, int32_t id, const std::string& name, int32_t plan_node_id,
                 int32_t driver_sequence, CounterPtr counter)
            : Operator(factory, id, name, plan_node_id, false, driver_sequence), _counter(std::move(counter)) {}
    ~TestOperator() override = default;

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(Operator::prepare(state));
        _counter->process_prepare();
        return Status::OK();
    }

    Status set_finishing(RuntimeState* state) override {
        _counter->process_finishing();
        return Status::OK();
    }

    Status set_finished(RuntimeState* state) override {
        if (!is_finished()) {
            _counter->process_wrong();
        }
        _counter->process_finished();
        return Status::OK();
    }

    void close(RuntimeState* state) override {
        _counter->process_closed();
        return Operator::close(state);
    }

protected:
    CounterPtr _counter = nullptr;
};

class TestSourceOperator : public SourceOperator {
public:
    TestSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                       size_t chunk_num, size_t chunk_size, CounterPtr counter, int32_t pending_finish_cnt)
            : SourceOperator(factory, id, "test_source", plan_node_id, false, driver_sequence),
              _counter(std::move(counter)),
              _pending_finish_cnt(pending_finish_cnt) {
        for (size_t i = 0; i < chunk_num; ++i) {
            _chunks.push_back(PipelineTestBase::_create_and_fill_chunk(chunk_size));
        }
    }
    ~TestSourceOperator() override = default;

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(SourceOperator::prepare(state));
        _counter->process_prepare();
        return Status::OK();
    }

    Status set_finishing(RuntimeState* state) override {
        _counter->process_finishing();
        return Status::OK();
    }

    Status set_finished(RuntimeState* state) override {
        _counter->process_finished();
        return Status::OK();
    }

    void close(RuntimeState* state) override {
        ASSERT_LT(_pending_finish_cnt, 0);
        _counter->process_closed();
        return SourceOperator::close(state);
    }

    bool has_output() const override { return _index < _chunks.size(); }
    bool is_finished() const override { return !has_output(); }
    bool pending_finish() const override { return --_pending_finish_cnt >= 0; }

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    CounterPtr _counter;
    std::vector<ChunkPtr> _chunks;
    size_t _index = 0;
    mutable std::atomic<int32_t> _pending_finish_cnt;
};

Status TestSourceOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    _counter->process_push(chunk);
    return Status::InternalError("Shouldn't push chunk to source operator");
}

StatusOr<ChunkPtr> TestSourceOperator::pull_chunk(RuntimeState* state) {
    auto chunk = _chunks[_index++];
    _counter->process_pull(chunk);
    return chunk;
}

class TestSourceOperatorFactory final : public SourceOperatorFactory {
public:
    TestSourceOperatorFactory(int32_t id, int32_t plan_node_id, size_t chunk_num, size_t chunk_size, CounterPtr counter,
                              int32_t pending_finish_cnt)
            : SourceOperatorFactory(id, "test_source", plan_node_id),
              _chunk_num(chunk_num),
              _chunk_size(chunk_size),
              _counter(std::move(counter)),
              _pending_finish_cnt(pending_finish_cnt) {}

    ~TestSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestSourceOperator>(this, _id, _plan_node_id, driver_sequence, _chunk_num, _chunk_size,
                                                    _counter, _pending_finish_cnt);
    }
    SourceOperatorFactory::AdaptiveState adaptive_state() const override { return AdaptiveState::ACTIVE; }

private:
    size_t _chunk_num;
    size_t _chunk_size;
    CounterPtr _counter;
    int32_t _pending_finish_cnt;
};

class TestNormalOperator : public TestOperator {
public:
    TestNormalOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                       CounterPtr counter)
            : TestOperator(factory, id, "test_normal", plan_node_id, driver_sequence, std::move(counter)) {}
    ~TestNormalOperator() override = default;

    bool need_input() const override { return true; }
    bool has_output() const override { return _chunk != nullptr; }
    bool is_finished() const override { return _is_finished && !has_output(); }
    Status set_finishing(RuntimeState* state) override {
        TestOperator::set_finishing(state);
        _is_finished = true;
        return Status::OK();
    }

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    bool _is_finished = false;
    ChunkPtr _chunk;
};

Status TestNormalOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    _chunk = chunk;
    _counter->process_push(chunk);
    return Status::OK();
}

StatusOr<ChunkPtr> TestNormalOperator::pull_chunk(RuntimeState* state) {
    ChunkPtr chunk = _chunk;
    _counter->process_pull(chunk);
    _chunk = nullptr;
    return chunk;
}

class TestNormalOperatorFactory final : public OperatorFactory {
public:
    TestNormalOperatorFactory(int32_t id, int32_t plan_node_id, CounterPtr counter)
            : OperatorFactory(id, "test_normal", plan_node_id), _counter(std::move(counter)) {}

    ~TestNormalOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestNormalOperator>(this, _id, _plan_node_id, driver_sequence, _counter);
    }

private:
    CounterPtr _counter;
};

class TestSinkOperator : public TestOperator {
public:
    TestSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                     CounterPtr counter)
            : TestOperator(factory, id, "test_sink", plan_node_id, driver_sequence, std::move(counter)) {}
    ~TestSinkOperator() override = default;

    bool need_input() const override { return true; }
    bool has_output() const override { return false; }
    bool is_finished() const override { return _is_finished; }
    Status set_finishing(RuntimeState* state) override {
        TestOperator::set_finishing(state);
        _is_finished = true;
        return Status::OK();
    }

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    bool _is_finished = false;
};

Status TestSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    _counter->process_push(chunk);
    return Status::OK();
}

StatusOr<ChunkPtr> TestSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk to sink operator");
}

class TestSinkOperatorFactory final : public OperatorFactory {
public:
    TestSinkOperatorFactory(int32_t id, int32_t plan_node_id, CounterPtr counter)
            : OperatorFactory(id, "test_sink", plan_node_id), _counter(std::move(counter)) {}

    ~TestSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestSinkOperator>(this, _id, _plan_node_id, driver_sequence, _counter);
    }

private:
    CounterPtr _counter;
};
class TestPipelineControlFlow : public PipelineTestBase {};

TEST_F(TestPipelineControlFlow, test_two_operatories) {
    std::default_random_engine e;
    std::uniform_int_distribution<int32_t> u32(0, 100);
    size_t chunk_num = 1;
    _vector_chunk_size = 1;
    CounterPtr sourceCounter = std::make_shared<Counter>();
    CounterPtr sinkCounter = std::make_shared<Counter>();

    _pipeline_builder = [&](RuntimeState* state) {
        OpFactories op_factories;

        op_factories.push_back(std::make_shared<TestSourceOperatorFactory>(
                next_operator_id(), next_plan_node_id(), chunk_num, _vector_chunk_size, sourceCounter, u32(e)));
        op_factories.push_back(
                std::make_shared<TestSinkOperatorFactory>(next_operator_id(), next_plan_node_id(), sinkCounter));

        _pipelines.push_back(std::make_shared<Pipeline>(next_pipeline_id(), op_factories));
    };

    start_test();

    ASSERT_EQ(std::future_status::ready, _fragment_future.wait_for(std::chrono::seconds(15)));
    ASSERT_COUNTER_CHUNK_NUM(sourceCounter, 1, 0, chunk_num);
    ASSERT_COUNTER_CHUNK_NUM(sinkCounter, 1, chunk_num, 0);
}

TEST_F(TestPipelineControlFlow, test_three_operatories) {
    std::default_random_engine e;
    std::uniform_int_distribution<int32_t> u32(0, 100);
    size_t chunk_num = 1;
    _vector_chunk_size = 1;
    CounterPtr sourceCounter = std::make_shared<Counter>();
    CounterPtr normalCounter = std::make_shared<Counter>();
    CounterPtr sinkCounter = std::make_shared<Counter>();

    _pipeline_builder = [&](RuntimeState* state) {
        OpFactories op_factories;

        op_factories.push_back(std::make_shared<TestSourceOperatorFactory>(
                next_operator_id(), next_plan_node_id(), chunk_num, _vector_chunk_size, sourceCounter, u32(e)));
        op_factories.push_back(
                std::make_shared<TestNormalOperatorFactory>(next_operator_id(), next_plan_node_id(), normalCounter));
        op_factories.push_back(
                std::make_shared<TestSinkOperatorFactory>(next_operator_id(), next_plan_node_id(), sinkCounter));

        _pipelines.push_back(std::make_shared<Pipeline>(next_pipeline_id(), op_factories));
    };

    start_test();

    ASSERT_EQ(std::future_status::ready, _fragment_future.wait_for(std::chrono::seconds(15)));
    ASSERT_COUNTER_CHUNK_NUM(sourceCounter, 1, 0, chunk_num);
    ASSERT_COUNTER_CHUNK_NUM(normalCounter, 1, chunk_num, chunk_num);
    ASSERT_COUNTER_CHUNK_NUM(sinkCounter, 1, chunk_num, 0);
}

TEST_F(TestPipelineControlFlow, test_multi_operators) {
    std::default_random_engine e;
    std::uniform_int_distribution<int32_t> u32(0, 100);
    size_t max_mid_operator_num = 128;
    size_t chunk_num = 1;
    _vector_chunk_size = 1;

    for (size_t i = 1; i <= max_mid_operator_num; ++i) {
        CounterPtr sourceCounter = std::make_shared<Counter>();
        CounterPtr sinkCounter = std::make_shared<Counter>();
        std::vector<CounterPtr> normalCounters;
        for (size_t j = 0; j < i; ++j) {
            normalCounters.push_back(std::make_shared<Counter>());
        }

        _pipeline_builder = [&](RuntimeState* state) {
            OpFactories op_factories;

            op_factories.push_back(std::make_shared<TestSourceOperatorFactory>(
                    next_operator_id(), next_plan_node_id(), chunk_num, _vector_chunk_size, sourceCounter, u32(e)));
            for (size_t j = 0; j < i; ++j) {
                op_factories.push_back(std::make_shared<TestNormalOperatorFactory>(
                        next_operator_id(), next_plan_node_id(), normalCounters[j]));
            }
            op_factories.push_back(
                    std::make_shared<TestSinkOperatorFactory>(next_operator_id(), next_plan_node_id(), sinkCounter));

            _pipelines.push_back(std::make_shared<Pipeline>(next_pipeline_id(), op_factories));
        };

        start_test();

        ASSERT_EQ(std::future_status::ready, _fragment_future.wait_for(std::chrono::seconds(15)));
        ASSERT_COUNTER_CHUNK_NUM(sourceCounter, 1, 0, chunk_num);
        for (size_t j = 0; j < i; ++j) {
            ASSERT_COUNTER_CHUNK_NUM(normalCounters[j], 1, chunk_num, chunk_num);
        }
        ASSERT_COUNTER_CHUNK_NUM(sinkCounter, 1, chunk_num, 0);
    }
}

TEST_F(TestPipelineControlFlow, test_full_chunk_size) {
    std::default_random_engine e;
    std::uniform_int_distribution<int32_t> u32(0, 100);
    size_t chunk_num = 1;
    CounterPtr sourceCounter = std::make_shared<Counter>();
    CounterPtr normalCounter = std::make_shared<Counter>();
    CounterPtr sinkCounter = std::make_shared<Counter>();

    _pipeline_builder = [&](RuntimeState* state) {
        OpFactories op_factories;

        op_factories.push_back(std::make_shared<TestSourceOperatorFactory>(
                next_operator_id(), next_plan_node_id(), chunk_num, _vector_chunk_size, sourceCounter, u32(e)));
        op_factories.push_back(
                std::make_shared<TestNormalOperatorFactory>(next_operator_id(), next_plan_node_id(), normalCounter));
        op_factories.push_back(
                std::make_shared<TestSinkOperatorFactory>(next_operator_id(), next_plan_node_id(), sinkCounter));

        _pipelines.push_back(std::make_shared<Pipeline>(next_pipeline_id(), op_factories));
    };

    start_test();

    ASSERT_EQ(std::future_status::ready, _fragment_future.wait_for(std::chrono::seconds(15)));
    ASSERT_COUNTER_CHUNK_NUM(sourceCounter, 1, 0, chunk_num);
    ASSERT_COUNTER_CHUNK_NUM(normalCounter, 1, chunk_num, chunk_num);
    ASSERT_COUNTER_CHUNK_NUM(sinkCounter, 1, chunk_num, 0);
}

TEST_F(TestPipelineControlFlow, test_multi_chunks) {
    std::default_random_engine e;
    std::uniform_int_distribution<int32_t> u32(0, 100);
    size_t chunk_num = 1000;
    _vector_chunk_size = 1;
    CounterPtr sourceCounter = std::make_shared<Counter>();
    CounterPtr normalCounter = std::make_shared<Counter>();
    CounterPtr sinkCounter = std::make_shared<Counter>();

    _pipeline_builder = [&](RuntimeState* state) {
        OpFactories op_factories;

        op_factories.push_back(std::make_shared<TestSourceOperatorFactory>(
                next_operator_id(), next_plan_node_id(), chunk_num, _vector_chunk_size, sourceCounter, u32(e)));
        op_factories.push_back(
                std::make_shared<TestNormalOperatorFactory>(next_operator_id(), next_plan_node_id(), normalCounter));
        op_factories.push_back(
                std::make_shared<TestSinkOperatorFactory>(next_operator_id(), next_plan_node_id(), sinkCounter));

        _pipelines.push_back(std::make_shared<Pipeline>(next_pipeline_id(), op_factories));
    };

    start_test();

    ASSERT_EQ(std::future_status::ready, _fragment_future.wait_for(std::chrono::seconds(15)));
    ASSERT_COUNTER_CHUNK_NUM(sourceCounter, 1, 0, chunk_num);
    ASSERT_COUNTER_CHUNK_NUM(normalCounter, 1, chunk_num, chunk_num);
    ASSERT_COUNTER_CHUNK_NUM(sinkCounter, 1, chunk_num, 0);
}
} // namespace starrocks::pipeline
