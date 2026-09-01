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

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "base/testutil/assert.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_builder.h"
#include "column/nullable_column.h"
#include "common/config_exec_fwd.h"
#include "compute_env/workgroup/pipeline_executor_set.h"
#include "compute_env/workgroup/work_group.h"
#include "exec/ai_project_node.h"
#include "exec/pipeline/ai/ai_chunk_buffer.h"
#include "exec/pipeline/ai/ai_project_factory.h"
#include "exec/pipeline/ai/ai_project_operator.h"
#include "exec/pipeline/ai/ai_project_runtime.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/project_operator.h"
#include "exec/pipeline_node.h"
#include "exec/runtime/fragment_context.h"
#include "exec/runtime/pipeline.h"
#include "exec/runtime/pipeline_builder_context.h"
#include "exec/runtime/query_context.h"
#include "exec_primitive/pipeline/operator_factory.h"
#include "exec_primitive/pipeline/source_operator.h"
#include "exprs/expr_context.h"
#include "exprs/expr_factory.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "platform/llm/ai_admission_controller.h"
#include "platform/llm/ai_http_client.h"
#include "platform/llm/ai_metrics.h"
#include "platform/llm/ai_runtime.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/service_contexts.h"
#include "types/type_descriptor.h"

namespace starrocks {
namespace pipeline {
namespace {

constexpr int64_t kAICompletePromptFid = 200100;
constexpr int64_t kAICompleteModelPromptFid = 200102;
constexpr TSlotId kPromptSlotId = 1;
constexpr TSlotId kCommonSlotId = 2;
constexpr TSlotId kAnswerSlotId = 3;
constexpr int64_t kLimit = 5;
constexpr size_t kUpstreamDop = 6;
constexpr size_t kBuilderDop = 9;
constexpr std::string_view kSystemChatConfigId = "__system_chat__";
constexpr std::string_view kApiKeyEnvironment = "AI_FUNCTION_MODEL_API_KEY";
constexpr std::string_view kEndpointEnvironment = "AI_FUNCTION_MODEL_ENDPOINT";
constexpr std::string_view kEndpointSentinel = "https://127.0.0.1/v1/chat/completions";
constexpr std::string_view kModelSentinel = "unit-test-model";
constexpr std::string_view kSecretSentinel = "unit-test-secret-must-not-leak";
constexpr size_t kMiB = 1024UL * 1024;

TypeDescriptor varchar_type() {
    return TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
}

TExpr make_slot_ref(TTupleId tuple_id, TSlotId slot_id) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::SLOT_REF);
    node.__set_num_children(0);
    node.__set_type(varchar_type().to_thrift());
    node.__set_is_nullable(true);

    TSlotRef slot;
    slot.__set_tuple_id(tuple_id);
    slot.__set_slot_id(slot_id);
    node.__set_slot_ref(slot);

    TExpr expression;
    expression.nodes.emplace_back(std::move(node));
    return expression;
}

TExpr make_ai_complete(TTupleId tuple_id, TSlotId prompt_slot) {
    TFunctionName name;
    name.__set_function_name("ai_complete");

    TFunction function;
    function.__set_name(name);
    function.__set_binary_type(TFunctionBinaryType::AI);
    function.__set_arg_types({varchar_type().to_thrift()});
    function.__set_ret_type(varchar_type().to_thrift());
    function.__set_has_var_args(false);
    function.__set_fid(kAICompletePromptFid);
    function.__set_ai_model_source(TAIModelSource::SYSTEM);

    TExprNode call;
    call.__set_node_type(TExprNodeType::FUNCTION_CALL);
    call.__set_num_children(1);
    call.__set_type(varchar_type().to_thrift());
    call.__set_is_nullable(true);
    call.__set_fn(function);
    call.__set_ai_model_config_id(std::string(kSystemChatConfigId));

    TExpr expression;
    expression.nodes.emplace_back(std::move(call));
    expression.nodes.emplace_back(make_slot_ref(tuple_id, prompt_slot).nodes.front());
    return expression;
}

TExpr make_ai_complete_with_explicit_model(TTupleId tuple_id, TSlotId prompt_slot) {
    TFunctionName name;
    name.__set_function_name("ai_complete");

    TFunction function;
    function.__set_name(name);
    function.__set_binary_type(TFunctionBinaryType::AI);
    function.__set_arg_types({varchar_type().to_thrift(), varchar_type().to_thrift()});
    function.__set_ret_type(varchar_type().to_thrift());
    function.__set_has_var_args(false);
    function.__set_fid(kAICompleteModelPromptFid);
    function.__set_ai_model_source(TAIModelSource::SYSTEM);

    TExprNode call;
    call.__set_node_type(TExprNodeType::FUNCTION_CALL);
    call.__set_num_children(2);
    call.__set_type(varchar_type().to_thrift());
    call.__set_is_nullable(true);
    call.__set_fn(function);
    call.__set_ai_model_config_id(std::string(kSystemChatConfigId));

    TExprNode model;
    model.__set_node_type(TExprNodeType::STRING_LITERAL);
    model.__set_num_children(0);
    model.__set_type(varchar_type().to_thrift());
    model.__set_is_nullable(false);
    TStringLiteral literal;
    literal.__set_value("explicit-model");
    model.__set_string_literal(std::move(literal));

    TExpr expression;
    expression.nodes.emplace_back(std::move(call));
    expression.nodes.emplace_back(std::move(model));
    expression.nodes.emplace_back(make_slot_ref(tuple_id, prompt_slot).nodes.front());
    return expression;
}

TPlanNode make_ai_node(TTupleId tuple_id) {
    TAIEndpointConfig endpoint;
    endpoint.__set_endpoint(std::string(kEndpointSentinel));
    endpoint.__set_model(std::string(kModelSentinel));
    endpoint.__set_provider("openai_compatible");

    TAIModelConfiguration model;
    model.__set_chat(std::move(endpoint));

    TAIProjectNode project;
    project.__set_slot_map({
            {kPromptSlotId, make_slot_ref(tuple_id, kPromptSlotId)},
            {kAnswerSlotId, make_ai_complete(tuple_id, kCommonSlotId)},
    });
    project.__set_common_slot_map({
            {kCommonSlotId, make_slot_ref(tuple_id, kPromptSlotId)},
    });
    project.__set_ai_model_configs({
            {std::string(kSystemChatConfigId), std::move(model)},
    });

    TPlanNode node;
    node.__set_node_id(10);
    node.__set_node_type(TPlanNodeType::AI_PROJECT_NODE);
    node.__set_num_children(1);
    node.__set_limit(kLimit);
    node.row_tuples.emplace_back(tuple_id);
    node.__set_ai_project_node(std::move(project));
    return node;
}

TPlanNode make_child_node(TTupleId tuple_id) {
    TPlanNode node;
    node.__set_node_id(11);
    node.__set_node_type(TPlanNodeType::EMPTY_SET_NODE);
    node.__set_num_children(0);
    node.__set_limit(-1);
    node.row_tuples.emplace_back(tuple_id);
    return node;
}

class ScopedEnvironment {
public:
    ScopedEnvironment(std::string name, std::optional<std::string> value) : _name(std::move(name)) {
        const char* previous = std::getenv(_name.c_str());
        if (previous != nullptr) {
            _previous = previous;
        }
        if (value.has_value()) {
            (void)setenv(_name.c_str(), value->c_str(), 1);
        } else {
            (void)unsetenv(_name.c_str());
        }
    }

    ~ScopedEnvironment() {
        if (_previous.has_value()) {
            (void)setenv(_name.c_str(), _previous->c_str(), 1);
        } else {
            (void)unsetenv(_name.c_str());
        }
    }

private:
    std::string _name;
    std::optional<std::string> _previous;
};

class NeverYieldWorkGroupSchedulePolicy final : public workgroup::WorkGroupSchedulePolicy {
public:
    bool should_yield(const workgroup::WorkGroup*) const override { return false; }
    size_t num_workgroups() const override { return 1; }
};

class ScopedWorkGroupExecutors {
public:
    ScopedWorkGroupExecutors(workgroup::WorkGroupPtr workgroup, workgroup::PipelineExecutorSet* executors)
            : _workgroup(std::move(workgroup)) {
        _workgroup->set_shared_executors(executors);
    }

    ~ScopedWorkGroupExecutors() { _workgroup->set_shared_executors(nullptr); }

private:
    workgroup::WorkGroupPtr _workgroup;
};

class TestSourceOperator final : public SourceOperator {
public:
    TestSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : SourceOperator(factory, id, "test_source", plan_node_id, false, driver_sequence) {}

    bool has_output() const override { return false; }
    bool is_finished() const override { return false; }
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }
};

class TestSourceOperatorFactory final : public SourceOperatorFactory {
public:
    TestSourceOperatorFactory(int32_t id, int32_t plan_node_id)
            : SourceOperatorFactory(id, "test_source", plan_node_id) {
        set_degree_of_parallelism(kUpstreamDop);
        set_could_local_shuffle(false);
        set_partition_type(TPartitionType::RANDOM);
        set_skewed(true);

        TBucketProperty bucket;
        bucket.__set_bucket_func(TBucketFunction::MURMUR3_X86_32);
        bucket.__set_bucket_num(17);
        _bucket_properties.emplace_back(std::move(bucket));
    }

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestSourceOperator>(this, id(), plan_node_id(), driver_sequence);
    }
};

class TestSourceNode final : public PipelineNode {
public:
    TestSourceNode(ObjectPool* pool, const TPlanNode& node, const DescriptorTbl& descriptors)
            : PipelineNode(pool, node, descriptors) {}

    StatusOr<OpFactories> decompose_to_pipeline(PipelineBuilderContext* context) override {
        return OpFactories{
                std::make_shared<TestSourceOperatorFactory>(context->next_operator_id(), id()),
        };
    }
};

class FixedClock final : public AIClock {
public:
    int64_t monotonic_now_ns() const noexcept override { return _monotonic_ns; }
    int64_t unix_now_seconds() const noexcept override { return _unix_seconds; }

    void set_monotonic_now_ns(int64_t value) { _monotonic_ns = value; }

private:
    int64_t _monotonic_ns = 100'000'000'000;
    int64_t _unix_seconds = 1'700'000'000;
};

class DrainableControlScheduler final : public AIControlScheduler {
public:
    explicit DrainableControlScheduler(const AIClock* clock) : _clock(clock) {}

    Status post(Task task) override {
        _ready.emplace_back(std::move(task));
        return Status::OK();
    }

    StatusOr<TaskId> schedule_at(int64_t monotonic_time_ns, Task task) override {
        const TaskId id = ++_next_id;
        _timers.emplace_back(Timer{id, monotonic_time_ns, std::move(task), false});
        return id;
    }

    void cancel(TaskId id) override {
        for (Timer& timer : _timers) {
            if (timer.id == id) {
                timer.cancelled = true;
            }
        }
    }

    void shutdown_and_drain() override {
        _timers.clear();
        run_until_idle();
    }

    void run_until_idle() {
        for (;;) {
            std::stable_sort(_timers.begin(), _timers.end(), [](const Timer& lhs, const Timer& rhs) {
                return std::pair(lhs.deadline_ns, lhs.id) < std::pair(rhs.deadline_ns, rhs.id);
            });
            while (!_timers.empty() && _timers.front().deadline_ns <= _clock->monotonic_now_ns()) {
                Timer timer = std::move(_timers.front());
                _timers.erase(_timers.begin());
                if (!timer.cancelled) {
                    _ready.emplace_back(std::move(timer.task));
                }
            }
            if (_ready.empty()) {
                return;
            }
            Task task = std::move(_ready.front());
            _ready.pop_front();
            task();
        }
    }

private:
    struct Timer {
        TaskId id;
        int64_t deadline_ns;
        Task task;
        bool cancelled;
    };

    const AIClock* _clock;
    TaskId _next_id = 0;
    std::deque<Task> _ready;
    std::vector<Timer> _timers;
};

class QueuedCompletionExecutor final : public AICompletionExecutor {
public:
    ~QueuedCompletionExecutor() override {
        while (!_work.empty()) {
            AICompletionWork work = std::move(_work.front());
            _work.pop_front();
            work.cancel();
        }
    }

    Status try_submit(AICompletionWork work) override {
        _work.emplace_back(std::move(work));
        return Status::OK();
    }

    void run_until_idle() {
        while (!_work.empty()) {
            AICompletionWork work = std::move(_work.front());
            _work.pop_front();
            work.run();
        }
    }

private:
    std::deque<AICompletionWork> _work;
};

class CapturingHttpClient final : public AIHttpClient {
public:
    Status submit(AIHttpRequest request, AIHttpCallback callback) override {
        _pending.emplace_back(Pending{std::move(request), std::move(callback)});
        return Status::OK();
    }

    void shutdown() override {
        while (!_pending.empty()) {
            complete_next(AIHttpNoResponseCode::SHUTDOWN);
        }
    }

    size_t pending_count() const { return _pending.size(); }
    const AIHttpRequest& request(size_t index = 0) const { return _pending.at(index).request; }

    void complete_next(AIHttpNoResponseCode code) {
        ASSERT_FALSE(_pending.empty());
        Pending pending = std::move(_pending.front());
        _pending.pop_front();
        pending.callback(AIHttpNoResponse{.code = code});
    }

private:
    struct Pending {
        AIHttpRequest request;
        AIHttpCallback callback;
    };

    std::deque<Pending> _pending;
};

class ZeroRandom final : public AIRandom {
protected:
    uint32_t uniform_unlocked(uint32_t exclusive_upper) override { return 0; }
};

class TestTaskHandle final : public AIProjectTaskHandle {
public:
    void cancel() noexcept override {}
};

class ManualTaskSubmitter final : public AIProjectTaskSubmitter {
public:
    struct Pending {
        std::string prompt;
        AITaskCallback callback;
    };

    StatusOr<std::unique_ptr<AIProjectTaskHandle>> submit(AIProjectTaskRequest request,
                                                          AITaskCallback&& callback) override {
        _pending.emplace_back(Pending{.prompt = std::string(request.prompt), .callback = std::move(callback)});
        std::unique_ptr<AIProjectTaskHandle> handle = std::make_unique<TestTaskHandle>();
        return handle;
    }

    size_t pending_count() const { return _pending.size(); }

    void succeed(std::string_view prompt) {
        auto iterator = std::find_if(_pending.begin(), _pending.end(),
                                     [&](const Pending& task) { return task.prompt == prompt; });
        ASSERT_NE(_pending.end(), iterator);
        Pending task = std::move(*iterator);
        _pending.erase(iterator);

        auto success = AITaskSuccess::create("result-" + task.prompt, {});
        ASSERT_TRUE(success.ok()) << success.status();
        task.callback(std::move(success).value());
    }

private:
    std::vector<Pending> _pending;
};

class RecordingTaskSubmitter final : public AIProjectTaskSubmitter {
public:
    explicit RecordingTaskSubmitter(std::shared_ptr<AIProjectTaskSubmitter> delegate)
            : _delegate(std::move(delegate)) {}

    StatusOr<std::unique_ptr<AIProjectTaskHandle>> submit(AIProjectTaskRequest request,
                                                          AITaskCallback&& callback) override {
        return _delegate->submit(
                std::move(request), [this, callback = std::move(callback)](AITaskResult result) mutable {
                    if (const auto* cancelled = std::get_if<AILifecycleCancelled>(&result); cancelled != nullptr) {
                        lifecycle_reasons.emplace_back(cancelled->reason);
                    } else if (std::holds_alternative<AISanitizedRowFailure>(result)) {
                        ++sanitized_failures;
                    }
                    callback(std::move(result));
                });
    }

    std::vector<AILifecycleReason> lifecycle_reasons;
    size_t sanitized_failures = 0;

private:
    std::shared_ptr<AIProjectTaskSubmitter> _delegate;
};

class RecordingLifecycleProjection final : public AIProjectProjection {
public:
    Status prepare(RuntimeState* state, int32_t dop) override {
        ++prepare_calls;
        prepared_states.emplace_back(state);
        prepared_dops.emplace_back(dop);
        return Status::OK();
    }

    void close(RuntimeState* state) override {
        ++close_calls;
        closed_states.emplace_back(state);
    }

    StatusOr<AIProjectPreparedSubchunk> prepare_subchunk(RuntimeState*, int32_t, const ChunkPtr&) override {
        return Status::InternalError("lifecycle-only projection must not evaluate");
    }

    size_t prepare_calls = 0;
    size_t close_calls = 0;
    std::vector<RuntimeState*> prepared_states;
    std::vector<RuntimeState*> closed_states;
    std::vector<int32_t> prepared_dops;
};

ChunkPtr make_prompt_chunk(size_t begin, size_t rows, std::optional<size_t> null_row = std::nullopt) {
    auto data = BinaryColumn::create();
    auto nulls = NullColumn::create();
    for (size_t offset = 0; offset < rows; ++offset) {
        if (null_row.has_value() && *null_row == offset) {
            data->append_default();
            nulls->append(1);
        } else {
            data->append("prompt-" + std::to_string(begin + offset));
            nulls->append(0);
        }
    }

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(NullableColumn::create(std::move(data), std::move(nulls)), kPromptSlotId);
    return chunk;
}

std::optional<std::string> nullable_string(const ChunkPtr& chunk, SlotId slot_id, size_t row) {
    const auto& nullable = down_cast<const NullableColumn&>(*chunk->get_column_by_slot_id(slot_id));
    if (nullable.is_null(row)) {
        return std::nullopt;
    }
    const auto& data = down_cast<const BinaryColumn&>(*nullable.data_column());
    return data.get_slice(row).to_string();
}

class AIProjectPipelineTest : public ::testing::Test {
protected:
    AIProjectPipelineTest() : _control(&_clock) {}

    void SetUp() override {
        TDescriptorTableBuilder table;
        TTupleDescriptorBuilder tuple;
        tuple.add_slot(TSlotDescriptorBuilder()
                               .id(kPromptSlotId)
                               .type(varchar_type())
                               .nullable(true)
                               .column_name("prompt")
                               .build());
        tuple.add_slot(TSlotDescriptorBuilder()
                               .id(kAnswerSlotId)
                               .type(varchar_type())
                               .nullable(true)
                               .column_name("answer")
                               .build());
        tuple.build(&table);

        auto config = AIRuntimeConfigSource::create();
        ASSERT_TRUE(config.ok()) << config.status();
        _config_source = std::move(config).value();
        _admission = std::make_unique<AIAdmissionController>(&_clock, &_control, _config_source.get(), 32);

        _ai_services.config_source = _config_source.get();
        _ai_services.admission_controller = _admission.get();
        _ai_services.http_client = &_http;
        _ai_services.completion_executor = &_completion;
        _ai_services.clock = &_clock;
        _ai_services.random = &_random;
        _ai_services.metrics = AIMetrics::instance();
        _query_services.ai = &_ai_services;

        _runtime_state = std::make_shared<RuntimeState>(TQueryGlobals{});
        _runtime_state->set_query_execution_services(&_query_services);
        ASSERT_OK(DescriptorTbl::create(_runtime_state.get(), &_descriptor_pool, table.desc_tbl(), &_descriptors,
                                        config::vector_chunk_size));
        _runtime_state->set_desc_tbl(_descriptors);

        std::vector<TupleDescriptor*> tuple_descriptors;
        _descriptors->get_tuple_descs(&tuple_descriptors);
        ASSERT_EQ(1, tuple_descriptors.size());
        _tuple_id = tuple_descriptors.front()->id();

        _process_tracker = std::make_shared<MemTracker>(MemTrackerType::PROCESS, -1, "process");
        _query_pool_tracker =
                std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, -1, "query_pool", _process_tracker.get());
        _workgroup = std::make_shared<workgroup::WorkGroup>("wg", 7, 3, 1, -1, 0, 1.0, TWorkGroupType::WG_NORMAL,
                                                            "ai_test_pool");
        _workgroup->_shared_mem_tracker = _query_pool_tracker;
        _workgroup->_mem_tracker =
                std::make_shared<MemTracker>(MemTrackerType::RESOURCE_GROUP, -1, "wg", _query_pool_tracker.get());
        _query_tracker =
                std::make_shared<MemTracker>(MemTrackerType::QUERY, -1, "query", _workgroup->_mem_tracker.get());
        _runtime_state->set_query_mem_tracker(_query_tracker);

        _query_context = QueryContext::create();
        _query_context->attach_to_runtime_state(_runtime_state.get());
        _fragment_context.set_workgroup(_workgroup);
        _fragment_context.attach_to_runtime_state(_runtime_state.get());
        _fragment_context.set_runtime_state(std::shared_ptr<RuntimeState>(_runtime_state));
        _endpoint_binding =
                std::make_unique<ScopedEnvironment>(std::string(kEndpointEnvironment), std::string(kEndpointSentinel));
    }

    void TearDown() override {
        _admission->shutdown();
        _control.run_until_idle();
        _http.shutdown();
        _completion.run_until_idle();
        _control.run_until_idle();
        _endpoint_binding.reset();
    }

    StatusOr<ExprContext*> create_expr_context(TExpr expression) {
        ExprContext* context = nullptr;
        RETURN_IF_ERROR(ExprFactory::create_expr_tree(_runtime_state->obj_pool(), expression, &context,
                                                      _runtime_state.get(), true));
        if (context == nullptr) {
            return Status::InternalError("test expression factory returned a null context");
        }
        return context;
    }

    StatusOr<std::shared_ptr<AIProjectExpressionProjection>> create_expression_projection(
            bool explicit_model = false, std::string default_model = std::string(kModelSentinel)) {
        ASSIGN_OR_RETURN(ExprContext * identity, create_expr_context(make_slot_ref(_tuple_id, kPromptSlotId)));
        ASSIGN_OR_RETURN(ExprContext * common, create_expr_context(make_slot_ref(_tuple_id, kPromptSlotId)));
        ASSIGN_OR_RETURN(
                ExprContext * ai,
                create_expr_context(explicit_model ? make_ai_complete_with_explicit_model(_tuple_id, kCommonSlotId)
                                                   : make_ai_complete(_tuple_id, kCommonSlotId)));

        AIProjectProjectionSpec spec(_runtime_state.get(),
                                     {
                                             AIProjectOutputSpec{
                                                     .slot_id = kPromptSlotId,
                                                     .expr_ctx = identity,
                                                     .nullable = true,
                                                     .kind = AIProjectOutputKind::PASSTHROUGH,
                                             },
                                             AIProjectOutputSpec{
                                                     .slot_id = kAnswerSlotId,
                                                     .expr_ctx = ai,
                                                     // Deliberately false: AI result nullability is
                                                     // intrinsic and must not trust this metadata.
                                                     .nullable = false,
                                                     .kind = AIProjectOutputKind::AI,
                                             },
                                     },
                                     {AIProjectCommonSpec{.slot_id = kCommonSlotId, .expr_ctx = common}},
                                     std::move(default_model));
        return AIProjectExpressionProjection::create(std::move(spec));
    }

    void set_query_deadline_ns(int64_t deadline_ns) {
        ASSERT_GT(deadline_ns, 0);
        ASSERT_EQ(0, deadline_ns % 1'000'000);
        _query_context->query_runtime_state()._query_deadline_ms.store(deadline_ns / 1'000'000 - 1);
        ASSERT_EQ(deadline_ns, _query_context->query_runtime_state().query_deadline_ns());
    }

    ObjectPool _descriptor_pool;
    DescriptorTbl* _descriptors = nullptr;
    TTupleId _tuple_id = 0;

    FixedClock _clock;
    DrainableControlScheduler _control;
    QueuedCompletionExecutor _completion;
    CapturingHttpClient _http;
    ZeroRandom _random;
    std::unique_ptr<AIRuntimeConfigSource> _config_source;
    std::unique_ptr<AIAdmissionController> _admission;
    AIServices _ai_services;
    QueryExecutionServices _query_services;

    std::shared_ptr<MemTracker> _process_tracker;
    std::shared_ptr<MemTracker> _query_pool_tracker;
    workgroup::WorkGroupPtr _workgroup;
    std::shared_ptr<MemTracker> _query_tracker;
    std::shared_ptr<RuntimeState> _runtime_state;
    QueryContextPtr _query_context;
    FragmentContext _fragment_context;
    std::unique_ptr<ScopedEnvironment> _endpoint_binding;
};

TEST_F(AIProjectPipelineTest, SplitsPipelinesAndPreservesDopSkewBucketAndLimitOrder) {
    ScopedEnvironment api_key(std::string(kApiKeyEnvironment), "unit-test-secret");

    ObjectPool node_pool;
    TPlanNode child_thrift = make_child_node(_tuple_id);
    auto* child = node_pool.add(new TestSourceNode(&node_pool, child_thrift, *_descriptors));

    TPlanNode ai_thrift = make_ai_node(_tuple_id);
    AIProjectNode node(&node_pool, ai_thrift, *_descriptors);
    node.add_child(child);
    ASSERT_OK(node.init(ai_thrift, _runtime_state.get()));

    PipelineBuilderContext context(&_fragment_context, kBuilderDop, 1);
    auto downstream_or = node.decompose_to_pipeline(&context);
    ASSERT_TRUE(downstream_or.ok()) << downstream_or.status();
    OpFactories downstream = std::move(downstream_or).value();

    const Pipeline* upstream_pipeline = context.last_pipeline();
    ASSERT_NE(nullptr, upstream_pipeline);
    ASSERT_EQ(2, upstream_pipeline->_op_factories.size());
    auto* upstream_source = dynamic_cast<TestSourceOperatorFactory*>(upstream_pipeline->_op_factories.front().get());
    auto* ai_sink = dynamic_cast<AISinkOperatorFactory*>(upstream_pipeline->_op_factories.back().get());
    ASSERT_NE(nullptr, upstream_source);
    ASSERT_NE(nullptr, ai_sink);

    ASSERT_EQ(2, downstream.size());
    auto* ai_source = dynamic_cast<AISourceOperatorFactory*>(downstream.front().get());
    auto* limit = dynamic_cast<LimitOperatorFactory*>(downstream.back().get());
    ASSERT_NE(nullptr, ai_source);
    ASSERT_NE(nullptr, limit);
    EXPECT_EQ(kLimit, limit->limit());

    EXPECT_EQ(kUpstreamDop, upstream_pipeline->degree_of_parallelism());
    EXPECT_EQ(kUpstreamDop, ai_source->degree_of_parallelism());
    EXPECT_NE(kBuilderDop, ai_source->degree_of_parallelism());
    EXPECT_FALSE(ai_source->could_local_shuffle());
    EXPECT_EQ(TPartitionType::RANDOM, ai_source->partition_type());
    EXPECT_TRUE(ai_source->is_skewed());
    EXPECT_EQ(upstream_source->get_bucket_properties(), ai_source->get_bucket_properties());
    EXPECT_FALSE(ai_source->with_morsels());

    EXPECT_EQ(ai_sink->_processor, ai_source->_processor);
    EXPECT_EQ(ai_sink->_processor->input_buffer(), ai_source->_processor->input_buffer());

    for (const OperatorFactoryPtr& factory : upstream_pipeline->_op_factories) {
        EXPECT_EQ(nullptr, dynamic_cast<ProjectOperatorFactory*>(factory.get()));
    }
    for (const OperatorFactoryPtr& factory : downstream) {
        EXPECT_EQ(nullptr, dynamic_cast<ProjectOperatorFactory*>(factory.get()));
    }
}

TEST_F(AIProjectPipelineTest, FactoryFailureClosesPairedProjectionContextsWithoutPublishingFactories) {
    ScopedEnvironment missing_credential(std::string(kApiKeyEnvironment), std::nullopt);

    auto identity_or = create_expr_context(make_slot_ref(_tuple_id, kPromptSlotId));
    ASSERT_TRUE(identity_or.ok()) << identity_or.status();
    ExprContext* identity = std::move(identity_or).value();
    auto common_or = create_expr_context(make_slot_ref(_tuple_id, kPromptSlotId));
    ASSERT_TRUE(common_or.ok()) << common_or.status();
    ExprContext* common = std::move(common_or).value();
    auto ai_or = create_expr_context(make_ai_complete(_tuple_id, kCommonSlotId));
    ASSERT_TRUE(ai_or.ok()) << ai_or.status();
    ExprContext* ai = std::move(ai_or).value();
    ASSERT_OK(identity->prepare(_runtime_state.get()));
    ASSERT_OK(common->prepare(_runtime_state.get()));
    ASSERT_OK(ai->prepare(_runtime_state.get()));
    AIProjectProjectionSpec spec(_runtime_state.get(),
                                 {
                                         AIProjectOutputSpec{.slot_id = kPromptSlotId,
                                                             .expr_ctx = identity,
                                                             .nullable = true,
                                                             .kind = AIProjectOutputKind::PASSTHROUGH},
                                         AIProjectOutputSpec{.slot_id = kAnswerSlotId,
                                                             .expr_ctx = ai,
                                                             .nullable = true,
                                                             .kind = AIProjectOutputKind::AI},
                                 },
                                 {AIProjectCommonSpec{.slot_id = kCommonSlotId, .expr_ctx = common}},
                                 std::string(kModelSentinel));

    PipelineBuilderContext context(&_fragment_context, kBuilderDop, 1);
    auto factories =
            AIProjectFactory::create(&context, 10, kUpstreamDop, std::string(kEndpointSentinel), std::move(spec));

    ASSERT_FALSE(factories.ok());
    EXPECT_TRUE(factories.status().is_invalid_argument()) << factories.status();
    EXPECT_TRUE(identity->closed());
    EXPECT_TRUE(common->closed());
    EXPECT_TRUE(ai->closed());
    EXPECT_TRUE(context._pipelines.empty());
}

TEST_F(AIProjectPipelineTest, FixedBeLocalCredentialRejectsMissingEmptyAndControlCharactersWithoutLeaks) {
    struct Case {
        const char* name;
        std::optional<std::string> value;
    };
    const std::vector<Case> cases{
            {"missing", std::nullopt},
            {"empty", std::string()},
            {"newline", "bad\ncredential"},
            {"delete", std::string("bad\x7f"
                                   "credential")},
    };

    for (const Case& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        ScopedEnvironment credential(std::string(kApiKeyEnvironment), test_case.value);
        auto submitter = AIProjectDispatcherSubmitter::create(_runtime_state.get(), std::string(kEndpointSentinel),
                                                              _config_source->snapshot());
        ASSERT_FALSE(submitter.ok());
        EXPECT_TRUE(submitter.status().is_invalid_argument()) << submitter.status();
        EXPECT_EQ(std::string::npos, submitter.status().to_string().find(kEndpointSentinel));
        EXPECT_EQ(std::string::npos, submitter.status().to_string().find(kModelSentinel));
        if (test_case.value.has_value() && !test_case.value->empty()) {
            EXPECT_EQ(std::string::npos, submitter.status().to_string().find(*test_case.value));
        }
    }

    ScopedEnvironment credential{std::string(kApiKeyEnvironment), std::string(kSecretSentinel)};
    auto submitter = AIProjectDispatcherSubmitter::create(_runtime_state.get(), std::string(kEndpointSentinel),
                                                          _config_source->snapshot());
    ASSERT_TRUE(submitter.ok()) << submitter.status();
    EXPECT_TRUE(submitter.value()->_api_key == kSecretSentinel);
    EXPECT_TRUE(submitter.value()->_endpoint == kEndpointSentinel);
    ASSERT_NE(nullptr, submitter.value()->_resolved_endpoint);
    EXPECT_EQ("127.0.0.1", submitter.value()->_resolved_endpoint->host);
    EXPECT_EQ(443, submitter.value()->_resolved_endpoint->port);
    EXPECT_EQ((std::vector<std::string>{"127.0.0.1"}), submitter.value()->_resolved_endpoint->addresses);
}

TEST_F(AIProjectPipelineTest, CredentialBindingRequiresHttpsEndpoint) {
    ScopedEnvironment credential{std::string(kApiKeyEnvironment), std::string(kSecretSentinel)};
    const std::string insecure_endpoint = "http://unit.test.invalid/v1/chat/completions";

    auto submitter =
            AIProjectDispatcherSubmitter::create(_runtime_state.get(), insecure_endpoint, _config_source->snapshot());

    ASSERT_FALSE(submitter.ok());
    EXPECT_TRUE(submitter.status().is_invalid_argument()) << submitter.status();
    EXPECT_EQ(std::string::npos, submitter.status().to_string().find(insecure_endpoint));
    EXPECT_EQ(std::string::npos, submitter.status().to_string().find(kSecretSentinel));
}

TEST_F(AIProjectPipelineTest, CredentialBindingRejectsEndpointNotAllowedByThisBe) {
    ScopedEnvironment credential{std::string(kApiKeyEnvironment), std::string(kSecretSentinel)};
    const std::string unbound_endpoint = "https://127.0.0.2:444/v1/chat/completions";

    auto submitter =
            AIProjectDispatcherSubmitter::create(_runtime_state.get(), unbound_endpoint, _config_source->snapshot());

    ASSERT_FALSE(submitter.ok());
    EXPECT_TRUE(submitter.status().is_invalid_argument()) << submitter.status();
    EXPECT_EQ(std::string::npos, submitter.status().to_string().find(unbound_endpoint));
    EXPECT_EQ(std::string::npos, submitter.status().to_string().find(kSecretSentinel));
}

TEST_F(AIProjectPipelineTest, CredentialBindingRejectsLinkLocalResolvedEndpoint) {
    ScopedEnvironment credential{std::string(kApiKeyEnvironment), std::string(kSecretSentinel)};
    const std::string link_local_endpoint = "https://169.254.169.254/v1/chat/completions";
    ScopedEnvironment endpoint_binding{std::string(kEndpointEnvironment), link_local_endpoint};

    auto submitter =
            AIProjectDispatcherSubmitter::create(_runtime_state.get(), link_local_endpoint, _config_source->snapshot());

    ASSERT_FALSE(submitter.ok());
    EXPECT_TRUE(submitter.status().is_invalid_argument()) << submitter.status();
    EXPECT_EQ(std::string::npos, submitter.status().to_string().find(link_local_endpoint));
    EXPECT_EQ(std::string::npos, submitter.status().to_string().find(kSecretSentinel));
}

TEST_F(AIProjectPipelineTest, ProductionProjectionEvaluatesCommonBeforeAiAndPreservesIdentityNullabilityAndOwner) {
    auto projection_or = create_expression_projection();
    ASSERT_TRUE(projection_or.ok()) << projection_or.status();
    auto projection = std::move(projection_or).value();
    ASSERT_OK(projection->prepare(_runtime_state.get(), 1));

    ChunkPtr input = make_prompt_chunk(0, 3, 1);
    input->owner_info().set_owner_id(71, true);
    input->owner_info().set_passthrough(true);

    auto prepared_or = projection->prepare_subchunk(_runtime_state.get(), 0, input);
    ASSERT_TRUE(prepared_or.ok()) << prepared_or.status();
    AIProjectPreparedSubchunk prepared = std::move(prepared_or).value();

    ASSERT_TRUE(input->is_slot_exist(kCommonSlotId));
    EXPECT_EQ(nullable_string(input, kPromptSlotId, 0), nullable_string(input, kCommonSlotId, 0));
    EXPECT_EQ(nullable_string(input, kPromptSlotId, 1), nullable_string(input, kCommonSlotId, 1));
    EXPECT_EQ(nullable_string(input, kPromptSlotId, 2), nullable_string(input, kCommonSlotId, 2));

    ASSERT_NE(nullptr, prepared.output_chunk);
    ASSERT_EQ(3, prepared.output_chunk->num_rows());
    ASSERT_TRUE(prepared.output_chunk->is_slot_exist(kPromptSlotId));
    ASSERT_TRUE(prepared.output_chunk->is_slot_exist(kAnswerSlotId));
    EXPECT_TRUE(prepared.output_chunk->get_column_by_slot_id(kPromptSlotId)->is_nullable());
    EXPECT_TRUE(prepared.output_chunk->get_column_by_slot_id(kAnswerSlotId)->is_nullable());
    for (size_t row = 0; row < 3; ++row) {
        EXPECT_EQ(nullable_string(input, kPromptSlotId, row),
                  nullable_string(prepared.output_chunk, kPromptSlotId, row));
        EXPECT_TRUE(nullable_string(prepared.output_chunk, kAnswerSlotId, row) == std::nullopt);
    }

    ASSERT_EQ(1, prepared.ai_outputs.size());
    EXPECT_EQ(kAnswerSlotId, prepared.ai_outputs.front().slot_id);
    EXPECT_TRUE(prepared.ai_outputs.front().replace_existing);
    ASSERT_EQ(3, prepared.ai_outputs.front().input.rows.size());
    EXPECT_EQ(AIFunctionRowAction::DISPATCH, prepared.ai_outputs.front().input.rows[0].action);
    EXPECT_EQ("prompt-0", prepared.ai_outputs.front().input.rows[0].prompt);
    EXPECT_EQ(AIFunctionRowAction::SQL_NULL, prepared.ai_outputs.front().input.rows[1].action);
    EXPECT_EQ(AIFunctionRowAction::DISPATCH, prepared.ai_outputs.front().input.rows[2].action);
    EXPECT_EQ("prompt-2", prepared.ai_outputs.front().input.rows[2].prompt);

    EXPECT_EQ(71, prepared.output_chunk->owner_info().owner_id());
    EXPECT_TRUE(prepared.output_chunk->owner_info().is_last_chunk());
    EXPECT_TRUE(prepared.output_chunk->owner_info().is_passthrough());

    projection->close(_runtime_state.get());
    projection->close(_runtime_state.get());
}

TEST_F(AIProjectPipelineTest, ExplicitModelProjectionAllowsBlankDefaultModel) {
    auto projection = create_expression_projection(true, "");
    ASSERT_TRUE(projection.ok()) << projection.status();
}

TEST_F(AIProjectPipelineTest, ZeroRowLastChunkSkipsExpressionEvaluationAndProducesNullableOutputSchema) {
    auto projection_or = create_expression_projection();
    ASSERT_TRUE(projection_or.ok()) << projection_or.status();
    auto projection = std::move(projection_or).value();
    ASSERT_OK(projection->prepare(_runtime_state.get(), 1));

    auto input = std::make_shared<Chunk>();
    input->owner_info().set_owner_id(79, true);
    input->owner_info().set_passthrough(true);
    auto prepared_or = projection->prepare_subchunk(_runtime_state.get(), 0, input);
    ASSERT_TRUE(prepared_or.ok()) << prepared_or.status();
    AIProjectPreparedSubchunk prepared = std::move(prepared_or).value();

    ASSERT_NE(nullptr, prepared.output_chunk);
    EXPECT_EQ(0, prepared.output_chunk->num_rows());
    EXPECT_TRUE(prepared.output_chunk->is_slot_exist(kPromptSlotId));
    EXPECT_TRUE(prepared.output_chunk->is_slot_exist(kAnswerSlotId));
    EXPECT_TRUE(prepared.output_chunk->get_column_by_slot_id(kPromptSlotId)->is_nullable());
    EXPECT_TRUE(prepared.output_chunk->get_column_by_slot_id(kAnswerSlotId)->is_nullable());
    ASSERT_EQ(1, prepared.ai_outputs.size());
    EXPECT_TRUE(prepared.ai_outputs.front().replace_existing);
    EXPECT_TRUE(prepared.ai_outputs.front().input.rows.empty());
    EXPECT_EQ(79, prepared.output_chunk->owner_info().owner_id());
    EXPECT_TRUE(prepared.output_chunk->owner_info().is_last_chunk());
    EXPECT_TRUE(prepared.output_chunk->owner_info().is_passthrough());

    projection->close(_runtime_state.get());
}

TEST_F(AIProjectPipelineTest, ProductionProjectionAndProcessorKeepStableRowsAcross64And1Slices) {
    auto projection_or = create_expression_projection();
    ASSERT_TRUE(projection_or.ok()) << projection_or.status();
    auto projection = std::move(projection_or).value();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto buffer_or = AIChunkBuffer::create(2, 32 * kMiB);
    ASSERT_TRUE(buffer_or.ok()) << buffer_or.status();
    auto buffer = std::move(buffer_or).value();

    AIRuntimeConfig config;
    config.sub_chunk_size = 64;
    config.on_error = "ignore";
    auto processor_or = AIProjectProcessor::create(buffer, projection, submitter, config);
    ASSERT_TRUE(processor_or.ok()) << processor_or.status();
    auto processor = std::move(processor_or).value();
    ASSERT_OK(processor->prepare(_runtime_state.get(), 1));

    ChunkPtr input = make_prompt_chunk(0, 65);
    input->owner_info().set_owner_id(83, true);
    input->owner_info().set_passthrough(true);
    auto admitted = buffer->try_put(0, input);
    ASSERT_TRUE(admitted.ok()) << admitted.status();
    ASSERT_TRUE(admitted.value());
    ASSERT_OK(buffer->set_sink_eos(0));

    ASSERT_OK(processor->try_process(_runtime_state.get(), 0));
    ASSERT_EQ(64, submitter->pending_count());
    for (size_t row = 64; row > 0; --row) {
        submitter->succeed("prompt-" + std::to_string(row - 1));
    }
    ASSERT_TRUE(processor->has_output(0));

    auto first_or = processor->pull_chunk(_runtime_state.get(), 0);
    ASSERT_TRUE(first_or.ok()) << first_or.status();
    ChunkPtr first = std::move(first_or).value();
    ASSERT_NE(nullptr, first);
    ASSERT_EQ(64, first->num_rows());
    EXPECT_EQ(83, first->owner_info().owner_id());
    EXPECT_FALSE(first->owner_info().is_last_chunk());
    EXPECT_TRUE(first->owner_info().is_passthrough());
    for (size_t row = 0; row < 64; ++row) {
        EXPECT_EQ("prompt-" + std::to_string(row), nullable_string(first, kPromptSlotId, row));
        EXPECT_EQ("result-prompt-" + std::to_string(row), nullable_string(first, kAnswerSlotId, row));
    }

    ASSERT_OK(processor->try_process(_runtime_state.get(), 0));
    ASSERT_EQ(1, submitter->pending_count());
    submitter->succeed("prompt-64");

    auto second_or = processor->pull_chunk(_runtime_state.get(), 0);
    ASSERT_TRUE(second_or.ok()) << second_or.status();
    ChunkPtr second = std::move(second_or).value();
    ASSERT_NE(nullptr, second);
    ASSERT_EQ(1, second->num_rows());
    EXPECT_EQ("prompt-64", nullable_string(second, kPromptSlotId, 0));
    EXPECT_EQ("result-prompt-64", nullable_string(second, kAnswerSlotId, 0));
    EXPECT_EQ(83, second->owner_info().owner_id());
    EXPECT_TRUE(second->owner_info().is_last_chunk());
    EXPECT_TRUE(second->owner_info().is_passthrough());

    processor->close(_runtime_state.get());
}

TEST_F(AIProjectPipelineTest, OperatorCreateIsAllocationOnlyAndPrepareConfiguresFinalDopOnce) {
    auto projection = std::make_shared<RecordingLifecycleProjection>();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto buffer_or = AIChunkBuffer::create(8, 32 * kMiB);
    ASSERT_TRUE(buffer_or.ok()) << buffer_or.status();

    AIRuntimeConfig config;
    config.on_error = "ignore";
    auto processor_or =
            AIProjectProcessor::create(std::move(buffer_or).value(), projection, submitter, std::move(config));
    ASSERT_TRUE(processor_or.ok()) << processor_or.status();
    auto processor = std::move(processor_or).value();
    AISinkOperatorFactory sink_factory(0, 10, processor);
    AISourceOperatorFactory source_factory(1, 10, processor);

    ASSERT_OK(source_factory.prepare(_runtime_state.get()));
    EXPECT_EQ(0, projection->prepare_calls);
    EXPECT_EQ(0, processor->_configured_dop);

    constexpr int32_t kFinalDop = 4;
    OperatorPtr sink0 = sink_factory.create(kFinalDop, 0);
    OperatorPtr source0 = source_factory.create(kFinalDop, 0);
    OperatorPtr source1 = source_factory.create(kFinalDop, 1);
    ASSERT_EQ(0, processor->_configured_dop);
    EXPECT_EQ(0, projection->prepare_calls);

    ASSERT_OK(sink0->prepare(_runtime_state.get()));
    ASSERT_EQ(kFinalDop, processor->_configured_dop);
    EXPECT_EQ(0, projection->prepare_calls);
    ASSERT_OK(source0->prepare(_runtime_state.get()));
    ASSERT_EQ(kFinalDop, processor->_configured_dop);
    ASSERT_OK(source1->prepare(_runtime_state.get()));
    ASSERT_EQ(1, projection->prepare_calls);
    ASSERT_EQ((std::vector<int32_t>{kFinalDop}), projection->prepared_dops);
    ASSERT_EQ((std::vector<RuntimeState*>{_runtime_state.get()}), projection->prepared_states);

    sink0->close(_runtime_state.get());
    source0->close(_runtime_state.get());
    source1->close(_runtime_state.get());
    source_factory.close(_runtime_state.get());
    EXPECT_EQ(1, projection->close_calls);
    source_factory.close(_runtime_state.get());
    EXPECT_EQ(1, projection->close_calls);
}

TEST_F(AIProjectPipelineTest, SinkPreparePropagatesFinalDopMismatch) {
    auto projection = std::make_shared<RecordingLifecycleProjection>();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto buffer_or = AIChunkBuffer::create(8, 32 * kMiB);
    ASSERT_TRUE(buffer_or.ok()) << buffer_or.status();

    AIRuntimeConfig config;
    config.on_error = "ignore";
    auto processor_or =
            AIProjectProcessor::create(std::move(buffer_or).value(), projection, submitter, std::move(config));
    ASSERT_TRUE(processor_or.ok()) << processor_or.status();
    auto processor = std::move(processor_or).value();
    AISinkOperatorFactory sink_factory(0, 10, processor);

    OperatorPtr first = sink_factory.create(4, 0);
    OperatorPtr mismatched = sink_factory.create(3, 1);
    ASSERT_OK(first->prepare(_runtime_state.get()));
    const Status status = mismatched->prepare(_runtime_state.get());
    EXPECT_TRUE(status.is_internal_error()) << status;
    EXPECT_EQ(0, projection->prepare_calls);

    first->close(_runtime_state.get());
    mismatched->close(_runtime_state.get());
    processor->close(_runtime_state.get());
}

TEST_F(AIProjectPipelineTest, ProcessorCloseReleasesUnpreparedProjectionExactlyOnce) {
    auto projection = std::make_shared<RecordingLifecycleProjection>();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto buffer_or = AIChunkBuffer::create(2, 32 * kMiB);
    ASSERT_TRUE(buffer_or.ok()) << buffer_or.status();

    AIRuntimeConfig config;
    config.on_error = "ignore";
    auto processor_or =
            AIProjectProcessor::create(std::move(buffer_or).value(), projection, submitter, std::move(config));
    ASSERT_TRUE(processor_or.ok()) << processor_or.status();
    auto processor = std::move(processor_or).value();

    processor->close(_runtime_state.get());
    processor->close(_runtime_state.get());
    EXPECT_EQ(0, projection->prepare_calls);
    EXPECT_EQ(1, projection->close_calls);
    ASSERT_EQ((std::vector<RuntimeState*>{_runtime_state.get()}), projection->closed_states);
}

TEST_F(AIProjectPipelineTest, DispatcherSnapshotsConfigAndSeparatesLiveQueryFromImmutableRequestDeadline) {
    ScopedEnvironment credential{std::string(kApiKeyEnvironment), std::string(kSecretSentinel)};
    const AIRuntimeConfig baseline = _config_source->snapshot();
    struct Case {
        const char* name;
        int64_t query_deadline_ns;
        int64_t request_timeout_ms;
        int64_t expected_request_deadline_ns;
        int64_t expected_effective_deadline_ns;
    };
    const std::vector<Case> cases{
            {"request timeout first", 120'000'000'000, 5'000, 105'000'000'000, 105'000'000'000},
            {"query timeout first", 103'000'000'000, 5'000, 105'000'000'000, 103'000'000'000},
            {"zero request timeout uses query", 120'000'000'000, 0, 0, 120'000'000'000},
    };

    for (const Case& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        set_query_deadline_ns(test_case.query_deadline_ns);
        AIRuntimeConfig snapshot = baseline;
        snapshot.request_timeout_ms = test_case.request_timeout_ms;
        snapshot.connect_timeout_ms = 0;
        snapshot.max_response_bytes = 12'345;

        auto submitter_or =
                AIProjectDispatcherSubmitter::create(_runtime_state.get(), std::string(kEndpointSentinel), snapshot);
        ASSERT_TRUE(submitter_or.ok()) << submitter_or.status();
        auto submitter = std::move(submitter_or).value();

        // A live config-source hot update after fragment construction must not
        // alter that fragment's immutable execution snapshot.
        AIRuntimeConfig changed = baseline;
        changed.request_timeout_ms = 1;
        changed.connect_timeout_ms = 37;
        changed.max_response_bytes = 54'321;
        ASSERT_OK(_config_source->update(std::move(changed)));

        std::vector<AITaskResult> results;
        auto handle_or = submitter->submit(
                AIProjectTaskRequest{
                        .task_id = 11,
                        .model = kModelSentinel,
                        .prompt = "deadline-prompt",
                },
                [&](AITaskResult result) { results.emplace_back(std::move(result)); });
        ASSERT_TRUE(handle_or.ok()) << handle_or.status();
        auto handle = std::move(handle_or).value();

        _control.run_until_idle();
        ASSERT_EQ(1, _http.pending_count());
        const AIHttpRequest& request = _http.request();
        EXPECT_EQ(test_case.expected_request_deadline_ns, request.request_deadline_ns);
        ASSERT_TRUE(request.lifecycle);
        const AILifecycleObservation before_extension =
                observe_ai_lifecycle(request.lifecycle, request.request_deadline_ns, _clock.monotonic_now_ns());
        EXPECT_EQ(AILifecycleState::ACTIVE, before_extension.state);
        EXPECT_EQ(test_case.expected_effective_deadline_ns, before_extension.effective_deadline_ns);
        EXPECT_EQ(12'345, request.max_response_bytes);
        ASSERT_NE(nullptr, request.resolved_endpoint);
        EXPECT_EQ("127.0.0.1", request.resolved_endpoint->host);
        EXPECT_EQ(443, request.resolved_endpoint->port);
        const int64_t remaining_ns = test_case.expected_request_deadline_ns - _clock.monotonic_now_ns();
        const int64_t expected_connect_ms =
                test_case.expected_request_deadline_ns == 0
                        ? 0
                        : remaining_ns / 1'000'000 + (remaining_ns % 1'000'000 != 0 ? 1 : 0);
        EXPECT_EQ(expected_connect_ms, request.connect_timeout_ms);

        set_query_deadline_ns(130'000'000'000);
        const AILifecycleObservation after_extension =
                observe_ai_lifecycle(request.lifecycle, request.request_deadline_ns, _clock.monotonic_now_ns());
        EXPECT_EQ(AILifecycleState::ACTIVE, after_extension.state);
        EXPECT_EQ(
                test_case.expected_request_deadline_ns == 0 ? 130'000'000'000 : test_case.expected_request_deadline_ns,
                after_extension.effective_deadline_ns);

        _http.complete_next(AIHttpNoResponseCode::SHUTDOWN);
        _completion.run_until_idle();
        _control.run_until_idle();
        ASSERT_EQ(1, results.size());
        ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(results.front()));
        EXPECT_EQ(AILifecycleReason::SHUTDOWN, std::get<AILifecycleCancelled>(results.front()).reason);
        handle->cancel();
        ASSERT_OK(_config_source->update(baseline));
    }
}

TEST_F(AIProjectPipelineTest, QueryCancellationRemainsLifecycleCancelledAndDoesNotBecomeIgnoredNull) {
    ScopedEnvironment credential{std::string(kApiKeyEnvironment), std::string(kSecretSentinel)};
    set_query_deadline_ns(120'000'000'000);
    AIRuntimeConfig config = _config_source->snapshot();
    config.request_timeout_ms = 5'000;
    config.connect_timeout_ms = 0;
    config.on_error = "ignore";
    auto submitter_or =
            AIProjectDispatcherSubmitter::create(_runtime_state.get(), std::string(kEndpointSentinel), config);
    ASSERT_TRUE(submitter_or.ok()) << submitter_or.status();
    auto submitter = std::move(submitter_or).value();

    std::vector<AITaskResult> results;
    auto handle_or = submitter->submit(
            AIProjectTaskRequest{
                    .task_id = 17,
                    .model = kModelSentinel,
                    .prompt = "cancel-prompt",
            },
            [&](AITaskResult result) { results.emplace_back(std::move(result)); });
    ASSERT_TRUE(handle_or.ok()) << handle_or.status();
    auto handle = std::move(handle_or).value();
    _control.run_until_idle();
    ASSERT_EQ(1, _http.pending_count());

    _query_context->cancel(Status::Cancelled("test query cancellation"), true);
    EXPECT_EQ(AILifecycleState::CANCELLED,
              observe_ai_lifecycle(_http.request().lifecycle, _http.request().request_deadline_ns,
                                   _clock.monotonic_now_ns())
                      .state);
    _http.complete_next(AIHttpNoResponseCode::CANCELLATION);
    _completion.run_until_idle();
    _control.run_until_idle();

    ASSERT_EQ(1, results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(results.front()));
    EXPECT_EQ(AILifecycleReason::CANCELLED, std::get<AILifecycleCancelled>(results.front()).reason);
    EXPECT_FALSE(std::holds_alternative<AISanitizedRowFailure>(results.front()));
    handle->cancel();
}

TEST_F(AIProjectPipelineTest, FragmentCancellationCancelsSourceRequestAndDrainsPendingFinish) {
    ScopedEnvironment credential{std::string(kApiKeyEnvironment), std::string(kSecretSentinel)};
    set_query_deadline_ns(120'000'000'000);
    AIRuntimeConfig config = _config_source->snapshot();
    config.request_timeout_ms = 5'000;
    config.connect_timeout_ms = 0;
    config.on_error = "ignore";

    auto projection_or = create_expression_projection();
    ASSERT_TRUE(projection_or.ok()) << projection_or.status();
    auto production_submitter_or =
            AIProjectDispatcherSubmitter::create(_runtime_state.get(), std::string(kEndpointSentinel), config);
    ASSERT_TRUE(production_submitter_or.ok()) << production_submitter_or.status();
    auto submitter = std::make_shared<RecordingTaskSubmitter>(std::move(production_submitter_or).value());
    auto buffer_or = AIChunkBuffer::create(2, 32 * kMiB);
    ASSERT_TRUE(buffer_or.ok()) << buffer_or.status();
    auto buffer = std::move(buffer_or).value();
    auto processor_or =
            AIProjectProcessor::create(buffer, std::move(projection_or).value(), submitter, std::move(config));
    ASSERT_TRUE(processor_or.ok()) << processor_or.status();
    auto processor = std::move(processor_or).value();

    AISourceOperatorFactory source_factory(1, 10, processor);
    ASSERT_OK(source_factory.prepare(_runtime_state.get()));
    OperatorPtr source = source_factory.create(1, 0);
    ASSERT_OK(source->prepare(_runtime_state.get()));

    auto admitted = buffer->try_put(0, make_prompt_chunk(0, 1));
    ASSERT_TRUE(admitted.ok()) << admitted.status();
    ASSERT_TRUE(admitted.value());
    ASSERT_OK(buffer->set_sink_eos(0));
    auto output_or = source->pull_chunk(_runtime_state.get());
    ASSERT_TRUE(output_or.ok()) << output_or.status();
    EXPECT_EQ(nullptr, output_or.value());
    _control.run_until_idle();
    ASSERT_EQ(1, _http.pending_count());

    workgroup::PipelineExecutorSetConfig executor_config{1, 1, 1, 1, CpuUtil::CpuIds{}, false, false, nullptr};
    NeverYieldWorkGroupSchedulePolicy schedule_policy;
    workgroup::PipelineExecutorSet executors(executor_config, "ai_project_test", CpuUtil::CpuIds{}, {},
                                             schedule_policy);
    ScopedWorkGroupExecutors scoped_executors(_workgroup, &executors);
    // LimitReach is a normal fragment-cancellation reason, so main skips
    // failure-audit reporting while still running the driver cancellation path.
    _fragment_context.cancel(Status::Cancelled("LimitReach"));
    ASSERT_TRUE(_runtime_state->is_cancelled());
    ASSERT_OK(source->set_cancelled(_runtime_state.get()));
    EXPECT_EQ(AILifecycleState::CANCELLED,
              observe_ai_lifecycle(_http.request().lifecycle, _http.request().request_deadline_ns,
                                   _clock.monotonic_now_ns())
                      .state);
    EXPECT_TRUE(source->pending_finish());

    _http.complete_next(AIHttpNoResponseCode::CANCELLATION);
    _completion.run_until_idle();
    _control.run_until_idle();

    ASSERT_EQ((std::vector<AILifecycleReason>{AILifecycleReason::CANCELLED}), submitter->lifecycle_reasons);
    EXPECT_EQ(0, submitter->sanitized_failures)
            << "lifecycle cancellation must not be converted to an ignored row failure";
    EXPECT_FALSE(source->pending_finish());
    EXPECT_FALSE(source->has_output());

    source->close(_runtime_state.get());
    source_factory.close(_runtime_state.get());
}

} // namespace
} // namespace pipeline
} // namespace starrocks
