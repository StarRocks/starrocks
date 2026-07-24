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

#include "exec/ai_project_node.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <limits>
#include <memory>
#include <string_view>
#include <unordered_set>
#include <utility>

#include "exec/pipeline/ai/ai_chunk_buffer.h"
#include "exec/pipeline/ai/ai_project_operator.h"
#include "exec/pipeline/ai/ai_project_processor.h"
#include "exec/pipeline/ai/ai_project_runtime.h"
#include "exec/pipeline/exec_node_pipeline_adapter.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec_primitive/pipeline/source_operator.h"
#include "exprs/ai/ai_function_call_expr.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/service_contexts.h"

namespace starrocks {

namespace {

constexpr std::string_view kSystemChatConfigId = "__system_chat__";
constexpr std::string_view kOpenAICompatibleProvider = "openai_compatible";
constexpr std::string_view kInvalidAIProjectPlan = "Invalid AI project plan";
constexpr std::string_view kAIRuntimeUnavailable = "AI runtime services are unavailable";
constexpr std::string_view kAIQueryMemoryUnavailable = "AI query memory tracker is unavailable";

Status invalid_ai_project_plan() {
    return Status::InvalidArgument(kInvalidAIProjectPlan);
}

bool contains_control_character(std::string_view value) {
    return std::any_of(value.begin(), value.end(), [](unsigned char c) { return c <= 0x1f || c == 0x7f; });
}

bool is_non_blank(std::string_view value) {
    return std::any_of(value.begin(), value.end(), [](unsigned char c) { return std::isspace(c) == 0; });
}

size_t count_ai_expressions(const Expr* expr) {
    if (expr == nullptr) {
        return 0;
    }
    size_t count = dynamic_cast<const AIFunctionCallExpr*>(expr) != nullptr ? 1 : 0;
    for (const Expr* child : expr->children()) {
        count += count_ai_expressions(child);
    }
    return count;
}

bool references_any_slot(const Expr* expr, const std::unordered_set<SlotId>& slots) {
    bool found = false;
    expr->for_each_slot_id([&](SlotId slot_id) { found = found || slots.contains(slot_id); });
    return found;
}

bool requires_default_model(AIFunctionSignature signature) {
    return signature == AIFunctionSignature::PROMPT || signature == AIFunctionSignature::PROMPT_OPTIONS;
}

Status create_ai_project_expr(ObjectPool* pool, const TExpr& thrift_expr, RuntimeState* state, ExprContext** expr_ctx) {
    *expr_ctx = nullptr;
    const Status status = ExprFactory::create_expr_tree(pool, thrift_expr, expr_ctx, state, true);
    if (!status.ok() || *expr_ctx == nullptr || (*expr_ctx)->root() == nullptr) {
        return invalid_ai_project_plan();
    }
    return Status::OK();
}

} // namespace

AIProjectNode::AIProjectNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : PipelineNode(pool, tnode, descs) {}

AIProjectNode::~AIProjectNode() {
    if (runtime_state() != nullptr) {
        close(runtime_state());
    }
}

Status AIProjectNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));

    if (tnode.node_type != TPlanNodeType::AI_PROJECT_NODE || !tnode.__isset.ai_project_node ||
        tnode.num_children != 1 || _children.size() != 1 || !_conjunct_ctxs.empty() ||
        row_desc().tuple_descriptors().size() != 1) {
        return invalid_ai_project_plan();
    }

    const TAIProjectNode& thrift_project = tnode.ai_project_node;
    if (!thrift_project.__isset.slot_map || !thrift_project.__isset.common_slot_map ||
        !thrift_project.__isset.ai_model_configs || thrift_project.slot_map.empty() ||
        thrift_project.ai_model_configs.size() != 1) {
        return invalid_ai_project_plan();
    }

    const TupleDescriptor* output_tuple = row_desc().tuple_descriptors().front();
    if (output_tuple == nullptr) {
        return invalid_ai_project_plan();
    }

    const auto config_it = thrift_project.ai_model_configs.find(std::string(kSystemChatConfigId));
    if (config_it == thrift_project.ai_model_configs.end()) {
        return invalid_ai_project_plan();
    }
    const TAIModelConfiguration& model_config = config_it->second;
    if (!model_config.__isset.chat || !model_config.chat.__isset.endpoint || !model_config.chat.__isset.model ||
        !model_config.chat.__isset.provider || !is_non_blank(model_config.chat.endpoint) ||
        contains_control_character(model_config.chat.endpoint) ||
        model_config.chat.provider != kOpenAICompatibleProvider ||
        contains_control_character(model_config.chat.model)) {
        return invalid_ai_project_plan();
    }

    _endpoint = model_config.chat.endpoint;
    _default_model = model_config.chat.model;

    _output_slot_ids.reserve(thrift_project.slot_map.size());
    _output_expr_ctxs.reserve(thrift_project.slot_map.size());
    _output_nullables.reserve(thrift_project.slot_map.size());
    _output_is_ai.reserve(thrift_project.slot_map.size());

    std::unordered_set<SlotId> output_slots;
    std::unordered_set<SlotId> ai_output_slots;
    bool has_ai_output = false;
    bool needs_default_model = false;

    for (const auto& [slot_id, thrift_expr] : thrift_project.slot_map) {
        const SlotDescriptor* slot_desc = output_tuple->get_slot_by_id(slot_id);
        if (slot_desc == nullptr || !slot_desc->is_materialized() || !output_slots.emplace(slot_id).second) {
            return invalid_ai_project_plan();
        }

        ExprContext* expr_ctx = nullptr;
        RETURN_IF_ERROR(create_ai_project_expr(_pool, thrift_expr, state, &expr_ctx));
        _output_slot_ids.emplace_back(slot_id);
        _output_expr_ctxs.emplace_back(expr_ctx);
        _output_nullables.emplace_back(slot_desc->is_nullable());

        Expr* root = expr_ctx->root();
        if (auto* column_ref = dynamic_cast<ColumnRef*>(root); column_ref != nullptr) {
            if (column_ref->slot_id() != slot_id || !column_ref->children().empty() ||
                count_ai_expressions(root) != 0) {
                return invalid_ai_project_plan();
            }
            _output_is_ai.emplace_back(false);
            continue;
        }

        auto* ai_expr = dynamic_cast<AIFunctionCallExpr*>(root);
        if (ai_expr == nullptr || count_ai_expressions(root) != 1 ||
            ai_expr->model_config_id() != kSystemChatConfigId) {
            return invalid_ai_project_plan();
        }
        _output_is_ai.emplace_back(true);
        ai_output_slots.emplace(slot_id);
        has_ai_output = true;
        needs_default_model = needs_default_model || requires_default_model(ai_expr->signature());
    }

    if (!has_ai_output || (needs_default_model && !is_non_blank(_default_model))) {
        return invalid_ai_project_plan();
    }
    for (size_t i = 0; i < _output_expr_ctxs.size(); ++i) {
        if (_output_is_ai[i] && references_any_slot(_output_expr_ctxs[i]->root(), ai_output_slots)) {
            return invalid_ai_project_plan();
        }
    }

    _common_slot_ids.reserve(thrift_project.common_slot_map.size());
    _common_expr_ctxs.reserve(thrift_project.common_slot_map.size());
    for (const auto& [slot_id, thrift_expr] : thrift_project.common_slot_map) {
        const SlotDescriptor* slot_desc = output_tuple->get_slot_by_id(slot_id);
        if (slot_desc == nullptr || output_slots.contains(slot_id)) {
            return invalid_ai_project_plan();
        }

        ExprContext* expr_ctx = nullptr;
        RETURN_IF_ERROR(create_ai_project_expr(_pool, thrift_expr, state, &expr_ctx));
        _common_slot_ids.emplace_back(slot_id);
        _common_expr_ctxs.emplace_back(expr_ctx);
        if (count_ai_expressions(expr_ctx->root()) != 0 || references_any_slot(expr_ctx->root(), ai_output_slots)) {
            return invalid_ai_project_plan();
        }
    }

    return Status::OK();
}

void AIProjectNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }
    if (!_expressions_transferred) {
        ExprExecutor::close(_output_expr_ctxs, state);
        ExprExecutor::close(_common_expr_ctxs, state);
    }
    ExecNode::close(state);
}

void AIProjectNode::push_down_join_runtime_filter(RuntimeState* state, RuntimeFilterProbeCollector* collector) {
    if (collector == nullptr || collector->empty()) {
        return;
    }
    _runtime_filter_collector.push_down(state, id(), collector, _tuple_ids, _local_rf_waiting_set);
}

void AIProjectNode::push_down_tuple_slot_mappings(RuntimeState* state,
                                                  const std::vector<TupleSlotMapping>& parent_mappings) {
    _tuple_slot_mappings = parent_mappings;
    const std::vector<TupleSlotMapping> empty_mappings;
    for (ExecNode* child : _children) {
        child->push_down_tuple_slot_mappings(state, empty_mappings);
    }
}

StatusOr<pipeline::OpFactories> AIProjectNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    if (context == nullptr || _expressions_transferred || _children.size() != 1) {
        return Status::InternalError(kInvalidAIProjectPlan);
    }

    ASSIGN_OR_RETURN(auto upstream_operators, _children.front()->decompose_to_pipeline(context));
    if (upstream_operators.empty()) {
        return Status::InternalError(kInvalidAIProjectPlan);
    }
    SourceOperatorFactory* upstream_source = context->source_operator(upstream_operators);
    if (upstream_source == nullptr || upstream_source->degree_of_parallelism() == 0 ||
        upstream_source->degree_of_parallelism() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        return Status::InternalError(kInvalidAIProjectPlan);
    }
    const size_t upstream_dop = upstream_source->degree_of_parallelism();

    RuntimeState* state = context->runtime_state();
    const QueryExecutionServices* services = state == nullptr ? nullptr : state->query_execution_services();
    if (services == nullptr || services->ai == nullptr || services->ai->config_source == nullptr) {
        return Status::InternalError(kAIRuntimeUnavailable);
    }
    const auto query_mem_tracker = state->query_mem_tracker_ptr();
    if (query_mem_tracker == nullptr) {
        return Status::InternalError(kAIQueryMemoryUnavailable);
    }
    const AIRuntimeConfig config = services->ai->config_source->snapshot();

    ASSIGN_OR_RETURN(const size_t buffer_capacity, AIChunkBuffer::capacity_for_dop(upstream_dop));
    const int64_t query_memory_limit = query_mem_tracker->limit();
    ASSIGN_OR_RETURN(const size_t buffer_memory_limit, AIChunkBuffer::memory_limit_for_query(query_memory_limit));
    ASSIGN_OR_RETURN(auto input_buffer, AIChunkBuffer::create(static_cast<int64_t>(buffer_capacity),
                                                              static_cast<int64_t>(buffer_memory_limit)));

    std::vector<AIProjectExpressionOutput> outputs;
    outputs.reserve(_output_slot_ids.size());
    for (size_t i = 0; i < _output_slot_ids.size(); ++i) {
        outputs.emplace_back(AIProjectExpressionOutput{
                .slot_id = _output_slot_ids[i],
                .expr_ctx = _output_expr_ctxs[i],
                .nullable = _output_nullables[i],
                .is_ai = _output_is_ai[i],
        });
    }

    ASSIGN_OR_RETURN(auto projection, AIProjectExpressionProjection::create(std::move(outputs), _common_slot_ids,
                                                                            _common_expr_ctxs, _default_model));
    ASSIGN_OR_RETURN(auto submitter, AIProjectDispatcherSubmitter::create(state, _endpoint, config));
    ASSIGN_OR_RETURN(auto processor, AIProjectProcessor::create(std::move(input_buffer), std::move(projection),
                                                                std::move(submitter), config));

    auto sink = std::make_shared<AIBufferSinkOperatorFactory>(context->next_operator_id(), id(), processor);
    auto source = std::make_shared<AISourceOperatorFactory>(context->next_operator_id(), id(), processor);

    context->inherit_upstream_source_properties(source.get(), upstream_source);
    source->set_skewed(upstream_source->is_skewed());
    source->set_bucket_properties(upstream_source->get_bucket_properties());

    upstream_operators.emplace_back(std::move(sink));
    context->add_pipeline(upstream_operators);

    auto rf_collector = std::make_shared<RcRfProbeCollector>(1, std::move(runtime_filter_collector()));
    init_runtime_filter_for_operator(*this, source.get(), context, rf_collector);

    OpFactories downstream_operators;
    downstream_operators.emplace_back(std::move(source));
    if (limit() != -1) {
        downstream_operators.emplace_back(
                std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }

    _expressions_transferred = true;
    _output_expr_ctxs.clear();
    _common_expr_ctxs.clear();
    return downstream_operators;
}

} // namespace starrocks
