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
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "exec/pipeline/ai/ai_project_operator.h"
#include "exec/pipeline/exec_node_pipeline_adapter.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec_primitive/pipeline/source_operator.h"
#include "exprs/ai/ai_function_call_expr.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "exprs/expr_factory.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace starrocks {

namespace {

constexpr std::string_view kSystemChatConfigId = "__system_chat__";
constexpr std::string_view kOpenAICompatibleProvider = "openai_compatible";
constexpr std::string_view kInvalidAIProjectPlan = "Invalid AI project plan";

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
        tnode.num_children != 1 || _children.size() != 1 || !_conjunct_ctxs.empty()) {
        return invalid_ai_project_plan();
    }

    const TAIProjectNode& thrift_project = tnode.ai_project_node;
    if (!thrift_project.__isset.slot_map || !thrift_project.__isset.ai_model_configs ||
        thrift_project.slot_map.empty() || thrift_project.ai_model_configs.size() != 1) {
        return invalid_ai_project_plan();
    }

    std::unordered_map<SlotId, const SlotDescriptor*> record_slots;
    record_slots.reserve(record_desc().num_slots());
    for (const SlotDescriptor* slot : record_desc().slots()) {
        if (slot == nullptr || !record_slots.emplace(slot->id(), slot).second) {
            return invalid_ai_project_plan();
        }
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
    pipeline::AIProjectProjectionSpec projection_spec(state, {}, {}, model_config.chat.model);

    std::unordered_set<SlotId> output_slots;
    std::unordered_set<SlotId> ai_output_slots;
    bool has_ai_output = false;
    bool needs_default_model = false;

    for (const auto& [slot_id, thrift_expr] : thrift_project.slot_map) {
        const auto slot = record_slots.find(slot_id);
        if (slot == record_slots.end() || !slot->second->is_materialized() || !output_slots.emplace(slot_id).second) {
            return invalid_ai_project_plan();
        }
        const SlotDescriptor* slot_desc = slot->second;

        ExprContext* expr_ctx = nullptr;
        RETURN_IF_ERROR(create_ai_project_expr(_pool, thrift_expr, state, &expr_ctx));
        auto& output = projection_spec.add_output(pipeline::AIProjectOutputSpec{
                .slot_id = slot_id,
                .expr_ctx = expr_ctx,
                .nullable = slot_desc->is_nullable(),
                .kind = pipeline::AIProjectOutputKind::PASSTHROUGH,
        });

        Expr* root = expr_ctx->root();
        if (auto* column_ref = dynamic_cast<ColumnRef*>(root); column_ref != nullptr) {
            if (column_ref->slot_id() != slot_id || !column_ref->children().empty() ||
                count_ai_expressions(root) != 0) {
                return invalid_ai_project_plan();
            }
            continue;
        }

        auto* ai_expr = dynamic_cast<AIFunctionCallExpr*>(root);
        if (ai_expr == nullptr || count_ai_expressions(root) != 1 ||
            ai_expr->model_config_id() != kSystemChatConfigId) {
            return invalid_ai_project_plan();
        }
        output.kind = pipeline::AIProjectOutputKind::AI;
        ai_output_slots.emplace(slot_id);
        has_ai_output = true;
        needs_default_model = needs_default_model || requires_default_model(ai_expr->signature());
    }

    if (!has_ai_output || (needs_default_model && !is_non_blank(projection_spec.default_model()))) {
        return invalid_ai_project_plan();
    }
    for (const pipeline::AIProjectOutputSpec& output : projection_spec.outputs()) {
        if (output.kind == pipeline::AIProjectOutputKind::AI &&
            references_any_slot(output.expr_ctx->root(), ai_output_slots)) {
            return invalid_ai_project_plan();
        }
    }

    for (const auto& [slot_id, thrift_expr] : thrift_project.common_slot_map) {
        // FE common-expression slots are hidden temporaries. They are marked
        // non-materialized and therefore intentionally absent from the wire
        // DescriptorTable/RecordDescriptor. Only projected outputs are
        // resolved through record_slots.
        if (record_slots.contains(slot_id) || output_slots.contains(slot_id)) {
            return invalid_ai_project_plan();
        }

        ExprContext* expr_ctx = nullptr;
        RETURN_IF_ERROR(create_ai_project_expr(_pool, thrift_expr, state, &expr_ctx));
        projection_spec.add_common_output(pipeline::AIProjectCommonSpec{.slot_id = slot_id, .expr_ctx = expr_ctx});
        if (count_ai_expressions(expr_ctx->root()) != 0 || references_any_slot(expr_ctx->root(), ai_output_slots)) {
            return invalid_ai_project_plan();
        }
    }

    _projection_spec = std::move(projection_spec);
    return Status::OK();
}

void AIProjectNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }
    _projection_spec.close(state);
    ExecNode::close(state);
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

    if (context == nullptr || !_projection_spec.valid() || _projection_spec.empty() || _children.size() != 1) {
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

    ASSIGN_OR_RETURN(auto ai_factories,
                     AIProjectFactory::create(context, id(), upstream_dop, _endpoint, std::move(_projection_spec)));
    auto sink = std::move(ai_factories.sink);
    auto source = std::move(ai_factories.source);

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

    return downstream_operators;
}

} // namespace starrocks
