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

#include "exec/pipeline/ai/ai_project_factory.h"

#include <limits>
#include <new>
#include <string_view>
#include <utility>

#include "exec/pipeline/ai/ai_chunk_buffer.h"
#include "exec/pipeline/ai/ai_project_operator.h"
#include "exec/pipeline/ai/ai_project_processor.h"
#include "exec/pipeline/ai/ai_project_runtime.h"
#include "exec/runtime/pipeline_builder_context.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/service_contexts.h"

namespace starrocks::pipeline {
namespace {

constexpr std::string_view kAIRuntimeUnavailable = "AI runtime services are unavailable";
constexpr std::string_view kAIQueryMemoryUnavailable = "AI query memory tracker is unavailable";

bool appeared_before(const std::vector<AIProjectOutputSpec>& outputs,
                     const std::vector<AIProjectCommonSpec>& common_outputs, size_t output_limit, size_t common_limit,
                     const ExprContext* context) noexcept {
    for (size_t index = 0; index < output_limit; ++index) {
        if (outputs[index].expr_ctx == context) {
            return true;
        }
    }
    for (size_t index = 0; index < common_limit; ++index) {
        if (common_outputs[index].expr_ctx == context) {
            return true;
        }
    }
    return false;
}

void close_context_noexcept(ExprContext* context, RuntimeState* state) noexcept {
    if (context == nullptr || state == nullptr) {
        return;
    }
    try {
        ExprExecutor::close(context, state);
    } catch (...) {
    }
}

} // namespace

AIProjectProjectionSpec::AIProjectProjectionSpec(RuntimeState* state, std::vector<AIProjectOutputSpec> outputs,
                                                 std::vector<AIProjectCommonSpec> common_outputs,
                                                 std::string default_model)
        : _state(state),
          _outputs(std::move(outputs)),
          _common_outputs(std::move(common_outputs)),
          _default_model(std::move(default_model)) {}

AIProjectProjectionSpec::~AIProjectProjectionSpec() {
    close();
}

AIProjectProjectionSpec::AIProjectProjectionSpec(AIProjectProjectionSpec&& other) noexcept
        : _state(other._state),
          _outputs(std::move(other._outputs)),
          _common_outputs(std::move(other._common_outputs)),
          _default_model(std::move(other._default_model)),
          _closed(other._closed) {
    other._state = nullptr;
    other._outputs.clear();
    other._common_outputs.clear();
    other._closed = true;
}

AIProjectProjectionSpec& AIProjectProjectionSpec::operator=(AIProjectProjectionSpec&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    close();
    _state = other._state;
    _outputs = std::move(other._outputs);
    _common_outputs = std::move(other._common_outputs);
    _default_model = std::move(other._default_model);
    _closed = other._closed;
    other._state = nullptr;
    other._outputs.clear();
    other._common_outputs.clear();
    other._closed = true;
    return *this;
}

AIProjectOutputSpec& AIProjectProjectionSpec::add_output(AIProjectOutputSpec output) {
    _outputs.emplace_back(std::move(output));
    return _outputs.back();
}

AIProjectCommonSpec& AIProjectProjectionSpec::add_common_output(AIProjectCommonSpec output) {
    _common_outputs.emplace_back(std::move(output));
    return _common_outputs.back();
}

void AIProjectProjectionSpec::close(RuntimeState* state) noexcept {
    if (_closed) {
        return;
    }
    RuntimeState* close_state = state != nullptr ? state : _state;
    for (size_t index = 0; index < _outputs.size(); ++index) {
        ExprContext* context = _outputs[index].expr_ctx;
        if (!appeared_before(_outputs, _common_outputs, index, 0, context)) {
            close_context_noexcept(context, close_state);
        }
    }
    for (size_t index = 0; index < _common_outputs.size(); ++index) {
        ExprContext* context = _common_outputs[index].expr_ctx;
        if (!appeared_before(_outputs, _common_outputs, _outputs.size(), index, context)) {
            close_context_noexcept(context, close_state);
        }
    }
    _outputs.clear();
    _common_outputs.clear();
    _closed = true;
}

StatusOr<AIProjectOperatorFactories> AIProjectFactory::create(PipelineBuilderContext* context, int32_t plan_node_id,
                                                              size_t upstream_dop, std::string endpoint,
                                                              AIProjectProjectionSpec projection_spec) {
    if (context == nullptr || upstream_dop == 0 ||
        upstream_dop > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        return Status::InvalidArgument("Invalid AI project factory input");
    }

    RuntimeState* state = context->runtime_state();
    if (projection_spec.runtime_state() != state) {
        return Status::InvalidArgument("AI project projection belongs to a different runtime state");
    }
    const QueryExecutionServices* services = state == nullptr ? nullptr : state->query_execution_services();
    if (services == nullptr || services->ai == nullptr || services->ai->config_source == nullptr) {
        return Status::InternalError(kAIRuntimeUnavailable);
    }
    const auto query_mem_tracker = state->query_mem_tracker_ptr();
    if (query_mem_tracker == nullptr) {
        return Status::InternalError(kAIQueryMemoryUnavailable);
    }

    try {
        const AIRuntimeConfig config = services->ai->config_source->snapshot();
        ASSIGN_OR_RETURN(const size_t buffer_capacity, AIChunkBuffer::capacity_for_dop(upstream_dop));
        ASSIGN_OR_RETURN(const size_t buffer_memory_limit,
                         AIChunkBuffer::memory_limit_for_query(query_mem_tracker->limit()));
        ASSIGN_OR_RETURN(auto input_buffer, AIChunkBuffer::create(static_cast<int64_t>(buffer_capacity),
                                                                  static_cast<int64_t>(buffer_memory_limit)));
        ASSIGN_OR_RETURN(auto projection, AIProjectExpressionProjection::create(std::move(projection_spec)));
        ASSIGN_OR_RETURN(auto submitter, AIProjectDispatcherSubmitter::create(state, std::move(endpoint), config));
        ASSIGN_OR_RETURN(auto processor, AIProjectProcessor::create(std::move(input_buffer), std::move(projection),
                                                                    std::move(submitter), config));

        auto sink = std::make_shared<AISinkOperatorFactory>(context->next_operator_id(), plan_node_id, processor);
        auto source = std::make_shared<AISourceOperatorFactory>(context->next_operator_id(), plan_node_id, processor);
        return AIProjectOperatorFactories{.sink = std::move(sink), .source = std::move(source)};
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Failed to allocate AI project feature graph");
    } catch (...) {
        return Status::InternalError("Failed to create AI project feature graph");
    }
}

} // namespace starrocks::pipeline
