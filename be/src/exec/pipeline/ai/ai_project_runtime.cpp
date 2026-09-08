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

#include "exec/pipeline/ai/ai_project_runtime.h"

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <new>
#include <unordered_set>
#include <utility>

#include "base/utility/scoped_cleanup.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "compute_env/global_dict/parser.h"
#include "compute_env/query/fragment_runtime_state.h"
#include "compute_env/workgroup/work_group.h"
#include "exec/pipeline/ai/ai_query_memory_account.h"
#include "exec/runtime/query_context.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "platform/llm/ai_http_client.h"
#include "runtime/runtime_state.h"
#include "runtime/service_contexts.h"

namespace starrocks::pipeline {
namespace {

constexpr std::string_view kApiKeyEnvironment = "AI_FUNCTION_MODEL_API_KEY";
constexpr std::string_view kEndpointEnvironment = "AI_FUNCTION_MODEL_ENDPOINT";
constexpr std::string_view kSystemChatConfigId = "__system_chat__";
constexpr std::string_view kSystemEmbeddingConfigId = "__system_text_embedding__";
constexpr int64_t kNanosecondsPerMillisecond = 1'000'000;

Status invalid_projection() {
    return Status::InvalidArgument("Invalid AI project expression projection");
}

bool contains_ai_expression(const Expr* expr) {
    if (expr == nullptr) {
        return false;
    }
    if (dynamic_cast<const AIFunctionCallExpr*>(expr) != nullptr) {
        return true;
    }
    return std::any_of(expr->children().begin(), expr->children().end(), contains_ai_expression);
}

bool contains_only_ascii_whitespace(std::string_view value) {
    if (value.empty()) {
        return true;
    }
    return std::all_of(value.begin(), value.end(), [](unsigned char byte) {
        return byte == ' ' || byte == '\t' || byte == '\n' || byte == '\r' || byte == '\f' || byte == '\v';
    });
}

bool contains_control_character(std::string_view value) {
    return std::any_of(value.begin(), value.end(), [](unsigned char byte) { return byte <= 0x1f || byte == 0x7f; });
}

bool has_invalid_api_key_byte(std::string_view api_key) {
    return api_key.empty() || contains_control_character(api_key);
}

int64_t saturating_timeout_deadline(int64_t now_ns, int64_t timeout_ms) {
    if (timeout_ms <= 0) {
        return 0;
    }
    if (timeout_ms > std::numeric_limits<int64_t>::max() / kNanosecondsPerMillisecond) {
        return std::numeric_limits<int64_t>::max();
    }
    const int64_t timeout_ns = timeout_ms * kNanosecondsPerMillisecond;
    if (now_ns > std::numeric_limits<int64_t>::max() - timeout_ns) {
        return std::numeric_limits<int64_t>::max();
    }
    return now_ns + timeout_ns;
}

template <typename Function>
void clear_function_in_physical_scope(const AIMemoryContext& memory, Function* function) noexcept {
    try {
        auto clear = [&] { Function().swap(*function); };
        memory.run_in_physical_scope([](void* opaque) { (*static_cast<decltype(clear)*>(opaque))(); }, &clear);
    } catch (...) {
        Function().swap(*function);
    }
}

class DispatcherTaskHandle final : public AIProjectTaskHandle {
public:
    void set_handle(AITaskHandle handle) noexcept { _handle = std::move(handle); }

    void cancel() noexcept override {
        try {
            _handle.cancel();
        } catch (...) {
        }
    }

private:
    AITaskHandle _handle;
};

void close_expr_contexts_noexcept(const std::vector<ExprContext*>& contexts, RuntimeState* state) noexcept {
    for (ExprContext* context : contexts) {
        try {
            ExprExecutor::close(context, state);
        } catch (...) {
        }
    }
}

} // namespace

StatusOr<std::shared_ptr<AIProjectExpressionProjection>> AIProjectExpressionProjection::create(
        AIProjectProjectionSpec spec) {
    if (spec.empty()) {
        return invalid_projection();
    }

    try {
        const auto& outputs = spec.outputs();
        const auto& common_outputs = spec.common_outputs();
        std::unordered_set<SlotId> slot_ids;
        std::unordered_set<const ExprContext*> contexts;
        slot_ids.reserve(outputs.size() + common_outputs.size());
        contexts.reserve(outputs.size() + common_outputs.size());

        for (const AIProjectCommonSpec& common : common_outputs) {
            ExprContext* context = common.expr_ctx;
            if (context == nullptr || context->root() == nullptr || contains_ai_expression(context->root()) ||
                !slot_ids.emplace(common.slot_id).second || !contexts.emplace(context).second) {
                return invalid_projection();
            }
        }

        for (const AIProjectOutputSpec& output : outputs) {
            if (output.expr_ctx == nullptr || output.expr_ctx->root() == nullptr ||
                !slot_ids.emplace(output.slot_id).second || !contexts.emplace(output.expr_ctx).second) {
                return invalid_projection();
            }

            Expr* root = output.expr_ctx->root();
            if (output.kind == AIProjectOutputKind::AI) {
                const auto* ai_expression = dynamic_cast<const AIFunctionCallExpr*>(root);
                if (ai_expression == nullptr) {
                    return invalid_projection();
                }
                const auto route = spec.model_configs().find(ai_expression->model_config_id());
                if (route == spec.model_configs().end() || route->second.capability != ai_expression->capability() ||
                    route->second.source != ai_expression->model_source() ||
                    (ai_expression->requires_default_model() && contains_only_ascii_whitespace(route->second.model))) {
                    return invalid_projection();
                }
                continue;
            }
            if (output.kind != AIProjectOutputKind::PASSTHROUGH) {
                return invalid_projection();
            }

            const auto* column_ref = dynamic_cast<const ColumnRef*>(root);
            if (contains_ai_expression(root) || column_ref == nullptr || column_ref->slot_id() != output.slot_id) {
                return invalid_projection();
            }
        }
        return std::shared_ptr<AIProjectExpressionProjection>(new AIProjectExpressionProjection(std::move(spec)));
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Failed to allocate AI project expression projection");
    }
}

AIProjectExpressionProjection::AIProjectExpressionProjection(AIProjectProjectionSpec spec) : _spec(std::move(spec)) {}

AIProjectExpressionProjection::~AIProjectExpressionProjection() {
    close(_prepared_state);
}

Status AIProjectExpressionProjection::prepare(RuntimeState* state, int32_t dop) {
    if (state == nullptr || state != _spec.runtime_state() || dop <= 0 || state->obj_pool() == nullptr) {
        return Status::InvalidArgument("AI project expression runtime is invalid");
    }

    std::lock_guard lock(_lifecycle_mutex);
    if (_closed) {
        return Status::InternalError("AI project expression projection is already closed");
    }
    if (_prepare_attempted) {
        if (_prepared_state != state || _dop != dop) {
            return Status::InternalError("AI project expression projection was prepared with a different runtime");
        }
        return _lifecycle_status;
    }

    _prepare_attempted = true;
    _prepared_state = state;
    _dop = dop;

    std::vector<ExprContext*> output_expr_ctxs;
    std::vector<ExprContext*> common_expr_ctxs;
    std::vector<SlotId> output_slot_ids;
    std::vector<SlotId> common_slot_ids;
    std::vector<DriverExpressionContexts> drivers;
    try {
        output_expr_ctxs.reserve(_spec.outputs().size());
        output_slot_ids.reserve(_spec.outputs().size());
        common_expr_ctxs.reserve(_spec.common_outputs().size());
        common_slot_ids.reserve(_spec.common_outputs().size());
        for (const AIProjectCommonSpec& common : _spec.common_outputs()) {
            common_expr_ctxs.emplace_back(common.expr_ctx);
            common_slot_ids.emplace_back(common.slot_id);
        }
        for (const AIProjectOutputSpec& output : _spec.outputs()) {
            output_expr_ctxs.emplace_back(output.expr_ctx);
            output_slot_ids.emplace_back(output.slot_id);
        }

        _lifecycle_status = ExprExecutor::prepare(common_expr_ctxs, state);
        if (_lifecycle_status.ok()) {
            _lifecycle_status = ExprExecutor::prepare(output_expr_ctxs, state);
        }
        if (_lifecycle_status.ok()) {
            DictOptimizeParser::set_output_slot_id(&common_expr_ctxs, common_slot_ids);
            DictOptimizeParser::set_output_slot_id(&output_expr_ctxs, output_slot_ids);
            _lifecycle_status = ExprExecutor::open(common_expr_ctxs, state);
        }
        if (_lifecycle_status.ok()) {
            _lifecycle_status = ExprExecutor::open(output_expr_ctxs, state);
        }

        if (_lifecycle_status.ok()) {
            drivers.reserve(dop);
            for (int32_t driver_sequence = 0; driver_sequence < dop; ++driver_sequence) {
                drivers.emplace_back();
                DriverExpressionContexts& driver = drivers.back();
                driver.outputs.reserve(output_expr_ctxs.size());
                driver.common.reserve(common_expr_ctxs.size());
                for (ExprContext* context : output_expr_ctxs) {
                    ExprContext* clone = nullptr;
                    try {
                        _lifecycle_status = context->clone(state, state->obj_pool(), &clone);
                    } catch (...) {
                        if (clone != nullptr) {
                            driver.outputs.emplace_back(clone);
                        }
                        throw;
                    }
                    if (clone != nullptr) {
                        driver.outputs.emplace_back(clone);
                    }
                    if (!_lifecycle_status.ok() || clone == nullptr) {
                        if (_lifecycle_status.ok()) {
                            _lifecycle_status =
                                    Status::InternalError("Failed to clone AI project output expression context");
                        }
                        break;
                    }
                }
                if (_lifecycle_status.ok()) {
                    for (ExprContext* context : common_expr_ctxs) {
                        ExprContext* clone = nullptr;
                        try {
                            _lifecycle_status = context->clone(state, state->obj_pool(), &clone);
                        } catch (...) {
                            if (clone != nullptr) {
                                driver.common.emplace_back(clone);
                            }
                            throw;
                        }
                        if (clone != nullptr) {
                            driver.common.emplace_back(clone);
                        }
                        if (!_lifecycle_status.ok() || clone == nullptr) {
                            if (_lifecycle_status.ok()) {
                                _lifecycle_status =
                                        Status::InternalError("Failed to clone AI project common expression context");
                            }
                            break;
                        }
                    }
                }
                if (!_lifecycle_status.ok()) {
                    break;
                }
            }
        }
    } catch (const std::bad_alloc&) {
        _lifecycle_status = Status::MemoryLimitExceeded("Failed to prepare AI project expression contexts");
    } catch (...) {
        _lifecycle_status = Status::InternalError("Failed to prepare AI project expression contexts");
    }

    if (!_lifecycle_status.ok()) {
        _close_contexts(state, &drivers);
        return _lifecycle_status;
    }
    _drivers = std::move(drivers);
    return Status::OK();
}

void AIProjectExpressionProjection::close(RuntimeState* state) {
    std::vector<DriverExpressionContexts> drivers;
    RuntimeState* prepared_state = nullptr;
    {
        std::lock_guard lock(_lifecycle_mutex);
        if (_closed) {
            return;
        }
        _closed = true;
        prepared_state = _prepared_state != nullptr ? _prepared_state : state;
        drivers = std::move(_drivers);
    }
    _close_contexts(prepared_state, &drivers);
    _spec.close(prepared_state);
}

void AIProjectExpressionProjection::_close_contexts(RuntimeState* state,
                                                    std::vector<DriverExpressionContexts>* drivers) noexcept {
    if (state == nullptr) {
        return;
    }
    for (DriverExpressionContexts& driver : *drivers) {
        close_expr_contexts_noexcept(driver.outputs, state);
        close_expr_contexts_noexcept(driver.common, state);
    }
}

StatusOr<AIProjectPreparedSubchunk> AIProjectExpressionProjection::prepare_subchunk(RuntimeState* state,
                                                                                    int32_t driver_sequence,
                                                                                    const ChunkPtr& input) {
    if (state == nullptr || input == nullptr) {
        return Status::InvalidArgument("AI project expression input cannot be null");
    }

    const DriverExpressionContexts* driver = nullptr;
    {
        std::lock_guard lock(_lifecycle_mutex);
        if (!_prepare_attempted) {
            return Status::InternalError("AI project expression projection is not prepared");
        }
        if (!_lifecycle_status.ok()) {
            return _lifecycle_status;
        }
        if (_closed) {
            return Status::InternalError("AI project expression projection is closed");
        }
        if (_prepared_state != state || driver_sequence < 0 || driver_sequence >= _dop ||
            static_cast<size_t>(driver_sequence) >= _drivers.size()) {
            return Status::InvalidArgument("AI project expression driver is invalid");
        }
        driver = &_drivers[driver_sequence];
    }

    try {
        AIProjectPreparedSubchunk prepared;
        prepared.output_chunk = std::make_shared<Chunk>();
        prepared.ai_outputs.reserve(_spec.outputs().size());
        const size_t rows = input->num_rows();

        if (rows == 0) {
            for (size_t index = 0; index < _spec.outputs().size(); ++index) {
                const AIProjectOutputSpec& output = _spec.outputs()[index];
                const bool is_ai = output.kind == AIProjectOutputKind::AI;
                MutableColumnPtr column =
                        ColumnHelper::create_column(driver->outputs[index]->root()->type(), is_ai || output.nullable);
                prepared.output_chunk->append_column(std::move(column), output.slot_id);
                if (is_ai) {
                    const auto* ai_expression = dynamic_cast<const AIFunctionCallExpr*>(driver->outputs[index]->root());
                    if (ai_expression == nullptr) {
                        return invalid_projection();
                    }
                    AIProjectPreparedOutput ai_output;
                    ai_output.slot_id = output.slot_id;
                    ai_output.replace_existing = true;
                    ai_output.result_type = ai_expression->type();
                    ai_output.result_kind = ai_expression->result_kind();
                    ai_output.model_config_id = ai_expression->model_config_id();
                    ai_output.capability = ai_expression->capability();
                    prepared.ai_outputs.emplace_back(std::move(ai_output));
                }
            }
            prepared.output_chunk->owner_info() = input->owner_info();
            return prepared;
        }

        for (size_t index = 0; index < _spec.common_outputs().size(); ++index) {
            ASSIGN_OR_RETURN(ColumnPtr column, driver->common[index]->evaluate(input.get()));
            if (column == nullptr || column->size() != rows) {
                return Status::InternalError("AI project common expression returned an invalid column");
            }
            input->append_column(std::move(column), _spec.common_outputs()[index].slot_id);
            RETURN_IF_ERROR(driver->common[index]->get_udf_error());
        }

        for (size_t index = 0; index < _spec.outputs().size(); ++index) {
            const AIProjectOutputSpec& output = _spec.outputs()[index];
            ExprContext* context = driver->outputs[index];
            if (output.kind == AIProjectOutputKind::PASSTHROUGH) {
                ASSIGN_OR_RETURN(ColumnPtr column, context->evaluate(input.get()));
                if (column == nullptr || column->size() != rows) {
                    return Status::InternalError("AI project output expression returned an invalid column");
                }
                MutableColumnPtr aligned = ColumnHelper::align_return_type(std::move(column), context->root()->type(),
                                                                           rows, output.nullable);
                prepared.output_chunk->append_column(std::move(aligned), output.slot_id);
                RETURN_IF_ERROR(context->get_udf_error());
                continue;
            }

            auto* ai_expression = dynamic_cast<AIFunctionCallExpr*>(context->root());
            if (ai_expression == nullptr) {
                return invalid_projection();
            }
            ASSIGN_OR_RETURN(AIFunctionInputBatch batch,
                             ai_expression->build_input_batch(
                                     context, input.get(),
                                     _spec.model_configs().at(std::string(ai_expression->model_config_id())).model));
            if (batch.rows.size() != rows) {
                return Status::InternalError("AI project expression returned an invalid input batch");
            }
            RETURN_IF_ERROR(context->get_udf_error());

            MutableColumnPtr placeholder = ColumnHelper::create_column(context->root()->type(), true);
            placeholder->append_nulls(rows);
            prepared.output_chunk->append_column(std::move(placeholder), output.slot_id);
            prepared.ai_outputs.emplace_back(AIProjectPreparedOutput{
                    .slot_id = output.slot_id,
                    .input = std::move(batch),
                    .replace_existing = true,
                    .result_type = ai_expression->type(),
                    .result_kind = ai_expression->result_kind(),
                    .model_config_id = std::string(ai_expression->model_config_id()),
                    .capability = ai_expression->capability(),
            });
        }
        prepared.output_chunk->owner_info() = input->owner_info();
        return prepared;
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Failed to evaluate AI project expressions");
    } catch (...) {
        return Status::InternalError("Failed to evaluate AI project expressions");
    }
}

StatusOr<std::shared_ptr<AIProjectDispatcherSubmitter>> AIProjectDispatcherSubmitter::create(
        RuntimeState* state, const AIProjectModelConfigs& configs, AIRuntimeConfig config) {
    if (state == nullptr) {
        return Status::InvalidArgument("AI project dispatcher requires a runtime state");
    }
    RETURN_IF_ERROR(config.validate());
    if (configs.empty()) {
        return Status::InvalidArgument("AI project model configurations are required");
    }

    const QueryExecutionServices* query_services = state->query_execution_services();
    const AIServices* services = query_services == nullptr ? nullptr : query_services->ai;
    if (services == nullptr || services->admission_controller == nullptr || services->http_client == nullptr ||
        services->completion_executor == nullptr || services->clock == nullptr || services->random == nullptr ||
        services->metrics == nullptr) {
        return Status::InvalidArgument("AI project dispatcher runtime services are unavailable");
    }

    auto lifetime = state->query_ctx_lifetime().lock();
    auto query_context = std::dynamic_pointer_cast<QueryContext>(std::move(lifetime));
    if (query_context == nullptr) {
        return Status::InvalidArgument("AI project dispatcher requires a live QueryContext");
    }
    const QueryRuntimeState* query_runtime_state = state->query_runtime_state();
    if (query_runtime_state == nullptr) {
        return Status::InvalidArgument("AI project dispatcher requires query runtime state");
    }

    const FragmentRuntimeState* fragment_runtime_state = state->fragment_runtime_state();
    workgroup::WorkGroupPtr workgroup =
            fragment_runtime_state == nullptr ? workgroup::WorkGroupPtr{} : fragment_runtime_state->workgroup();
    if (workgroup == nullptr) {
        return Status::InvalidArgument("AI project dispatcher requires a WorkGroup");
    }

    ASSIGN_OR_RETURN(std::shared_ptr<AIQueryMemoryAccount> memory_account, AIQueryMemoryAccount::create(*state));

    try {
        std::map<std::string, Route, std::less<>> routes;
        std::unordered_set<int64_t> model_ids;
        for (const auto& [id, model_config] : configs) {
            if (contains_only_ascii_whitespace(id) || contains_control_character(id) ||
                contains_control_character(model_config.model)) {
                return Status::InvalidArgument("AI function model configuration is invalid");
            }
            if (model_config.capability != AICapability::CHAT &&
                model_config.capability != AICapability::TEXT_EMBEDDING) {
                return Status::InvalidArgument("AI function model capability is invalid");
            }
            std::string endpoint_environment;
            std::string key_environment;
            if (model_config.source == TAIModelSource::SYSTEM) {
                if (model_config.model_id != 0 || !model_config.credential_ref.empty()) {
                    return Status::InvalidArgument("AI function model configuration is invalid");
                }
                if (id == kSystemChatConfigId && model_config.capability == AICapability::CHAT) {
                    endpoint_environment = kEndpointEnvironment;
                    key_environment = kApiKeyEnvironment;
                } else if (id == kSystemEmbeddingConfigId && model_config.capability == AICapability::TEXT_EMBEDDING) {
                    endpoint_environment = "AI_FUNCTION_EMBEDDING_ENDPOINT";
                    key_environment = "AI_FUNCTION_EMBEDDING_API_KEY";
                } else {
                    return Status::InvalidArgument("AI function model configuration is invalid");
                }
            } else if (model_config.source == TAIModelSource::AI_MODEL) {
                if (model_config.model_id <= 0 || !model_ids.emplace(model_config.model_id).second ||
                    id == kSystemChatConfigId || id == kSystemEmbeddingConfigId ||
                    contains_only_ascii_whitespace(model_config.model)) {
                    return Status::InvalidArgument("AI function model configuration is invalid");
                }
                const std::string& ref = model_config.credential_ref;
                if (ref.empty() || ref.size() > 64 || !std::all_of(ref.begin(), ref.end(), [](unsigned char c) {
                        return (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
                    })) {
                    return Status::InvalidArgument("AI function model credential reference is invalid");
                }
                endpoint_environment = "AI_FUNCTION_CREDENTIAL_" + ref + "_ENDPOINT";
                key_environment = "AI_FUNCTION_CREDENTIAL_" + ref + "_API_KEY";
            } else {
                return Status::InvalidArgument("AI function model configuration is invalid");
            }
            RETURN_IF_ERROR(validate_ai_https_url(model_config.endpoint));
            const char* allowed_endpoint = std::getenv(endpoint_environment.c_str());
            if (allowed_endpoint == nullptr || model_config.endpoint != allowed_endpoint) {
                return Status::InvalidArgument("AI function model endpoint is unavailable or invalid");
            }
            const char* api_key = std::getenv(key_environment.c_str());
            if (api_key == nullptr || has_invalid_api_key_byte(api_key)) {
                return Status::InvalidArgument("AI function model credential is unavailable or invalid");
            }
            Route route{.endpoint = model_config.endpoint, .api_key = api_key, .capability = model_config.capability};
            auto resolved_endpoint = resolve_http_endpoint(route.endpoint, OutboundHttpAddressPolicy::BLOCK_LINK_LOCAL);
            if (!resolved_endpoint.ok()) {
                return Status::InvalidArgument("AI function model endpoint is unavailable or invalid");
            }
            route.resolved_endpoint =
                    std::make_shared<const ResolvedHttpEndpoint>(std::move(resolved_endpoint).value());
            routes.emplace(id, std::move(route));
        }
        return std::shared_ptr<AIProjectDispatcherSubmitter>(new AIProjectDispatcherSubmitter(
                std::move(routes), AIWorkGroupKey{workgroup->version(), workgroup->id()}, UniqueId{state->query_id()},
                query_context, std::move(memory_account), *services, std::move(config)));
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Failed to allocate AI project dispatcher");
    } catch (...) {
        return Status::InternalError("Failed to create AI project dispatcher");
    }
}

AIProjectDispatcherSubmitter::AIProjectDispatcherSubmitter(std::map<std::string, Route, std::less<>> routes,
                                                           AIWorkGroupKey workgroup_key, UniqueId query_id,
                                                           std::weak_ptr<QueryContext> query_context,
                                                           std::shared_ptr<AIQueryMemoryAccount> memory_account,
                                                           const AIServices& services, AIRuntimeConfig config)
        : _routes(std::move(routes)),
          _workgroup_key(workgroup_key),
          _query_id(query_id),
          _query_context(std::move(query_context)),
          _memory_account(std::move(memory_account)),
          _memory(_memory_account->memory_context()),
          _clock(services.clock),
          _config(std::move(config)),
          _dispatcher(services.admission_controller, services.http_client, &_provider, services.completion_executor,
                      services.clock, services.random, services.metrics,
                      AITaskDispatcherOptions{.max_retries = _config.max_retries,
                                              .max_throttle_retries = _config.max_retries_on_throttle}) {}

StatusOr<std::unique_ptr<AIProjectTaskHandle>> AIProjectDispatcherSubmitter::submit(AIProjectTaskRequest request,
                                                                                    AITaskCallback&& callback) {
    if (!callback) {
        return Status::InvalidArgument("AI project task callback is required");
    }

    AIDispatchRequest dispatch_request;
    SCOPED_CLEANUP({
        clear_function_in_physical_scope(_memory, &callback);
        clear_function_in_physical_scope(_memory, &dispatch_request.lifecycle);
    });
    try {
        const auto route_it = _routes.find(request.model_config_id);
        if (route_it == _routes.end() || route_it->second.capability != request.capability) {
            return Status::InvalidArgument("AI function model configuration is invalid");
        }
        const Route& route = route_it->second;
        auto handle = std::make_unique<DispatcherTaskHandle>();
        const int64_t now_ns = _clock->monotonic_now_ns();
        const int64_t request_deadline_ns = saturating_timeout_deadline(now_ns, _config.request_timeout_ms);

        std::weak_ptr<QueryContext> weak_query = _query_context;
        auto build_dispatch_request = [&] {
            dispatch_request = AIDispatchRequest{
                    .workgroup_key = _workgroup_key,
                    .query_id = _query_id,
                    .task_id = request.task_id,
                    .chat_request =
                            AIChatRequest{
                                    .endpoint = route.endpoint,
                                    .model = request.model,
                                    .api_key = route.api_key,
                                    .prompt = request.prompt,
                                    .options = request.options,
                                    .capability = route.capability,
                            },
                    .request_deadline_ns = request_deadline_ns,
                    // Zero disables an independent connect cap. The HTTP transport
                    // uses only the immutable request timeout, when enabled; live
                    // Query lifecycle is probed separately.
                    .connect_timeout_ms = _config.connect_timeout_ms,
                    .max_response_bytes = static_cast<size_t>(_config.max_response_bytes),
                    .resolved_endpoint = route.resolved_endpoint,
                    .lifecycle =
                            [weak_query] {
                                std::shared_ptr<QueryContext> query = weak_query.lock();
                                if (query == nullptr) {
                                    return AIQueryLifecycleSnapshot{.cancelled = true, .monotonic_deadline_ns = 1};
                                }
                                return AIQueryLifecycleSnapshot{
                                        .cancelled = query->is_cancelled(),
                                        .monotonic_deadline_ns = query->query_runtime_state().query_deadline_ns(),
                                };
                            },
                    .memory = _memory,
            };
        };
        _memory.run_in_physical_scope([](void* opaque) { (*static_cast<decltype(build_dispatch_request)*>(opaque))(); },
                                      &build_dispatch_request);

        auto dispatcher_handle = _dispatcher.submit(std::move(dispatch_request), std::move(callback));
        if (!dispatcher_handle.ok()) {
            return dispatcher_handle.status();
        }
        handle->set_handle(std::move(dispatcher_handle).value());
        return std::unique_ptr<AIProjectTaskHandle>(std::move(handle));
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Failed to submit AI project task");
    } catch (...) {
        return Status::InternalError("Failed to submit AI project task");
    }
}

} // namespace starrocks::pipeline
