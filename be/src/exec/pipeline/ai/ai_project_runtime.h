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

#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "exec/pipeline/ai/ai_project_factory.h"
#include "exec/pipeline/ai/ai_project_processor.h"
#include "platform/llm/openai_compatible_provider.h"

namespace starrocks {

class ExprContext;
struct AIServices;

namespace pipeline {

class AIQueryMemoryAccount;
class QueryContext;

// Production expression adapter for AIProject. The original ExprContexts are
// fragment-local prototypes; prepare() clones every context for every source
// driver so expression state is never shared across concurrent drivers.
class AIProjectExpressionProjection final : public AIProjectProjection {
public:
    static StatusOr<std::shared_ptr<AIProjectExpressionProjection>> create(AIProjectProjectionSpec spec);

    ~AIProjectExpressionProjection() override;

    AIProjectExpressionProjection(const AIProjectExpressionProjection&) = delete;
    AIProjectExpressionProjection& operator=(const AIProjectExpressionProjection&) = delete;

    Status prepare(RuntimeState* state, int32_t dop) override;
    void close(RuntimeState* state) override;
    StatusOr<AIProjectPreparedSubchunk> prepare_subchunk(RuntimeState* state, int32_t driver_sequence,
                                                         const ChunkPtr& input) override;

private:
    struct DriverExpressionContexts {
        std::vector<ExprContext*> outputs;
        std::vector<ExprContext*> common;
    };

    explicit AIProjectExpressionProjection(AIProjectProjectionSpec spec);

    void _close_contexts(RuntimeState* state, std::vector<DriverExpressionContexts>* drivers) noexcept;

    AIProjectProjectionSpec _spec;

    mutable std::mutex _lifecycle_mutex;
    Status _lifecycle_status;
    RuntimeState* _prepared_state = nullptr;
    int32_t _dop = 0;
    bool _prepare_attempted = false;
    bool _closed = false;
    std::vector<DriverExpressionContexts> _drivers;
};

// Production dispatcher adapter. Endpoint and credential are copied once at
// fragment construction. The credential is read only from the fixed BE-local
// environment variable and never enters a plan, profile, log, or Status.
class AIProjectDispatcherSubmitter final : public AIProjectTaskSubmitter {
public:
    static StatusOr<std::shared_ptr<AIProjectDispatcherSubmitter>> create(RuntimeState* state, std::string endpoint,
                                                                          AIRuntimeConfig config);

    AIProjectDispatcherSubmitter(const AIProjectDispatcherSubmitter&) = delete;
    AIProjectDispatcherSubmitter& operator=(const AIProjectDispatcherSubmitter&) = delete;

    AIMemoryContext memory_context() const noexcept override { return _memory; }
    StatusOr<std::unique_ptr<AIProjectTaskHandle>> submit(AIProjectTaskRequest request,
                                                          AITaskCallback&& callback) override;

private:
    AIProjectDispatcherSubmitter(std::string endpoint, std::string api_key,
                                 std::shared_ptr<const ResolvedHttpEndpoint> resolved_endpoint,
                                 AIWorkGroupKey workgroup_key, UniqueId query_id,
                                 std::weak_ptr<QueryContext> query_context,
                                 std::shared_ptr<AIQueryMemoryAccount> memory_account, const AIServices& services,
                                 AIRuntimeConfig config);

    const std::string _endpoint;
    const std::string _api_key;
    const std::shared_ptr<const ResolvedHttpEndpoint> _resolved_endpoint;
    const AIWorkGroupKey _workgroup_key;
    const UniqueId _query_id;
    const std::weak_ptr<QueryContext> _query_context;
    const std::shared_ptr<AIQueryMemoryAccount> _memory_account;
    const AIMemoryContext _memory;
    const AIClock* const _clock;
    const AIRuntimeConfig _config;

    // Declaration order is intentional: the dispatcher and all of its core
    // state are destroyed before the provider whose address they retain.
    OpenAICompatibleProvider _provider;
    AITaskDispatcher _dispatcher;
};

} // namespace pipeline
} // namespace starrocks
