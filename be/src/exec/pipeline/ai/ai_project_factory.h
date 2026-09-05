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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "base/statusor.h"
#include "common/global_types.h"

namespace starrocks {

class ExprContext;
class RuntimeState;

namespace pipeline {

class AISinkOperatorFactory;
class AISourceOperatorFactory;
class PipelineBuilderContext;

enum class AIProjectOutputKind : uint8_t {
    PASSTHROUGH,
    AI,
};

struct AIProjectOutputSpec {
    SlotId slot_id = 0;
    ExprContext* expr_ctx = nullptr;
    bool nullable = false;
    AIProjectOutputKind kind = AIProjectOutputKind::PASSTHROUGH;
};

struct AIProjectCommonSpec {
    SlotId slot_id = 0;
    ExprContext* expr_ctx = nullptr;
};

// Move-only owner for the fragment-local expression prototypes. Each slot is
// paired with its expression and metadata so construction cannot silently
// misalign parallel vectors. Ownership moves from AIProjectNode to the feature
// factory and finally to AIProjectExpressionProjection.
class AIProjectProjectionSpec {
public:
    AIProjectProjectionSpec() = default;
    AIProjectProjectionSpec(RuntimeState* state, std::vector<AIProjectOutputSpec> outputs,
                            std::vector<AIProjectCommonSpec> common_outputs, std::string default_model);
    ~AIProjectProjectionSpec();

    AIProjectProjectionSpec(const AIProjectProjectionSpec&) = delete;
    AIProjectProjectionSpec& operator=(const AIProjectProjectionSpec&) = delete;
    AIProjectProjectionSpec(AIProjectProjectionSpec&& other) noexcept;
    AIProjectProjectionSpec& operator=(AIProjectProjectionSpec&& other) noexcept;

    AIProjectOutputSpec& add_output(AIProjectOutputSpec output);
    AIProjectCommonSpec& add_common_output(AIProjectCommonSpec output);
    void close(RuntimeState* state = nullptr) noexcept;

    bool empty() const noexcept { return _outputs.empty(); }
    bool valid() const noexcept { return !_closed; }
    const std::vector<AIProjectOutputSpec>& outputs() const noexcept { return _outputs; }
    const std::vector<AIProjectCommonSpec>& common_outputs() const noexcept { return _common_outputs; }
    const std::string& default_model() const noexcept { return _default_model; }
    RuntimeState* runtime_state() const noexcept { return _state; }

private:
    RuntimeState* _state = nullptr;
    std::vector<AIProjectOutputSpec> _outputs;
    std::vector<AIProjectCommonSpec> _common_outputs;
    std::string _default_model;
    bool _closed = false;
};

struct AIProjectOperatorFactories {
    std::shared_ptr<AISinkOperatorFactory> sink;
    std::shared_ptr<AISourceOperatorFactory> source;
};

// Builds one fragment-scoped AI feature graph from one immutable runtime
// snapshot. Callers receive either the complete sink/source pair or an error.
class AIProjectFactory {
public:
    static StatusOr<AIProjectOperatorFactories> create(PipelineBuilderContext* context, int32_t plan_node_id,
                                                       size_t upstream_dop, std::string endpoint,
                                                       AIProjectProjectionSpec projection_spec);
};

} // namespace pipeline
} // namespace starrocks
