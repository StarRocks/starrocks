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

#include <memory>
#include <optional>
#include <variant>

#include "exprs/ai/ai_function_call_expr.h"
#include "platform/llm/ai_task_dispatcher.h"

namespace starrocks::pipeline {

struct AIWorkRequest {
    uint64_t ordinal = 0;
    bool aggregate_leaf = false;
    bool first_in_round = false;
    std::string prompt;
    std::optional<AIMediaInput> media;
    std::shared_ptr<const AIProviderOptions> options;
};
enum class AIWorkProgress : uint8_t { RUNNABLE, WAITING, COMPLETE };
class AIAggregateReduceWork;

// Driver-owned SQL cell. Callback publication belongs to RequestSlot instead.
class AIProjectRowWork {
public:
    static StatusOr<AIProjectRowWork> create(AIProjectInputBatch& batch, size_t row);
    ~AIProjectRowWork();
    AIProjectRowWork(AIProjectRowWork&&) noexcept;
    AIProjectRowWork& operator=(AIProjectRowWork&&) noexcept;
    StatusOr<std::optional<AIWorkRequest>> next_request(size_t byte_budget);
    Status consume_success(uint64_t ordinal, AITaskSuccess&& result);
    void finish_null();
    AIWorkProgress progress() const;
    std::optional<AITaskSuccess> take_result();
    bool is_aggregate() const;
    std::string_view model() const;

private:
    struct SingleRequestWork {
        AIFunctionRowInput input;
        std::shared_ptr<const AIProviderOptions> options;
        AIWorkProgress progress = AIWorkProgress::RUNNABLE;
        std::optional<AITaskSuccess> result;
    };
    explicit AIProjectRowWork(SingleRequestWork work);
    explicit AIProjectRowWork(std::unique_ptr<AIAggregateReduceWork> work);
    std::variant<SingleRequestWork, std::unique_ptr<AIAggregateReduceWork>> _work;
};

} // namespace starrocks::pipeline
