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

#include "exec/pipeline/ai/ai_project_row_work.h"
#include "exprs/ai/ai_aggregate_state.h"

namespace starrocks::pipeline {

// Internal policy, injectable in deterministic tests. Byte limits apply to the
// complete escaped request, not a token estimate or the unwrapped input alone.
struct AIAggregateReducePolicy {
    size_t max_prompt_bytes = 64 * 1024;
    size_t max_summary_bytes = 4 * 1024;
    size_t max_retained_summary_bytes = kAiAggregateStateMaxBytes;
    size_t max_rounds = 8;
    size_t max_requests = 1024;
};

class AIAggregateReduceWork {
public:
    static StatusOr<std::unique_ptr<AIAggregateReduceWork>> create(const AIAggregateReduceInputBatch& batch, size_t row,
                                                                   AIAggregateReducePolicy policy = {});
    StatusOr<std::optional<AIWorkRequest>> next_request(size_t byte_budget);
    Status consume_success(uint64_t ordinal, AITaskSuccess&& success);
    void finish_null();
    AIWorkProgress progress() const;
    std::optional<AITaskSuccess> take_result();
    std::string_view model() const { return _model; }
    size_t rounds() const { return _round; }
    size_t requests() const { return _requests; }

private:
    AIAggregateReduceWork(const AIAggregateReduceInputBatch& batch, AIAggregateReducePolicy policy);
    Status _advance_level();
    Status _prepare_intermediate_options();
    std::string _prefix() const;
    bool _next_value(AiAggregateStateCursor* raw, size_t* summary, Slice* value) const;

    const AIAggregateReducePolicy _policy;
    ColumnPtr _state_owner;
    std::string _instruction;
    bool _instruction_over_budget = false;
    std::string _model;
    std::shared_ptr<const AIProviderOptions> _final_options;
    std::shared_ptr<const AIProviderOptions> _intermediate_options;
    AiAggregateStateCursor _raw;
    bool _raw_level = true;
    bool _done = false;
    bool _exhausted = false;
    bool _final = false;
    size_t _summary_cursor = 0;
    size_t _round = 1;
    size_t _requests = 0;
    size_t _level_begin = 0;
    size_t _pending = 0;
    size_t _retained_bytes = 0;
    size_t _previous_bytes = 0;
    std::vector<AITaskSuccess> _summaries;
    std::vector<std::optional<AITaskSuccess>> _produced;
    std::optional<AITaskSuccess> _result;
};

} // namespace starrocks::pipeline
