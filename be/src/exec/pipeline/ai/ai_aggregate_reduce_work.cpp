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

#include "exec/pipeline/ai/ai_aggregate_reduce_work.h"

#include <algorithm>

#include "types/datum.h"
#include "types/json_value.h"

namespace starrocks::pipeline {
namespace {
constexpr std::string_view kFinal = "\nProduce the final result for the instruction.\n";
constexpr std::string_view kIntermediate =
        "\nSummarize these records concisely for a later reduction. Preserve facts needed by the instruction.\n";
Status budget_failure() {
    return Status::InvalidArgument("AI aggregate reduction budget exceeded");
}
} // namespace

AIAggregateReduceWork::AIAggregateReduceWork(const AIAggregateReduceInputBatch& batch, AIAggregateReducePolicy policy)
        : _policy(policy),
          _state_owner(batch.states),
          // Retain no oversized per-group copy. The driver still validates
          // state first, then applies on_error to this local budget failure.
          _instruction(batch.instruction.size() <= policy.max_prompt_bytes ? batch.instruction : std::string()),
          _instruction_over_budget(batch.instruction.size() > policy.max_prompt_bytes),
          _model(batch.model),
          _final_options(batch.options) {}

StatusOr<std::unique_ptr<AIAggregateReduceWork>> AIAggregateReduceWork::create(const AIAggregateReduceInputBatch& batch,
                                                                               size_t row,
                                                                               AIAggregateReducePolicy policy) {
    auto work = std::unique_ptr<AIAggregateReduceWork>(new AIAggregateReduceWork(batch, policy));
    if (batch.states == nullptr || batch.states->is_null(row)) {
        work->finish_null();
        return work;
    }
    ASSIGN_OR_RETURN(auto state, parse_ai_aggregate_state(batch.states->get(row).get_slice()));
    if (state.count() == 0) {
        work->finish_null();
        return work;
    }
    work->_raw = state.cursor();
    work->_previous_bytes = state.payload().size;
    return work;
}

Status AIAggregateReduceWork::_prepare_intermediate_options() {
    if (_intermediate_options != nullptr || _final_options == nullptr) return Status::OK();
    AIProviderOptions::Members intermediate;
    size_t copied_option_bytes = 0;
    for (const auto& member : _final_options->members()) {
        if (member.key == "response_format") continue;
        // Bound the intermediate copy before allocating or reparsing it. Direct
        // final requests retain the original shared options without this copy.
        if (member.key.size() > _policy.max_prompt_bytes - copied_option_bytes ||
            member.serialized_json.size() > _policy.max_prompt_bytes - copied_option_bytes - member.key.size()) {
            return budget_failure();
        }
        copied_option_bytes += member.key.size() + member.serialized_json.size();
        intermediate.push_back(member);
    }
    ASSIGN_OR_RETURN(auto options, AIProviderOptions::create(std::move(intermediate)));
    _intermediate_options = std::make_shared<const AIProviderOptions>(std::move(options));
    return Status::OK();
}

std::string AIAggregateReduceWork::_prefix() const {
    return "Instruction:\n" + _instruction + (_raw_level ? "\n\nInput records:\n[" : "\n\nIntermediate summaries:\n[");
}

bool AIAggregateReduceWork::_next_value(AiAggregateStateCursor* raw, size_t* summary, Slice* value) const {
    if (_raw_level) return raw->next(value);
    if (*summary == _summaries.size()) return false;
    const auto content = _summaries[(*summary)++].content();
    *value = Slice(content.data(), content.size());
    return true;
}

StatusOr<std::optional<AIWorkRequest>> AIAggregateReduceWork::next_request(size_t byte_budget) {
    if (_done || (_exhausted && _pending != 0)) return std::optional<AIWorkRequest>{};
    if (_instruction_over_budget) return budget_failure();
    if (_exhausted) RETURN_IF_ERROR(_advance_level());
    if (_requests >= _policy.max_requests || _round > _policy.max_rounds) return budget_failure();
    // A single call constructs at most one bounded prompt. Returning no request
    // with RUNNABLE tells the processor to yield and resume with a fresh budget.
    if (byte_budget < _policy.max_prompt_bytes) return std::optional<AIWorkRequest>{};
    const auto initial_raw = _raw;
    const auto initial_summary = _summary_cursor;
    bool final_candidate = _produced.empty();
    std::string prompt;
    for (;;) {
        auto raw = initial_raw;
        size_t summary = initial_summary;
        prompt = _prefix();
        const auto suffix = final_candidate ? kFinal : kIntermediate;
        if (prompt.size() + 1 + suffix.size() > _policy.max_prompt_bytes) return budget_failure();
        size_t records = 0;
        bool exhausted = false;
        while (true) {
            auto next_raw = raw;
            size_t next_summary = summary;
            Slice value;
            if (!_next_value(&next_raw, &next_summary, &value)) {
                exhausted = true;
                break;
            }
            if (value.size > _policy.max_prompt_bytes) return budget_failure();
            ASSIGN_OR_RETURN(auto quoted, JsonValue::from_string(value).to_string());
            if (quoted.size() + (records == 0 ? 0 : 1) > _policy.max_prompt_bytes - prompt.size() - 1 - suffix.size())
                break;
            if (records != 0) prompt += ',';
            prompt += quoted;
            ++records;
            raw = next_raw;
            summary = next_summary;
        }
        if (final_candidate && !exhausted) {
            final_candidate = false;
            continue;
        }
        if (records == 0) return budget_failure();
        prompt += ']';
        prompt += suffix;
        _raw = raw;
        _summary_cursor = summary;
        _exhausted = exhausted;
        _final = final_candidate;
        break;
    }
    if (!_final) RETURN_IF_ERROR(_prepare_intermediate_options());
    const size_t ordinal = _requests++;
    _produced.emplace_back();
    ++_pending;
    return std::optional<AIWorkRequest>(AIWorkRequest{.ordinal = ordinal,
                                                      .aggregate_leaf = _raw_level && !_final,
                                                      .first_in_round = ordinal == _level_begin,
                                                      .prompt = std::move(prompt),
                                                      .options = _final ? _final_options : _intermediate_options});
}

Status AIAggregateReduceWork::consume_success(uint64_t ordinal, AITaskSuccess&& success) {
    if (_done || ordinal < _level_begin || ordinal - _level_begin >= _produced.size() ||
        _produced[ordinal - _level_begin].has_value() || _pending == 0) {
        return Status::InternalError("Invalid AI aggregate request completion");
    }
    if (!std::holds_alternative<std::string>(success.result())) {
        return Status::InternalError("AI aggregate chat request returned a non-string result");
    }
    if (_final) {
        _result.emplace(std::move(success));
        _done = true;
        _state_owner.reset();
        _summaries.clear();
        _produced.clear();
    } else {
        const size_t bytes = success.content().size();
        const size_t source_bytes = _raw_level ? 0 : _previous_bytes;
        if (source_bytes > _policy.max_retained_summary_bytes ||
            _retained_bytes > _policy.max_retained_summary_bytes - source_bytes || bytes > _policy.max_summary_bytes ||
            bytes > _policy.max_retained_summary_bytes - source_bytes - _retained_bytes) {
            return budget_failure();
        }
        _retained_bytes += bytes;
        _produced[ordinal - _level_begin].emplace(std::move(success));
    }
    --_pending;
    return Status::OK();
}

Status AIAggregateReduceWork::_advance_level() {
    if (_produced.size() > 1 && _retained_bytes >= _previous_bytes) return budget_failure();
    _summaries.clear();
    _summaries.reserve(_produced.size());
    for (auto& result : _produced) {
        if (!result) return Status::InternalError("Missing AI aggregate intermediate result");
        _summaries.push_back(std::move(*result));
    }
    _produced.clear();
    _state_owner.reset();
    _raw_level = false;
    _summary_cursor = 0;
    _level_begin = _requests;
    _previous_bytes = _retained_bytes;
    _retained_bytes = 0;
    _exhausted = false;
    ++_round;
    return Status::OK();
}

void AIAggregateReduceWork::finish_null() {
    _done = true;
    _state_owner.reset();
    _summaries.clear();
    _produced.clear();
    _result.reset();
}

AIWorkProgress AIAggregateReduceWork::progress() const {
    if (_done) return AIWorkProgress::COMPLETE;
    return _exhausted && _pending != 0 ? AIWorkProgress::WAITING : AIWorkProgress::RUNNABLE;
}

std::optional<AITaskSuccess> AIAggregateReduceWork::take_result() {
    return std::move(_result);
}

} // namespace starrocks::pipeline
