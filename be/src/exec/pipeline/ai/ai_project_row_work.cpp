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

#include "exec/pipeline/ai/ai_project_row_work.h"

#include "exec/pipeline/ai/ai_aggregate_reduce_work.h"

namespace starrocks::pipeline {

AIProjectRowWork::AIProjectRowWork(SingleRequestWork work) : _work(std::move(work)) {}
AIProjectRowWork::AIProjectRowWork(std::unique_ptr<AIAggregateReduceWork> work) : _work(std::move(work)) {}
AIProjectRowWork::~AIProjectRowWork() = default;
AIProjectRowWork::AIProjectRowWork(AIProjectRowWork&&) noexcept = default;
AIProjectRowWork& AIProjectRowWork::operator=(AIProjectRowWork&&) noexcept = default;

StatusOr<AIProjectRowWork> AIProjectRowWork::create(AIProjectInputBatch& batch, size_t row) {
    if (auto* scalar = std::get_if<AIFunctionInputBatch>(&batch)) {
        auto& input = scalar->rows.at(row);
        if (input.action == AIFunctionRowAction::TERMINAL_ROW_FAILURE) {
            return Status::InvalidArgument("AI function row input is invalid");
        }
        return AIProjectRowWork(SingleRequestWork{.input = std::move(input),
                                                  .options = scalar->options,
                                                  .progress = input.action == AIFunctionRowAction::SQL_NULL
                                                                      ? AIWorkProgress::COMPLETE
                                                                      : AIWorkProgress::RUNNABLE});
    }
    ASSIGN_OR_RETURN(auto aggregate, AIAggregateReduceWork::create(std::get<AIAggregateReduceInputBatch>(batch), row));
    return AIProjectRowWork(std::move(aggregate));
}

StatusOr<std::optional<AIWorkRequest>> AIProjectRowWork::next_request(size_t byte_budget) {
    if (auto* single = std::get_if<SingleRequestWork>(&_work)) {
        if (single->progress != AIWorkProgress::RUNNABLE) return std::optional<AIWorkRequest>{};
        single->progress = AIWorkProgress::WAITING;
        return std::optional<AIWorkRequest>(AIWorkRequest{.prompt = std::move(single->input.prompt),
                                                          .media = std::move(single->input.media),
                                                          .options = single->options});
    }
    return std::get<std::unique_ptr<AIAggregateReduceWork>>(_work)->next_request(byte_budget);
}

Status AIProjectRowWork::consume_success(uint64_t ordinal, AITaskSuccess&& result) {
    if (auto* single = std::get_if<SingleRequestWork>(&_work)) {
        single->result.emplace(std::move(result));
        single->progress = AIWorkProgress::COMPLETE;
        return Status::OK();
    }
    return std::get<std::unique_ptr<AIAggregateReduceWork>>(_work)->consume_success(ordinal, std::move(result));
}

void AIProjectRowWork::finish_null() {
    if (auto* single = std::get_if<SingleRequestWork>(&_work)) {
        single->progress = AIWorkProgress::COMPLETE;
        single->result.reset();
    } else {
        std::get<std::unique_ptr<AIAggregateReduceWork>>(_work)->finish_null();
    }
}

AIWorkProgress AIProjectRowWork::progress() const {
    if (const auto* single = std::get_if<SingleRequestWork>(&_work)) return single->progress;
    return std::get<std::unique_ptr<AIAggregateReduceWork>>(_work)->progress();
}

std::optional<AITaskSuccess> AIProjectRowWork::take_result() {
    if (auto* single = std::get_if<SingleRequestWork>(&_work)) return std::move(single->result);
    return std::get<std::unique_ptr<AIAggregateReduceWork>>(_work)->take_result();
}

bool AIProjectRowWork::is_aggregate() const {
    return std::holds_alternative<std::unique_ptr<AIAggregateReduceWork>>(_work);
}

std::string_view AIProjectRowWork::model() const {
    if (const auto* single = std::get_if<SingleRequestWork>(&_work)) return single->input.model;
    return std::get<std::unique_ptr<AIAggregateReduceWork>>(_work)->model();
}

} // namespace starrocks::pipeline
