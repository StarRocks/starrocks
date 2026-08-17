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

#include "exprs/ai_functions.h"

#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "common/status.h"
#include "common/statusor.h"
#include "gutil/strings/substitute.h"
#include "platform/llm/llm_query_service.h"
#include "platform/llm/model_config.h"
#include "types/json_value.h"

namespace starrocks {

StatusOr<ColumnPtr> AiFunctions::ai_query(FunctionContext* context, const starrocks::Columns& columns) {
    if (columns.size() != 2) {
        return Status::InvalidArgument("Ai_query function only call by ai_query(prompt, config)");
    }

    auto* query_service = LLMQueryService::instance();
    RETURN_IF_ERROR(query_service->init());

    auto num_rows = columns[0]->size();
    auto prompt_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto json_viewer = ColumnViewer<TYPE_JSON>(columns[1]);
    ColumnBuilder<TYPE_VARCHAR> result(num_rows);

    bool config_is_const = columns[1]->is_constant();
    std::unique_ptr<ModelConfig> const_config = nullptr;
    if (context->is_notnull_constant_column(1)) {
        JsonValue* json_value = json_viewer.value(0);
        auto status_or_config = parse_model_config(*json_value);
        if (!status_or_config.ok()) {
            return Status::InvalidArgument(
                    strings::Substitute("Failed to parse constant config: $0", status_or_config.status().to_string()));
        }
        const_config = std::make_unique<ModelConfig>(status_or_config.value());
    }

    std::vector<std::shared_future<StatusOr<std::string>>> futures;
    futures.reserve(num_rows);
    std::vector<bool> is_valid_row(num_rows, false);
    std::vector<int> timeout_ms_row(num_rows, -1);

    for (int row = 0; row < num_rows; ++row) {
        if (prompt_viewer.is_null(row) || json_viewer.is_null(row)) {
            is_valid_row[row] = false;
            continue;
        }

        ModelConfig config;
        if (config_is_const && const_config) {
            config = *const_config;
        } else {
            JsonValue* json_value = json_viewer.value(row);
            auto status_or_config = parse_model_config(*json_value);
            if (!status_or_config.ok()) {
                return Status::InvalidArgument(strings::Substitute("Failed to parse config at row $0: $1", row,
                                                                   status_or_config.status().to_string()));
            }
            config = status_or_config.value();
            timeout_ms_row[row] = config.timeout_ms;
        }
        auto prompt = prompt_viewer.value(row);
        std::string prompt_str = prompt.to_string();

        // Asynchronously sending the large language model request
        futures.push_back(query_service->async_query(prompt_str, config));
        is_valid_row[row] = true;
    }

    size_t future_idx = 0;
    for (int row = 0; row < num_rows; ++row) {
        if (!is_valid_row[row]) {
            result.append_null();
            continue;
        }

        auto& future = futures[future_idx++];

        // Wait with timeout (use config timeout or default 60s)
        std::chrono::milliseconds timeout_duration(60000);
        if (config_is_const && const_config) {
            timeout_duration = std::chrono::milliseconds(const_config->timeout_ms);
        } else {
            timeout_duration = std::chrono::milliseconds(timeout_ms_row[row]);
        }

        auto status = future.wait_for(timeout_duration);
        if (status == std::future_status::timeout) {
            return Status::InternalError(strings::Substitute("LLM query timeout at row $0", row));
        }

        auto llm_result = future.get();
        if (!llm_result.ok()) {
            return Status::InternalError(
                    strings::Substitute("LLM query failed at row $0: $1", row, llm_result.status().to_string()));
        }

        result.append(llm_result.value());
    }

    return result.build(ColumnHelper::is_all_const(columns));
}
} // namespace starrocks

#include "gen_cpp/opcode/AiFunctions.inc"
