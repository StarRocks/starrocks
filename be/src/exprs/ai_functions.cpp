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

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "column/column_viewer.h"
#include "column/datum.h"
#include "common/status.h"
#include "common/statusor.h"
#include "http/http_client.h"
#include "util/json.h"
#include "util/json_converter.h"
#include "util/json_flattener.h"
#include "util/llm_cache.h"
#include "util/llm_query_service.h"

namespace starrocks {

StatusOr<ModelConfig> AiFunctions::parse_model_config(const JsonValue& json) {
    ModelConfig config;

    // Required parameters
    ASSIGN_OR_RETURN(auto model_obj, json.get_obj("model"));
    if (model_obj.is_null_or_none()) {
        return Status::InvalidArgument("Missing required field: model");
    }
    ASSIGN_OR_RETURN(auto model_str, model_obj.get_string());
    config.model = model_str.to_string();

    ASSIGN_OR_RETURN(auto api_key_obj, json.get_obj("api_key"));
    if (api_key_obj.is_null_or_none()) {
        return Status::InvalidArgument("Missing required field: api_key");
    }
    ASSIGN_OR_RETURN(auto api_key_str, api_key_obj.get_string());
    std::string api_key = api_key_str.to_string();
    if (api_key.substr(0, 4) == "env.") {
        std::string env_var = api_key.substr(4);
        const char* env_value = getenv(env_var.c_str());
        if (env_value == nullptr) {
            return Status::InvalidArgument(strings::Substitute("Environment variable not found: $0", env_var));
        }
        config.api_key = std::string(env_value);
    } else {
        config.api_key = api_key;
    }

    // Optional parameters
    auto endpoint_result = json.get_obj("endpoint");
    if (endpoint_result.ok() && !endpoint_result->is_null_or_none()) {
        auto endpoint_value = endpoint_result->get_string();
        if (endpoint_value.ok()) {
            config.endpoint = endpoint_value->to_string();
        }
    }

    auto temperature_result = json.get_obj("temperature");
    if (temperature_result.ok() && !temperature_result->is_null_or_none()) {
        auto temp_value = temperature_result->get_double();
        if (temp_value.ok()) {
            config.temperature = temp_value.value();
        }
    }

    auto max_tokens_result = json.get_obj("max_tokens");
    if (max_tokens_result.ok() && !max_tokens_result->is_null_or_none()) {
        auto tokens_value = max_tokens_result->get_int();
        if (tokens_value.ok()) {
            config.max_tokens = tokens_value.value();
        }
    }

    auto top_p_result = json.get_obj("top_p");
    if (top_p_result.ok() && !top_p_result->is_null_or_none()) {
        auto top_p_value = top_p_result->get_double();
        if (top_p_value.ok()) {
            config.top_p = top_p_value.value();
        }
    }

    auto timeout_result = json.get_obj("timeout_ms");
    if (timeout_result.ok() && !timeout_result->is_null_or_none()) {
        auto timeout_value = timeout_result->get_int();
        if (timeout_value.ok()) {
            config.timeout_ms = timeout_value.value();
        }
    }

    // Validate parameter value range
    if (config.temperature < 0 || config.temperature > 2) {
        return Status::InvalidArgument("temperature must be between 0 and 2");
    }
    if (config.max_tokens <= 0) {
        return Status::InvalidArgument("max_tokens must be positive");
    }
    if (config.top_p < 0 || config.top_p > 1) {
        return Status::InvalidArgument("top_p must be between 0 and 1");
    }
    if (config.timeout_ms <= 0) {
        return Status::InvalidArgument("timeout_ms must be positive");
    }

    return config;
}

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
