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
#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "http/http_client.h"
#include "util/json.h"
#include "util/json_converter.h"
#include "util/json_flattener.h"
#include "util/llm_cache_manager.h"
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
    config.api_key = api_key_str.to_string();

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

    return config;
}

Status AiFunctions::init_llm_cache() {
    static std::once_flag init_flag;
    std::call_once(init_flag, []() { LLMCacheManager::instance()->init(config::llm_cache_size); });
    return Status::OK();
}

StatusOr<ColumnPtr> AiFunctions::ai_query(FunctionContext* context, const starrocks::Columns& columns) {
    if (columns.size() != 2) {
        return Status::InvalidArgument("Ai_query function only call by ai_query(propmt, config)");
    }

    init_llm_cache();
    auto* query_service = LLMQueryService::instance();
    query_service->init();

    auto num_rows = columns[0]->size();
    auto prompt_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto json_viewer = ColumnViewer<TYPE_JSON>(columns[1]);
    ColumnBuilder<TYPE_VARCHAR> result(num_rows);

    std::vector<std::shared_future<StatusOr<std::string>>> futures;
    futures.reserve(num_rows);
    std::vector<bool> is_valid_row(num_rows, false);

    for (int row = 0; row < num_rows; ++row) {
        if (prompt_viewer.is_null(row) || json_viewer.is_null(row)) {
            is_valid_row[row] = false;
            continue;
        }

        JsonValue* json_value = json_viewer.value(row);
        auto status_or_config = parse_model_config(*json_value);
        if (!status_or_config.ok()) {
            LOG(WARNING) << "Failed to parse config at row " << row << ": " << status_or_config.status().to_string();
            is_valid_row[row] = false;
            continue;
        }

        ModelConfig config = status_or_config.value();
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

        auto llm_result = futures[future_idx++].get();
        if (llm_result.ok()) {
            result.append(llm_result.value());
        } else {
            LOG(WARNING) << "LLM query failed at row " << row << ": " << llm_result.status().to_string();
            result.append_null();
        }
    }

    return result.build(ColumnHelper::is_all_const(columns));
}
} // namespace starrocks