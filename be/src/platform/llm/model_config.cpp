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

#include "platform/llm/model_config.h"

#include <cstdlib>

#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "types/json_value.h"

namespace starrocks {

const std::string kDefaultEndpoint = "https://api.openai.com/v1/chat/completions";

ModelConfig::ModelConfig() : endpoint(kDefaultEndpoint) {}

StatusOr<ModelConfig> parse_model_config(const JsonValue& json) {
    ModelConfig config;

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

} // namespace starrocks
