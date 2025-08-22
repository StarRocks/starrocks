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

#include "exprs/builtin_functions.h"
#include "exprs/function_helper.h"
#include "util/lru_cache.h"

namespace starrocks {

constexpr double kDefaultTemperature = 0.7;
constexpr int kDefaultMaxTokens = 1024;
constexpr double kDefaultTopP = 1.0;
const std::string kDefaultEndpoint = "https://api.openai.com/v1/completions";
constexpr int kDefaultTimeout = 60000;

struct ModelConfig {
    std::string endpoint;
    std::string model;
    std::string api_key;
    double temperature;
    int max_tokens;
    double top_p;
    int timeout_ms;

    ModelConfig()
            : endpoint(kDefaultEndpoint),
              temperature(kDefaultTemperature),
              max_tokens(kDefaultMaxTokens),
              top_p(kDefaultTopP),
              timeout_ms(kDefaultTimeout) {}
};

class AiFunctions {
public:
    DEFINE_VECTORIZED_FN(ai_query);
    static StatusOr<ModelConfig> parse_model_config(const JsonValue& json);
};

} // namespace starrocks
