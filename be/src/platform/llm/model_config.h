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

#include <string>

#include "common/statusor.h"

namespace starrocks {

class JsonValue;

constexpr double kDefaultTemperature = 0.7;
constexpr int kDefaultMaxTokens = 1024;
constexpr double kDefaultTopP = 1.0;
extern const std::string kDefaultEndpoint;
constexpr int kDefaultTimeout = 60000;

struct ModelConfig {
    std::string endpoint;
    std::string model;
    std::string api_key;
    double temperature{kDefaultTemperature};
    int max_tokens{kDefaultMaxTokens};
    double top_p{kDefaultTopP};
    int timeout_ms{kDefaultTimeout};

    ModelConfig();
};

StatusOr<ModelConfig> parse_model_config(const JsonValue& json);

} // namespace starrocks
