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

#include <map>
#include <optional>
#include <string>

#include "common/config.h"

namespace starrocks {

#define TRACE_BATCH_WRITE LOG_IF(INFO, config::merge_commit_trace_log_enable)

using BatchWriteLoadParams = std::map<std::string, std::string>;

struct BatchWriteId {
    std::string db;
    std::string table;
    BatchWriteLoadParams load_params;
};

// Hash function for BatchWriteId
struct BatchWriteIdHash {
    std::size_t operator()(const BatchWriteId& id) const {
        std::size_t hash = std::hash<std::string>{}(id.db);
        hash ^= std::hash<std::string>{}(id.table) << 1;

        for (const auto& param : id.load_params) {
            hash ^= std::hash<std::string>{}(param.first) << 1;
            hash ^= std::hash<std::string>{}(param.second) << 1;
        }

        return hash;
    }
};

// Equality function for BatchWriteId
struct BatchWriteIdEqual {
    bool operator()(const BatchWriteId& lhs, const BatchWriteId& rhs) const {
        return lhs.db == rhs.db && lhs.table == rhs.table && lhs.load_params == rhs.load_params;
    }
};

std::ostream& operator<<(std::ostream& out, const BatchWriteId& id);

class HttpRequest;

BatchWriteLoadParams get_load_parameters_from_http(HttpRequest* http_req);
BatchWriteLoadParams get_load_parameters_from_brpc(const std::map<std::string, std::string>& input_params);

} // namespace starrocks
