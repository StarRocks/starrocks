// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "types/logical_type.h"
#include "velocypack/vpack.h"

namespace starrocks {
namespace vpack = arangodb::velocypack;

class JsonFlattener {
public:
    static const uint8_t JSON_NULL_TYPE_BITS = 0xFC | 1; // must be the NULL type

public:
    JsonFlattener() = default;

    ~JsonFlattener() = default;

    JsonFlattener(std::vector<std::string>& paths);

    JsonFlattener(std::vector<std::string>& paths, const std::vector<LogicalType>& types);

    void flatten(const Column* json_column, std::vector<ColumnPtr>* result);

    void derived_paths(std::vector<ColumnPtr>& json_datas);

    std::vector<std::string>& get_flat_paths() { return _flat_paths; }

    std::vector<LogicalType> get_flat_types();

    static uint8_t get_compatibility_type(vpack::ValueType type1, uint8_t type2);

private:
    std::vector<std::string> _flat_paths;
    std::vector<uint8_t> _flat_types;
    std::unordered_map<std::string, int> _flat_index;
};
} // namespace starrocks