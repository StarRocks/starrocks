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
#include <vector>

#include "column/datum.h"
#include "gen_cpp/InternalService_types.h"
#include "types/logical_type.h"

namespace starrocks {

class GeneralCondition {
public:
    std::string column_name;
    std::string condition_op;
    std::vector<Datum> condition_values;
    bool is_index_filter_only;
    // When `condition_op` is `IS`, _is_null indicates whether the column is `NULL` or `NOT NULL`.
    bool _is_null = false;

    void set_column_name(const std::string& column_name) { this->column_name = column_name; }
    void set_condition_op(const std::string& condition_op) { this->condition_op = condition_op; }
    void set_is_index_filter_only(bool is_index_filter_only) { this->is_index_filter_only = is_index_filter_only; }
    void add_condition_value(Datum value, [[maybe_unused]] LogicalType lt, [[maybe_unused]] int precision,
                             [[maybe_unused]] int scale) {
        condition_values.emplace_back(std::move(value));
    }
    void set_is_null(bool is_null) { _is_null = is_null; }

    bool is_null() const { return _is_null; }
};

class OlapCondition final : public TCondition {
public:
    void set_column_name(const std::string& column_name) { __set_column_name(column_name); }
    void set_condition_op(const std::string& condition_op) { __set_condition_op(condition_op); }
    void set_is_index_filter_only(bool is_index_filter_only) { __set_is_index_filter_only(is_index_filter_only); }
    void add_condition_value(const auto& value, [[maybe_unused]] LogicalType lt, [[maybe_unused]] int precision,
                             [[maybe_unused]] int scale) {
        condition_values.push_back(cast_to_string(value, lt, precision, scale));
    }
    void set_is_null(bool is_null) {
        if (is_null) {
            condition_values.emplace_back("NULL");
        } else {
            condition_values.emplace_back("NOT NULL");
        }
    }

    bool is_null() const { return strcasecmp(condition_values[0].c_str(), "null") == 0; }
};

} // namespace starrocks
