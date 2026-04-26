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

#include <vector>

#include "common/logging.h"

namespace starrocks {

struct SortDesc {
    int sort_order;
    int null_first;

    SortDesc() = default;
    SortDesc(bool is_asc, bool inull_first) {
        sort_order = is_asc ? 1 : -1;
        null_first = (inull_first ? -1 : 1) * sort_order;
    }
    SortDesc(int order, int null) : sort_order(order), null_first(null) {}

    // Discard sort_order effect on the null_first
    int nan_direction() const { return null_first * sort_order; }
    bool is_null_first() const { return (null_first * sort_order) == -1; }
    bool asc_order() const { return sort_order == 1; }
};

struct SortDescs {
    std::vector<SortDesc> descs;
    mutable bool use_german_string = false;

    SortDescs() = default;
    ~SortDescs() = default;

    SortDescs(const std::vector<bool>& orders, const std::vector<bool>& null_firsts) {
        descs.resize(orders.size());
        for (size_t i = 0; i < orders.size(); ++i) {
            descs[i] = SortDesc(orders.at(i), null_firsts.at(i));
        }
    }

    SortDescs(const std::vector<int>& orders, const std::vector<int>& nulls) {
        DCHECK_EQ(orders.size(), nulls.size());
        descs.reserve(orders.size());
        for (int i = 0; i < orders.size(); i++) {
            descs.emplace_back(orders[i], nulls[i]);
        }
    }

    // Create a default desc with asc order and null_first
    static SortDescs asc_null_first(int columns) {
        SortDescs res;
        for (int i = 0; i < columns; i++) {
            res.descs.emplace_back(1, -1);
        }
        return res;
    }

    size_t num_columns() const { return descs.size(); }

    SortDesc get_column_desc(int col) const { return descs[col]; }
    bool is_use_german_string() const { return use_german_string; }
    void set_use_german_string(bool value) const { use_german_string = value; }
};

} // namespace starrocks
