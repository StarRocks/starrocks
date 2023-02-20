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

#include <ostream>

namespace starrocks {

enum StorageAggregateType {
    STORAGE_AGGREGATE_NONE = 0,
    STORAGE_AGGREGATE_SUM = 1,
    STORAGE_AGGREGATE_MIN = 2,
    STORAGE_AGGREGATE_MAX = 3,
    STORAGE_AGGREGATE_REPLACE = 4,
    STORAGE_AGGREGATE_HLL_UNION = 5,
    STORAGE_AGGREGATE_UNKNOWN = 6,
    STORAGE_AGGREGATE_BITMAP_UNION = 7,
    // Replace if and only if added value is not null
    STORAGE_AGGREGATE_REPLACE_IF_NOT_NULL = 8,
    STORAGE_AGGREGATE_PERCENTILE_UNION = 9
};

StorageAggregateType get_aggregation_type_by_string(const std::string& str);
std::string get_string_by_aggregation_type(StorageAggregateType type);

} // namespace starrocks

namespace std {
ostream& operator<<(ostream& os, starrocks::StorageAggregateType method);
}