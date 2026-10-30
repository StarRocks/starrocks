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

#include "exprs/agg/factory/aggregate_resolver_maxminby.hpp"

namespace starrocks {

void AggregateFuncResolver::register_maxminby1() {
    std::vector<LogicalType> ret_types = aggregate_types();
    ret_types.push_back(TYPE_JSON);
    ret_types.push_back(TYPE_ARRAY);
    ret_types.push_back(TYPE_STRUCT);
    ret_types.push_back(TYPE_MAP);
    register_maxminby_for_outer<TYPE_BOOLEAN>(this, ret_types);
    register_maxminby_for_outer<TYPE_TINYINT>(this, ret_types);
    register_maxminby_for_outer<TYPE_SMALLINT>(this, ret_types);
    register_maxminby_for_outer<TYPE_INT>(this, ret_types);
    register_maxminby_for_outer<TYPE_BIGINT>(this, ret_types);
    register_maxminby_for_outer<TYPE_LARGEINT>(this, ret_types);
    register_maxminby_for_outer<TYPE_FLOAT>(this, ret_types);
    register_maxminby_for_outer<TYPE_DOUBLE>(this, ret_types);
}

} // namespace starrocks
