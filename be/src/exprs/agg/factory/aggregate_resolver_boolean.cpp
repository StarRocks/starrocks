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

#include "column/type_traits.h"
#include "exprs/agg/boolor.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "types/logical_type.h"

namespace starrocks {

void AggregateFuncResolver::register_boolean() {
    auto bool_or_func = std::make_shared<BoolOrAggregateFunction>();

    _infos_mapping.emplace(std::make_tuple("bool_or", TYPE_BOOLEAN, TYPE_BOOLEAN, false, false), bool_or_func);

    _infos_mapping.emplace(std::make_tuple("bool_or", TYPE_BOOLEAN, TYPE_BOOLEAN, false, true), bool_or_func);

    _infos_mapping.emplace(std::make_tuple("bool_or", TYPE_BOOLEAN, TYPE_BOOLEAN, true, false), bool_or_func);
    _infos_mapping.emplace(std::make_tuple("bool_or", TYPE_BOOLEAN, TYPE_BOOLEAN, true, true), bool_or_func);

    _infos_mapping.emplace(std::make_tuple("boolor_agg", TYPE_BOOLEAN, TYPE_BOOLEAN, false, false), bool_or_func);
    _infos_mapping.emplace(std::make_tuple("boolor_agg", TYPE_BOOLEAN, TYPE_BOOLEAN, false, true), bool_or_func);
    _infos_mapping.emplace(std::make_tuple("boolor_agg", TYPE_BOOLEAN, TYPE_BOOLEAN, true, false), bool_or_func);
    _infos_mapping.emplace(std::make_tuple("boolor_agg", TYPE_BOOLEAN, TYPE_BOOLEAN, true, true), bool_or_func);
}

} // namespace starrocks