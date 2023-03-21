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

#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/array_agg.h"
#include "exprs/agg/array_agg_any_type.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "types/logical_type.h"

namespace starrocks {

struct ArrayAggDispatcher {
    template <LogicalType lt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_aggregate<lt> || lt_is_string<lt> || lt_is_json<lt>) {
            auto func = std::make_shared<ArrayAggAggregateFunction<lt>>();
            using AggState = ArrayAggAggregateState<lt>;
            resolver->add_aggregate_mapping<lt, TYPE_ARRAY, AggState, AggregateFunctionPtr, false>("array_agg", false,
                                                                                                   func);
        }
    }
};

void AggregateFuncResolver::register_array_functions() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, ArrayAggDispatcher(), this);
    }
    type_dispatch_all(TYPE_JSON, ArrayAggDispatcher(), this);

    {
        auto func = std::make_shared<ArrayAggAnyTypeAggregateFunction>();
        using AggState = ArrayAggAnyTypeAggregateState;
        add_aggregate_mapping<TYPE_ARRAY, TYPE_ARRAY, AggState, AggregateFunctionPtr, false>("array_agg", false, func);
        add_aggregate_mapping<TYPE_MAP, TYPE_ARRAY, AggState, AggregateFunctionPtr, false>("array_agg", false, func);
        add_aggregate_mapping<TYPE_STRUCT, TYPE_ARRAY, AggState, AggregateFunctionPtr, false>("array_agg", false, func);
    }
}

} // namespace starrocks
