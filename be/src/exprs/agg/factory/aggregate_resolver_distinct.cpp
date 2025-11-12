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
#include "exprs/agg/distinct.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "types/logical_type.h"

namespace starrocks {

struct DistinctDispatcher {
    template <LogicalType lt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_aggregate<lt>) {
            using DistinctState = DistinctAggregateState<lt, SumResultLT<lt>>;
            using DistinctState2 = DistinctAggregateStateV2<lt, SumResultLT<lt>>;
            resolver->add_aggregate_mapping<lt, TYPE_BIGINT, DistinctState>(
                    "multi_distinct_count", false, AggregateFactory::MakeCountDistinctAggregateFunction<lt>());
            resolver->add_aggregate_mapping<lt, TYPE_BIGINT, DistinctState2>(
                    "multi_distinct_count2", false, AggregateFactory::MakeCountDistinctAggregateFunctionV2<lt>());

            resolver->add_aggregate_mapping<lt, SumResultLT<lt>, DistinctState>(
                    "multi_distinct_sum", false, AggregateFactory::MakeSumDistinctAggregateFunction<lt>());
            resolver->add_aggregate_mapping<lt, SumResultLT<lt>, DistinctState2>(
                    "multi_distinct_sum2", false, AggregateFactory::MakeSumDistinctAggregateFunctionV2<lt>());
        }
    }
};

void AggregateFuncResolver::register_distinct() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, DistinctDispatcher(), this);
    }
}

} // namespace starrocks