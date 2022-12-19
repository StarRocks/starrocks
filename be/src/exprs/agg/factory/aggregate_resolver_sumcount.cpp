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
#include "exprs/agg/sum.h"
#include "runtime/primitive_type.h"

namespace starrocks::vectorized {

struct SumDispatcher {
    template <LogicalType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (pt_is_decimal<pt>) {
            resolver->add_decimal_mapping<pt, TYPE_DECIMAL128, true>("decimal_sum");
        } else if constexpr (pt_is_numeric<pt> || pt_is_decimalv2<pt>) {
            using SumState = SumAggregateState<RunTimeCppType<SumResultPT<pt>>>;
            resolver->add_aggregate_mapping<pt, SumResultPT<pt>, SumState>(
                    "sum", true, AggregateFactory::MakeSumAggregateFunction<pt>());
        }
    }
};

VALUE_GUARD(LogicalType, StorageSumPTGuard, pt_is_sum_in_storage, TYPE_BOOLEAN, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT,
            TYPE_BIGINT, TYPE_LARGEINT, TYPE_FLOAT, TYPE_DOUBLE, TYPE_DECIMAL, TYPE_DECIMALV2, TYPE_DECIMAL32,
            TYPE_DECIMAL64, TYPE_DECIMAL128);

struct StorageSumDispatcher {
    template <LogicalType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (pt_is_sum_in_storage<pt>) {
            using SumState = SumAggregateState<RunTimeCppType<pt>>;
            resolver->add_aggregate_mapping<pt, pt, SumState>(
                    "sum", true,
                    std::make_shared<SumAggregateFunction<pt, RunTimeCppType<pt>, pt, RunTimeCppType<pt>>>());
        }
    }
};

struct DistinctDispatcher {
    template <LogicalType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (pt_is_aggregate<pt> || pt_is_string<pt>) {
            using DistinctState = DistinctAggregateState<pt, SumResultPT<pt>>;
            using DistinctState2 = DistinctAggregateStateV2<pt, SumResultPT<pt>>;
            resolver->add_aggregate_mapping<pt, TYPE_BIGINT, DistinctState>(
                    "multi_distinct_count", false, AggregateFactory::MakeCountDistinctAggregateFunction<pt>());
            resolver->add_aggregate_mapping<pt, TYPE_BIGINT, DistinctState2>(
                    "multi_distinct_count2", false, AggregateFactory::MakeCountDistinctAggregateFunctionV2<pt>());

            resolver->add_aggregate_mapping<pt, SumResultPT<pt>, DistinctState>(
                    "multi_distinct_sum", false, AggregateFactory::MakeSumDistinctAggregateFunction<pt>());
            resolver->add_aggregate_mapping<pt, SumResultPT<pt>, DistinctState2>(
                    "multi_distinct_sum2", false, AggregateFactory::MakeSumDistinctAggregateFunctionV2<pt>());
        }
    }
};

void AggregateFuncResolver::register_sumcount() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, SumDispatcher(), this);
    }

    // In storage layer, sum result type is the same as input type.
    // So we need to add these functions here to support it.
    // Some of the following functions will be the same as the above ones.
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, StorageSumDispatcher(), this);
    }

    _infos_mapping.emplace(std::make_tuple("count", TYPE_BIGINT, TYPE_BIGINT, false, false),
                           AggregateFactory::MakeCountAggregateFunction<false>());
    _infos_mapping.emplace(std::make_tuple("count", TYPE_BIGINT, TYPE_BIGINT, true, false),
                           AggregateFactory::MakeCountAggregateFunction<true>());
    _infos_mapping.emplace(std::make_tuple("count", TYPE_BIGINT, TYPE_BIGINT, false, true),
                           AggregateFactory::MakeCountNullableAggregateFunction<false>());
    _infos_mapping.emplace(std::make_tuple("count", TYPE_BIGINT, TYPE_BIGINT, true, true),
                           AggregateFactory::MakeCountNullableAggregateFunction<true>());
}

void AggregateFuncResolver::register_distinct() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, DistinctDispatcher(), this);
    }
}

} // namespace starrocks::vectorized
