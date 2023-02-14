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
#include "types/logical_type.h"

namespace starrocks {

struct SumDispatcher {
    template <LogicalType lt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_decimal<lt>) {
            resolver->add_decimal_mapping<lt, TYPE_DECIMAL128, true>("decimal_sum");
        } else if constexpr (lt_is_numeric<lt> || lt_is_decimalv2<lt>) {
            using SumState = SumAggregateState<RunTimeCppType<SumResultLT<lt>>>;
            resolver->add_aggregate_mapping<lt, SumResultLT<lt>, SumState>(
                    "sum", true, AggregateFactory::MakeSumAggregateFunction<lt>());
        }
    }
};

VALUE_GUARD(LogicalType, StorageSumLTGuard, lt_is_sum_in_storage, TYPE_BOOLEAN, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT,
            TYPE_BIGINT, TYPE_LARGEINT, TYPE_FLOAT, TYPE_DOUBLE, TYPE_DECIMAL, TYPE_DECIMALV2, TYPE_DECIMAL32,
            TYPE_DECIMAL64, TYPE_DECIMAL128);

struct StorageSumDispatcher {
    template <LogicalType lt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_sum_in_storage<lt>) {
            using SumState = SumAggregateState<RunTimeCppType<lt>>;
            resolver->add_aggregate_mapping<lt, lt, SumState>(
                    "sum", true,
                    std::make_shared<SumAggregateFunction<lt, RunTimeCppType<lt>, lt, RunTimeCppType<lt>>>());
        }
    }
};

struct DistinctDispatcher {
    template <LogicalType lt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_aggregate<lt> || lt_is_string<lt>) {
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

} // namespace starrocks
