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
#include "exprs/agg/exchange_perf.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"

namespace starrocks {

struct HistogramDispatcher {
    template <LogicalType lt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_aggregate<lt>) {
            resolver->add_aggregate_mapping_notnull<lt, TYPE_VARCHAR>(
                    "histogram", false, AggregateFactory::MakeHistogramAggregationFunction<lt>());
        }
    }
};

void AggregateFuncResolver::register_utility() {
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_BIGINT>(
            "exchange_bytes", false, AggregateFactory::MakeExchangePerfAggregateFunction<AggExchangePerfType::BYTES>());
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_VARCHAR>(
            "exchange_speed", false, AggregateFactory::MakeExchangePerfAggregateFunction<AggExchangePerfType::SPEED>());

    for (auto type : aggregate_types()) {
        type_dispatch_all(type, HistogramDispatcher(), this);
    }
}

} // namespace starrocks
