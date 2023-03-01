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
#include "exprs/agg/avg.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "types/logical_type.h"

namespace starrocks {

struct AvgDispatcher {
    template <LogicalType lt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_aggregate<lt> && !lt_is_string<lt>) {
            auto func = AggregateFactory::MakeAvgAggregateFunction<lt>();
            using AvgState = AvgAggregateState<RunTimeCppType<ImmediateAvgResultLT<lt>>>;
            resolver->add_aggregate_mapping<lt, AvgResultLT<lt>, AvgState>("avg", true, func);
        }
    }
};

void AggregateFuncResolver::register_avg() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, AvgDispatcher(), this);
    }
    add_decimal_mapping<TYPE_DECIMAL32, TYPE_DECIMAL128, true>("decimal_avg");
    add_decimal_mapping<TYPE_DECIMAL64, TYPE_DECIMAL128, true>("decimal_avg");
    add_decimal_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, true>("decimal_avg");
}

} // namespace starrocks
