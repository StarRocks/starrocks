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

#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "exprs/agg/stream/retract_maxmin.h"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"

namespace starrocks {

struct RetractMinMaxDispatcher {
    template <LogicalType lt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_aggregate<lt>) {
            resolver->add_aggregate_mapping<lt, lt, MinAggregateDataRetractable<lt>>(
                    "retract_min", true, AggregateFactory::MakeRetractMinAggregateFunction<lt>());
            resolver->add_aggregate_mapping<lt, lt, MaxAggregateDataRetractable<lt>>(
                    "retract_max", true, AggregateFactory::MakeRetractMaxAggregateFunction<lt>());
        }
    }
};

void AggregateFuncResolver::register_retract_functions() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, RetractMinMaxDispatcher(), this);
    }
}

} // namespace starrocks
