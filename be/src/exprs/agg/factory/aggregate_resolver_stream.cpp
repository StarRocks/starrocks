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
    template <LogicalType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (pt_is_aggregate<pt> || pt_is_string<pt>) {
            resolver->add_aggregate_mapping<pt, pt, MinAggregateDataRetractable<pt>>(
                    "retract_min", true, AggregateFactory::MakeRetractMinAggregateFunction<pt>());
            resolver->add_aggregate_mapping<pt, pt, MaxAggregateDataRetractable<pt>>(
                    "retract_max", true, AggregateFactory::MakeRetractMaxAggregateFunction<pt>());
        }
    }
};

void AggregateFuncResolver::register_retract_functions() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, RetractMinMaxDispatcher(), this);
    }
}

} // namespace starrocks
