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

#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "exprs/agg/variance.h"
#include "types/logical_type.h"

namespace starrocks {

struct StdDispatcher {
    template <LogicalType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (pt_is_numeric<pt>) {
            using VarState = DevFromAveAggregateState<RunTimeCppType<DevFromAveResultPT<pt>>>;
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "variance", false, AggregateFactory::MakeVarianceAggregateFunction<pt, false>());
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "variance_pop", false, AggregateFactory::MakeVarianceAggregateFunction<pt, false>());
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "var_pop", false, AggregateFactory::MakeVarianceAggregateFunction<pt, false>());

            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "variance_samp", false, AggregateFactory::MakeVarianceAggregateFunction<pt, true>());
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "var_samp", false, AggregateFactory::MakeVarianceAggregateFunction<pt, true>());

            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "stddev", false, AggregateFactory::MakeStddevAggregateFunction<pt, false>());
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "std", false, AggregateFactory::MakeStddevAggregateFunction<pt, false>());
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "stddev_pop", false, AggregateFactory::MakeStddevAggregateFunction<pt, false>());
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "stddev_samp", false, AggregateFactory::MakeStddevAggregateFunction<pt, true>());
        }
    }
};

void AggregateFuncResolver::register_variance() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, StdDispatcher(), this);
    }
}

} // namespace starrocks
