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
    template <LogicalType lt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_numeric<lt>) {
            using VarState = DevFromAveAggregateState<RunTimeCppType<DevFromAveResultLT<lt>>>;
            resolver->add_aggregate_mapping<lt, DevFromAveResultLT<lt>, VarState>(
                    "variance", false, AggregateFactory::MakeVarianceAggregateFunction<lt, false>());
            resolver->add_aggregate_mapping<lt, DevFromAveResultLT<lt>, VarState>(
                    "variance_pop", false, AggregateFactory::MakeVarianceAggregateFunction<lt, false>());
            resolver->add_aggregate_mapping<lt, DevFromAveResultLT<lt>, VarState>(
                    "var_pop", false, AggregateFactory::MakeVarianceAggregateFunction<lt, false>());

            resolver->add_aggregate_mapping<lt, DevFromAveResultLT<lt>, VarState>(
                    "variance_samp", false, AggregateFactory::MakeVarianceAggregateFunction<lt, true>());
            resolver->add_aggregate_mapping<lt, DevFromAveResultLT<lt>, VarState>(
                    "var_samp", false, AggregateFactory::MakeVarianceAggregateFunction<lt, true>());

            resolver->add_aggregate_mapping<lt, DevFromAveResultLT<lt>, VarState>(
                    "stddev", false, AggregateFactory::MakeStddevAggregateFunction<lt, false>());
            resolver->add_aggregate_mapping<lt, DevFromAveResultLT<lt>, VarState>(
                    "std", false, AggregateFactory::MakeStddevAggregateFunction<lt, false>());
            resolver->add_aggregate_mapping<lt, DevFromAveResultLT<lt>, VarState>(
                    "stddev_pop", false, AggregateFactory::MakeStddevAggregateFunction<lt, false>());
            resolver->add_aggregate_mapping<lt, DevFromAveResultLT<lt>, VarState>(
                    "stddev_samp", false, AggregateFactory::MakeStddevAggregateFunction<lt, true>());
        }
    }
};

void AggregateFuncResolver::register_variance() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, StdDispatcher(), this);
    }
}

} // namespace starrocks
