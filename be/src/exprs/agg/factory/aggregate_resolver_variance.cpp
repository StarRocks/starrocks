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
#include "exprs/agg/covariance.h"
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
                    "variance", true, AggregateFactory::MakeVarianceAggregateFunction<lt, false>());
            resolver->add_aggregate_mapping<lt, DevFromAveResultLT<lt>, VarState>(
                    "variance_pop", true, AggregateFactory::MakeVarianceAggregateFunction<lt, false>());
            resolver->add_aggregate_mapping<lt, DevFromAveResultLT<lt>, VarState>(
                    "var_pop", true, AggregateFactory::MakeVarianceAggregateFunction<lt, false>());

            resolver->add_aggregate_mapping<lt, DevFromAveResultLT<lt>, VarState>(
                    "variance_samp", true, AggregateFactory::MakeVarianceAggregateFunction<lt, true>());
            resolver->add_aggregate_mapping<lt, DevFromAveResultLT<lt>, VarState>(
                    "var_samp", true, AggregateFactory::MakeVarianceAggregateFunction<lt, true>());

            resolver->add_aggregate_mapping<lt, DevFromAveResultLT<lt>, VarState>(
                    "stddev", true, AggregateFactory::MakeStddevAggregateFunction<lt, false>());
            resolver->add_aggregate_mapping<lt, DevFromAveResultLT<lt>, VarState>(
                    "std", true, AggregateFactory::MakeStddevAggregateFunction<lt, false>());
            resolver->add_aggregate_mapping<lt, DevFromAveResultLT<lt>, VarState>(
                    "stddev_pop", true, AggregateFactory::MakeStddevAggregateFunction<lt, false>());
            resolver->add_aggregate_mapping<lt, DevFromAveResultLT<lt>, VarState>(
                    "stddev_samp", true, AggregateFactory::MakeStddevAggregateFunction<lt, true>());
        }
    }
};

struct CorVarDispatcher {
    template <LogicalType lt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_numeric<lt>) {
            using VarState = CovarianceCorelationAggregateState<false>;
            resolver->add_aggregate_mapping_variadic<lt, TYPE_DOUBLE, VarState>(
                    "covar_pop", true, AggregateFactory::MakeCovarianceAggregateFunction<lt, false>());

            resolver->add_aggregate_mapping_variadic<lt, TYPE_DOUBLE, VarState>(
                    "covar_samp", true, AggregateFactory::MakeCovarianceAggregateFunction<lt, true>());

            using CorrState = CovarianceCorelationAggregateState<true>;
            resolver->add_aggregate_mapping_variadic<lt, TYPE_DOUBLE, CorrState>(
                    "corr", true, AggregateFactory::MakeCorelationAggregateFunction<lt>());
        }
    }
};

void AggregateFuncResolver::register_variance() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, StdDispatcher(), this);
        type_dispatch_all(type, CorVarDispatcher(), this);
    }
}

} // namespace starrocks
