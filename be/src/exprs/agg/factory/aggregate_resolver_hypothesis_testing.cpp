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
#include "exprs/agg/ttest_1samp.h"
#include "exprs/agg/ttest_2samp.h"
#include "types/logical_type.h"

namespace starrocks {

void AggregateFuncResolver::register_hypothesis_testing() {
    add_aggregate_mapping_variadic<TYPE_DOUBLE, TYPE_JSON, MannWhitneyAggregateState>(
            "mann_whitney_u_test", false, AggregateFactory::MakeMannWhitneyUTestAggregateFunction());

    // register for ttest 1 samp
    add_aggregate_mapping<TYPE_VARCHAR, Ttest1SampAggregateState>(
            std::string("ttest_1samp"), std::vector{TYPE_VARCHAR, TYPE_VARCHAR, TYPE_DOUBLE, TYPE_ARRAY}, false,
            std::make_shared<Ttest1SampAggregateFunction>());

    // register for ttest 1 samp
    add_aggregate_mapping<TYPE_VARCHAR, Ttest1SampAggregateState>(
            std::string("ttest_1samp"), std::vector{TYPE_VARCHAR, TYPE_VARCHAR, TYPE_DOUBLE, TYPE_ARRAY, TYPE_VARCHAR},
            false, std::make_shared<Ttest1SampAggregateFunction>());

    // register for ttest 1 samp
    add_aggregate_mapping<TYPE_VARCHAR, Ttest1SampAggregateState>(
            std::string("ttest_1samp"),
            std::vector{TYPE_VARCHAR, TYPE_VARCHAR, TYPE_DOUBLE, TYPE_ARRAY, TYPE_VARCHAR, TYPE_DOUBLE}, false,
            std::make_shared<Ttest1SampAggregateFunction>());

    // expression, side, treatment, data, [cuped, alpha]
    // register for ttest 2 samp
    add_aggregate_mapping<TYPE_VARCHAR, Ttest2SampAggregateState>(
            std::string("ttest_2samp"), std::vector{TYPE_VARCHAR, TYPE_VARCHAR, TYPE_BOOLEAN, TYPE_ARRAY}, false,
            std::make_shared<Ttest2SampAggregateFunction>());

    // register for ttest 2 samp
    add_aggregate_mapping<TYPE_VARCHAR, Ttest2SampAggregateState>(
            std::string("ttest_2samp"), std::vector{TYPE_VARCHAR, TYPE_VARCHAR, TYPE_BOOLEAN, TYPE_ARRAY, TYPE_VARCHAR},
            false, std::make_shared<Ttest2SampAggregateFunction>());

    // register for ttest 2 samp
    add_aggregate_mapping<TYPE_VARCHAR, Ttest2SampAggregateState>(
            std::string("ttest_2samp"),
            std::vector{TYPE_VARCHAR, TYPE_VARCHAR, TYPE_BOOLEAN, TYPE_ARRAY, TYPE_VARCHAR, TYPE_DOUBLE}, false,
            std::make_shared<Ttest2SampAggregateFunction>());
}

} // namespace starrocks
