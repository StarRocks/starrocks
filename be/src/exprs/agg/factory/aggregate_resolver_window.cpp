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

#include <vector> // for allocator, vector

#include "exprs/agg/factory/aggregate_factory.hpp"  // for AggregateFactory
#include "exprs/agg/factory/aggregate_resolver.hpp" // for AggregateFuncRes...
#include "types/logical_type.h"                     // for TYPE_BIGINT, Pri...
#include "types/logical_type_infra.h"               // for type_dispatch_all

namespace starrocks {

struct WindowDispatcher {
    template <LogicalType lt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_aggregate<lt> || lt_is_string<lt> || is_object_type(lt)) {
            resolver->add_aggregate_mapping_notnull<lt, lt>(
                    "first_value", true, AggregateFactory::MakeFirstValueWindowFunction<lt, false>());
            // use first_value_in for first_value with ingnore nulls.
            resolver->add_aggregate_mapping_notnull<lt, lt>("first_value_in", true,
                                                            AggregateFactory::MakeFirstValueWindowFunction<lt, true>());
            resolver->add_aggregate_mapping_notnull<lt, lt>("last_value", true,
                                                            AggregateFactory::MakeLastValueWindowFunction<lt, false>());
            // use last_value_in for last_value with ingnore nulls.
            resolver->add_aggregate_mapping_notnull<lt, lt>("last_value_in", true,
                                                            AggregateFactory::MakeLastValueWindowFunction<lt, true>());
            resolver->add_aggregate_mapping_notnull<lt, lt>(
                    "lead", true, AggregateFactory::MakeLeadLagWindowFunction<lt, false, false>());
            // use lead_in for lead with ingnore nulls.
            resolver->add_aggregate_mapping_notnull<lt, lt>(
                    "lead_in", true, AggregateFactory::MakeLeadLagWindowFunction<lt, true, false>());
            resolver->add_aggregate_mapping_notnull<lt, lt>(
                    "lag", true, AggregateFactory::MakeLeadLagWindowFunction<lt, false, true>());
            // use lag_in for lag with ingnore nulls.
            resolver->add_aggregate_mapping_notnull<lt, lt>(
                    "lag_in", true, AggregateFactory::MakeLeadLagWindowFunction<lt, true, true>());
        }
    }
};

void AggregateFuncResolver::register_window() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, WindowDispatcher(), this);
    }
    type_dispatch_all(TYPE_JSON, WindowDispatcher(), this);

    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_BIGINT>("dense_rank", true,
                                                            AggregateFactory::MakeDenseRankWindowFunction());
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_BIGINT>("rank", true, AggregateFactory::MakeRankWindowFunction());
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_BIGINT>("row_number", true,
                                                            AggregateFactory::MakeRowNumberWindowFunction());
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_BIGINT>("ntile", true, AggregateFactory::MakeNtileWindowFunction());
}

} // namespace starrocks
