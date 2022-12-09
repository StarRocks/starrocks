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
#include "runtime/primitive_type.h"                 // for TYPE_BIGINT, Pri...
#include "runtime/primitive_type_infra.h"           // for type_dispatch_all

namespace starrocks::vectorized {

struct WindowDispatcher {
    template <LogicalType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (pt_is_aggregate<pt> || pt_is_string<pt> || is_object_type(pt)) {
            resolver->add_aggregate_mapping_notnull<pt, pt>("first_value", true,
                                                            AggregateFactory::MakeFirstValueWindowFunction<pt>());
            resolver->add_aggregate_mapping_notnull<pt, pt>("last_value", true,
                                                            AggregateFactory::MakeLastValueWindowFunction<pt>());
            resolver->add_aggregate_mapping_notnull<pt, pt>("lead", true,
                                                            AggregateFactory::MakeLeadLagWindowFunction<pt>());
            resolver->add_aggregate_mapping_notnull<pt, pt>("lag", true,
                                                            AggregateFactory::MakeLeadLagWindowFunction<pt>());
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

} // namespace starrocks::vectorized
