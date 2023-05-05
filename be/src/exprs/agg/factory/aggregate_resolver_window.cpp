// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <vector> // for allocator, vector

#include "exprs/agg/factory/aggregate_factory.hpp"  // for AggregateFactory
#include "exprs/agg/factory/aggregate_resolver.hpp" // for AggregateFuncRes...
#include "runtime/primitive_type.h"                 // for TYPE_BIGINT, Pri...
#include "runtime/primitive_type_infra.h"           // for type_dispatch_all

namespace starrocks::vectorized {

struct WindowDispatcher {
    template <PrimitiveType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (pt_is_aggregate<pt> || is_object_type(pt)) {
            resolver->add_aggregate_mapping_notnull<pt, pt>(
                    "first_value", true, AggregateFactory::MakeFirstValueWindowFunction<pt, false>());
            // use first_value_in for first_value with ingnore nulls.
            resolver->add_aggregate_mapping_notnull<pt, pt>("first_value_in", true,
                                                            AggregateFactory::MakeFirstValueWindowFunction<pt, true>());
            resolver->add_aggregate_mapping_notnull<pt, pt>("last_value", true,
                                                            AggregateFactory::MakeLastValueWindowFunction<pt, false>());
            // use last_value_in for last_value with ingnore nulls.
            resolver->add_aggregate_mapping_notnull<pt, pt>("last_value_in", true,
                                                            AggregateFactory::MakeLastValueWindowFunction<pt, true>());
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
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_BIGINT>("dense_rank", true,
                                                            AggregateFactory::MakeDenseRankWindowFunction());
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_BIGINT>("rank", true, AggregateFactory::MakeRankWindowFunction());
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_BIGINT>("row_number", true,
                                                            AggregateFactory::MakeRowNumberWindowFunction());
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_BIGINT>("ntile", true, AggregateFactory::MakeNtileWindowFunction());
}

} // namespace starrocks::vectorized
