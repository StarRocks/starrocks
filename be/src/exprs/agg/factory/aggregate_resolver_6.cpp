// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/bitmap_intersect.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "runtime/primitive_type.h"
#include "types/bitmap_value.h"
#include "types/hll.h"

namespace starrocks::vectorized {

template <PrimitiveType pt>
struct NDVBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeHllNdvAggregateFunction<pt>(); }
};

template <PrimitiveType pt>
using NDVStateTrait = HyperLogLog;

template <PrimitiveType pt>
using IntersectCountStateTrait = BitmapIntersectAggregateState<BitmapRuntimeCppType<pt>>;

template <PrimitiveType pt>
struct IntesectCountBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeIntersectCountAggregateFunction<pt>(); }
};

void AggregateFuncResolver::register_6() {
    add_object_mapping<TYPE_HLL, TYPE_HLL, false, HyperLogLog>("hll_union",
                                                               AggregateFactory::MakeHllUnionAggregateFunction());
    add_object_mapping<TYPE_HLL, TYPE_HLL, false, HyperLogLog>("hll_raw_agg",
                                                               AggregateFactory::MakeHllUnionAggregateFunction());
    add_object_mapping<TYPE_HLL, TYPE_BIGINT, false, HyperLogLog>(
            "hll_union_agg", AggregateFactory::MakeHllUnionCountAggregateFunction());

    add_object_mapping<TYPE_OBJECT, TYPE_OBJECT, false, BitmapValue>(
            "bitmap_union", AggregateFactory::MakeBitmapUnionAggregateFunction());
    add_object_mapping<TYPE_OBJECT, TYPE_OBJECT, false, BitmapValuePacked>(
            "bitmap_intersect", AggregateFactory::MakeBitmapIntersectAggregateFunction());
    add_object_mapping<TYPE_OBJECT, TYPE_BIGINT, true, BitmapValue>(
            "bitmap_union_count", AggregateFactory::MakeBitmapUnionCountAggregateFunction());

    // This first type is the second type input of intersect_count.
    // And the first type is Bitmap.
    add_object_mapping<TYPE_TINYINT, TYPE_BIGINT, false, IntesectCountBuilder, IntersectCountStateTrait>(
            "intersect_count");
    add_object_mapping<TYPE_SMALLINT, TYPE_BIGINT, false, IntesectCountBuilder, IntersectCountStateTrait>(
            "intersect_count");
    add_object_mapping<TYPE_INT, TYPE_BIGINT, false, IntesectCountBuilder, IntersectCountStateTrait>("intersect_count");
    add_object_mapping<TYPE_BIGINT, TYPE_BIGINT, false, IntesectCountBuilder, IntersectCountStateTrait>(
            "intersect_count");
    add_object_mapping<TYPE_LARGEINT, TYPE_BIGINT, false, IntesectCountBuilder, IntersectCountStateTrait>(
            "intersect_count");
    add_object_mapping<TYPE_FLOAT, TYPE_BIGINT, false, IntesectCountBuilder, IntersectCountStateTrait>(
            "intersect_count");
    add_object_mapping<TYPE_DOUBLE, TYPE_BIGINT, false, IntesectCountBuilder, IntersectCountStateTrait>(
            "intersect_count");
    add_object_mapping<TYPE_DATE, TYPE_BIGINT, false, IntesectCountBuilder, IntersectCountStateTrait>(
            "intersect_count");
    add_object_mapping<TYPE_DATETIME, TYPE_BIGINT, false, IntesectCountBuilder, IntersectCountStateTrait>(
            "intersect_count");
    add_object_mapping<TYPE_DECIMALV2, TYPE_BIGINT, false, IntesectCountBuilder, IntersectCountStateTrait>(
            "intersect_count");
    add_object_mapping<TYPE_CHAR, TYPE_BIGINT, false, IntesectCountBuilder, IntersectCountStateTrait>(
            "intersect_count");
    add_object_mapping<TYPE_VARCHAR, TYPE_BIGINT, false, IntesectCountBuilder, IntersectCountStateTrait>(
            "intersect_count");

    for (auto func_name : std::vector<std::string>{"ndv", "approx_count_distinct"}) {
        add_object_mapping<TYPE_BOOLEAN, TYPE_BIGINT, false, NDVBuilder, NDVStateTrait>(func_name);
        add_object_mapping<TYPE_TINYINT, TYPE_BIGINT, false, NDVBuilder, NDVStateTrait>(func_name);
        add_object_mapping<TYPE_SMALLINT, TYPE_BIGINT, false, NDVBuilder, NDVStateTrait>(func_name);
        add_object_mapping<TYPE_INT, TYPE_BIGINT, false, NDVBuilder, NDVStateTrait>(func_name);
        add_object_mapping<TYPE_BIGINT, TYPE_BIGINT, false, NDVBuilder, NDVStateTrait>(func_name);
        add_object_mapping<TYPE_LARGEINT, TYPE_BIGINT, false, NDVBuilder, NDVStateTrait>(func_name);
        add_object_mapping<TYPE_FLOAT, TYPE_BIGINT, false, NDVBuilder, NDVStateTrait>(func_name);
        add_object_mapping<TYPE_DOUBLE, TYPE_BIGINT, false, NDVBuilder, NDVStateTrait>(func_name);
        add_object_mapping<TYPE_CHAR, TYPE_BIGINT, false, NDVBuilder, NDVStateTrait>(func_name);
        add_object_mapping<TYPE_VARCHAR, TYPE_BIGINT, false, NDVBuilder, NDVStateTrait>(func_name);
        add_object_mapping<TYPE_DECIMALV2, TYPE_BIGINT, false, NDVBuilder, NDVStateTrait>(func_name);
        add_object_mapping<TYPE_DATETIME, TYPE_BIGINT, false, NDVBuilder, NDVStateTrait>(func_name);
        add_object_mapping<TYPE_DATE, TYPE_BIGINT, false, NDVBuilder, NDVStateTrait>(func_name);
        add_object_mapping<TYPE_DECIMAL32, TYPE_BIGINT, false, NDVBuilder, NDVStateTrait>(func_name);
        add_object_mapping<TYPE_DECIMAL64, TYPE_BIGINT, false, NDVBuilder, NDVStateTrait>(func_name);
        add_object_mapping<TYPE_DECIMAL128, TYPE_BIGINT, false, NDVBuilder, NDVStateTrait>(func_name);
    }
}

} // namespace starrocks::vectorized