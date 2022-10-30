// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "boost/exception/exception.hpp"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/any_value.h"
#include "exprs/agg/bitmap_intersect.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "exprs/agg/maxmin.h"
#include "runtime/primitive_type.h"
#include "types/bitmap_value.h"

namespace starrocks::vectorized {

template <PrimitiveType pt>
using MinStateTrait = MinAggregateData<pt>;
template <PrimitiveType pt>
using MaxStateTrait = MaxAggregateData<pt>;
template <PrimitiveType pt>
using AnyStateTrait = AnyValueAggregateData<pt>;
template <PrimitiveType pt>
inline constexpr PrimitiveType MaxMinResultTrait = pt;

template <PrimitiveType pt>
struct MinBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeMinAggregateFunction<pt>(); }
};
template <PrimitiveType pt>
struct MaxBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeMaxAggregateFunction<pt>(); }
};
template <PrimitiveType pt>
struct AnyBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeAnyValueAggregateFunction<pt>(); }
};
template <PrimitiveType pt>
struct MaybyBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeMaxByAggregateFunction<pt>(); }
};

void AggregateFuncResolver::register_bitmap() {
    add_aggregate_mapping<TYPE_TINYINT, TYPE_BIGINT, BitmapValue>(
            "bitmap_union_int", false, AggregateFactory::MakeBitmapUnionIntAggregateFunction<TYPE_TINYINT>());
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_BIGINT, BitmapValue>(
            "bitmap_union_int", false, AggregateFactory::MakeBitmapUnionIntAggregateFunction<TYPE_SMALLINT>());
    add_aggregate_mapping<TYPE_INT, TYPE_BIGINT, BitmapValue>(
            "bitmap_union_int", false, AggregateFactory::MakeBitmapUnionIntAggregateFunction<TYPE_INT>());
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, BitmapValue>(
            "bitmap_union_int", false, AggregateFactory::MakeBitmapUnionIntAggregateFunction<TYPE_BIGINT>());
    add_object_mapping<TYPE_OBJECT, TYPE_OBJECT, false, BitmapValue>(
            "bitmap_union", AggregateFactory::MakeBitmapUnionAggregateFunction());
    add_object_mapping<TYPE_OBJECT, TYPE_OBJECT, false, BitmapValuePacked>(
            "bitmap_intersect", AggregateFactory::MakeBitmapIntersectAggregateFunction());
    add_object_mapping<TYPE_OBJECT, TYPE_BIGINT, true, BitmapValue>(
            "bitmap_union_count", AggregateFactory::MakeBitmapUnionCountAggregateFunction());
}

void AggregateFuncResolver::register_minmaxany() {
    ADD_ALL_TYPE1("max_by", TYPE_BOOLEAN);
    ADD_ALL_TYPE1("max_by", TYPE_TINYINT);
    ADD_ALL_TYPE1("max_by", TYPE_SMALLINT);
    ADD_ALL_TYPE1("max_by", TYPE_INT);
    ADD_ALL_TYPE1("max_by", TYPE_BIGINT);
    ADD_ALL_TYPE1("max_by", TYPE_LARGEINT);
    ADD_ALL_TYPE1("max_by", TYPE_FLOAT);
    ADD_ALL_TYPE1("max_by", TYPE_DOUBLE);
    ADD_ALL_TYPE1("max_by", TYPE_VARCHAR);
    ADD_ALL_TYPE1("max_by", TYPE_CHAR);
    ADD_ALL_TYPE1("max_by", TYPE_DECIMALV2);
    ADD_ALL_TYPE1("max_by", TYPE_DATETIME);
    ADD_ALL_TYPE1("max_by", TYPE_DATE);
    ADD_ALL_TYPE1("max_by", TYPE_DECIMAL32);
    ADD_ALL_TYPE1("max_by", TYPE_DECIMAL64);
    ADD_ALL_TYPE1("max_by", TYPE_DECIMAL128);

    AGGREGATE_ALL_TYPE_FROM_TRAIT("max", true, MaxMinResultTrait, MaxStateTrait, MaxBuilder);
    AGGREGATE_ALL_TYPE_FROM_TRAIT("min", true, MaxMinResultTrait, MinStateTrait, MinBuilder);
    AGGREGATE_ALL_TYPE_FROM_TRAIT("any_value", true, MaxMinResultTrait, AnyStateTrait, AnyBuilder);

    add_aggregate_mapping<TYPE_VARCHAR, TYPE_VARCHAR, MinStateTrait, MinBuilder>("min", true);
    add_aggregate_mapping<TYPE_CHAR, TYPE_CHAR, MinStateTrait, MinBuilder>("min", true);

    add_aggregate_mapping<TYPE_VARCHAR, TYPE_VARCHAR, MaxStateTrait, MaxBuilder>("max", true);
    add_aggregate_mapping<TYPE_CHAR, TYPE_CHAR, MaxStateTrait, MaxBuilder>("max", true);

    add_aggregate_mapping<TYPE_VARCHAR, TYPE_VARCHAR, AnyStateTrait, AnyBuilder>("any_value", true);
    add_aggregate_mapping<TYPE_CHAR, TYPE_CHAR, AnyStateTrait, AnyBuilder>("any_value", true);
    add_aggregate_mapping<TYPE_JSON, TYPE_JSON, AnyStateTrait, AnyBuilder>("any_value", true);
}

} // namespace starrocks::vectorized
