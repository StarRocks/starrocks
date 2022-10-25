// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/any_value.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "exprs/agg/maxmin.h"

namespace starrocks::vectorized {

template <PrimitiveType pt>
using MinStateTrait = MinAggregateData<pt>;
template <PrimitiveType pt>
using MaxStateTrait = MaxAggregateData<pt>;
template <PrimitiveType pt>
using AnyStateTrait = AnyValueAggregateData<pt>;

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

void AggregateFuncResolver::register_2() {
    add_aggregate_mapping<TYPE_TINYINT, TYPE_BIGINT, BitmapValue>(
            "bitmap_union_int", false, AggregateFactory::MakeBitmapUnionIntAggregateFunction<TYPE_TINYINT>());
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_BIGINT, BitmapValue>(
            "bitmap_union_int", false, AggregateFactory::MakeBitmapUnionIntAggregateFunction<TYPE_SMALLINT>());
    add_aggregate_mapping<TYPE_INT, TYPE_BIGINT, BitmapValue>(
            "bitmap_union_int", false, AggregateFactory::MakeBitmapUnionIntAggregateFunction<TYPE_INT>());
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, BitmapValue>(
            "bitmap_union_int", false, AggregateFactory::MakeBitmapUnionIntAggregateFunction<TYPE_BIGINT>());

    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, true>("count");

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

    {
        std::string func_name = "max";
        add_aggregate_mapping<TYPE_BOOLEAN, TYPE_BOOLEAN, MaxStateTrait, MaxBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_TINYINT, TYPE_TINYINT, MaxStateTrait, MaxBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_SMALLINT, TYPE_SMALLINT, MaxStateTrait, MaxBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_INT, TYPE_INT, MaxStateTrait, MaxBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, MaxStateTrait, MaxBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_LARGEINT, TYPE_LARGEINT, MaxStateTrait, MaxBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_FLOAT, TYPE_FLOAT, MaxStateTrait, MaxBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE, MaxStateTrait, MaxBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_VARCHAR, TYPE_VARCHAR, MaxStateTrait, MaxBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_CHAR, TYPE_CHAR, MaxStateTrait, MaxBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2, MaxStateTrait, MaxBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DATETIME, TYPE_DATETIME, MaxStateTrait, MaxBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DATE, TYPE_DATE, MaxStateTrait, MaxBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DECIMAL32, TYPE_DECIMAL32, MaxStateTrait, MaxBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DECIMAL64, TYPE_DECIMAL64, MaxStateTrait, MaxBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, MaxStateTrait, MaxBuilder>(func_name, true);
    }
    {
        std::string func_name = "min";
        add_aggregate_mapping<TYPE_BOOLEAN, TYPE_BOOLEAN, MinStateTrait, MinBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_TINYINT, TYPE_TINYINT, MinStateTrait, MinBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_SMALLINT, TYPE_SMALLINT, MinStateTrait, MinBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_INT, TYPE_INT, MinStateTrait, MinBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, MinStateTrait, MinBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_LARGEINT, TYPE_LARGEINT, MinStateTrait, MinBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_FLOAT, TYPE_FLOAT, MinStateTrait, MinBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE, MinStateTrait, MinBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_VARCHAR, TYPE_VARCHAR, MinStateTrait, MinBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_CHAR, TYPE_CHAR, MinStateTrait, MinBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2, MinStateTrait, MinBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DATETIME, TYPE_DATETIME, MinStateTrait, MinBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DATE, TYPE_DATE, MinStateTrait, MinBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DECIMAL32, TYPE_DECIMAL32, MinStateTrait, MinBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DECIMAL64, TYPE_DECIMAL64, MinStateTrait, MinBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, MinStateTrait, MinBuilder>(func_name, true);
    }
    {
        std::string func_name = "any_value";
        add_aggregate_mapping<TYPE_BOOLEAN, TYPE_BOOLEAN, AnyStateTrait, AnyBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_TINYINT, TYPE_TINYINT, AnyStateTrait, AnyBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_SMALLINT, TYPE_SMALLINT, AnyStateTrait, AnyBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_INT, TYPE_INT, AnyStateTrait, AnyBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, AnyStateTrait, AnyBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_LARGEINT, TYPE_LARGEINT, AnyStateTrait, AnyBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_FLOAT, TYPE_FLOAT, AnyStateTrait, AnyBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE, AnyStateTrait, AnyBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_VARCHAR, TYPE_VARCHAR, AnyStateTrait, AnyBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_CHAR, TYPE_CHAR, AnyStateTrait, AnyBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2, AnyStateTrait, AnyBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DATETIME, TYPE_DATETIME, AnyStateTrait, AnyBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DATE, TYPE_DATE, AnyStateTrait, AnyBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DECIMAL32, TYPE_DECIMAL32, AnyStateTrait, AnyBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DECIMAL64, TYPE_DECIMAL64, AnyStateTrait, AnyBuilder>(func_name, true);
        add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, AnyStateTrait, AnyBuilder>(func_name, true);
    }

    add_aggregate_mapping<TYPE_JSON, TYPE_JSON, AnyValueAggregateData<TYPE_JSON>>(
            "any_value", false, AggregateFactory::MakeAnyValueAggregateFunction<TYPE_JSON>());
}

} // namespace starrocks::vectorized