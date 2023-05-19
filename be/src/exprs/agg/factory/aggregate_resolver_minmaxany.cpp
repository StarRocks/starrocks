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

#include "exprs/agg/any_value.h"
#include "exprs/agg/bitmap_intersect.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "exprs/agg/maxmin.h"
#include "types/bitmap_value.h"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"

namespace starrocks {

void AggregateFuncResolver::register_bitmap() {
    add_aggregate_mapping<TYPE_TINYINT, TYPE_BIGINT, BitmapValue>(
            "bitmap_union_int", false, AggregateFactory::MakeBitmapUnionIntAggregateFunction<TYPE_TINYINT>());
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_BIGINT, BitmapValue>(
            "bitmap_union_int", false, AggregateFactory::MakeBitmapUnionIntAggregateFunction<TYPE_SMALLINT>());
    add_aggregate_mapping<TYPE_INT, TYPE_BIGINT, BitmapValue>(
            "bitmap_union_int", false, AggregateFactory::MakeBitmapUnionIntAggregateFunction<TYPE_INT>());
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, BitmapValue>(
            "bitmap_union_int", false, AggregateFactory::MakeBitmapUnionIntAggregateFunction<TYPE_BIGINT>());
    add_aggregate_mapping<TYPE_OBJECT, TYPE_OBJECT, BitmapValue>("bitmap_union", false,
                                                                 AggregateFactory::MakeBitmapUnionAggregateFunction());

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_OBJECT, BitmapValue>(
            "bitmap_agg", false, AggregateFactory::MakeBitmapAggAggregateFunction<TYPE_BOOLEAN>());
    add_aggregate_mapping<TYPE_TINYINT, TYPE_OBJECT, BitmapValue>(
            "bitmap_agg", false, AggregateFactory::MakeBitmapAggAggregateFunction<TYPE_TINYINT>());
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_OBJECT, BitmapValue>(
            "bitmap_agg", false, AggregateFactory::MakeBitmapAggAggregateFunction<TYPE_SMALLINT>());
    add_aggregate_mapping<TYPE_INT, TYPE_OBJECT, BitmapValue>(
            "bitmap_agg", false, AggregateFactory::MakeBitmapAggAggregateFunction<TYPE_INT>());
    add_aggregate_mapping<TYPE_BIGINT, TYPE_OBJECT, BitmapValue>(
            "bitmap_agg", false, AggregateFactory::MakeBitmapAggAggregateFunction<TYPE_BIGINT>());
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_OBJECT, BitmapValue>(
            "bitmap_agg", false, AggregateFactory::MakeBitmapAggAggregateFunction<TYPE_LARGEINT>());

    add_aggregate_mapping<TYPE_OBJECT, TYPE_OBJECT, BitmapValuePacked>(
            "bitmap_intersect", false, AggregateFactory::MakeBitmapIntersectAggregateFunction());
    add_aggregate_mapping<TYPE_OBJECT, TYPE_BIGINT, BitmapValue>(
            "bitmap_union_count", true, AggregateFactory::MakeBitmapUnionCountAggregateFunction());
}

struct MinMaxAnyDispatcher {
    template <LogicalType lt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_aggregate<lt> || lt_is_json<lt>) {
            resolver->add_aggregate_mapping<lt, lt, MinAggregateData<lt>>(
                    "min", true, AggregateFactory::MakeMinAggregateFunction<lt>());
            resolver->add_aggregate_mapping<lt, lt, MaxAggregateData<lt>>(
                    "max", true, AggregateFactory::MakeMaxAggregateFunction<lt>());
            resolver->add_aggregate_mapping<lt, lt, AnyValueAggregateData<lt>>(
                    "any_value", true, AggregateFactory::MakeAnyValueAggregateFunction<lt>());
        }
    }
};

template <LogicalType ret_type, bool is_max_by>
struct MaxMinByDispatcherInner {
    template <LogicalType arg_type>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr ((lt_is_aggregate<arg_type> || lt_is_json<arg_type>)&&(lt_is_aggregate<ret_type> ||
                                                                            lt_is_json<ret_type>)) {
            if constexpr (is_max_by) {
                resolver->add_aggregate_mapping_variadic<arg_type, ret_type, MaxByAggregateData<arg_type>>(
                        "max_by", true, AggregateFactory::MakeMaxByAggregateFunction<arg_type>());
            } else {
                resolver->add_aggregate_mapping_variadic<arg_type, ret_type, MinByAggregateData<arg_type>>(
                        "min_by", true, AggregateFactory::MakeMinByAggregateFunction<arg_type>());
            }
        }
    }
};

template <bool is_max_by>
struct MaxMinByDispatcher {
    template <LogicalType lt>
    void operator()(AggregateFuncResolver* resolver, LogicalType ret_type) {
        type_dispatch_all(ret_type, MaxMinByDispatcherInner<lt, is_max_by>(), resolver);
    }
};

void AggregateFuncResolver::register_minmaxany() {
    auto minmax_types = aggregate_types();
    minmax_types.push_back(TYPE_JSON);
    for (auto ret_type : minmax_types) {
        for (auto arg_type : minmax_types) {
            type_dispatch_all(arg_type, MaxMinByDispatcher<true>(), this, ret_type);
            type_dispatch_all(arg_type, MaxMinByDispatcher<false>(), this, ret_type);
        }
    }

    for (auto type : minmax_types) {
        type_dispatch_all(type, MinMaxAnyDispatcher(), this);
    }
}

} // namespace starrocks
