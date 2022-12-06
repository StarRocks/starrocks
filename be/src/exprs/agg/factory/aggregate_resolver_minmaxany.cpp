// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#include "boost/exception/exception.hpp"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/any_value.h"
#include "exprs/agg/bitmap_intersect.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "exprs/agg/maxmin.h"
#include "runtime/primitive_type.h"
#include "runtime/primitive_type_infra.h"
#include "types/bitmap_value.h"

namespace starrocks::vectorized {

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
    add_aggregate_mapping<TYPE_OBJECT, TYPE_OBJECT, BitmapValuePacked>(
            "bitmap_intersect", false, AggregateFactory::MakeBitmapIntersectAggregateFunction());
    add_aggregate_mapping<TYPE_OBJECT, TYPE_BIGINT, BitmapValue>(
            "bitmap_union_count", true, AggregateFactory::MakeBitmapUnionCountAggregateFunction());
}

struct MinMaxAnyDispatcher {
    template <LogicalType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (pt_is_aggregate<pt> || pt_is_string<pt>) {
            resolver->add_aggregate_mapping<pt, pt, MinAggregateData<pt>>(
                    "min", true, AggregateFactory::MakeMinAggregateFunction<pt>());
            resolver->add_aggregate_mapping<pt, pt, MaxAggregateData<pt>>(
                    "max", true, AggregateFactory::MakeMaxAggregateFunction<pt>());
            resolver->add_aggregate_mapping<pt, pt, AnyValueAggregateData<pt>>(
                    "any_value", true, AggregateFactory::MakeAnyValueAggregateFunction<pt>());
        }
        if constexpr (pt_is_json<pt>) {
            resolver->add_aggregate_mapping<pt, pt, AnyValueAggregateData<pt>>(
                    "any_value", true, AggregateFactory::MakeAnyValueAggregateFunction<pt>());
        }
    }
};

template <LogicalType ret_type>
struct MaxByDispatcherInner {
    template <LogicalType arg_type>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr ((pt_is_aggregate<arg_type> || pt_is_string<arg_type>)&&(pt_is_aggregate<ret_type> ||
                                                                              pt_is_string<ret_type>)) {
            resolver->add_aggregate_mapping_variadic<arg_type, ret_type, MaxByAggregateData<arg_type>>(
                    "max_by", true, AggregateFactory::MakeMaxByAggregateFunction<arg_type>());
        }
    }
};

struct MaxByDispatcher {
    template <LogicalType pt>
    void operator()(AggregateFuncResolver* resolver, LogicalType ret_type) {
        type_dispatch_all(ret_type, MaxByDispatcherInner<pt>(), resolver);
    }
};

void AggregateFuncResolver::register_minmaxany() {
    for (auto ret_type : aggregate_types()) {
        for (auto arg_type : aggregate_types()) {
            type_dispatch_all(arg_type, MaxByDispatcher(), this, ret_type);
        }
    }

    for (auto type : aggregate_types()) {
        type_dispatch_all(type, MinMaxAnyDispatcher(), this);
    }
    type_dispatch_all(TYPE_JSON, MinMaxAnyDispatcher(), this);
}

} // namespace starrocks::vectorized
