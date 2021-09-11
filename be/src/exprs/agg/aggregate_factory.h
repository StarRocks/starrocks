// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/agg/aggregate_factory.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "exprs/agg/aggregate.h"
#include "runtime/primitive_type.h"

namespace starrocks {
namespace vectorized {

class AggregateFactory {
public:
    // The function should be placed by alphabetical order
    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeAvgAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeBitmapUnionIntAggregateFunction();

    static AggregateFunctionPtr MakeBitmapUnionAggregateFunction();

    static AggregateFunctionPtr MakeBitmapIntersectAggregateFunction();

    static AggregateFunctionPtr MakeBitmapUnionCountAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeIntersectCountAggregateFunction();

    static AggregateFunctionPtr MakeCountAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeCountDistinctAggregateFunction();
    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeCountDistinctAggregateFunctionV2();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeGroupConcatAggregateFunction();

    static AggregateFunctionPtr MakeCountNullableAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeMaxAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeMinAggregateFunction();

    template <typename NestedState>
    static AggregateFunctionPtr MakeNullableAggregateFunctionUnary(AggregateFunctionPtr nested_function);

    template <typename NestedState>
    static AggregateFunctionPtr MakeNullableAggregateFunctionVariadic(AggregateFunctionPtr nested_function);

    template <PrimitiveType T>
    static AggregateFunctionPtr MakeSumAggregateFunction();

    template <PrimitiveType PT, bool is_sample>
    static AggregateFunctionPtr MakeVarianceAggregateFunction();

    template <PrimitiveType PT, bool is_sample>
    static AggregateFunctionPtr MakeStddevAggregateFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeSumDistinctAggregateFunction();
    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeSumDistinctAggregateFunctionV2();

    // Hyperloglog functions:
    static AggregateFunctionPtr MakeHllUnionAggregateFunction();

    static AggregateFunctionPtr MakeHllUnionCountAggregateFunction();

    template <PrimitiveType T>
    static AggregateFunctionPtr MakeHllNdvAggregateFunction();

    static AggregateFunctionPtr MakePercentileApproxAggregateFunction();

    static AggregateFunctionPtr MakePercentileUnionAggregateFunction();

    // Windows functions:
    static AggregateFunctionPtr MakeDenseRankWindowFunction();

    static AggregateFunctionPtr MakeRankWindowFunction();

    static AggregateFunctionPtr MakeRowNumberWindowFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeFirstValueWindowFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeLastValueWindowFunction();

    template <PrimitiveType PT>
    static AggregateFunctionPtr MakeLeadLagWindowFunction();
};

extern const AggregateFunction* get_aggregate_function(const std::string& name, PrimitiveType arg_type,
                                                       PrimitiveType return_type, bool is_null);

} // namespace vectorized
} // namespace starrocks
