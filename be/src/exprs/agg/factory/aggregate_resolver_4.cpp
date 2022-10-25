// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "exprs/agg/variance.h"

namespace starrocks::vectorized {

template <PrimitiveType pt>
using VarStateTrait = DevFromAveAggregateState<RunTimeCppType<DevFromAveResultPT<pt>>>;

template <PrimitiveType pt>
struct VarBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeVarianceAggregateFunction<pt, false>(); }
};
template <PrimitiveType pt>
struct VarSampBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeVarianceAggregateFunction<pt, true>(); }
};

template <PrimitiveType pt>
struct StdBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeStddevAggregateFunction<pt, false>(); }
};
template <PrimitiveType pt>
struct StdSampBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeStddevAggregateFunction<pt, true>(); }
};

void AggregateFuncResolver::register_4() {
    constexpr bool is_window = false;
    {
        std::vector<std::string> names = {"variance", "variance_pop", "var_pop"};
        for (auto var_name : names) {
            add_aggregate_mapping<TYPE_BOOLEAN, TYPE_DOUBLE, VarStateTrait, VarBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_TINYINT, TYPE_DOUBLE, VarStateTrait, VarBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_SMALLINT, TYPE_DOUBLE, VarStateTrait, VarBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_INT, TYPE_DOUBLE, VarStateTrait, VarBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE, VarStateTrait, VarBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_LARGEINT, TYPE_DOUBLE, VarStateTrait, VarBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE, VarStateTrait, VarBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE, VarStateTrait, VarBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2, VarStateTrait, VarBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, VarStateTrait, VarBuilder>(var_name, is_window);
        }
    }
    {
        std::vector<std::string> names = {"variance_samp", "var_samp"};
        for (auto var_name : names) {
            add_aggregate_mapping<TYPE_BOOLEAN, TYPE_DOUBLE, VarStateTrait, VarSampBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_TINYINT, TYPE_DOUBLE, VarStateTrait, VarSampBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_SMALLINT, TYPE_DOUBLE, VarStateTrait, VarSampBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_INT, TYPE_DOUBLE, VarStateTrait, VarSampBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE, VarStateTrait, VarSampBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_LARGEINT, TYPE_DOUBLE, VarStateTrait, VarSampBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE, VarStateTrait, VarSampBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE, VarStateTrait, VarSampBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2, VarStateTrait, VarSampBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, VarStateTrait, VarSampBuilder>(var_name, is_window);
        }
    }
    {
        std::vector<std::string> names = {"stddev", "std", "stddev_pop"};
        for (auto var_name : names) {
            add_aggregate_mapping<TYPE_BOOLEAN, TYPE_DOUBLE, VarStateTrait, StdBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_TINYINT, TYPE_DOUBLE, VarStateTrait, StdBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_SMALLINT, TYPE_DOUBLE, VarStateTrait, StdBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_INT, TYPE_DOUBLE, VarStateTrait, StdBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE, VarStateTrait, StdBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_LARGEINT, TYPE_DOUBLE, VarStateTrait, StdBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE, VarStateTrait, StdBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE, VarStateTrait, StdBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2, VarStateTrait, StdBuilder>(var_name, is_window);
            add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, VarStateTrait, StdBuilder>(var_name, is_window);
        }
    }

    std::string var_name = "stddev_samp";
    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_DOUBLE, VarStateTrait, StdSampBuilder>(var_name, is_window);
    add_aggregate_mapping<TYPE_TINYINT, TYPE_DOUBLE, VarStateTrait, StdSampBuilder>(var_name, is_window);
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_DOUBLE, VarStateTrait, StdSampBuilder>(var_name, is_window);
    add_aggregate_mapping<TYPE_INT, TYPE_DOUBLE, VarStateTrait, StdSampBuilder>(var_name, is_window);
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE, VarStateTrait, StdSampBuilder>(var_name, is_window);
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_DOUBLE, VarStateTrait, StdSampBuilder>(var_name, is_window);
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE, VarStateTrait, StdSampBuilder>(var_name, is_window);
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE, VarStateTrait, StdSampBuilder>(var_name, is_window);
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2, VarStateTrait, StdSampBuilder>(var_name, is_window);
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, VarStateTrait, StdSampBuilder>(var_name, is_window);
}

} // namespace starrocks::vectorized