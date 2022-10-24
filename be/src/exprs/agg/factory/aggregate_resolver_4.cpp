// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"

namespace starrocks::vectorized {

void AggregateFuncResolver::register_4() {
    std::vector<std::string> names = {"variance", "variance_pop", "var_pop",    "variance_samp", "var_samp",
                                      "stddev",   "std",          "stddev_pop", "stddev_samp"};

    for (auto var_name : names) {
        add_aggregate_mapping<TYPE_BOOLEAN, TYPE_DOUBLE>(var_name);
        add_aggregate_mapping<TYPE_TINYINT, TYPE_DOUBLE>(var_name);
        add_aggregate_mapping<TYPE_SMALLINT, TYPE_DOUBLE>(var_name);
        add_aggregate_mapping<TYPE_INT, TYPE_DOUBLE>(var_name);
        add_aggregate_mapping<TYPE_LARGEINT, TYPE_DOUBLE>(var_name);
        add_aggregate_mapping<TYPE_BIGINT, TYPE_DOUBLE>(var_name);
        add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE>(var_name);
        add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE>(var_name);
        add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2>(var_name);
        add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128>(var_name);
    }
}
} // namespace starrocks::vectorized