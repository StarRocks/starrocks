// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"

namespace starrocks::vectorized {

void AggregateFuncResolver::register_2() {
    add_aggregate_mapping<TYPE_TINYINT, TYPE_BIGINT>("bitmap_union_int");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_BIGINT>("bitmap_union_int");
    add_aggregate_mapping<TYPE_INT, TYPE_BIGINT>("bitmap_union_int");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT>("bitmap_union_int");

    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, true>("count");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT>("exchange_bytes");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_VARCHAR>("exchange_speed");

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

    ADD_ALL_TYPE("max", true);
    ADD_ALL_TYPE("min", true);
    ADD_ALL_TYPE("any_value", true);
    add_aggregate_mapping<TYPE_JSON, TYPE_JSON>("any_value");
}

} // namespace starrocks::vectorized