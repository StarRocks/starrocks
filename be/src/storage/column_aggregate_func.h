// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/vectorized_field.h"
#include "storage/column_aggregator.h"

namespace starrocks::vectorized {
class ColumnAggregatorFactory {
public:
    static ColumnAggregatorPtr create_key_column_aggregator(const VectorizedFieldPtr& field);
    static ColumnAggregatorPtr create_value_column_aggregator(const VectorizedFieldPtr& field);
};

} // namespace starrocks::vectorized
