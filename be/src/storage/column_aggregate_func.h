// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/field.h"
#include "storage/column_aggregator.h"

namespace starrocks::vectorized {
class ColumnAggregatorFactory {
public:
    static ColumnAggregatorPtr create_key_column_aggregator(const FieldPtr& field);
    static ColumnAggregatorPtr create_value_column_aggregator(const FieldPtr& field);
};

} // namespace starrocks::vectorized
