// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "geo/geo_common.h"
#include "udf/udf.h"

namespace starrocks {
namespace vectorized {

class GroupingSetsFunctions {
public:
    /**
     * @param: [bigint]
     * Returns the grouping_id of columns
     */
    static ColumnPtr grouping_id(FunctionContext* context, const Columns& columns);

    /**
     * @param: [bigint]
     * Returns the grouping_id
     */
    static ColumnPtr grouping(FunctionContext* context, const Columns& columns);
};

} // namespace vectorized
} // namespace starrocks
