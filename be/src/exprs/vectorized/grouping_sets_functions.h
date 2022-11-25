// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exprs/vectorized/function_helper.h"
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
    DEFINE_VECTORIZED_FN(grouping_id);

    /**
     * @param: [bigint]
     * Returns the grouping_id
     */
    DEFINE_VECTORIZED_FN(grouping);
};

} // namespace vectorized
} // namespace starrocks
