// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/column.h"
#include "exprs/vectorized/function_helper.h"

namespace starrocks {
namespace vectorized {
class PercentileFunctions {
public:
    /**
     * @param:
     * @paramType columns: [TYPE_DOUBLE]
     * @return TYPE_PERCENTILE
     */
    DEFINE_VECTORIZED_FN(percentile_hash);

    /**
     * @param:
     * @paramType columns: []
     * @return TYPE_PERCENTILE
     */
    DEFINE_VECTORIZED_FN(percentile_empty);

    /**
     * @param:
     * @paramType columns: [TYPE_PERCENTILE, TYPE_DOUBLE]
     * @return TYPE_DOUBLE
     */
    DEFINE_VECTORIZED_FN(percentile_approx_raw);
};
} // namespace vectorized
} // namespace starrocks
