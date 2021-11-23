// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exprs/vectorized/function_helper.h"

namespace starrocks {
namespace vectorized {
class UtilityFunctions {
public:
    /**
     * return version.
     */
    DEFINE_VECTORIZED_FN(version);
    DEFINE_VECTORIZED_FN(current_version);

    /**
     * sleep for int seconds.
     * @param: [int]
     * @return true
     */
    DEFINE_VECTORIZED_FN(sleep);

    DEFINE_VECTORIZED_FN(last_query_id);

    /**
     * returns uuid.
     */
    DEFINE_VECTORIZED_FN(uuid);
};

} // namespace vectorized
} // namespace starrocks
