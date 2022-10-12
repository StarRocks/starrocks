// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exprs/vectorized/function_helper.h"

namespace starrocks {
namespace vectorized {
class ESFunctions {
public:
    /**
     * used to push down query conditions to es.
     * @return true.
     */
    DEFINE_VECTORIZED_FN(match);
};

} // namespace vectorized
} // namespace starrocks
