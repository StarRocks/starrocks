// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exprs/vectorized/function_helper.h"

namespace starrocks::vectorized {
class HyperloglogFunctions {
public:
    /**
     * @param: [hyperloglog string]
     * @paramType: [BinaryColumn]
     * @return: BigIntColumn
     */
    DEFINE_VECTORIZED_FN(hll_cardinality_from_string);

    /**
     * @param: [hyperloglog string]
     * @paramType: [HyperLogLogColumn]
     * @return: BigIntColumn
     */
    DEFINE_VECTORIZED_FN(hll_cardinality);

    /**
     * @param: [hyperloglog string]
     * @paramType: [BinaryColumn]
     * @return: HllColumn
     */
    DEFINE_VECTORIZED_FN(hll_hash);

    /**
     * @param: []
     * @paramType: [BinaryColumn]
     * @return: HllColumn
     */
    DEFINE_VECTORIZED_FN(hll_empty);

    DEFINE_VECTORIZED_FN(hll_serialize);

    DEFINE_VECTORIZED_FN(hll_deserialize);
};

} // namespace starrocks::vectorized
