// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/column.h"
#include "exprs/vectorized/function_helper.h"

namespace starrocks {
namespace vectorized {
class BitmapFunctions {
public:
    /**
     * @param: 
     * @paramType columns: [TYPE_VARCHAR]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN_TEMPLATE(to_bitmap);

    /**
     * @param: 
     * @paramType columns: [TYPE_VARCHAR]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_hash);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT]
     * @return TYPE_BIGINT
     */
    DEFINE_VECTORIZED_FN(bitmap_count);

    /**
     * @param: 
     * @paramType columns: []
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_empty);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT, TYPE_OBJECT]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_or);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT, TYPE_OBJECT]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_and);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT]
     * @return TYPE_VARCHAR
     */
    DEFINE_VECTORIZED_FN(bitmap_to_string);

    /**
     * @param: 
     * @paramType columns: [TYPE_VARCHAR]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_from_string);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT, TYPE_BIGINT]
     * @return TYPE_BOOLEAN
     */
    DEFINE_VECTORIZED_FN(bitmap_contains);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT, TYPE_OBJECT]
     * @return TYPE_BOOLEAN
     */
    DEFINE_VECTORIZED_FN(bitmap_has_any);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT, TYPE_OBJECT]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_andnot);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT, TYPE_OBJECT]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_xor);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT, TYPE_BIGINT]
     * @return TYPE_BOOLEAN
     */
    DEFINE_VECTORIZED_FN(bitmap_remove);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT]
     * @return ARRAY_VARCHAR
     */
    DEFINE_VECTORIZED_FN(bitmap_to_array);

    /**
     * @param:
     * @paramType columns: [TYPE_OBJECT]
     * @return TYPE_BIGINT
     */
    DEFINE_VECTORIZED_FN(bitmap_max);

    /**
     * @param:
     * @paramType columns: [TYPE_OBJECT]
     * @return TYPE_BIGINT
     */
    DEFINE_VECTORIZED_FN(bitmap_min);
};

} // namespace vectorized
} // namespace starrocks
