// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
     * @return ARRAY_BIGINT
     */
    DEFINE_VECTORIZED_FN(bitmap_to_array);
    static void detect_bitmap_cardinality(size_t* data_size, const int64_t cardinality);

    /**
     * @param:
     * @paramType columns: [ARRAY_BIGINT]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(array_to_bitmap);

    /**
     * @param:
     * @paramType columns: [TYPE_OBJECT]
     * @return TYPE_LARGEINT
     */
    DEFINE_VECTORIZED_FN(bitmap_max);

    /**
     * @param:
     * @paramType columns: [TYPE_OBJECT]
     * @return TYPE_LARGEINT
     */
    DEFINE_VECTORIZED_FN(bitmap_min);

    /**
     * @param:
     * @paramType columns: [TYPE_VARCHAR]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(base64_to_bitmap);

    /**
     * @param:
     * @paramType columns: [TYPE_OBJECT, TYPE_BIGINT, TYPE_BIGINT]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(sub_bitmap);

    /**
     * @param:
     * @paramType columns: [TYPE_OBJECT]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_to_base64);
<<<<<<< HEAD:be/src/exprs/vectorized/bitmap_functions.h
=======

    /**
     * @param:
     * @paramType columns: [TYPE_OBJECT, TYPE_BIGINT, TYPE_BIGINT]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_subset_in_range);

    /**
     * @param:
     * @paramType columns: [TYPE_OBJECT, TYPE_BIGINT, TYPE_BIGINT]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_subset_limit);

    /**
     * @param:
     * @paramType columns: [TYPE_BITMAP]
     * @return TYPE_VARCHAR
     */
    DEFINE_VECTORIZED_FN(bitmap_to_binary);

    /**
     * @param
     * @paramType columns: [TYPE_VARCHAR]
     * @return TYPE_BITMAP
     */
    DEFINE_VECTORIZED_FN(bitmap_from_binary);
>>>>>>> eb4f6912b2 ([Feature] Add function bitmap_from_binary and bitmap_to_binary (#35621)):be/src/exprs/bitmap_functions.h
};

} // namespace vectorized
} // namespace starrocks
