// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "column/column.h"
#include "exprs/function_helper.h"

namespace starrocks {
class BitmapFunctions {
public:
    /**
     * @param: 
     * @paramType columns: [TYPE_VARCHAR]
     * @return TYPE_OBJECT
     */
    template <LogicalType LT>
    static StatusOr<ColumnPtr> to_bitmap(FunctionContext* context, const starrocks::Columns& columns);

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
};

} // namespace starrocks
