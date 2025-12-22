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
#include "common/statusor.h"
#include "formats/parquet/variant.h"
#include "function_helper.h"
#include "types/logical_type.h"

namespace starrocks {

class VariantFunctions {
public:
    /**
     * @param: [variant, path]
     * @paramType: [VariantColumn, BinaryColumn]
     * @return: VariantColumn
     */
    DEFINE_VECTORIZED_FN(variant_query);

    /**
     *
     * @param [variant, json_path]
     * @paramType: [VariantColumn, BinaryColumn]
     * @return : ResultTypeColumn
     */
    DEFINE_VECTORIZED_FN(get_variant_bool);
    // return bigint to unify all integer types
    DEFINE_VECTORIZED_FN(get_variant_int);
    DEFINE_VECTORIZED_FN(get_variant_double);
    DEFINE_VECTORIZED_FN(get_variant_string);

    /**
     * @param: [variant, path]
     * @paramType: [VariantColumn, BinaryColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(variant_typeof);

    // Preload the variant segments if necessary.
    // This function is called once per query execution
    // The scope indicates whether the state is shared across the plan fragment
    // (FRAGMENT_LOCAL) or local to the execution thread (THREAD_LOCAL).
    // Returns Status::OK() on success, or an error status if initialization fails.
    static Status variant_segments_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    // Clear the variant segments state.
    static Status variant_segments_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

private:
    template <LogicalType ResultType>
    static StatusOr<ColumnPtr> _do_variant_query(FunctionContext* context, const Columns& vector);
};

} // namespace starrocks