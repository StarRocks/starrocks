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

#include "exprs/function_helper.h"
#include "types/logical_type.h"

namespace starrocks {

class BitFunctions {
public:
    /**
     * @tparam : TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT
     * @param: [TypeColumn, TypeColumn]
     * @return: TypeColumn
     */
    template <LogicalType Type>
    DEFINE_VECTORIZED_FN(bitAnd);

    /**
     * @tparam : TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT
     * @param: [TypeColumn, TypeColumn]
     * @return: TypeColumn
     */
    template <LogicalType Type>
    DEFINE_VECTORIZED_FN(bitOr);

    /**
     * @tparam : TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT
     * @param: [TypeColumn, TypeColumn]
     * @return: TypeColumn
     */
    template <LogicalType Type>
    DEFINE_VECTORIZED_FN(bitXor);

    /**
     * @tparam : TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT
     * @param: [TypeColumn, TypeColumn]
     * @return: TypeColumn
     */
    template <LogicalType Type>
    DEFINE_VECTORIZED_FN(bitShiftLeft);

    /**
     * @tparam : TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT
     * @param: [TypeColumn, TypeColumn]
     * @return: TypeColumn
     */
    template <LogicalType Type>
    DEFINE_VECTORIZED_FN(bitShiftRight);

    /**
     * @tparam : TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT
     * @param: [TypeColumn, TypeColumn]
     * @return: TypeColumn
     */
    template <LogicalType Type>
    DEFINE_VECTORIZED_FN(bitShiftRightLogical);

    /**
     * @tparam : TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT
     * @param: [TypeColumn]
     * @return: TypeColumn
     */
    template <LogicalType Type>
    DEFINE_VECTORIZED_FN(bitNot);
};
} // namespace starrocks
