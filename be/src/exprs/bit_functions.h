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
#include "exprs/binary_function.h"
#include "exprs/unary_function.h"
#include "types/logical_type.h"

namespace starrocks {

#define VECTORIZED_BIT_BINARY_IMPL(NAME, OP) \
    DEFINE_BINARY_FUNCTION_WITH_IMPL(NAME##Impl, l, r) { return l OP r; }

VECTORIZED_BIT_BINARY_IMPL(bitAnd, &);
VECTORIZED_BIT_BINARY_IMPL(bitOr, |);
VECTORIZED_BIT_BINARY_IMPL(bitXor, ^);
VECTORIZED_BIT_BINARY_IMPL(bitShiftLeft, <<);
VECTORIZED_BIT_BINARY_IMPL(bitShiftRight, >>);

#undef VECTORIZED_BIT_BINARY_IMPL

DEFINE_BINARY_FUNCTION_WITH_IMPL(bitShiftRightLogicalImpl, v, shift) {
    if constexpr (std::is_same_v<LType, int8_t>) {
        return uint8_t(v) >> shift;
    } else if constexpr (std::is_same_v<LType, int16_t>) {
        return uint16_t(v) >> shift;
    } else if constexpr (std::is_same_v<LType, int32_t>) {
        return uint32_t(v) >> shift;
    } else if constexpr (std::is_same_v<LType, int64_t>) {
        return uint64_t(v) >> shift;
    } else if constexpr (std::is_same_v<LType, __int128_t>) {
        return uint128_t(v) >> shift;
    } else {
        return v >> shift;
    }
}

DEFINE_UNARY_FN_WITH_IMPL(bitNotImpl, v) {
    return ~v;
}

class BitFunctions {
public:
    /**
     * @tparam : TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT
     * @param: [TypeColumn, TypeColumn]
     * @return: TypeColumn
     */
    template <LogicalType Type>
    DEFINE_VECTORIZED_FN(bitAnd) {
        auto l = VECTORIZED_FN_ARGS(0);
        auto r = VECTORIZED_FN_ARGS(1);
        return VectorizedStrictBinaryFunction<bitAndImpl>::evaluate<Type>(l, r);
    }

    /**
     * @tparam : TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT
     * @param: [TypeColumn, TypeColumn]
     * @return: TypeColumn
     */
    template <LogicalType Type>
    DEFINE_VECTORIZED_FN(bitOr) {
        auto l = VECTORIZED_FN_ARGS(0);
        auto r = VECTORIZED_FN_ARGS(1);
        return VectorizedStrictBinaryFunction<bitOrImpl>::evaluate<Type>(l, r);
    }

    /**
     * @tparam : TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT
     * @param: [TypeColumn, TypeColumn]
     * @return: TypeColumn
     */
    template <LogicalType Type>
    DEFINE_VECTORIZED_FN(bitXor) {
        auto l = VECTORIZED_FN_ARGS(0);
        auto r = VECTORIZED_FN_ARGS(1);
        return VectorizedStrictBinaryFunction<bitXorImpl>::evaluate<Type>(l, r);
    }

    /**
     * @tparam : TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT
     * @param: [TypeColumn, TypeColumn]
     * @return: TypeColumn
     */
    template <LogicalType Type>
    DEFINE_VECTORIZED_FN(bitShiftLeft) {
        auto l = VECTORIZED_FN_ARGS(0);
        auto r = VECTORIZED_FN_ARGS(1);
        return VectorizedStrictBinaryFunction<bitShiftLeftImpl>::evaluate<Type>(l, r);
    }

    /**
     * @tparam : TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT
     * @param: [TypeColumn, TypeColumn]
     * @return: TypeColumn
     */
    template <LogicalType Type>
    DEFINE_VECTORIZED_FN(bitShiftRight) {
        auto l = VECTORIZED_FN_ARGS(0);
        auto r = VECTORIZED_FN_ARGS(1);
        return VectorizedStrictBinaryFunction<bitShiftRightImpl>::evaluate<Type>(l, r);
    }

    /**
     * @tparam : TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT
     * @param: [TypeColumn, TypeColumn]
     * @return: TypeColumn
     */
    template <LogicalType Type>
    DEFINE_VECTORIZED_FN(bitShiftRightLogical) {
        auto l = VECTORIZED_FN_ARGS(0);
        auto r = VECTORIZED_FN_ARGS(1);
        return VectorizedStrictBinaryFunction<bitShiftRightLogicalImpl>::evaluate<Type>(l, r);
    }

    /**
     * @tparam : TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT
     * @param: [TypeColumn]
     * @return: TypeColumn
     */
    template <LogicalType Type>
    DEFINE_VECTORIZED_FN(bitNot) {
        auto v = VECTORIZED_FN_ARGS(0);
        return VectorizedStrictUnaryFunction<bitNotImpl>::evaluate<Type>(v);
    }
};
} // namespace starrocks
