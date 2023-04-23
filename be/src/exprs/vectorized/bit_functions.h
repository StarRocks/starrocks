// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/column.h"
#include "exprs/vectorized/binary_function.h"
#include "exprs/vectorized/unary_function.h"
#include "runtime/primitive_type.h"

namespace starrocks {
namespace vectorized {

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
    template <PrimitiveType Type>
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
    template <PrimitiveType Type>
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
    template <PrimitiveType Type>
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
    template <PrimitiveType Type>
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
    template <PrimitiveType Type>
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
    template <PrimitiveType Type>
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
    template <PrimitiveType Type>
    DEFINE_VECTORIZED_FN(bitNot) {
        auto v = VECTORIZED_FN_ARGS(0);
        return VectorizedStrictUnaryFunction<bitNotImpl>::evaluate<Type>(v);
    }
};
} // namespace vectorized
} // namespace starrocks
