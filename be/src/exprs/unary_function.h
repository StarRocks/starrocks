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

#include <fmt/compile.h>
#include <fmt/format.h>

#include <cstdint>

#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "common/logging.h"
#include "function_helper.h"
#include "simd/simd.h"

namespace starrocks {

class NopCheck {
    template <typename Type, typename ResultType>
    static inline ResultType apply(const Type& VALUE) {
        return false;
    }
};

template <typename OP, typename INPUT_NULL_OP = NopCheck, typename OUTPUT_NULL_OP = NopCheck>
class ProduceNullUnaryFunction {
public:
    template <LogicalType Type, LogicalType ResultType, typename... Args>
    static ColumnPtr evaluate(const ColumnPtr& v1, Args&&... args) {
        auto* r1 = ColumnHelper::cast_to_raw<Type>(v1)->get_data().data();

        auto result = RunTimeColumnType<ResultType>::create(std::forward<Args>(args)...);
        result->resize(v1->size());
        auto* r3 = result->get_data().data();

        int size = v1->size();
        for (int i = 0; i < size; ++i) {
            r3[i] = OP::template apply<RunTimeCppType<Type>, RunTimeCppType<ResultType>>(r1[i]);
        }

        auto nulls = RunTimeColumnType<TYPE_NULL>::create();
        nulls->resize(v1->size());
        auto* ns = nulls->get_data().data();

        if constexpr (!std::is_same<INPUT_NULL_OP, NopCheck>::value) {
            for (int i = 0; i < size; ++i) {
                ns[i] = INPUT_NULL_OP::template apply<RunTimeCppType<Type>, RunTimeCppType<ResultType>>(r1[i]);
            }
        }

        if constexpr (!std::is_same<OUTPUT_NULL_OP, NopCheck>::value) {
            for (int i = 0; i < size; ++i) {
                ns[i] = OUTPUT_NULL_OP::template apply<RunTimeCppType<ResultType>, RunTimeCppType<ResultType>>(r3[i]);
            }
        }

        if (SIMD::count_nonzero(nulls->get_data())) {
            return NullableColumn::create(result, nulls);
        }
        return result;
    }
};

/**
 * Execute operator function
 * @param OP: the operations impl, like NullMerge
 */

template <typename OP>
class UnaryFunction {
public:
    /**
   * The Type, ResultType is LogicalType which can return actual CppType and ColumnType
   * through RuntimeTypeTraits.
   *
   * The method declaration like: ResultType::CppType apply(Type::CppType l)
   */
    template <LogicalType Type, LogicalType ResultType, typename... Args>
    static ColumnPtr evaluate(const ColumnPtr& v1, Args&&... args) {
        auto* r1 = ColumnHelper::cast_to_raw<Type>(v1)->get_data().data();

        auto result = RunTimeColumnType<ResultType>::create(std::forward<Args>(args)...);
        result->resize(v1->size());
        auto* r3 = result->get_data().data();

        int size = v1->size();
        for (int i = 0; i < size; ++i) {
            r3[i] = OP::template apply<RunTimeCppType<Type>, RunTimeCppType<ResultType>>(r1[i]);
        }

        return result;
    }
};

/**
 * Special for result type is String function
 * @param OP: the operations impl, like NullMerge
 */
template <typename OP>
struct StringUnaryFunction {
public:
    template <LogicalType Type, LogicalType ResultType, typename... Args>
    static ColumnPtr evaluate(const ColumnPtr& v1, Args&&... args) {
        auto& r1 = ColumnHelper::cast_to_raw<Type>(v1)->get_data();

        auto result = RunTimeColumnType<TYPE_VARCHAR>::create(std::forward<Args>(args)...);

        auto& offset = result->get_offset();
        auto& bytes = result->get_bytes();
        int size = v1->size();
        for (int i = 0; i < size; ++i) {
            std::string ret = OP::template apply<RunTimeCppType<Type>, std::string>(r1[i], std::forward<Args>(args)...);
            bytes.reserve(ret.size());
            bytes.insert(bytes.end(), (uint8_t*)ret.data(), (uint8_t*)ret.data() + ret.size());
            offset.emplace_back(bytes.size());
        }

        return result;
    }
};

/**
 * First: open const column
 * Then: execute FN function
 * @param OP the actual function
 */
template <typename FN>
class UnpackConstColumnUnaryFunction {
public:
    template <LogicalType Type, LogicalType ResultType, typename... Args>
    static inline ColumnPtr evaluate(const ColumnPtr& v1, Args&&... args) {
        if (v1->is_constant()) {
            auto eva1 = ColumnHelper::as_raw_column<ConstColumn>(v1)->data_column();
            ColumnPtr data_column = FN::template evaluate<Type, ResultType, Args...>(eva1, std::forward<Args>(args)...);

            return ConstColumn::create(data_column, v1->size());
        } else {
            return FN::template evaluate<Type, ResultType, Args...>(v1, std::forward<Args>(args)...);
        }
    }
};

/**
 * First: check nullable column
 * Then: execute FN function
 * @param FN the actual function
 */
template <typename FN>
class DealNullableColumnUnaryFunction {
public:
    template <LogicalType Type, LogicalType ResultType, typename... Args>
    static ColumnPtr evaluate(const ColumnPtr& v1, Args&&... args) {
        if (v1->only_null()) {
            return v1;
        }

        if (v1->is_nullable()) {
            auto col = ColumnHelper::as_raw_column<NullableColumn>(v1);

            if (v1->size() == ColumnHelper::count_nulls(v1)) {
                auto data = RunTimeColumnType<ResultType>::create();
                data->resize(v1->size());
                auto nul = NullColumn::create();
                nul->append(*col->null_column(), 0, col->null_column()->size());
                return NullableColumn::create(data, std::move(nul));
            }

            ColumnPtr result =
                    FN::template evaluate<Type, ResultType, Args...>(col->data_column(), std::forward<Args>(args)...);
            if (result->is_nullable()) {
                // when result column is NullableColumn, null columns in src and dst columns
                // must be merged to produce finally result.
                if (result->is_constant()) {
                    // case 1: the result rows are nulls, return the original const null result
                    // without modified.
                    return result;
                }

                auto nullable_data = down_cast<NullableColumn*>(result.get());
                if (result->has_null()) {
                    // case 2: the result rows are partially nulls, must merge null columns
                    // both inside the input column and inside the results.
                    auto finally_null_column =
                            FunctionHelper::union_null_column(col->null_column(), nullable_data->null_column());
                    return NullableColumn::create(nullable_data->data_column(), finally_null_column);

                } else {
                    // case 3: the result rows are all non-nulls, the data of null column should
                    // keep same as before
                    auto nul = NullColumn::create();
                    nul->append(*col->null_column(), 0, col->null_column()->size());
                    return NullableColumn::create(nullable_data->data_column(), std::move(nul));
                }
            } else {
                // the result of data column is not NullableColumn
                auto nul = NullColumn::create();
                nul->append(*col->null_column(), 0, col->null_column()->size());
                return NullableColumn::create(result, std::move(nul));
            }
        } else {
            return FN::template evaluate<Type, ResultType, Args...>(v1, std::forward<Args>(args)...);
        }
    }

    template <LogicalType Type, typename... Args>
    static inline ColumnPtr evaluate(const ColumnPtr& v1, Args&&... args) {
        return evaluate<Type, Type, Args...>(v1, std::forward<Args>(args)...);
    }
};

/**
 * Use for strict unary operations function, usually the result
 * contains (nullable column, data column) , like:
 *
 * Cast operations: cast INT to BIGINT, etc...
 * Bitwise operations: ~
 *
 * @param OP: the operation impl, like NullMerge
 */
template <typename OP>
using VectorizedStrictUnaryFunction =
        DealNullableColumnUnaryFunction<UnpackConstColumnUnaryFunction<UnaryFunction<OP>>>;

/**
 * Use for strict unary operations function, and special for slice!
 * You should use it if your function result type is TYPE_VARCHAR, TYPE_CHAR.
 *
 * Support functions like:
 *  cast_xxx_to_string
 *  upper
 *  ...etc
 *
 * @param OP: the operation impl, like NullMerge
 */
template <typename OP>
using VectorizedStringStrictUnaryFunction =
        DealNullableColumnUnaryFunction<UnpackConstColumnUnaryFunction<StringUnaryFunction<OP>>>;
template <typename VectorizedOp>
using VectorizedUnaryFunction = DealNullableColumnUnaryFunction<UnpackConstColumnUnaryFunction<VectorizedOp>>;

template <typename OP, typename NULL_OP>
using VectorizedInputCheckUnaryFunction = DealNullableColumnUnaryFunction<
        UnpackConstColumnUnaryFunction<ProduceNullUnaryFunction<OP, NULL_OP, NopCheck>>>;

template <typename OP, typename NULL_OP>
using VectorizedOutputCheckUnaryFunction = DealNullableColumnUnaryFunction<
        UnpackConstColumnUnaryFunction<ProduceNullUnaryFunction<OP, NopCheck, NULL_OP>>>;

/**
 * Define a unary function use FN(),
 * FN's signature must be a `ResultType FN(Type a)`
 */
#define DEFINE_UNARY_FN(NAME, FN)                       \
    struct NAME {                                       \
        template <typename Type, typename ResultType>   \
        static inline ResultType apply(const Type& l) { \
            return FN(l);                               \
        }                                               \
    };

/**
 * Define a unary function use FN() with args cast,
 * FN's signature must be a `ResultType FN(Type a)`
 * For abs(ResultType(integer)) function.
 */
#define DEFINE_UNARY_FN_CAST(NAME, FN)                  \
    struct NAME {                                       \
        template <typename Type, typename ResultType>   \
        static inline ResultType apply(const Type& l) { \
            return FN(ResultType(l));                   \
        }                                               \
    };

/**
 *
 * Define a unary function and must implement by yourself
 *
 */
#define DEFINE_UNARY_FN_WITH_IMPL(NAME, VALUE)             \
    struct NAME {                                          \
        template <typename Type, typename ResultType>      \
        static inline ResultType apply(const Type& VALUE); \
    };                                                     \
                                                           \
    template <typename Type, typename ResultType>          \
    ResultType NAME::apply(const Type& VALUE)

/**
 *
 * Define a unary function and must implement by yourself
 * Special for return value is String
 */
#define DEFINE_STRING_UNARY_FN_WITH_IMPL(NAME, VALUE)       \
    struct NAME {                                           \
        template <typename Type, typename ResultType>       \
        static inline std::string apply(const Type& VALUE); \
    };                                                      \
                                                            \
    template <typename Type, typename ResultType>           \
    std::string NAME::apply(const Type& VALUE)

} // namespace starrocks
