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

#include <cstdint>

#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "exprs/function_helper.h"
#include "simd/simd.h"
#include "typeinfo"

namespace starrocks {

class ResultNopCheck {
    template <typename Type, typename ResultType>
    static inline ResultType apply(const Type& VALUE) {
        return false;
    }
};

/**
 * Execute vector vector operator function
 * @param OP: the operations impl, like NullUnion
 */

template <typename OP>
class BaseBinaryFunction {
public:
    template <LogicalType LType, LogicalType RType, LogicalType ResultType>
    static ColumnPtr vector_vector(const ColumnPtr& v1, const ColumnPtr& v2) {
        using LCppType = RunTimeCppType<LType>;
        using RCppType = RunTimeCppType<RType>;
        using ResultCppType = RunTimeCppType<ResultType>;
        using ResultColumnType = RunTimeColumnType<ResultType>;

        const int s = std::min(v1->size(), v2->size());

        auto result = ResultColumnType::create();
        result->resize_uninitialized(v1->size());
        auto* data3 = result->get_data().data();

        if constexpr (lt_is_string<LType> || lt_is_binary<LType>) {
            auto& r1 = ColumnHelper::cast_to_raw<LType>(v1)->get_proxy_data();
            auto& r2 = ColumnHelper::cast_to_raw<RType>(v2)->get_proxy_data();
            for (int i = 0; i < s; ++i) {
                data3[i] = OP::template apply<LCppType, RCppType, ResultCppType>(r1[i], r2[i]);
            }
        } else {
            auto* data1 = ColumnHelper::cast_to_raw<LType>(v1)->get_data().data();
            auto* data2 = ColumnHelper::cast_to_raw<RType>(v2)->get_data().data();
            for (int i = 0; i < s; ++i) {
                data3[i] = OP::template apply<LCppType, RCppType, ResultCppType>(data1[i], data2[i]);
            }
        }

        return result;
    }

    template <LogicalType LType, LogicalType RType, LogicalType ResultType>
    static ColumnPtr const_vector(const ColumnPtr& v1, const ColumnPtr& v2) {
        using LCppType = RunTimeCppType<LType>;
        using RCppType = RunTimeCppType<RType>;
        using ResultCppType = RunTimeCppType<ResultType>;
        using ResultColumnType = RunTimeColumnType<ResultType>;

        int size = v2->size();
        auto result = ResultColumnType::create();
        result->resize_uninitialized(size);
        auto* data3 = result->get_data().data();

        if constexpr (lt_is_string<LType> || lt_is_binary<LType>) {
            auto data1 = ColumnHelper::cast_to_raw<LType>(v1)->get_proxy_data()[0];
            auto& r2 = ColumnHelper::cast_to_raw<RType>(v2)->get_proxy_data();
            for (int i = 0; i < size; ++i) {
                data3[i] = OP::template apply<LCppType, RCppType, ResultCppType>(data1, r2[i]);
            }
        } else {
            auto data1 = ColumnHelper::cast_to_raw<LType>(v1)->get_data()[0];
            auto* data2 = ColumnHelper::cast_to_raw<RType>(v2)->get_data().data();
            for (int i = 0; i < size; ++i) {
                data3[i] = OP::template apply<LCppType, RCppType, ResultCppType>(data1, data2[i]);
            }
        }

        return result;
    }

    template <LogicalType LType, LogicalType RType, LogicalType ResultType>
    static ColumnPtr vector_const(const ColumnPtr& v1, const ColumnPtr& v2) {
        using LCppType = RunTimeCppType<LType>;
        using RCppType = RunTimeCppType<RType>;
        using ResultCppType = RunTimeCppType<ResultType>;
        using ResultColumnType = RunTimeColumnType<ResultType>;

        int size = v1->size();
        auto result = ResultColumnType::create();
        result->resize_uninitialized(size);
        auto& r3 = result->get_data();
        auto* data3 = r3.data();

        if constexpr (lt_is_string<LType> || lt_is_binary<LType>) {
            auto& r1 = ColumnHelper::cast_to_raw<LType>(v1)->get_proxy_data();
            auto data2 = ColumnHelper::cast_to_raw<RType>(v2)->get_proxy_data()[0];
            for (int i = 0; i < size; ++i) {
                data3[i] = OP::template apply<LCppType, RCppType, ResultCppType>(r1[i], data2);
            }
        } else {
            auto* data1 = ColumnHelper::cast_to_raw<LType>(v1)->get_data().data();
            auto data2 = ColumnHelper::cast_to_raw<RType>(v2)->get_data()[0];
            for (int i = 0; i < size; ++i) {
                data3[i] = OP::template apply<LCppType, RCppType, ResultCppType>(data1[i], data2);
            }
        }

        return result;
    }

    template <LogicalType LType, LogicalType RType, LogicalType ResultType>
    static ColumnPtr const_const(const ColumnPtr& v1, const ColumnPtr& v2) {
        using LCppType = RunTimeCppType<LType>;
        using RCppType = RunTimeCppType<RType>;
        using ResultCppType = RunTimeCppType<ResultType>;
        using ResultColumnType = RunTimeColumnType<ResultType>;

        auto result = ResultColumnType::create();
        result->resize_uninitialized(1);
        auto& r3 = result->get_data();

        if constexpr (lt_is_string<LType> || lt_is_binary<LType>) {
            auto& r1 = ColumnHelper::cast_to_raw<LType>(v1)->get_proxy_data();
            auto& r2 = ColumnHelper::cast_to_raw<RType>(v2)->get_proxy_data();
            r3[0] = OP::template apply<LCppType, RCppType, ResultCppType>(r1[0], r2[0]);
        } else {
            auto& r1 = ColumnHelper::cast_to_raw<LType>(v1)->get_data();
            auto& r2 = ColumnHelper::cast_to_raw<RType>(v2)->get_data();
            r3[0] = OP::template apply<LCppType, RCppType, ResultCppType>(r1[0], r2[0]);
        }

        return result;
    }
};

/**
 * First: open const column
 * Then: execute actual operations
 * @param OP
 */
template <typename OP>
class UnpackConstColumnBinaryFunction {
public:
    template <LogicalType LType, LogicalType RType, LogicalType ResultType>
    static ColumnPtr evaluate(const ColumnPtr& v1, const ColumnPtr& v2) {
        if (!v1->is_constant() && !v2->is_constant()) {
            return BaseBinaryFunction<OP>::template vector_vector<LType, RType, ResultType>(v1, v2);

        } else if (!v1->is_constant() && v2->is_constant()) {
            const ColumnPtr& data2 = ColumnHelper::as_raw_column<ConstColumn>(v2)->data_column();

            return BaseBinaryFunction<OP>::template vector_const<LType, RType, ResultType>(v1, data2);
        } else if (v1->is_constant() && !v2->is_constant()) {
            const ColumnPtr& data1 = ColumnHelper::as_raw_column<ConstColumn>(v1)->data_column();

            return BaseBinaryFunction<OP>::template const_vector<LType, RType, ResultType>(data1, v2);
        } else {
            const ColumnPtr& data1 = ColumnHelper::as_raw_column<ConstColumn>(v1)->data_column();
            const ColumnPtr& data2 = ColumnHelper::as_raw_column<ConstColumn>(v2)->data_column();

            auto result = BaseBinaryFunction<OP>::template const_const<LType, RType, ResultType>(data1, data2);

            return ConstColumn::create(result, v1->size());
        }
    }
};

/**
 * First: union nullable column
 * Then: execute FN function
 *
 * The function use for union nullable column, result is NullableColumn or actual data column,
 * like function:
 *      ColumnPtr add(NullableColumn<NullColumn, Int32Column> a,
 *                    NullableColumn<NullColumn, Int32Column> b)
 *
 * The result will be NullableColumn<NullColumn, Int32Column>, we should union a.NullColumn and
 * b.NullColumn, and compute a.Int32Column + b.Int32Column.
 *
 * @param FN the actual function
 */
template <typename FN>
class UnionNullableColumnBinaryFunction {
public:
    template <LogicalType LType, LogicalType RType, LogicalType ResultType>
    static ColumnPtr evaluate(const ColumnPtr& v1, const ColumnPtr& v2) {
        if (v1->only_null() || v2->only_null()) {
            return ColumnHelper::create_const_null_column(v1->size());
        }

        const ColumnPtr& data1 = FunctionHelper::get_data_column_of_nullable(v1);
        const ColumnPtr& data2 = FunctionHelper::get_data_column_of_nullable(v2);

        ColumnPtr data_result = FN::template evaluate<LType, RType, ResultType>(data1, data2);
        if (v1->has_null() || v2->has_null()) {
            // two NOT-nullable column can generate a {nullable, not-nullable, const, const-null} column
            // because when decimal overflow checking is enabled, overflow results will yield NULL.
            NullColumnPtr null_flags = FunctionHelper::union_nullable_column(v1, v2);
            return FunctionHelper::merge_column_and_null_column(std::move(data_result), std::move(null_flags));
        } else {
            return data_result;
        }
    }

    template <LogicalType Type>
    static inline ColumnPtr evaluate(const ColumnPtr& v1, const ColumnPtr& v2) {
        return evaluate<Type, Type, Type>(v1, v2);
    }

    template <LogicalType Type, LogicalType ResultType>
    static inline ColumnPtr evaluate(const ColumnPtr& v1, const ColumnPtr& v2) {
        return evaluate<Type, Type, ResultType>(v1, v2);
    }
};

template <typename FN, typename NULL_OP = ResultNopCheck>
class CheckOutputBinaryFunction {
public:
    template <LogicalType LType, LogicalType RType, LogicalType ResultType>
    static ColumnPtr evaluate(const ColumnPtr& v1, const ColumnPtr& v2) {
        ColumnPtr data_result = FN::template evaluate<LType, RType, ResultType>(v1, v2);

        if constexpr (!std::is_same<NULL_OP, ResultNopCheck>::value) {
            ColumnPtr data;
            if (data_result->is_constant()) {
                if (data_result->only_null()) {
                    return data_result;
                }
                data = ColumnHelper::as_column<ConstColumn>(data_result)->data_column();
            } else {
                data = data_result;
            }

            NullColumnPtr null_flags;
            if (data->is_nullable()) {
                null_flags = ColumnHelper::as_raw_column<NullableColumn>(data)->null_column();
            } else {
                null_flags = RunTimeColumnType<TYPE_NULL>::create();
                null_flags->resize(data->size());
            }
            const auto& real_data =
                    ColumnHelper::cast_to_raw<ResultType>(FunctionHelper::get_data_column_of_nullable(data).get());

            // Avoid calling virtual fuctions `size` in for loop
            const auto size = data->size();

            for (size_t i = 0; i < size; ++i) {
                // DO NOT overwrite null flag if it is already set
                null_flags->get_data()[i] |=
                        NULL_OP::template apply<RunTimeCppType<ResultType>, RunTimeCppType<TYPE_BOOLEAN>>(
                                real_data->get_data()[i]);
            }

            if (data->is_nullable()) {
                // null flag may be changed, so update here manually
                ColumnHelper::as_raw_column<NullableColumn>(data)->update_has_null();
            } else {
                if (SIMD::count_nonzero(null_flags->get_data())) {
                    auto null_result = NullableColumn::create(data, null_flags);
                    if (data_result->is_constant()) {
                        return ConstColumn::create(null_result, data_result->size());
                    }
                    return null_result;
                }
            }
        }

        return data_result;
    }
};

/**
 * First: execute PRODUCE_NULL_OP function
 * Then: union nullable column and the PRODUCE_NULL_OP's result column
 * Last: execute OP function
 *
 * @param PRODUCE_NULL_FN produce null function
 * @param FN the actual operation
 */
template <typename PRODUCE_NULL_FN, typename FN>
class ProduceNullableColumnBinaryFunction {
public:
    template <LogicalType LType, LogicalType RType, LogicalType ResultType>
    static ColumnPtr evaluate(const ColumnPtr& v1, const ColumnPtr& v2) {
        if (v1->only_null() || v2->only_null()) {
            return ColumnHelper::create_const_null_column(v1->size());
        }

        // UnpackConstColumnBinaryFunction will return const column if v1 and v2 all const column,
        // should avoid the case.
        if (v1->is_constant() && v2->is_constant()) {
            const ColumnPtr& data2 = ColumnHelper::as_column<ConstColumn>(v2)->data_column();

            auto null_result = NullColumn::static_pointer_cast(
                    PRODUCE_NULL_FN::template evaluate<LType, RType, TYPE_NULL>(v1, data2));

            // is null, return only null
            if (1 == null_result->get_data()[0]) {
                return ColumnHelper::create_const_null_column(v1->size());
            } else {
                // not null return const column
                return FN::template evaluate<LType, RType, ResultType>(v1, v2);
            }
        }

        if (!v1->is_nullable() && !v2->is_nullable()) {
            NullColumnPtr null_result = NullColumn::static_pointer_cast(
                    PRODUCE_NULL_FN::template evaluate<LType, RType, TYPE_NULL>(v1, v2));

            ColumnPtr data_result = FN::template evaluate<LType, RType, ResultType>(v1, v2);
            // for decimal arithmetics, non-nullable lhs op non-nullable rhs maybe produce a nullable result if
            // result overflows. so we must merge null_result generated by PRODUCE_NULL_FN and null_column of the
            // data_result.
            if constexpr (lt_is_decimal<ResultType>) {
                return FunctionHelper::merge_column_and_null_column(std::move(data_result), std::move(null_result));
            } else {
                return NullableColumn::create(std::move(data_result), std::move(null_result));
            }
        }

        const ColumnPtr& data1 = FunctionHelper::get_data_column_of_nullable(v1);
        const ColumnPtr& data2 = FunctionHelper::get_data_column_of_nullable(v2);

        ColumnPtr produce_null = PRODUCE_NULL_FN::template evaluate<LType, RType, TYPE_NULL>(data1, data2);

        NullColumnPtr null_result = ColumnHelper::as_column<NullColumn>(produce_null);
        FunctionHelper::union_produce_nullable_column(v1, v2, &null_result);

        ColumnPtr data_result = FN::template evaluate<LType, RType, ResultType>(data1, data2);
        // for decimal arithmetics, data_column of nullable lhs op data_column of nullable rhs maybe produce a nullable
        // result if result overflows. so we must merge null_column of lhs, null_column of rhs, produce_null generated
        // by PRODUCE_NULL_FN and null_column of result into one NullColumn.
        if constexpr (lt_is_decimal<ResultType>) {
            return FunctionHelper::merge_column_and_null_column(std::move(data_result), std::move(null_result));
        } else {
            return NullableColumn::create(std::move(data_result), std::move(null_result));
        }
    }

    template <LogicalType Type>
    static inline ColumnPtr evaluate(const ColumnPtr& v1, const ColumnPtr& v2) {
        return evaluate<Type, Type, Type>(v1, v2);
    }
};

template <typename FN>
class UnpackNotAlignDataAndNullColumnBinaryFunction {
public:
    template <LogicalType LType, LogicalType RType, LogicalType ResultType>
    static ColumnPtr evaluate(const ColumnPtr& v1, const NullColumnPtr& n1, const ColumnPtr& v2,
                              const NullColumnPtr& n2) {
        if (v1->size() == v2->size()) {
            return FN::template vector_vector<LType, RType, ResultType>(v1, n1, v2, n2);
        } else if (v2->size() == 1) {
            return FN::template vector_const<LType, RType, ResultType>(v1, n1, v2, n2);
        } else if (v1->size() == 1) {
            return FN::template const_vector<LType, RType, ResultType>(v1, n1, v2, n2);
        } else {
            DCHECK(v1->size() == v2->size());
            LOG(WARNING) << "It's impossible column1 size: " << v1->size() << ", column2 size: " << v2->size();
            return FN::template vector_vector<LType, RType, ResultType>(v1, n1, v2, n2);
        }
    }
};

/**
 * Special for logic predicate
 * @tparam NULL_FN Logic null handle function, will input values and nulls
 * @tparam FN Data handle function
 */
template <typename NULL_FN, typename FN>
class LogicPredicateBaseBinaryFunction {
public:
    template <LogicalType LType, LogicalType RType, LogicalType ResultType>
    static ColumnPtr vector_vector(const ColumnPtr& lv, const NullColumnPtr& ln, const ColumnPtr& rv,
                                   const NullColumnPtr& rn) {
        auto* lvd = ColumnHelper::cast_to_raw<LType>(lv)->get_data().data();
        auto* rvd = ColumnHelper::cast_to_raw<RType>(rv)->get_data().data();

        auto* lnd = ln->get_data().data();
        auto* rnd = rn->get_data().data();

        int size = std::min(lv->size(), rv->size());

        auto null_column = NullColumn::create();
        null_column->resize_uninitialized(size);

        auto* nd = null_column->get_data().data();

        for (int i = 0; i < size; ++i) {
            nd[i] = NULL_FN::template apply<RunTimeCppType<LType>, RunTimeCppType<RType>, RunTimeCppType<ResultType>>(
                    lvd[i], lnd[i], rvd[i], rnd[i]);
        }

        auto data_column = RunTimeColumnType<ResultType>::create();
        data_column->resize_uninitialized(size);
        auto* dd = data_column->get_data().data();

        for (int i = 0; i < size; ++i) {
            dd[i] = FN::template apply<RunTimeCppType<LType>, RunTimeCppType<RType>, RunTimeCppType<ResultType>>(
                    lvd[i], rvd[i]);
        }

        return NullableColumn::create(std::move(data_column), std::move(null_column));
    }

    template <LogicalType LType, LogicalType RType, LogicalType ResultType>
    static ColumnPtr const_vector(const ColumnPtr& lv, const NullColumnPtr& ln, const ColumnPtr& rv,
                                  const NullColumnPtr& rn) {
        auto* lvd = ColumnHelper::cast_to_raw<LType>(lv)->get_data().data();
        auto* rvd = ColumnHelper::cast_to_raw<RType>(rv)->get_data().data();

        auto* lnd = ln->get_data().data();
        auto* rnd = rn->get_data().data();

        int size = rv->size();

        auto null_column = NullColumn::create();
        null_column->resize_uninitialized(size);

        auto* nd = null_column->get_data().data();

        for (int i = 0; i < size; ++i) {
            nd[i] = NULL_FN::template apply<RunTimeCppType<LType>, RunTimeCppType<RType>, RunTimeCppType<ResultType>>(
                    lvd[0], lnd[0], rvd[i], rnd[i]);
        }

        auto data_column = RunTimeColumnType<ResultType>::create();
        data_column->resize_uninitialized(size);
        auto* dd = data_column->get_data().data();

        for (int i = 0; i < size; ++i) {
            dd[i] = FN::template apply<RunTimeCppType<LType>, RunTimeCppType<RType>, RunTimeCppType<ResultType>>(
                    lvd[0], rvd[i]);
        }

        return NullableColumn::create(std::move(data_column), std::move(null_column));
    }

    template <LogicalType LType, LogicalType RType, LogicalType ResultType>
    static ColumnPtr vector_const(const ColumnPtr& lv, const NullColumnPtr& ln, const ColumnPtr& rv,
                                  const NullColumnPtr& rn) {
        auto* lvd = ColumnHelper::cast_to_raw<LType>(lv)->get_data().data();
        auto* rvd = ColumnHelper::cast_to_raw<RType>(rv)->get_data().data();

        auto* lnd = ln->get_data().data();
        auto* rnd = rn->get_data().data();

        int size = lv->size();

        auto null_column = NullColumn::create();
        null_column->resize_uninitialized(size);

        auto* nd = null_column->get_data().data();

        for (int i = 0; i < size; ++i) {
            nd[i] = NULL_FN::template apply<RunTimeCppType<LType>, RunTimeCppType<RType>, RunTimeCppType<ResultType>>(
                    lvd[i], lnd[i], rvd[0], rnd[0]);
        }

        auto data_column = RunTimeColumnType<ResultType>::create();
        data_column->resize_uninitialized(size);
        auto* dd = data_column->get_data().data();

        for (int i = 0; i < size; ++i) {
            dd[i] = FN::template apply<RunTimeCppType<LType>, RunTimeCppType<RType>, RunTimeCppType<ResultType>>(
                    lvd[i], rvd[0]);
        }

        return NullableColumn::create(std::move(data_column), std::move(null_column));
    }
};

/**
 * Special implement for NULL handler
 *
 * @tparam CONST_FN for resolve DATA_COLUMN OP DATA_COLUMN
 * @tparam NULLABLE_FN for resolve NULL_COLUMN OP OTHER_COLUMN
 */
template <typename CONST_FN, typename NULLABLE_FN>
class LogicPredicateBinaryFunction {
public:
    template <LogicalType LType, LogicalType RType, LogicalType ResultType>
    static ColumnPtr evaluate(const ColumnPtr& v1, const ColumnPtr& v2) {
        // const/regular
        if (!v1->is_nullable() && !v2->is_nullable()) {
            return CONST_FN::template evaluate<LType, RType, ResultType>(v1, v2);
        }

        ColumnPtr ld;
        ColumnPtr rd;

        NullColumnPtr ln;
        NullColumnPtr rn;

        if (v1->only_null()) {
            auto p = ColumnHelper::as_raw_column<NullableColumn>(
                    ColumnHelper::as_raw_column<ConstColumn>(v1)->data_column());
            ld = RunTimeColumnType<LType>::create();
            ld->append_default();
            ln = p->null_column();
        } else if (v1->is_constant()) {
            ld = ColumnHelper::as_raw_column<ConstColumn>(v1)->data_column();
            ln = NullColumn::create(v1->size(), 0);
        } else if (v1->is_nullable()) {
            auto p = ColumnHelper::as_raw_column<NullableColumn>(v1);
            ld = p->data_column();
            ln = p->null_column();
        } else {
            ld = v1;
            ln = NullColumn::create(v1->size(), 0);
        }

        if (v2->only_null()) {
            auto p = ColumnHelper::as_raw_column<NullableColumn>(
                    ColumnHelper::as_raw_column<ConstColumn>(v2)->data_column());
            rd = RunTimeColumnType<LType>::create();
            rd->append_default();
            rn = p->null_column();
        } else if (v2->is_constant()) {
            rd = ColumnHelper::as_raw_column<ConstColumn>(v2)->data_column();
            rn = NullColumn::create(v2->size(), 0);
        } else if (v2->is_nullable()) {
            auto p = ColumnHelper::as_raw_column<NullableColumn>(v2);
            rd = p->data_column();
            rn = p->null_column();
        } else {
            rd = v2;
            rn = NullColumn::create(v2->size(), 0);
        }

        // return must be nullable column
        auto p = NULLABLE_FN::template evaluate<LType, RType, ResultType>(ld, ln, rd, rn);

        // INPUT IS CONSTANT, RETURN CONSTANT
        if (v1->is_constant() && v2->is_constant()) {
            if (p->is_null(0)) {
                return ConstColumn::create(std::move(p), v1->size());
            } else {
                auto np = ColumnHelper::as_raw_column<NullableColumn>(p);
                return ConstColumn::create(std::move(np->data_column()), v1->size());
            }
        } else {
            return p;
        }
    }

    template <LogicalType Type>
    static inline ColumnPtr evaluate(const ColumnPtr& v1, const ColumnPtr& v2) {
        return evaluate<Type, Type, Type>(v1, v2);
    }
};

/**
 * Use for strict binary operations function, usually the result
 * contains (nullable column, data column) , like:
 *
 * Arithmetic operations: +, -, *
 * Bitwise operations: |, &, ^
 * Compare operations: eq, ne, le, lt, ge, gt
 *
 * Can't resolve operations which will produce new nullable column, like '/', '%'
 *
 * @param OP: the operation impl, like NullUnion
 */
template <typename OP>
using VectorizedStrictBinaryFunction = UnionNullableColumnBinaryFunction<UnpackConstColumnBinaryFunction<OP>>;

template <typename OP, typename NULL_OP>
using VectorizedOuputCheckBinaryFunction =
        CheckOutputBinaryFunction<UnionNullableColumnBinaryFunction<UnpackConstColumnBinaryFunction<OP>>, NULL_OP>;

/**
* Use for unstrict binary operations function, usually the result
* contains (nullable column, data column), special for operations which will produce null
*
* Arithmetic operations: /, %
*
* @param PRODUCE_NULL_OP: the produce null operation
* @param OP: the operation impl, like NullUnion
*/
template <typename PRODUCE_NULL_OP, typename OP>
using VectorizedUnstrictBinaryFunction =
        ProduceNullableColumnBinaryFunction<UnpackConstColumnBinaryFunction<PRODUCE_NULL_OP>,
                                            UnpackConstColumnBinaryFunction<OP>>;

/**
 * Use for logic binary operations function, usually the result
 * contains (nullable column, data column), special for operations which will produce null
 *
 * Login operations: and, or
 *
 * @param PRODUCE_NULL_OP: the produce null operation
 * @param OP: the operation impl, like NullUnion
 */
template <typename NULL_OP, typename OP>
using VectorizedLogicPredicateBinaryFunction = LogicPredicateBinaryFunction<
        UnpackConstColumnBinaryFunction<OP>,
        UnpackNotAlignDataAndNullColumnBinaryFunction<LogicPredicateBaseBinaryFunction<NULL_OP, OP>>>;

/**
 * Define a binary function use FN(),
 * FN's signature must be a `ResultType FN(LType l, RType r)`
 */
#define DEFINE_BINARY_FUNCTION(NAME, FN)                                 \
    struct NAME {                                                        \
        template <typename LType, typename RType, typename ResultType>   \
        static inline ResultType apply(const LType& l, const RType& r) { \
            return FN(l, r);                                             \
        }                                                                \
    };

/**
* Define a binary function and must be implement by yourself.
*/
#define DEFINE_BINARY_FUNCTION_WITH_IMPL(NAME, L_VALUE, R_VALUE)        \
    struct NAME {                                                       \
        template <typename LType, typename RType, typename ResultType>  \
        static inline ResultType apply(const LType& l, const RType& r); \
    };                                                                  \
                                                                        \
    template <typename LType, typename RType, typename ResultType>      \
    ResultType NAME::apply(const LType& L_VALUE, const RType& R_VALUE)

/**
* Define a logic null handler binary function and must be implement by yourself.
*/
#define DEFINE_LOGIC_NULL_BINARY_FUNCTION_WITH_IMPL(NAME, L_VALUE, L_NULL, R_VALUE, R_NULL)           \
    struct NAME {                                                                                     \
        template <typename LType, typename RType, typename ResultType>                                \
        static inline uint8_t apply(const LType& l_value, const uint8_t l_null, const RType& r_value, \
                                    const uint8_t r_null);                                            \
    };                                                                                                \
                                                                                                      \
    template <typename LType, typename RType, typename ResultType>                                    \
    uint8_t NAME::apply(const LType& L_VALUE, const uint8_t L_NULL, const RType& R_VALUE, const uint8_t R_NULL)

} // namespace starrocks
