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
#include <memory>

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_hash.h"
#include "column/column_viewer.h"
#include "column/json_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exec/sorting/sorting.h"
#include "exprs/arithmetic_operation.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "util/bit_mask.h"
#include "util/orlp/pdqsort.h"
#include "util/phmap/phmap.h"

namespace starrocks {
template <LogicalType LT>
class ArrayDistinct {
public:
    using CppType = RunTimeCppType<LT>;

    static ColumnPtr process(FunctionContext* ctx, const Columns& columns) {
        static_assert(lt_is_largeint<LT> || lt_is_fixedlength<LT> || lt_is_string<LT>);
        RETURN_IF_COLUMNS_ONLY_NULL(columns);

        return _array_distinct<phmap::flat_hash_set<CppType, PhmapDefaultHashFunc<LT, PhmapSeed1>>>(columns);
    }

private:
    template <typename HashSet>
    static ColumnPtr _array_distinct(const Columns& columns) {
        DCHECK_EQ(columns.size(), 1);

        size_t chunk_size = columns[0]->size();
        ColumnPtr src_column = ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[0]);

        ColumnPtr dest_column = src_column->clone_empty();

        HashSet hash_set;

        if (columns[0]->is_nullable()) {
            const auto* src_nullable_column = down_cast<const NullableColumn*>(src_column.get());
            const auto* src_data_column = down_cast<const ArrayColumn*>(src_nullable_column->data_column().get());
            auto& dest_nullable_column = down_cast<NullableColumn&>(*dest_column);
            auto& dest_null_data = down_cast<NullableColumn&>(*dest_column).null_column_data();
            auto& dest_data_column = down_cast<ArrayColumn&>(*dest_nullable_column.data_column());

            dest_null_data = src_nullable_column->immutable_null_column_data();
            dest_nullable_column.set_has_null(src_nullable_column->has_null());

            if (src_nullable_column->has_null()) {
                for (size_t i = 0; i < chunk_size; i++) {
                    if (!src_nullable_column->is_null(i)) {
                        _array_distinct_item<HashSet>(*src_data_column, i, &hash_set, &dest_data_column);
                        hash_set.clear();
                    } else {
                        dest_data_column.append_default();
                    }
                }
            } else {
                for (size_t i = 0; i < chunk_size; i++) {
                    _array_distinct_item<HashSet>(*src_data_column, i, &hash_set, &dest_data_column);
                    hash_set.clear();
                }
            }
        } else {
            const auto* src_data_column = down_cast<const ArrayColumn*>(src_column.get());
            auto* dest_data_column = down_cast<ArrayColumn*>(dest_column.get());

            for (size_t i = 0; i < chunk_size; i++) {
                _array_distinct_item<HashSet>(*src_data_column, i, &hash_set, dest_data_column);
                hash_set.clear();
            }
        }
        return dest_column;
    }

    template <typename HashSet>
    static void _array_distinct_item(const ArrayColumn& column, size_t index, HashSet* hash_set,
                                     ArrayColumn* dest_column) {
        bool has_null = false;
        // TODO: may be has performance problem, optimize later
        Datum v = column.get(index);
        const auto& items = v.get<DatumArray>();

        auto& dest_data_column = dest_column->elements_column();
        auto& dest_offsets = dest_column->offsets_column()->get_data();

        for (const auto& item : items) {
            if (item.is_null()) {
                if (!has_null) {
                    dest_data_column->append_nulls(1);
                    has_null = true;
                }
                continue;
            }

            const auto& tt = item.get<CppType>();
            if (hash_set->count(tt) == 0) {
                hash_set->emplace(tt);
                dest_data_column->append_datum(tt);
            }
        }

        dest_offsets.emplace_back(dest_offsets.back() + hash_set->size() + has_null);
    }
};

template <LogicalType LT>
class ArrayDifference {
public:
    using CppType = RunTimeCppType<LT>;

    static ColumnPtr process(FunctionContext* ctx, const Columns& columns) {
        RETURN_IF_COLUMNS_ONLY_NULL(columns);
        if constexpr (lt_is_float<LT>) {
            return _array_difference<TYPE_DOUBLE>(ctx, columns);
        } else if constexpr (lt_is_sum_bigint<LT>) {
            return _array_difference<TYPE_BIGINT>(ctx, columns);
        } else if constexpr (lt_is_largeint<LT>) {
            return _array_difference<TYPE_LARGEINT>(ctx, columns);
        } else if constexpr (lt_is_decimalv2<LT>) {
            return _array_difference<TYPE_DECIMALV2>(ctx, columns);
        } else if constexpr (lt_is_decimal32<LT>) {
            return _array_difference<TYPE_DECIMAL32>(ctx, columns);
        } else if constexpr (lt_is_decimal64<LT>) {
            return _array_difference<TYPE_DECIMAL64>(ctx, columns);
        } else if constexpr (lt_is_decimal128<LT>) {
            return _array_difference<TYPE_DECIMAL128>(ctx, columns);
        } else {
            assert(false);
        }
    }

private:
    template <LogicalType ResultType>
    static ColumnPtr _array_difference(FunctionContext* ctx, const Columns& columns) {
        DCHECK_EQ(columns.size(), 1);

        size_t chunk_size = columns[0]->size();
        ColumnPtr src_column = ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[0]);
        ColumnPtr dest_column_data = nullptr;
        ColumnPtr dest_column = nullptr;

        if constexpr (lt_is_decimal<LT>) {
            // array<decimal<>>
            auto desc = ctx->get_return_type();
            DCHECK(desc.children.size() == 1);
            dest_column_data = NullableColumn::create(
                    RunTimeColumnType<ResultType>::create(desc.children[0].precision, desc.children[0].scale),
                    NullColumn::create());
        } else {
            dest_column_data = NullableColumn::create(RunTimeColumnType<ResultType>::create(), NullColumn::create());
        }

        if (columns[0]->is_nullable()) {
            const auto* src_nullable_column = down_cast<const NullableColumn*>(src_column.get());
            const auto* src_data_column = down_cast<const ArrayColumn*>(src_nullable_column->data_column().get());

            dest_column = NullableColumn::create(
                    ArrayColumn::create(dest_column_data, UInt32Column::create(src_data_column->offsets())),
                    NullColumn::create());

            auto& dest_nullable_column = down_cast<NullableColumn&>(*dest_column);
            auto& dest_null_data = down_cast<NullableColumn&>(*dest_column).null_column_data();
            auto& dest_data_column = down_cast<ArrayColumn&>(*dest_nullable_column.data_column());

            dest_null_data = src_nullable_column->immutable_null_column_data();
            dest_nullable_column.set_has_null(src_nullable_column->has_null());

            if (src_nullable_column->has_null()) {
                for (size_t i = 0; i < chunk_size; i++) {
                    if (!src_nullable_column->is_null(i)) {
                        _array_difference_item<ResultType>(*src_data_column, i, &dest_data_column);
                    }
                }
            } else {
                for (size_t i = 0; i < chunk_size; i++) {
                    _array_difference_item<ResultType>(*src_data_column, i, &dest_data_column);
                }
            }
        } else {
            const auto* src_data_column = down_cast<const ArrayColumn*>(src_column.get());
            dest_column = ArrayColumn::create(dest_column_data, UInt32Column::create(src_data_column->offsets()));

            auto* dest_data_column = down_cast<ArrayColumn*>(dest_column.get());
            for (size_t i = 0; i < chunk_size; i++) {
                _array_difference_item<ResultType>(*src_data_column, i, dest_data_column);
            }
        }
        return dest_column;
    }

    template <LogicalType ResultType>
    static void _array_difference_item(const ArrayColumn& column, size_t index, ArrayColumn* dest_column) {
        Datum v = column.get(index);
        const auto& items = v.get<DatumArray>();
        auto& dest_data_column = dest_column->elements_column();

        RunTimeCppType<ResultType> zero = RunTimeCppType<ResultType>{0};
        RunTimeCppType<ResultType> sub = RunTimeCppType<ResultType>{0};

        int scale = 0;
        if constexpr (lt_is_decimal<LT>) {
            auto* ele = &column.elements();
            if (ele->is_nullable()) {
                scale = down_cast<const RunTimeColumnType<LT>*>(
                                down_cast<const NullableColumn*>(ele)->data_column().get())
                                ->scale();
            } else {
                scale = down_cast<const RunTimeColumnType<LT>*>(ele)->scale();
            }
        }

        for (size_t i = 0; i < items.size(); ++i) {
            if (i == 0) {
                if (items[i].is_null()) {
                    dest_data_column->append_nulls(1);
                } else {
                    dest_data_column->append_datum(zero);
                }
            } else {
                if (items[i - 1].is_null() || items[i].is_null()) {
                    dest_data_column->append_nulls(1);
                } else {
                    if constexpr (!lt_is_decimal<LT>) {
                        sub = items[i].get<CppType>() - items[i - 1].get<CppType>();
                    } else if constexpr (lt_is_decimal<LT>) {
                        RunTimeCppType<ResultType> lhs = RunTimeCppType<ResultType>{items[i].get<CppType>()};
                        RunTimeCppType<ResultType> rhs = RunTimeCppType<ResultType>{items[i - 1].get<CppType>()};
                        sub = decimal_sub<RunTimeCppType<ResultType>>(lhs, rhs, scale);
                    }
                    dest_data_column->append_datum(sub);
                }
            }
        }
    }
};

template <typename HashSet>
struct ArrayOverlapState {
    bool left_is_notnull_const = false;
    bool right_is_notnull_const = false;
    bool has_overlapping = false;
    bool has_null = false;
    std::unique_ptr<HashSet> hash_set;
};

template <LogicalType LT>
class ArrayOverlap {
public:
    using CppType = RunTimeCppType<LT>;
    using ColumnType = RunTimeColumnType<LT>;
    using DataArray = RunTimeProxyContainerType<LT>;
    using HashFunc = PhmapDefaultHashFunc<LT, PhmapSeed1>;
    using HashSet = phmap::flat_hash_set<CppType, HashFunc>;

    static Status prepare(FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
        if (scope != FunctionContext::FRAGMENT_LOCAL) {
            return Status::OK();
        }

        auto* state = new ArrayOverlapState<HashSet>();
        ctx->set_function_state(scope, state);

        if (!ctx->is_notnull_constant_column(0) && !ctx->is_notnull_constant_column(1)) {
            return Status::OK();
        }

        if (ctx->is_notnull_constant_column(1)) {
            const auto* array_column =
                    ColumnHelper::get_data_column_by_type<TYPE_ARRAY>(ctx->get_constant_column(1).get());
            state->right_is_notnull_const = true;
            state->hash_set = std::make_unique<HashSet>();
            state->has_null = _put_array_to_hash_set(*array_column, 0, state->hash_set.get());
        }

        if (ctx->is_notnull_constant_column(0)) {
            const auto* array_column =
                    ColumnHelper::get_data_column_by_type<TYPE_ARRAY>(ctx->get_constant_column(0).get());
            state->left_is_notnull_const = true;

            if (state->right_is_notnull_const) {
                const auto* elements_column = &array_column->elements();

                DCHECK(elements_column->is_nullable());

                state->has_overlapping =
                        _check_column_overlap_nullable(*state->hash_set, *array_column, 0, state->has_null);
            } else {
                state->hash_set = std::make_unique<HashSet>();
                state->has_null = _put_array_to_hash_set(*array_column, 0, state->hash_set.get());
            }
        }

        return Status::OK();
    }

    static Status close(FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            auto* state = reinterpret_cast<ArrayOverlapState<HashSet>*>(
                    ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
            delete state;
        }

        return Status::OK();
    }

    static StatusOr<ColumnPtr> process(FunctionContext* ctx, const Columns& columns) {
        RETURN_IF_COLUMNS_ONLY_NULL(columns);
        static_assert(PhmapDefaultHashFunc<LT, PhmapSeed1>::is_supported());

        auto* state =
                reinterpret_cast<ArrayOverlapState<HashSet>*>(ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (UNLIKELY(state == nullptr)) {
            return Status::InternalError("array_overloap get state failed");
        }

        bool is_nullable = columns[0]->is_nullable() || columns[1]->is_nullable();
        auto chunk_size = columns[0]->size();

        if (state->left_is_notnull_const && state->right_is_notnull_const) {
            ColumnPtr result_column;
            if (state->has_overlapping) {
                result_column = ColumnHelper::create_const_column<TYPE_BOOLEAN>(1, chunk_size);
            } else {
                result_column = ColumnHelper::create_const_column<TYPE_BOOLEAN>(0, chunk_size);
            }
            if (is_nullable) {
                result_column = ColumnHelper::cast_to_nullable_column(result_column);
            }
            return result_column;
        } else if (state->left_is_notnull_const) {
            return _array_overlap_const(*state, *columns[1]);
        } else if (state->right_is_notnull_const) {
            return _array_overlap_const(*state, *columns[0]);
        } else {
            return _array_overlap(columns);
        }
    }

private:
    static ColumnPtr _array_overlap_const(const ArrayOverlapState<HashSet>& state, const Column& column) {
        const size_t num_rows = column.size();
        const auto [is_const, num_packed_rows] = ColumnHelper::num_packed_rows(&column);

        auto result_data_column = BooleanColumn::create(num_packed_rows, 0);
        auto& result_data = result_data_column->get_data();
        NullColumnPtr result_null_column;
        const ArrayColumn* src_data_column = ColumnHelper::get_data_column_by_type<TYPE_ARRAY>(&column);
        const NullColumn* src_null_column = ColumnHelper::get_null_column(&column);

        if (src_null_column != nullptr) {
            result_null_column = ColumnHelper::as_column<UInt8Column>(src_null_column->clone());
        }

        DCHECK(src_data_column->elements_column()->is_nullable());

        for (size_t i = 0; i < num_packed_rows; i++) {
            result_data[i] = _check_column_overlap_nullable(*state.hash_set, *src_data_column, i, state.has_null);
        }

        if (is_const) {
            // Const null column is handled by `RETURN_IF_COLUMNS_ONLY_NULL` in `process`.
            DCHECK(result_null_column == nullptr);
            return ConstColumn::create(std::move(result_data_column), num_rows);
        } else if (result_null_column != nullptr) {
            return NullableColumn::create(std::move(result_data_column), std::move(result_null_column));
        } else {
            return result_data_column;
        }
    }

    static ColumnPtr _array_overlap(const Columns& columns) {
        const size_t num_rows = columns[0]->size();
        const auto [is_all_const, num_packed_rows] = ColumnHelper::num_packed_rows(columns);

        auto result_data_column = BooleanColumn::create(num_packed_rows, 0);
        auto& result_data = result_data_column->get_data();

        const auto* src_data_column_0 = ColumnHelper::get_data_column_by_type<TYPE_ARRAY>(columns[0].get());
        const auto* src_data_column_1 = ColumnHelper::get_data_column_by_type<TYPE_ARRAY>(columns[1].get());

        NullColumnPtr result_null_column = FunctionHelper::union_nullable_column(columns[0], columns[1]);

        DCHECK(src_data_column_0->elements_column()->is_nullable());

        if (!is_all_const && (columns[0]->is_constant() || columns[1]->is_constant())) {
            const auto* const_data_column = columns[0]->is_constant() ? src_data_column_0 : src_data_column_1;
            const auto* non_const_column = columns[0]->is_constant() ? src_data_column_1 : src_data_column_0;

            HashSet hash_set;
            const bool has_null = _put_array_to_hash_set(*const_data_column, 0, &hash_set);
            for (size_t i = 0; i < num_packed_rows; i++) {
                result_data[i] = _check_column_overlap_nullable(hash_set, *non_const_column, i, has_null);
            }
        } else {
            //TODO: use small array to build hash set
            for (size_t i = 0; i < num_packed_rows; i++) {
                HashSet hash_set;
                const bool has_null = _put_array_to_hash_set(*src_data_column_1, i, &hash_set);
                result_data[i] = _check_column_overlap_nullable(hash_set, *src_data_column_0, i, has_null);
            }
        }

        if (is_all_const) {
            // Const null column is handled by `RETURN_IF_COLUMNS_ONLY_NULL` in `process`.
            DCHECK(result_null_column == nullptr);
            return ConstColumn::create(std::move(result_data_column), num_rows);
        } else if (result_null_column != nullptr) {
            return NullableColumn::create(std::move(result_data_column), std::move(result_null_column));
        } else {
            return result_data_column;
        }
    }

    static bool _put_array_to_hash_set(const ArrayColumn& column, size_t index, HashSet* hash_set) {
        const auto* elements_column = column.elements_column().get();
        const auto& offsets = column.offsets().get_data();
        bool has_null = false;
        uint32_t start = offsets[index];
        uint32_t end = offsets[index + 1];

        DCHECK(elements_column->is_nullable());

        const NullableColumn* nullable_column = down_cast<const NullableColumn*>(elements_column);
        const auto& datas = GetContainer<LT>::get_data(nullable_column->data_column());
        const auto& nulls = nullable_column->null_column()->get_data();

        if (nullable_column->has_null()) {
            for (size_t i = start; i < end; i++) {
                if (nulls[i]) {
                    has_null = true;
                } else {
                    hash_set->emplace(datas[i]);
                }
            }
        } else {
            for (size_t i = start; i < end; i++) {
                hash_set->emplace(datas[i]);
            }
        }

        return has_null;
    }

    static bool _check_column_overlap_nullable(const HashSet& hash_set, const ArrayColumn& column, size_t index,
                                               bool has_null) {
        const auto* elements_column = column.elements_column().get();
        const auto& offsets = column.offsets().get_data();
        uint32_t start = offsets[index];
        uint32_t end = offsets[index + 1];
        bool overlap = false;

        DCHECK(elements_column->is_nullable());

        const NullableColumn* nullable_elements_column = down_cast<const NullableColumn*>(elements_column);
        const auto& datas = GetContainer<LT>::get_data(nullable_elements_column->data_column());

        if (nullable_elements_column->has_null()) {
            const auto& nulls = nullable_elements_column->null_column()->get_data();

            overlap = _check_overlap_nullable(hash_set, datas, nulls, start, end, has_null, index);
        } else {
            overlap = _check_overlap(hash_set, datas, start, end, index);
        }

        return overlap;
    }

    static bool _check_overlap(const HashSet& hash_set, const DataArray& data, uint32_t start, uint32_t end,
                               size_t index) {
        for (auto i = start; i < end; i++) {
            if (hash_set.contains(data[i])) {
                return true;
            }
        }
        return false;
    }

    static bool _check_overlap_nullable(const HashSet& hash_set, const DataArray& data, const NullData& null_data,
                                        uint32_t start, uint32_t end, bool has_null, size_t index) {
        for (auto i = start; i < end; i++) {
            if (null_data[i] == 1) {
                if (has_null) {
                    return true;
                }
            } else {
                if (hash_set.contains(data[i])) {
                    return true;
                }
            }
        }
        return false;
    }
};

template <LogicalType LT>
class ArrayIntersect {
public:
    using CppType = RunTimeCppType<LT>;

    class CppTypeWithOverlapTimes {
    public:
        CppTypeWithOverlapTimes(const CppType& item, size_t n = 0) : value(item), overlap_times(n) {}

        CppType value;
        mutable size_t overlap_times;
    };

    template <LogicalType type>
    struct CppTypeWithOverlapTimesHash {
        std::size_t operator()(const CppTypeWithOverlapTimes& cpp_type_value) const {
            return PhmapDefaultHashFunc<LT, PhmapSeed1>()(cpp_type_value.value);
        }
    };

    struct CppTypeWithOverlapTimesEqual {
        bool operator()(const CppTypeWithOverlapTimes& x, const CppTypeWithOverlapTimes& y) const {
            return x.value == y.value;
        }
    };

    static ColumnPtr process(FunctionContext* ctx, const Columns& columns) {
        static_assert(lt_is_largeint<LT> || lt_is_fixedlength<LT> || lt_is_string<LT>);

        return _array_intersect<phmap::flat_hash_set<CppTypeWithOverlapTimes, CppTypeWithOverlapTimesHash<LT>,
                                                     CppTypeWithOverlapTimesEqual>>(columns);
    }

private:
    template <typename HashSet>
    static ColumnPtr _array_intersect(const Columns& original_columns) {
        if (original_columns.size() == 1) {
            return original_columns[0];
        }

        RETURN_IF_COLUMNS_ONLY_NULL(original_columns);

        Columns columns;
        for (const auto& col : original_columns) {
            columns.push_back(ColumnHelper::unpack_and_duplicate_const_column(col->size(), col));
        }

        size_t chunk_size = columns[0]->size();
        bool is_nullable = false;
        bool has_null = false;
        int null_index = 0;
        std::vector<const ArrayColumn*> src_columns;
        src_columns.reserve(columns.size());
        NullColumnPtr null_result = NullColumn::create();
        null_result->resize(chunk_size);

        for (int i = 0; i < columns.size(); ++i) {
            if (columns[i]->is_nullable()) {
                is_nullable = true;
                has_null = (columns[i]->has_null() || has_null);
                null_index = i;

                const auto* src_nullable_column = down_cast<const NullableColumn*>(columns[i].get());
                src_columns.emplace_back(down_cast<const ArrayColumn*>(src_nullable_column->data_column().get()));
                null_result = FunctionHelper::union_null_column(null_result, src_nullable_column->null_column());
            } else {
                src_columns.emplace_back(down_cast<const ArrayColumn*>(columns[i].get()));
            }
        }

        ColumnPtr src_column = ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[null_index]);
        ColumnPtr dest_column = src_column->clone_empty();

        ArrayColumn* dest_data_column = nullptr;
        if (is_nullable) {
            auto& dest_nullable_column = down_cast<NullableColumn&>(*dest_column);
            dest_data_column = down_cast<ArrayColumn*>(dest_nullable_column.data_column().get());
            auto& dest_null_data = dest_nullable_column.null_column_data();

            dest_null_data = null_result->get_data();
            dest_nullable_column.set_has_null(has_null);
        } else {
            dest_data_column = down_cast<ArrayColumn*>(dest_column.get());
        }

        HashSet hash_set;
        for (size_t i = 0; i < chunk_size; i++) {
            _array_intersect_item<HashSet>(src_columns, i, &hash_set, dest_data_column);
            hash_set.clear();
        }

        return dest_column;
    }

    template <typename HashSet>
    static void _array_intersect_item(const std::vector<const ArrayColumn*>& columns, size_t index, HashSet* hash_set,
                                      ArrayColumn* dest_column) {
        bool has_null = false;

        {
            Datum v = columns[0]->get(index);
            const auto& items = v.get<DatumArray>();
            for (const auto& item : items) {
                if (item.is_null()) {
                    has_null = true;
                } else {
                    hash_set->emplace(CppTypeWithOverlapTimes(item.get<CppType>(), 0));
                }
            }
        }

        for (int i = 1; i < columns.size(); ++i) {
            Datum v = columns[i]->get(index);
            const auto& items = v.get<DatumArray>();
            bool local_has_null = false;
            for (const auto& item : items) {
                if (item.is_null()) {
                    local_has_null = true;
                } else {
                    auto iter = hash_set->find(item.get<CppType>());
                    if (iter != hash_set->end()) {
                        if (iter->overlap_times < i) {
                            ++iter->overlap_times;
                        }
                    }
                }
            }

            has_null = (has_null && local_has_null);
        }

        auto& dest_data_column = dest_column->elements_column();
        auto& dest_offsets = dest_column->offsets_column()->get_data();

        auto max_overlap_times = columns.size() - 1;
        size_t result_size = 0;
        for (auto iterator = hash_set->begin(); iterator != hash_set->end(); ++iterator) {
            if (iterator->overlap_times == max_overlap_times) {
                dest_data_column->append_datum(iterator->value);
                ++result_size;
            }
        }

        if (has_null) {
            dest_data_column->append_nulls(1);
        }

        dest_offsets.emplace_back(dest_offsets.back() + result_size + has_null);
    }
};

template <LogicalType LT>
class ArraySort {
public:
    using ColumnType = RunTimeColumnType<LT>;

    static ColumnPtr process(FunctionContext* ctx, const Columns& columns) {
        DCHECK_EQ(columns.size(), 1);
        RETURN_IF_COLUMNS_ONLY_NULL(columns);

        size_t chunk_size = columns[0]->size();

        // TODO: For fixed-length types, you can operate directly on the original column without using sort index,
        //  which will be optimized later
        std::vector<uint32_t> sort_index;
        ColumnPtr src_column = ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[0]);
        ColumnPtr dest_column = src_column->clone_empty();

        if (src_column->is_nullable()) {
            const auto* src_nullable_column = down_cast<const NullableColumn*>(src_column.get());
            const auto& src_data_column = src_nullable_column->data_column_ref();
            const auto& src_null_column = src_nullable_column->null_column_ref();

            auto* dest_nullable_column = down_cast<NullableColumn*>(dest_column.get());
            auto* dest_data_column = dest_nullable_column->mutable_data_column();
            auto* dest_null_column = dest_nullable_column->mutable_null_column();

            if (src_column->has_null()) {
                dest_null_column->get_data().assign(src_null_column.get_data().begin(),
                                                    src_null_column.get_data().end());
            } else {
                dest_null_column->get_data().resize(chunk_size, 0);
            }
            dest_nullable_column->set_has_null(src_nullable_column->has_null());

            _sort_array_column(dest_data_column, &sort_index, src_data_column);
        } else {
            _sort_array_column(dest_column.get(), &sort_index, *src_column);
        }
        return dest_column;
    }

protected:
    static void _sort_column(std::vector<uint32_t>* sort_index, const Column& src_column, size_t offset, size_t count) {
        const auto& data = down_cast<const ColumnType&>(src_column).get_data();

        auto less_fn = [&data](uint32_t l, uint32_t r) -> bool { return data[l] < data[r]; };
        pdqsort(sort_index->begin() + offset, sort_index->begin() + offset + count, less_fn);
    }

    // For JSON type
    static void _sort_column(std::vector<uint32_t>* sort_index, const JsonColumn& src_column, size_t offset,
                             size_t count) {
        auto less_fn = [&](uint32_t l, uint32_t r) -> bool { return src_column.compare_at(l, r, src_column, -1) < 0; };
        pdqsort(sort_index->begin() + offset, sort_index->begin() + offset + count, less_fn);
    }

    static void _sort_item(std::vector<uint32_t>* sort_index, const Column& src_column,
                           const UInt32Column& offset_column, size_t index) {
        const auto& offsets = offset_column.get_data();

        size_t start = offsets[index];
        size_t count = offsets[index + 1] - offsets[index];
        if (count <= 0) {
            return;
        }

        _sort_column(sort_index, down_cast<const RunTimeColumnType<LT>&>(src_column), start, count);
    }

    static void _sort_nullable_item(std::vector<uint32_t>* sort_index, const Column& src_data_column,
                                    const NullColumn& src_null_column, const UInt32Column& offset_column,
                                    size_t index) {
        const auto& offsets = offset_column.get_data();
        size_t start = offsets[index];
        size_t count = offsets[index + 1] - offsets[index];

        if (count <= 0) {
            return;
        }

        auto null_first_fn = [&src_null_column](size_t i) -> bool { return src_null_column.get_data()[i] == 1; };

        auto begin_of_not_null =
                std::partition(sort_index->begin() + start, sort_index->begin() + start + count, null_first_fn);
        size_t data_offset = begin_of_not_null - sort_index->begin();
        size_t null_count = data_offset - start;
        _sort_column(sort_index, down_cast<const RunTimeColumnType<LT>&>(src_data_column), start + null_count,
                     count - null_count);
    }

    static void _sort_array_column(Column* dest_array_column, std::vector<uint32_t>* sort_index,
                                   const Column& src_array_column) {
        const auto& src_elements_column = down_cast<const ArrayColumn&>(src_array_column).elements();
        const auto& offsets_column = down_cast<const ArrayColumn&>(src_array_column).offsets();

        auto* dest_elements_column = down_cast<ArrayColumn*>(dest_array_column)->elements_column().get();
        auto* dest_offsets_column = down_cast<ArrayColumn*>(dest_array_column)->offsets_column().get();
        dest_offsets_column->get_data() = offsets_column.get_data();

        size_t chunk_size = src_array_column.size();
        _init_sort_index(sort_index, src_elements_column.size());

        if (src_elements_column.is_nullable()) {
            if (src_elements_column.has_null()) {
                const auto& src_data_column = down_cast<const NullableColumn&>(src_elements_column).data_column_ref();
                const auto& null_column = down_cast<const NullableColumn&>(src_elements_column).null_column_ref();

                for (size_t i = 0; i < chunk_size; i++) {
                    _sort_nullable_item(sort_index, src_data_column, null_column, offsets_column, i);
                }
            } else {
                const auto& src_data_column = down_cast<const NullableColumn&>(src_elements_column).data_column_ref();

                for (size_t i = 0; i < chunk_size; i++) {
                    _sort_item(sort_index, src_data_column, offsets_column, i);
                }
            }
        } else {
            for (size_t i = 0; i < chunk_size; i++) {
                _sort_item(sort_index, src_elements_column, offsets_column, i);
            }
        }
        dest_elements_column->append_selective(src_elements_column, *sort_index);
    }

    static void _init_sort_index(std::vector<uint32_t>* sort_index, size_t count) {
        sort_index->resize(count);
        for (size_t i = 0; i < count; i++) {
            (*sort_index)[i] = i;
        }
    }
};

template <LogicalType LT>
class ArrayReverse {
public:
    using ColumnType = RunTimeColumnType<LT>;
    using CppType = RunTimeCppType<LT>;

    static ColumnPtr process(FunctionContext* ctx, const Columns& columns) {
        DCHECK_EQ(columns.size(), 1);

        size_t chunk_size = columns[0]->size();

        if (columns[0]->only_null()) {
            return ColumnHelper::create_const_null_column(chunk_size);
        }

        ColumnPtr src_column = ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[0]);
        ColumnPtr dest_column = src_column->clone();

        if (dest_column->is_nullable()) {
            _reverse_array_column(down_cast<NullableColumn*>(dest_column.get())->mutable_data_column(), chunk_size);
        } else {
            _reverse_array_column(dest_column.get(), chunk_size);
        }
        return dest_column;
    }

private:
    static void _reverse_fixed_column(Column* column, const Buffer<uint32_t>& array_offsets, size_t chunk_size) {
        for (size_t i = 0; i < chunk_size; i++) {
            auto& data = down_cast<ColumnType*>(column)->get_data();
            std::reverse(data.begin() + array_offsets[i], data.begin() + array_offsets[i + 1]);
        }
    }

    static void _reverse_binary_column(Column* column, const Buffer<uint32_t>& array_offsets, size_t chunk_size) {
        auto& offsets = down_cast<BinaryColumn*>(column)->get_offset();
        // convert offset ot size
        for (size_t i = offsets.size() - 1; i > 0; i--) {
            offsets[i] = offsets[i] - offsets[i - 1];
        }

        for (size_t i = 0; i < chunk_size; i++) {
            size_t begin = array_offsets[i];
            size_t end = array_offsets[i + 1];

            // revert size
            std::reverse(offsets.begin() + begin + 1, offsets.begin() + end + 1);

            // convert size to offset
            for (size_t j = begin; j < end; j++) {
                offsets[j + 1] = offsets[j] + offsets[j + 1];
            }

            // revert all byte of one array
            auto& bytes = down_cast<BinaryColumn*>(column)->get_bytes();
            std::reverse(bytes.begin() + offsets[begin], bytes.begin() + offsets[end]);

            // revert string one by one
            for (size_t j = begin; j < end; j++) {
                std::reverse(bytes.begin() + offsets[j], bytes.begin() + offsets[j + 1]);
            }
        }
    }

    static void _reverse_json_column(Column* column, const Buffer<uint32_t>& array_offsets, size_t chunk_size) {
        auto json_column = down_cast<JsonColumn*>(column);
        auto& pool = json_column->get_pool();
        for (size_t i = 0; i < chunk_size; i++) {
            std::reverse(pool.begin() + array_offsets[i], pool.begin() + array_offsets[i + 1]);
        }
        json_column->reset_cache();
    }

    static void _reverse_data_column(Column* column, const Buffer<uint32_t>& offsets, size_t chunk_size) {
        if constexpr (lt_is_fixedlength<LT>) {
            _reverse_fixed_column(column, offsets, chunk_size);
        } else if constexpr (lt_is_string<LT>) {
            _reverse_binary_column(column, offsets, chunk_size);
        } else if constexpr (lt_is_json<LT>) {
            _reverse_json_column(column, offsets, chunk_size);
        } else {
            assert(false);
        }
    }

    static void _reverse_null_column(Column* column, const Buffer<uint32_t>& offsets, size_t chunk_size) {
        auto& data = down_cast<UInt8Column*>(column)->get_data();

        for (size_t i = 0; i < chunk_size; i++) {
            std::reverse(data.begin() + offsets[i], data.begin() + offsets[i + 1]);
        }
    }

    static void _reverse_array_column(Column* column, size_t chunk_size) {
        auto* array_column = down_cast<ArrayColumn*>(column);
        auto& elements_column = array_column->elements_column();
        auto& offsets = array_column->offsets_column()->get_data();

        if (elements_column->is_nullable()) {
            auto* nullable_column = down_cast<NullableColumn*>(elements_column.get());
            auto* null_column = nullable_column->mutable_null_column();
            auto* data_column = nullable_column->data_column().get();

            if (nullable_column->has_null()) {
                _reverse_null_column(null_column, offsets, chunk_size);
            }
            _reverse_data_column(data_column, offsets, chunk_size);
        } else {
            _reverse_data_column(column, offsets, chunk_size);
        }
    }
};

class ArrayJoin {
public:
    static ColumnPtr process(FunctionContext* ctx, const Columns& columns) {
        // TODO: optimize the performance of const sep or const null replace str
        DCHECK_GE(columns.size(), 2);
        size_t chunk_size = columns[0]->size();

        RETURN_IF_COLUMNS_ONLY_NULL(columns);

        ColumnPtr src_column = ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[0]);
        if (columns.size() <= 2) {
            return _join_column_ignore_null(src_column, columns[1], chunk_size);
        } else {
            return _join_column_replace_null(src_column, columns[1], columns[2], chunk_size);
        }
    }

private:
    static ColumnPtr _join_column_replace_null(const ColumnPtr& src_column, const ColumnPtr& sep_column,
                                               const ColumnPtr& null_replace_column, size_t chunk_size) {
        NullableBinaryColumnBuilder res;
        // byte_size may be smaller or larger than actual used size
        // byte_size is only one reserve size
        size_t byte_size = ColumnHelper::get_data_column(src_column.get())->byte_size() +
                           ColumnHelper::get_data_column(sep_column.get())->byte_size(0) * src_column->size() +
                           ColumnHelper::get_data_column(null_replace_column.get())->byte_size(0) *
                                   ColumnHelper::count_nulls(src_column);
        res.resize(chunk_size, byte_size);

        for (size_t i = 0; i < chunk_size; i++) {
            if (src_column->is_null(i) || sep_column->is_null(i) || null_replace_column->is_null(i)) {
                res.set_null(i);
                continue;
            }
            auto tmp_datum = src_column->get(i);
            const auto& datum_array = tmp_datum.get_array();
            bool append = false;
            Slice sep_slice = sep_column->get(i).get_slice();
            Slice null_slice = null_replace_column->get(i).get_slice();
            for (const auto& datum : datum_array) {
                if (append) {
                    res.append_partial(sep_slice);
                }
                if (datum.is_null()) {
                    res.append_partial(null_slice);
                } else {
                    Slice value_slice = datum.get_slice();
                    res.append_partial(value_slice);
                }
                append = true;
            }
            res.append_complete(i);
        }
        return res.build_nullable_column();
    }

    static ColumnPtr _join_column_ignore_null(const ColumnPtr& src_column, const ColumnPtr& sep_column,
                                              size_t chunk_size) {
        NullableBinaryColumnBuilder res;
        // bytes_size may be smaller or larger then actual used size
        // byte_size is only one reserve size
        size_t byte_size = ColumnHelper::get_data_column(src_column.get())->byte_size() +
                           ColumnHelper::get_data_column(sep_column.get())->byte_size(0) * src_column->size();
        res.resize(chunk_size, byte_size);

        for (size_t i = 0; i < chunk_size; i++) {
            if (src_column->is_null(i) || sep_column->is_null(i)) {
                res.set_null(i);
                continue;
            }

            auto tmp_datum = src_column->get(i);
            const auto& datum_array = tmp_datum.get_array();
            bool append = false;
            Slice sep_slice = sep_column->get(i).get_slice();
            for (const auto& datum : datum_array) {
                if (datum.is_null()) {
                    continue;
                }
                if (append) {
                    res.append_partial(sep_slice);
                }
                Slice value_slice = datum.get_slice();
                res.append_partial(value_slice);
                append = true;
            }
            res.append_complete(i);
        }

        return res.build_nullable_column();
    }
};

// all/any_match(lambda_func, array1, array2...)-> all/any_match(array_map(lambda_func, array1, array2...))
// -> all/any_match(bool_array), result is bool type.
// any_match: if there are true  matched, return true,  else if there are null, return null, otherwise, return false;
// all_match: if there are false matched, return false, else if there are null, return null, otherwise, return true;
template <bool isAny>
class ArrayMatch {
public:
    static ColumnPtr process([[maybe_unused]] FunctionContext* ctx, const Columns& columns) {
        return _array_match(columns);
    }

private:
    static ColumnPtr _array_match(const Columns& columns) {
        DCHECK(columns.size() == 1);
        RETURN_IF_COLUMNS_ONLY_NULL(columns);
        bool is_const = columns[0]->is_constant();

        size_t chunk_size = columns[0]->size();
        ColumnPtr bool_column = is_const ? FunctionHelper::get_data_column_of_const(columns[0]) : columns[0];

        size_t dest_num_rows = is_const ? 1 : chunk_size;
        auto dest_null_column = NullColumn::create(dest_num_rows, 0);
        auto dest_data_column = BooleanColumn::create(dest_num_rows);
        dest_null_column->get_data().resize(dest_num_rows, 0);

        ArrayColumn* bool_array;
        NullColumn* array_null_map = nullptr;

        if (bool_column->is_nullable()) {
            auto nullable_column = down_cast<NullableColumn*>(bool_column.get());
            bool_array = down_cast<ArrayColumn*>(nullable_column->data_column().get());
            array_null_map = nullable_column->null_column().get();
        } else {
            bool_array = down_cast<ArrayColumn*>(bool_column.get());
        }
        const auto& offsets = bool_array->offsets().get_data();

        ColumnViewer<TYPE_BOOLEAN> bool_elements(bool_array->elements_column());

        for (size_t i = 0; i < dest_num_rows; ++i) {
            if (array_null_map == nullptr || !array_null_map->get_data()[i]) { // array_null_map[i] is not null
                bool has_null = false;
                bool res = !isAny;
                for (auto id = offsets[i]; id < offsets[i + 1]; ++id) {
                    if (bool_elements.is_null(id)) {
                        has_null = true;
                    } else {
                        if (bool_elements.value(id) == isAny) {
                            res = isAny;
                            break;
                        }
                    }
                }
                dest_null_column->get_data()[i] = res != isAny && has_null;
                dest_data_column->get_data()[i] = res;
            } else { // array_null_map[i] is null, result is null
                dest_null_column->get_data()[i] = 1;
                dest_data_column->get_data()[i] = 0;
            }
        }

        ColumnPtr dest_column = NullableColumn::create(std::move(dest_data_column), std::move(dest_null_column));
        if (is_const) {
            dest_column = ConstColumn::create(std::move(dest_column), chunk_size);
        }
        return dest_column;
    }
};

// by design array_filter(array, bool_array), if bool_array is null, return an empty array. We do not return null, as
// it will change the null property of return results which keeps the same with the first argument array.
class ArrayFilter {
public:
    static ColumnPtr process([[maybe_unused]] FunctionContext* ctx, const Columns& columns) {
        const auto& src_column = columns[0];
        const auto& filter_column = columns[1];
        return _array_filter(src_column, filter_column);
    }

private:
    static ColumnPtr _array_filter(const ColumnPtr& src_column, const ColumnPtr& filter_column) {
        if (src_column->only_null()) {
            return src_column;
        }
        bool is_src_const = src_column->is_constant();
        bool is_filter_const = filter_column->is_constant();

        size_t chunk_size = src_column->size();
        if (filter_column->only_null()) {
            if (is_src_const) {
                // return a const column with empty array
                auto data_column = FunctionHelper::get_data_column_of_const(src_column);
                auto dest_data_column = data_column->clone_empty();
                dest_data_column->append_default();
                return ConstColumn::create(std::move(dest_data_column), chunk_size);
            } else {
                // return a nullable column with only empty arrays, the null column shoule be same with src column.
                ColumnPtr dest_column = src_column->clone_empty();
                ColumnPtr data_column = dest_column;
                if (src_column->is_nullable()) {
                    const auto src_null_column = down_cast<const NullableColumn*>(src_column.get())->null_column();
                    auto dest_nullable_column = down_cast<NullableColumn*>(dest_column.get());
                    auto dest_null_column = dest_nullable_column->mutable_null_column();
                    dest_null_column->get_data().assign(src_null_column->get_data().begin(),
                                                        src_null_column->get_data().end());
                    dest_nullable_column->set_has_null(src_column->has_null());
                    data_column = dest_nullable_column->data_column();
                }
                data_column->append_default(chunk_size);
                return dest_column;
            }
        }

        ColumnPtr dest_column = is_src_const ? FunctionHelper::get_data_column_of_const(src_column)->clone_empty()
                                             : src_column->clone_empty();

        NullColumn* dest_null_column = nullptr;
        if (src_column->is_nullable()) {
            const auto* src_nullable_column = down_cast<const NullableColumn*>(src_column.get());
            const auto& src_null_column = src_nullable_column->null_column();
            auto* dest_nullable_column = down_cast<NullableColumn*>(dest_column.get());
            dest_null_column = dest_nullable_column->mutable_null_column();

            dest_null_column->get_data().assign(src_null_column->get_data().begin(), src_null_column->get_data().end());
            dest_nullable_column->set_has_null(src_nullable_column->has_null());
        }

        ColumnPtr src_data_column = src_column;
        ColumnPtr dest_data_column = dest_column;
        if (is_src_const) {
            src_data_column = FunctionHelper::get_data_column_of_const(src_column);
            src_data_column = FunctionHelper::get_data_column_of_nullable(src_data_column);
            dest_data_column = FunctionHelper::get_data_column_of_const(dest_column);
            dest_data_column = FunctionHelper::get_data_column_of_nullable(dest_data_column);
        } else {
            src_data_column = FunctionHelper::get_data_column_of_nullable(src_data_column);
            dest_data_column = FunctionHelper::get_data_column_of_nullable(dest_data_column);
        }

        ColumnPtr filter_data_column =
                is_filter_const ? FunctionHelper::get_data_column_of_const(filter_column) : filter_column;
        size_t num_rows = (is_src_const && is_filter_const) ? 1 : chunk_size;
        if (is_src_const && is_filter_const) {
            _filter_array_items<true, true>(down_cast<ArrayColumn*>(src_data_column.get()), filter_data_column,
                                            down_cast<ArrayColumn*>(dest_data_column.get()), dest_null_column,
                                            num_rows);
        } else if (is_src_const && !is_filter_const) {
            _filter_array_items<true, false>(down_cast<ArrayColumn*>(src_data_column.get()), filter_data_column,
                                             down_cast<ArrayColumn*>(dest_data_column.get()), dest_null_column,
                                             num_rows);
        } else if (!is_src_const && is_filter_const) {
            _filter_array_items<false, true>(down_cast<ArrayColumn*>(src_data_column.get()), filter_data_column,
                                             down_cast<ArrayColumn*>(dest_data_column.get()), dest_null_column,
                                             num_rows);
        } else {
            _filter_array_items<false, false>(down_cast<ArrayColumn*>(src_data_column.get()), filter_data_column,
                                              down_cast<ArrayColumn*>(dest_data_column.get()), dest_null_column,
                                              num_rows);
        }
        dest_column->check_or_die();
        if (is_src_const && is_filter_const) {
            auto const_column = ConstColumn::create(std::move(dest_column), chunk_size);
            const_column->check_or_die();
            return const_column;
        } else {
            return dest_column;
        }
    }

    template <bool ConstSrc, bool ConstFilter>
    static void _filter_array_items(const ArrayColumn* src_column, const ColumnPtr& raw_filter,
                                    ArrayColumn* dest_column, NullColumn* dest_null_map, size_t src_rows) {
        if constexpr (!ConstSrc) {
            DCHECK_EQ(src_column->size(), src_rows);
        }
        const ArrayColumn* filter;
        const NullColumn* filter_null_map = nullptr;
        auto& dest_offsets = dest_column->offsets_column()->get_data();

        if (raw_filter->is_nullable()) {
            const auto nullable_column = down_cast<const NullableColumn*>(raw_filter.get());
            filter = down_cast<const ArrayColumn*>(nullable_column->data_column().get());
            filter_null_map = nullable_column->null_column().get();
        } else {
            filter = down_cast<const ArrayColumn*>(raw_filter.get());
        }

        std::vector<uint32_t> indexes;
        size_t num_rows = ConstSrc ? src_rows : src_column->size();
        for (size_t i = 0; i < num_rows; i++) {
            if (filter_null_map == nullptr || !filter_null_map->get_data()[i]) {
                bool filter_is_not_null =
                        (filter_null_map == nullptr ||
                         (ConstFilter ? !filter_null_map->get_data()[0] : !filter_null_map->get_data()[i]));
                if (filter_is_not_null) {
                    // if filter is not null, we should filter each elements in array
                    const auto& src_offsets = src_column->offsets().get_data();
                    size_t src_start = ConstSrc ? src_offsets[0] : src_offsets[i];
                    size_t src_end = ConstSrc ? src_offsets[1] : src_offsets[i + 1];
                    size_t src_elements_num = src_end - src_start;

                    const auto& filter_offsets = filter->offsets().get_data();
                    size_t filter_start = ConstFilter ? filter_offsets[0] : filter_offsets[i];
                    size_t filter_end = ConstFilter ? filter_offsets[1] : filter_offsets[i + 1];
                    size_t filter_elements_num = filter_end - filter_start;

                    const auto& filter_elements = filter->elements();
                    size_t valid_elements_num = 0;

                    for (size_t idx = 0; idx < src_elements_num; idx++) {
                        if (idx < filter_elements_num && !filter_elements.is_null(filter_start + idx) &&
                            filter_elements.get(filter_start + idx).get_int8() != 0) {
                            indexes.emplace_back(src_start + idx);
                            valid_elements_num++;
                        }
                    }
                    dest_offsets.emplace_back(dest_offsets.back() + valid_elements_num);
                } else {
                    dest_offsets.emplace_back(dest_offsets.back());
                }
            } else {
                dest_offsets.emplace_back(dest_offsets.back());
            }
        }
        dest_column->elements_column()->append_selective(src_column->elements(), indexes);
    }
};

// array_sortby(array, key_array) the key_array should not change the null property of array, if key_array is null,
// keep the array the same.
template <LogicalType LT>
class ArraySortBy : public ArraySort<LT> {
public:
    using ColumnType = RunTimeColumnType<LT>;

    static ColumnPtr process(FunctionContext* ctx, const Columns& columns) {
        DCHECK_EQ(columns.size(), 2);

        if (columns[0]->only_null() || columns[1]->only_null()) {
            return columns[0];
        }

        size_t chunk_size = columns[0]->size();

        // TODO: For fixed-length types, you can operate directly on the original column without using sort index,
        //  which will be optimized later

        ColumnPtr src_column = ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[0]);
        ColumnPtr dest_column = src_column->clone_empty();
        ColumnPtr key_column = ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[1]);
        if (key_column->size() != src_column->size()) {
            throw std::runtime_error("Input array size is not equal in array_sortby.");
        }

        if (src_column->is_nullable()) {
            const auto* src_nullable_column = down_cast<const NullableColumn*>(src_column.get());
            const auto& src_data_column = src_nullable_column->data_column_ref();
            const auto& src_null_column = src_nullable_column->null_column_ref();

            auto* dest_nullable_column = down_cast<NullableColumn*>(dest_column.get());
            auto* dest_data_column = dest_nullable_column->mutable_data_column();
            auto* dest_null_column = dest_nullable_column->mutable_null_column();

            if (src_column->has_null()) {
                dest_null_column->get_data().assign(src_null_column.get_data().begin(),
                                                    src_null_column.get_data().end());
            } else {
                dest_null_column->get_data().resize(chunk_size, 0);
            }
            dest_nullable_column->set_has_null(src_nullable_column->has_null());

            _sort_array_column(dest_data_column, src_data_column, key_column, &src_null_column);
        } else {
            _sort_array_column(dest_column.get(), *src_column, key_column, nullptr);
        }
        return dest_column;
    }

private:
    static void _sort_array_column(Column* dest_array_column, const Column& src_array_column,
                                   const ColumnPtr& key_array_ptr, const NullColumn* src_null_map) {
        NullColumnPtr key_null_map = nullptr;
        ColumnPtr key_data = key_array_ptr;
        if (key_array_ptr->is_nullable()) { // Nullable(array(Nullable(element), offsets), null_map)
            const auto* key_nullable_column = down_cast<const NullableColumn*>(key_array_ptr.get());
            key_data = key_nullable_column->data_column();
            key_null_map = key_nullable_column->null_column();
        }
        // key_data is of array(Nullable(element), offsets)

        const auto& key_element_column = down_cast<ArrayColumn*>(key_data.get())->elements();
        const auto& key_offsets_column = down_cast<ArrayColumn*>(key_data.get())->offsets();

        const auto& src_elements_column = down_cast<const ArrayColumn&>(src_array_column).elements();
        const auto& src_offsets_column = down_cast<const ArrayColumn&>(src_array_column).offsets();

        auto* dest_elements_column = down_cast<ArrayColumn*>(dest_array_column)->elements_column().get();
        auto* dest_offsets_column = down_cast<ArrayColumn*>(dest_array_column)->offsets_column().get();
        dest_offsets_column->get_data() = src_offsets_column.get_data();

        size_t chunk_size = src_array_column.size();
        // key_element_column's size may be not equal with src_element_column, so should align their sort index for
        // each array.
        std::vector<uint32_t> key_sort_index, src_sort_index;
        src_sort_index.reserve(src_elements_column.size());
        ArraySort<LT>::_init_sort_index(&key_sort_index, key_element_column.size());
        // element column is nullable
        if (key_element_column.has_null()) {
            const auto& key_data_column = down_cast<const NullableColumn&>(key_element_column).data_column_ref();
            const auto& null_column = down_cast<const NullableColumn&>(key_element_column).null_column_ref();

            for (size_t i = 0; i < chunk_size; i++) {
                if ((src_null_map == nullptr || !src_null_map->get_data()[i]) &&
                    (key_null_map == nullptr || !key_null_map->get_data()[i])) {
                    if (src_offsets_column.get_data()[i + 1] - src_offsets_column.get_data()[i] !=
                        key_offsets_column.get_data()[i + 1] - key_offsets_column.get_data()[i]) {
                        throw std::runtime_error("Input arrays' size are not equal in array_sortby.");
                    }
                    ArraySort<LT>::_sort_nullable_item(&key_sort_index, key_data_column, null_column,
                                                       key_offsets_column, i);
                    auto delta = key_offsets_column.get_data()[i] - src_offsets_column.get_data()[i];
                    for (auto id = key_offsets_column.get_data()[i]; id < key_offsets_column.get_data()[i + 1]; ++id) {
                        src_sort_index.push_back(key_sort_index[id] - delta);
                    }
                } else {
                    for (auto id = src_offsets_column.get_data()[i]; id < src_offsets_column.get_data()[i + 1]; ++id) {
                        src_sort_index.push_back(id);
                    }
                }
            }
        } else {
            const auto& key_data_column = down_cast<const NullableColumn&>(key_element_column).data_column_ref();

            for (size_t i = 0; i < chunk_size; i++) {
                if ((src_null_map == nullptr || !src_null_map->get_data()[i]) &&
                    (key_null_map == nullptr || !key_null_map->get_data()[i])) {
                    if (src_offsets_column.get_data()[i + 1] - src_offsets_column.get_data()[i] !=
                        key_offsets_column.get_data()[i + 1] - key_offsets_column.get_data()[i]) {
                        throw std::runtime_error("Input arrays' size are not equal in array_sortby.");
                    }
                    ArraySort<LT>::_sort_item(&key_sort_index, key_data_column, key_offsets_column, i);
                    auto delta = key_offsets_column.get_data()[i] - src_offsets_column.get_data()[i];
                    for (auto id = key_offsets_column.get_data()[i]; id < key_offsets_column.get_data()[i + 1]; ++id) {
                        src_sort_index.push_back(key_sort_index[id] - delta);
                    }
                } else {
                    for (auto id = src_offsets_column.get_data()[i]; id < src_offsets_column.get_data()[i + 1]; ++id) {
                        src_sort_index.push_back(id);
                    }
                }
            }
        }
        // the element of src_sort_index should less than the size of src_elements_column
        dest_elements_column->append_selective(src_elements_column, src_sort_index);
    }
};

template <bool isMin>
class ArrayMinMax {
public:
    template <LogicalType ResultType, LogicalType ElementType, bool HasNull>
    static void process(const Column* elements, const NullColumn* elements_null_col, const UInt32Column* offsets,
                        Column* result_col, NullColumn* null_cols) {
        const RunTimeCppType<TYPE_NULL>* elements_nulls = nullptr;
        if constexpr (HasNull) {
            elements_nulls = elements_null_col->get_data().data();
        }
        const auto& elements_data = GetContainer<ElementType>::get_data(elements);

        auto* offsets_ptr = offsets->get_data().data();
        auto* null_ptr = null_cols->get_data().data();
        const size_t rows = offsets->size() - 1;

        result_col->reserve(rows);
        auto result_ptr = down_cast<RunTimeColumnType<ResultType>*>(result_col);

        for (size_t i = 0; i < rows; i++) {
            size_t offset = offsets_ptr[i];
            size_t array_size = offsets_ptr[i + 1] - offsets_ptr[i];

            if (array_size <= 0 || null_ptr[i] == 1) {
                result_ptr->append_default();
                null_ptr[i] = 1;
                continue;
            }

            RunTimeCppType<ResultType> result;
            int index = 0;
            if constexpr (!lt_is_string<ResultType>) {
                if constexpr (isMin) {
                    result = RunTimeTypeLimits<ResultType>::max_value();
                } else {
                    result = RunTimeTypeLimits<ResultType>::min_value();
                }
                index = 0;
            } else {
                if constexpr (HasNull) {
                    while (index < array_size && elements_nulls[offset + index] != 0) {
                        index++;
                    }

                    if (index < array_size) {
                        result = elements_data[offset + index];
                    } else {
                        result_ptr->append_default();
                        null_ptr[i] = 1;
                        continue;
                    }
                } else {
                    result = elements_data[offset + index];
                }
                index++;
            }

            bool has_data = false;
            for (; index < array_size; index++) {
                if constexpr (HasNull) {
                    if (elements_nulls[offset + index] != 0) {
                        continue;
                    }
                }

                has_data = true;
                const auto& value = elements_data[offset + index];
                if constexpr (isMin) {
                    result = result < value ? result : value;
                } else {
                    result = result < value ? value : result;
                }
            }

            if constexpr (!lt_is_string<ResultType>) {
                if (has_data) {
                    result_ptr->append(result);
                } else {
                    result_ptr->append_default();
                    null_ptr[i] = 1;
                }
            } else {
                result_ptr->append(result);
            }
        }
    }
};

template <bool isSum>
class ArraySumAvg {
public:
    template <LogicalType ResultType, LogicalType ElementType, bool HasNull>
    static void process(const Column* elements, const NullColumn* elements_null_col, const UInt32Column* offsets,
                        Column* result_col, NullColumn* null_cols) {
        const RunTimeCppType<TYPE_NULL>* elements_nulls = nullptr;
        if constexpr (HasNull) {
            elements_nulls = elements_null_col->get_data().data();
        }

        auto* elements_data = down_cast<const RunTimeColumnType<ElementType>*>(elements)->get_data().data();
        auto* offsets_ptr = offsets->get_data().data();
        auto* null_ptr = null_cols->get_data().data();
        const int64_t rows = offsets->size() - 1;

        result_col->reserve(rows);
        auto result_ptr = down_cast<RunTimeColumnType<ResultType>*>(result_col);

        using ResultCppType = RunTimeCppType<ResultType>;
        int scale = 0;
        if constexpr (lt_is_decimal<ElementType>) {
            scale = down_cast<const RunTimeColumnType<ElementType>*>(elements)->scale();
        }

        for (size_t i = 0; i < rows; i++) {
            size_t offset = offsets_ptr[i];
            int64_t array_size = offsets_ptr[i + 1] - offsets_ptr[i];
            RunTimeCppType<ResultType> sum{};

            bool has_data = false;
            if (null_ptr[i] != 1) {
                for (size_t j = 0; j < array_size; j++) {
                    if constexpr (HasNull) {
                        if (elements_nulls[offset + j] != 0) {
                            continue;
                        }
                    }

                    has_data = true;
                    auto& value = elements_data[offset + j];
                    sum += value;
                }
            }

            if (has_data) {
                if constexpr (lt_is_decimalv2<ElementType>) {
                    if constexpr (isSum) {
                        result_ptr->append(sum);
                    } else {
                        result_ptr->append(sum / DecimalV2Value(array_size, 0));
                    }
                } else if constexpr (lt_is_arithmetic<ElementType>) {
                    if constexpr (isSum) {
                        result_ptr->append(sum);
                    } else {
                        result_ptr->append(sum / array_size);
                    }
                } else if constexpr (lt_is_decimal<ElementType>) {
                    if constexpr (isSum) {
                        result_ptr->append(sum);
                    } else {
                        ResultCppType ds = ResultCppType(sum);
                        ResultCppType dc = ResultCppType(array_size);
                        result_ptr->append(decimal_div_integer<ResultCppType>(ds, dc, scale));
                    }
                } else {
                    LOG(ERROR) << "unhandled types other than arithmetic/time/decimal for sum and avg";
                    DCHECK(false) << "other types than arithmetic/time/decimal is not support sum "
                                     "and avg";
                    result_ptr->append_default();
                }
            } else {
                result_ptr->append_default();
                null_ptr[i] = 1;
            }
        }
    }
};

class ArrayArithmetic {
public:
    template <LogicalType ElementType>
    static StatusOr<ColumnPtr> array_max(FunctionContext* context, const Columns& columns) {
        return ArrayArithmetic::template process<ElementType, ElementType, ArrayMinMax<false>>(context, columns);
    }

    template <LogicalType ElementType>
    static StatusOr<ColumnPtr> array_min(FunctionContext* context, const Columns& columns) {
        return ArrayArithmetic::template process<ElementType, ElementType, ArrayMinMax<true>>(context, columns);
    }

    template <LogicalType ElementType>
    static StatusOr<ColumnPtr> array_avg(FunctionContext* context, const Columns& columns) {
        if constexpr (lt_is_sum_bigint<ElementType> || lt_is_float<ElementType> || lt_is_largeint<ElementType>) {
            return ArrayArithmetic::template process<TYPE_DOUBLE, ElementType, ArraySumAvg<false>>(context, columns);
        } else if constexpr (ElementType == TYPE_DECIMALV2) {
            return ArrayArithmetic::template process<TYPE_DECIMALV2, ElementType, ArraySumAvg<false>>(context, columns);
        } else if constexpr (lt_is_decimal<ElementType>) {
            return ArrayArithmetic::template process<TYPE_DECIMAL128, ElementType, ArraySumAvg<false>>(context,
                                                                                                       columns);
        } else {
            LOG(ERROR) << "array_avg doesn't support column type: " << ElementType;
            DCHECK(false) << "array_avg doesn't support column type: " << ElementType;
            auto all_null = ColumnHelper::create_const_null_column(1);
            return all_null;
        }
    }

    template <LogicalType ElementType>
    static StatusOr<ColumnPtr> array_sum(FunctionContext* context, const Columns& columns) {
        if constexpr (lt_is_sum_bigint<ElementType>) {
            return ArrayArithmetic::template process<TYPE_BIGINT, ElementType, ArraySumAvg<true>>(context, columns);
        } else if constexpr (lt_is_largeint<ElementType>) {
            return ArrayArithmetic::template process<TYPE_LARGEINT, ElementType, ArraySumAvg<true>>(context, columns);
        } else if constexpr (lt_is_float<ElementType>) {
            return ArrayArithmetic::template process<TYPE_DOUBLE, ElementType, ArraySumAvg<true>>(context, columns);
        } else if constexpr (ElementType == TYPE_DECIMALV2) {
            return ArrayArithmetic::template process<TYPE_DECIMALV2, ElementType, ArraySumAvg<true>>(context, columns);
        } else if constexpr (lt_is_decimal<ElementType>) {
            return ArrayArithmetic::template process<TYPE_DECIMAL128, ElementType, ArraySumAvg<true>>(context, columns);
        } else {
            LOG(ERROR) << "array_sum doesn't support column type: " << ElementType;
            DCHECK(false) << "array_sum doesn't support column type: " << ElementType;
            auto all_null = ColumnHelper::create_const_null_column(1);
            return all_null;
        }
    }

private:
    template <LogicalType ResultType, LogicalType ElementType, typename FUNC>
    static StatusOr<ColumnPtr> process(FunctionContext* context, const Columns& columns) {
        DCHECK_EQ(1, columns.size());
        if (columns[0]->only_null()) {
            return columns[0];
        }

        NullColumnPtr array_null = nullptr;
        ArrayColumn* array_col = nullptr;

        auto array_column = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]);
        if (array_column->is_nullable()) {
            auto nullable = down_cast<NullableColumn*>(array_column.get());

            array_col = down_cast<ArrayColumn*>(nullable->data_column().get());
            array_null = NullColumn::create(*nullable->null_column());
        } else {
            array_col = down_cast<ArrayColumn*>(array_column.get());
            array_null = NullColumn::create(array_column->size(), 0);
        }

        const UInt32Column& offsets = array_col->offsets();
        auto elements = array_col->elements_column().get();

        ColumnPtr result = nullptr;

        if constexpr (lt_is_decimal<ResultType>) {
            auto desc = context->get_return_type();
            result = RunTimeColumnType<ResultType>::create(desc.precision, desc.scale);
        } else {
            result = RunTimeColumnType<ResultType>::create();
        }

        Column* elements_data = elements;
        NullColumn* elements_nulls = nullptr;
        if (elements->is_nullable()) {
            auto nullable = down_cast<NullableColumn*>(elements);
            elements_data = nullable->data_column().get();
            elements_nulls = nullable->null_column().get();
        }

        if (elements->has_null()) {
            FUNC::template process<ResultType, ElementType, true>(elements_data, elements_nulls, &offsets, result.get(),
                                                                  array_null.get());
        } else {
            FUNC::template process<ResultType, ElementType, false>(elements_data, elements_nulls, &offsets,
                                                                   result.get(), array_null.get());
        }

        return NullableColumn::create(std::move(result), std::move(array_null));
    }
};

// Todo:support datetime/date
template <LogicalType Type>
class ArrayGenerate {
public:
    using InputColumnType = RunTimeColumnType<Type>;
    using InputCppType = RunTimeCppType<Type>;
    static StatusOr<ColumnPtr> process(FunctionContext* ctx, const Columns& columns) {
        RETURN_IF_COLUMNS_ONLY_NULL(columns);
        DCHECK(columns.size() == 3);

        auto num_rows = columns[0]->size();

        // compute nulls first. if any input is null, then output is null
        NullColumnPtr nulls;
        for (auto& column : columns) {
            if (column->has_null()) {
                const auto* nullable_column = down_cast<const NullableColumn*>(column.get());
                if (nulls == nullptr) {
                    nulls = NullColumn::static_pointer_cast(nullable_column->null_column()->clone());
                } else {
                    ColumnHelper::or_two_filters(num_rows, nulls->get_data().data(),
                                                 nullable_column->null_column()->get_data().data());
                }
            }
        }

        auto array_offsets = UInt32Column::create(0);
        auto array_elements = ColumnHelper::create_column(TypeDescriptor(Type), true, false, 0);

        auto offsets = array_offsets.get();
        auto elements = down_cast<NullableColumn*>(array_elements.get());

        offsets->reserve(num_rows + 1);
        offsets->append(0);

        auto all_const_cols = columns[0]->is_constant() && columns[1]->is_constant() && columns[2]->is_constant();

        auto num_rows_to_process = all_const_cols ? 1 : num_rows;

        size_t total_elements = 0;
        auto* data_column = elements->mutable_data_column();
        auto* null_column = elements->mutable_null_column();
        ColumnViewer start_viewer = ColumnViewer<Type>(columns[0]);
        ColumnViewer stop_viewer = ColumnViewer<Type>(columns[1]);
        ColumnViewer step_viewer = ColumnViewer<Type>(columns[2]);
        for (size_t cur_row = 0; cur_row < num_rows_to_process; cur_row++) {
            if (nulls && nulls->get_data()[cur_row]) {
                continue;
            }
            auto step = step_viewer.value(cur_row);
            if (step == 0) {
                continue;
            }
            auto start = start_viewer.value(cur_row);
            auto stop = stop_viewer.value(cur_row);
            if (step > 0 && start <= stop) {
                total_elements += (stop - start) / step + 1;
            } else if (step < 0 && start >= stop) {
                total_elements += (start - stop) / (-step) + 1;
            }
        }
        TRY_CATCH_BAD_ALLOC(data_column->reserve(total_elements));

        size_t total_elements_num = 0;
        for (size_t cur_row = 0; cur_row < num_rows_to_process; cur_row++) {
            if (nulls && nulls->get_data()[cur_row]) {
                offsets->append(offsets->get_data().back());
                continue;
            }

            auto step = step_viewer.value(cur_row);

            // just return empty array
            if (step == 0) {
                offsets->append(offsets->get_data().back());
                continue;
            }
            auto start = start_viewer.value(cur_row);
            auto stop = stop_viewer.value(cur_row);

            InputCppType temp;
            for (InputCppType cur_element = start; step > 0 ? cur_element <= stop : cur_element >= stop;
                 cur_element += step) {
                data_column->append_datum(cur_element);
                total_elements_num++;
                if (__builtin_add_overflow(cur_element, step, &temp)) break;
            }
            offsets->append(total_elements_num);
        }

        null_column->get_data().resize(total_elements_num, 0);
        CHECK_EQ(offsets->get_data().back(), elements->size());

        auto dst = ArrayColumn::create(std::move(array_elements), std::move(array_offsets));

        if (all_const_cols) {
            if (nulls->is_null(0)) {
                return ColumnHelper::create_const_null_column(num_rows);
            } else {
                return ConstColumn::create(std::move(dst), num_rows);
            }
        }

        if (nulls == nullptr) {
            return std::move(dst);
        } else {
            // if any of input column has null value, then output column is nullable
            return NullableColumn::create(std::move(dst), std::move(nulls));
        }
    }
};

template <LogicalType LT, bool PositionEnabled, typename ReturnType>
class ArrayContains {
public:
    using CppType = RunTimeCppType<LT>;
    using ColumnType = RunTimeColumnType<LT>;
    using HashFunc = PhmapDefaultHashFunc<LT, PhmapSeed1>;
    using HashMap = phmap::flat_hash_map<CppType, size_t, HashFunc>;

    struct ArrayContainsState {
        size_t first_null_position = 0;
        HashMap hash_map;
    };

    static Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        if (scope != FunctionContext::FRAGMENT_LOCAL) {
            return Status::OK();
        }
        if (!context->is_notnull_constant_column(0)) {
            return Status::OK();
        }
        auto* state = new ArrayContainsState();
        context->set_function_state(scope, state);
        if constexpr (!HashFunc::is_supported()) {
            return Status::OK();
        }
        ColumnPtr column = context->get_constant_column(0);
        ColumnPtr array_column = FunctionHelper::get_data_column_of_const(column);
        _build_hash_table(array_column, state);

        return Status::OK();
    }

    static Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            auto* state =
                    reinterpret_cast<ArrayContainsState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
            delete state;
        }
        return Status::OK();
    }

    static StatusOr<ColumnPtr> process(FunctionContext* context, const Columns& columns) {
        if constexpr (!is_supported(LT)) {
            return Status::NotSupported(fmt::format("not support type {}", LT));
        }
        if (columns[0]->only_null()) {
            return columns[0];
        }

        const ColumnPtr& array_column = columns[0];
        const ColumnPtr& target_column = columns[1];

        bool is_const_array = array_column->is_constant();
        ColumnPtr array_data_column = FunctionHelper::get_data_column_of_const(array_column);
        bool is_nullable_array = array_data_column->is_nullable();
        NullColumnPtr array_null_column;
        const NullColumn::ValueType* array_null_data = nullptr;
        if (is_nullable_array) {
            array_null_column = down_cast<NullableColumn*>(array_data_column.get())->null_column();
            array_null_data = down_cast<NullableColumn*>(array_data_column.get())->null_column_data().data();
            array_data_column = down_cast<NullableColumn*>(array_data_column.get())->data_column();
        }

        bool is_const_target = target_column->is_constant();
        ColumnPtr target_data_column = FunctionHelper::get_data_column_of_const(target_column);
        const NullColumn::ValueType* target_null_data = nullptr;
        bool is_nullable_target = target_data_column->is_nullable();
        if (is_nullable_target) {
            target_null_data = down_cast<NullableColumn*>(target_data_column.get())->null_column_data().data();
            target_data_column = down_cast<NullableColumn*>(target_data_column.get())->data_column();
        }

        auto process_func = [&]() -> StatusOr<ColumnPtr> {
            if constexpr (HashFunc::is_supported()) {
                if (is_const_array) {
                    auto* state = reinterpret_cast<ArrayContainsState*>(
                            context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
                    if (UNLIKELY(state == nullptr)) {
                        return Status::InternalError("array_contains get state failed");
                    }
                    return is_nullable_target ? _process_with_hash_table<true>(state, target_data_column,
                                                                               target_null_data, is_const_target)
                                              : _process_with_hash_table<false>(state, target_data_column,
                                                                                target_null_data, is_const_target);
                }
            }

            if (is_nullable_array && is_nullable_target) {
                return _process_generic<true, true>(array_data_column, target_data_column, array_null_data,
                                                    target_null_data, is_const_array, is_const_target);
            } else if (is_nullable_array && !is_nullable_target) {
                return _process_generic<true, false>(array_data_column, target_data_column, array_null_data,
                                                     target_null_data, is_const_array, is_const_target);
            } else if (!is_nullable_array && is_nullable_target) {
                return _process_generic<false, true>(array_data_column, target_data_column, array_null_data,
                                                     target_null_data, is_const_array, is_const_target);
            } else {
                return _process_generic<false, false>(array_data_column, target_data_column, array_null_data,
                                                      target_null_data, is_const_array, is_const_target);
            }
        };

        ASSIGN_OR_RETURN(ColumnPtr result_column, process_func());

        // wrap nullable and const column for result
        if (is_nullable_array) {
            result_column = NullableColumn::create(std::move(result_column), array_null_column->clone());
            result_column->check_or_die();
        }
        if (is_const_array && is_const_target) {
            result_column = ConstColumn::create(std::move(result_column), array_column->size());
            result_column->check_or_die();
        }
        return result_column;
    }

private:
    static constexpr bool is_supported(LogicalType type) {
        return is_scalar_logical_type(type) || type == TYPE_ARRAY || type == TYPE_MAP || type == TYPE_STRUCT;
    }

    static void _build_hash_table(const ColumnPtr& column, ArrayContainsState* state) {
        DCHECK(!column->is_constant() && !column->is_nullable());
        const ArrayColumn* array_column = down_cast<const ArrayColumn*>(column.get());

        const auto& elements_column =
                down_cast<const NullableColumn*>(array_column->elements_column().get())->data_column();
        const auto& null_column =
                down_cast<const NullableColumn*>(array_column->elements_column().get())->null_column();
        const auto& offsets_column = array_column->offsets_column();

        const CppType* elements_data = reinterpret_cast<const CppType*>(elements_column->raw_data());
        const NullColumn::ValueType* null_data = null_column->raw_data();
        const UInt32Column::ValueType* offsets_data = offsets_column->get_data().data();
        // column may be null
        size_t offset = offsets_data[0];
        size_t array_size = offsets_data[1] - offset;

        for (size_t i = 0; i < array_size; i++) {
            if (null_data[offset + i]) {
                if (state->first_null_position == 0) {
                    state->first_null_position = i + 1;
                }
                continue;
            }
            const auto& value = elements_data[offset + i];
            if (!state->hash_map.contains(value)) {
                state->hash_map[value] = i + 1;
            }
        }
    }

    template <bool NullableTarget>
    static ColumnPtr _process_with_hash_table(ArrayContainsState* state, const ColumnPtr& targets,
                                              const NullColumn::ValueType* targets_null_data, bool is_const_target) {
        DCHECK(!targets->is_constant() && !targets->is_nullable()) << "targets should be real data column";

        size_t num_rows = targets->size();
        auto result_column = ReturnType::create();
        // if target is const column, we only compute once
        result_column->resize(is_const_target ? 1 : num_rows);
        size_t result_size = result_column->size();

        const CppType* target_data = reinterpret_cast<const CppType*>(targets->raw_data());
        auto* result_data = result_column->get_data().data();

        for (size_t i = 0; i < result_size; i++) {
            if constexpr (NullableTarget) {
                if (targets_null_data[i]) {
                    result_data[i] = PositionEnabled ? state->first_null_position : (state->first_null_position != 0);
                    continue;
                }
            }
            const CppType& value = target_data[i];
            auto iter = state->hash_map.find(value);
            if (iter != state->hash_map.end()) {
                result_data[i] = PositionEnabled ? iter->second : 1;
            } else {
                result_data[i] = 0;
            }
        }
        return result_column;
    }

    template <bool NullableArray, bool NullableTarget>
    static ColumnPtr _process_generic(const ColumnPtr& arrays, const ColumnPtr& targets,
                                      const NullColumn::ValueType* arrays_null_data,
                                      const NullColumn::ValueType* targets_null_data, bool is_const_array,
                                      bool is_const_target) {
        DCHECK(!arrays->is_constant() && !arrays->is_nullable() && arrays->is_array());
        DCHECK(!targets->is_nullable() && !targets->is_constant());
        if (!is_const_array && !is_const_target) {
            DCHECK_EQ(arrays->size(), targets->size());
        }

        const auto& elements_column = down_cast<const ArrayColumn*>(arrays.get())->elements_column();
        const auto& elements = down_cast<const NullableColumn*>(elements_column.get())->data_column();
        const CppType* elements_data = reinterpret_cast<const CppType*>(elements->raw_data());
        const NullColumn::ValueType* elements_null_data =
                down_cast<const NullableColumn*>(elements_column.get())->null_column()->get_data().data();

        const auto& offsets_column = down_cast<const ArrayColumn*>(arrays.get())->offsets_column();
        const auto& offsets_data = offsets_column->get_data();

        const CppType* targets_data = reinterpret_cast<const CppType*>(targets->raw_data());

        // if both two columns are constant, we only compute the first row once
        size_t num_rows = (is_const_array && is_const_target) ? 1 : std::max(arrays->size(), targets->size());
        auto result_column = ReturnType::create();
        result_column->resize(num_rows);
        auto* result_data = result_column->get_data().data();

        for (size_t i = 0; i < num_rows; i++) {
            if constexpr (NullableArray) {
                bool is_array_null = is_const_array ? arrays_null_data[0] : arrays_null_data[i];
                if (is_array_null) {
                    // if array is null, result will be null
                    result_data[i] = 0;
                    continue;
                }
            }
            size_t offset = is_const_array ? offsets_data[0] : offsets_data[i];
            size_t array_size =
                    is_const_array ? offsets_data[1] - offsets_data[0] : offsets_data[i + 1] - offsets_data[i];

            size_t position = 0;
            if constexpr (NullableTarget) {
                bool is_target_null = is_const_target ? targets_null_data[0] : targets_null_data[i];
                if (is_target_null) {
                    // if target is null, we should try to find null in array
                    for (size_t j = 0; j < array_size; j++) {
                        if (elements_null_data[offset + j]) {
                            position = j + 1;
                            break;
                        }
                    }
                    result_data[i] = PositionEnabled ? position : (position != 0);
                    continue;
                }
            }

            // check non-null value one by one
            size_t target_idx = is_const_target ? 0 : i;
            for (size_t j = 0; j < array_size; j++) {
                if (!elements_null_data[offset + j] && elements_data[offset + j] == targets_data[target_idx]) {
                    position = j + 1;
                    break;
                }
            }
            result_data[i] = PositionEnabled ? position : (position != 0);
        }
        return result_column;
    }
};

// Implementation of array_contains_all and array_contains_seq
// for array_contains_all, we build  hash table to speed up the search.
// for array_contains_seq, we use the idea of ​​KMP algorithm to speed up the search.
template <LogicalType LT, bool ContainsSeq>
class ArrayContainsAll {
    using CppType = RunTimeCppType<LT>;
    using ColumnType = RunTimeColumnType<LT>;
    using HashFunc = PhmapDefaultHashFunc<LT, PhmapSeed1>;
    using HashMap = phmap::flat_hash_map<CppType, size_t, HashFunc>;
    using PrefixTable = std::vector<size_t>;

    struct ArrayContainsAllState {
        bool has_null = false;
        // final result, only used when both two inputs are constant
        bool contains = false;
        std::variant<HashMap, PrefixTable> variant;
    };

public:
    static Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        if (scope != FunctionContext::FRAGMENT_LOCAL) {
            return Status::OK();
        }

        if constexpr (!HashFunc::is_supported()) {
            return Status::OK();
        }

        bool is_left_notnull_const = context->is_notnull_constant_column(0);
        bool is_right_notnull_const = context->is_notnull_constant_column(1);

        if constexpr (ContainsSeq) {
            if (!is_right_notnull_const) {
                return Status::OK();
            }
        } else {
            if (!is_left_notnull_const && !is_right_notnull_const) {
                return Status::OK();
            }
        }

        auto* state = new ArrayContainsAllState();
        context->set_function_state(scope, state);

        ColumnPtr column;
        if constexpr (ContainsSeq) {
            column = context->get_constant_column(1);
        } else {
            // for array_contains_all, prefer to use the left column to build hash table
            column = is_left_notnull_const ? context->get_constant_column(0) : context->get_constant_column(1);
        }
        ColumnPtr array_column = FunctionHelper::get_data_column_of_const(column);
        const auto& [offsets_column, elements_column, null_column] = ColumnHelper::unpack_array_column(array_column);
        const CppType* elements_data = reinterpret_cast<const CppType*>(elements_column->raw_data());
        const NullColumn::ValueType* null_data = null_column->raw_data();
        const UInt32Column::ValueType* offsets_data = offsets_column->get_data().data();
        size_t offset = offsets_data[0];
        size_t array_size = offsets_data[1] - offset;

        if constexpr (ContainsSeq) {
            state->variant = PrefixTable{};
            _build_prefix_table(elements_data, null_data, offset, array_size, state);
        } else {
            state->variant = HashMap{};
            _build_hash_table(elements_data, null_data, offset, array_size, state);
        }

        if (is_left_notnull_const && is_right_notnull_const) {
            // if both inputs are constant, we just compute result directly
            ColumnPtr target_column = ContainsSeq ? context->get_constant_column(0) : context->get_constant_column(1);
            const auto& [target_offsets_column, target_elements_column, target_null_column] =
                    ColumnHelper::unpack_array_column(FunctionHelper::get_data_column_of_const(target_column));

            const CppType* target_elements_data = reinterpret_cast<const CppType*>(target_elements_column->raw_data());
            const NullColumn::ValueType* target_elements_null_data = target_null_column->raw_data();
            const UInt32Column::ValueType* target_offsets_data = target_offsets_column->get_data().data();

            size_t target_offset = target_offsets_data[0];
            size_t target_array_size = target_offsets_data[1] - offset;
            if constexpr (ContainsSeq) {
                state->contains = _process_with_prefix_table(state, target_elements_data, elements_data,
                                                             target_elements_null_data, null_data, target_offset,
                                                             target_array_size, offset, array_size);
            } else {
                state->contains = _process_with_hash_table<true>(state, target_elements_data, target_elements_null_data,
                                                                 target_offset, target_array_size);
            }
        }

        return Status::OK();
    }

    static Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            auto* state = reinterpret_cast<ArrayContainsAllState*>(
                    context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
            delete state;
        }
        return Status::OK();
    }

    static StatusOr<ColumnPtr> process(FunctionContext* context, const Columns& columns) {
        if constexpr (!is_supported(LT)) {
            return Status::NotSupported(fmt::format("not support type {}", LT));
        }
        RETURN_IF_COLUMNS_ONLY_NULL(columns);

        const ColumnPtr& left_column = columns[0];
        const ColumnPtr& right_column = columns[1];
        bool is_const_left = left_column->is_constant();
        bool is_const_right = right_column->is_constant();
        [[maybe_unused]] auto* state =
                reinterpret_cast<ArrayContainsAllState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

        if (is_const_left && is_const_right) {
            // if both input columns are constant, return result directly
            auto result_column = BooleanColumn::create();
            result_column->append(state->contains);
            return ConstColumn::create(std::move(result_column), columns[0]->size());
        }

        NullColumnPtr result_null_column = nullptr;

        ColumnPtr left_data_column = FunctionHelper::get_data_column_of_const(left_column);
        bool is_nullable_left = left_data_column->is_nullable();
        const NullColumn::ValueType* left_null_data = nullptr;
        if (is_nullable_left) {
            left_null_data = down_cast<NullableColumn*>(left_data_column.get())->null_column_data().data();
            left_data_column = down_cast<NullableColumn*>(left_data_column.get())->data_column();
        }

        ColumnPtr right_data_column = FunctionHelper::get_data_column_of_const(right_column);
        const NullColumn::ValueType* right_null_data = nullptr;
        bool is_nullable_right = right_data_column->is_nullable();
        if (is_nullable_right) {
            right_null_data = down_cast<NullableColumn*>(right_data_column.get())->null_column_data().data();
            right_data_column = down_cast<NullableColumn*>(right_data_column.get())->data_column();
        }

        if (is_nullable_left && is_nullable_right) {
            return _process<true, true>(state, left_data_column, right_data_column, left_null_data, right_null_data,
                                        is_const_left, is_const_right);
        } else if (is_nullable_left && !is_nullable_right) {
            return _process<true, false>(state, left_data_column, right_data_column, left_null_data, right_null_data,
                                         is_const_left, is_const_right);
        } else if (!is_nullable_left && is_nullable_right) {
            return _process<false, true>(state, left_data_column, right_data_column, left_null_data, right_null_data,
                                         is_const_left, is_const_right);
        } else {
            return _process<false, false>(state, left_data_column, right_data_column, left_null_data, right_null_data,
                                          is_const_left, is_const_right);
        }
    }

private:
    static constexpr bool is_supported(LogicalType type) { return is_scalar_logical_type(type); }

    static void _build_hash_table(const CppType* elements_data, const NullColumn::ValueType* elements_null_data,
                                  size_t offset, size_t array_size, ArrayContainsAllState* state) {
        HashMap* hash_map = std::get_if<HashMap>(&(state->variant));
        DCHECK(hash_map != nullptr);

        size_t count = 0;
        for (size_t i = 0; i < array_size; i++) {
            if (elements_null_data[offset + i]) {
                state->has_null = true;
                continue;
            }
            const auto& value = elements_data[offset + i];
            if (!hash_map->contains(value)) {
                hash_map->insert({value, count++});
            }
        }
    }

    template <bool HTFromLeft>
    static bool _process_with_hash_table(const ArrayContainsAllState* state, const CppType* elements_data,
                                         const NullColumn::ValueType* elements_null_data, size_t offset,
                                         size_t array_size) {
        const HashMap* hash_map = std::get_if<HashMap>(&(state->variant));
        DCHECK(hash_map != nullptr);
        // for array_contains_all(left, right), hash table may be built from the left or the right.
        // if ht comes from left side, all the data of right side must be found in ht.
        // if ht comes from right side, all the data in ht must be appeared in the left side.

        if (hash_map->empty()) {
            if (state->has_null) {
                size_t null_elements_num = SIMD::count_nonzero(elements_null_data + offset, array_size);
                return HTFromLeft ? null_elements_num == array_size : null_elements_num > 0;
            } else {
                return HTFromLeft ? array_size == 0 : true;
            }
        }

        if constexpr (HTFromLeft) {
            for (size_t i = 0; i < array_size; i++) {
                if (elements_null_data[i + offset]) {
                    if (!state->has_null) {
                        return false;
                    }
                    continue;
                }
                const auto& value = elements_data[i + offset];
                if (!hash_map->contains(value)) {
                    return false;
                }
            }
        } else {
            BitMask bit_mask(hash_map->size());
            size_t find_count = 0;
            bool has_null = false;
            for (size_t i = 0; i < array_size; i++) {
                if (elements_null_data[i + offset]) {
                    has_null = true;
                    continue;
                }
                const auto& value = elements_data[i + offset];
                auto iter = hash_map->find(value);
                if (iter != hash_map->end()) {
                    size_t idx = iter->second;
                    find_count += bit_mask.try_set_bit(idx);
                }
            }
            if (!(has_null == state->has_null && find_count == hash_map->size())) {
                return false;
            }
        }

        return true;
    }

    static inline bool _check_element_equal(const CppType* left_data, const NullColumn::ValueType* left_null_data,
                                            const CppType* right_data, const NullColumn::ValueType* right_null_data,
                                            size_t lhs, size_t rhs) {
        bool is_lhs_null = left_null_data[lhs];
        bool is_rhs_null = right_null_data[rhs];
        if (is_lhs_null ^ is_rhs_null) {
            return false;
        }
        if (is_lhs_null & is_rhs_null) {
            return true;
        }
        return left_data[lhs] == right_data[rhs];
    }

    static void _build_prefix_table(const CppType* elements_data, const NullColumn::ValueType* null_data, size_t offset,
                                    size_t array_size, ArrayContainsAllState* state) {
        if (array_size == 0) {
            return;
        }
        PrefixTable* prefix_table = std::get_if<PrefixTable>(&(state->variant));
        DCHECK(prefix_table != nullptr);
        prefix_table->resize(array_size);

        (*prefix_table)[0] = 0;
        size_t length = 0;
        size_t idx = 1;
        while (idx < array_size) {
            if (_check_element_equal(elements_data, null_data, elements_data, null_data, offset + idx,
                                     offset + length)) {
                length++;
                (*prefix_table)[idx] = length;
                idx++;
            } else {
                if (length != 0) {
                    length = (*prefix_table)[length - 1];
                } else {
                    (*prefix_table)[idx] = 0;
                    idx++;
                }
            }
        }
    }

    static bool _process_with_prefix_table(const ArrayContainsAllState* state, const CppType* left_elements_data,
                                           const CppType* right_elements_data,
                                           const NullColumn::ValueType* left_elements_null_data,
                                           const NullColumn::ValueType* right_elements_null_data, size_t left_offset,
                                           size_t left_array_size, size_t right_offset, size_t right_array_size) {
        if (right_array_size == 0) {
            return true;
        }
        if (right_array_size > left_array_size) {
            return false;
        }
        const PrefixTable* prefix_table = std::get_if<PrefixTable>(&(state->variant));
        DCHECK(prefix_table != nullptr && !prefix_table->empty());
        DCHECK_EQ(prefix_table->size(), right_array_size);

        size_t left_idx = 0;
        size_t right_idx = 0;
        while (left_idx < left_array_size) {
            bool is_equal =
                    _check_element_equal(left_elements_data, left_elements_null_data, right_elements_data,
                                         right_elements_null_data, left_offset + left_idx, right_offset + right_idx);
            if (is_equal) {
                left_idx++;
                right_idx++;
            }
            if (right_idx == right_array_size) {
                return true;
            } else if (left_idx < left_array_size &&
                       !_check_element_equal(left_elements_data, left_elements_null_data, right_elements_data,
                                             right_elements_null_data, left_offset + left_idx,
                                             right_offset + right_idx)) {
                if (right_idx != 0) {
                    right_idx = (*prefix_table)[right_idx - 1];
                } else {
                    left_idx++;
                }
            }
        }
        return false;
    }

    template <bool NullableLeft, bool NullableRight>
    static ColumnPtr _process(const ArrayContainsAllState* state, const ColumnPtr& left_arrays,
                              const ColumnPtr& right_arrays, const NullColumn::ValueType* left_null_data,
                              const NullColumn::ValueType* right_null_data, bool is_const_left, bool is_const_right) {
        DCHECK(!left_arrays->is_constant() && !left_arrays->is_nullable() && left_arrays->is_array());
        DCHECK(!right_arrays->is_constant() && !right_arrays->is_nullable() && right_arrays->is_array());
        if (!is_const_left && !is_const_right) {
            DCHECK_EQ(left_arrays->size(), right_arrays->size());
        }

        const auto& [left_offsets_column, left_elements_column, left_elements_null_column] =
                ColumnHelper::unpack_array_column(left_arrays);
        const CppType* left_elements_data = reinterpret_cast<const CppType*>(left_elements_column->raw_data());
        const NullColumn::ValueType* left_elements_null_data = left_elements_null_column->get_data().data();
        const auto* left_offsets_data = left_offsets_column->get_data().data();

        const auto& [right_offsets_column, right_elements_column, right_elements_null_column] =
                ColumnHelper::unpack_array_column(right_arrays);
        const CppType* right_elements_data = reinterpret_cast<const CppType*>(right_elements_column->raw_data());
        const NullColumn::ValueType* right_elements_null_data = right_elements_null_column->get_data().data();
        const auto* right_offsets_data = right_offsets_column->get_data().data();

        size_t num_rows = (is_const_left && is_const_right) ? 1 : std::max(left_arrays->size(), right_arrays->size());

        auto result_column = BooleanColumn::create();
        result_column->resize(num_rows);
        auto* result_data = result_column->get_data().data();

        [[maybe_unused]] NullColumnPtr result_null_column;
        [[maybe_unused]] NullColumn::ValueType* result_null_data = nullptr;
        if constexpr (NullableLeft || NullableRight) {
            result_null_column = NullColumn::create();
            result_null_column->resize(num_rows);
            result_null_data = result_null_column->get_data().data();
        }

        for (size_t i = 0; i < num_rows; i++) {
            if constexpr (NullableLeft) {
                bool is_array_null = is_const_left ? left_null_data[0] : left_null_data[i];
                if (is_array_null) {
                    result_data[i] = false;
                    result_null_data[i] = 1;
                    continue;
                }
            }
            if constexpr (NullableRight) {
                bool is_array_null = is_const_right ? right_null_data[0] : right_null_data[i];
                if (is_array_null) {
                    result_data[i] = false;
                    result_null_data[i] = 1;
                    continue;
                }
            }

            size_t left_array_offset = is_const_left ? left_offsets_data[0] : left_offsets_data[i];
            size_t left_array_size = is_const_left ? left_offsets_data[1] - left_offsets_data[0]
                                                   : left_offsets_data[i + 1] - left_offsets_data[i];
            size_t left_null_element_num =
                    NullableLeft ? 0
                                 : SIMD::count_nonzero(left_elements_null_data + left_array_offset, left_array_size);
            size_t left_not_null_element_num = left_array_size - left_null_element_num;

            size_t right_array_offset = is_const_right ? right_offsets_data[0] : right_offsets_data[i];
            size_t right_array_size = is_const_right ? right_offsets_data[1] - right_offsets_data[0]
                                                     : right_offsets_data[i + 1] - right_offsets_data[i];
            size_t right_null_element_num =
                    NullableRight
                            ? 0
                            : SIMD::count_nonzero(right_elements_null_data + right_array_offset, right_array_size);
            size_t right_not_null_element_num = right_array_size - right_null_element_num;

            [[maybe_unused]] const ArrayContainsAllState* state_ref = nullptr;
            [[maybe_unused]] ArrayContainsAllState tmp_state;
            if constexpr (ContainsSeq) {
                if (is_const_right) {
                    state_ref = state;
                } else {
                    tmp_state.variant = PrefixTable{};
                    _build_prefix_table(right_elements_data, right_elements_null_data, right_array_offset,
                                        right_array_size, &tmp_state);
                    state_ref = &tmp_state;
                }
                result_data[i] =
                        _process_with_prefix_table(state_ref, left_elements_data, right_elements_data,
                                                   left_elements_null_data, right_elements_null_data, left_array_offset,
                                                   left_array_size, right_array_offset, right_array_size);
            } else {
                bool build_from_left;

                if (is_const_left || is_const_right) {
                    state_ref = state;
                    build_from_left = is_const_left;
                } else {
                    tmp_state.variant = HashMap{};
                    // we build hash table on the side with less elements
                    build_from_left = left_not_null_element_num <= right_not_null_element_num;
                    const CppType* build_elements_data = build_from_left ? left_elements_data : right_elements_data;
                    const NullColumn::ValueType* build_elements_null_data =
                            build_from_left ? left_elements_null_data : right_elements_null_data;
                    size_t build_array_offset = build_from_left ? left_array_offset : right_array_offset;
                    size_t build_array_size = build_from_left ? left_array_size : right_array_size;

                    _build_hash_table(build_elements_data, build_elements_null_data, build_array_offset,
                                      build_array_size, &tmp_state);
                    state_ref = &tmp_state;
                }

                const CppType* probe_elements_data = !build_from_left ? left_elements_data : right_elements_data;
                const NullColumn::ValueType* probe_elements_null_data =
                        !build_from_left ? left_elements_null_data : right_elements_null_data;
                size_t probe_array_offset = !build_from_left ? left_array_offset : right_array_offset;
                size_t probe_array_size = !build_from_left ? left_array_size : right_array_size;

                result_data[i] = build_from_left ? _process_with_hash_table<true>(state_ref, probe_elements_data,
                                                                                  probe_elements_null_data,
                                                                                  probe_array_offset, probe_array_size)
                                                 : _process_with_hash_table<false>(
                                                           state_ref, probe_elements_data, probe_elements_null_data,
                                                           probe_array_offset, probe_array_size);
            }

            if constexpr (NullableLeft || NullableRight) {
                result_null_data[i] = 0;
            }
        }
        if constexpr (NullableLeft || NullableRight) {
            return NullableColumn::create(std::move(result_column), std::move(result_null_column));
        }
        return result_column;
    }
};

} // namespace starrocks
