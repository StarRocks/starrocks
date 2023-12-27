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

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_hash.h"
#include "column/column_viewer.h"
#include "column/json_column.h"
#include "column/type_traits.h"
#include "exprs/arithmetic_operation.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"
#include "runtime/current_thread.h"
#include "types/logical_type.h"
#include "util/orlp/pdqsort.h"
#include "util/phmap/phmap.h"

namespace starrocks {
template <LogicalType LT>
class ArrayDistinct {
public:
    using CppType = RunTimeCppType<LT>;

    static ColumnPtr process(FunctionContext* ctx, const Columns& columns) {
        RETURN_IF_COLUMNS_ONLY_NULL(columns);
        if constexpr (lt_is_largeint<LT>) {
            return _array_distinct<phmap::flat_hash_set<CppType, Hash128WithSeed<PhmapSeed1>>>(columns);
        } else if constexpr (lt_is_fixedlength<LT>) {
            return _array_distinct<phmap::flat_hash_set<CppType, StdHash<CppType>>>(columns);
        } else if constexpr (lt_is_string<LT>) {
            return _array_distinct<phmap::flat_hash_set<CppType, SliceHash>>(columns);
        } else {
            assert(false);
        }
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

        for (const auto& item : items) {
            if (item.is_null()) {
                has_null = true;
            } else {
                hash_set->emplace(item.get<CppType>());
            }
        }

        auto& dest_data_column = dest_column->elements_column();
        auto& dest_offsets = dest_column->offsets_column()->get_data();

        if (has_null) {
            dest_data_column->append_nulls(1);
        }

        auto iter = hash_set->begin();
        while (iter != hash_set->end()) {
            dest_data_column->append_datum(*iter);
            ++iter;
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
        } else if constexpr (lt_is_decimal32<LT> || lt_is_decimal64<LT>) {
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

template <LogicalType LT>
class ArrayOverlap {
public:
    using CppType = RunTimeCppType<LT>;

    static ColumnPtr process(FunctionContext* ctx, const Columns& columns) {
        RETURN_IF_COLUMNS_ONLY_NULL(columns);
        if constexpr (lt_is_largeint<LT> || lt_is_decimal128<LT>) {
            return _array_overlap<phmap::flat_hash_set<CppType, Hash128WithSeed<PhmapSeed1>>>(columns);
        } else if constexpr (lt_is_fixedlength<LT>) {
            return _array_overlap<phmap::flat_hash_set<CppType, StdHash<CppType>>>(columns);
        } else if constexpr (lt_is_string<LT>) {
            return _array_overlap<phmap::flat_hash_set<CppType, SliceHash>>(columns);
        } else {
            assert(false);
        }
    }

private:
    template <typename HashSet>
    static ColumnPtr _array_overlap(const Columns& original_columns) {
        size_t chunk_size = original_columns[0]->size();
        auto result_column = BooleanColumn::create(chunk_size, 0);
        Columns columns;
        for (const auto& col : original_columns) {
            columns.push_back(ColumnHelper::unpack_and_duplicate_const_column(chunk_size, col));
        }

        bool is_nullable = false;
        bool has_null = false;
        std::vector<ArrayColumn*> src_columns;
        src_columns.reserve(columns.size());
        NullColumnPtr null_result = NullColumn::create();
        null_result->resize(chunk_size);

        for (const auto& column : columns) {
            if (column->is_nullable()) {
                is_nullable = true;
                has_null = (column->has_null() || has_null);
                const auto* src_nullable_column = down_cast<const NullableColumn*>(column.get());
                src_columns.emplace_back(down_cast<ArrayColumn*>(src_nullable_column->data_column().get()));
                null_result = FunctionHelper::union_null_column(null_result, src_nullable_column->null_column());
            } else {
                src_columns.emplace_back(down_cast<ArrayColumn*>(column.get()));
            }
        }

        HashSet hash_set;
        for (size_t i = 0; i < chunk_size; i++) {
            _array_overlap_item<HashSet>(src_columns, i, &hash_set,
                                         static_cast<BooleanColumn*>(result_column.get())->get_data().data());
            hash_set.clear();
        }

        if (is_nullable) {
            return NullableColumn::create(result_column, null_result);
        }

        return result_column;
    }

    template <typename HashSet>
    static void _array_overlap_item(const std::vector<ArrayColumn*>& columns, size_t index, HashSet* hash_set,
                                    uint8_t* data) {
        bool has_null = false;

        {
            Datum v = columns[0]->get(index);
            const auto& items = v.get<DatumArray>();
            for (const auto& item : items) {
                if (item.is_null()) {
                    has_null = true;
                } else {
                    hash_set->emplace(item.get<CppType>());
                }
            }
        }

        {
            Datum v = columns[1]->get(index);
            const auto& items = v.get<DatumArray>();
            for (const auto& item : items) {
                if (item.is_null()) {
                    if (has_null) {
                        data[index] = 1;
                        return;
                    }
                } else {
                    auto iter = hash_set->find(item.get<CppType>());
                    if (iter != hash_set->end()) {
                        data[index] = 1;
                        return;
                    }
                }
            }

            data[index] = 0;
        }
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
            if constexpr (lt_is_largeint<LT>) {
                return phmap_mix_with_seed<sizeof(size_t), PhmapSeed1>()(hash_128(PhmapSeed1, cpp_type_value.value));
            } else if constexpr (lt_is_fixedlength<LT>) {
                return phmap_mix<sizeof(size_t)>()(std::hash<CppType>()(cpp_type_value.value));
            } else if constexpr (lt_is_string<LT>) {
                return crc_hash_64(cpp_type_value.value.data, static_cast<int32_t>(cpp_type_value.value.size),
                                   CRC_HASH_SEED1);
            } else {
                assert(false);
            }
        }
    };

    struct CppTypeWithOverlapTimesEqual {
        bool operator()(const CppTypeWithOverlapTimes& x, const CppTypeWithOverlapTimes& y) const {
            return x.value == y.value;
        }
    };

    static ColumnPtr process(FunctionContext* ctx, const Columns& columns) {
        if constexpr (lt_is_largeint<LT>) {
            return _array_intersect<phmap::flat_hash_set<CppTypeWithOverlapTimes, CppTypeWithOverlapTimesHash<LT>,
                                                         CppTypeWithOverlapTimesEqual>>(columns);
        } else if constexpr (lt_is_fixedlength<LT>) {
            return _array_intersect<phmap::flat_hash_set<CppTypeWithOverlapTimes, CppTypeWithOverlapTimesHash<LT>,
                                                         CppTypeWithOverlapTimesEqual>>(columns);
        } else if constexpr (lt_is_string<LT>) {
            return _array_intersect<phmap::flat_hash_set<CppTypeWithOverlapTimes, CppTypeWithOverlapTimesHash<LT>,
                                                         CppTypeWithOverlapTimesEqual>>(columns);
        } else {
            assert(false);
        }
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
        std::vector<ArrayColumn*> src_columns;
        src_columns.reserve(columns.size());
        NullColumnPtr null_result = NullColumn::create();
        null_result->resize(chunk_size);

        for (int i = 0; i < columns.size(); ++i) {
            if (columns[i]->is_nullable()) {
                is_nullable = true;
                has_null = (columns[i]->has_null() || has_null);
                null_index = i;

                const auto* src_nullable_column = down_cast<const NullableColumn*>(columns[i].get());
                src_columns.emplace_back(down_cast<ArrayColumn*>(src_nullable_column->data_column().get()));
                null_result = FunctionHelper::union_null_column(null_result, src_nullable_column->null_column());
            } else {
                src_columns.emplace_back(down_cast<ArrayColumn*>(columns[i].get()));
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
    static void _array_intersect_item(const std::vector<ArrayColumn*>& columns, size_t index, HashSet* hash_set,
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

        auto null_first_fn = [src_null_column](size_t i) -> bool { return src_null_column.get_data()[i] == 1; };

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
        // TODO: optimize the performace of const sep or const null replace str
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
        // byte_size may be smaller or larger then actual used size
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
        size_t chunk_size = columns[0]->size();
        ColumnPtr bool_column = ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[0]);

        auto dest_null_column = NullColumn::create(chunk_size, 0);
        auto dest_data_column = BooleanColumn::create(chunk_size);
        dest_null_column->get_data().resize(chunk_size, 0);

        ArrayColumn* bool_array;
        NullColumn* array_null_map = nullptr;

        if (bool_column->is_nullable()) {
            auto nullable_column = down_cast<NullableColumn*>(bool_column.get());
            bool_array = down_cast<ArrayColumn*>(nullable_column->data_column().get());
            array_null_map = nullable_column->null_column().get();
        } else {
            bool_array = down_cast<ArrayColumn*>(bool_column.get());
        }
        auto offsets = bool_array->offsets().get_data();

        ColumnViewer<TYPE_BOOLEAN> bool_elements(bool_array->elements_column());

        for (size_t i = 0; i < chunk_size; ++i) {
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

        return NullableColumn::create(dest_data_column, dest_null_column);
    }
};

// by design array_filter(array, bool_array), if bool_array is null, return an empty array. We do not return null, as
// it will change the null property of return results which keeps the same with the first argument array.
class ArrayFilter {
public:
    static ColumnPtr process([[maybe_unused]] FunctionContext* ctx, const Columns& columns) {
        return _array_filter(columns);
    }

private:
    static ColumnPtr _array_filter(const Columns& columns) {
        if (columns[0]->only_null()) {
            return columns[0];
        }

        size_t chunk_size = columns[0]->size();
        ColumnPtr src_column = ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[0]);
        ColumnPtr dest_column = src_column->clone_empty();
        if (columns[1]->only_null()) { // return empty array for non-null array by design, keep the same null with src.
            auto data_column = dest_column;
            if (dest_column->is_nullable()) {
                // set null from src
                auto* dest_nullable_column = down_cast<NullableColumn*>(dest_column.get());
                const auto* src_nullable_column = down_cast<const NullableColumn*>(src_column.get());
                dest_nullable_column->mutable_null_column()->get_data().assign(
                        src_nullable_column->null_column()->get_data().begin(),
                        src_nullable_column->null_column()->get_data().end());
                dest_nullable_column->set_has_null(src_nullable_column->has_null());

                data_column = dest_nullable_column->data_column();
            }
            data_column->append_default(chunk_size);
            return dest_column;
        }

        ColumnPtr bool_column = ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[1]);

        if (src_column->is_nullable()) {
            const auto* src_nullable_column = down_cast<const NullableColumn*>(src_column.get());
            const auto& src_data_column = src_nullable_column->data_column();
            const auto& src_null_column = src_nullable_column->null_column();

            auto* dest_nullable_column = down_cast<NullableColumn*>(dest_column.get());
            auto* dest_null_column = dest_nullable_column->mutable_null_column();
            auto* dest_data_column = dest_nullable_column->mutable_data_column();

            if (src_column->has_null()) {
                dest_null_column->get_data().assign(src_null_column->get_data().begin(),
                                                    src_null_column->get_data().end());
            } else {
                dest_null_column->get_data().resize(chunk_size, 0);
            }
            dest_nullable_column->set_has_null(src_nullable_column->has_null());

            _filter_array_items(down_cast<ArrayColumn*>(src_data_column.get()), bool_column,
                                down_cast<ArrayColumn*>(dest_data_column), dest_null_column);
        } else {
            _filter_array_items(down_cast<ArrayColumn*>(src_column.get()), bool_column,
                                down_cast<ArrayColumn*>(dest_column.get()), nullptr);
        }
        return dest_column;
    }

    static void _filter_array_items(const ArrayColumn* src_column, const ColumnPtr& raw_filter,
                                    ArrayColumn* dest_column, NullColumn* dest_null_map) {
        ArrayColumn* filter;
        NullColumn* filter_null_map = nullptr;
        auto& dest_offsets = dest_column->offsets_column()->get_data();

        if (raw_filter->is_nullable()) {
            auto nullable_column = down_cast<NullableColumn*>(raw_filter.get());
            filter = down_cast<ArrayColumn*>(nullable_column->data_column().get());
            filter_null_map = nullable_column->null_column().get();
        } else {
            filter = down_cast<ArrayColumn*>(raw_filter.get());
        }
        std::vector<uint32_t> indexes;
        // only keep the elements whose filter is not null and not 0.
        for (size_t i = 0; i < src_column->size(); ++i) {
            if (dest_null_map == nullptr || !dest_null_map->get_data()[i]) {         // dest_null_map[i] is not null
                if (filter_null_map == nullptr || !filter_null_map->get_data()[i]) { // filter_null_map[i] is not null
                    size_t elem_size = 0;
                    size_t filter_elem_id = filter->offsets().get_data()[i];
                    size_t filter_elem_limit = filter->offsets().get_data()[i + 1];
                    for (size_t src_elem_id = src_column->offsets().get_data()[i];
                         src_elem_id < src_column->offsets().get_data()[i + 1]; ++filter_elem_id, ++src_elem_id) {
                        // only keep the valid elements
                        if (filter_elem_id < filter_elem_limit && !filter->elements().is_null(filter_elem_id) &&
                            filter->elements().get(filter_elem_id).get_int8() != 0) {
                            indexes.emplace_back(src_elem_id);
                            ++elem_size;
                        }
                    }
                    dest_offsets.emplace_back(dest_offsets.back() + elem_size);
                } else { // filter_null_map[i] is null, empty the array by design[, alternatively keep all elements]
                    dest_offsets.emplace_back(dest_offsets.back());
                }
            } else { // dest_null_map[i] is null
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
        const auto& elements_data = GetContainer<ElementType>().get_data(elements);

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

        return NullableColumn::create(std::move(result), array_null);
    }
};

// Todo:support datatime/data
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
                auto nullable_column = down_cast<NullableColumn*>(column.get());
                if (nulls == nullptr) {
                    nulls = std::static_pointer_cast<NullColumn>(nullable_column->null_column()->clone_shared());
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

        size_t total_elements_num = 0;
        for (size_t cur_row = 0; cur_row < num_rows_to_process; cur_row++) {
            if (nulls && nulls->get_data()[cur_row]) {
                offsets->append(offsets->get_data().back());
                continue;
            }

            auto start = columns[0]->get(cur_row).get<InputCppType>();
            auto stop = columns[1]->get(cur_row).get<InputCppType>();
            auto step = columns[2]->get(cur_row).get<InputCppType>();

            // just return empty array
            if (step == 0) {
                offsets->append(offsets->get_data().back());
                continue;
            }

            InputCppType temp;
            for (InputCppType cur_element = start; step > 0 ? cur_element <= stop : cur_element >= stop;
                 cur_element += step) {
                TRY_CATCH_BAD_ALLOC(elements->append_numbers(&cur_element, sizeof(InputCppType)));
                total_elements_num++;
                if (__builtin_add_overflow(cur_element, step, &temp)) break;
            }
            offsets->append(total_elements_num);
        }

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

} // namespace starrocks
