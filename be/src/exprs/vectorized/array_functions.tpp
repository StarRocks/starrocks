// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_hash.h"
#include "column/column_viewer.h"
#include "column/json_column.h"
#include "column/type_traits.h"
#include "exprs/vectorized/function_helper.h"
#include "runtime/primitive_type.h"
#include "udf/udf.h"
#include "util/orlp/pdqsort.h"
#include "util/phmap/phmap.h"

namespace starrocks::vectorized {
template <PrimitiveType PT>
class ArrayDistinct {
public:
    using CppType = RunTimeCppType<PT>;

    static ColumnPtr process(FunctionContext* ctx, const Columns& columns) {
        RETURN_IF_COLUMNS_ONLY_NULL(columns);
        if constexpr (pt_is_largeint<PT>) {
            return _array_distinct<phmap::flat_hash_set<CppType, Hash128WithSeed<PhmapSeed1>>>(columns);
        } else if constexpr (pt_is_fixedlength<PT>) {
            return _array_distinct<phmap::flat_hash_set<CppType, StdHash<CppType>>>(columns);
        } else if constexpr (pt_is_string<PT>) {
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

template <PrimitiveType PT>
class ArrayDifference {
public:
    using CppType = RunTimeCppType<PT>;

    static ColumnPtr process(FunctionContext* ctx, const Columns& columns) {
        RETURN_IF_COLUMNS_ONLY_NULL(columns);
        if constexpr (pt_is_arithmetic<PT> || pt_is_decimalv2<PT>) {
            return _array_difference(columns);
        } else {
            assert(false);
        }
    }

private:
    static ColumnPtr _array_difference(const Columns& columns) {
        DCHECK_EQ(columns.size(), 1);

        size_t chunk_size = columns[0]->size();
        ColumnPtr src_column = ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[0]);
        ColumnPtr dest_column_data = nullptr;
        ColumnPtr dest_column = nullptr;

        if constexpr (pt_is_float<PT>) {
            dest_column_data = NullableColumn::create(DoubleColumn::create(), NullColumn::create());
        } else if constexpr (pt_is_largeint<PT>) {
            dest_column_data = NullableColumn::create(RunTimeColumnType<TYPE_LARGEINT>::create(), NullColumn::create());
        } else if constexpr (pt_is_integer<PT> || pt_is_boolean<PT>) {
            dest_column_data = NullableColumn::create(Int64Column::create(), NullColumn::create());
        } else if constexpr (pt_is_decimalv2<PT>) {
            dest_column_data = NullableColumn::create(DecimalColumn::create(), NullColumn::create());
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
                        _array_difference_item(*src_data_column, i, &dest_data_column);
                    }
                }
            } else {
                for (size_t i = 0; i < chunk_size; i++) {
                    _array_difference_item(*src_data_column, i, &dest_data_column);
                }
            }
        } else {
            const auto* src_data_column = down_cast<const ArrayColumn*>(src_column.get());
            dest_column = ArrayColumn::create(dest_column_data, UInt32Column::create(src_data_column->offsets()));

            auto* dest_data_column = down_cast<ArrayColumn*>(dest_column.get());
            for (size_t i = 0; i < chunk_size; i++) {
                _array_difference_item(*src_data_column, i, dest_data_column);
            }
        }
        return dest_column;
    }

    static void _array_difference_item(const ArrayColumn& column, size_t index, ArrayColumn* dest_column) {
        Datum v = column.get(index);
        const auto& items = v.get<DatumArray>();

        auto& dest_data_column = dest_column->elements_column();
        for (size_t i = 0; i < items.size(); ++i) {
            if (i == 0) {
                if (items[i].is_null()) {
                    dest_data_column->append_nulls(1);
                } else {
                    if constexpr (pt_is_largeint<PT>) {
                        dest_data_column->append_datum((CppType)0);
                    } else if constexpr (pt_is_integer<PT> || pt_is_boolean<PT>) {
                        dest_data_column->append_datum((int64_t)0);
                    } else if constexpr (pt_is_float<PT>) {
                        dest_data_column->append_datum((double)0);
                    } else {
                        dest_data_column->append_datum((DecimalV2Value)0);
                    }
                }
            } else {
                if (items[i - 1].is_null() || items[i].is_null()) {
                    dest_data_column->append_nulls(1);
                } else {
                    if constexpr (pt_is_largeint<PT>) {
                        dest_data_column->append_datum(
                                (CppType)(items[i].get<CppType>() - items[i - 1].get<CppType>()));
                    } else if constexpr (pt_is_integer<PT> || pt_is_boolean<PT>) {
                        dest_data_column->append_datum(
                                (int64_t)(items[i].get<CppType>() - items[i - 1].get<CppType>()));
                    } else if constexpr (pt_is_float<PT>) {
                        dest_data_column->append_datum((double)(items[i].get<CppType>() - items[i - 1].get<CppType>()));
                    } else {
                        dest_data_column->append_datum(
                                (DecimalV2Value)(items[i].get<CppType>() - items[i - 1].get<CppType>()));
                    }
                }
            }
        }
    }
};

template <PrimitiveType PT>
class ArraySlice {
public:
    using CppType = RunTimeCppType<PT>;

    static ColumnPtr process(FunctionContext* ctx, const Columns& columns) { return _array_slice(columns); }

private:
    static ColumnPtr _array_slice(const Columns& columns) {
        RETURN_IF_COLUMNS_ONLY_NULL(columns);

        size_t chunk_size = columns[0]->size();
        ColumnPtr src_column = ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[0]);
        ColumnPtr dest_column = src_column->clone_empty();

        bool is_nullable = false;
        bool has_null = false;
        NullColumnPtr null_result = nullptr;

        ArrayColumn* array_column = nullptr;
        if (columns[0]->is_nullable()) {
            is_nullable = true;
            has_null = (columns[0]->has_null() || has_null);

            const auto* src_nullable_column = down_cast<const NullableColumn*>(columns[0].get());
            array_column = down_cast<ArrayColumn*>(src_nullable_column->data_column().get());
            null_result = NullColumn::create(*src_nullable_column->null_column());
        } else {
            array_column = down_cast<ArrayColumn*>(src_column.get());
        }

        Int64Column* offset_column = nullptr;
        if (columns[1]->is_nullable()) {
            is_nullable = true;
            has_null = (columns[1]->has_null() || has_null);

            const auto* src_nullable_column = down_cast<const NullableColumn*>(columns[1].get());
            offset_column = down_cast<Int64Column*>(src_nullable_column->data_column().get());
            if (null_result) {
                null_result = FunctionHelper::union_null_column(null_result, src_nullable_column->null_column());
            } else {
                null_result = NullColumn::create(*src_nullable_column->null_column());
            }
        } else {
            offset_column = down_cast<Int64Column*>(
                    ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[1]).get());
        }

        Int64Column* length_column = nullptr;
        // length_column is provided.
        if (columns.size() > 2) {
            if (columns[2]->is_nullable()) {
                is_nullable = true;
                has_null = (columns[2]->has_null() || has_null);

                const auto* src_nullable_column = down_cast<const NullableColumn*>(columns[2].get());
                length_column = down_cast<Int64Column*>(src_nullable_column->data_column().get());
                if (null_result) {
                    null_result = FunctionHelper::union_null_column(null_result, src_nullable_column->null_column());
                } else {
                    null_result = NullColumn::create(*src_nullable_column->null_column());
                }
            } else {
                length_column = down_cast<Int64Column*>(
                        ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[2]).get());
            }
        }

        ArrayColumn* dest_data_column = nullptr;
        if (columns[0]->is_nullable()) {
            auto& dest_nullable_column = down_cast<NullableColumn&>(*dest_column);
            dest_data_column = down_cast<ArrayColumn*>(dest_nullable_column.data_column().get());
            auto& dest_null_data = dest_nullable_column.null_column_data();

            dest_null_data = null_result->get_data();
            dest_nullable_column.set_has_null(has_null);
        } else {
            dest_data_column = down_cast<ArrayColumn*>(dest_column.get());
        }

        if (columns.size() > 2) {
            for (size_t i = 0; i < chunk_size; i++) {
                _array_slice_item<true>(array_column, i, dest_data_column, offset_column->get(i).get_int64(),
                                        length_column->get(i).get_int64());
            }
        } else {
            for (size_t i = 0; i < chunk_size; i++) {
                _array_slice_item<false>(array_column, i, dest_data_column, offset_column->get(i).get_int64(), 0);
            }
        }

        if (is_nullable) {
            if (columns[0]->is_nullable()) {
                return dest_column;
            } else {
                return NullableColumn::create(dest_column, null_result);
            }
        } else {
            return dest_column;
        }
    }

    template <bool with_length>
    static void _array_slice_item(ArrayColumn* column, size_t index, ArrayColumn* dest_column, int64_t offset,
                                  int64_t length) {
        auto& dest_offsets = dest_column->offsets_column()->get_data();
        if (!offset) {
            dest_offsets.emplace_back(dest_offsets.back());
            return;
        }

        Datum v = column->get(index);
        const auto& items = v.get<DatumArray>();

        if (offset > 0) {
            // because offset start with 1.
            --offset;
        } else {
            offset += items.size();
        }

        auto& dest_data_column = dest_column->elements_column();
        int64_t end;
        if constexpr (with_length) {
            end = std::max((int64_t)0, std::min((int64_t)items.size(), (offset + length)));
        } else {
            end = items.size();
        }
        offset = (offset > 0 ? offset : 0);
        for (size_t i = offset; i < end; ++i) {
            if (items[i].is_null()) {
                dest_data_column->append_nulls(1);
            } else {
                dest_data_column->append_datum(items[i]);
            }
        }

        // Protect when length < 0.
        auto offset_delta = ((end < offset) ? 0 : end - offset);
        dest_offsets.emplace_back(dest_offsets.back() + offset_delta);
    }
};

template <PrimitiveType PT>
class ArrayConcat {
public:
    using CppType = RunTimeCppType<PT>;

    static ColumnPtr process(FunctionContext* ctx, const Columns& columns) {
        RETURN_IF_COLUMNS_ONLY_NULL(columns);
        return _array_concat(columns);
    }

private:
    static void collect_array_columns_and_null_columns(const Columns& columns, std::vector<ArrayColumn*>* src_columns,
                                                       NullColumnPtr* null_result, bool* is_nullable, bool* has_null,
                                                       int* null_index) {
        for (int i = 0; i < columns.size(); ++i) {
            auto col = ColumnHelper::unpack_and_duplicate_const_column(columns[i]->size(), columns[i]);
            if (col->is_nullable()) {
                (*is_nullable) = true;
                (*has_null) = (col->has_null() || (*has_null));
                (*null_index) = i;

                const auto* src_nullable_column = down_cast<const NullableColumn*>(col.get());
                src_columns->emplace_back(down_cast<ArrayColumn*>(src_nullable_column->data_column().get()));
                if ((*null_result)) {
                    (*null_result) =
                            FunctionHelper::union_null_column((*null_result), src_nullable_column->null_column());
                } else {
                    (*null_result) = NullColumn::create(*src_nullable_column->null_column());
                }
            } else {
                src_columns->emplace_back(down_cast<ArrayColumn*>(col.get()));
            }
        }
    }

    static ColumnPtr _array_concat(const Columns& columns) {
        if (columns.size() == 1) {
            return columns[0];
        }

        size_t chunk_size = columns[0]->size();
        bool is_nullable = false;
        bool has_null = false;
        int null_index = 0;
        std::vector<ArrayColumn*> src_columns;
        src_columns.reserve(columns.size());
        NullColumnPtr null_result = nullptr;

        collect_array_columns_and_null_columns(columns, &src_columns, &null_result, &is_nullable, &has_null,
                                               &null_index);

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

        for (size_t i = 0; i < chunk_size; i++) {
            _array_concat_item(src_columns, i, dest_data_column);
        }

        return dest_column;
    }

    static void _array_concat_item(const std::vector<ArrayColumn*>& columns, size_t index, ArrayColumn* dest_column) {
        size_t num_rows = 0;
        for (size_t i = 0; i < columns.size(); ++i) {
            Datum v = columns[i]->get(index);
            const auto& items = v.get<DatumArray>();

            auto& dest_data_column = dest_column->elements_column();
            for (size_t i = 0; i < items.size(); ++i) {
                if (items[i].is_null()) {
                    dest_data_column->append_nulls(1);
                } else {
                    dest_data_column->append_datum(items[i]);
                }
            }

            num_rows += items.size();
        }

        auto& dest_offsets = dest_column->offsets_column()->get_data();
        dest_offsets.emplace_back(dest_offsets.back() + num_rows);
    }
};

template <PrimitiveType PT>
class ArrayOverlap {
public:
    using CppType = RunTimeCppType<PT>;

    static ColumnPtr process(FunctionContext* ctx, const Columns& columns) {
        RETURN_IF_COLUMNS_ONLY_NULL(columns);
        if constexpr (pt_is_largeint<PT>) {
            return _array_overlap<phmap::flat_hash_set<CppType, Hash128WithSeed<PhmapSeed1>>>(columns);
        } else if constexpr (pt_is_fixedlength<PT>) {
            return _array_overlap<phmap::flat_hash_set<CppType, StdHash<CppType>>>(columns);
        } else if constexpr (pt_is_string<PT>) {
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

        for (int i = 0; i < columns.size(); ++i) {
            if (columns[i]->is_nullable()) {
                is_nullable = true;
                has_null = (columns[i]->has_null() || has_null);
                const auto* src_nullable_column = down_cast<const NullableColumn*>(columns[i].get());
                src_columns.emplace_back(down_cast<ArrayColumn*>(src_nullable_column->data_column().get()));
                null_result = FunctionHelper::union_null_column(null_result, src_nullable_column->null_column());
            } else {
                src_columns.emplace_back(down_cast<ArrayColumn*>(columns[i].get()));
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

template <PrimitiveType PT>
class ArrayIntersect {
public:
    using CppType = RunTimeCppType<PT>;

    class CppTypeWithOverlapTimes {
    public:
        CppTypeWithOverlapTimes(const CppType& item, size_t n = 0) : value(item), overlap_times(n) {}

        CppType value;
        mutable size_t overlap_times;
    };

    template <PrimitiveType type>
    struct CppTypeWithOverlapTimesHash {
        std::size_t operator()(const CppTypeWithOverlapTimes& cpp_type_value) const {
            if constexpr (pt_is_largeint<PT>) {
                return phmap_mix_with_seed<sizeof(size_t), PhmapSeed1>()(hash_128(PhmapSeed1, cpp_type_value.value));
            } else if constexpr (pt_is_fixedlength<PT>) {
                return phmap_mix<sizeof(size_t)>()(std::hash<CppType>()(cpp_type_value.value));
            } else if constexpr (pt_is_string<PT>) {
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
        if constexpr (pt_is_largeint<PT>) {
            return _array_intersect<phmap::flat_hash_set<CppTypeWithOverlapTimes, CppTypeWithOverlapTimesHash<PT>,
                                                         CppTypeWithOverlapTimesEqual>>(columns);
        } else if constexpr (pt_is_fixedlength<PT>) {
            return _array_intersect<phmap::flat_hash_set<CppTypeWithOverlapTimes, CppTypeWithOverlapTimesHash<PT>,
                                                         CppTypeWithOverlapTimesEqual>>(columns);
        } else if constexpr (pt_is_string<PT>) {
            return _array_intersect<phmap::flat_hash_set<CppTypeWithOverlapTimes, CppTypeWithOverlapTimesHash<PT>,
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

template <PrimitiveType PT>
class ArraySort {
public:
    using ColumnType = RunTimeColumnType<PT>;

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
        pdqsort(false, sort_index->begin() + offset, sort_index->begin() + offset + count, less_fn);
    }

    // For JSON type
    static void _sort_column(std::vector<uint32_t>* sort_index, const JsonColumn& src_column, size_t offset,
                             size_t count) {
        auto less_fn = [&](uint32_t l, uint32_t r) -> bool { return src_column.compare_at(l, r, src_column, -1) < 0; };
        pdqsort(false, sort_index->begin() + offset, sort_index->begin() + offset + count, less_fn);
    }

    static void _sort_item(std::vector<uint32_t>* sort_index, const Column& src_column,
                           const UInt32Column& offset_column, size_t index) {
        const auto& offsets = offset_column.get_data();

        size_t start = offsets[index];
        size_t count = offsets[index + 1] - offsets[index];
        if (count <= 0) {
            return;
        }

        _sort_column(sort_index, down_cast<const RunTimeColumnType<PT>&>(src_column), start, count);
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
        _sort_column(sort_index, down_cast<const RunTimeColumnType<PT>&>(src_data_column), start + null_count,
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

template <PrimitiveType PT>
class ArrayReverse {
public:
    using ColumnType = RunTimeColumnType<PT>;
    using CppType = RunTimeCppType<PT>;

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
        if constexpr (pt_is_fixedlength<PT>) {
            _reverse_fixed_column(column, offsets, chunk_size);
        } else if constexpr (pt_is_string<PT>) {
            _reverse_binary_column(column, offsets, chunk_size);
        } else if constexpr (pt_is_json<PT>) {
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
            auto datum = src_column->get(i);
            const auto& datum_array = datum.get_array();
            bool append = false;
            Slice sep_slice = sep_column->get(i).get_slice();
            Slice null_slice = null_replace_column->get(i).get_slice();
            for (size_t j = 0; j < datum_array.size(); j++) {
                if (append) {
                    res.append_partial(sep_slice);
                }
                if (datum_array[j].is_null()) {
                    res.append_partial(null_slice);
                } else {
                    Slice value_slice = datum_array[j].get_slice();
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

            auto datum = src_column->get(i);
            const auto& datum_array = datum.get_array();
            bool append = false;
            Slice sep_slice = sep_column->get(i).get_slice();
            for (size_t j = 0; j < datum_array.size(); j++) {
                if (datum_array[j].is_null()) {
                    continue;
                }
                if (append) {
                    res.append_partial(sep_slice);
                }
                Slice value_slice = datum_array[j].get_slice();
                res.append_partial(value_slice);
                append = true;
            }
            res.append_complete(i);
        }

        return res.build_nullable_column();
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

    static void _filter_array_items(const ArrayColumn* src_column, const ColumnPtr raw_filter, ArrayColumn* dest_column,
                                    NullColumn* dest_null_map) {
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
template <PrimitiveType PT>
class ArraySortBy : public ArraySort<PT> {
public:
    using ColumnType = RunTimeColumnType<PT>;

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
                                   const ColumnPtr key_array_ptr, const NullColumn* src_null_map) {
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
        ArraySort<PT>::_init_sort_index(&key_sort_index, key_element_column.size());
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
                    ArraySort<PT>::_sort_nullable_item(&key_sort_index, key_data_column, null_column,
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
                    ArraySort<PT>::_sort_item(&key_sort_index, key_data_column, key_offsets_column, i);
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

} // namespace starrocks::vectorized
