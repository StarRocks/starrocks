// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "column/array_column.h"
#include "column/column_hash.h"
#include "exprs/vectorized/function_helper.h"
#include "util/orlp/pdqsort.h"

namespace starrocks::vectorized {
template <PrimitiveType PT>
class ArrayDistinct {
public:
    using CppType = RunTimeCppType<PT>;

    static ColumnPtr process(FunctionContext* ctx, const Columns& columns) {
        if constexpr (pt_is_largeint<PT>) {
            return _array_distinct<phmap::flat_hash_set<CppType, Hash128WithSeed<PhmapSeed1>>>(columns);
        } else if constexpr (pt_is_fixedlength<PT>) {
            return _array_distinct<phmap::flat_hash_set<CppType, StdHash<CppType>>>(columns);
        } else if constexpr (pt_is_binary<PT>) {
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
class ArraySort {
public:
    using ColumnType = RunTimeColumnType<PT>;

    static ColumnPtr process(FunctionContext* ctx, const Columns& columns) {
        DCHECK_EQ(columns.size(), 1);

        size_t chunk_size = columns[0]->size();

        if (columns[0]->only_null()) {
            return ColumnHelper::create_const_null_column(chunk_size);
        }

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
                dest_null_column->get_data() = src_null_column.get_data();
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

private:
    static void _sort_column(std::vector<uint32_t>* sort_index, const Column& src_column, size_t offset, size_t count) {
        const auto& data = down_cast<const ColumnType&>(src_column).get_data();

        auto less_fn = [&data](uint32_t l, uint32_t r) -> bool { return data[l] < data[r]; };
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

        _sort_column(sort_index, src_column, start, count);
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
        _sort_column(sort_index, src_data_column, start + null_count, count - null_count);
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

    static void _reverse_data_column(Column* column, const Buffer<uint32_t>& offsets, size_t chunk_size) {
        if constexpr (pt_is_fixedlength<PT>) {
            _reverse_fixed_column(column, offsets, chunk_size);
        } else if constexpr (pt_is_binary<PT>) {
            _reverse_binary_column(column, offsets, chunk_size);
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
        DCHECK_GE(columns.size(), 2);

        size_t chunk_size = columns[0]->size();

        if (columns[0]->only_null()) {
            return ColumnHelper::create_const_null_column(chunk_size);
        }

        ColumnPtr src_column = ColumnHelper::unpack_and_duplicate_const_column(chunk_size, columns[0]);
        Slice sep = columns[1]->get(0).get_slice();
        Slice null_replace_str;
        bool ignore_null = true;

        if (columns.size() > 2) {
            null_replace_str = columns[2]->get(0).get_slice();
            ignore_null = false;
        }

        if (src_column->is_nullable()) {
            auto dest_column = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
            auto* dest_binary_column = down_cast<NullableColumn*>(dest_column.get())->data_column().get();
            dest_column->null_column_data() =
                    down_cast<const NullableColumn*>(src_column.get())->immutable_null_column_data();
            dest_column->set_has_null(down_cast<const NullableColumn*>(src_column.get())->has_null());

            if (ignore_null) {
                _join_array_column<true>(dest_binary_column,
                                         *down_cast<NullableColumn*>(src_column.get())->data_column(), sep,
                                         null_replace_str, chunk_size);
            } else {
                _join_array_column<false>(dest_binary_column,
                                          *down_cast<NullableColumn*>(src_column.get())->data_column(), sep,
                                          null_replace_str, chunk_size);
            }
            return dest_column;
        } else {
            auto dest_column = BinaryColumn::create();
            if (ignore_null) {
                _join_array_column<true>(dest_column.get(), *src_column, sep, null_replace_str, chunk_size);
            } else {
                _join_array_column<false>(dest_column.get(), *src_column, sep, null_replace_str, chunk_size);
            }
            return dest_column;
        }
    }

private:
    static void _join_binary_column(Column* dest_column, const Column& src_column, const Slice& sep,
                                    const Buffer<uint32_t>& offsets, size_t chunk_size) {
        const auto& src_binary_column = down_cast<const BinaryColumn&>(src_column);

        auto* dest_binary_column = down_cast<BinaryColumn*>(dest_column);
        auto& dest_offsets = dest_binary_column->get_offset();
        auto& dest_bytes = dest_binary_column->get_bytes();

        dest_offsets.resize(chunk_size + 1);
        dest_bytes.resize(src_column.byte_size() + sep.size * src_binary_column.size());
        uint8_t* dest_ptr = dest_bytes.data();

        size_t start = 0;
        for (size_t i = 0; i < chunk_size; i++) {
            size_t start_offset = offsets[i];
            size_t end_offset = offsets[i + 1];
            bool append = false;
            for (size_t j = start_offset; j < end_offset; j++) {
                Slice slice = src_binary_column.get_slice(j);
                memcpy(dest_ptr + start, slice.data, slice.size);
                start += slice.size;

                memcpy(dest_ptr + start, sep.data, sep.size);
                start += sep.size;

                append = true;
            }
            // remove the trailing separator
            if (append) {
                start -= sep.size;
            }
            dest_offsets[i + 1] = start;
        }
        // size of dest_bytes may be larger then actual used
        dest_bytes.resize(start);
    }

    template <bool ignore_null>
    static void _join_binary_column_nullable(Column* dest_column, const Column& src_column, const Slice& sep,
                                             const Slice& null_replace_str, const Buffer<uint32_t>& offsets,
                                             size_t chunk_size) {
        const auto& src_nullable_column = down_cast<const NullableColumn&>(src_column);
        const auto& src_binary_column = down_cast<const BinaryColumn&>(src_nullable_column.data_column_ref());

        auto* dest_binary_column = down_cast<BinaryColumn*>(dest_column);
        auto& dest_offsets = dest_binary_column->get_offset();
        auto& dest_bytes = dest_binary_column->get_bytes();

        dest_offsets.resize(chunk_size + 1);
        if constexpr (ignore_null) {
            dest_bytes.resize(src_binary_column.byte_size() + sep.size * src_binary_column.size());
        } else {
            dest_bytes.resize(src_binary_column.byte_size() + sep.size * src_binary_column.size() +
                              null_replace_str.size * src_nullable_column.null_count());
        }
        uint8_t* dest_ptr = dest_bytes.data();

        size_t start = 0;

        for (size_t i = 0; i < chunk_size; i++) {
            size_t start_offset = offsets[i];
            size_t end_offset = offsets[i + 1];
            bool append = false;
            for (size_t j = start_offset; j < end_offset; j++) {
                if (!src_nullable_column.is_null(j)) {
                    Slice slice = src_binary_column.get_slice(j);
                    memcpy(dest_ptr + start, slice.data, slice.size);
                    start += slice.size;

                    memcpy(dest_ptr + start, sep.data, sep.size);
                    start += sep.size;
                    append = true;
                } else if constexpr (!ignore_null) {
                    memcpy(dest_ptr + start, null_replace_str.data, null_replace_str.size);
                    start += null_replace_str.size;

                    memcpy(dest_ptr + start, sep.data, sep.size);
                    start += sep.size;
                    append = true;
                }
            }

            // remove the trailing separator
            if (append) {
                start -= sep.size;
            }
            dest_offsets[i + 1] = start;
        }
        // size of dest_bytes may be larger then actual used
        dest_bytes.resize(start);
    }

    template <bool ignore_null>
    static void _join_array_column(Column* dest_column, const Column& src_column, const Slice& sep,
                                   const Slice& null_replace_str, size_t chunk_size) {
        const auto& src_array_column = down_cast<const ArrayColumn&>(src_column);
        const auto& src_offsets = src_array_column.offsets().get_data();
        const auto& src_element_column = src_array_column.elements();

        if (src_element_column.is_nullable()) {
            if (src_element_column.has_null()) {
                _join_binary_column_nullable<ignore_null>(dest_column, src_element_column, sep, null_replace_str,
                                                          src_offsets, chunk_size);
            } else {
                const auto& src_data_column = down_cast<const NullableColumn&>(src_element_column).data_column();
                _join_binary_column(dest_column, *src_data_column, sep, src_offsets, chunk_size);
            }
        } else {
            _join_binary_column(dest_column, src_element_column, sep, src_offsets, chunk_size);
        }
    }
};
} // namespace starrocks::vectorized
