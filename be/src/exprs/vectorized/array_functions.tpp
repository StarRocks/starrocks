// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "column/array_column.h"
#include "column/column_hash.h"
#include "exprs/vectorized/function_helper.h"

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
            iter++;
        }
        dest_offsets.emplace_back(dest_offsets.back() + hash_set->size() + has_null);
    }
};
} // namespace starrocks::vectorized