// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/bitmap_functions.h"

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/vectorized/binary_function.h"
#include "exprs/vectorized/unary_function.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "util/string_parser.hpp"

namespace starrocks::vectorized {

template <PrimitiveType LT>
ColumnPtr BitmapFunctions::to_bitmap(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    ColumnViewer<PrimitiveType> viewer(columns[0]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_OBJECT> builder(size);
    for (int row = 0; row < size; ++row) {
        if (viewer.is_null(row)) {
            builder.append_null();
            continue;
        }

        uint64_t value;
        if constexpr (lt_is_integer<LT> || lt_is_boolean<LT>) {
            auto raw_value = viewer.value(row);
            // To be compatible with varchar type, set it null if raw value is less than 0 and less than uint64::max.
            if (UNLIKELY(raw_value < 0 || raw_value > std::numeric_limits<uint64_t>::max())) {
                context->set_error(strings::Substitute("The input: {0} is not valid, to_bitmap only "
                                                       "support bigint value from 0 to "
                                                       "18446744073709551615 currently",
                                                       raw_value)
                                           .c_str());

                builder.append_null();
                continue;
            }
            value = static_cast<uint64_t>(raw_value);
        } else {
            StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
            auto slice = viewer.value(row);
            value = StringParser::string_to_unsigned_int<uint64_t>(slice.data, slice.size, &parse_result);
            if (parse_result != StringParser::PARSE_SUCCESS) {
                context->set_error(strings::Substitute("The input: {0} is not valid, to_bitmap only "
                                                       "support bigint value from 0 to "
                                                       "18446744073709551615 currently",
                                                       slice.to_string())
                                           .c_str());
                builder.append_null();
                continue;
            }
        }

        BitmapValue bitmap;
        bitmap.add(value);

        builder.append(&bitmap);
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}
template StatusOr<ColumnPtr> BitmapFunctions::to_bitmap<TYPE_BOOLEAN>(FunctionContext* context,
                                                                      const starrocks::vectorized::Columns& columns);
template StatusOr<ColumnPtr> BitmapFunctions::to_bitmap<TYPE_TINYINT>(FunctionContext* context,
                                                                      const starrocks::vectorized::Columns& columns);
template StatusOr<ColumnPtr> BitmapFunctions::to_bitmap<TYPE_SMALLINT>(FunctionContext* context,
                                                                       const starrocks::vectorized::Columns& columns);
template StatusOr<ColumnPtr> BitmapFunctions::to_bitmap<TYPE_INT>(FunctionContext* context,
                                                                  const starrocks::vectorized::Columns& columns);
template StatusOr<ColumnPtr> BitmapFunctions::to_bitmap<TYPE_BIGINT>(FunctionContext* context,
                                                                     const starrocks::vectorized::Columns& columns);
template StatusOr<ColumnPtr> BitmapFunctions::to_bitmap<TYPE_LARGEINT>(FunctionContext* context,
                                                                       const starrocks::vectorized::Columns& columns);
template StatusOr<ColumnPtr> BitmapFunctions::to_bitmap<TYPE_VARCHAR>(FunctionContext* context,
                                                                      const starrocks::vectorized::Columns& columns);

ColumnPtr BitmapFunctions::bitmap_hash(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    ColumnViewer<TYPE_VARCHAR> viewer(columns[0]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_OBJECT> builder(size);
    for (int row = 0; row < size; ++row) {
        BitmapValue bitmap;

        if (!viewer.is_null(row)) {
            auto slice = viewer.value(row);
            uint32_t hash_value = HashUtil::murmur_hash3_32(slice.data, slice.size, HashUtil::MURMUR3_32_SEED);

            bitmap.add(hash_value);
        }

        builder.append(&bitmap);
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr BitmapFunctions::bitmap_count(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    ColumnViewer<TYPE_OBJECT> viewer(columns[0]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_BIGINT> builder(size);
    for (int row = 0; row < size; ++row) {
        int64_t value = viewer.is_null(row) ? 0 : viewer.value(row)->cardinality();
        builder.append(value);
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr BitmapFunctions::bitmap_empty(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    BitmapValue bitmap;
    return ColumnHelper::create_const_column<TYPE_OBJECT>(&bitmap, 1);
}

ColumnPtr BitmapFunctions::bitmap_or(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    ColumnViewer<TYPE_OBJECT> lhs(columns[0]);
    ColumnViewer<TYPE_OBJECT> rhs(columns[1]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_OBJECT> builder(size);
    for (int row = 0; row < size; ++row) {
        if (lhs.is_null(row) || rhs.is_null(row)) {
            builder.append_null();
            continue;
        }

        BitmapValue bitmap;
        bitmap |= (*lhs.value(row));
        bitmap |= (*rhs.value(row));

        builder.append(&bitmap);
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr BitmapFunctions::bitmap_and(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    ColumnViewer<TYPE_OBJECT> lhs(columns[0]);
    ColumnViewer<TYPE_OBJECT> rhs(columns[1]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_OBJECT> builder(size);
    for (int row = 0; row < size; ++row) {
        if (lhs.is_null(row) || rhs.is_null(row)) {
            builder.append_null();
            continue;
        }

        BitmapValue bitmap;
        bitmap |= (*lhs.value(row));
        bitmap &= (*rhs.value(row));

        builder.append(&bitmap);
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

// bitmap_to_string
DEFINE_STRING_UNARY_FN_WITH_IMPL(bitmapToStingImpl, bitmap_ptr) {
    return bitmap_ptr->to_string();
}

ColumnPtr BitmapFunctions::bitmap_to_string(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    return VectorizedStringStrictUnaryFunction<bitmapToStingImpl>::evaluate<TYPE_OBJECT, TYPE_VARCHAR>(columns[0]);
}

ColumnPtr BitmapFunctions::bitmap_from_string(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    ColumnViewer<TYPE_VARCHAR> viewer(columns[0]);
    std::vector<uint64_t> bits;

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_OBJECT> builder(size);
    for (int row = 0; row < size; ++row) {
        if (viewer.is_null(row)) {
            builder.append_null();
            continue;
        }

        auto slice = viewer.value(row);

        bits.clear();
        if (slice.size > INT32_MAX || !SplitStringAndParse({slice.data, (int)slice.size}, ",", &safe_strtou64, &bits)) {
            builder.append_null();
            continue;
        }

        BitmapValue bitmap(bits);
        builder.append(&bitmap);
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

// bitmap_contains
DEFINE_BINARY_FUNCTION_WITH_IMPL(bitmapContainsImpl, bitmap_ptr, int_value) {
    return bitmap_ptr->contains(int_value);
}

ColumnPtr BitmapFunctions::bitmap_contains(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    return VectorizedStrictBinaryFunction<bitmapContainsImpl>::evaluate<TYPE_OBJECT, TYPE_BIGINT, TYPE_BOOLEAN>(
            columns[0], columns[1]);
}

// bitmap_has_any
DEFINE_BINARY_FUNCTION_WITH_IMPL(bitmapHasAny, lhs, rhs) {
    BitmapValue bitmap;
    bitmap |= (*lhs);
    bitmap &= (*rhs);

    return bitmap.cardinality() != 0;
}

ColumnPtr BitmapFunctions::bitmap_has_any(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    return VectorizedStrictBinaryFunction<bitmapHasAny>::evaluate<TYPE_OBJECT, TYPE_BOOLEAN>(columns[0], columns[1]);
}

ColumnPtr BitmapFunctions::bitmap_andnot(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    ColumnViewer<TYPE_OBJECT> lhs(columns[0]);
    ColumnViewer<TYPE_OBJECT> rhs(columns[1]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_OBJECT> builder(size);
    for (int row = 0; row < size; ++row) {
        if (lhs.is_null(row) || rhs.is_null(row)) {
            builder.append_null();
            continue;
        }

        BitmapValue bitmap;
        bitmap |= (*lhs.value(row));
        bitmap -= (*rhs.value(row));

        builder.append(&bitmap);
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr BitmapFunctions::bitmap_xor(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    ColumnViewer<TYPE_OBJECT> lhs(columns[0]);
    ColumnViewer<TYPE_OBJECT> rhs(columns[1]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_OBJECT> builder(size);
    for (int row = 0; row < size; ++row) {
        if (lhs.is_null(row) || rhs.is_null(row)) {
            builder.append_null();
            continue;
        }

        BitmapValue bitmap;
        bitmap |= (*lhs.value(row));
        bitmap ^= (*rhs.value(row));

        builder.append(&bitmap);
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr BitmapFunctions::bitmap_remove(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    ColumnViewer<TYPE_OBJECT> lhs(columns[0]);
    ColumnViewer<TYPE_BIGINT> rhs(columns[1]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_OBJECT> builder(size);
    for (int row = 0; row < size; ++row) {
        if (lhs.is_null(row) || rhs.is_null(row)) {
            builder.append_null();
            continue;
        }

        BitmapValue bitmap;
        bitmap |= (*lhs.value(row));
        bitmap.remove(rhs.value(row));

        builder.append(&bitmap);
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr BitmapFunctions::bitmap_to_array(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    DCHECK_EQ(columns.size(), 1);
    ColumnViewer<TYPE_OBJECT> lhs(columns[0]);

    size_t size = columns[0]->size();
    UInt32Column::Ptr array_offsets = UInt32Column::create();
    array_offsets->reserve(size + 1);

    Int64Column::Ptr array_bigint_column = Int64Column::create();
    size_t data_size = 0;

    if (columns[0]->has_null()) {
        for (int row = 0; row < size; ++row) {
            if (!lhs.is_null(row)) {
                data_size += lhs.value(row)->cardinality();
            }
        }
    } else {
        for (int row = 0; row < size; ++row) {
            data_size += lhs.value(row)->cardinality();
        }
    }

    array_bigint_column->reserve(data_size);

    //Array Offset
    int offset = 0;
    if (columns[0]->has_null()) {
        for (int row = 0; row < size; ++row) {
            array_offsets->append(offset);
            if (lhs.is_null(row)) {
                continue;
            }

            auto& bitmap = *lhs.value(row);
            bitmap.to_array(&array_bigint_column->get_data());
            offset += bitmap.cardinality();
        }
    } else {
        for (int row = 0; row < size; ++row) {
            array_offsets->append(offset);
            auto& bitmap = *lhs.value(row);
            bitmap.to_array(&array_bigint_column->get_data());
            offset += bitmap.cardinality();
        }
    }
    array_offsets->append(offset);

    //Array Column
    if (!columns[0]->has_null()) {
        return ArrayColumn::create(NullableColumn::create(array_bigint_column, NullColumn::create(offset, 0)),
                                   array_offsets);
    } else if (columns[0]->only_null()) {
        return ColumnHelper::create_const_null_column(size);
    } else {
        return NullableColumn::create(
                ArrayColumn::create(NullableColumn::create(array_bigint_column, NullColumn::create(offset, 0)),
                                    array_offsets),
                NullColumn::create(*ColumnHelper::as_raw_column<NullableColumn>(columns[0])->null_column()));
    }
}

ColumnPtr BitmapFunctions::bitmap_max(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    ColumnViewer<TYPE_OBJECT> viewer(columns[0]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_BIGINT> builder(size);
    for (int row = 0; row < size; ++row) {
        if (viewer.is_null(row)) {
            builder.append_null();
        } else {
            int64_t value = viewer.value(row)->max();
            builder.append(value);
        }
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr BitmapFunctions::bitmap_min(FunctionContext* context, const starrocks::vectorized::Columns& columns) {
    ColumnViewer<TYPE_OBJECT> viewer(columns[0]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_BIGINT> builder(size);
    for (int row = 0; row < size; ++row) {
        if (viewer.is_null(row)) {
            builder.append_null();
        } else {
            int64_t value = viewer.value(row)->min();
            builder.append(value);
        }
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks::vectorized
