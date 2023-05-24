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

#include "exprs/bitmap_functions.h"

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/nullable_column.h"
#include "exprs/base64.h"
#include "exprs/binary_function.h"
#include "exprs/function_context.h"
#include "exprs/unary_function.h"
#include "gutil/casts.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "util/phmap/phmap.h"
#include "util/string_parser.hpp"

namespace starrocks {

template <LogicalType LT>
StatusOr<ColumnPtr> BitmapFunctions::to_bitmap(FunctionContext* context, const starrocks::Columns& columns) {
    ColumnViewer<LT> viewer(columns[0]);

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

        BitmapValue bitmap(value);

        builder.append(&bitmap);
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}
template StatusOr<ColumnPtr> BitmapFunctions::to_bitmap<TYPE_BOOLEAN>(FunctionContext* context,
                                                                      const starrocks::Columns& columns);
template StatusOr<ColumnPtr> BitmapFunctions::to_bitmap<TYPE_TINYINT>(FunctionContext* context,
                                                                      const starrocks::Columns& columns);
template StatusOr<ColumnPtr> BitmapFunctions::to_bitmap<TYPE_SMALLINT>(FunctionContext* context,
                                                                       const starrocks::Columns& columns);
template StatusOr<ColumnPtr> BitmapFunctions::to_bitmap<TYPE_INT>(FunctionContext* context,
                                                                  const starrocks::Columns& columns);
template StatusOr<ColumnPtr> BitmapFunctions::to_bitmap<TYPE_BIGINT>(FunctionContext* context,
                                                                     const starrocks::Columns& columns);
template StatusOr<ColumnPtr> BitmapFunctions::to_bitmap<TYPE_LARGEINT>(FunctionContext* context,
                                                                       const starrocks::Columns& columns);
template StatusOr<ColumnPtr> BitmapFunctions::to_bitmap<TYPE_VARCHAR>(FunctionContext* context,
                                                                      const starrocks::Columns& columns);

StatusOr<ColumnPtr> BitmapFunctions::bitmap_hash(FunctionContext* context, const starrocks::Columns& columns) {
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

StatusOr<ColumnPtr> BitmapFunctions::bitmap_count(FunctionContext* context, const starrocks::Columns& columns) {
    ColumnViewer<TYPE_OBJECT> viewer(columns[0]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_BIGINT> builder(size);
    for (int row = 0; row < size; ++row) {
        int64_t value = viewer.is_null(row) ? 0 : viewer.value(row)->cardinality();
        builder.append(value);
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> BitmapFunctions::bitmap_empty(FunctionContext* context, const starrocks::Columns& columns) {
    BitmapValue bitmap;
    return ColumnHelper::create_const_column<TYPE_OBJECT>(&bitmap, 1);
}

StatusOr<ColumnPtr> BitmapFunctions::bitmap_or(FunctionContext* context, const starrocks::Columns& columns) {
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

StatusOr<ColumnPtr> BitmapFunctions::bitmap_and(FunctionContext* context, const starrocks::Columns& columns) {
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
    if (bitmap_ptr->cardinality() > config::max_length_for_bitmap_function) {
        std::stringstream ss;
        ss << "bitmap_to_string not supported size > " << config::max_length_for_bitmap_function;
        throw std::runtime_error(ss.str());
    }
    return bitmap_ptr->to_string();
}

StatusOr<ColumnPtr> BitmapFunctions::bitmap_to_string(FunctionContext* context, const starrocks::Columns& columns) {
    return VectorizedStringStrictUnaryFunction<bitmapToStingImpl>::evaluate<TYPE_OBJECT, TYPE_VARCHAR>(columns[0]);
}

StatusOr<ColumnPtr> BitmapFunctions::bitmap_from_string(FunctionContext* context, const Columns& columns) {
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

StatusOr<ColumnPtr> BitmapFunctions::bitmap_contains(FunctionContext* context, const starrocks::Columns& columns) {
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

StatusOr<ColumnPtr> BitmapFunctions::bitmap_has_any(FunctionContext* context, const starrocks::Columns& columns) {
    return VectorizedStrictBinaryFunction<bitmapHasAny>::evaluate<TYPE_OBJECT, TYPE_BOOLEAN>(columns[0], columns[1]);
}

StatusOr<ColumnPtr> BitmapFunctions::bitmap_andnot(FunctionContext* context, const starrocks::Columns& columns) {
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

StatusOr<ColumnPtr> BitmapFunctions::bitmap_xor(FunctionContext* context, const starrocks::Columns& columns) {
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

StatusOr<ColumnPtr> BitmapFunctions::bitmap_remove(FunctionContext* context, const starrocks::Columns& columns) {
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

void BitmapFunctions::detect_bitmap_cardinality(size_t* data_size, const int64_t cardinality) {
    if (cardinality > config::max_length_for_bitmap_function) {
        std::stringstream ss;
        ss << "bitmap_to_array not supported size > " << config::max_length_for_bitmap_function;
        throw std::runtime_error(ss.str());
    }
    (*data_size) += cardinality;
}

StatusOr<ColumnPtr> BitmapFunctions::bitmap_to_array(FunctionContext* context, const starrocks::Columns& columns) {
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
                const auto cardinality = lhs.value(row)->cardinality();
                detect_bitmap_cardinality(&data_size, cardinality);
            }
        }
    } else {
        for (int row = 0; row < size; ++row) {
            const auto cardinality = lhs.value(row)->cardinality();
            detect_bitmap_cardinality(&data_size, cardinality);
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

StatusOr<ColumnPtr> BitmapFunctions::array_to_bitmap(FunctionContext* context, const starrocks::Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    const constexpr LogicalType TYPE = TYPE_BIGINT;
    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_OBJECT> builder(size);

    Column* data_column = ColumnHelper::get_data_column(columns[0].get());
    NullData::pointer null_data = columns[0]->is_nullable()
                                          ? down_cast<NullableColumn*>(columns[0].get())->null_column_data().data()
                                          : nullptr;
    auto* array_column = down_cast<ArrayColumn*>(data_column);

    RunTimeColumnType<TYPE>::Container& element_container =
            array_column->elements_column()->is_nullable()
                    ? down_cast<RunTimeColumnType<TYPE>*>(
                              down_cast<NullableColumn*>(array_column->elements_column().get())->data_column().get())
                              ->get_data()
                    : down_cast<RunTimeColumnType<TYPE>*>(array_column->elements_column().get())->get_data();
    const auto& offsets = array_column->offsets_column()->get_data();

    NullColumn::Container::pointer element_null_data =
            array_column->elements_column()->is_nullable()
                    ? down_cast<NullableColumn*>(array_column->elements_column().get())->null_column_data().data()
                    : nullptr;

    for (int row = 0; row < size; ++row) {
        uint32_t offset = offsets[row];
        uint32_t length = offsets[row + 1] - offsets[row];
        if (null_data && null_data[row]) {
            builder.append_null();
            continue;
        }
        // build bitmap
        BitmapValue bitmap;
        for (int j = offset; j < offset + length; j++) {
            if (element_null_data && element_null_data[j]) {
                continue;
            }
            if (element_container[j] >= 0) {
                bitmap.add(element_container[j]);
            }
        }
        // append bitmap
        builder.append(std::move(bitmap));
    }
    return builder.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> BitmapFunctions::bitmap_max(FunctionContext* context, const starrocks::Columns& columns) {
    ColumnViewer<TYPE_OBJECT> viewer(columns[0]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_LARGEINT> builder(size);
    for (int row = 0; row < size; ++row) {
        if (viewer.is_null(row)) {
            builder.append_null();
        } else {
            if (auto max_value = viewer.value(row)->max(); max_value.has_value()) {
                int128_t value128 = max_value.value();
                builder.append(value128);
            } else {
                builder.append_null();
            }
        }
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> BitmapFunctions::bitmap_min(FunctionContext* context, const starrocks::Columns& columns) {
    ColumnViewer<TYPE_OBJECT> viewer(columns[0]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_LARGEINT> builder(size);
    for (int row = 0; row < size; ++row) {
        if (viewer.is_null(row)) {
            builder.append_null();
        } else {
            if (auto min_value = viewer.value(row)->min(); min_value.has_value()) {
                int128_t value128 = min_value.value();
                builder.append(value128);
            } else {
                builder.append_null();
            }
        }
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> BitmapFunctions::base64_to_bitmap(FunctionContext* context, const starrocks::Columns& columns) {
    ColumnViewer<TYPE_VARCHAR> viewer(columns[0]);
    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_OBJECT> builder(size);
    std::unique_ptr<char[]> p;
    int last_len = 0;
    int curr_len = 0;

    for (int row = 0; row < size; ++row) {
        if (viewer.is_null(row)) {
            builder.append_null();
            continue;
        }

        auto src_value = viewer.value(row);
        int ssize = src_value.size;
        if (ssize == 0) {
            builder.append_null();
            continue;
        }

        curr_len = ssize + 3;
        if (last_len < curr_len) {
            p.reset(new char[curr_len]);
            last_len = curr_len;
        }

        int decode_res = base64_decode2(src_value.data, ssize, p.get());
        if (decode_res < 0) {
            builder.append_null();
            continue;
        }

        BitmapValue bitmap;
        bitmap.deserialize(p.get());
        builder.append(std::move(bitmap));
    }
    return builder.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> BitmapFunctions::sub_bitmap(FunctionContext* context, const starrocks::Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    ColumnViewer<TYPE_OBJECT> bitmap_viewer(columns[0]);
    ColumnViewer<TYPE_BIGINT> offset_viewer(columns[1]);
    ColumnViewer<TYPE_BIGINT> len_viewer(columns[2]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_OBJECT> builder(size);

    for (int row = 0; row < size; row++) {
        if (bitmap_viewer.is_null(row) || offset_viewer.is_null(row) || len_viewer.is_null(row) ||
            len_viewer.value(row) <= 0) {
            builder.append_null();
            continue;
        }

        auto bitmap = bitmap_viewer.value(row);
        auto offset = offset_viewer.value(row);
        auto len = len_viewer.value(row);
        if (bitmap->cardinality() == 0 || offset == INT_MIN || len <= 0) {
            builder.append_null();
            continue;
        }

        BitmapValue ret_bitmap;
        if (bitmap->sub_bitmap_internal(offset, len, &ret_bitmap) == 0) {
            builder.append_null();
            continue;
        }

        builder.append(std::move(ret_bitmap));
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> BitmapFunctions::bitmap_to_base64(FunctionContext* context, const starrocks::Columns& columns) {
    ColumnViewer<TYPE_OBJECT> viewer(columns[0]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> builder(size);

    for (int row = 0; row < size; ++row) {
        BitmapValue* bitmap = viewer.value(row);
        int byteSize = bitmap->getSizeInBytes();
        std::unique_ptr<char[]> buf;
        buf.reset(new char[byteSize]);

        int len = (size_t)(4.0 * ceil((double)byteSize / 3.0)) + 1;
        std::unique_ptr<char[]> p;
        p.reset(new char[len]);
        memset(p.get(), 0, len);

        bitmap->write((char*)buf.get());

        int resLen = base64_encode2((unsigned char*)buf.get(), byteSize, (unsigned char*)p.get());

        if (resLen < 0) {
            builder.append_null();
            continue;
        }
        builder.append(Slice(p.get(), resLen));
    }
    return builder.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> BitmapFunctions::bitmap_subset_limit(FunctionContext* context, const starrocks::Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    ColumnViewer<TYPE_OBJECT> bitmap_viewer(columns[0]);
    ColumnViewer<TYPE_BIGINT> range_start_viewer(columns[1]);
    ColumnViewer<TYPE_BIGINT> limit_viewer(columns[2]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_OBJECT> builder(size);

    for (int row = 0; row < size; row++) {
        if (bitmap_viewer.is_null(row) || range_start_viewer.is_null(row) || limit_viewer.is_null(row)) {
            builder.append_null();
            continue;
        }

        auto bitmap = bitmap_viewer.value(row);
        auto range_start = range_start_viewer.value(row);
        auto limit = limit_viewer.value(row);

        if (range_start < 0) {
            range_start = 0;
        }

        if (bitmap->cardinality() == 0) {
            builder.append_null();
            continue;
        }

        BitmapValue ret_bitmap;
        if (bitmap->bitmap_subset_limit_internal(range_start, limit, &ret_bitmap) == 0) {
            builder.append_null();
            continue;
        }

        builder.append(std::move(ret_bitmap));
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> BitmapFunctions::bitmap_subset_in_range(FunctionContext* context,
                                                            const starrocks::Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    ColumnViewer<TYPE_OBJECT> bitmap_viewer(columns[0]);
    ColumnViewer<TYPE_BIGINT> range_start_viewer(columns[1]);
    ColumnViewer<TYPE_BIGINT> range_end_viewer(columns[2]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_OBJECT> builder(size);

    for (int row = 0; row < size; row++) {
        if (bitmap_viewer.is_null(row) || range_start_viewer.is_null(row) || range_end_viewer.is_null(row)) {
            builder.append_null();
            continue;
        }

        auto bitmap = bitmap_viewer.value(row);
        auto range_start = range_start_viewer.value(row);
        auto range_end = range_end_viewer.value(row);

        if (range_start < 0) {
            range_start = 0;
        }

        if (bitmap->cardinality() == 0 || range_start >= range_end) {
            builder.append_null();
            continue;
        }

        BitmapValue ret_bitmap;
        if (bitmap->bitmap_subset_in_range_internal(range_start, range_end, &ret_bitmap) == 0) {
            builder.append_null();
            continue;
        }

        builder.append(std::move(ret_bitmap));
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks
