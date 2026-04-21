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

#include <algorithm>

#include "base/string/utf8.h"
#include "base/string/volnitsky.h"
#include "column/binary_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/string_functions.h"

namespace starrocks {

struct LocateCaseSensitiveUTF8 {
    using SearcherInBigHaystack = VolnitskyUTF8;
    using SearcherInSmallHaystack = LibcASCIICaseSensitiveStringSearcher;

    static SearcherInBigHaystack createSearcherInBigHaystack(const char* needle_data, size_t needle_size,
                                                             size_t haystack_size_hint) {
        return {needle_data, needle_size, haystack_size_hint};
    }

    static SearcherInSmallHaystack createSearcherInSmallHaystack(const char* needle_data, size_t needle_size) {
        return {needle_data, needle_size};
    }
};

template <typename OffsetValue>
static void locate_empty_needle_impl(const OffsetValue* __restrict offsets, size_t num_rows,
                                     const int32_t* __restrict start_data, int32_t* __restrict result_data) {
    for (size_t i = 0; i < num_rows; ++i) {
        int32_t start = start_data[i];
        if (start <= 0) {
            // start is zero or negative, the result is zero
            result_data[i] = 0;
        } else if (start == 1) {
            // needle and haystack are all empty, the result is one
            result_data[i] = 1;
        } else if (start > offsets[i + 1] - offsets[i]) {
            // start larger than haystack size, the result is zero
            result_data[i] = 0;
        } else {
            result_data[i] = start;
        }
    }
}

template <typename OffsetValue>
static void locate_non_empty_needle_impl(const OffsetValue* __restrict offsets, size_t num_rows, const char* begin,
                                         const char* end, Slice needle, const int32_t* __restrict start_data,
                                         int32_t* __restrict result_data) {
    const char* pos = begin;
    size_t i = 0;
    auto searcher = LocateCaseSensitiveUTF8::createSearcherInBigHaystack(needle.data, needle.size, end - pos);

    /// We will search for the next occurrence in all strings at once.
    while (pos < end && end != (pos = searcher.search(pos, end - pos))) {
        /// Determine which index it refers to.
        while (begin + offsets[i + 1] <= pos) {
            result_data[i] = 0;
            ++i;
        }
        const int32_t start = start_data[i];
        const char* row_begin = begin + offsets[i];
        const char* row_end = begin + offsets[i + 1];

        /// We check that the entry does not pass through the boundaries of strings.
        if (start <= 0 || pos + needle.size > row_end) {
            result_data[i] = 0;
        } else {
            size_t res_pos = 1 + utf8_len(row_begin, pos);
            if (res_pos < start) {
                pos = skip_leading_utf8(pos, row_end, start - res_pos);
                continue;
            }
            result_data[i] = res_pos;
        }
        pos = row_end;
        ++i;
    }

    if (i < num_rows) {
        memset(result_data + i, 0, (num_rows - i) * sizeof(result_data[0]));
    }
}

// locate haystack is a vector and needle is a constant
ColumnPtr haystack_vector_and_needle_const(const ColumnPtr& haystack_ptr, const ColumnPtr& needle_ptr,
                                           const ColumnPtr& start_pos_ptr) {
    const BinaryColumn* haystack = nullptr;
    const FixedLengthColumn<int32_t>* start_pos = nullptr;
    NullColumnPtr res_null = nullptr;
    ColumnPtr start_pos_expansion = nullptr;
    if (start_pos_ptr->is_constant()) {
        // expand vector in start_pos_ptr to specfied size
        auto start_pos_mut = RunTimeColumnType<TYPE_INT>::create();
        int32_t value = ColumnHelper::get_const_value<TYPE_INT>(start_pos_ptr);
        start_pos_mut->append_value_multiple_times(&value, haystack_ptr->size());
        start_pos_expansion = std::move(start_pos_mut);
    } else {
        start_pos_expansion = start_pos_ptr;
    }
    if (haystack_ptr->is_nullable() && start_pos_expansion->is_nullable()) {
        auto haystack_null = ColumnHelper::as_column<NullableColumn>(haystack_ptr);
        haystack = ColumnHelper::as_raw_column<BinaryColumn>(haystack_null->data_column());

        auto start_pos_null = ColumnHelper::as_column<NullableColumn>(start_pos_expansion);
        start_pos = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(start_pos_null->data_column());

        res_null = FunctionHelper::union_nullable_column(haystack_ptr, start_pos_expansion);
    } else if (!haystack_ptr->is_nullable() && start_pos_expansion->is_nullable()) {
        haystack = ColumnHelper::as_raw_column<BinaryColumn>(haystack_ptr);

        auto start_pos_null = ColumnHelper::as_column<NullableColumn>(start_pos_expansion);
        start_pos = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(start_pos_null->data_column());

        res_null = NullColumn::static_pointer_cast(start_pos_null->null_column()->clone());
    } else if (haystack_ptr->is_nullable() && !start_pos_expansion->is_nullable()) {
        auto haystack_null = ColumnHelper::as_column<NullableColumn>(haystack_ptr);
        haystack = ColumnHelper::as_raw_column<BinaryColumn>(haystack_null->data_column());

        start_pos = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(start_pos_expansion);
        res_null = NullColumn::static_pointer_cast(haystack_null->null_column()->clone());
    } else {
        haystack = ColumnHelper::as_raw_column<BinaryColumn>(haystack_ptr);
        start_pos = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(start_pos_expansion);
    }

    const auto& offsets = haystack->get_offset();
    Slice needle = ColumnHelper::get_const_value<TYPE_VARCHAR>(needle_ptr);
    auto res = RunTimeColumnType<TYPE_INT>::create();
    res->resize(haystack->size());
    auto* __restrict result_data = res->get_data().data();
    const auto* __restrict start_data = start_pos->get_data().data();
    const size_t num_rows = haystack->size();

    if (needle.size == 0) {
        // if needle is empty string, at legal start position in haystack
        offsets.visit_storage([&](const auto& offsets_buf) {
            locate_empty_needle_impl(offsets_buf.data(), num_rows, start_data, result_data);
        });
        if (res_null != nullptr) {
            return NullableColumn::create(std::move(res), std::move(res_null));
        } else {
            return res;
        }
    }

    const char* begin = haystack->get_string_begin();
    const char* end = haystack->get_string_end();

    offsets.visit_storage([&](const auto& offsets_buf) {
        locate_non_empty_needle_impl(offsets_buf.data(), num_rows, begin, end, needle, start_data, result_data);
    });

    if (res_null != nullptr) {
        return NullableColumn::create(std::move(res), std::move(res_null));
    } else {
        return res;
    }
}

// locate for needle is not constant
// haystack may be variable vector or constant
ColumnPtr haystack_vector_and_needle_vector(const ColumnPtr& haystack_ptr, const ColumnPtr& needle_ptr,
                                            const ColumnPtr& start_pos_ptr) {
    ColumnViewer<TYPE_VARCHAR> haystack_viewer(haystack_ptr);
    ColumnViewer<TYPE_VARCHAR> needle_viewer(needle_ptr);
    ColumnViewer<TYPE_INT> start_pos_viewer(start_pos_ptr);

    size_t size = haystack_ptr->size();
    ColumnBuilder<TYPE_INT> builder(size);

    for (size_t i = 0; i < size; ++i) {
        if (haystack_viewer.is_null(i) || needle_viewer.is_null(i) || start_pos_viewer.is_null(i)) {
            builder.append_null();
            continue;
        }

        const Slice& haystack_slice = haystack_viewer.value(i);
        size_t haystack_size = haystack_slice.size;

        const Slice& needle_slice = needle_viewer.value(i);
        size_t needle_size = needle_slice.size;

        int32_t start = start_pos_viewer.value(i);

        if (start <= 0) {
            // start is zero or negative, the result is zero
            builder.append(0);
        } else if (needle_size == 0 && start == 1) {
            // needle and haystack are all empty, the result is one
            builder.append(1);
        } else if (needle_size == 0 && start <= haystack_size) {
            // needle are empty, and start not greater than haystack_size
            // the result is start
            builder.append(start);
        } else if (start > haystack_size) {
            // start larger than haystack size, the result is zero
            builder.append(0);
        } else {
            /// It is assumed that the StringSearcher is not very difficult to initialize.
            auto searcher =
                    LocateCaseSensitiveUTF8::createSearcherInSmallHaystack(needle_slice.data, needle_slice.size);

            const char* beg =
                    skip_leading_utf8(haystack_slice.data, haystack_slice.data + haystack_size - 1, start - 1);
            /// searcher returns a pointer to the found substring or to the end of `haystack`.
            const char* res_pointer = searcher.search(beg, haystack_size - (beg - haystack_slice.data));
            if (!res_pointer) {
                builder.append(0);
            } else {
                builder.append(1 + utf8_len(haystack_slice.data, res_pointer));
            }
        }
    }

    return builder.build(ColumnHelper::is_all_const({haystack_ptr, needle_ptr, start_pos_ptr}));
}

StatusOr<ColumnPtr> StringFunctions::instr(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const ColumnPtr& haystack = columns[0];
    const ColumnPtr& needle = columns[1];
    ColumnPtr start_pos = ColumnHelper::create_const_column<TYPE_INT>(1, columns[0]->size());
    if (!haystack->is_constant() && needle->is_constant()) {
        return haystack_vector_and_needle_const(haystack, needle, start_pos);
    } else {
        return haystack_vector_and_needle_vector(haystack, needle, start_pos);
    }
}

// locate without specified position
StatusOr<ColumnPtr> StringFunctions::locate(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const ColumnPtr& haystack = columns[1];
    const ColumnPtr& needle = columns[0];
    ColumnPtr start_pos = ColumnHelper::create_const_column<TYPE_INT>(1, columns[0]->size());
    if (!haystack->is_constant() && needle->is_constant()) {
        return haystack_vector_and_needle_const(haystack, needle, start_pos);
    } else {
        return haystack_vector_and_needle_vector(haystack, needle, start_pos);
    }
}

// locate with specified position
StatusOr<ColumnPtr> StringFunctions::locate_pos(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const ColumnPtr& haystack = columns[1];
    const ColumnPtr& needle = columns[0];
    const ColumnPtr& start_pos = columns[2];
    if (!haystack->is_constant() && needle->is_constant()) {
        return haystack_vector_and_needle_const(haystack, needle, start_pos);
    } else {
        return haystack_vector_and_needle_vector(haystack, needle, start_pos);
    }
}

} // namespace starrocks
