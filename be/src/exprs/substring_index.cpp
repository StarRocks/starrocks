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

#include "column/binary_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/function_context.h"
#include "exprs/string_functions.h"
#include "util/utf8.h"

namespace starrocks {

static bool substring_with_index(const Slice& haystack, const Slice& delimiter, int32_t part_number, Slice& res) {
    if (part_number > 0) {
        if (delimiter.size == 1) {
            // if delimiter is a char, use memchr to split
            // Record the two adjacent offsets when matching delimiter.
            // If no matching, return NULL.
            // Else return the string between two adjacent offsets.
            int32_t offset = -1;
            int32_t num = 0;
            while (num < part_number) {
                size_t n = haystack.size - offset - 1;
                char* pos = reinterpret_cast<char*>(memchr(haystack.data + offset + 1, delimiter.data[0], n));
                if (pos != nullptr) {
                    offset = pos - haystack.data;
                    num++;
                } else {
                    offset = haystack.size;
                    num = (num == 0) ? 0 : num + 1;
                    break;
                }
            }

            if (num == part_number) {
                res.data = haystack.data;
                res.size = offset;
                return true;
            }
        } else {
            // if delimiter is a string, use memmem to split
            int32_t offset = -static_cast<int32_t>(delimiter.size);
            int32_t num = 0;
            while (num < part_number) {
                size_t n = haystack.size - offset - delimiter.size;
                char* pos = reinterpret_cast<char*>(
                        memmem(haystack.data + offset + delimiter.size, n, delimiter.data, delimiter.size));
                if (pos != nullptr) {
                    offset = pos - haystack.data;
                    num++;
                } else {
                    offset = haystack.size;
                    num = (num == 0) ? 0 : num + 1;
                    break;
                }
            }

            if (num == part_number) {
                res.data = haystack.data;
                res.size = offset;
                return true;
            }
        }
    } else {
        part_number = -part_number;
        auto haystack_str = haystack.to_string();
        int32_t offset = haystack.size;
        int32_t pre_offset = offset;
        int32_t num = 0;
        auto substr = haystack_str;
        while (num <= part_number && offset >= 0) {
            // TODO benchmarking rfind vs memrchr.
            offset = (int)substr.rfind(delimiter, offset);
            if (offset != -1) {
                if (++num == part_number) {
                    break;
                }
                pre_offset = offset;
                offset = offset - 1;
                substr = haystack_str.substr(0, pre_offset);
            } else {
                break;
            }
        }
        num = (offset == -1 && num != 0) ? num + 1 : num;
        if (num == part_number) {
            if (offset == -1) {
                res.data = haystack.data;
                res.size = haystack.size;
            } else {
                res.data = haystack.data + offset + delimiter.size;
                res.size = haystack.size - offset - delimiter.size;
            }
            return true;
        }
    }
    return false;
}

/**
 * @param: [haystack, delimiter, part_number]
 * @paramType: [BinaryColumn, BinaryColumn, IntColumn]
 * @return: BinaryColumn
 */
StatusOr<ColumnPtr> StringFunctions::substring_index(FunctionContext* context, const starrocks::Columns& columns) {
    DCHECK_EQ(columns.size(), 3);
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    // TODO use SIMD algorithm to optimize
    if (columns[2]->is_constant()) {
        // if part_number is 0, return NULL.
        int32_t part_number = ColumnHelper::get_const_value<TYPE_INT>(columns[2]);
        if (part_number == 0) {
            return ColumnHelper::create_const_null_column(columns[0]->size());
        }
    }

    ColumnViewer haystack_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    ColumnViewer delimiter_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    ColumnViewer part_number_viewer = ColumnViewer<TYPE_INT>(columns[2]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> res(size);
    Slice slice;
    for (int i = 0; i < size; ++i) {
        if (haystack_viewer.is_null(i) || delimiter_viewer.is_null(i) || part_number_viewer.is_null(i)) {
            res.append_null();
            continue;
        }

        int32_t part_number = part_number_viewer.value(i);
        Slice haystack = haystack_viewer.value(i);
        Slice delimiter = delimiter_viewer.value(i);
        if (part_number == 0 || delimiter.size == 0) {
            // if part_number or delimiter.size is 0, return NULL.
            res.append_null();
        } else {
            if (substring_with_index(haystack, delimiter, part_number, slice)) {
                res.append(slice);
            } else {
                slice.data = haystack.data;
                slice.size = haystack.size;
                res.append(slice);
            }
        }
    }
    return res.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks