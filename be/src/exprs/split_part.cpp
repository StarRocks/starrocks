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

namespace starrocks {

/**
 * @param: [haystack, delimiter, part_number]
 * @paramType: [BinaryColumn, BinaryColumn, IntColumn]
 * @return: BinaryColumn
 */
StatusOr<ColumnPtr> StringFunctions::split_part(FunctionContext* context, const starrocks::Columns& columns) {
    DCHECK_EQ(columns.size(), 3);
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    if (columns[2]->is_constant()) {
        // if part_number is a negative int, return NULL.
        int32_t part_number = ColumnHelper::get_const_value<TYPE_INT>(columns[2]);
        if (part_number <= 0) {
            return ColumnHelper::create_const_null_column(columns[0]->size());
        }
    }

    ColumnViewer haystack_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    ColumnViewer delimiter_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    ColumnViewer part_number_viewer = ColumnViewer<TYPE_INT>(columns[2]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> res(size);
    for (int i = 0; i < size; ++i) {
        if (haystack_viewer.is_null(i) || delimiter_viewer.is_null(i) || part_number_viewer.is_null(i)) {
            res.append_null();
            continue;
        }

        int32_t part_number = part_number_viewer.value(i);
        if (part_number <= 0) {
            res.append_null();
            continue;
        }

        Slice haystack = haystack_viewer.value(i);
        Slice delimiter = delimiter_viewer.value(i);
        if (delimiter.size == 0) {
            // Keep Consistent with split.
            if (part_number > haystack.size) {
                res.append_null();
            } else {
                res.append(Slice(haystack.data + part_number - 1, 1));
            }
        } else if (delimiter.size == 1) {
            // if delimiter is a char, use memchr to split
            // Record the two adjacent offsets when matching delimiter.
            // If no matching, return NULL.
            // Else return the string between two adjacent offsets.
            int32_t pre_offset = -1;
            int32_t offset = -1;
            int32_t num = 0;
            while (num < part_number) {
                pre_offset = offset;
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
                res.append(Slice(haystack.data + pre_offset + 1, offset - pre_offset - 1));
            } else {
                res.append_null();
            }
        } else {
            // if delimiter is a string, use memmem to split
            int32_t pre_offset = -static_cast<int32_t>(delimiter.size);
            int32_t offset = -static_cast<int32_t>(delimiter.size);
            int32_t num = 0;
            while (num < part_number) {
                pre_offset = offset;
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
                res.append(Slice(haystack.data + pre_offset + delimiter.size, offset - pre_offset - delimiter.size));
            } else {
                res.append_null();
            }
        }
    }
    return res.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks
