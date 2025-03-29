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
#include <stack>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/map_column.h"
#include "exprs/function_context.h"
#include "exprs/string_functions.h"
#include "util/utf8.h"

namespace starrocks {

/**
* @param: [string, delimiter, map_delimiter]
* @paramType: [BinaryColumn, BinaryColumn, BinaryColumn]
* @return: MapColumn map<string,string>
*/
StatusOr<ColumnPtr> StringFunctions::str_to_map(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 3);
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    // split first
    Columns split_columns{columns[0], columns[1]};
    ASSIGN_OR_RETURN(auto splited, StringFunctions::split(context, split_columns));

    Columns splited_columns{splited, columns[2]};
    return str_to_map_v1(context, splited_columns);
}

Status StringFunctions::str_to_map_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    return StringFunctions::split_prepare(context, scope);
}

Status StringFunctions::str_to_map_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    return StringFunctions::split_close(context, scope);
}

/**
* @param: [array_string, delimiter]
* @paramType: [ArrayBinaryColumn, BinaryColumn]
* @return: MapColumn map<string,string>

 the original str_to_map(str, del1, del2) is rewritten to str_to_map(split(str, del1), del2), the first input results
 from split(str, del1), its return type is array_string, note each array's item wouldn't NULL.

 empty array or string results into "" key, and not-found delimiter case causes NULL value.

 TODO: split UTF8 chinese character according to its size, which would be greater than 1.
*/

StatusOr<ColumnPtr> StringFunctions::str_to_map_v1(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 2);
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    // decompose array<string>
    auto array_str_column = ColumnHelper::unpack_and_duplicate_const_column(columns[0]->size(), columns[0]);
    NullColumnPtr nulls = nullptr;
    if (array_str_column->is_nullable()) {
        nulls = down_cast<NullableColumn*>(array_str_column.get())->null_column();
    }
    auto* array_str = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(array_str_column.get()));
    auto offsets = array_str->offsets_column();
    auto nullable_str = array_str->elements_column(); // no null here

    // construct result
    size_t str_num = nullable_str->size();
    size_t column_size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> keys_builder(str_num);
    ColumnBuilder<TYPE_VARCHAR> values_builder(str_num);
    auto res_null = NullColumn::create();
    auto res_offsets = UInt32Column::create();
    res_offsets->reserve(nullable_str->size() + 1);
    res_offsets->append(0);
    res_null->resize(column_size);

    auto is_unique = [=](std::vector<Slice>& exist_slice, Slice&& s) {
        for (auto& tmp : exist_slice) {
            if (s == tmp) {
                return false;
            }
        }
        exist_slice.emplace_back(s);
        return true;
    };
    ColumnViewer string_viewer = ColumnViewer<TYPE_VARCHAR>(nullable_str);
    ColumnViewer delimiter_viewer = ColumnViewer<TYPE_VARCHAR>(
            ColumnHelper::unpack_and_duplicate_const_column(column_size, columns[1])); // column range

    for (auto i = 0; i < column_size; ++i) {
        // either null input results into null to keep consistent with split()
        if ((nulls != nullptr && nulls->get_data()[i]) || delimiter_viewer.is_null(i)) {
            res_null->get_data()[i] = 1;
            res_offsets->append(res_offsets->get_data().back());
            continue;
        }
        res_null->get_data()[i] = 0;
        // empty array return {"":NULL}
        if (offsets->get_data()[i] == offsets->get_data()[i + 1]) {
            keys_builder.append("");
            values_builder.append_null();
            res_offsets->append(res_offsets->get_data().back() + 1);
            continue;
        }
        Slice delimiter = delimiter_viewer.value(i);
        std::vector<Slice> tmp_slice;
        std::stack<bool> val_is_null;
        std::stack<Slice> tmp_keys, tmp_values;

        // reverse order to get the last win.
        for (ssize_t off = offsets->get_data()[i + 1] - 1; off >= offsets->get_data()[i]; --off) {
            Slice haystack = string_viewer.value(off);
            if (haystack.empty()) { // return {"":NULL}
                if (is_unique(tmp_slice, Slice(""))) {
                    tmp_keys.push(Slice());
                    tmp_values.push(Slice());
                    val_is_null.push(true);
                }
                continue;
            }
            if (delimiter.size == 0) { // return {`1-th`:`rest`}
                auto char_size = UTF8_BYTE_LENGTH_TABLE[static_cast<unsigned char>(haystack.data[0])];
                if (is_unique(tmp_slice, Slice(haystack.data, char_size))) {
                    tmp_keys.push(Slice(haystack.data, char_size));
                    tmp_values.push(Slice(haystack.data + char_size, haystack.size - char_size));
                    val_is_null.push(false);
                }
            } else {
                char* pos = nullptr;
                if (delimiter.size == 1) {
                    pos = reinterpret_cast<char*>(memchr(haystack.data, delimiter.data[0], haystack.size));
                } else {
                    pos = reinterpret_cast<char*>(memmem(haystack.data, haystack.size, delimiter.data, delimiter.size));
                }
                if (pos != nullptr) { // return {`0-pos`:`rest`}
                    if (is_unique(tmp_slice, Slice(haystack.data, pos - haystack.data))) {
                        auto offset = pos - haystack.data + delimiter.size;
                        tmp_keys.push(Slice(haystack.data, pos - haystack.data));
                        tmp_values.push(Slice(haystack.data + offset, haystack.size - offset));
                        val_is_null.push(false);
                    }
                } else { // return {`all`:null}
                    if (is_unique(tmp_slice, Slice(haystack.data, haystack.size))) {
                        tmp_keys.push(Slice(haystack.data, haystack.size));
                        tmp_values.push(Slice());
                        val_is_null.push(true);
                    }
                }
            }
        }
        // append in order
        res_offsets->append(res_offsets->get_data().back() + tmp_keys.size());
        while (!tmp_keys.empty()) {
            auto key = tmp_keys.top();
            auto val = tmp_values.top();
            auto is_null = val_is_null.top();
            keys_builder.append(key);
            if (is_null) {
                values_builder.append_null();
            } else {
                values_builder.append(val);
            }
            tmp_keys.pop();
            tmp_values.pop();
            val_is_null.pop();
        }
    }

    auto map = MapColumn::create(keys_builder.build_nullable_column(), values_builder.build_nullable_column(),
                                 std::move(res_offsets));
    return NullableColumn::create(std::move(map), std::move(res_null));
}

} // namespace starrocks
