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

#include "gutil/strings/split.h"

#include <algorithm>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/string_functions.h"
#include "util/utf8.h"

namespace starrocks {

const void* _memchr(const void* big, size_t big_len, const void* little, size_t little_len) {
    return memchr(big, *((char*)little), big_len);
}

const void* _memmem(const void* big, size_t big_len, const void* little, size_t little_len) {
    return memmem(big, big_len, little, little_len);
}

struct SplitState {
    std::vector<std::string> const_split_strings;

    Slice delimiter;
    const void* (*find_delimiter)(const void* big, size_t big_len, const void* little, size_t little_len);
};

Status StringFunctions::split_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    auto* state = new SplitState();
    context->set_function_state(scope, state);

    if (context->is_notnull_constant_column(0) && context->is_notnull_constant_column(1)) {
        Slice haystack = ColumnHelper::get_const_value<TYPE_VARCHAR>(context->get_constant_column(0));
        Slice delimiter = ColumnHelper::get_const_value<TYPE_VARCHAR>(context->get_constant_column(1));
        std::vector<std::string> const_split_strings =
                strings::Split(StringPiece(haystack.get_data(), haystack.get_size()),
                               StringPiece(delimiter.get_data(), delimiter.get_size()));

        state->const_split_strings = const_split_strings;
    } else if (context->is_notnull_constant_column(1)) {
        Slice delimiter = ColumnHelper::get_const_value<TYPE_VARCHAR>(context->get_constant_column(1));

        state->delimiter = delimiter;
        if (delimiter.size == 1) {
            state->find_delimiter = _memchr;
        } else {
            state->find_delimiter = _memmem;
        }
    }
    return Status::OK();
}

Status StringFunctions::split_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* state = reinterpret_cast<SplitState*>(context->get_function_state(scope));
        delete state;
    }
    return Status::OK();
}

/**
* @param: [string, delimiter]
* @paramType: [BinaryColumn, BinaryColumn]
* @return: ArrayColumn
*/
StatusOr<ColumnPtr> StringFunctions::split(FunctionContext* context, const starrocks::Columns& columns) {
    DCHECK_EQ(columns.size(), 2);
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    ColumnViewer string_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    ColumnViewer delimiter_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);

    size_t row_nums = columns[0]->size();

    //Array Offset
    int offset = 0;
    UInt32Column::Ptr array_offsets = UInt32Column::create();
    array_offsets->reserve(row_nums + 1);

    //Array Binary
    auto* haystack_columns = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(columns[0].get()));
    BinaryColumn::Ptr array_binary_column = BinaryColumn::create();

    auto state = reinterpret_cast<SplitState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (context->is_notnull_constant_column(0) && context->is_notnull_constant_column(1)) {
        std::vector<std::string> split_string = state->const_split_strings;
        array_binary_column->reserve(row_nums * split_string.size(), haystack_columns->get_bytes().size());

        for (int row = 0; row < row_nums; ++row) {
            array_offsets->append(offset);
            for (auto& i : split_string) {
                array_binary_column->append(Slice(i.c_str()));
            }
            offset += split_string.size();
        }
        array_offsets->append(offset);

        return ArrayColumn::create(NullableColumn::create(array_binary_column, NullColumn::create(offset, 0)),
                                   array_offsets);
    } else if (columns[1]->is_constant()) {
        Slice delimiter = state->delimiter;

        if (delimiter.size == 0) { // split each character
            std::vector<Slice> v;
            v.reserve(haystack_columns->byte_size());
            array_binary_column->reserve(haystack_columns->byte_size(), haystack_columns->get_bytes().size());

            for (int row = 0; row < row_nums; ++row) {
                array_offsets->append(offset);
                Slice haystack = string_viewer.value(row);

                for (int h = 0; h < haystack.size;) {
                    auto char_size = UTF8_BYTE_LENGTH_TABLE[static_cast<unsigned char>(haystack.data[h])];
                    v.emplace_back(Slice(haystack.data + h, char_size));
                    h += char_size;
                    ++offset;
                }
            }
            array_offsets->append(offset);

            array_binary_column->append_continuous_strings(v);
        } else {
            //row_nums * 5 is an estimated value, because the true value cannot be obtained for the time being here
            array_binary_column->reserve(row_nums * 5, haystack_columns->get_bytes().size());
            for (int row = 0; row < row_nums; ++row) {
                array_offsets->append(offset);
                Slice haystack = string_viewer.value(row);
                int32_t haystack_offset = 0;
                int splits_size = 0;

                while (true) {
                    splits_size++;
                    const char* pos = reinterpret_cast<const char*>(
                            state->find_delimiter(haystack.data + haystack_offset, haystack.size - haystack_offset,
                                                  delimiter.data, delimiter.size));
                    if (pos != nullptr) {
                        array_binary_column->append(
                                Slice(haystack.data + haystack_offset, pos - (haystack.data + haystack_offset)));
                        haystack_offset = pos - haystack.data + delimiter.size;
                    } else {
                        array_binary_column->append(
                                Slice(haystack.data + haystack_offset, haystack.size - haystack_offset));
                        break;
                    }
                }
                offset += splits_size;
            }
            array_offsets->append(offset);
        }
        if (!columns[0]->has_null()) {
            return ArrayColumn::create(NullableColumn::create(array_binary_column, NullColumn::create(offset, 0)),
                                       array_offsets);
        } else {
            return NullableColumn::create(
                    ArrayColumn::create(NullableColumn::create(array_binary_column, NullColumn::create(offset, 0)),
                                        array_offsets),
                    NullColumn::create(*ColumnHelper::as_raw_column<NullableColumn>(columns[0])->null_column()));
        }
    } else {
        array_binary_column->reserve(row_nums * 5, haystack_columns->get_bytes().size() * sizeof(uint8_t));

        auto result_array = ArrayColumn::create(NullableColumn::create(BinaryColumn::create(), NullColumn::create()),
                                                UInt32Column::create());
        NullColumnPtr null_array = NullColumn::create();
        for (int row = 0; row < row_nums; ++row) {
            array_offsets->append(offset);

            if (string_viewer.is_null(row) || delimiter_viewer.is_null(row)) {
                null_array->append(1);
                continue;
            }
            null_array->append(0);

            Slice str = string_viewer.value(row);
            Slice delimiter = delimiter_viewer.value(row);
            if (delimiter.size == 0) { // split each character
                for (auto h = 0; h < str.size;) {
                    auto char_size = UTF8_BYTE_LENGTH_TABLE[static_cast<unsigned char>(str.data[h])];
                    array_binary_column->append(Slice(str.data + h, char_size));
                    h += char_size;
                    ++offset;
                }
            } else {
                std::vector<std::string> split_string =
                        strings::Split(StringPiece(str.get_data(), str.get_size()),
                                       StringPiece(delimiter.get_data(), delimiter.get_size()));
                for (auto& i : split_string) {
                    array_binary_column->append(Slice(i.c_str()));
                }
                offset += split_string.size();
            }
        }
        array_offsets->append(offset);
        result_array = ArrayColumn::create(NullableColumn::create(array_binary_column, NullColumn::create(offset, 0)),
                                           array_offsets);
        return NullableColumn::create(result_array, null_array);
    }
}

} // namespace starrocks
