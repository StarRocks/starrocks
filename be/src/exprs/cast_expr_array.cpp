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

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/json_column.h"
#include "exprs/cast_expr.h"
#include "exprs/expr_context.h"
#include "gutil/casts.h"
#include "gutil/strings/split.h"
#include "gutil/strings/strip.h"
#include "gutil/strings/substitute.h"
#include "runtime/memory/memory_resource.h"
#include "types/logical_type.h"
#include "util/slice.h"
#include "velocypack/Iterator.h"

namespace starrocks {

static const char* kArrayDelimeter = ",";

inline bool is_quote(char c) {
    return c == '\'' || c == '"';
}

bool is_valid_array(const Slice& src, std::vector<char>& container) {
    size_t length = src.get_size();
    bool has_bracket = false;
    container.clear();

    for (size_t i = 0; i < length; ++i) {
        // process string literal
        if (!container.empty() && is_quote(container.back())) {
            if (container.back() != src[i]) {
                continue;
            }
        }
        if (is_quote(src[i])) {
            if (!container.empty() && container.back() == src[i]) {
                container.pop_back();
            } else {
                container.push_back(src[i]);
            }
        } else if (src[i] == '[') {
            container.push_back(src[i]);
            has_bracket = true;
        } else if (src[i] == ']') {
            if (!container.empty() && container.back() == '[') {
                container.pop_back();
            } else {
                return false;
            }
        }
    }

    return has_bracket && container.empty();
}

// [[],[]] -> [],[]
// [],[] -> [],[]
// [[]] -> []
Slice strip_array_wrapper(const Slice& src) {
    size_t length = src.get_size();
    if (length < 2) {
        return src;
    }

    size_t stack_size = src[0] == '[';

    for (size_t i = 1; i < length - 1; ++i) {
        stack_size += src[i] == '[';
        stack_size -= src[i] == ']';

        if (stack_size == 0) {
            return src;
        }
    }

    return {src.get_data() + 1, src.get_size() - 2};
}

//
void array_delimeter_split(const Slice& src, std::vector<Slice>& res, std::vector<char>& stack) {
    stack.clear();

    size_t length = src.get_size();
    size_t begin = 0;
    const char* raw_data = src.get_data();
    size_t stack_size = 0;

    for (size_t i = 0; i < length; ++i) {
        if (is_quote(src[i])) {
            if (!stack.empty() && src[i] == stack.back()) {
                stack.pop_back();
            } else {
                stack.push_back(src[i]);
            }
        }

        if (!stack.empty()) {
            continue;
        }

        if (src[i] == *kArrayDelimeter && stack_size == 0) {
            res.emplace_back(raw_data + begin, i - begin);
            begin = i + 1;
        }

        stack_size += src[i] == '[';
        stack_size -= src[i] == ']';
    }
    if (begin <= length && stack_size == 0) {
        res.emplace_back(raw_data + begin, length - begin);
    }
}

// Cast string to array<ANY>
StatusOr<ColumnPtr> CastStringToArray::evaluate_checked(ExprContext* context, Chunk* input_chunk) {
    ASSIGN_OR_RETURN(ColumnPtr column, _children[0]->evaluate_checked(context, input_chunk));
    if (column->only_null()) {
        return ColumnHelper::create_const_null_column(column->size());
    }

    LogicalType element_type = _cast_elements_expr->type().type;
    ColumnViewer<TYPE_VARCHAR> src(column);
    UInt32Column::Ptr offsets = UInt32Column::create();
    NullColumn::Ptr null_column = NullColumn::create();

    std::vector<char> stack;

    // 1. Split string with ',' delimiter
    uint32_t offset = 0;
    bool has_null = false;
    ColumnBuilder<TYPE_VARCHAR> slice_builder(src.size());
    for (size_t i = 0; i < src.size(); i++) {
        offsets->append(offset);
        if (src.is_null(i)) {
            has_null = true;
            null_column->append(1);
            continue;
        }
        Slice str = src.value(i);

        // return null if not valid array
        if (!is_valid_array(str, stack)) {
            if (_throw_exception_if_err) {
                return Status::InternalError(fmt::format("invalid array input: {}", str));
            } else {
                has_null = true;
                null_column->append(1);
            }
            continue;
        }
        null_column->append(0);

        auto trimed = _trim(str);
        auto slice = strip_array_wrapper(trimed);

        if (!slice.empty()) {
            std::vector<Slice> splited_slice;
            array_delimeter_split(slice, splited_slice, stack);

            // Unquote slice for string type
            if (element_type == TYPE_VARCHAR || element_type == TYPE_CHAR) {
                for (auto& piece : splited_slice) {
                    slice_builder.append(_unquote(piece));
                }
            } else {
                for (auto& piece : splited_slice) {
                    slice_builder.append(piece);
                }
            }

            offset += splited_slice.size();
        }
    }
    offsets->append(offset);

    // 2. Cast string to specified type
    ColumnPtr elements = slice_builder.build_nullable_column();
    if (element_type != TYPE_VARCHAR && element_type != TYPE_CHAR) {
        ChunkPtr chunk = std::make_shared<Chunk>();
        SlotId slot_id = down_cast<ColumnRef*>(_cast_elements_expr->get_child(0))->slot_id();
        chunk->append_column(elements, slot_id);
        ASSIGN_OR_RETURN(auto cast_res, _cast_elements_expr->evaluate_checked(context, chunk.get()));
        elements = ColumnHelper::cast_to_nullable_column(cast_res);
    }

    // 3. Assemble elements into array column
    ColumnPtr res = ArrayColumn::create(elements, offsets);

    if (column->is_nullable() || has_null) {
        res = NullableColumn::create(res, null_column);
    }
    // Wrap constant column if source column is constant.
    if (column->is_constant()) {
        res = ConstColumn::create(res, column->size());
    }

    return res;
}

Slice CastStringToArray::_trim(Slice slice) const {
    while (slice.starts_with(" ")) {
        slice.remove_prefix(1);
    }
    while (slice.ends_with(" ")) {
        slice.remove_suffix(1);
    }
    return slice;
}

Slice CastStringToArray::_unquote(Slice slice) const {
    slice = _trim(slice);

    if ((slice.starts_with("\"") && slice.ends_with("\"")) || (slice.starts_with("'") && slice.ends_with("'"))) {
        slice.remove_prefix(1);
        slice.remove_suffix(1);
    }
    return slice;
}

StatusOr<ColumnPtr> CastJsonToArray::evaluate_checked(ExprContext* context, Chunk* input_chunk) {
    ASSIGN_OR_RETURN(ColumnPtr column, _children[0]->evaluate_checked(context, input_chunk));
    if (column->only_null()) {
        return ColumnHelper::create_const_null_column(column->size());
    }

    LogicalType element_type = _cast_elements_expr->type().type;
    ColumnViewer<TYPE_JSON> src(column);
    UInt32Column::Ptr offsets = UInt32Column::create();
    NullColumn::Ptr null_column = NullColumn::create();

    // 1. Cast JsonArray to ARRAY<JSON>
    uint32_t offset = 0;
    ColumnBuilder<TYPE_JSON> json_column_builder(src.size());
    for (size_t i = 0; i < src.size(); i++) {
        offsets->append(offset);
        if (src.is_null(i)) {
            null_column->append(1);
            continue;
        }
        const JsonValue* json_value = src.value(i);
        if (json_value && json_value->get_type() == JsonType::JSON_ARRAY) {
            vpack::Slice json_slice = json_value->to_vslice();
            DCHECK(json_slice.isArray());
            for (const auto& element : vpack::ArrayIterator(json_slice)) {
                JsonValue element_value(element);
                json_column_builder.append(std::move(element_value));
            }
            offset += json_slice.length();
            null_column->append(0);
        } else {
            null_column->append(1);
        }
    }
    offsets->append(offset);

    // 2. Cast json to specified type
    ColumnPtr elements = json_column_builder.build_nullable_column();
    if (element_type != TYPE_JSON) {
        ChunkPtr chunk = std::make_shared<Chunk>();
        SlotId slot_id = down_cast<ColumnRef*>(_cast_elements_expr->get_child(0))->slot_id();
        chunk->append_column(elements, slot_id);
        ASSIGN_OR_RETURN(auto cast_res, _cast_elements_expr->evaluate_checked(context, chunk.get()));
        elements = ColumnHelper::cast_to_nullable_column(cast_res);
    }

    // 3. Assemble elements into array column
    ColumnPtr res = ArrayColumn::create(elements, offsets);
    if (column->is_nullable()) {
        res = NullableColumn::create(res, null_column);
    }

    // Wrap constant column if source column is constant.
    if (column->is_constant()) {
        res = ConstColumn::create(res, column->size());
    }
    return res;
}

} // namespace starrocks
