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

#include "exprs/gin_functions.h"

#include <CLucene.h>

#include "column/array_column.h"
#include "column/column_viewer.h"
#include "storage/index/inverted/inverted_index_option.h"
#include "storage/index/inverted/tokenizer/tokenizer.h"
#include "storage/index/inverted/tokenizer/tokenizer_factory.h"

namespace starrocks {

Status GinFunctions::tokenize_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }

    auto column = context->get_constant_column(0);
    RETURN_IF(column == nullptr, Status::InvalidArgument("Tokenize function requires constant parameter"));
    auto method = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);

    const auto parser_type = get_inverted_index_parser_type_from_string(method.to_string());
    auto analyzer = TokenizerFactory::create(parser_type);
    context->set_function_state(scope, analyzer.release());
    return Status::OK();
}

Status GinFunctions::tokenize_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        const auto* analyzer = static_cast<Tokenizer*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
        delete analyzer;
    }
    return Status::OK();
}

StatusOr<ColumnPtr> GinFunctions::tokenize(FunctionContext* context, const starrocks::Columns& columns) {
    auto* analyzer = static_cast<Tokenizer*>(context->get_function_state(FunctionContext::THREAD_LOCAL));

    if (columns.size() != 2) {
        return Status::InvalidArgument("Tokenize function only call by tokenize('<index_type>', str_column)");
    }

    ColumnViewer<TYPE_VARCHAR> value_viewer(columns[1]);
    size_t num_rows = value_viewer.size();

    // Array Offset
    int offset = 0;
    UInt32Column::Ptr array_offsets = UInt32Column::create();
    array_offsets->reserve(num_rows + 1);

    // Array Binary
    BinaryColumn::Ptr array_binary_column = BinaryColumn::create();

    NullColumnPtr null_array = NullColumn::create();

    for (int row = 0; row < num_rows; ++row) {
        array_offsets->append(offset);

        if (value_viewer.is_null(row) || value_viewer.value(row).empty()) {
            null_array->append(1);
        } else {
            null_array->append(0);
            auto data = value_viewer.value(row);
            ASSIGN_OR_RETURN(auto tokens, analyzer->tokenize(&data));
            for (const auto& token : tokens) {
                offset++;
                array_binary_column->append(token.text);
            }
        }
    }
    array_offsets->append(offset);
    auto result_array = ArrayColumn::create(NullableColumn::create(array_binary_column, NullColumn::create(offset, 0)),
                                            array_offsets);
    return NullableColumn::create(result_array, null_array);
}

} // namespace starrocks
#include "gen_cpp/opcode/GinFunctions.inc"
