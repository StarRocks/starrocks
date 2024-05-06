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
#include <CLucene/analysis/LanguageBasedAnalyzer.h>

#include "column/array_column.h"
#include "column/column_viewer.h"
#include "storage/index/inverted/inverted_index_analyzer.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/inverted_index_context.h"
#include "storage/index/inverted/inverted_index_option.h"
#include "util/faststring.h"

namespace starrocks {

Status GinFunctions::tokenize_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }

    auto column = context->get_constant_column(0);
    auto method_col = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);

    InvertedIndexCtx* inverted_index_ctx = new InvertedIndexCtx();

    InvertedIndexParserType parser_type = get_inverted_index_parser_type_from_string(method_col.to_string());
    ASSIGN_OR_RETURN(auto analyzer, InvertedIndexAnalyzer::create_analyzer(parser_type));

    inverted_index_ctx->setParserType(parser_type);
    inverted_index_ctx->setAnalyzer(std::move(analyzer));

    context->set_function_state(scope, inverted_index_ctx);
    return Status::OK();
}

Status GinFunctions::tokenize_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        auto* inverted_index_ctx =
                reinterpret_cast<InvertedIndexCtx*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
        delete inverted_index_ctx;
    }
    return Status::OK();
}

StatusOr<ColumnPtr> GinFunctions::tokenize(FunctionContext* context, const starrocks::Columns& columns) {
    auto* inverted_index_ctx =
            reinterpret_cast<InvertedIndexCtx*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
    if (inverted_index_ctx == nullptr) {
        return Status::InternalError("InvertedIndexCtx is null.");
    }

    if (columns.size() != 2) {
        return Status::InvalidArgument("Tokenize function only call by tokenize('<index_type>', str_column)");
    }

    ColumnViewer<TYPE_VARCHAR> value_viewer(columns[1]);
    size_t num_rows = value_viewer.size();

    //Array Offset
    int offset = 0;
    UInt32Column::Ptr array_offsets = UInt32Column::create();
    array_offsets->reserve(num_rows + 1);

    //Array Binary
    BinaryColumn::Ptr array_binary_column = BinaryColumn::create();

    NullColumnPtr null_array = NullColumn::create();

    std::string field_name("tokenize");

    for (int row = 0; row < num_rows; ++row) {
        array_offsets->append(offset);

        if (value_viewer.is_null(row) || value_viewer.value(row).empty()) {
            null_array->append(1);
        } else {
            null_array->append(0);
            auto data = value_viewer.value(row);
            std::string slice_str(data.data, data.get_size());
            ASSIGN_OR_RETURN(auto tokens,
                             InvertedIndexAnalyzer::get_analyse_result(slice_str, field_name, inverted_index_ctx));
            for (auto& token : tokens) {
                offset++;
                array_binary_column->append(Slice(std::move(token)));
            }
        }
    }
    array_offsets->append(offset);
    auto result_array = ArrayColumn::create(NullableColumn::create(array_binary_column, NullColumn::create(offset, 0)),
                                            array_offsets);
    return NullableColumn::create(result_array, null_array);
}

} // namespace starrocks
