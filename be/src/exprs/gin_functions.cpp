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

#include <boost/locale/encoding_utf.hpp>

#include "column/array_column.h"
#include "column/column_viewer.h"
#include "column/datum.h"

namespace starrocks {

Status GinFunctions::tokenize_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }

    auto column = context->get_constant_column(0);
    RETURN_IF(column == nullptr, Status::InvalidArgument("Tokenize function requires constant parameter"));
    auto method = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);

    lucene::analysis::Analyzer* analyzer;

    if (method == "english") {
        analyzer = _CLNEW lucene::analysis::SimpleAnalyzer();
    } else if (method == "standard") {
        analyzer = _CLNEW lucene::analysis::standard::StandardAnalyzer();
    } else if (method == "chinese") {
        auto* canalyzer = _CLNEW lucene::analysis::LanguageBasedAnalyzer();
        canalyzer->setLanguage(L"cjk");
        canalyzer->setStem(false);
        analyzer = canalyzer;
    } else {
        return Status::NotSupported("Unknown method '" + method.to_string() +
                                    "'. Supported methods are: 'english', 'standard', 'chinese'.");
    }

    context->set_function_state(scope, analyzer);

    return Status::OK();
}

Status GinFunctions::tokenize_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        auto* analyzer = reinterpret_cast<lucene::analysis::Analyzer*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        delete analyzer;
    }
    return Status::OK();
}

StatusOr<ColumnPtr> GinFunctions::tokenize(FunctionContext* context, const starrocks::Columns& columns) {
    auto* analyzer =
            reinterpret_cast<lucene::analysis::Analyzer*>(context->get_function_state(FunctionContext::THREAD_LOCAL));

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
            std::string slice_str(data.data, data.get_size());
            std::wstring wstr = boost::locale::conv::utf_to_utf<wchar_t>(slice_str);
            lucene::util::StringReader reader(wstr.c_str(), wstr.size(), false);
            auto stream = analyzer->reusableTokenStream(L"", &reader);
            lucene::analysis::Token token;
            while (stream->next(&token)) {
                if (token.termLength() != 0) {
                    offset++;
                    std::string str =
                            boost::locale::conv::utf_to_utf<char>(std::wstring(token.termBuffer(), token.termLength()));
                    array_binary_column->append(Slice(std::move(str)));
                }
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
