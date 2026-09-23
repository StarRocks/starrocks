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
#include "exprs/function_context.h"
#include "types/datum.h"

namespace starrocks {

namespace {
// Build a CLucene analyzer for the given tokenize method, or nullptr if the method is unknown.
lucene::analysis::Analyzer* make_tokenize_analyzer(const Slice& method) {
    if (method == "english") {
        return _CLNEW lucene::analysis::SimpleAnalyzer();
    } else if (method == "standard") {
        return _CLNEW lucene::analysis::standard::StandardAnalyzer();
    } else if (method == "chinese") {
        auto* canalyzer = _CLNEW lucene::analysis::LanguageBasedAnalyzer();
        canalyzer->setLanguage(L"cjk");
        canalyzer->setStem(false);
        return canalyzer;
    }
    return nullptr;
}

// A CLucene Analyzer is not thread-safe (it reuses a per-instance token stream), so each worker
// thread gets its own via FunctionContext::get_or_create_thread_state instead of a per-thread
// FunctionContext clone.
struct GinTokenizeThreadState : FunctionThreadState {
    lucene::analysis::Analyzer* analyzer = nullptr;
    ~GinTokenizeThreadState() override { delete analyzer; }
};
} // namespace

Status GinFunctions::tokenize_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    // Validate the (constant) method once, so an unknown method fails at prepare time. The
    // per-thread analyzer itself is created lazily during evaluation (see tokenize()).
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    // Defence in depth: the FE analyzer already requires a string literal here. get_const_value()
    // casts the constant's data column straight to a BinaryColumn, so a non-constant or NULL
    // argument would be a wild read rather than an error.
    RETURN_IF(!context->is_notnull_constant_column(0),
              Status::InvalidArgument("Tokenize function requires a non-null constant string parameter"));
    auto column = context->get_constant_column(0);
    auto method = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);
    if (method != "english" && method != "standard" && method != "chinese") {
        return Status::NotSupported("Unknown method '" + method.to_string() +
                                    "'. Supported methods are: 'english', 'standard', 'chinese'.");
    }

    return Status::OK();
}

Status GinFunctions::tokenize_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    // Per-thread analyzers live in the FunctionContext's thread-state registry and are freed
    // when the FunctionContext is destroyed; nothing to free here.
    return Status::OK();
}

StatusOr<ColumnPtr> GinFunctions::tokenize(FunctionContext* context, const starrocks::Columns& columns) {
    if (columns.size() != 2) {
        return Status::InvalidArgument("Tokenize function only call by tokenize('<index_type>', str_column)");
    }

    RETURN_IF(!context->is_notnull_constant_column(0),
              Status::InvalidArgument("Tokenize function requires a non-null constant string parameter"));
    auto method_column = context->get_constant_column(0);
    auto method = ColumnHelper::get_const_value<TYPE_VARCHAR>(method_column);

    auto* ts = context->get_or_create_thread_state<GinTokenizeThreadState>([&]() {
        auto s = std::make_unique<GinTokenizeThreadState>();
        s->analyzer = make_tokenize_analyzer(method);
        return s;
    });
    if (ts->analyzer == nullptr) {
        return Status::NotSupported("Unknown method '" + method.to_string() +
                                    "'. Supported methods are: 'english', 'standard', 'chinese'.");
    }
    auto* analyzer = ts->analyzer;

    ColumnViewer<TYPE_VARCHAR> value_viewer(columns[1]);
    size_t num_rows = value_viewer.size();

    // Array Offset
    int offset = 0;
    UInt32Column::MutablePtr array_offsets = UInt32Column::create();
    array_offsets->reserve(num_rows + 1);

    // Array Binary
    BinaryColumn::MutablePtr array_binary_column = BinaryColumn::create();

    NullColumn::MutablePtr null_array = NullColumn::create();

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
                    array_binary_column->append(Slice(str));
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
