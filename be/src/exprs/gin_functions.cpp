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
#include <memory>
#include <string>

#include "column/array_column.h"
#include "column/column_viewer.h"
#include "column/datum.h"
#include "runtime/runtime_state.h"
#include "storage/index/inverted/tantivy/tantivy_ffi_guards.h"

namespace starrocks {

namespace {

struct TokenizeState {
    bool use_tantivy = true;
    TantivyAnalyzerGuard analyzer;
    std::unique_ptr<lucene::analysis::Analyzer> clucene_analyzer;
};

Status configure_tantivy_tokenizer(const Slice& method, TokenizeState* state) {
    std::string definition = method.to_string();
    tb::RustResult result = tb::tantivy_create_analyzer(definition.c_str(), "");
    TantivyResultGuard result_guard(result);
    RETURN_IF_ERROR(tantivy_status_from_error(result));
    state->analyzer = TantivyAnalyzerGuard(result.value.ptr);
    return Status::OK();
}

Status configure_clucene_tokenizer(const Slice& method, TokenizeState* state) {
    if (method == "english") {
        state->clucene_analyzer = std::make_unique<lucene::analysis::SimpleAnalyzer>();
    } else if (method == "standard") {
        state->clucene_analyzer = std::make_unique<lucene::analysis::standard::StandardAnalyzer>();
    } else if (method == "chinese" || method == "cjk") {
        auto analyzer = std::make_unique<lucene::analysis::LanguageBasedAnalyzer>();
        analyzer->setLanguage(L"cjk");
        analyzer->setStem(false);
        state->clucene_analyzer = std::move(analyzer);
    } else {
        return Status::NotSupported("Unknown CLucene tokenizer '" + method.to_string() +
                                    "'. Supported tokenizers are: 'english', 'standard', 'chinese', 'cjk'.");
    }
    return Status::OK();
}

Status append_tantivy_tokens(const TokenizeState& state, const Slice& data, BinaryColumn* elements, uint32_t* offset) {
    tb::RustStringArray output{};
    tb::RustResult result = tb::tantivy_analyzer_tokenize(
            state.analyzer.get(), reinterpret_cast<const uint8_t*>(data.data), data.size, &output);
    TantivyResultGuard result_guard(result);
    if (!result.success) {
        if (output.ptr != nullptr) {
            tb::tantivy_free_string_array(output);
        }
        return tantivy_status_from_error(result);
    }

    for (size_t i = 0; i < output.len; ++i) {
        elements->append(Slice(output.ptr[i]));
        ++*offset;
    }
    tb::tantivy_free_string_array(output);
    return Status::OK();
}

Status append_clucene_tokens(TokenizeState* state, const Slice& data, BinaryColumn* elements, uint32_t* offset) {
    std::string text(data.data, data.size);
    std::wstring wide_text = boost::locale::conv::utf_to_utf<wchar_t>(text);
    lucene::util::StringReader reader(wide_text.c_str(), wide_text.size(), false);
    auto* stream = state->clucene_analyzer->reusableTokenStream(L"", &reader);
    lucene::analysis::Token token;
    while (stream->next(&token)) {
        if (token.termLength() == 0) {
            continue;
        }
        std::string term = boost::locale::conv::utf_to_utf<char>(std::wstring(token.termBuffer(), token.termLength()));
        elements->append(Slice(term));
        ++*offset;
    }
    return Status::OK();
}

} // namespace

Status GinFunctions::tokenize_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }

    auto tokenizer_column = context->get_constant_column(0);
    RETURN_IF(tokenizer_column == nullptr || tokenizer_column->only_null(),
              Status::InvalidArgument("tokenize() requires a non-NULL constant tokenizer name"));

    auto state = std::make_unique<TokenizeState>();
    if (context->state() != nullptr) {
        state->use_tantivy = context->state()->query_options().use_tantivy_tokenize;
    }

    auto method = ColumnHelper::get_const_value<TYPE_VARCHAR>(tokenizer_column);
    if (!method.empty() && method.data[0] == '{') {
        state->use_tantivy = true;
    }
    if (state->use_tantivy) {
        RETURN_IF_ERROR(configure_tantivy_tokenizer(method, state.get()));
    } else {
        RETURN_IF_ERROR(configure_clucene_tokenizer(method, state.get()));
    }

    context->set_function_state(scope, state.release());
    return Status::OK();
}

Status GinFunctions::tokenize_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        auto* state = reinterpret_cast<TokenizeState*>(context->get_function_state(scope));
        delete state;
        context->set_function_state(scope, nullptr);
    }
    return Status::OK();
}

StatusOr<ColumnPtr> GinFunctions::tokenize(FunctionContext* context, const Columns& columns) {
    if (columns.size() != 2) {
        return Status::InvalidArgument("tokenize() must be called as tokenize('<tokenizer>', content)");
    }
    auto* state = reinterpret_cast<TokenizeState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
    RETURN_IF(state == nullptr, Status::InternalError("tokenize() state is not initialized"));

    ColumnViewer<TYPE_VARCHAR> value_viewer(columns[1]);
    const size_t num_rows = value_viewer.size();
    uint32_t offset = 0;

    auto offsets = UInt32Column::create();
    offsets->reserve(num_rows + 1);
    auto elements = BinaryColumn::create();
    auto nulls = NullColumn::create();
    nulls->reserve(num_rows);

    for (size_t row = 0; row < num_rows; ++row) {
        offsets->append(offset);
        if (value_viewer.is_null(row) || value_viewer.value(row).empty()) {
            nulls->append(1);
            continue;
        }

        nulls->append(0);
        auto data = value_viewer.value(row);
        if (state->use_tantivy) {
            RETURN_IF_ERROR(append_tantivy_tokens(*state, data, elements.get(), &offset));
        } else {
            RETURN_IF_ERROR(append_clucene_tokens(state, data, elements.get(), &offset));
        }
    }
    offsets->append(offset);

    auto nullable_elements = NullableColumn::create(elements, NullColumn::create(offset, 0));
    auto result = ArrayColumn::create(nullable_elements, offsets);
    return NullableColumn::create(result, nulls);
}

} // namespace starrocks
