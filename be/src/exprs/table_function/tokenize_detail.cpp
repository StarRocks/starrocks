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

#include "exprs/table_function/tokenize_detail.h"

#include "column/column_viewer.h"
#include "storage/index/inverted/tantivy/tantivy_ffi_guards.h"

namespace starrocks {

Status TokenizeDetail::init(const TFunction& fn, TableFunctionState** state) const {
    *state = new TableFunctionState();
    return Status::OK();
}

Status TokenizeDetail::close(RuntimeState* runtime_state, TableFunctionState* state) const {
    delete state;
    return Status::OK();
}

std::pair<Columns, UInt32Column::Ptr> TokenizeDetail::process(RuntimeState* runtime_state,
                                                              TableFunctionState* state) const {
    Columns result;
    auto terms = BinaryColumn::create();
    auto positions = Int64Column::create();
    auto position_lengths = Int64Column::create();
    auto start_offsets = Int64Column::create();
    auto end_offsets = Int64Column::create();
    auto token_types = BinaryColumn::create();
    result = {terms, positions, position_lengths, start_offsets, end_offsets, token_types};

    auto offsets = UInt32Column::create();
    offsets->append(0);
    if (state->get_columns().size() != 2) {
        state->set_status(Status::InvalidArgument("tokenize_detail() requires analyzer and content arguments"));
        return {std::move(result), std::move(offsets)};
    }

    ColumnViewer<TYPE_VARCHAR> analyzer_viewer(state->get_columns()[0]);
    ColumnViewer<TYPE_VARCHAR> text_viewer(state->get_columns()[1]);
    const size_t rows = text_viewer.size();
    state->set_processed_rows(rows);
    uint32_t output_rows = 0;
    std::string current_definition;
    TantivyAnalyzerGuard analyzer;
    for (size_t row = 0; row < rows; ++row) {
        if (analyzer_viewer.is_null(row) || text_viewer.is_null(row) || text_viewer.value(row).empty()) {
            offsets->append(output_rows);
            continue;
        }

        std::string definition = analyzer_viewer.value(row).to_string();
        if (!analyzer || definition != current_definition) {
            tb::RustResult create_result = tb::tantivy_create_analyzer(definition.c_str(), "");
            TantivyResultGuard create_guard(create_result);
            Status status = tantivy_status_from_error(create_result);
            if (!status.ok()) {
                state->set_status(status);
                return {std::move(result), std::move(offsets)};
            }
            analyzer = TantivyAnalyzerGuard(create_result.value.ptr);
            current_definition = definition;
        }

        Slice text = text_viewer.value(row);
        tb::RustTokenArray tokens{};
        tb::RustResult tokenize_result = tb::tantivy_analyzer_tokenize_detail(
                analyzer.get(), reinterpret_cast<const uint8_t*>(text.data), text.size, &tokens);
        TantivyResultGuard tokenize_guard(tokenize_result);
        if (!tokenize_result.success) {
            if (tokens.ptr != nullptr) {
                tb::tantivy_free_token_array(tokens);
            }
            state->set_status(tantivy_status_from_error(tokenize_result));
            return {std::move(result), std::move(offsets)};
        }
        TantivyTokenArrayGuard tokens_guard(tokens);
        for (size_t i = 0; i < tokens.len; ++i) {
            const tb::RustToken& token = tokens.ptr[i];
            terms->append(Slice(token.term));
            positions->append(static_cast<int64_t>(token.position));
            position_lengths->append(static_cast<int64_t>(token.position_length));
            start_offsets->append(static_cast<int64_t>(token.start_offset));
            end_offsets->append(static_cast<int64_t>(token.end_offset));
            token_types->append(Slice(token.token_type));
            ++output_rows;
        }
        offsets->append(output_rows);
    }
    return {std::move(result), std::move(offsets)};
}

} // namespace starrocks
