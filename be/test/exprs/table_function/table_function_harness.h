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

#pragma once

#include <gtest/gtest.h>

#include <algorithm>
#include <span>
#include <sstream>
#include <string>
#include <vector>

#include "column/column.h"
#include "column/nullable_column.h"
#include "exprs/table_function/table_function.h"
#include "runtime/runtime_state.h"

// A replay of the contract TableFunctionOperator imposes on TableFunction::process(), for testing any
// implementation that consumes a bounded slice of its input per call.
//
// The reason this is shared rather than written per test: an implementation that returns its whole
// expansion in one call satisfies the contract trivially, so none of it is exercised until the
// implementation starts batching - and every way of breaking it then produces *silently wrong data*
// rather than a crash. Both halves of the contract belong in one place:
//
//   * what the harness *checks*  - the invariants every bounded implementation shares (below);
//   * what the harness *emulates* - the operator's own behavior, so that a test can compare the
//     flattened output against what a query would actually return.
//
// What it emulates, matching table_function_operator.cpp:
//   * process() is called repeatedly while the state still owes input rows;
//   * bracket k of a batch's offsets column belongs to input row (processed_rows() before that call)
//     + k, which is how the operator picks the outer-column row (`_input_index_of_first_result +
//     _next_output_row_offset`);
//   * under LEFT JOIN a zero-length bracket becomes one all-NULL output row that consumes no source
//     element - the injection the operator performs while assembling its (bounded) output chunk.
//
// What it checks on every call:
//   * the offsets column is batch-local and starts at 0;
//   * brackets are non-decreasing;
//   * the fn-result columns hold exactly offsets.back() rows when the expanded value is required
//     (when it is not, the operator reads nothing but the bracket counts, so their size is only
//     reported - see DriveResult::max_fn_result_rows - and left for the caller to assert on);
//   * the call made progress, so a bug cannot turn into an infinite pipeline-driver spin.
//
// Anything specific to one function - what a row is supposed to expand to - stays in that function's
// own test, as an independently computed expectation compared against `DriveResult::rows`.
namespace starrocks::table_function_test {

// One output row, rendered. debug_item() prints "NULL" for a null and is type-agnostic, so the same
// harness serves an INT unnest, a BIGINT generate_series and a JSON json_each.
inline std::string render_row(size_t input_row, const std::vector<std::string>& values) {
    std::ostringstream out;
    out << "row=" << input_row << " [";
    for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) {
            out << ",";
        }
        out << values[i];
    }
    out << "]";
    return out.str();
}

struct DriveResult {
    // The flattened output, one entry per output row, in emission order.
    std::vector<std::string> rows;
    // How many process() calls it took, and the largest batch any of them returned. Together these
    // are what says the expansion was actually bounded: max_rows_per_call <= chunk_size, and more
    // than one call whenever the expansion exceeds a chunk.
    size_t process_calls = 0;
    uint32_t max_rows_per_call = 0;
    // Zero-length brackets seen. Under LEFT JOIN these are the ones the operator turns into an
    // injected NULL row; an implementation that emits that row itself (MultiUnnest) should report 0.
    size_t zero_length_brackets = 0;
    // The largest fn-result column any call returned. Only interesting when the expanded value is not
    // required: the operator then reads the column's *type* (clone_empty()) but none of its rows, so an
    // implementation may either skip materializing it (MultiUnnest, UnnestBitmap - 0 here) or hand back
    // a column it already had, for free, by reference (Unnest's zero-copy path - the whole expansion).
    uint32_t max_fn_result_rows = 0;
};

// Drains one input chunk. `state` must already carry its params (set_params()), its is_left_join and
// its is_required flag; `runtime_state->chunk_size()` is the bound under test.
inline DriveResult drive(const TableFunction& fn, RuntimeState* runtime_state, TableFunctionState* state) {
    DriveResult result;
    const size_t input_rows = state->input_rows();
    const bool is_left_join = state->get_is_left_join();
    const bool required = state->is_required();

    while (state->processed_rows() < input_rows) {
        const size_t first_row = state->processed_rows();
        const int64_t offset_before = state->get_offset();

        auto [columns, offsets] = fn.process(runtime_state, state);

        result.process_calls++;
        if (offsets == nullptr || offsets->size() == 0) {
            ADD_FAILURE() << "process() returned no offsets column at input row " << first_row;
            break;
        }
        const std::span<const uint32_t> brackets = offsets->immutable_data();
        EXPECT_EQ(0u, brackets[0]) << "offsets column must be batch-local, at input row " << first_row;
        result.max_rows_per_call = std::max(result.max_rows_per_call, brackets.back());
        for (const auto& column : columns) {
            result.max_fn_result_rows = std::max(result.max_fn_result_rows, static_cast<uint32_t>(column->size()));
            if (required) {
                EXPECT_EQ(brackets.back(), column->size()) << "fn-result column size at input row " << first_row;
            }
        }

        for (size_t b = 0; b + 1 < brackets.size(); ++b) {
            EXPECT_LE(brackets[b], brackets[b + 1]) << "brackets must not decrease";
            if (brackets[b] == brackets[b + 1]) {
                result.zero_length_brackets++;
                if (is_left_join) {
                    // The operator's injection: one all-NULL row that consumes no source element.
                    result.rows.emplace_back(
                            render_row(first_row + b, std::vector<std::string>(required ? columns.size() : 0, "NULL")));
                }
                continue;
            }
            for (uint32_t k = brackets[b]; k < brackets[b + 1]; ++k) {
                std::vector<std::string> values;
                values.reserve(required ? columns.size() : 0);
                if (required) {
                    for (const auto& column : columns) {
                        values.emplace_back(column->debug_item(k));
                    }
                }
                result.rows.emplace_back(render_row(first_row + b, values));
            }
        }

        const bool progressed = state->processed_rows() > first_row || state->get_offset() > offset_before;
        if (!progressed) {
            ADD_FAILURE() << "process() made no progress at input row " << first_row;
            break;
        }
    }
    return result;
}

} // namespace starrocks::table_function_test
