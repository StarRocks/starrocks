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

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "column/json_column.h"
#include "common/logging.h"
#include "exprs/table_function/json_each.h"
#include "exprs/table_function/table_function_harness.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"
#include "types/json_value.h"

namespace starrocks {

class JsonEachCoreTest : public ::testing::Test {
protected:
    void run_json_each(const std::vector<std::string>& inputs, std::vector<std::pair<std::string, std::string>>* rows,
                       std::vector<uint32_t>* offsets) {
        JsonEach function;
        TableFunctionState* state = nullptr;
        ASSERT_OK(function.init(TFunction(), &state));
        ASSERT_NE(nullptr, state);
        ASSERT_OK(function.prepare(state));

        RuntimeState runtime_state;
        runtime_state.set_chunk_size(4096);
        ASSERT_OK(function.open(&runtime_state, state));

        Columns input_columns;
        if (!inputs.empty()) {
            auto json_column = JsonColumn::create();
            for (const auto& input : inputs) {
                auto parsed = JsonValue::parse(input);
                ASSERT_TRUE(parsed.ok()) << parsed.status().to_string();
                json_column->append(std::move(parsed).value());
            }
            input_columns.emplace_back(std::move(json_column));
        }
        state->set_params(std::move(input_columns));

        auto [result_columns, offset_column] = function.process(&runtime_state, state);
        ASSERT_EQ(inputs.size(), state->processed_rows());
        ASSERT_EQ(2, result_columns.size());

        auto key_column = ColumnHelper::cast_to<TYPE_VARCHAR>(result_columns[0]);
        auto value_column = ColumnHelper::cast_to<TYPE_JSON>(result_columns[1]);

        rows->clear();
        for (size_t i = 0; i < result_columns[0]->size(); ++i) {
            rows->emplace_back(key_column->get(i).get_slice().to_string(),
                               value_column->get(i).get_json()->to_string_uncheck());
        }

        offsets->assign(offset_column->immutable_data().begin(), offset_column->immutable_data().end());
        ASSERT_OK(function.close(&runtime_state, state));
    }
};

TEST_F(JsonEachCoreTest, process_object_array_and_scalar) {
    std::vector<std::pair<std::string, std::string>> rows;
    std::vector<uint32_t> offsets;

    run_json_each({R"({"a":1})", R"([3, true])", "null", "42"}, &rows, &offsets);

    EXPECT_EQ((std::vector<std::pair<std::string, std::string>>{{"a", "1"}, {"0", "3"}, {"1", "true"}}), rows);
    EXPECT_EQ((std::vector<uint32_t>{0, 1, 3, 3, 3}), offsets);
}

TEST_F(JsonEachCoreTest, process_empty_input) {
    std::vector<std::pair<std::string, std::string>> rows;
    std::vector<uint32_t> offsets;

    run_json_each({}, &rows, &offsets);

    EXPECT_TRUE(rows.empty());
    EXPECT_EQ((std::vector<uint32_t>{0}), offsets);
}

// JsonEach now emits a bounded slice of the expansion per process() call, cutting inside a row when
// one JSON value alone has more members than a chunk holds. table_function_test::drive() replays the
// contract TableFunctionOperator imposes - which input row each bracket belongs to included - and
// checks the invariants every bounded implementation shares. What the tests below add is the part
// specific to json_each: the flattened result must not depend on chunk_size at all (only the number
// of calls and the per-call size may), and every member must stay attributed to the input row it came
// from. What each member expands *to* is pinned by process_object_array_and_scalar above, which still
// runs the whole thing in a single call.
namespace {

Columns make_json_params(const std::vector<std::string>& inputs) {
    auto json_column = JsonColumn::create();
    for (const auto& input : inputs) {
        auto parsed = JsonValue::parse(input);
        CHECK(parsed.ok()) << parsed.status().to_string();
        json_column->append(std::move(parsed).value());
    }
    Columns params;
    params.emplace_back(std::move(json_column));
    return params;
}

// init() hands back a TableFunctionState whose concrete type is private to the function, so tests
// hold it the way the operator does and let close() delete it.
struct StateHolder {
    explicit StateHolder(const TableFunction& fn) {
        CHECK(fn.init(TFunction(), &state).ok());
        CHECK(fn.prepare(state).ok());
    }
    TableFunctionState* state = nullptr;
};

// The input row a rendered output row was attributed to, i.e. the "row=N" prefix render_row() writes.
size_t rendered_input_row(const std::string& rendered) {
    return static_cast<size_t>(std::stoull(rendered.substr(std::string("row=").size())));
}

// One entry per input row: how many (key, value) pairs it must expand to.
std::vector<size_t> input_rows_of(const std::vector<std::string>& rows, size_t num_input_rows) {
    std::vector<size_t> counts(num_input_rows, 0);
    for (const auto& row : rows) {
        counts[rendered_input_row(row)]++;
    }
    return counts;
}

} // namespace

TEST_F(JsonEachCoreTest, expansion_is_independent_of_chunk_size) {
    // Objects and arrays of several widths, plus the three shapes that expand to nothing: a scalar,
    // an empty object and an empty array.
    const std::vector<std::string> inputs = {
            R"({"a":1,"b":2,"c":3})", "[10,20,30,40,50]", "null", "{}", "[]", R"({"k":{"n":[1,2]}})", "42",
    };
    const std::vector<size_t> expected_members = {3, 5, 0, 0, 0, 1, 0};
    const size_t expected_rows = 9;

    JsonEach fn;
    std::vector<std::string> reference;
    for (int chunk_size : {4096, 1, 2, 3, 5}) {
        RuntimeState runtime_state;
        runtime_state.set_chunk_size(chunk_size);

        StateHolder holder(fn);
        holder.state->set_params(make_json_params(inputs));

        const auto result = table_function_test::drive(fn, &runtime_state, holder.state);
        EXPECT_EQ(expected_rows, result.rows.size()) << "chunk_size=" << chunk_size;
        EXPECT_EQ(expected_members, input_rows_of(result.rows, inputs.size())) << "chunk_size=" << chunk_size;
        EXPECT_LE(result.max_rows_per_call, static_cast<uint32_t>(chunk_size)) << "chunk_size=" << chunk_size;
        if (reference.empty()) {
            reference = result.rows;
        } else {
            EXPECT_EQ(reference, result.rows) << "chunk_size=" << chunk_size;
        }
        // The point of the change: with a chunk smaller than the expansion, the work is spread over
        // several calls instead of one unbounded allocation.
        if (static_cast<size_t>(chunk_size) < expected_rows) {
            EXPECT_GT(result.process_calls, 1u) << "chunk_size=" << chunk_size;
        }
        ASSERT_OK(fn.close(&runtime_state, holder.state));
    }
}

// One value with more members than a chunk holds: it must be cut across calls, and while it is only
// partially emitted processed_rows() must not advance - the operator would otherwise pair the
// remaining members with the next input row's outer columns.
TEST_F(JsonEachCoreTest, one_row_split_across_calls_keeps_its_input_row) {
    const std::vector<std::string> inputs = {"[0,1,2,3,4,5,6,7,8,9]", R"({"x":1,"y":2})"};

    RuntimeState runtime_state;
    runtime_state.set_chunk_size(4);

    JsonEach fn;
    StateHolder holder(fn);
    holder.state->set_params(make_json_params(inputs));

    // First call on its own, to pin the cursor state in the middle of the row.
    auto [first_columns, first_offsets] = fn.process(&runtime_state, holder.state);
    EXPECT_EQ(4u, first_columns[0]->size());
    EXPECT_EQ(4u, first_columns[1]->size());
    EXPECT_EQ(0u, holder.state->processed_rows());
    EXPECT_EQ(4, holder.state->get_offset());
    // A row contributes at most one bracket per call, otherwise two brackets would map to two input
    // rows: one leading 0 plus one closing bracket.
    EXPECT_EQ(2u, first_offsets->size());

    holder.state->set_params(make_json_params(inputs));
    const auto result = table_function_test::drive(fn, &runtime_state, holder.state);
    EXPECT_EQ((std::vector<size_t>{10, 2}), input_rows_of(result.rows, inputs.size()));
    // An array member's key is its index in the whole array, not a counter local to the batch.
    EXPECT_NE(std::string::npos, result.rows[9].find("'9'"));
    EXPECT_EQ(3u, result.process_calls); // 10 members / chunk 4, the second row rides along
    EXPECT_EQ(4u, result.max_rows_per_call);
    ASSERT_OK(fn.close(&runtime_state, holder.state));
}

// With is_required() false the operator reads nothing but the bracket counts, so no key has to be
// copied and no value sub-tree extracted. The harness asserts the result columns stay empty; what is
// checked here is that the bracket-to-input-row mapping is unchanged.
TEST_F(JsonEachCoreTest, counts_only_when_result_is_not_required) {
    const std::vector<std::string> inputs = {R"({"a":1,"b":2,"c":3})", "null", "[7,8]", "{}"};
    const std::vector<size_t> expected_members = {3, 0, 2, 0};

    JsonEach fn;
    for (int chunk_size : {1, 2, 3, 4096}) {
        RuntimeState runtime_state;
        runtime_state.set_chunk_size(chunk_size);

        StateHolder holder(fn);
        holder.state->set_is_required(false);
        holder.state->set_params(make_json_params(inputs));

        const auto result = table_function_test::drive(fn, &runtime_state, holder.state);
        EXPECT_EQ(expected_members, input_rows_of(result.rows, inputs.size())) << "chunk_size=" << chunk_size;
        EXPECT_LE(result.max_rows_per_call, static_cast<uint32_t>(chunk_size)) << "chunk_size=" << chunk_size;
        ASSERT_OK(fn.close(&runtime_state, holder.state));
    }
}

// set_params() resets processed_rows() but leaves the intra-row cursor to on_new_params(). Without
// that reset, a chunk arriving while a row is only half emitted - reset_state(), which calls
// set_params(Columns{}), or a re-primed pipeline - makes the next chunk's first row resume from the
// leftover member index, silently skipping that many of its members.
TEST_F(JsonEachCoreTest, intra_row_cursor_is_reset_for_new_params) {
    const std::vector<std::string> first_chunk = {"[0,1,2,3,4,5]"};
    const std::vector<std::string> second_chunk = {R"({"a":1,"b":2,"c":3})", "[9]"};

    RuntimeState runtime_state;
    runtime_state.set_chunk_size(2);

    JsonEach fn;
    StateHolder holder(fn);

    // Abandon the first chunk mid-row.
    holder.state->set_params(make_json_params(first_chunk));
    (void)fn.process(&runtime_state, holder.state);
    ASSERT_EQ(0u, holder.state->processed_rows());
    ASSERT_EQ(2, holder.state->get_offset());

    holder.state->set_params(make_json_params(second_chunk));
    EXPECT_EQ(0, holder.state->get_offset());
    const auto result = table_function_test::drive(fn, &runtime_state, holder.state);
    EXPECT_EQ((std::vector<size_t>{3, 1}), input_rows_of(result.rows, second_chunk.size()));
    ASSERT_OK(fn.close(&runtime_state, holder.state));
}

} // namespace starrocks
