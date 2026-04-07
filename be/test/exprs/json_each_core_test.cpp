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
#include "exprs/table_function/json_each.h"
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

} // namespace starrocks
