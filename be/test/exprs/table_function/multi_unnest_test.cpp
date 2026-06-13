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

#include "column/array_column.h"
#include "column/column_helper.h"
#include "exprs/table_function/table_function_factory.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks {

namespace {

ColumnPtr create_int_array_column(const std::vector<std::vector<int32_t>>& rows) {
    auto elem = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto offs = UInt32Column::create();
    auto array = ArrayColumn::create(std::move(elem), std::move(offs));
    for (const auto& row : rows) {
        DatumArray datum_row;
        datum_row.reserve(row.size());
        for (int32_t value : row) {
            datum_row.emplace_back(value);
        }
        array->append_datum(datum_row);
    }
    return array;
}

TFunction create_unnest_function(bool is_array_join, bool is_left_join) {
    TFunction fn;
    TTableFunction table_fn;
    if (is_array_join) {
        table_fn.__set_is_array_join(true);
    }
    if (is_left_join) {
        table_fn.__set_is_left_join(true);
    }
    fn.__set_table_fn(table_fn);
    return fn;
}

} // namespace

class MultiUnnestTest : public testing::Test {};

TEST_F(MultiUnnestTest, array_join_mode_rejects_mismatched_array_sizes) {
    const TableFunction* func = get_table_function("unnest", {}, {}, TFunctionBinaryType::BUILTIN);
    ASSERT_NE(nullptr, func);

    auto rt_state = std::make_unique<RuntimeState>();
    rt_state->set_chunk_size(4096);

    Columns input_columns;
    input_columns.emplace_back(create_int_array_column({{1, 2, 3}}));
    input_columns.emplace_back(create_int_array_column({{4, 5}}));

    TableFunctionState* func_state = nullptr;
    TFunction fn = create_unnest_function(true, false);
    ASSERT_OK(func->init(fn, &func_state));
    func_state->set_params(input_columns);
    ASSERT_OK(func->open(rt_state.get(), func_state));

    auto [result_columns, offset_column] = func->process(rt_state.get(), func_state);
    EXPECT_TRUE(result_columns.empty());
    EXPECT_EQ("Sizes of ARRAY-JOIN-ed arrays do not match.", func_state->status().message());

    func->close(rt_state.get(), func_state);
}

TEST_F(MultiUnnestTest, array_join_mode_accepts_matching_array_sizes) {
    const TableFunction* func = get_table_function("unnest", {}, {}, TFunctionBinaryType::BUILTIN);
    ASSERT_NE(nullptr, func);

    auto rt_state = std::make_unique<RuntimeState>();
    rt_state->set_chunk_size(4096);

    Columns input_columns;
    input_columns.emplace_back(create_int_array_column({{1, 2, 3}}));
    input_columns.emplace_back(create_int_array_column({{4, 5, 6}}));

    TableFunctionState* func_state = nullptr;
    TFunction fn = create_unnest_function(true, false);
    ASSERT_OK(func->init(fn, &func_state));
    func_state->set_params(input_columns);
    ASSERT_OK(func->open(rt_state.get(), func_state));

    auto [result_columns, offset_column] = func->process(rt_state.get(), func_state);
    ASSERT_OK(func_state->status());
    ASSERT_EQ(2, result_columns.size());
    ASSERT_EQ(3, result_columns[0]->size());
    ASSERT_EQ(3, result_columns[1]->size());

    func->close(rt_state.get(), func_state);
}

TEST_F(MultiUnnestTest, non_array_join_mode_allows_mismatched_array_sizes) {
    const TableFunction* func = get_table_function("unnest", {}, {}, TFunctionBinaryType::BUILTIN);
    ASSERT_NE(nullptr, func);

    auto rt_state = std::make_unique<RuntimeState>();
    rt_state->set_chunk_size(4096);

    Columns input_columns;
    input_columns.emplace_back(create_int_array_column({{1, 2, 3}}));
    input_columns.emplace_back(create_int_array_column({{4, 5}}));

    TableFunctionState* func_state = nullptr;
    TFunction fn = create_unnest_function(false, false);
    ASSERT_OK(func->init(fn, &func_state));
    func_state->set_params(input_columns);
    ASSERT_OK(func->open(rt_state.get(), func_state));

    auto [result_columns, offset_column] = func->process(rt_state.get(), func_state);
    ASSERT_OK(func_state->status());
    ASSERT_EQ(2, result_columns.size());
    ASSERT_EQ(3, result_columns[0]->size());
    ASSERT_EQ(3, result_columns[1]->size());

    func->close(rt_state.get(), func_state);
}

} // namespace starrocks
