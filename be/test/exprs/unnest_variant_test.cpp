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

#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string>
#include <vector>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/variant_column.h"
#include "column/variant_encoder.h"
#include "exprs/table_function/table_function_factory.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace {

ColumnPtr variant_elements(const std::vector<std::optional<std::string>>& values) {
    auto data = VariantColumn::create();
    auto nulls = NullColumn::create();
    for (const auto& value : values) {
        if (!value.has_value()) {
            data->append_default();
            nulls->append(1);
        } else {
            auto encoded = VariantEncoder::encode_json_text_to_variant(*value);
            CHECK(encoded.ok()) << encoded.status().to_string();
            data->append(encoded.value());
            nulls->append(0);
        }
    }
    return NullableColumn::create(std::move(data), std::move(nulls));
}

UInt32Column::Ptr array_offsets(std::initializer_list<uint32_t> values) {
    auto offsets = UInt32Column::create();
    for (uint32_t value : values) {
        offsets->append(value);
    }
    return offsets;
}

void expect_offsets(const UInt32Column::Ptr& actual, const std::vector<uint32_t>& expected) {
    EXPECT_EQ(expected, std::vector<uint32_t>(actual->get_data().begin(), actual->get_data().end()));
}

void expect_json(const ColumnPtr& column, size_t row, const std::string& expected) {
    ASSERT_FALSE(column->is_null(row));
    const auto* variants = down_cast<const VariantColumn*>(ColumnHelper::get_data_column(column.get()));
    VariantRowValue buffer;
    const auto* value = variants->get_row_value(row, &buffer);
    ASSERT_NE(nullptr, value);
    auto json = value->to_json();
    ASSERT_TRUE(json.ok()) << json.status().to_string();
    EXPECT_EQ(expected, json.value());
}

} // namespace

class UnnestVariantTest : public testing::Test {
protected:
    void SetUp() override {
        // Resolve the signature used by both TableFunctionNode and the pipeline operator.
        // Instantiating Unnest directly would miss the registration regression.
        _function = get_table_function("unnest", {TYPE_ARRAY}, {TYPE_VARIANT});
        ASSERT_NE(nullptr, _function);
        ASSERT_TRUE(_function->init(TFunction(), &_state).ok());
        ASSERT_TRUE(_function->prepare(_state).ok());
        ASSERT_TRUE(_function->open(&_runtime_state, _state).ok());
    }

    void TearDown() override {
        if (_state != nullptr) {
            EXPECT_TRUE(_function->close(&_runtime_state, _state).ok());
        }
    }

    std::pair<Columns, UInt32Column::Ptr> process(ColumnPtr input, bool left_join = false, bool required = true) {
        _state->set_is_left_join(left_join);
        _state->set_is_required(required);
        _state->set_params({std::move(input)});
        return _function->process(&_runtime_state, _state);
    }

    const TableFunction* _function = nullptr;
    TableFunctionState* _state = nullptr;
    RuntimeState _runtime_state;
};

TEST_F(UnnestVariantTest, ReusesElementsForNonNullArrays) {
    auto elements = variant_elements({R"({"k":1})", "null", std::nullopt, R"("x")"});
    auto input = ArrayColumn::create(elements, array_offsets({0, 2, 2, 4}));
    auto [columns, offsets] = process(input);
    ASSERT_EQ(1, columns.size());
    EXPECT_EQ(input->elements_column().get(), columns[0].get());
    expect_offsets(offsets, {0, 2, 2, 4});
    EXPECT_EQ(3, _state->processed_rows());
    expect_json(columns[0], 0, R"({"k":1})");
    expect_json(columns[0], 1, "null");
    EXPECT_TRUE(columns[0]->is_null(2));
    expect_json(columns[0], 3, R"("x")");
}

TEST_F(UnnestVariantTest, NullableAndEmptyArrays) {
    auto elements = variant_elements({R"({"k":1})", "null", R"({"ignored":99})", std::nullopt, R"("x")"});
    auto arrays = ArrayColumn::create(elements, array_offsets({0, 2, 3, 3, 5}));
    auto nulls = NullColumn::create(4, 0);
    nulls->get_data()[1] = 1;
    auto input = NullableColumn::create(std::move(arrays), std::move(nulls));

    auto [columns, offsets] = process(input);
    ASSERT_EQ(1, columns.size());
    ASSERT_EQ(4, columns[0]->size());
    expect_offsets(offsets, {0, 2, 2, 2, 4});
    expect_json(columns[0], 0, R"({"k":1})");
    expect_json(columns[0], 1, "null");
    EXPECT_TRUE(columns[0]->is_null(2));
    expect_json(columns[0], 3, R"("x")");

    auto [left_columns, left_offsets] = process(input, true);
    ASSERT_EQ(1, left_columns.size());
    ASSERT_EQ(6, left_columns[0]->size());
    expect_offsets(left_offsets, {0, 2, 3, 4, 6});
    expect_json(left_columns[0], 0, R"({"k":1})");
    expect_json(left_columns[0], 1, "null");
    for (size_t row : {2, 3, 4}) {
        EXPECT_TRUE(left_columns[0]->is_null(row));
    }
    expect_json(left_columns[0], 5, R"("x")");

    auto [unused_columns, count_offsets] = process(input, false, false);
    expect_offsets(count_offsets, {0, 2, 2, 2, 4});
    EXPECT_EQ(0, unused_columns[0]->size());
}

TEST_F(UnnestVariantTest, TypedOnlyShreddedElements) {
    auto values = Int64Column::create();
    for (int64_t value : {10, 20, 30}) {
        values->append(value);
    }
    MutableColumns typed;
    typed.emplace_back(NullableColumn::create(std::move(values), NullColumn::create(3, 0)));
    auto variants = VariantColumn::create();
    variants->set_shredded_columns({"n"}, {TypeDescriptor(TYPE_BIGINT)}, std::move(typed), nullptr, nullptr);
    auto elements = NullableColumn::create(std::move(variants), NullColumn::create(3, 0));
    auto arrays = ArrayColumn::create(elements, array_offsets({0, 2, 3}));
    auto nulls = NullColumn::create(2, 0);
    nulls->get_data()[1] = 1;
    auto input = NullableColumn::create(std::move(arrays), std::move(nulls));
    auto [columns, offsets] = process(input, true);
    ASSERT_EQ(1, columns.size());
    ASSERT_EQ(3, columns[0]->size());
    expect_offsets(offsets, {0, 2, 3});
    expect_json(columns[0], 0, R"({"n":10})");
    expect_json(columns[0], 1, R"({"n":20})");
    EXPECT_TRUE(columns[0]->is_null(2));
    const auto* output = down_cast<const VariantColumn*>(ColumnHelper::get_data_column(columns[0].get()));
    EXPECT_TRUE(output->is_shredded_variant());
}

TEST_F(UnnestVariantTest, NestedArrays) {
    auto leaves = variant_elements({"1", R"({"k":2})", std::nullopt});
    auto inner = ArrayColumn::create(leaves, array_offsets({0, 2, 3}));
    auto inner_elements = NullableColumn::create(std::move(inner), NullColumn::create(2, 0));
    auto outer = ArrayColumn::create(inner_elements, array_offsets({0, 2}));
    const auto* outer_function = get_table_function("unnest", {TYPE_ARRAY}, {TYPE_ARRAY});
    ASSERT_EQ(_function, outer_function);
    _state->set_params({outer});
    auto [inner_columns, outer_offsets] = outer_function->process(&_runtime_state, _state);
    ASSERT_EQ(1, inner_columns.size());
    expect_offsets(outer_offsets, {0, 2});
    auto [columns, offsets] = process(inner_columns[0]);
    ASSERT_EQ(1, columns.size());
    ASSERT_EQ(3, columns[0]->size());
    expect_offsets(offsets, {0, 2, 3});
    expect_json(columns[0], 0, "1");
    expect_json(columns[0], 1, R"({"k":2})");
    EXPECT_TRUE(columns[0]->is_null(2));
}

} // namespace starrocks
