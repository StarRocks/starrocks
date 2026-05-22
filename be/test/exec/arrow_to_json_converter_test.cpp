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

#include <arrow/array.h>
#include <arrow/builder.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <memory>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "column/json_column.h"
#include "common/status.h"

namespace starrocks {

Status convert_arrow_to_json(const arrow::Array* array, JsonColumn* output, size_t array_start_idx,
                             size_t num_elements);

static std::string compact_json(const JsonColumn& column, size_t idx) {
    std::string value = column.get(idx).get_json()->to_string_uncheck();
    value.erase(std::ranges::remove_if(value, [](unsigned char c) { return std::isspace(c) != 0; }).begin(),
                value.end());
    return value;
}

TEST(ArrowToJsonConverterTest, StringArrayToJsonColumn) {
    arrow::StringBuilder builder;
    ASSERT_OK(builder.Append(R"({"a": 1})"));
    ASSERT_OK(builder.Append("plain"));

    std::shared_ptr<arrow::Array> array;
    ASSERT_OK(builder.Finish(&array));

    auto column = JsonColumn::create();
    ASSERT_OK(convert_arrow_to_json(array.get(), column.get(), 0, array->length()));

    ASSERT_EQ(2, column->size());
    EXPECT_EQ(R"({"a":1})", compact_json(*column, 0));
    EXPECT_EQ(R"("plain")", compact_json(*column, 1));
}

TEST(ArrowToJsonConverterTest, ListStructAndMapToJsonColumn) {
    {
        auto value_builder = std::make_shared<arrow::Int32Builder>();
        arrow::ListBuilder builder(arrow::default_memory_pool(), value_builder);
        ASSERT_OK(builder.Append());
        ASSERT_OK(value_builder->Append(1));
        ASSERT_OK(value_builder->Append(2));

        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder.Finish(&array));

        auto column = JsonColumn::create();
        ASSERT_OK(convert_arrow_to_json(array.get(), column.get(), 0, array->length()));
        ASSERT_EQ(1, column->size());
        EXPECT_EQ("[1,2]", compact_json(*column, 0));
    }

    {
        std::vector fields = {arrow::field("id", arrow::int32()), arrow::field("score", arrow::int32())};
        auto type = arrow::struct_(fields);
        auto id_builder = std::make_shared<arrow::Int32Builder>();
        auto score_builder = std::make_shared<arrow::Int32Builder>();
        arrow::StructBuilder builder(type, arrow::default_memory_pool(), {id_builder, score_builder});
        ASSERT_OK(builder.Append());
        ASSERT_OK(id_builder->Append(7));
        ASSERT_OK(score_builder->Append(70));

        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder.Finish(&array));

        auto column = JsonColumn::create();
        ASSERT_OK(convert_arrow_to_json(array.get(), column.get(), 0, array->length()));
        ASSERT_EQ(1, column->size());
        EXPECT_EQ(R"({"id":7,"score":70})", compact_json(*column, 0));
    }

    {
        auto key_builder = std::make_shared<arrow::StringBuilder>();
        auto item_builder = std::make_shared<arrow::Int32Builder>();
        arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, item_builder);
        ASSERT_OK(builder.Append());
        ASSERT_OK(key_builder->Append("a"));
        ASSERT_OK(item_builder->Append(1));
        ASSERT_OK(key_builder->Append("b"));
        ASSERT_OK(item_builder->Append(2));

        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder.Finish(&array));

        auto column = JsonColumn::create();
        ASSERT_OK(convert_arrow_to_json(array.get(), column.get(), 0, array->length()));
        ASSERT_EQ(1, column->size());
        EXPECT_EQ(R"({"a":1,"b":2})", compact_json(*column, 0));
    }
}

TEST(ArrowToJsonConverterTest, LargeListToJsonColumn) {
    {
        auto value_builder = std::make_shared<arrow::Int32Builder>();
        arrow::LargeListBuilder builder(arrow::default_memory_pool(), value_builder);
        ASSERT_OK(builder.Append());
        ASSERT_OK(value_builder->Append(1));
        ASSERT_OK(value_builder->Append(2));
        ASSERT_OK(value_builder->Append(3));
        ASSERT_OK(builder.Append());
        ASSERT_OK(value_builder->Append(4));

        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder.Finish(&array));

        auto column = JsonColumn::create();
        ASSERT_OK(convert_arrow_to_json(array.get(), column.get(), 0, array->length()));
        ASSERT_EQ(2, column->size());
        EXPECT_EQ("[1,2,3]", compact_json(*column, 0));
        EXPECT_EQ("[4]", compact_json(*column, 1));
    }

    {
        auto large_list_value_builder = std::make_shared<arrow::Int32Builder>();
        auto large_list_builder =
                std::make_shared<arrow::LargeListBuilder>(arrow::default_memory_pool(), large_list_value_builder);
        std::vector fields = {arrow::field("ids", arrow::large_list(arrow::int32()))};
        auto type = arrow::struct_(fields);
        arrow::StructBuilder builder(type, arrow::default_memory_pool(), {large_list_builder});
        ASSERT_OK(builder.Append());
        ASSERT_OK(large_list_builder->Append());
        ASSERT_OK(large_list_value_builder->Append(10));
        ASSERT_OK(large_list_value_builder->Append(20));

        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder.Finish(&array));

        auto column = JsonColumn::create();
        ASSERT_OK(convert_arrow_to_json(array.get(), column.get(), 0, array->length()));
        ASSERT_EQ(1, column->size());
        EXPECT_EQ(R"({"ids":[10,20]})", compact_json(*column, 0));
    }

    {
        auto inner_value_builder = std::make_shared<arrow::Int32Builder>();
        auto inner_builder =
                std::make_shared<arrow::LargeListBuilder>(arrow::default_memory_pool(), inner_value_builder);
        arrow::ListBuilder outer_builder(arrow::default_memory_pool(), inner_builder);
        ASSERT_OK(outer_builder.Append());
        ASSERT_OK(inner_builder->Append());
        ASSERT_OK(inner_value_builder->Append(1));
        ASSERT_OK(inner_value_builder->Append(2));
        ASSERT_OK(inner_builder->Append());
        ASSERT_OK(inner_value_builder->Append(3));

        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(outer_builder.Finish(&array));

        auto column = JsonColumn::create();
        ASSERT_OK(convert_arrow_to_json(array.get(), column.get(), 0, array->length()));
        ASSERT_EQ(1, column->size());
        EXPECT_EQ("[[1,2],[3]]", compact_json(*column, 0));
    }
}

TEST(ArrowToJsonConverterTest, FixedSizeListToJsonColumn) {
    {
        auto value_builder = std::make_shared<arrow::Int32Builder>();
        arrow::FixedSizeListBuilder builder(arrow::default_memory_pool(), value_builder, 3);
        ASSERT_OK(builder.Append());
        ASSERT_OK(value_builder->Append(1));
        ASSERT_OK(value_builder->Append(2));
        ASSERT_OK(value_builder->Append(3));
        ASSERT_OK(builder.Append());
        ASSERT_OK(value_builder->Append(4));
        ASSERT_OK(value_builder->Append(5));
        ASSERT_OK(value_builder->Append(6));

        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder.Finish(&array));

        auto column = JsonColumn::create();
        ASSERT_OK(convert_arrow_to_json(array.get(), column.get(), 0, array->length()));
        ASSERT_EQ(2, column->size());
        EXPECT_EQ("[1,2,3]", compact_json(*column, 0));
        EXPECT_EQ("[4,5,6]", compact_json(*column, 1));
    }

    {
        auto fsl_value_builder = std::make_shared<arrow::Int32Builder>();
        auto fsl_builder =
                std::make_shared<arrow::FixedSizeListBuilder>(arrow::default_memory_pool(), fsl_value_builder, 2);
        std::vector fields = {arrow::field("ids", arrow::fixed_size_list(arrow::int32(), 2))};
        auto type = arrow::struct_(fields);
        arrow::StructBuilder builder(type, arrow::default_memory_pool(), {fsl_builder});
        ASSERT_OK(builder.Append());
        ASSERT_OK(fsl_builder->Append());
        ASSERT_OK(fsl_value_builder->Append(10));
        ASSERT_OK(fsl_value_builder->Append(20));

        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder.Finish(&array));

        auto column = JsonColumn::create();
        ASSERT_OK(convert_arrow_to_json(array.get(), column.get(), 0, array->length()));
        ASSERT_EQ(1, column->size());
        EXPECT_EQ(R"({"ids":[10,20]})", compact_json(*column, 0));
    }

    {
        auto inner_value_builder = std::make_shared<arrow::Int32Builder>();
        auto inner_builder =
                std::make_shared<arrow::FixedSizeListBuilder>(arrow::default_memory_pool(), inner_value_builder, 2);
        arrow::ListBuilder outer_builder(arrow::default_memory_pool(), inner_builder);
        ASSERT_OK(outer_builder.Append());
        ASSERT_OK(inner_builder->Append());
        ASSERT_OK(inner_value_builder->Append(1));
        ASSERT_OK(inner_value_builder->Append(2));
        ASSERT_OK(inner_builder->Append());
        ASSERT_OK(inner_value_builder->Append(3));
        ASSERT_OK(inner_value_builder->Append(4));

        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(outer_builder.Finish(&array));

        auto column = JsonColumn::create();
        ASSERT_OK(convert_arrow_to_json(array.get(), column.get(), 0, array->length()));
        ASSERT_EQ(1, column->size());
        EXPECT_EQ("[[1,2],[3,4]]", compact_json(*column, 0));
    }
}

TEST(ArrowToJsonConverterTest, UnsupportedArrowType) {
    arrow::BinaryBuilder builder;
    ASSERT_OK(builder.Append("bytes"));

    std::shared_ptr<arrow::Array> array;
    ASSERT_OK(builder.Finish(&array));

    auto column = JsonColumn::create();
    auto status = convert_arrow_to_json(array.get(), column.get(), 0, array->length());
    ASSERT_TRUE(status.is_not_supported()) << status.to_string();
}

} // namespace starrocks
