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

#include "column/arrow/arrow_to_json_converter.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <memory>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "column/json_column.h"

namespace starrocks {

static std::string compact_json(const JsonColumn& column, size_t idx) {
    std::string value = column.get(idx).get_json()->to_string_uncheck();
    value.erase(std::remove_if(value.begin(), value.end(), [](unsigned char c) { return std::isspace(c) != 0; }),
                value.end());
    return value;
}

TEST(ArrowToJsonConverterTest, StringArrayToJsonColumn) {
    arrow::StringBuilder builder;
    ASSERT_TRUE(builder.Append(R"({"a": 1})").ok());
    ASSERT_TRUE(builder.Append("plain").ok());

    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

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
        ASSERT_TRUE(builder.Append().ok());
        ASSERT_TRUE(value_builder->Append(1).ok());
        ASSERT_TRUE(value_builder->Append(2).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());

        auto column = JsonColumn::create();
        ASSERT_OK(convert_arrow_to_json(array.get(), column.get(), 0, array->length()));
        ASSERT_EQ(1, column->size());
        EXPECT_EQ("[1,2]", compact_json(*column, 0));
    }

    {
        std::vector<std::shared_ptr<arrow::Field>> fields = {arrow::field("id", arrow::int32()),
                                                             arrow::field("score", arrow::int32())};
        auto type = arrow::struct_(fields);
        auto id_builder = std::make_shared<arrow::Int32Builder>();
        auto score_builder = std::make_shared<arrow::Int32Builder>();
        arrow::StructBuilder builder(type, arrow::default_memory_pool(), {id_builder, score_builder});
        ASSERT_TRUE(builder.Append().ok());
        ASSERT_TRUE(id_builder->Append(7).ok());
        ASSERT_TRUE(score_builder->Append(70).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());

        auto column = JsonColumn::create();
        ASSERT_OK(convert_arrow_to_json(array.get(), column.get(), 0, array->length()));
        ASSERT_EQ(1, column->size());
        EXPECT_EQ(R"({"id":7,"score":70})", compact_json(*column, 0));
    }

    {
        auto key_builder = std::make_shared<arrow::StringBuilder>();
        auto item_builder = std::make_shared<arrow::Int32Builder>();
        arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, item_builder);
        ASSERT_TRUE(builder.Append().ok());
        ASSERT_TRUE(key_builder->Append("a").ok());
        ASSERT_TRUE(item_builder->Append(1).ok());
        ASSERT_TRUE(key_builder->Append("b").ok());
        ASSERT_TRUE(item_builder->Append(2).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());

        auto column = JsonColumn::create();
        ASSERT_OK(convert_arrow_to_json(array.get(), column.get(), 0, array->length()));
        ASSERT_EQ(1, column->size());
        EXPECT_EQ(R"({"a":1,"b":2})", compact_json(*column, 0));
    }
}

TEST(ArrowToJsonConverterTest, UnsupportedArrowType) {
    arrow::BinaryBuilder builder;
    ASSERT_TRUE(builder.Append("bytes").ok());

    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    auto column = JsonColumn::create();
    auto status = convert_arrow_to_json(array.get(), column.get(), 0, array->length());
    ASSERT_TRUE(status.is_not_supported()) << status.to_string();
}

} // namespace starrocks
