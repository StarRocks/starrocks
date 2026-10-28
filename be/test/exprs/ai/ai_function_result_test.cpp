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

#include "exprs/ai/ai_function_result.h"

#include <gtest/gtest.h>

#include <limits>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "types/json_value.h"

namespace starrocks {

TEST(AIFunctionResultTest, BooleanAcceptsOnlyWholeTrimmedTokens) {
    auto output = ColumnHelper::create_column(TypeDescriptor(TYPE_BOOLEAN), true);
    for (const auto& text : {"true", " TRUE \n", "false", "\tFalse"}) {
        ASSERT_TRUE(append_ai_function_result(AIFunctionResultKind::BOOLEAN, std::string(text), output.get()).ok());
    }
    EXPECT_EQ(1, output->get(0).get_uint8());
    EXPECT_EQ(0, output->get(2).get_uint8());
    for (const auto& text : {"", "true because", "t r u e", "1", "\"true\"", "'true'", "\"false\"", "'false'"}) {
        auto status = append_ai_function_result(AIFunctionResultKind::BOOLEAN, std::string(text), output.get());
        EXPECT_TRUE(status.is_invalid_argument()) << status;
        EXPECT_EQ(4, output->size());
        EXPECT_EQ(std::string::npos, status.to_string().find(text == std::string("") ? "secret" : text));
    }
}

TEST(AIFunctionResultTest, SimilarityRequiresFiniteWholeValueWithinUnitInterval) {
    auto output = ColumnHelper::create_column(TypeDescriptor(TYPE_FLOAT), true);
    for (const auto& text : {"0", " 0.25\n", "1.0", "1e-1"}) {
        ASSERT_TRUE(append_ai_function_result(AIFunctionResultKind::SIMILARITY, std::string(text), output.get()).ok());
    }
    EXPECT_FLOAT_EQ(0.25, output->get(1).get_float());
    for (const auto& text : {"", "0 .5", "0.5 extra", "NaN", "inf", "-0.01", "1.000000001", "1e999"}) {
        EXPECT_TRUE(append_ai_function_result(AIFunctionResultKind::SIMILARITY, std::string(text), output.get())
                            .is_invalid_argument());
        EXPECT_EQ(4, output->size());
    }
}

TEST(AIFunctionResultTest, JsonPreservesWhitespaceInsideStringsAndRejectsMalformedResponses) {
    auto output = ColumnHelper::create_column(TypeDescriptor(TYPE_JSON), true);
    ASSERT_TRUE(append_ai_function_result(AIFunctionResultKind::JSON,
                                          std::string(" {\"response\": {\"name\": \"Mary Jane\"}} \n"), output.get())
                        .ok());
    EXPECT_NE(std::string::npos, output->get(0).get_json()->to_string_uncheck().find("Mary Jane"));
    for (const auto& text : {"not JSON secret", "{\"a\":1} trailing", "```json\n{}\n```", "[]", "null", "\"value\""}) {
        auto status = append_ai_function_result(AIFunctionResultKind::JSON, std::string(text), output.get());
        EXPECT_TRUE(status.is_invalid_argument());
        EXPECT_EQ(std::string::npos, status.to_string().find("secret"));
        EXPECT_EQ(1, output->size());
    }
}

TEST(AIFunctionResultTest, PlainStringIsUnmodifiedAndMismatchedColumnsAreNotRowFailures) {
    auto output = ColumnHelper::create_column(TypeDescriptor::create_varchar_type(1024), true);
    ASSERT_TRUE(
            append_ai_function_result(AIFunctionResultKind::STRING, std::string("  exact text\n"), output.get()).ok());
    EXPECT_EQ("  exact text\n", output->get(0).get_slice().to_string());
    ASSERT_TRUE(append_ai_function_result(AIFunctionResultKind::STRING, std::string(), output.get()).ok());
    EXPECT_TRUE(output->get(1).get_slice().empty());
    EXPECT_TRUE(append_ai_function_result(AIFunctionResultKind::STRING, std::vector<float>{1.f}, output.get())
                        .is_invalid_argument());
    auto status = append_ai_function_result(AIFunctionResultKind::BOOLEAN, std::string("true"), output.get());
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(status.is_invalid_argument());
    EXPECT_EQ(2, output->size());
}

TEST(AIFunctionResultTest, JsonRejectsExcessiveNestingBeforeRecursiveMaterialization) {
    auto output = ColumnHelper::create_column(TypeDescriptor(TYPE_JSON), true);
    const std::string nested = "{\"value\":" + std::string(128, '[') + "0" + std::string(128, ']') + "}";
    EXPECT_TRUE(append_ai_function_result(AIFunctionResultKind::JSON, nested, output.get()).is_invalid_argument());
    EXPECT_EQ(0, output->size());
}

TEST(AIFunctionResultTest, SentimentIsAValidatedNormalizedLabel) {
    auto output = ColumnHelper::create_column(TypeDescriptor::create_varchar_type(1024), true);
    for (const auto& text : {"positive", " NEGATIVE\n", "neutral", "mixed", "unknown"}) {
        ASSERT_TRUE(append_ai_function_result(AIFunctionResultKind::SENTIMENT, std::string(text), output.get()).ok());
    }
    EXPECT_EQ("negative", output->get(1).get_slice().to_string());
    EXPECT_TRUE(append_ai_function_result(AIFunctionResultKind::SENTIMENT, std::string("positive."), output.get())
                        .is_invalid_argument());
    EXPECT_EQ(5, output->size());
}

TEST(AIFunctionResultTest, EmbeddingAppendsNullableFloatElementsAndDoesNotPartiallyAppendInvalidValues) {
    auto output = ColumnHelper::create_column(TypeDescriptor::create_array_type(TypeDescriptor(TYPE_FLOAT)), true);
    ASSERT_TRUE(
            append_ai_function_result(AIFunctionResultKind::EMBEDDING, std::vector<float>{0.25f, -0.5f}, output.get())
                    .ok());
    ASSERT_EQ(1, output->size());
    ASSERT_EQ(2, output->get(0).get_array().size());
    EXPECT_FLOAT_EQ(-0.5f, output->get(0).get_array()[1].get_float());
    const auto* array =
            down_cast<const ArrayColumn*>(down_cast<const NullableColumn*>(output.get())->data_column().get());
    EXPECT_TRUE(array->elements().is_nullable());
    EXPECT_EQ(2, array->offsets().get_data().back());
    for (AIProviderValue invalid : {AIProviderValue(std::vector<float>{}), AIProviderValue(std::string("[1,2]")),
                                    AIProviderValue(std::vector<float>{1.f, std::numeric_limits<float>::infinity()})}) {
        EXPECT_TRUE(append_ai_function_result(AIFunctionResultKind::EMBEDDING, invalid, output.get())
                            .is_invalid_argument());
        EXPECT_EQ(1, output->size());
        EXPECT_EQ(2, array->elements().size());
    }
}

} // namespace starrocks
