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

#include "exprs/variant_functions.h"

#include <glog/logging.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "column/column.h"
#include "column/const_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/mock_vectorized_expr.h"
#include "formats/parquet/variant.h"
#include "fs/fs.h"
#include "gutil/casts.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

class VariantFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    }

    TExprNode expr_node;
};

std::string starrocks_home = getenv("STARROCKS_HOME");
static std::string test_exec_dir = starrocks_home + "/be/test/exec";
static std::string variant_test_data_dir = starrocks_home + "/be/test/formats/parquet/test_data/variant";

// Helper function to read variant test data from parquet test files
static std::pair<std::string, std::string> load_variant_test_data(const std::string& metadata_file,
                                                           const std::string& value_file) {
    FileSystem* fs = FileSystem::Default();

    auto metadata_path = variant_test_data_dir + "/" + metadata_file;
    auto value_path = variant_test_data_dir + "/" + value_file;

    auto metadata_file_obj = *fs->new_random_access_file(metadata_path);
    auto value_file_obj = *fs->new_random_access_file(value_path);

    std::string metadata_content = *metadata_file_obj->read_all();
    std::string value_content = *value_file_obj->read_all();

    return {std::move(metadata_content), std::move(value_content)};
}

// Helper function to create VariantValue from test data files
static void create_variant_from_test_data(const std::string& metadata_file, const std::string& value_file,
                                         VariantValue& variant_value) {
    auto [metadata, value] = load_variant_test_data(metadata_file, value_file);
    variant_value = VariantValue(metadata, value);
}

// Test cases using real variant test data
class VariantQueryTestFixture
        : public ::testing::TestWithParam<std::tuple<std::string, std::string, std::string, std::string>> {};

TEST_P(VariantQueryTestFixture, variant_query_with_test_data) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();
    ColumnBuilder<TYPE_VARCHAR> path_builder(1);

    std::string metadata_file = std::get<0>(GetParam());
    std::string value_file = std::get<1>(GetParam());
    std::string param_path = std::get<2>(GetParam());
    std::string param_result = std::get<3>(GetParam());

    VariantValue variant_value;

    create_variant_from_test_data(metadata_file, value_file, variant_value);
    VLOG(10) << "Loaded variant value from test data: " << variant_value.to_string();
    variant_column->append(variant_value);

    if (param_path == "NULL") {
        path_builder.append_null();
    } else {
        path_builder.append(param_path);
    }

    Columns columns{variant_column, path_builder.build(true)};
    ctx->set_constant_columns(columns);
    std::ignore = VariantFunctions::variant_segments_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);

    ColumnPtr result = VariantFunctions::variant_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone("-04:00", ctz);

    Datum datum = result->get(0);
    if (param_result == "NULL") {
        ASSERT_TRUE(datum.is_null());
    } else {
        ASSERT_TRUE(!datum.is_null());
        auto variant_result = datum.get_variant();
        ASSERT_TRUE(!!variant_result);
        auto json_result = variant_result->to_json(ctz);
        ASSERT_TRUE(json_result.ok());
        std::string variant_str = json_result.value();
        ASSERT_EQ(param_result, variant_str);
    }

    ASSERT_TRUE(VariantFunctions::variant_segments_close(
            ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
}

// Test cases using real variant test data from parquet test files
INSTANTIATE_TEST_SUITE_P(
        VariantQueryTestWithRealData, VariantQueryTestFixture,
        ::testing::Values(
                // clang-format off
                // Basic primitive tests using real test data
                std::make_tuple("primitive_boolean_true.metadata", "primitive_boolean_true.value", "$", "true"),
                std::make_tuple("primitive_boolean_false.metadata", "primitive_boolean_false.value", "$", "false"),
                std::make_tuple("primitive_int8.metadata", "primitive_int8.value", "$", "42"),
                std::make_tuple("primitive_int16.metadata", "primitive_int16.value", "$", "1234"),
                std::make_tuple("primitive_int32.metadata", "primitive_int32.value", "$", "123456"),
                std::make_tuple("primitive_int64.metadata", "primitive_int64.value", "$", "1234567890123456789"),
                std::make_tuple("primitive_float.metadata", "primitive_float.value", "$", "1234567936.000000"),
                std::make_tuple("primitive_double.metadata", "primitive_double.value", "$", "1234567890.123400"),
                std::make_tuple("primitive_decimal4.metadata", "primitive_decimal4.value", "$", "12.34"),
                std::make_tuple("primitive_decimal8.metadata", "primitive_decimal8.value", "$", "12345678.90"),
                std::make_tuple("primitive_decimal16.metadata", "primitive_decimal16.value", "$", "12345678912345678.90"),
                std::make_tuple("short_string.metadata", "short_string.value", "$", "Less than 64 bytes (❤️ with utf8)"),
                std::make_tuple("primitive_string.metadata", "primitive_string.value", "$", "This string is longer than 64 bytes and therefore does not fit in a short_string and it also includes several non ascii characters such as 🐢, 💖, ♥️, 🎣 and 🤦!!"),
                std::make_tuple("primitive_timestamp.metadata", "primitive_timestamp.value", "$", "\"2025-04-16 12:34:56.78-04:00\""),
                std::make_tuple("primitive_timestampntz.metadata", "primitive_timestampntz.value", "$", "\"2025-04-16 12:34:56.780000\""),
                std::make_tuple("primitive_date.metadata", "primitive_date.value", "$", "\"2025-04-16\""),

                // Object and array tests
                std::make_tuple("object_primitive.metadata", "object_primitive.value", "$.int_field", "1"),
                std::make_tuple("object_nested.metadata", "object_nested.value", "$.observation.location", "In the Volcano"),
                std::make_tuple("array_primitive.metadata", "array_primitive.value", "$[0]", "2"),
                std::make_tuple("array_nested.metadata", "array_nested.value", "$[0].thing.names[0]", "Contrarian"),

                // Non-existent path tests
                std::make_tuple("primitive_int8.metadata", "primitive_int8.value", "$.nonexistent", "NULL"),
                std::make_tuple("primitive_string.metadata", "primitive_string.value", "$.missing", "NULL"),

                // Null path tests
                std::make_tuple("primitive_int8.metadata", "primitive_int8.value", "NULL", "NULL")
                // clang-format on
                ));

// Simplified test cases for basic functionality using simple variant values
class VariantQuerySimpleTestFixture
        : public ::testing::TestWithParam<std::tuple<std::string, std::string, std::string>> {};

TEST_F(VariantFunctionsTest, variant_query_invalid_arguments) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    // Test with no arguments
    {
        Columns columns;
        auto result = VariantFunctions::variant_query(ctx.get(), columns);
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().is_invalid_argument());
    }

    // Test with one argument
    {
        auto variant_column = VariantColumn::create();
        Columns columns{variant_column};
        auto result = VariantFunctions::variant_query(ctx.get(), columns);
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().is_invalid_argument());
    }

    // Test with three arguments
    {
        auto variant_column = VariantColumn::create();
        auto path_column = BinaryColumn::create();
        auto extra_column = BinaryColumn::create();
        Columns columns{variant_column, path_column, extra_column};
        auto result = VariantFunctions::variant_query(ctx.get(), columns);
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().is_invalid_argument());
    }
}

TEST_F(VariantFunctionsTest, variant_query_null_columns) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    // Test with all null columns
    auto variant_column = NullableColumn::create(VariantColumn::create(), NullColumn::create());
    auto path_column = NullableColumn::create(BinaryColumn::create(), NullColumn::create());

    variant_column->append_nulls(2);
    path_column->append_nulls(2);

    Columns columns{variant_column, path_column};

    ColumnPtr result = VariantFunctions::variant_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    ASSERT_EQ(2, result->size());
    ASSERT_TRUE(result->is_null(0));
    ASSERT_TRUE(result->is_null(1));
}

TEST_F(VariantFunctionsTest, variant_query_invalid_path) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();
    auto path_column = BinaryColumn::create();

    VariantValue variant_value;
    create_variant_from_test_data("primitive_int8.metadata", "primitive_int8.value", variant_value);
    variant_column->append(variant_value);

    // Invalid path syntax
    path_column->append("$.invalid..path");

    Columns columns{variant_column, path_column};

    ColumnPtr result = VariantFunctions::variant_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    ASSERT_EQ(1, result->size());
    ASSERT_TRUE(result->is_null(0));
}

TEST_F(VariantFunctionsTest, variant_query_complex_types) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();
    ColumnBuilder<TYPE_VARCHAR> path_builder(1);

    VariantValue variant_value;
    create_variant_from_test_data("object_primitive.metadata", "object_primitive.value", variant_value);
    variant_column->append(variant_value);
    path_builder.append("$.int_field");

    Columns columns{variant_column, path_builder.build(true)};
    ctx->set_constant_columns(columns);

    ColumnPtr result = VariantFunctions::variant_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    ASSERT_EQ(1, result->size());

    // Handle potential ConstColumn wrapping
    Datum datum = result->get(0);
    ASSERT_FALSE(datum.is_null());

    // For TYPE_VARIANT result, the datum should contain a VariantValue pointer
    auto variant_result = datum.get_variant();
    ASSERT_TRUE(!!variant_result);
    auto json_result = variant_result->to_json();
    ASSERT_TRUE(json_result.ok());
    std::string variant_str = json_result.value();
    ASSERT_EQ("1", variant_str);
}

TEST_F(VariantFunctionsTest, variant_query_multiple_rows) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();
    ColumnBuilder<TYPE_VARCHAR> path_builder(1);

    // Create multiple variant values using test data
    std::vector<std::pair<std::string, std::string>> test_files = {
            {"primitive_int8.metadata", "primitive_int8.value"},
            {"primitive_boolean_true.metadata", "primitive_boolean_true.value"},
            {"short_string.metadata", "short_string.value"}};

    std::vector<VariantValue> variant_values;
    variant_values.reserve(test_files.size());

    for (size_t i = 0; i < test_files.size(); ++i) {
        const auto& [metadata_file, value_file] = test_files[i];
        VariantValue variant_value;
        create_variant_from_test_data(metadata_file, value_file, variant_value);
        variant_values.push_back(variant_value);
        variant_column->append(variant_value);
        path_builder.append("$");
    }

    Columns columns{variant_column, path_builder.build(true)};
    ctx->set_constant_columns(columns);

    ColumnPtr result = VariantFunctions::variant_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    ASSERT_EQ(3, result->size());

    std::vector<std::string> expected_results = {"42", "true", "Less than 64 bytes (❤️ with utf8)"};
    for (size_t i = 0; i < 3; ++i) {
        auto variant_result = result->get(i).get_variant();
        ASSERT_TRUE(!!variant_result);
        auto json_result = variant_result->to_json();
        ASSERT_TRUE(json_result.ok());
        std::string variant_str = json_result.value();
        ASSERT_EQ(expected_results[i], variant_str);
    }
}

TEST_F(VariantFunctionsTest, variant_query_const_columns) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();
    ColumnBuilder<TYPE_VARCHAR> path_builder(1);

    // Create variant value using test data
    VariantValue variant_value;
    create_variant_from_test_data("short_string.metadata", "short_string.value", variant_value);
    variant_column->append(variant_value);
    path_builder.append("$");

    // Create const columns
    auto const_variant = ConstColumn::create(variant_column, 3);
    auto const_path = path_builder.build(true);

    Columns columns{const_variant, const_path};
    ctx->set_constant_columns(columns);

    ColumnPtr result = VariantFunctions::variant_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    ASSERT_EQ(3, result->size());

    for (size_t i = 0; i < 3; ++i) {
        auto variant_result = result->get(i).get_variant();
        ASSERT_TRUE(!!variant_result);
        auto json_result = variant_result->to_json();
        ASSERT_TRUE(json_result.ok());
        std::string variant_str = json_result.value();
        ASSERT_EQ("Less than 64 bytes (❤️ with utf8)", variant_str);
    }
}

} // namespace starrocks
