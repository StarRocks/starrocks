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
#include <random>
#include <memory>
#include <vector>
#include <string>
#include <unordered_map>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "exprs/builtin_functions.h"
#include "exprs/function_context.h"
#include "runtime/types.h"
#include "types/logical_type.h"
#include "util/json.h"

namespace starrocks {

class BuiltinFunctionsFuzzyTest : public ::testing::Test {
public:
    void SetUp() override {
        _rng.seed(42); // Fixed seed for reproducible tests
    }

protected:
    std::mt19937 _rng;
    
    // Generate a random value for the given type
    template<typename T>
    T generate_random_value() {
        std::uniform_int_distribution<T> dist;
        return dist(_rng);
    }
    
    // Specialized for floating point types
    template<>
    double generate_random_value<double>() {
        std::uniform_real_distribution<double> dist(-1000.0, 1000.0);
        return dist(_rng);
    }
    
    template<>
    float generate_random_value<float>() {
        std::uniform_real_distribution<float> dist(-1000.0f, 1000.0f);
        return dist(_rng);
    }
    
    // Generate random string
    std::string generate_random_string(size_t max_length = 100) {
        std::uniform_int_distribution<size_t> len_dist(0, max_length);
        std::uniform_int_distribution<char> char_dist('a', 'z');
        
        size_t length = len_dist(_rng);
        std::string result;
        result.reserve(length);
        
        for (size_t i = 0; i < length; ++i) {
            result.push_back(char_dist(_rng));
        }
        return result;
    }
    
    // Create a column of the specified type with random data
    ColumnPtr create_random_column(LogicalType type, size_t size = 10) {
        switch (type) {
            case TYPE_BOOLEAN: {
                auto column = BooleanColumn::create();
                std::uniform_int_distribution<int> dist(0, 1);
                for (size_t i = 0; i < size; ++i) {
                    column->append(static_cast<uint8_t>(dist(_rng)));
                }
                return column;
            }
            case TYPE_TINYINT: {
                auto column = Int8Column::create();
                for (size_t i = 0; i < size; ++i) {
                    column->append(generate_random_value<int8_t>());
                }
                return column;
            }
            case TYPE_SMALLINT: {
                auto column = Int16Column::create();
                for (size_t i = 0; i < size; ++i) {
                    column->append(generate_random_value<int16_t>());
                }
                return column;
            }
            case TYPE_INT: {
                auto column = Int32Column::create();
                for (size_t i = 0; i < size; ++i) {
                    column->append(generate_random_value<int32_t>());
                }
                return column;
            }
            case TYPE_BIGINT: {
                auto column = Int64Column::create();
                for (size_t i = 0; i < size; ++i) {
                    column->append(generate_random_value<int64_t>());
                }
                return column;
            }
            case TYPE_LARGEINT: {
                auto column = Int128Column::create();
                for (size_t i = 0; i < size; ++i) {
                    column->append(generate_random_value<int128_t>());
                }
                return column;
            }
            case TYPE_FLOAT: {
                auto column = FloatColumn::create();
                for (size_t i = 0; i < size; ++i) {
                    column->append(generate_random_value<float>());
                }
                return column;
            }
            case TYPE_DOUBLE: {
                auto column = DoubleColumn::create();
                for (size_t i = 0; i < size; ++i) {
                    column->append(generate_random_value<double>());
                }
                return column;
            }
            case TYPE_VARCHAR: {
                auto column = BinaryColumn::create();
                for (size_t i = 0; i < size; ++i) {
                    column->append(generate_random_string());
                }
                return column;
            }
            case TYPE_VARBINARY: {
                auto column = BinaryColumn::create();
                for (size_t i = 0; i < size; ++i) {
                    std::string data = generate_random_string(50);
                    column->append(Slice(data));
                }
                return column;
            }
            case TYPE_DATE: {
                auto column = DateColumn::create();
                std::uniform_int_distribution<int32_t> dist(1, 3652425); // Valid date range
                for (size_t i = 0; i < size; ++i) {
                    column->append(DateValue::create(2000, 1, 1).add<DAY>(dist(_rng)));
                }
                return column;
            }
            case TYPE_DATETIME: {
                auto column = TimestampColumn::create();
                std::uniform_int_distribution<int64_t> dist(0, 253402300799); // Valid timestamp range
                for (size_t i = 0; i < size; ++i) {
                    TimestampValue ts;
                    ts.from_unix_second(dist(_rng));
                    column->append(ts);
                }
                return column;
            }
            case TYPE_DECIMAL32: {
                auto column = Decimal32Column::create(9, 2);
                std::uniform_int_distribution<int32_t> dist(-999999999, 999999999);
                for (size_t i = 0; i < size; ++i) {
                    column->append(dist(_rng));
                }
                return column;
            }
            case TYPE_DECIMAL64: {
                auto column = Decimal64Column::create(18, 4);
                std::uniform_int_distribution<int64_t> dist(-999999999999999999LL, 999999999999999999LL);
                for (size_t i = 0; i < size; ++i) {
                    column->append(dist(_rng));
                }
                return column;
            }
            case TYPE_DECIMAL128: {
                auto column = Decimal128Column::create(38, 8);
                for (size_t i = 0; i < size; ++i) {
                    int128_t value = generate_random_value<int128_t>();
                    column->append(value);
                }
                return column;
            }
            case TYPE_JSON: {
                auto column = JsonColumn::create();
                for (size_t i = 0; i < size; ++i) {
                    std::string json_str = R"({"key": ")" + generate_random_string(10) + R"("})";
                    auto json = JsonValue::parse(json_str);
                    if (json.ok()) {
                        column->append(json.value());
                    } else {
                        column->append_nulls(1);
                    }
                }
                return column;
            }
            default: {
                // For unsupported types, create a simple int column
                auto column = Int32Column::create();
                for (size_t i = 0; i < size; ++i) {
                    column->append(generate_random_value<int32_t>());
                }
                return column;
            }
        }
    }
    
    // Create array column with random element type
    ColumnPtr create_random_array_column(LogicalType element_type, size_t size = 10) {
        auto element_column = make_nullable(
                create_random_column(element_type, size * 3)); // More elements for arrays, wrapped as nullable
        auto offsets_column = UInt32Column::create();
        
        offsets_column->append(0);
        for (size_t i = 0; i < size; ++i) {
            std::uniform_int_distribution<uint32_t> dist(1, 5); // 1-5 elements per array
            uint32_t array_size = dist(_rng);
            uint32_t last_offset = offsets_column->get_data().back();
            offsets_column->append(last_offset + array_size);
        }

        return ArrayColumn::create(std::move(element_column), std::move(offsets_column));
    }
    
    // Wrap column with Nullable wrapper
    ColumnPtr make_nullable(ColumnPtr column) {
        auto null_column = NullColumn::create();
        std::uniform_int_distribution<int> dist(0, 4); // 20% null probability
        
        for (size_t i = 0; i < column->size(); ++i) {
            null_column->append(dist(_rng) == 0 ? 1 : 0);
        }
        
        return NullableColumn::create(column, null_column);
    }
    
    // Wrap column with Const wrapper
    ColumnPtr make_const(ColumnPtr column) {
        if (column->empty()) {
            return column;
        }
        return ConstColumn::create(column, 1);
    }
    
    // Get all possible logical types for testing
    std::vector<LogicalType> get_all_logical_types() {
        return {
            TYPE_BOOLEAN,
            TYPE_TINYINT,
            TYPE_SMALLINT, 
            TYPE_INT,
            TYPE_BIGINT,
            TYPE_LARGEINT,
            TYPE_FLOAT,
            TYPE_DOUBLE,
            TYPE_VARCHAR,
            TYPE_VARBINARY,
            TYPE_DATE,
            TYPE_DATETIME,
            TYPE_DECIMAL32,
            TYPE_DECIMAL64,
            TYPE_DECIMAL128,
            TYPE_JSON
        };
    }
    
    // Generate all possible column variations (base, nullable, const, nullable+const)
    std::vector<ColumnPtr> generate_column_variations(LogicalType type, size_t size = 10) {
        std::vector<ColumnPtr> variations;
        
        // Base column
        auto base_column = create_random_column(type, size);
        if (base_column && !base_column->empty()) {
            variations.push_back(base_column);
            
            // Nullable column
            variations.push_back(make_nullable(base_column->clone()));
            
            // Const column
            variations.push_back(make_const(base_column->clone()));
            
            // Nullable + Const column
            variations.push_back(make_const(make_nullable(base_column->clone())));
        }
        
        // Array columns
        auto array_column = create_random_array_column(type, size);
        if (array_column && !array_column->empty()) {
            variations.push_back(array_column);
            variations.push_back(make_nullable(array_column->clone()));
            variations.push_back(make_const(array_column->clone()));
            variations.push_back(make_const(make_nullable(array_column->clone())));
        }
        
        return variations;
    }
    
    // Test a single function with random inputs
    void test_function_with_random_inputs(uint64_t function_id, const FunctionDescriptor& desc) {
        auto ctx = FunctionContext::create_test_context();
        
        // Get all possible column types
        auto types = get_all_logical_types();
        
        // Generate different argument count scenarios
        std::vector<size_t> arg_counts;
        if (desc.args_nums == 0) {
            arg_counts = {0};
        } else {
            // Test with exact number of args and some variations
            arg_counts = {desc.args_nums};
            if (desc.args_nums > 1) {
                arg_counts.push_back(desc.args_nums - 1); // Too few args
            }
            arg_counts.push_back(desc.args_nums + 1); // Too many args
        }
        
        for (size_t arg_count : arg_counts) {
            // Generate multiple random combinations for this arg count
            for (int combination = 0; combination < 5; ++combination) {
                Columns columns;
                
                for (size_t i = 0; i < arg_count; ++i) {
                    // Randomly pick a type
                    std::uniform_int_distribution<size_t> type_dist(0, types.size() - 1);
                    LogicalType random_type = types[type_dist(_rng)];
                    
                    // Generate variations of this type
                    auto variations = generate_column_variations(random_type, 10);
                    if (!variations.empty()) {
                        std::uniform_int_distribution<size_t> var_dist(0, variations.size() - 1);
                        columns.emplace_back(variations[var_dist(_rng)]);
                    }
                }
                
                // Test the function - it should not crash
                try {
                    if (desc.scalar_function) {
                        auto result = desc.scalar_function(ctx, columns);
                        // Function may return error, but should not crash
                        if (result.ok()) {
                            // Verify result is valid
                            ASSERT_TRUE(result.value() != nullptr);
                        }
                    }
                } catch (const std::exception& e) {
                    // Log the exception but don't fail the test - this is expected for type mismatches
                    std::cout << "Function " << desc.name << " (ID: " << function_id 
                              << ") threw exception with " << arg_count << " args: " << e.what() << std::endl;
                } catch (...) {
                    // Unexpected exception type - this might indicate a real problem
                    FAIL() << "Function " << desc.name << " (ID: " << function_id 
                           << ") threw unexpected exception type with " << arg_count << " args";
                }
            }
        }
    }
};

TEST_F(BuiltinFunctionsFuzzyTest, TestAllBuiltinFunctions) {
    // Get all builtin functions from the function table
    const auto& fn_tables = BuiltinFunctions::get_all_functions();
    
    std::cout << "Testing " << fn_tables.size() << " builtin functions..." << std::endl;
    
    int tested_count = 0;
    int skipped_count = 0;
    std::vector<std::string> passed_cases;
    std::vector<std::string> failed_cases;

    for (const auto& [function_id, descriptor] : fn_tables) {
        // Skip functions that don't have scalar function implementation
        if (!descriptor.scalar_function) {
            skipped_count++;
            continue;
        }
        
        std::cout << "Testing function: " << descriptor.name 
                  << " (ID: " << function_id 
                  << ", args: " << static_cast<int>(descriptor.args_nums) << ")" << std::endl;

        // Run in subprocess and check exit code
        ::testing::TestPartResultArray results;
        bool passed = false;
        try {
            EXPECT_EXIT(
                    {
                        test_function_with_random_inputs(function_id, descriptor);
                        exit(0);
                    },
                    ::testing::ExitedWithCode(0), ".*");
            passed = true;
        } catch (...) {
            passed = false;
        }
        if (passed) {
            passed_cases.push_back(descriptor.name);
            std::cout << "[PASSED] " << descriptor.name << std::endl;
        } else {
            failed_cases.push_back(descriptor.name);
            std::cout << "[FAILED] " << descriptor.name << std::endl;
        }
        tested_count++;
    }

    std::cout << "\nFuzzy test completed. Tested: " << tested_count << ", Skipped: " << skipped_count << " functions."
              << std::endl;
    std::cout << "Passed cases (" << passed_cases.size() << "): ";
    for (const auto& name : passed_cases) {
        std::cout << name << ", ";
    }
    std::cout << std::endl;
    std::cout << "Failed cases (" << failed_cases.size() << "): ";
    for (const auto& name : failed_cases) {
        std::cout << name << ", ";
    }
    std::cout << std::endl;

    ASSERT_GT(tested_count, 0) << "No functions were tested!";
}

TEST_F(BuiltinFunctionsFuzzyTest, TestSpecificFunctionTypes) {
    const auto& fn_tables = BuiltinFunctions::get_all_functions();
    
    // Test functions with specific characteristics
    std::unordered_map<std::string, int> function_type_counts;
    
    for (const auto& [function_id, descriptor] : fn_tables) {
        if (!descriptor.scalar_function) continue;
        
        // Categorize functions
        std::string category;
        if (descriptor.name.find("array_") == 0) {
            category = "array";
        } else if (descriptor.name.find("map_") == 0) {
            category = "map";
        } else if (descriptor.name.find("json_") == 0 || descriptor.name.find("get_json") == 0) {
            category = "json";
        } else if (descriptor.name.find("date_") == 0 || descriptor.name.find("time_") == 0) {
            category = "time";
        } else if (descriptor.name.find("str") != std::string::npos || 
                   descriptor.name.find("char") != std::string::npos ||
                   descriptor.name.find("concat") != std::string::npos) {
            category = "string";
        } else {
            category = "other";
        }
        
        function_type_counts[category]++;
        
        // Test a few functions from each category more thoroughly
        if (function_type_counts[category] <= 3) {
            std::cout << "Intensive testing of " << category << " function: " << descriptor.name << std::endl;
            
            // Run more test combinations for these functions
            for (int round = 0; round < 10; ++round) {
                test_function_with_random_inputs(function_id, descriptor);
            }
        }
    }
    
    std::cout << "Function categories tested:" << std::endl;
    for (const auto& [category, count] : function_type_counts) {
        std::cout << "  " << category << ": " << count << " functions" << std::endl;
    }
}

TEST_F(BuiltinFunctionsFuzzyTest, TestNullAndEmptyInputs) {
    const auto& fn_tables = BuiltinFunctions::get_all_functions();
    
    for (const auto& [function_id, descriptor] : fn_tables) {
        if (!descriptor.scalar_function) continue;
        
        auto ctx = FunctionContext::create_test_context();
        
        // Test with empty columns
        {
            Columns empty_columns;
            for (uint8_t i = 0; i < descriptor.args_nums; ++i) {
                auto column = Int32Column::create();
                empty_columns.emplace_back(column);
            }
            
            try {
                auto result = descriptor.scalar_function(ctx, empty_columns);
                if (result.ok()) {
                    ASSERT_TRUE(result.value() != nullptr);
                    ASSERT_EQ(result.value()->size(), 0);
                }
            } catch (...) {
                // Expected for some functions
            }
        }
        
        // Test with all-null columns
        if (descriptor.args_nums > 0) {
            Columns null_columns;
            for (uint8_t i = 0; i < descriptor.args_nums; ++i) {
                auto base_column = Int32Column::create();
                base_column->append(1);
                auto null_column = NullColumn::create();
                null_column->append(1);
                auto nullable_column = NullableColumn::create(base_column, null_column);
                null_columns.emplace_back(nullable_column);
            }
            
            try {
                auto result = descriptor.scalar_function(ctx, null_columns);
                if (result.ok()) {
                    ASSERT_TRUE(result.value() != nullptr);
                }
            } catch (...) {
                // Expected for some functions
            }
        }
    }
}

TEST_F(BuiltinFunctionsFuzzyTest, TestMixedConstAndNonConstInputs) {
    const auto& fn_tables = BuiltinFunctions::get_all_functions();
    
    // Test functions with mixed const and non-const inputs
    for (const auto& [function_id, descriptor] : fn_tables) {
        if (!descriptor.scalar_function || descriptor.args_nums < 2) continue;
        
        auto ctx = FunctionContext::create_test_context();
        auto types = get_all_logical_types();
        
        // Generate mixed const/non-const combinations
        for (int round = 0; round < 3; ++round) {
            Columns mixed_columns;
            
            for (uint8_t i = 0; i < descriptor.args_nums; ++i) {
                std::uniform_int_distribution<size_t> type_dist(0, types.size() - 1);
                LogicalType random_type = types[type_dist(_rng)];
                
                auto base_column = create_random_column(random_type, 10);
                if (!base_column || base_column->empty()) continue;
                
                // Randomly decide if this column should be const
                std::uniform_int_distribution<int> const_dist(0, 1);
                if (const_dist(_rng) == 0) {
                    mixed_columns.push_back(make_const(base_column));
                } else {
                    mixed_columns.push_back(base_column);
                }
            }
            
            if (mixed_columns.size() == descriptor.args_nums) {
                try {
                    auto result = descriptor.scalar_function(ctx, mixed_columns);
                    if (result.ok()) {
                        ASSERT_TRUE(result.value() != nullptr);
                    }
                } catch (...) {
                    // Expected for type mismatches
                }
            }
        }
    }
}

TEST_F(BuiltinFunctionsFuzzyTest, TestLargeDataInputs) {
    const auto& fn_tables = BuiltinFunctions::get_all_functions();
    
    // Test with larger data sizes to catch potential memory issues
    for (const auto& [function_id, descriptor] : fn_tables) {
        if (!descriptor.scalar_function) continue;
        
        // Only test a subset of functions with large data to avoid long test times
        if (function_id % 10 != 0) continue;
        
        auto ctx = FunctionContext::create_test_context();
        auto types = get_all_logical_types();
        
        Columns large_columns;
        for (uint8_t i = 0; i < descriptor.args_nums; ++i) {
            std::uniform_int_distribution<size_t> type_dist(0, types.size() - 1);
            LogicalType random_type = types[type_dist(_rng)];
            
            // Create larger columns (1000 rows instead of 10)
            auto column = create_random_column(random_type, 1000);
            if (column && !column->empty()) {
                large_columns.push_back(column);
            }
        }
        
        if (large_columns.size() == descriptor.args_nums) {
            try {
                auto result = descriptor.scalar_function(ctx, large_columns);
                if (result.ok()) {
                    ASSERT_TRUE(result.value() != nullptr);
                    ASSERT_EQ(result.value()->size(), 1000);
                }
            } catch (...) {
                // Expected for type mismatches
            }
        }
    }
}

TEST_F(BuiltinFunctionsFuzzyTest, TestVariadicFunctions) {
    const auto& fn_tables = BuiltinFunctions::get_all_functions();
    
    // Test functions that accept variable number of arguments
    for (const auto& [function_id, descriptor] : fn_tables) {
        if (!descriptor.scalar_function) continue;
        
        // Look for variadic functions by checking function names that typically accept variable args
        bool is_variadic = (descriptor.name.find("concat") != std::string::npos ||
                           descriptor.name.find("coalesce") != std::string::npos ||
                           descriptor.name.find("greatest") != std::string::npos ||
                           descriptor.name.find("least") != std::string::npos);
        
        if (!is_variadic) continue;
        
        auto ctx = FunctionContext::create_test_context();
        auto types = get_all_logical_types();
        
        // Test with different numbers of arguments
        for (size_t arg_count = descriptor.args_nums; arg_count <= descriptor.args_nums + 3; ++arg_count) {
            Columns variadic_columns;
            
            for (size_t i = 0; i < arg_count; ++i) {
                std::uniform_int_distribution<size_t> type_dist(0, types.size() - 1);
                LogicalType random_type = types[type_dist(_rng)];
                
                auto column = create_random_column(random_type, 10);
                if (column && !column->empty()) {
                    variadic_columns.push_back(column);
                }
            }
            
            if (variadic_columns.size() == arg_count) {
                try {
                    auto result = descriptor.scalar_function(ctx, variadic_columns);
                    if (result.ok()) {
                        ASSERT_TRUE(result.value() != nullptr);
                    }
                } catch (...) {
                    // Expected for type mismatches
                }
            }
        }
    }
}

} // namespace starrocks