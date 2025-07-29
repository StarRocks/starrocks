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

#include <chrono>
#include <random>
#include <string>
#include <vector>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "exprs/builtin_functions.h"
#include "exprs/function_context.h"
#include "runtime/types.h"
#include "testutil/init_test_env.h"
#include "types/logical_type.h"
#include "util/json.h"

namespace starrocks {

class BuiltinFunctionsFuzzyTest {
protected:
    std::mt19937 _rng;

    // Generate a random value for the given type
    template <typename T>
    T generate_random_value() {
        if constexpr (std::is_floating_point_v<T>) {
            std::uniform_real_distribution<T> dist(-1000.0, 1000.0);
            return dist(_rng);
        } else {
            std::uniform_int_distribution<T> dist;
            return dist(_rng);
        }
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

    // Create a column based on TypeDescriptor (supports complex types)
    ColumnPtr create_random_column(const TypeDescriptor& type_desc, size_t size = 10) {
        LogicalType type = type_desc.type;
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
        case TYPE_ARRAY: {
            // For array type, use the element type from TypeDescriptor
            if (type_desc.children.size() > 0) {
                return create_random_array_column(type_desc.children[0], size);
            } else {
                // Fallback to INT if no element type specified
                return create_random_array_column(TypeDescriptor(TYPE_INT), size);
            }
        }
        case TYPE_MAP: {
            // For map type, use key and value types from TypeDescriptor
            TypeDescriptor key_type =
                    (type_desc.children.size() > 0) ? type_desc.children[0] : TypeDescriptor(TYPE_INT);
            TypeDescriptor value_type =
                    (type_desc.children.size() > 1) ? type_desc.children[1] : TypeDescriptor(TYPE_VARCHAR);

            // Wrap keys_column and values_column as nullable columns
            auto keys_column = make_nullable(create_random_column(key_type, size * 2)); // More keys for maps, nullable
            auto values_column =
                    make_nullable(create_random_column(value_type, size * 2)); // More values for maps, nullable
            auto offsets_column = UInt32Column::create();

            offsets_column->append(0);
            for (size_t i = 0; i < size; ++i) {
                std::uniform_int_distribution<uint32_t> dist(1, 3); // 1-3 key-value pairs per map
                uint32_t map_size = dist(_rng);
                uint32_t last_offset = offsets_column->get_data().back();
                offsets_column->append(last_offset + map_size);
            }

            return MapColumn::create(std::move(keys_column), std::move(values_column), std::move(offsets_column));
        }
        case TYPE_STRUCT: {
            // For struct type, use field types from TypeDescriptor
            std::vector<ColumnPtr> fields;
            for (const auto& field_type : type_desc.children) {
                fields.push_back(create_random_column(field_type, size));
            }

            // If no field types specified, create default fields
            if (fields.empty()) {
                fields.push_back(create_random_column(TypeDescriptor(TYPE_INT), size));
                fields.push_back(create_random_column(TypeDescriptor(TYPE_VARCHAR), size));
            }

            return StructColumn::create(fields);
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

    // Create array column with TypeDescriptor element type
    ColumnPtr create_random_array_column(const TypeDescriptor& element_type_desc, size_t size = 10) {
        auto element_column = make_nullable(
                create_random_column(element_type_desc, size * 3)); // More elements for arrays, wrapped as nullable
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
        return {TYPE_BOOLEAN,   TYPE_TINYINT,   TYPE_SMALLINT,   TYPE_INT,       TYPE_BIGINT, TYPE_LARGEINT,
                TYPE_FLOAT,     TYPE_DOUBLE,    TYPE_VARCHAR,    TYPE_VARBINARY, TYPE_DATE,   TYPE_DATETIME,
                TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128, TYPE_JSON};
    }

    // Generate all possible column variations (base, nullable, const, nullable+const)
    std::vector<ColumnPtr> generate_column_variations(const TypeDescriptor& type_desc, size_t size = 10) {
        std::vector<ColumnPtr> variations;

        // Base column
        auto base_column = create_random_column(type_desc, size);
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
        // auto array_column = create_random_array_column(type_desc.type, size);
        // if (array_column && !array_column->empty()) {
        //     variations.push_back(array_column);
        //     variations.push_back(make_nullable(array_column->clone()));
        //     variations.push_back(make_const(array_column->clone()));
        //     variations.push_back(make_const(make_nullable(array_column->clone())));
        // }

        return variations;
    }

    struct FnTypeDesc {
        TypeDescriptor ret_type;
        std::vector<TypeDescriptor> arg_types;

        static FnTypeDesc create(const FunctionDescriptor& desc) {
            FnTypeDesc ret;
            ret.ret_type = gen_type_desc(desc.return_type);
            for (size_t i = 0; i < desc.arg_types.size(); ++i) {
                ret.arg_types.emplace_back(gen_type_desc(desc.arg_types[i]));
            }
            return ret;
        }

        // Resolve type description in functions.py
        static TypeDescriptor gen_type_desc(const char* type_name) {
            std::string name_str(type_name);
            // ARRAY_INT
            if (name_str.find("ARRAY_") != std::string::npos) {
                TypeDescriptor element_type = gen_type_desc(name_str.data() + 6);
                return TypeDescriptor::create_array_type(element_type);
            } else if (name_str.find("ANY_ARRAY") != std::string::npos) {
                // TODO: randomize
                return TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT));
            } else if (name_str.find("ANY_STRUCT") != std::string::npos) {
                // TODO: randomize
                // Create a struct type with two INT fields named "field1" and "field2"
                return TypeDescriptor::create_struct_type(
                        std::vector<std::string>{"field1", "field2"},
                        std::vector<TypeDescriptor>{TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_INT)});
            } else if (name_str.find("ANY_MAP") != std::string::npos) {
                // TODO: randomize
                return TypeDescriptor::create_map_type(TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_INT));
            } else if (name_str.find("ANY_ELEMENT") != std::string::npos) {
                return TypeDescriptor(TYPE_INT);
            } else if (name_str.find("...") != std::string::npos) {
                return TypeDescriptor(TYPE_VARCHAR);
            } else {
                LogicalType ret = string_to_logical_type(type_name);
                if (ret == TYPE_UNKNOWN) {
                    throw std::runtime_error("Unsupported type: " + name_str);
                }
                return TypeDescriptor(ret);
            }
        }
    };
};

// Parameterized test for individual builtin functions
class BuiltinFunctionTest : public BuiltinFunctionsFuzzyTest,
                            public ::testing::TestWithParam<std::pair<uint64_t, FunctionDescriptor>> {
protected:
    void test_single_function(uint64_t function_id, const FunctionDescriptor& desc) {
        size_t arg_count = desc.args_nums;

        // Generate multiple test combinations using the specified argument types
        for (int combination = 0; combination < 5; ++combination) {
            Columns columns;
            FnTypeDesc fn_type_desc = FnTypeDesc::create(desc);

            for (size_t i = 0; i < arg_count; ++i) {
                // Generate variations of this type
                auto column = create_random_column(fn_type_desc.arg_types[i], 10);
                if (column && !column->empty()) {
                    columns.emplace_back(column);
                }
            }

            // Create FunctionContext with proper argument types and return type
            auto ctx = FunctionContext::create_test_context(std::move(fn_type_desc.arg_types), fn_type_desc.ret_type);

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
                std::cout << "Function " << desc.name << " (ID: " << function_id << ") threw exception with "
                          << arg_count << " args: " << e.what() << std::endl;
            } catch (...) {
                // Unexpected exception type - log it but don't fail the test
                std::cout << "Function " << desc.name << " (ID: " << function_id
                          << ") threw unexpected exception type with " << arg_count << " args" << std::endl;
            }
        }
    }
};

TEST_P(BuiltinFunctionTest, TestIndividualFunction) {
    std::vector<std::string> todolist = {
            "named_struct", // it's variadic function
            // "any_match",
            "row",
            // "least",
            // "greatest",
            // "coalesce",
            // "concat",
            // "concat_ws",
            // "ifnull",
            // "nullif",
            "array_flatten",
    };
    auto [function_id, descriptor] = GetParam();

    if (std::find(todolist.begin(), todolist.end(), descriptor.name) != todolist.end()) {
        GTEST_SKIP() << "Function " << descriptor.name << " is not supported yet";
    }

    // Skip functions that don't have scalar function implementation
    if (!descriptor.scalar_function) {
        GTEST_SKIP() << "Function " << descriptor.name << " has no scalar function implementation";
    }
    if (!descriptor.return_type) {
        GTEST_SKIP() << "Function " << descriptor.name << " has no return type";
    }
    for (auto& arg_type : descriptor.arg_types) {
        if (!arg_type) {
            GTEST_SKIP() << "Function " << descriptor.name << " has no argument type";
        }
    }

    try {
        test_single_function(function_id, descriptor);
    } catch (const std::exception& e) {
        FAIL() << "Function " << descriptor.name << " failed with exception: " << e.what();
    } catch (...) {
        FAIL() << "Function " << descriptor.name << " failed with unknown exception";
    }
}

// Generate test cases for all builtin functions
INSTANTIATE_TEST_SUITE_P(AllBuiltinFunctions, BuiltinFunctionTest,
                         ::testing::ValuesIn([]() -> std::vector<std::pair<uint64_t, FunctionDescriptor>> {
                             const auto& fn_tables = BuiltinFunctions::get_all_functions();
                             std::vector<std::pair<uint64_t, FunctionDescriptor>> test_cases;

                             for (const auto& [function_id, descriptor] : fn_tables) {
                                 test_cases.emplace_back(function_id, descriptor);
                             }

                             return test_cases;
                         }()),
                         [](const ::testing::TestParamInfo<std::pair<uint64_t, FunctionDescriptor>>& info) {
                             return "Function_" + std::to_string(info.param.first) + "_" + info.param.second.name;
                         });

int main(int argc, char** argv) {
    // Set the glog basename before calling init_test_env
    setenv("GTEST_LOG_BASENAME", "builtin_functions_fuzzy_test", 1);
    return starrocks::init_test_env(argc, argv);
}

} // namespace starrocks