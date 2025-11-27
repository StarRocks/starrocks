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

#include "util/type_utils.h"

#include <gtest/gtest.h>

#include "runtime/types.h"

namespace starrocks {

struct TypeCostTestCase {
    std::string name;
    std::function<TypeDescriptor()> type_builder;
    size_t expected_cost;
};

class TypeUtilsTest : public ::testing::TestWithParam<TypeCostTestCase> {};

static TypeDescriptor make_struct(std::initializer_list<TypeDescriptor> children) {
    TypeDescriptor t;
    t.type = TYPE_STRUCT;
    for (const auto& c : children) {
        t.children.push_back(c);
        t.field_names.push_back("f" + std::to_string(t.field_names.size()));
    }
    return t;
}

static TypeDescriptor make_array(const TypeDescriptor& element) {
    TypeDescriptor t;
    t.type = TYPE_ARRAY;
    t.children.push_back(element);
    return t;
}

static TypeDescriptor make_map(const TypeDescriptor& key, const TypeDescriptor& value) {
    TypeDescriptor t;
    t.type = TYPE_MAP;
    t.children.push_back(key);
    t.children.push_back(value);
    return t;
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(
    MaterializationCost,
    TypeUtilsTest,
    ::testing::Values(
        // Scalar types
        TypeCostTestCase{"INT", [] { return TypeDescriptor(TYPE_INT); }, 4},
        TypeCostTestCase{"BIGINT", [] { return TypeDescriptor(TYPE_BIGINT); }, 8},
        TypeCostTestCase{"DOUBLE", [] { return TypeDescriptor(TYPE_DOUBLE); }, 8},
        TypeCostTestCase{"BOOLEAN", [] { return TypeDescriptor(TYPE_BOOLEAN); }, 1},
        TypeCostTestCase{"TINYINT", [] { return TypeDescriptor(TYPE_TINYINT); }, 1},
        TypeCostTestCase{"LARGEINT", [] { return TypeDescriptor(TYPE_LARGEINT); }, 16},
        
        // String types (Slice = 16 bytes)
        TypeCostTestCase{"VARCHAR", [] { return TypeDescriptor::create_varchar_type(100); }, 16},
        TypeCostTestCase{"CHAR", [] { return TypeDescriptor::create_char_type(50); }, 16},
        
        // Struct: 8 (base) + sum of children
        TypeCostTestCase{"Struct<INT,DOUBLE>", [] { 
            return make_struct({TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_DOUBLE)}); 
        }, 8 + 4 + 8},  // 20
        
        TypeCostTestCase{"Struct<INT,VARCHAR>", [] { 
            return make_struct({TypeDescriptor(TYPE_INT), TypeDescriptor::create_varchar_type(100)}); 
        }, 8 + 4 + 16},  // 28
        
        // Array: 4 (offset) + element_cost * 10
        TypeCostTestCase{"Array<INT>", [] { 
            return make_array(TypeDescriptor(TYPE_INT)); 
        }, 4 + 4 * 10},  // 44
        
        TypeCostTestCase{"Array<BIGINT>", [] { 
            return make_array(TypeDescriptor(TYPE_BIGINT)); 
        }, 4 + 8 * 10},  // 84
        
        // Map: 4 (offset) + (key + value) * 5
        TypeCostTestCase{"Map<INT,DOUBLE>", [] { 
            return make_map(TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_DOUBLE)); 
        }, 4 + (4 + 8) * 5},  // 64
        
        TypeCostTestCase{"Map<VARCHAR,INT>", [] { 
            return make_map(TypeDescriptor::create_varchar_type(100), TypeDescriptor(TYPE_INT)); 
        }, 4 + (16 + 4) * 5},  // 104
        
        // Nested: Struct containing Struct
        TypeCostTestCase{"Struct<BIGINT,Struct<INT,DOUBLE>>", [] { 
            auto inner = make_struct({TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_DOUBLE)});
            return make_struct({TypeDescriptor(TYPE_BIGINT), inner}); 
        }, 8 + 8 + (8 + 4 + 8)},  // 36
        
        // Nested: Array of Struct
        TypeCostTestCase{"Array<Struct<INT,DOUBLE>>", [] { 
            auto s = make_struct({TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_DOUBLE)});
            return make_array(s); 
        }, 4 + (8 + 4 + 8) * 10},  // 204
        
        // Nested: Struct containing Array
        TypeCostTestCase{"Struct<BIGINT,Array<INT>>", [] { 
            auto arr = make_array(TypeDescriptor(TYPE_INT));
            return make_struct({TypeDescriptor(TYPE_BIGINT), arr}); 
        }, 8 + 8 + (4 + 4 * 10)},  // 60
        
        // Deeply nested: Array<Array<INT>>
        TypeCostTestCase{"Array<Array<INT>>", [] { 
            auto inner = make_array(TypeDescriptor(TYPE_INT));
            return make_array(inner); 
        }, 4 + (4 + 4 * 10) * 10},  // 444
        
        // Map with Struct value
        TypeCostTestCase{"Map<VARCHAR,Struct<INT,VARCHAR>>", [] { 
            auto s = make_struct({TypeDescriptor(TYPE_INT), TypeDescriptor::create_varchar_type(100)});
            return make_map(TypeDescriptor::create_varchar_type(50), s); 
        }, 4 + (16 + (8 + 4 + 16)) * 5}  // 224
    ),
    [](const ::testing::TestParamInfo<TypeCostTestCase>& info) { return info.param.name; }
);
// clang-format on

TEST_P(TypeUtilsTest, ComputeMaterializationCost) {
    const auto& param = GetParam();
    TypeDescriptor type = param.type_builder();
    EXPECT_EQ(param.expected_cost, TypeUtils::compute_materialization_cost(type));
}

} // namespace starrocks
