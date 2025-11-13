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

#include "exec/parquet_schema_builder.h"

#include <gtest/gtest.h>

#include "parquet/schema.h"
#include "parquet/types.h"
#include "runtime/types.h"

namespace starrocks {

class ParquetSchemaBuilderTest : public testing::Test {
public:
    ParquetSchemaBuilderTest() = default;
    ~ParquetSchemaBuilderTest() override = default;

protected:
    // Helper function to create a primitive node
    static parquet::schema::NodePtr create_primitive_node(const std::string& name,
                                                          const parquet::Repetition::type repetition,
                                                          const parquet::Type::type type) {
        return parquet::schema::PrimitiveNode::Make(name, repetition, type);
    }

    // Helper function to create a group node
    static parquet::schema::NodePtr create_group_node(const std::string& name,
                                                      const parquet::Repetition::type repetition,
                                                      const parquet::schema::NodeVector& fields) {
        return parquet::schema::GroupNode::Make(name, repetition, fields);
    }

    // Helper function to create a group node with logical type
    static parquet::schema::NodePtr create_list_node(const std::string& name,
                                                     const parquet::Repetition::type repetition,
                                                     const parquet::schema::NodeVector& fields) {
        return ::parquet::schema::GroupNode::Make(name, repetition, fields, parquet::LogicalType::List());
    }

    static parquet::schema::NodePtr create_map_node(const std::string& name, const parquet::Repetition::type repetition,
                                                    const parquet::schema::NodeVector& fields) {
        return parquet::schema::GroupNode::Make(name, repetition, fields, parquet::LogicalType::Map());
    }
};

// Test basic primitive types
TEST_F(ParquetSchemaBuilderTest, PrimitiveTypes) {
    TypeDescriptor type_desc;
    Status st;

    // Test BOOLEAN
    {
        auto node = create_primitive_node("bool_col", ::parquet::Repetition::REQUIRED, ::parquet::Type::BOOLEAN);
        st = get_parquet_type(node, &type_desc);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(TYPE_BOOLEAN, type_desc.type);
    }

    // Test INT32
    {
        auto node = create_primitive_node("int_col", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32);
        st = get_parquet_type(node, &type_desc);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(TYPE_INT, type_desc.type);
    }

    // Test INT64
    {
        auto node = create_primitive_node("bigint_col", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT64);
        st = get_parquet_type(node, &type_desc);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(TYPE_BIGINT, type_desc.type);
    }

    // Test FLOAT
    {
        auto node = create_primitive_node("float_col", ::parquet::Repetition::REQUIRED, ::parquet::Type::FLOAT);
        st = get_parquet_type(node, &type_desc);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(TYPE_FLOAT, type_desc.type);
    }

    // Test DOUBLE
    {
        auto node = create_primitive_node("double_col", ::parquet::Repetition::REQUIRED, ::parquet::Type::DOUBLE);
        st = get_parquet_type(node, &type_desc);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(TYPE_DOUBLE, type_desc.type);
    }

    // Test BYTE_ARRAY (default to VARBINARY)
    {
        auto node = create_primitive_node("binary_col", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY);
        st = get_parquet_type(node, &type_desc);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(TYPE_VARBINARY, type_desc.type);
    }
}

// Test variant type with valid unshredded structure (2 fields: metadata and value)
TEST_F(ParquetSchemaBuilderTest, VariantTypeUnshredded) {
    TypeDescriptor type_desc;
    Status st;

    // Create a valid variant structure:
    // group variant_col {
    //   required binary metadata;
    //   required binary value;
    // }
    ::parquet::schema::NodeVector fields;
    fields.push_back(create_primitive_node("metadata", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));
    fields.push_back(create_primitive_node("value", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));

    auto node = create_group_node("variant_col", ::parquet::Repetition::OPTIONAL, fields);

    st = get_parquet_type(node, &type_desc);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(TYPE_VARIANT, type_desc.type);
}

// Test variant type with required repetition
TEST_F(ParquetSchemaBuilderTest, VariantTypeRequired) {
    TypeDescriptor type_desc;
    Status st;

    ::parquet::schema::NodeVector fields;
    fields.push_back(create_primitive_node("metadata", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));
    fields.push_back(create_primitive_node("value", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));

    auto node = create_group_node("variant_col", ::parquet::Repetition::REQUIRED, fields);

    st = get_parquet_type(node, &type_desc);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(TYPE_VARIANT, type_desc.type);
}

// Test invalid variant - missing metadata field
TEST_F(ParquetSchemaBuilderTest, VariantInvalidMissingMetadata) {
    TypeDescriptor type_desc;
    Status st;

    // Only has value field, missing metadata
    parquet::schema::NodeVector fields;
    fields.push_back(create_primitive_node("value", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));

    auto node = create_group_node("not_variant_col", ::parquet::Repetition::OPTIONAL, fields);

    st = get_parquet_type(node, &type_desc);
    ASSERT_TRUE(st.ok());
    // Should be inferred as STRUCT, not VARIANT
    ASSERT_EQ(TYPE_STRUCT, type_desc.type);
}

// Test invalid variant - missing value field
TEST_F(ParquetSchemaBuilderTest, VariantInvalidMissingValue) {
    TypeDescriptor type_desc;
    Status st;

    // Only has metadata field, missing value
    parquet::schema::NodeVector fields;
    fields.push_back(create_primitive_node("metadata", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));

    auto node = create_group_node("not_variant_col", ::parquet::Repetition::OPTIONAL, fields);

    st = get_parquet_type(node, &type_desc);
    ASSERT_TRUE(st.ok());
    // Should be inferred as STRUCT, not VARIANT
    ASSERT_EQ(TYPE_STRUCT, type_desc.type);
}

// Test invalid variant - wrong field type (metadata is INT32 instead of BYTE_ARRAY)
TEST_F(ParquetSchemaBuilderTest, VariantInvalidWrongFieldType) {
    TypeDescriptor type_desc;
    Status st;

    // metadata is INT32 instead of BYTE_ARRAY
    parquet::schema::NodeVector fields;
    fields.push_back(create_primitive_node("metadata", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32));
    fields.push_back(create_primitive_node("value", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));

    auto node = create_group_node("not_variant_col", ::parquet::Repetition::OPTIONAL, fields);

    st = get_parquet_type(node, &type_desc);
    ASSERT_TRUE(st.ok());
    // Should be inferred as STRUCT, not VARIANT
    ASSERT_EQ(TYPE_STRUCT, type_desc.type);
}

// Test invalid variant - value field is wrong type
TEST_F(ParquetSchemaBuilderTest, VariantInvalidWrongValueType) {
    TypeDescriptor type_desc;
    Status st;

    // value is INT32 instead of BYTE_ARRAY
    parquet::schema::NodeVector fields;
    fields.push_back(create_primitive_node("metadata", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));
    fields.push_back(create_primitive_node("value", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32));

    auto node = create_group_node("not_variant_col", ::parquet::Repetition::OPTIONAL, fields);

    st = get_parquet_type(node, &type_desc);
    ASSERT_TRUE(st.ok());
    // Should be inferred as STRUCT, not VARIANT
    ASSERT_EQ(TYPE_STRUCT, type_desc.type);
}

// Test shredded variant type (3 fields) - not yet supported
TEST_F(ParquetSchemaBuilderTest, VariantShreddedNotSupported) {
    TypeDescriptor type_desc;
    Status st;

    // Shredded variant with 3 fields: metadata, value, and typed_value
    parquet::schema::NodeVector fields;
    fields.push_back(create_primitive_node("metadata", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));
    fields.push_back(create_primitive_node("value", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));
    fields.push_back(
            create_primitive_node("typed_value", ::parquet::Repetition::OPTIONAL, ::parquet::Type::BYTE_ARRAY));

    auto node = create_group_node("variant_col", ::parquet::Repetition::OPTIONAL, fields);

    st = get_parquet_type(node, &type_desc);
    // Should return NotSupported status for shredded variant
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_not_supported());
}

// Test invalid variant - extra field
TEST_F(ParquetSchemaBuilderTest, VariantInvalidExtraField) {
    TypeDescriptor type_desc;
    Status st;

    // Has 3 fields but the 3rd field is not "typed_value"
    parquet::schema::NodeVector fields;
    fields.push_back(create_primitive_node("metadata", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));
    fields.push_back(create_primitive_node("value", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));
    fields.push_back(create_primitive_node("extra", ::parquet::Repetition::OPTIONAL, ::parquet::Type::BYTE_ARRAY));

    auto node = create_group_node("not_variant_col", ::parquet::Repetition::OPTIONAL, fields);

    st = get_parquet_type(node, &type_desc);
    ASSERT_TRUE(st.ok());
    // Should be inferred as STRUCT, not VARIANT
    ASSERT_EQ(TYPE_STRUCT, type_desc.type);
}

// Test variant nested in a struct
TEST_F(ParquetSchemaBuilderTest, VariantNestedInStruct) {
    TypeDescriptor type_desc;
    Status st;

    // Create variant field
    parquet::schema::NodeVector variant_fields;
    variant_fields.push_back(
            create_primitive_node("metadata", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));
    variant_fields.push_back(
            create_primitive_node("value", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));
    auto variant_node = create_group_node("variant_field", ::parquet::Repetition::OPTIONAL, variant_fields);

    // Create struct containing the variant
    parquet::schema::NodeVector struct_fields;
    struct_fields.push_back(variant_node);
    auto struct_node = create_group_node("struct_col", ::parquet::Repetition::OPTIONAL, struct_fields);

    st = get_parquet_type(struct_node, &type_desc);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(TYPE_STRUCT, type_desc.type);
    ASSERT_EQ(1, type_desc.children.size());
    // The nested variant field should be TYPE_VARIANT
    ASSERT_EQ(TYPE_VARIANT, type_desc.children[0].type);
}

// Test struct type inference (non-variant struct)
TEST_F(ParquetSchemaBuilderTest, StructTypeInference) {
    TypeDescriptor type_desc;
    Status st;

    // Create a regular struct with different field names
    parquet::schema::NodeVector fields;
    fields.push_back(create_primitive_node("field1", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32));
    fields.push_back(create_primitive_node("field2", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));

    auto node = create_group_node("struct_col", ::parquet::Repetition::OPTIONAL, fields);

    st = get_parquet_type(node, &type_desc);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(TYPE_STRUCT, type_desc.type);
    ASSERT_EQ(2, type_desc.children.size());
    ASSERT_EQ(TYPE_INT, type_desc.children[0].type);
    ASSERT_EQ(TYPE_VARBINARY, type_desc.children[1].type);
}

// Test list type
TEST_F(ParquetSchemaBuilderTest, ListType) {
    TypeDescriptor type_desc;

    // Create element node
    auto element = create_primitive_node("element", ::parquet::Repetition::OPTIONAL, ::parquet::Type::INT32);

    // Create list wrapper
    parquet::schema::NodeVector list_fields;
    list_fields.push_back(element);
    auto list_node = create_group_node("list", ::parquet::Repetition::REPEATED, list_fields);

    // Create outer list node with LIST logical type
    parquet::schema::NodeVector outer_fields;
    outer_fields.push_back(list_node);
    auto outer_node = create_list_node("list_col", ::parquet::Repetition::OPTIONAL, outer_fields);

    Status st = get_parquet_type(outer_node, &type_desc);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(TYPE_ARRAY, type_desc.type);
    ASSERT_EQ(TYPE_INT, type_desc.children[0].type);
}

// Test map type
TEST_F(ParquetSchemaBuilderTest, MapType) {
    TypeDescriptor type_desc;
    Status st;

    // Create key and value nodes
    auto key = create_primitive_node("key", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32);
    auto value = create_primitive_node("value", ::parquet::Repetition::OPTIONAL, ::parquet::Type::BYTE_ARRAY);

    // Create key_value group
    parquet::schema::NodeVector kv_fields;
    kv_fields.push_back(key);
    kv_fields.push_back(value);
    auto kv_node = create_group_node("key_value", ::parquet::Repetition::REPEATED, kv_fields);

    // Create outer map node with MAP logical type
    parquet::schema::NodeVector outer_fields;
    outer_fields.push_back(kv_node);
    auto outer_node = create_map_node("map_col", ::parquet::Repetition::OPTIONAL, outer_fields);

    st = get_parquet_type(outer_node, &type_desc);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(TYPE_MAP, type_desc.type);
    ASSERT_EQ(TYPE_INT, type_desc.children[0].type);       // key type
    ASSERT_EQ(TYPE_VARBINARY, type_desc.children[1].type); // value type
}

// Test complex nested structure with variant
TEST_F(ParquetSchemaBuilderTest, ComplexNestedWithVariant) {
    TypeDescriptor type_desc;

    // Create variant field
    parquet::schema::NodeVector variant_fields;
    variant_fields.push_back(
            create_primitive_node("metadata", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));
    variant_fields.push_back(
            create_primitive_node("value", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));
    auto variant_node = create_group_node("variant_field", ::parquet::Repetition::OPTIONAL, variant_fields);

    // Create struct containing variant and other fields
    parquet::schema::NodeVector struct_fields;
    // child 1: id (INT32)
    struct_fields.push_back(create_primitive_node("id", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32));
    // child 2: variant field
    struct_fields.push_back(variant_node);
    // child 3: name (BYTE_ARRAY)
    struct_fields.push_back(
            create_primitive_node("name", ::parquet::Repetition::OPTIONAL, ::parquet::Type::BYTE_ARRAY));
    const auto struct_node = create_group_node("complex_struct", ::parquet::Repetition::OPTIONAL, struct_fields);

    Status st = get_parquet_type(struct_node, &type_desc);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(TYPE_STRUCT, type_desc.type);
    ASSERT_EQ(3, type_desc.children.size());
    ASSERT_EQ(TYPE_INT, type_desc.children[0].type);
    ASSERT_EQ(TYPE_VARIANT, type_desc.children[1].type);
    ASSERT_EQ(TYPE_VARBINARY, type_desc.children[2].type);
}

// Test variant field order doesn't matter (value before metadata)
TEST_F(ParquetSchemaBuilderTest, VariantFieldOrderReversed) {
    TypeDescriptor type_desc;

    // Create variant with value field before metadata field
    parquet::schema::NodeVector fields;
    fields.push_back(create_primitive_node("value", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));
    fields.push_back(create_primitive_node("metadata", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY));

    const auto node = create_group_node("variant_col", ::parquet::Repetition::OPTIONAL, fields);

    Status st = get_parquet_type(node, &type_desc);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(TYPE_VARIANT, type_desc.type);
}

// Test empty group (should fail)
TEST_F(ParquetSchemaBuilderTest, EmptyGroup) {
    TypeDescriptor type_desc;

    parquet::schema::NodeVector fields;
    const auto node = create_group_node("empty_group", ::parquet::Repetition::OPTIONAL, fields);

    Status st = get_parquet_type(node, &type_desc);
    // Empty groups should return error or be treated as VARCHAR
    ASSERT_TRUE(!st.ok() || type_desc.type == TYPE_VARCHAR);
}

} // namespace starrocks
