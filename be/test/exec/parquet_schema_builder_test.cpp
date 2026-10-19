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

#include "formats/parquet/schema.h"
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
    static ::parquet::schema::NodePtr create_primitive_node(const std::string& name,
                                                            const ::parquet::Repetition::type repetition,
                                                            const ::parquet::Type::type type) {
        return ::parquet::schema::PrimitiveNode::Make(name, repetition, type);
    }

    // Helper function to create a group node
    static ::parquet::schema::NodePtr create_group_node(const std::string& name,
                                                        const ::parquet::Repetition::type repetition,
                                                        const ::parquet::schema::NodeVector& fields) {
        return ::parquet::schema::GroupNode::Make(name, repetition, fields);
    }

    // Helper function to create a group node with logical type
    static ::parquet::schema::NodePtr create_list_node(const std::string& name,
                                                       const ::parquet::Repetition::type repetition,
                                                       const ::parquet::schema::NodeVector& fields) {
        return ::parquet::schema::GroupNode::Make(name, repetition, fields, ::parquet::LogicalType::List());
    }

    static ::parquet::schema::NodePtr create_map_node(const std::string& name,
                                                      const ::parquet::Repetition::type repetition,
                                                      const ::parquet::schema::NodeVector& fields) {
        return ::parquet::schema::GroupNode::Make(name, repetition, fields, ::parquet::LogicalType::Map());
    }
};

namespace {

// Flattens an Arrow schema node into the thrift schema elements the native reader resolves.
// LIST/MAP are carried by logicalType only.
void node_to_thrift(const ::parquet::schema::NodePtr& node, std::vector<tparquet::SchemaElement>* t_schemas) {
    tparquet::SchemaElement element;
    element.__set_name(node->name());
    switch (node->repetition()) {
    case ::parquet::Repetition::REQUIRED:
        element.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
        break;
    case ::parquet::Repetition::OPTIONAL:
        element.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        break;
    default:
        element.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
        break;
    }
    if (node->logical_type()->is_list()) {
        tparquet::LogicalType logical_type;
        logical_type.__set_LIST(tparquet::ListType());
        element.__set_logicalType(logical_type);
    } else if (node->logical_type()->is_map()) {
        tparquet::LogicalType logical_type;
        logical_type.__set_MAP(tparquet::MapType());
        element.__set_logicalType(logical_type);
    }
    if (node->is_group()) {
        const auto* group_node = static_cast<const ::parquet::schema::GroupNode*>(node.get());
        element.__set_num_children(group_node->field_count());
        t_schemas->push_back(element);
        for (int i = 0; i < group_node->field_count(); ++i) {
            node_to_thrift(group_node->field(i), t_schemas);
        }
    } else {
        const auto* primitive_node = static_cast<const ::parquet::schema::PrimitiveNode*>(node.get());
        element.__set_type(static_cast<tparquet::Type::type>(primitive_node->physical_type()));
        t_schemas->push_back(element);
    }
}

Status resolve_by_native_reader(const ::parquet::schema::NodePtr& node, starrocks::parquet::SchemaDescriptor* desc) {
    std::vector<tparquet::SchemaElement> t_schemas;
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);
    t_schemas.push_back(root);
    node_to_thrift(node, &t_schemas);
    return desc->from_thrift(t_schemas, true);
}

void check_same_shape(const starrocks::parquet::ParquetField& field, const TypeDescriptor& type_desc) {
    switch (field.type) {
    case starrocks::parquet::ColumnType::ARRAY:
        ASSERT_EQ(TYPE_ARRAY, type_desc.type) << field.debug_string();
        break;
    case starrocks::parquet::ColumnType::MAP:
        ASSERT_EQ(TYPE_MAP, type_desc.type) << field.debug_string();
        break;
    case starrocks::parquet::ColumnType::STRUCT:
        ASSERT_EQ(TYPE_STRUCT, type_desc.type) << field.debug_string();
        ASSERT_EQ(field.children.size(), type_desc.field_names.size());
        for (size_t i = 0; i < field.children.size(); ++i) {
            ASSERT_EQ(field.children[i].name, type_desc.field_names[i]);
        }
        break;
    default:
        ASSERT_FALSE(type_desc.is_complex_type()) << field.debug_string();
        break;
    }
    ASSERT_EQ(field.children.size(), type_desc.children.size()) << field.debug_string();
    for (size_t i = 0; i < field.children.size(); ++i) {
        check_same_shape(field.children[i], type_desc.children[i]);
    }
}

// Infers the node's type and checks that the native reader resolves the same shape.
void check_inferred_type(const ::parquet::schema::NodePtr& node, const TypeDescriptor& expected) {
    TypeDescriptor type_desc;
    Status st = get_parquet_type(node, &type_desc);
    ASSERT_TRUE(st.ok()) << st.message();
    ASSERT_EQ(expected, type_desc);

    starrocks::parquet::SchemaDescriptor desc;
    st = resolve_by_native_reader(node, &desc);
    ASSERT_TRUE(st.ok()) << st.message();
    ASSERT_EQ(1, desc.get_fields_size());
    check_same_shape(desc.get_parquet_fields()[0], type_desc);
}

// Checks that both inference and the native reader reject the node.
void check_rejected(const ::parquet::schema::NodePtr& node) {
    TypeDescriptor type_desc;
    ASSERT_FALSE(get_parquet_type(node, &type_desc).ok()) << node->name();

    starrocks::parquet::SchemaDescriptor desc;
    ASSERT_FALSE(resolve_by_native_reader(node, &desc).ok()) << node->name();
}

TypeDescriptor varbinary_type() {
    return TypeDescriptor::create_varbinary_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
}

} // namespace

TEST_F(ParquetSchemaBuilderTest, MapKeyOnly) {
    // A map without values (a set) is read as a list of keys.
    // optional group my_set (MAP) {
    //   repeated group key_value {
    //     required int32 key;
    //   }
    // }
    auto key = create_primitive_node("key", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32);
    auto kv = create_group_node("key_value", ::parquet::Repetition::REPEATED, {key});
    auto node = create_map_node("my_set", ::parquet::Repetition::OPTIONAL, {kv});
    check_inferred_type(node, TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT)));
}

TEST_F(ParquetSchemaBuilderTest, MapInvalidKeyValue) {
    // Arrow nodes keep a parent pointer, so every group gets its own children.
    auto key = [] { return create_primitive_node("key", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32); };
    auto value = [] { return create_primitive_node("value", ::parquet::Repetition::OPTIONAL, ::parquet::Type::INT32); };

    // key_value is not repeated
    {
        auto kv = create_group_node("key_value", ::parquet::Repetition::REQUIRED, {key(), value()});
        check_rejected(create_map_node("map_1", ::parquet::Repetition::OPTIONAL, {kv}));
    }
    // key_value is not a group
    {
        auto kv = create_primitive_node("key_value", ::parquet::Repetition::REPEATED, ::parquet::Type::INT32);
        check_rejected(create_map_node("map_2", ::parquet::Repetition::OPTIONAL, {kv}));
    }
    // key_value has more than 2 children
    {
        auto extra = create_primitive_node("extra", ::parquet::Repetition::OPTIONAL, ::parquet::Type::INT32);
        auto kv = create_group_node("key_value", ::parquet::Repetition::REPEATED, {key(), value(), extra});
        check_rejected(create_map_node("map_3", ::parquet::Repetition::OPTIONAL, {kv}));
    }
    // the MAP group has more than 1 child
    {
        auto kv1 = create_group_node("key_value", ::parquet::Repetition::REPEATED, {key(), value()});
        auto kv2 = create_group_node("key_value2", ::parquet::Repetition::REPEATED, {key(), value()});
        check_rejected(create_map_node("map_4", ::parquet::Repetition::OPTIONAL, {kv1, kv2}));
    }
    // the MAP group is repeated
    {
        auto kv = create_group_node("key_value", ::parquet::Repetition::REPEATED, {key(), value()});
        check_rejected(create_map_node("map_5", ::parquet::Repetition::REPEATED, {kv}));
    }
}

TEST_F(ParquetSchemaBuilderTest, MapGroupKey) {
    // optional group my_map (MAP) {
    //   repeated group key_value {
    //     required group key { required int32 a; }
    //     optional int32 value;
    //   }
    // }
    auto a = create_primitive_node("a", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32);
    auto key = create_group_node("key", ::parquet::Repetition::REQUIRED, {a});
    auto value = create_primitive_node("value", ::parquet::Repetition::OPTIONAL, ::parquet::Type::INT32);
    auto kv = create_group_node("key_value", ::parquet::Repetition::REPEATED, {key, value});
    check_rejected(create_map_node("my_map", ::parquet::Repetition::OPTIONAL, {kv}));
}

TEST_F(ParquetSchemaBuilderTest, TopLevelRepeatedFields) {
    // One-level list encoding: repeated int32 a;
    {
        auto node = create_primitive_node("a", ::parquet::Repetition::REPEATED, ::parquet::Type::INT32);
        check_inferred_type(node, TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT)));
    }
    // repeated group rg { required int32 x; optional binary y; }
    {
        auto x = create_primitive_node("x", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32);
        auto y = create_primitive_node("y", ::parquet::Repetition::OPTIONAL, ::parquet::Type::BYTE_ARRAY);
        auto node = create_group_node("rg", ::parquet::Repetition::REPEATED, {x, y});
        check_inferred_type(node, TypeDescriptor::create_array_type(TypeDescriptor::create_struct_type(
                                          {"x", "y"}, {TypeDescriptor(TYPE_INT), varbinary_type()})));
    }
    // A repeated field inside a struct: optional group s { repeated int32 a; }
    {
        auto a = create_primitive_node("a", ::parquet::Repetition::REPEATED, ::parquet::Type::INT32);
        auto node = create_group_node("s", ::parquet::Repetition::OPTIONAL, {a});
        check_inferred_type(node, TypeDescriptor::create_struct_type(
                                          {"a"}, {TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT))}));
    }
}

TEST_F(ParquetSchemaBuilderTest, LegacyListEncodings) {
    const auto int_array = TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT));

    // Two-level encoding, the repeated primitive is the element (not a nested list):
    // optional group my_list (LIST) { repeated int32 element; }
    {
        auto element = create_primitive_node("element", ::parquet::Repetition::REPEATED, ::parquet::Type::INT32);
        check_inferred_type(create_list_node("my_list", ::parquet::Repetition::OPTIONAL, {element}), int_array);
    }
    // A repeated group with several fields is a struct element:
    // optional group my_list (LIST) { repeated group element { required binary str; required int32 num; } }
    {
        auto str = create_primitive_node("str", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY);
        auto num = create_primitive_node("num", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32);
        auto element = create_group_node("element", ::parquet::Repetition::REPEATED, {str, num});
        check_inferred_type(create_list_node("my_list", ::parquet::Repetition::OPTIONAL, {element}),
                            TypeDescriptor::create_array_type(TypeDescriptor::create_struct_type(
                                    {"str", "num"}, {varbinary_type(), TypeDescriptor(TYPE_INT)})));
    }
    // Nested two-level list:
    // optional group my_list (LIST) { repeated group array (LIST) { repeated int32 array; } }
    {
        auto inner = create_primitive_node("array", ::parquet::Repetition::REPEATED, ::parquet::Type::INT32);
        auto array = create_list_node("array", ::parquet::Repetition::REPEATED, {inner});
        check_inferred_type(create_list_node("my_list", ::parquet::Repetition::OPTIONAL, {array}),
                            TypeDescriptor::create_array_type(int_array));
    }
    // Same without the LIST annotation on the repeated group:
    // optional group my_list (LIST) { repeated group bag { repeated int32 item; } }
    {
        auto item = create_primitive_node("item", ::parquet::Repetition::REPEATED, ::parquet::Type::INT32);
        auto bag = create_group_node("bag", ::parquet::Repetition::REPEATED, {item});
        check_inferred_type(create_list_node("my_list", ::parquet::Repetition::OPTIONAL, {bag}),
                            TypeDescriptor::create_array_type(int_array));
    }
    // ... but an unannotated repeated group named `array` or `<list name>_tuple` stays a struct element, even
    // when its single child is repeated:
    // optional group my_list (LIST) { repeated group array { repeated int32 nums; } }
    // optional group my_list (LIST) { repeated group my_list_tuple { repeated int32 nums; } }
    for (const std::string name : {"array", "my_list_tuple"}) {
        auto nums = create_primitive_node("nums", ::parquet::Repetition::REPEATED, ::parquet::Type::INT32);
        auto group = create_group_node(name, ::parquet::Repetition::REPEATED, {nums});
        check_inferred_type(
                create_list_node("my_list", ::parquet::Repetition::OPTIONAL, {group}),
                TypeDescriptor::create_array_type(TypeDescriptor::create_struct_type({"nums"}, {int_array})));
    }
    // A single-field repeated group named `array` is a struct element:
    // optional group my_list (LIST) { repeated group array { required binary str; } }
    {
        auto str = create_primitive_node("str", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY);
        auto array = create_group_node("array", ::parquet::Repetition::REPEATED, {str});
        check_inferred_type(
                create_list_node("my_list", ::parquet::Repetition::OPTIONAL, {array}),
                TypeDescriptor::create_array_type(TypeDescriptor::create_struct_type({"str"}, {varbinary_type()})));
    }
    // ... and so is one named `<list name>_tuple`:
    // optional group my_list (LIST) { repeated group my_list_tuple { required binary str; } }
    {
        auto str = create_primitive_node("str", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY);
        auto tuple = create_group_node("my_list_tuple", ::parquet::Repetition::REPEATED, {str});
        check_inferred_type(
                create_list_node("my_list", ::parquet::Repetition::OPTIONAL, {tuple}),
                TypeDescriptor::create_array_type(TypeDescriptor::create_struct_type({"str"}, {varbinary_type()})));
    }
    // ... but not one named after another list: 3-level encoding.
    // optional group other_list (LIST) { repeated group my_list_tuple { required binary str; } }
    {
        auto str = create_primitive_node("str", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY);
        auto tuple = create_group_node("my_list_tuple", ::parquet::Repetition::REPEATED, {str});
        check_inferred_type(create_list_node("other_list", ::parquet::Repetition::OPTIONAL, {tuple}),
                            TypeDescriptor::create_array_type(varbinary_type()));
    }
    // A repeated LIST group is invalid.
    {
        auto element = create_primitive_node("element", ::parquet::Repetition::REPEATED, ::parquet::Type::INT32);
        check_rejected(create_list_node("my_list", ::parquet::Repetition::REPEATED, {element}));
    }
}

} // namespace starrocks
