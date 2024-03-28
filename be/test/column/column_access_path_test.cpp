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

#include "column/column_access_path.h"

#include <gtest/gtest.h>

namespace starrocks {

class ColumnAccessPathTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(ColumnAccessPathTest, test_struct) {
    ColumnAccessPathPtr root = std::make_unique<ColumnAccessPath>();
    Status st = root->init(TAccessPathType::ROOT, "c1", -1);
    ASSERT_TRUE(st.ok());

    ColumnAccessPathPtr subfield1 = std::make_unique<ColumnAccessPath>();
    st = subfield1->init(TAccessPathType::FIELD, "subfield1", -1);
    ASSERT_TRUE(st.ok());
    root->children().emplace_back(std::move(subfield1));

    {
        TypeDescriptor struct_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
        struct_type.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        struct_type.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        struct_type.field_names.emplace_back("subfield1");
        struct_type.field_names.emplace_back("subfield2");

        ColumnAccessPathUtil::rewrite_complex_type_descriptor(struct_type, nullptr);
        ASSERT_EQ("STRUCT{subfield1 INT, subfield2 INT}", struct_type.debug_string());

        ColumnAccessPathUtil::rewrite_complex_type_descriptor(struct_type, root);
        ASSERT_EQ("STRUCT{subfield1 INT}", struct_type.debug_string());
    }

    {
        TypeDescriptor struct_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
        TypeDescriptor struct_struct_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);

        struct_struct_type.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        struct_struct_type.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        struct_struct_type.field_names.emplace_back("subfield1");
        struct_struct_type.field_names.emplace_back("subfield2");

        struct_type.children.emplace_back(struct_struct_type);
        struct_type.field_names.emplace_back("subfield1");

        ColumnAccessPathPtr subfield1_subfield1 = std::make_unique<ColumnAccessPath>();
        st = subfield1_subfield1->init(TAccessPathType::FIELD, "subfield1", -1);
        ASSERT_TRUE(st.ok());
        root->children()[0]->children().emplace_back(std::move(subfield1_subfield1));

        ColumnAccessPathUtil::rewrite_complex_type_descriptor(struct_type, root);
        ASSERT_EQ("STRUCT{subfield1 STRUCT{subfield1 INT}}", struct_type.debug_string());
    }
}

TEST_F(ColumnAccessPathTest, test_map_keys) {
    ColumnAccessPathPtr root = std::make_unique<ColumnAccessPath>();
    Status st = root->init(TAccessPathType::ROOT, "c1", -1);
    ASSERT_TRUE(st.ok());

    ColumnAccessPathPtr keys = std::make_unique<ColumnAccessPath>();
    st = keys->init(TAccessPathType::KEY, "P", -1);
    ASSERT_TRUE(st.ok());
    root->children().emplace_back(std::move(keys));

    TypeDescriptor map_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_MAP);
    map_type.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    map_type.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    ColumnAccessPathUtil::rewrite_complex_type_descriptor(map_type, root);
    ASSERT_EQ("MAP<INT, INVALID>", map_type.debug_string());
}

TEST_F(ColumnAccessPathTest, test_map_values) {
    ColumnAccessPathPtr root = std::make_unique<ColumnAccessPath>();
    Status st = root->init(TAccessPathType::ROOT, "c1", -1);
    ASSERT_TRUE(st.ok());

    ColumnAccessPathPtr keys = std::make_unique<ColumnAccessPath>();
    st = keys->init(TAccessPathType::VALUE, "P", -1);
    ASSERT_TRUE(st.ok());
    root->children().emplace_back(std::move(keys));

    TypeDescriptor map_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_MAP);
    map_type.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    map_type.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    ColumnAccessPathUtil::rewrite_complex_type_descriptor(map_type, nullptr);
    ASSERT_EQ("MAP<INT, INT>", map_type.debug_string());

    ColumnAccessPathUtil::rewrite_complex_type_descriptor(map_type, root);
    ASSERT_EQ("MAP<INVALID, INT>", map_type.debug_string());
}

TEST_F(ColumnAccessPathTest, test_map_values_struct) {
    ColumnAccessPathPtr root = std::make_unique<ColumnAccessPath>();
    Status st = root->init(TAccessPathType::ROOT, "c1", -1);
    ASSERT_TRUE(st.ok());

    ColumnAccessPathPtr keys = std::make_unique<ColumnAccessPath>();
    st = keys->init(TAccessPathType::VALUE, "P", -1);
    ASSERT_TRUE(st.ok());
    root->children().emplace_back(std::move(keys));

    ColumnAccessPathPtr keys_index = std::make_unique<ColumnAccessPath>();
    st = keys_index->init(TAccessPathType::INDEX, "P", -1);
    ASSERT_TRUE(st.ok());
    root->children()[0]->children().emplace_back(std::move(keys_index));

    ColumnAccessPathPtr keys_index_field = std::make_unique<ColumnAccessPath>();
    st = keys_index_field->init(TAccessPathType::FIELD, "subfield1", -1);
    ASSERT_TRUE(st.ok());
    root->children()[0]->children()[0]->children().emplace_back(std::move(keys_index_field));

    TypeDescriptor map_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_MAP);
    map_type.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    TypeDescriptor map_value_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    map_value_type.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    map_value_type.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    map_value_type.field_names.emplace_back("subfield1");
    map_value_type.field_names.emplace_back("subfield2");

    map_type.children.emplace_back(map_value_type);

    ColumnAccessPathUtil::rewrite_complex_type_descriptor(map_type, nullptr);
    ASSERT_EQ("MAP<INT, STRUCT{subfield1 INT, subfield2 INT}>", map_type.debug_string());

    ColumnAccessPathUtil::rewrite_complex_type_descriptor(map_type, root);
    ASSERT_EQ("MAP<INVALID, STRUCT{subfield1 INT}>", map_type.debug_string());
}

TEST_F(ColumnAccessPathTest, test_array_struct) {
    ColumnAccessPathPtr root = std::make_unique<ColumnAccessPath>();
    Status st = root->init(TAccessPathType::ROOT, "c1", -1);
    ASSERT_TRUE(st.ok());

    ColumnAccessPathPtr elements = std::make_unique<ColumnAccessPath>();
    st = elements->init(TAccessPathType::INDEX, "P", -1);
    ASSERT_TRUE(st.ok());
    root->children().emplace_back(std::move(elements));

    ColumnAccessPathPtr elements_struct = std::make_unique<ColumnAccessPath>();
    st = elements_struct->init(TAccessPathType::FIELD, "subfield1", -1);
    ASSERT_TRUE(st.ok());
    root->children()[0]->children().emplace_back(std::move(elements_struct));

    TypeDescriptor array_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);
    TypeDescriptor array_struct_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    array_struct_type.children.emplace_back(TypeDescriptor::from_logical_type(TYPE_INT));
    array_struct_type.children.emplace_back(TypeDescriptor::from_logical_type(TYPE_INT));
    array_struct_type.field_names.emplace_back("subfield1");
    array_struct_type.field_names.emplace_back("subfield2");
    array_type.children.emplace_back(array_struct_type);

    ColumnAccessPathUtil::rewrite_complex_type_descriptor(array_type, nullptr);
    ASSERT_EQ("ARRAY<STRUCT{subfield1 INT, subfield2 INT}>", array_type.debug_string());

    ColumnAccessPathUtil::rewrite_complex_type_descriptor(array_type, root);
    ASSERT_EQ("ARRAY<STRUCT{subfield1 INT}>", array_type.debug_string());
}

} // namespace starrocks