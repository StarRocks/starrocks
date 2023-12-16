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

#include <iostream>
#include <utility>
#include <vector>

#include "formats/parquet/schema.h"

namespace starrocks::parquet {

using namespace tparquet;

class PrimitiveNode {
public:
    static SchemaElement make(const std::string& name, FieldRepetitionType::type repetition, Type::type type,
                              ConvertedType::type converted_type) {
        SchemaElement element;
        element.__set_name(name);
        element.__set_repetition_type(repetition);
        element.__set_type(type);
        element.__set_converted_type(converted_type);
        return element;
    }
    static SchemaElement make(const std::string& name, FieldRepetitionType::type repetition, Type::type type) {
        SchemaElement element;
        element.__set_name(name);
        element.__set_repetition_type(repetition);
        element.__set_type(type);
        return element;
    }
    static ParquetField make_field(const std::string& name, bool is_nullable, Type::type type) {
        ParquetField field;
        field.name = name;
        field.physical_type = type;
        field.is_nullable = is_nullable;
        return field;
    }
};

class GroupNode {
public:
    static SchemaElement make_root(int32_t num_children) {
        SchemaElement element;
        element.__set_name("root_node");
        element.__set_num_children(num_children);
        return element;
    }
    static SchemaElement make(const std::string& name, FieldRepetitionType::type repetition,
                              ConvertedType::type converted_type, int32_t num_children) {
        SchemaElement element;
        element.__set_name(name);
        element.__set_repetition_type(repetition);
        element.__set_converted_type(converted_type);
        element.__set_num_children(num_children);
        return element;
    }
    static SchemaElement make(const std::string& name, FieldRepetitionType::type repetition, int32_t num_children) {
        SchemaElement element;
        element.__set_name(name);
        element.__set_repetition_type(repetition);
        element.__set_num_children(num_children);
        return element;
    }
    static ParquetField make_field(const std::string& name, bool is_nullable, LogicalType type,
                                   std::vector<ParquetField> children) {
        ParquetField field;
        field.name = name;
        field.is_nullable = is_nullable;
        field.type.type = type;
        field.children = std::move(children);
        return field;
    }
};

class ParquetLevelsTest : public testing::Test {
public:
    ParquetLevelsTest() = default;
    ~ParquetLevelsTest() override = default;

protected:
    LevelInfo make(int16_t def_level, int16_t rep_level, int16_t ancestor_def_level) {
        LevelInfo level;
        level.immediate_repeated_ancestor_def_level = ancestor_def_level;
        level.max_def_level = def_level;
        level.max_rep_level = rep_level;
        return level;
    }

    void do_check(const std::vector<SchemaElement>& t_schemas, const std::vector<LevelInfo>& expected_levels) {
        SchemaDescriptor desc;
        auto st = desc.from_thrift(t_schemas, true);
        ASSERT_TRUE(st.ok()) << st.message();
        const auto& actual = _collect_flatten_levels(desc.get_parquet_fields());
        _check_flatten_levels(expected_levels, actual);
    }

private:
    const std::vector<LevelInfo> _collect_flatten_levels(const std::vector<ParquetField>& fields) {
        std::vector<LevelInfo> vec;
        for (const auto& field : fields) {
            vec.emplace_back(field.level_info);
            if (field.children.size() > 0) {
                const auto& child = _collect_flatten_levels(field.children);
                vec.insert(vec.end(), child.begin(), child.end());
            }
        }
        return vec;
    }

    void _check_flatten_levels(const std::vector<LevelInfo>& expected, const std::vector<LevelInfo>& actual) {
        ASSERT_EQ(expected.size(), actual.size());
        for (size_t i = 0; i < expected.size(); i++) {
            ASSERT_EQ(expected[i].immediate_repeated_ancestor_def_level,
                      actual[i].immediate_repeated_ancestor_def_level);
            ASSERT_EQ(expected[i].max_def_level, actual[i].max_def_level);
            ASSERT_EQ(expected[i].max_rep_level, actual[i].max_rep_level);
        }
    }
};

class ParquetSchemaTest : public testing::Test {
public:
    ParquetSchemaTest() = default;
    ~ParquetSchemaTest() override = default;

protected:
    void check_flat_parquet_field(const std::vector<ParquetField>& expected, const std::vector<ParquetField>& actual) {
        ASSERT_EQ(expected.size(), actual.size());
        if (expected.size() == 0) {
            return;
        }
        for (size_t i = 0; i < expected.size(); i++) {
            if (expected[i].children.size() > 0) {
                // Is group node
                ASSERT_EQ(expected[i].name, actual[i].name);
                ASSERT_EQ(expected[i].is_nullable, actual[i].is_nullable);
                ASSERT_EQ(expected[i].type.type, actual[i].type.type);
            } else {
                // is primitive node
                ASSERT_EQ(expected[i].name, actual[i].name);
                ASSERT_EQ(expected[i].is_nullable, actual[i].is_nullable);
                ASSERT_EQ(expected[i].physical_type, actual[i].physical_type);
            }
            // check for children
            check_flat_parquet_field(expected[i].children, actual[i].children);
        }
    }
};

TEST_F(ParquetSchemaTest, EmptySchema) {
    std::vector<SchemaElement> t_schemas;
    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, OnlyRoot) {
    std::vector<SchemaElement> t_schemas;

    t_schemas.resize(1);
    {
        auto& t_schema = t_schemas[0];
        t_schema.name = "hive-schema";
        t_schema.__set_num_children(0);
    }
    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, OnlyLeafType) {
    std::vector<SchemaElement> t_schemas;

    t_schemas.resize(3);
    // Root
    {
        auto& t_schema = t_schemas[0];
        t_schema.name = "hive-schema";
        t_schema.__set_num_children(2);
    }
    // INT32
    {
        auto& t_schema = t_schemas[1];
        t_schema.__set_type(Type::INT32);
        t_schema.name = "col1";
        t_schema.__set_num_children(0);
        t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
    }
    // BYTE_ARRAY
    {
        auto& t_schema = t_schemas[2];
        t_schema.__set_type(Type::BYTE_ARRAY);
        t_schema.name = "col2";
        t_schema.__set_num_children(0);
        t_schema.__set_repetition_type(FieldRepetitionType::REQUIRED);
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_TRUE(st.ok());

    {
        auto idx = desc.get_field_idx_by_column_name("col1");
        ASSERT_EQ(0, idx);
        auto field = desc.get_stored_column_by_field_idx(0);
        ASSERT_STREQ("col1", field->name.c_str());
        ASSERT_EQ(0, field->physical_column_index);
        ASSERT_EQ(1, field->max_def_level());
        ASSERT_EQ(0, field->max_rep_level());
        ASSERT_EQ(true, field->is_nullable);
    }

    {
        auto idx = desc.get_field_idx_by_column_name("col2");
        ASSERT_EQ(1, idx);
        auto field = desc.get_stored_column_by_field_idx(1);
        ASSERT_STREQ("col2", field->name.c_str());
        ASSERT_EQ(1, field->physical_column_index);
        ASSERT_EQ(0, field->max_def_level());
        ASSERT_EQ(0, field->max_rep_level());
        ASSERT_EQ(false, field->is_nullable);
    }
    LOG(INFO) << desc.debug_string();
}

TEST_F(ParquetSchemaTest, NestedType) {
    std::vector<SchemaElement> t_schemas;

    t_schemas.resize(8);
    // Root
    {
        auto& t_schema = t_schemas[0];
        t_schema.name = "hive-schema";
        t_schema.__set_num_children(3);
    }
    // col1: INT32
    {
        auto& t_schema = t_schemas[1];
        t_schema.__set_type(Type::INT32);
        t_schema.name = "col1";
        t_schema.__set_num_children(0);
        t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
    }

    // col2: LIST(BYTE_ARRAY)
    {
        int idx_base = 2;

        // level-1
        {
            auto& t_schema = t_schemas[idx_base + 0];
            t_schema.name = "col2";
            t_schema.__set_num_children(1);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(ConvertedType::LIST);
        }

        // level-2
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "list";
            t_schema.__set_num_children(1);
            t_schema.__set_repetition_type(FieldRepetitionType::REPEATED);
        }

        // level-3
        {
            auto& t_schema = t_schemas[idx_base + 2];
            t_schema.__set_type(Type::BYTE_ARRAY);
            t_schema.name = "element";
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
        }
    }

    // col3: STRUCT{ a: INT32, b: BYTE_ARRAY}
    {
        int idx_base = 5;

        {
            auto& t_schema = t_schemas[idx_base + 0];
            t_schema.name = "col3";
            t_schema.__set_num_children(2);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
        }

        // field-a
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.__set_type(Type::INT32);
            t_schema.name = "a";
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
        }

        // field-b
        {
            auto& t_schema = t_schemas[idx_base + 2];
            t_schema.__set_type(Type::BYTE_ARRAY);
            t_schema.name = "b";
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(FieldRepetitionType::REQUIRED);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_TRUE(st.ok());

    // Check col2
    {
        auto field = desc.get_stored_column_by_column_name("col2");
        ASSERT_EQ(TYPE_ARRAY, field->type.type);
        ASSERT_EQ(2, field->max_def_level());
        ASSERT_EQ(1, field->max_rep_level());
        ASSERT_EQ(0, field->level_info.immediate_repeated_ancestor_def_level);
        ASSERT_EQ(true, field->is_nullable);

        auto child = &field->children[0];
        ASSERT_EQ(3, child->max_def_level());
        ASSERT_EQ(1, child->max_rep_level());
        ASSERT_EQ(2, child->level_info.immediate_repeated_ancestor_def_level);
        ASSERT_EQ(1, child->physical_column_index);
        ASSERT_EQ(true, child->is_nullable);
    }

    // Check col3
    {
        auto field = desc.get_stored_column_by_column_name("col3");
        ASSERT_EQ(TYPE_STRUCT, field->type.type);
        ASSERT_EQ(1, field->max_def_level());
        ASSERT_EQ(0, field->max_rep_level());
        ASSERT_EQ(true, field->is_nullable);

        auto child_a = &field->children[0];
        ASSERT_EQ(2, child_a->max_def_level());
        ASSERT_EQ(0, child_a->max_rep_level());
        ASSERT_EQ(2, child_a->physical_column_index);
        ASSERT_EQ(true, child_a->is_nullable);

        auto child_b = &field->children[1];
        ASSERT_EQ(1, child_b->max_def_level());
        ASSERT_EQ(0, child_b->max_rep_level());
        ASSERT_EQ(3, child_b->physical_column_index);
        ASSERT_EQ(false, child_b->is_nullable);
    }

    LOG(INFO) << desc.debug_string();
}

TEST_F(ParquetSchemaTest, InvalidCase1) {
    std::vector<SchemaElement> t_schemas;

    t_schemas.resize(3);
    // Root
    {
        auto& t_schema = t_schemas[0];
        t_schema.name = "hive-schema";
        t_schema.__set_num_children(1);
    }
    // INT32
    {
        auto& t_schema = t_schemas[1];
        t_schema.__set_type(Type::INT32);
        t_schema.name = "col1";
        t_schema.__set_num_children(0);
        t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
    }
    // BYTE_ARRAY
    {
        auto& t_schema = t_schemas[2];
        t_schema.__set_type(Type::BYTE_ARRAY);
        t_schema.name = "col2";
        t_schema.__set_num_children(0);
        t_schema.__set_repetition_type(FieldRepetitionType::REQUIRED);
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, InvalidCase2) {
    std::vector<SchemaElement> t_schemas;

    t_schemas.resize(3);
    // Root
    {
        auto& t_schema = t_schemas[0];
        t_schema.name = "hive-schema";
        t_schema.__set_num_children(3);
    }
    // INT32
    {
        auto& t_schema = t_schemas[1];
        t_schema.__set_type(Type::INT32);
        t_schema.name = "col1";
        t_schema.__set_num_children(0);
        t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
    }
    // BYTE_ARRAY
    {
        auto& t_schema = t_schemas[2];
        t_schema.__set_type(Type::BYTE_ARRAY);
        t_schema.name = "col2";
        t_schema.__set_num_children(0);
        t_schema.__set_repetition_type(FieldRepetitionType::REQUIRED);
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, InvalidList1) {
    std::vector<SchemaElement> t_schemas;

    t_schemas.resize(2);
    // Root
    {
        auto& t_schema = t_schemas[0];
        t_schema.name = "hive-schema";
        t_schema.__set_num_children(1);
    }
    // Col1: Array
    {
        int idx_base = 1;

        // level-1
        {
            auto& t_schema = t_schemas[idx_base + 0];
            t_schema.name = "col2";
            t_schema.__set_num_children(1);
            t_schema.__set_repetition_type(FieldRepetitionType::REPEATED);
            t_schema.__set_converted_type(ConvertedType::LIST);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, InvalidList2) {
    std::vector<SchemaElement> t_schemas;

    t_schemas.resize(2);
    // Root
    {
        auto& t_schema = t_schemas[0];
        t_schema.name = "hive-schema";
        t_schema.__set_num_children(1);
    }
    // Col1: Array
    {
        int idx_base = 1;

        // level-1
        {
            auto& t_schema = t_schemas[idx_base + 0];
            t_schema.name = "col2";
            t_schema.__set_num_children(2);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(ConvertedType::LIST);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, InvalidList3) {
    std::vector<SchemaElement> t_schemas;

    t_schemas.resize(2);
    // Root
    {
        auto& t_schema = t_schemas[0];
        t_schema.name = "hive-schema";
        t_schema.__set_num_children(1);
    }
    // Col1: Array
    {
        int idx_base = 1;

        // level-1
        {
            auto& t_schema = t_schemas[idx_base + 0];
            t_schema.name = "col2";
            t_schema.__set_num_children(1);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(ConvertedType::LIST);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, InvalidList4) {
    std::vector<SchemaElement> t_schemas;

    t_schemas.resize(3);
    // Root
    {
        auto& t_schema = t_schemas[0];
        t_schema.name = "hive-schema";
        t_schema.__set_num_children(1);
    }
    // Col1: Array
    {
        int idx_base = 1;

        // level-1
        {
            auto& t_schema = t_schemas[idx_base + 0];
            t_schema.name = "col2";
            t_schema.__set_num_children(1);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(ConvertedType::LIST);
        }

        // level-2
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "list";
            t_schema.__set_num_children(1);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, SimpleArray) {
    std::vector<SchemaElement> t_schemas;

    t_schemas.resize(2);
    // Root
    {
        auto& t_schema = t_schemas[0];
        t_schema.name = "hive-schema";
        t_schema.__set_num_children(1);
    }
    // Col1: Array
    {
        int idx_base = 1;

        // level-1
        {
            auto& t_schema = t_schemas[idx_base + 0];
            t_schema.name = "col2";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(FieldRepetitionType::REPEATED);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_TRUE(st.ok());
    {
        auto field = desc.get_stored_column_by_column_name("col2");
        ASSERT_EQ(TYPE_ARRAY, field->type.type);
        ASSERT_EQ(1, field->max_def_level());
        ASSERT_EQ(1, field->max_rep_level());
        ASSERT_EQ(false, field->is_nullable);
        ASSERT_EQ(0, field->level_info.immediate_repeated_ancestor_def_level);

        auto child = &field->children[0];
        ASSERT_EQ(1, child->max_def_level());
        ASSERT_EQ(1, child->max_rep_level());
        ASSERT_EQ(false, child->is_nullable);
        ASSERT_EQ(1, child->level_info.immediate_repeated_ancestor_def_level);
        ASSERT_EQ(0, child->physical_column_index);
    }
}

TEST_F(ParquetSchemaTest, TwoLevelArray) {
    std::vector<SchemaElement> t_schemas;

    t_schemas.resize(3);
    // Root
    {
        auto& t_schema = t_schemas[0];
        t_schema.name = "hive-schema";
        t_schema.__set_num_children(1);
    }
    // Col1: Array
    {
        int idx_base = 1;

        // level-1
        {
            auto& t_schema = t_schemas[idx_base + 0];
            t_schema.name = "col2";
            t_schema.__set_num_children(1);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(ConvertedType::LIST);
        }

        // level-2
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "field";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(FieldRepetitionType::REPEATED);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_TRUE(st.ok());
    {
        auto field = desc.get_stored_column_by_column_name("col2");
        ASSERT_EQ(TYPE_ARRAY, field->type.type);
        ASSERT_EQ(2, field->max_def_level());
        ASSERT_EQ(1, field->max_rep_level());
        ASSERT_EQ(true, field->is_nullable);
        ASSERT_EQ(0, field->level_info.immediate_repeated_ancestor_def_level);

        auto child = &field->children[0];
        ASSERT_EQ(2, child->max_def_level());
        ASSERT_EQ(1, child->max_rep_level());
        ASSERT_EQ(false, child->is_nullable);
        ASSERT_EQ(2, child->level_info.immediate_repeated_ancestor_def_level);
        ASSERT_EQ(0, child->physical_column_index);
    }
}

TEST_F(ParquetSchemaTest, MapNormal) {
    std::vector<SchemaElement> t_schemas;

    t_schemas.resize(5);
    // Root
    {
        auto& t_schema = t_schemas[0];
        t_schema.name = "hive-schema";
        t_schema.__set_num_children(1);
    }
    // Col1: Array
    {
        int idx_base = 1;

        // level-1
        {
            auto& t_schema = t_schemas[idx_base + 0];
            t_schema.name = "col2";
            t_schema.__set_num_children(1);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(ConvertedType::MAP);
        }

        // level-2
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "key_value";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(2);
            t_schema.__set_repetition_type(FieldRepetitionType::REPEATED);
        }
        // key
        {
            auto& t_schema = t_schemas[idx_base + 2];
            t_schema.name = "key";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(FieldRepetitionType::REQUIRED);
        }
        // value
        {
            auto& t_schema = t_schemas[idx_base + 3];
            t_schema.name = "value";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_TRUE(st.ok());
    {
        auto field = desc.get_stored_column_by_column_name("col2");
        ASSERT_EQ(TYPE_MAP, field->type.type);
        ASSERT_EQ(2, field->max_def_level());
        ASSERT_EQ(1, field->max_rep_level());
        ASSERT_EQ(true, field->is_nullable);
        ASSERT_EQ(0, field->level_info.immediate_repeated_ancestor_def_level);

        auto key_value = &field->children[0];
        ASSERT_EQ(2, key_value->max_def_level());
        ASSERT_EQ(1, key_value->max_rep_level());
        ASSERT_EQ(false, key_value->is_nullable);
        ASSERT_EQ(2, key_value->level_info.immediate_repeated_ancestor_def_level);
    }
}

TEST_F(ParquetSchemaTest, InvalidMap1) {
    std::vector<SchemaElement> t_schemas;

    t_schemas.resize(3);
    // Root
    {
        auto& t_schema = t_schemas[0];
        t_schema.name = "hive-schema";
        t_schema.__set_num_children(1);
    }
    // Col1: Array
    {
        int idx_base = 1;

        // level-1
        {
            auto& t_schema = t_schemas[idx_base + 0];
            t_schema.name = "col2";
            t_schema.__set_num_children(1);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(ConvertedType::MAP);
        }

        // level-2
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "key_value";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(2);
            t_schema.__set_repetition_type(FieldRepetitionType::REPEATED);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, InvalidMap2) {
    std::vector<SchemaElement> t_schemas;

    t_schemas.resize(4);
    // Root
    {
        auto& t_schema = t_schemas[0];
        t_schema.name = "hive-schema";
        t_schema.__set_num_children(1);
    }
    // Col1: Array
    {
        int idx_base = 1;

        // level-1
        {
            auto& t_schema = t_schemas[idx_base + 0];
            t_schema.name = "col2";
            t_schema.__set_num_children(2);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(ConvertedType::MAP);
        }

        // key
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "key";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(FieldRepetitionType::REQUIRED);
        }
        // value
        {
            auto& t_schema = t_schemas[idx_base + 2];
            t_schema.name = "value";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, InvalidMap3) {
    std::vector<SchemaElement> t_schemas;

    t_schemas.resize(4);
    // Root
    {
        auto& t_schema = t_schemas[0];
        t_schema.name = "hive-schema";
        t_schema.__set_num_children(1);
    }
    // Col1: Array
    {
        int idx_base = 1;

        // level-1
        {
            auto& t_schema = t_schemas[idx_base + 0];
            t_schema.name = "col2";
            t_schema.__set_num_children(2);
            t_schema.__set_repetition_type(FieldRepetitionType::REPEATED);
            t_schema.__set_converted_type(ConvertedType::MAP);
        }

        // key
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "key";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(FieldRepetitionType::REQUIRED);
        }
        // value
        {
            auto& t_schema = t_schemas[idx_base + 2];
            t_schema.name = "value";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, InvalidMap4) {
    std::vector<SchemaElement> t_schemas;

    t_schemas.resize(5);
    // Root
    {
        auto& t_schema = t_schemas[0];
        t_schema.name = "hive-schema";
        t_schema.__set_num_children(1);
    }
    // Col1: Array
    {
        int idx_base = 1;

        // level-1
        {
            auto& t_schema = t_schemas[idx_base + 0];
            t_schema.name = "col2";
            t_schema.__set_num_children(1);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(ConvertedType::MAP);
        }

        // level-2
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "key_value";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(2);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
        }
        // key
        {
            auto& t_schema = t_schemas[idx_base + 2];
            t_schema.name = "key";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(FieldRepetitionType::REQUIRED);
        }
        // value
        {
            auto& t_schema = t_schemas[idx_base + 3];
            t_schema.name = "value";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

// But we still need to support an invalid map which map's key is optional, just make compatible with Trino
TEST_F(ParquetSchemaTest, InvalidMap5) {
    std::vector<SchemaElement> t_schemas;

    t_schemas.resize(5);
    // Root
    {
        auto& t_schema = t_schemas[0];
        t_schema.name = "hive-schema";
        t_schema.__set_num_children(1);
    }
    // Col1: Array
    {
        int idx_base = 1;

        // level-1
        {
            auto& t_schema = t_schemas[idx_base + 0];
            t_schema.name = "col2";
            t_schema.__set_num_children(1);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(ConvertedType::MAP);
        }

        // level-2
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "key_value";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(2);
            t_schema.__set_repetition_type(FieldRepetitionType::REPEATED);
        }
        // key
        {
            auto& t_schema = t_schemas[idx_base + 2];
            t_schema.name = "key";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
        }
        // value
        {
            auto& t_schema = t_schemas[idx_base + 3];
            t_schema.name = "value";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    EXPECT_TRUE(st.ok());
}

TEST_F(ParquetSchemaTest, InvalidMap6) {
    std::vector<SchemaElement> t_schemas;

    t_schemas.resize(6);
    // Root
    {
        auto& t_schema = t_schemas[0];
        t_schema.name = "hive-schema";
        t_schema.__set_num_children(1);
    }
    // Col1: Array
    {
        int idx_base = 1;

        // level-1
        {
            auto& t_schema = t_schemas[idx_base + 0];
            t_schema.name = "col2";
            t_schema.__set_num_children(1);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(ConvertedType::MAP);
        }

        // level-2
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "key_value";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(3);
            t_schema.__set_repetition_type(FieldRepetitionType::REPEATED);
        }
        // key
        {
            auto& t_schema = t_schemas[idx_base + 2];
            t_schema.name = "key";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
        }
        // value
        {
            auto& t_schema = t_schemas[idx_base + 3];
            t_schema.name = "value";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
        }
        // value3
        {
            auto& t_schema = t_schemas[idx_base + 4];
            t_schema.name = "value2";
            t_schema.__set_type(Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(FieldRepetitionType::OPTIONAL);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, DuplicateFieldNames) {
    std::vector<SchemaElement> t_schemas;
    t_schemas.emplace_back(GroupNode::make_root(2));
    t_schemas.emplace_back(PrimitiveNode::make("xxx", FieldRepetitionType::type::REQUIRED, Type::type::BYTE_ARRAY,
                                               ConvertedType::type::UTF8));
    t_schemas.emplace_back(PrimitiveNode::make("xxx", FieldRepetitionType::type::REQUIRED, Type::type::BYTE_ARRAY,
                                               ConvertedType::type::UTF8));

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok()) << st.message();
}

// The UT logic is copied from https://github.com/apache/arrow/blob/main/cpp/src/parquet/arrow/arrow_schema_test.cc

TEST_F(ParquetSchemaTest, ParquetMaps) {
    std::vector<SchemaElement> t_schemas;
    t_schemas.emplace_back(GroupNode::make_root(3));
    std::vector<ParquetField> expected_fields;
    // two column map
    {
        t_schemas.emplace_back(GroupNode::make("my_map", FieldRepetitionType::REQUIRED, ConvertedType::type::MAP, 1));
        t_schemas.emplace_back(GroupNode::make("key_value", FieldRepetitionType::REPEATED, 2));
        t_schemas.emplace_back(PrimitiveNode::make("key", FieldRepetitionType::type::REQUIRED, Type::type::BYTE_ARRAY,
                                                   ConvertedType::type::UTF8));
        t_schemas.emplace_back(PrimitiveNode::make("value", FieldRepetitionType::type::OPTIONAL, Type::type::BYTE_ARRAY,
                                                   ConvertedType::type::UTF8));

        expected_fields.emplace_back(
                GroupNode::make_field("my_map", false, LogicalType::TYPE_MAP,
                                      {PrimitiveNode::make_field("key", false, Type::type::BYTE_ARRAY),
                                       PrimitiveNode::make_field("value", true, Type::type::BYTE_ARRAY)}));
    }
    // Single column map (i.e. set) gets converted to list of struct.
    {
        t_schemas.emplace_back(
                GroupNode::make("my_set", FieldRepetitionType::type::REQUIRED, ConvertedType::type::MAP, 1));
        t_schemas.emplace_back(GroupNode::make("key_value", FieldRepetitionType::type::REPEATED, 1));
        t_schemas.emplace_back(PrimitiveNode::make("key", FieldRepetitionType::type::REQUIRED, Type::type::BYTE_ARRAY,
                                                   ConvertedType::type::UTF8));

        expected_fields.emplace_back(
                GroupNode::make_field("my_set", false, LogicalType::TYPE_ARRAY,
                                      {PrimitiveNode::make_field("key", false, Type::type::BYTE_ARRAY)}));
    }
    // Two column map with non-standard field names.
    {
        t_schemas.emplace_back(
                GroupNode::make("items", FieldRepetitionType::type::REQUIRED, ConvertedType::type::MAP, 1));
        t_schemas.emplace_back(GroupNode::make("item2", FieldRepetitionType::type::REPEATED, 2));
        t_schemas.emplace_back(PrimitiveNode::make("int_key", FieldRepetitionType::type::REQUIRED, Type::type::INT32,
                                                   ConvertedType::type::INT_32));
        t_schemas.emplace_back(PrimitiveNode::make("str_value", FieldRepetitionType::type::OPTIONAL,
                                                   Type::type::BYTE_ARRAY, ConvertedType::type::UTF8));

        expected_fields.emplace_back(
                GroupNode::make_field("items", false, LogicalType::TYPE_MAP,
                                      {PrimitiveNode::make_field("int_key", false, Type::type::INT32),
                                       PrimitiveNode::make_field("str_value", true, Type::type::BYTE_ARRAY)}));
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_TRUE(st.ok()) << st.message();
    check_flat_parquet_field(expected_fields, desc.get_parquet_fields());
}

TEST_F(ParquetSchemaTest, ParquetLists) {
    std::vector<SchemaElement> t_schemas;
    t_schemas.emplace_back(GroupNode::make_root(9));
    std::vector<ParquetField> expected_fields;

    // LIST encoding example taken from parquet-format/LogicalTypes.md

    // // List<String> (list non-null, elements nullable)
    // required group my_list (LIST) {
    //   repeated group list {
    //     optional binary element (UTF8);
    //   }
    // }
    {
        t_schemas.emplace_back(
                GroupNode::make("my_list_1", FieldRepetitionType::REQUIRED, ConvertedType::type::LIST, 1));
        t_schemas.emplace_back(GroupNode::make("list", FieldRepetitionType::REPEATED, 1));
        t_schemas.emplace_back(PrimitiveNode::make("string", FieldRepetitionType::type::OPTIONAL,
                                                   Type::type::BYTE_ARRAY, ConvertedType::type::UTF8));

        expected_fields.emplace_back(
                GroupNode::make_field("my_list_1", false, LogicalType::TYPE_ARRAY,
                                      {PrimitiveNode::make_field("string", true, Type::type::BYTE_ARRAY)}));
    }

    // // List<String> (list nullable, elements non-null)
    // optional group my_list (LIST) {
    //   repeated group list {
    //     required binary element (UTF8);
    //   }
    // }
    {
        t_schemas.emplace_back(
                GroupNode::make("my_list_2", FieldRepetitionType::OPTIONAL, ConvertedType::type::LIST, 1));
        t_schemas.emplace_back(GroupNode::make("list", FieldRepetitionType::REPEATED, 1));
        t_schemas.emplace_back(PrimitiveNode::make("string", FieldRepetitionType::type::REQUIRED,
                                                   Type::type::BYTE_ARRAY, ConvertedType::type::UTF8));

        expected_fields.emplace_back(
                GroupNode::make_field("my_list_2", true, LogicalType::TYPE_ARRAY,
                                      {PrimitiveNode::make_field("string", false, Type::type::BYTE_ARRAY)}));
    }

    // Element types can be nested structures. For example, a list of lists:
    //
    // // List<List<Integer>>
    // optional group array_of_arrays (LIST) {
    //   repeated group list {
    //     required group element (LIST) {
    //       repeated group list {
    //         required int32 element;
    //       }
    //     }
    //   }
    // }
    {
        t_schemas.emplace_back(
                GroupNode::make("array_of_arrays", FieldRepetitionType::OPTIONAL, ConvertedType::type::LIST, 1));
        t_schemas.emplace_back(GroupNode::make("list", FieldRepetitionType::REPEATED, 1));
        t_schemas.emplace_back(GroupNode::make("element", FieldRepetitionType::REQUIRED, ConvertedType::type::LIST, 1));
        t_schemas.emplace_back(GroupNode::make("list", FieldRepetitionType::REPEATED, 1));
        t_schemas.emplace_back(PrimitiveNode::make("int32", FieldRepetitionType::type::REQUIRED, Type::type::INT32,
                                                   ConvertedType::type::INT_32));

        expected_fields.emplace_back(GroupNode::make_field(
                "array_of_arrays", true, LogicalType::TYPE_ARRAY,
                {GroupNode::make_field("element", false, LogicalType::TYPE_ARRAY,
                                       {PrimitiveNode::make_field("int32", false, Type::type::INT32)})}));
    }

    // // List<String> (list nullable, elements non-null)
    // optional group my_list (LIST) {
    //   repeated group element {
    //     required binary str (UTF8);
    //   };
    // }
    {
        t_schemas.emplace_back(
                GroupNode::make("my_list_3", FieldRepetitionType::OPTIONAL, ConvertedType::type::LIST, 1));
        t_schemas.emplace_back(GroupNode::make("element", FieldRepetitionType::REPEATED, 1));
        t_schemas.emplace_back(PrimitiveNode::make("str", FieldRepetitionType::type::REQUIRED, Type::type::BYTE_ARRAY,
                                                   ConvertedType::type::UTF8));

        expected_fields.emplace_back(
                GroupNode::make_field("my_list_3", true, LogicalType::TYPE_ARRAY,
                                      {PrimitiveNode::make_field("str", false, Type::type::BYTE_ARRAY)}));
    }

    // // List<Integer> (nullable list, non-null elements)
    // optional group my_list (LIST) {
    //   repeated int32 element;
    // }
    {
        t_schemas.emplace_back(
                GroupNode::make("my_list_4", FieldRepetitionType::OPTIONAL, ConvertedType::type::LIST, 1));
        t_schemas.emplace_back(PrimitiveNode::make("element", FieldRepetitionType::type::REPEATED, Type::type::INT32,
                                                   ConvertedType::type::INT_32));

        expected_fields.emplace_back(
                GroupNode::make_field("my_list_4", true, LogicalType::TYPE_ARRAY,
                                      {PrimitiveNode::make_field("element", false, Type::type::INT32)}));
    }

    // // List<Tuple<String, Integer>> (nullable list, non-null elements)
    // optional group my_list (LIST) {
    //   repeated group element {
    //     required binary str (UTF8);
    //     required int32 num;
    //   };
    // }
    {
        t_schemas.emplace_back(
                GroupNode::make("my_list_5", FieldRepetitionType::OPTIONAL, ConvertedType::type::LIST, 1));
        t_schemas.emplace_back(GroupNode::make("element", FieldRepetitionType::type::REPEATED, 2));
        t_schemas.emplace_back(PrimitiveNode::make("str", FieldRepetitionType::type::REQUIRED, Type::type::BYTE_ARRAY,
                                                   ConvertedType::type::UTF8));
        t_schemas.emplace_back(PrimitiveNode::make("num", FieldRepetitionType::type::REQUIRED, Type::type::INT32,
                                                   ConvertedType::type::INT_32));

        expected_fields.emplace_back(GroupNode::make_field(
                "my_list_5", true, LogicalType::TYPE_ARRAY,
                {GroupNode::make_field("element", false, LogicalType::TYPE_STRUCT,
                                       {PrimitiveNode::make_field("str", false, Type::type::BYTE_ARRAY),
                                        PrimitiveNode::make_field("num", false, Type::type::INT32)})}));
    }

    // // List<OneTuple<String>> (nullable list, non-null elements)
    // optional group my_list (LIST) {
    //   repeated group array {
    //     required binary str (UTF8);
    //   };
    // }
    // Special case: group is named array
    {
        t_schemas.emplace_back(
                GroupNode::make("my_list_6", FieldRepetitionType::OPTIONAL, ConvertedType::type::LIST, 1));
        t_schemas.emplace_back(GroupNode::make("array", FieldRepetitionType::type::REPEATED, 1));
        t_schemas.emplace_back(PrimitiveNode::make("str", FieldRepetitionType::type::REQUIRED, Type::type::BYTE_ARRAY,
                                                   ConvertedType::type::UTF8));

        expected_fields.emplace_back(GroupNode::make_field(
                "my_list_6", true, LogicalType::TYPE_ARRAY,
                {GroupNode::make_field("array", false, LogicalType::TYPE_STRUCT,
                                       {PrimitiveNode::make_field("str", false, Type::type::BYTE_ARRAY)})}));
    }

    // // List<OneTuple<String>> (nullable list, non-null elements)
    // optional group my_list (LIST) {
    //   repeated group my_list_tuple {
    //     required binary str (UTF8);
    //   };
    // }
    // Special case: group named ends in _tuple
    {
        t_schemas.emplace_back(
                GroupNode::make("my_list_7", FieldRepetitionType::OPTIONAL, ConvertedType::type::LIST, 1));
        t_schemas.emplace_back(GroupNode::make("my_list_tuple", FieldRepetitionType::type::REPEATED, 1));
        t_schemas.emplace_back(PrimitiveNode::make("str", FieldRepetitionType::type::REQUIRED, Type::type::BYTE_ARRAY,
                                                   ConvertedType::type::UTF8));

        expected_fields.emplace_back(GroupNode::make_field(
                "my_list_7", true, LogicalType::TYPE_ARRAY,
                {GroupNode::make_field("my_list_tuple", false, LogicalType::TYPE_STRUCT,
                                       {PrimitiveNode::make_field("str", false, Type::type::BYTE_ARRAY)})}));
    }

    // One-level encoding: Only allows required lists with required cells
    //   repeated value_type name
    {
        t_schemas.emplace_back(PrimitiveNode::make("name", FieldRepetitionType::REPEATED, Type::type::INT32));
        expected_fields.emplace_back(GroupNode::make_field(
                "name", false, LogicalType::TYPE_ARRAY, {PrimitiveNode::make_field("name", false, Type::type::INT32)}));
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_TRUE(st.ok()) << st.message();
    check_flat_parquet_field(expected_fields, desc.get_parquet_fields());
}

TEST_F(ParquetSchemaTest, ParquetNestedSchema) {
    std::vector<SchemaElement> t_schemas;
    t_schemas.emplace_back(GroupNode::make_root(2));
    std::vector<ParquetField> expected_fields;

    // required group group1 {
    //   required bool leaf1;
    //   required int32 leaf2;
    // }
    // required int64 leaf3;
    {
        t_schemas.emplace_back(GroupNode::make("group1", FieldRepetitionType::REQUIRED, 2));
        t_schemas.emplace_back(PrimitiveNode::make("leaf1", FieldRepetitionType::type::REQUIRED, Type::type::BOOLEAN));
        t_schemas.emplace_back(PrimitiveNode::make("leaf2", FieldRepetitionType::type::REQUIRED, Type::type::INT32));

        t_schemas.emplace_back(PrimitiveNode::make("leaf3", FieldRepetitionType::type::REQUIRED, Type::type::INT64));

        expected_fields.emplace_back(
                GroupNode::make_field("group1", false, LogicalType::TYPE_STRUCT,
                                      {PrimitiveNode::make_field("leaf1", false, Type::type::BOOLEAN),
                                       PrimitiveNode::make_field("leaf2", false, Type::type::INT32)}));
        expected_fields.emplace_back(PrimitiveNode::make_field("leaf3", false, Type::type::INT64));
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_TRUE(st.ok()) << st.message();
    check_flat_parquet_field(expected_fields, desc.get_parquet_fields());
}

TEST_F(ParquetSchemaTest, ParquetNestedSchema2) {
    std::vector<SchemaElement> t_schemas;
    t_schemas.emplace_back(GroupNode::make_root(3));
    std::vector<ParquetField> expected_fields;

    // Full Parquet Schema:
    // required group group1 {
    //   required int64 leaf1;
    //   required int64 leaf2;
    // }
    // required group group2 {
    //   required int64 leaf3;
    //   required int64 leaf4;
    // }
    // required int64 leaf5;
    {
        t_schemas.emplace_back(GroupNode::make("group1", FieldRepetitionType::REQUIRED, 2));
        t_schemas.emplace_back(PrimitiveNode::make("leaf1", FieldRepetitionType::type::REQUIRED, Type::type::INT64));
        t_schemas.emplace_back(PrimitiveNode::make("leaf2", FieldRepetitionType::type::REQUIRED, Type::type::INT64));

        t_schemas.emplace_back(GroupNode::make("group2", FieldRepetitionType::REQUIRED, 2));
        t_schemas.emplace_back(PrimitiveNode::make("leaf3", FieldRepetitionType::type::REQUIRED, Type::type::INT64));
        t_schemas.emplace_back(PrimitiveNode::make("leaf4", FieldRepetitionType::type::REQUIRED, Type::type::INT64));

        t_schemas.emplace_back(PrimitiveNode::make("leaf5", FieldRepetitionType::type::REQUIRED, Type::type::INT64));

        expected_fields.emplace_back(
                GroupNode::make_field("group1", false, LogicalType::TYPE_STRUCT,
                                      {PrimitiveNode::make_field("leaf1", false, Type::type::INT64),
                                       PrimitiveNode::make_field("leaf2", false, Type::type::INT64)}));
        expected_fields.emplace_back(
                GroupNode::make_field("group2", false, LogicalType::TYPE_STRUCT,
                                      {PrimitiveNode::make_field("leaf3", false, Type::type::INT64),
                                       PrimitiveNode::make_field("leaf4", false, Type::type::INT64)}));
        expected_fields.emplace_back(PrimitiveNode::make_field("leaf5", false, Type::type::INT64));
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_TRUE(st.ok()) << st.message();
    check_flat_parquet_field(expected_fields, desc.get_parquet_fields());
}

TEST_F(ParquetSchemaTest, ParquetRepeatedNestedSchema) {
    std::vector<SchemaElement> t_schemas;
    t_schemas.emplace_back(GroupNode::make_root(2));
    std::vector<ParquetField> expected_fields;

    //   optional int32 leaf1;
    //   repeated group outerGroup {
    //     optional int32 leaf2;
    //     repeated group innerGroup {
    //       optional int32 leaf3;
    //     }
    //   }
    {
        t_schemas.emplace_back(PrimitiveNode::make("leaf1", FieldRepetitionType::OPTIONAL, Type::type::INT32));
        t_schemas.emplace_back(GroupNode::make("outerGroup", FieldRepetitionType::REPEATED, 2));
        t_schemas.emplace_back(PrimitiveNode::make("leaf2", FieldRepetitionType::type::OPTIONAL, Type::type::INT32));
        t_schemas.emplace_back(GroupNode::make("innerGroup", FieldRepetitionType::REPEATED, 1));
        t_schemas.emplace_back(PrimitiveNode::make("leaf3", FieldRepetitionType::type::OPTIONAL, Type::type::INT32));

        expected_fields.emplace_back(PrimitiveNode::make_field("leaf1", true, Type::type::INT32));

        auto leaf2_field = PrimitiveNode::make_field("leaf2", true, Type::type::INT32);
        auto leaf3_field = PrimitiveNode::make_field("leaf3", true, Type::type::INT32);
        auto inner_group_struct = GroupNode::make_field("innerGroup", false, LogicalType::TYPE_STRUCT, {leaf3_field});
        auto inner_group = GroupNode::make_field("innerGroup", false, LogicalType::TYPE_ARRAY, {inner_group_struct});
        auto outer_group_struct =
                GroupNode::make_field("outerGroup", false, LogicalType::TYPE_STRUCT, {leaf2_field, inner_group});
        auto outer_group = GroupNode::make_field("outerGroup", false, LogicalType::TYPE_ARRAY, {outer_group_struct});
        expected_fields.emplace_back(outer_group);
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_TRUE(st.ok()) << st.message();
    check_flat_parquet_field(expected_fields, desc.get_parquet_fields());
}

TEST_F(ParquetLevelsTest, TestPrimitive) {
    {
        std::vector<SchemaElement> t_schemas;
        t_schemas.emplace_back(GroupNode::make_root(1));
        t_schemas.emplace_back(PrimitiveNode::make("node_name", FieldRepetitionType::REQUIRED, Type::type::BOOLEAN));

        std::vector<LevelInfo> expected;
        expected.emplace_back(make(0, 0, 0));

        do_check(t_schemas, expected);
    }
    {
        std::vector<SchemaElement> t_schemas;
        t_schemas.emplace_back(GroupNode::make_root(1));
        t_schemas.emplace_back(PrimitiveNode::make("node_name", FieldRepetitionType::OPTIONAL, Type::type::BOOLEAN));

        std::vector<LevelInfo> expected;
        expected.emplace_back(make(1, 0, 0));
        do_check(t_schemas, expected);
    }
    {
        std::vector<SchemaElement> t_schemas;
        t_schemas.emplace_back(GroupNode::make_root(1));
        t_schemas.emplace_back(PrimitiveNode::make("node_name", FieldRepetitionType::REPEATED, Type::type::BOOLEAN));

        std::vector<LevelInfo> expected;
        expected.emplace_back(make(1, 1, 0));
        expected.emplace_back(make(1, 1, 1));
        do_check(t_schemas, expected);
    }
}

TEST_F(ParquetLevelsTest, TestMaps) {
    // two column map
    {
        std::vector<SchemaElement> t_schemas;
        t_schemas.emplace_back(GroupNode::make_root(1));

        t_schemas.emplace_back(GroupNode::make("my_map", FieldRepetitionType::OPTIONAL, ConvertedType::type::MAP, 1));
        t_schemas.emplace_back(GroupNode::make("key_value", FieldRepetitionType::REPEATED, 2));
        t_schemas.emplace_back(PrimitiveNode::make("key", FieldRepetitionType::type::REQUIRED, Type::type::BYTE_ARRAY,
                                                   ConvertedType::type::UTF8));
        t_schemas.emplace_back(PrimitiveNode::make("value", FieldRepetitionType::type::OPTIONAL, Type::type::BYTE_ARRAY,
                                                   ConvertedType::type::UTF8));

        std::vector<LevelInfo> expected;
        expected.emplace_back(make(2, 1, 0));
        expected.emplace_back(make(2, 1, 2));
        expected.emplace_back(make(3, 1, 2));
        do_check(t_schemas, expected);
    }
    // Single column map (i.e. set) gets converted to list of struct.
    {
        std::vector<SchemaElement> t_schemas;
        t_schemas.emplace_back(GroupNode::make_root(1));

        t_schemas.emplace_back(
                GroupNode::make("my_set", FieldRepetitionType::type::REQUIRED, ConvertedType::type::MAP, 1));
        t_schemas.emplace_back(GroupNode::make("key_value", FieldRepetitionType::type::REPEATED, 1));
        t_schemas.emplace_back(PrimitiveNode::make("key", FieldRepetitionType::type::REQUIRED, Type::type::BYTE_ARRAY,
                                                   ConvertedType::type::UTF8));

        std::vector<LevelInfo> expected;
        expected.emplace_back(make(1, 1, 0));
        expected.emplace_back(make(1, 1, 1));
        do_check(t_schemas, expected);
    }
}

TEST_F(ParquetLevelsTest, TestSimpleGroups) {
    // schema: struct(child: struct(inner: boolean not null))
    {
        std::vector<SchemaElement> t_schemas;
        t_schemas.emplace_back(GroupNode::make_root(1));

        t_schemas.emplace_back(GroupNode::make("parent", FieldRepetitionType::OPTIONAL, 1));
        t_schemas.emplace_back(GroupNode::make("child", FieldRepetitionType::OPTIONAL, 1));
        t_schemas.emplace_back(PrimitiveNode::make("inner", FieldRepetitionType::REQUIRED, Type::type::BOOLEAN));

        std::vector<LevelInfo> expected;
        expected.emplace_back(make(1, 0, 0));
        expected.emplace_back(make(2, 0, 0));
        expected.emplace_back(make(2, 0, 0));
        do_check(t_schemas, expected);
    }
    // schema: struct(child: struct(inner: boolean ))
    {
        std::vector<SchemaElement> t_schemas;
        t_schemas.emplace_back(GroupNode::make_root(1));

        t_schemas.emplace_back(GroupNode::make("parent", FieldRepetitionType::OPTIONAL, 1));
        t_schemas.emplace_back(GroupNode::make("child", FieldRepetitionType::OPTIONAL, 1));
        t_schemas.emplace_back(PrimitiveNode::make("inner", FieldRepetitionType::OPTIONAL, Type::type::BOOLEAN));

        std::vector<LevelInfo> expected;
        expected.emplace_back(make(1, 0, 0));
        expected.emplace_back(make(2, 0, 0));
        expected.emplace_back(make(3, 0, 0));
        do_check(t_schemas, expected);
    }
    // schema: struct(child: struct(inner: boolean)) not null
    {
        std::vector<SchemaElement> t_schemas;
        t_schemas.emplace_back(GroupNode::make_root(1));

        t_schemas.emplace_back(GroupNode::make("parent", FieldRepetitionType::REQUIRED, 1));
        t_schemas.emplace_back(GroupNode::make("child", FieldRepetitionType::OPTIONAL, 1));
        t_schemas.emplace_back(PrimitiveNode::make("inner", FieldRepetitionType::OPTIONAL, Type::type::BOOLEAN));

        std::vector<LevelInfo> expected;
        expected.emplace_back(make(0, 0, 0));
        expected.emplace_back(make(1, 0, 0));
        expected.emplace_back(make(2, 0, 0));
        do_check(t_schemas, expected);
    }
}

TEST_F(ParquetLevelsTest, TestRepeatedGroups) {
    // schema: list(bool)
    {
        std::vector<SchemaElement> t_schemas;
        t_schemas.emplace_back(GroupNode::make_root(1));

        t_schemas.emplace_back(
                GroupNode::make("child_list", FieldRepetitionType::OPTIONAL, ConvertedType::type::LIST, 1));
        t_schemas.emplace_back(GroupNode::make("list", FieldRepetitionType::REPEATED, 1));
        t_schemas.emplace_back(PrimitiveNode::make("element", FieldRepetitionType::OPTIONAL, Type::type::BOOLEAN));

        std::vector<LevelInfo> expected;
        expected.emplace_back(make(2, 1, 0));
        expected.emplace_back(make(3, 1, 2));
        do_check(t_schemas, expected);
    }
    // schema: list(bool) not null
    {
        std::vector<SchemaElement> t_schemas;
        t_schemas.emplace_back(GroupNode::make_root(1));

        t_schemas.emplace_back(
                GroupNode::make("child_list", FieldRepetitionType::REQUIRED, ConvertedType::type::LIST, 1));
        t_schemas.emplace_back(GroupNode::make("list", FieldRepetitionType::REPEATED, 1));
        t_schemas.emplace_back(PrimitiveNode::make("element", FieldRepetitionType::OPTIONAL, Type::type::BOOLEAN));

        std::vector<LevelInfo> expected;
        expected.emplace_back(make(1, 1, 0));
        expected.emplace_back(make(2, 1, 1));
        do_check(t_schemas, expected);
    }
    // schema: list(bool not null)
    {
        std::vector<SchemaElement> t_schemas;
        t_schemas.emplace_back(GroupNode::make_root(1));

        t_schemas.emplace_back(
                GroupNode::make("child_list", FieldRepetitionType::OPTIONAL, ConvertedType::type::LIST, 1));
        t_schemas.emplace_back(GroupNode::make("list", FieldRepetitionType::REPEATED, 1));
        t_schemas.emplace_back(PrimitiveNode::make("element", FieldRepetitionType::REQUIRED, Type::type::BOOLEAN));

        std::vector<LevelInfo> expected;
        expected.emplace_back(make(2, 1, 0));
        expected.emplace_back(make(2, 1, 2));
        do_check(t_schemas, expected);
    }
    // schema: list(bool not null) not null
    {
        std::vector<SchemaElement> t_schemas;
        t_schemas.emplace_back(GroupNode::make_root(1));

        t_schemas.emplace_back(
                GroupNode::make("child_list", FieldRepetitionType::REQUIRED, ConvertedType::type::LIST, 1));
        t_schemas.emplace_back(GroupNode::make("list", FieldRepetitionType::REPEATED, 1));
        t_schemas.emplace_back(PrimitiveNode::make("element", FieldRepetitionType::REQUIRED, Type::type::BOOLEAN));

        std::vector<LevelInfo> expected;
        expected.emplace_back(make(1, 1, 0));
        expected.emplace_back(make(1, 1, 1));
        do_check(t_schemas, expected);
    }
    // schema: list(struct(child: struct(list(bool not null) not null)) non null) not null
    {
        std::vector<SchemaElement> t_schemas;
        t_schemas.emplace_back(GroupNode::make_root(1));

        t_schemas.emplace_back(GroupNode::make("parent", FieldRepetitionType::REPEATED, 1));
        t_schemas.emplace_back(GroupNode::make("child", FieldRepetitionType::OPTIONAL, 1));
        t_schemas.emplace_back(PrimitiveNode::make("inner", FieldRepetitionType::REPEATED, Type::type::BOOLEAN));

        std::vector<LevelInfo> expected;
        expected.emplace_back(make(1, 1, 0));
        expected.emplace_back(make(1, 1, 1));

        expected.emplace_back(make(2, 1, 1)); // optional child struct

        expected.emplace_back(make(3, 2, 1)); // repeated field
        expected.emplace_back(make(3, 2, 3)); // inner field

        do_check(t_schemas, expected);
    }
    // schema: list(struct(child_list: list(struct(f0: bool f1: bool))) not null) not null
    {
        std::vector<SchemaElement> t_schemas;
        t_schemas.emplace_back(GroupNode::make_root(1));

        t_schemas.emplace_back(GroupNode::make("parent", FieldRepetitionType::REPEATED, 1));
        t_schemas.emplace_back(
                GroupNode::make("child_list", FieldRepetitionType::OPTIONAL, ConvertedType::type::LIST, 1));
        t_schemas.emplace_back(GroupNode::make("list", FieldRepetitionType::REPEATED, 1));
        t_schemas.emplace_back(GroupNode::make("element", FieldRepetitionType::OPTIONAL, 2));
        t_schemas.emplace_back(PrimitiveNode::make("f0", FieldRepetitionType::OPTIONAL, Type::type::BOOLEAN));
        t_schemas.emplace_back(PrimitiveNode::make("f1", FieldRepetitionType::REQUIRED, Type::type::BOOLEAN));

        std::vector<LevelInfo> expected;
        expected.emplace_back(make(1, 1, 0));
        expected.emplace_back(make(1, 1, 1));

        // Def_level=2 is handled together with def_level=3
        // When decoding. Def_level=2 indicates present but empty
        // list. def_level=3 indicates a present element in the
        // list.
        expected.emplace_back(make(3, 2, 1)); // list field
        expected.emplace_back(make(4, 2, 3)); // inner struct field

        expected.emplace_back(make(5, 2, 3)); // f0 bool field
        expected.emplace_back(make(4, 2, 3)); // f1 bool field

        do_check(t_schemas, expected);
    }
    // schema: list(struct(child_list: list(bool not null)) not null) not null
    // Legacy 2-level encoding (required for backwards compatibility.  See
    // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#nested-types
    // for definitions).
    {
        std::vector<SchemaElement> t_schemas;
        t_schemas.emplace_back(GroupNode::make_root(1));

        t_schemas.emplace_back(GroupNode::make("parent", FieldRepetitionType::REPEATED, 1));
        t_schemas.emplace_back(
                GroupNode::make("child_list", FieldRepetitionType::OPTIONAL, ConvertedType::type::LIST, 1));
        t_schemas.emplace_back(PrimitiveNode::make("bool", FieldRepetitionType::REPEATED, Type::type::BOOLEAN));

        std::vector<LevelInfo> expected;
        expected.emplace_back(make(1, 1, 0)); // parent list
        expected.emplace_back(make(1, 1, 1)); // parent struct

        // Def_level=2 is handled together with def_level=3
        // When decoding. Def_level=2 indicates present but empty
        // list. def_level=3 indicates a present element in the
        // list.
        expected.emplace_back(make(3, 2, 1)); // list field
        expected.emplace_back(make(3, 2, 3)); // inner bool
    }
}

TEST_F(ParquetLevelsTest, ListErrors) {
    {
        std::vector<SchemaElement> t_schemas;
        t_schemas.emplace_back(GroupNode::make_root(1));

        t_schemas.emplace_back(
                GroupNode::make("child_list", FieldRepetitionType::REPEATED, ConvertedType::type::LIST, 1));
        t_schemas.emplace_back(PrimitiveNode::make("bool", FieldRepetitionType::type::REPEATED, Type::type::BOOLEAN));

        SchemaDescriptor desc;
        auto st = desc.from_thrift(t_schemas, true);
        ASSERT_FALSE(st.ok()) << st.message();
        ASSERT_EQ("LIST-annotated groups must not be repeated.", st.message());
    }
    {
        std::vector<SchemaElement> t_schemas;
        t_schemas.emplace_back(GroupNode::make_root(1));

        t_schemas.emplace_back(
                GroupNode::make("child_list", FieldRepetitionType::OPTIONAL, ConvertedType::type::LIST, 2));
        t_schemas.emplace_back(PrimitiveNode::make("f1", FieldRepetitionType::type::REPEATED, Type::type::BOOLEAN));
        t_schemas.emplace_back(PrimitiveNode::make("f2", FieldRepetitionType::type::REPEATED, Type::type::BOOLEAN));

        SchemaDescriptor desc;
        auto st = desc.from_thrift(t_schemas, true);
        ASSERT_FALSE(st.ok()) << st.message();
        ASSERT_EQ("LIST-annotated groups must have a single child.", st.message());
    }
    {
        std::vector<SchemaElement> t_schemas;
        t_schemas.emplace_back(GroupNode::make_root(1));

        t_schemas.emplace_back(
                GroupNode::make("child_list", FieldRepetitionType::OPTIONAL, ConvertedType::type::LIST, 1));
        t_schemas.emplace_back(PrimitiveNode::make("f1", FieldRepetitionType::type::OPTIONAL, Type::type::BOOLEAN));

        SchemaDescriptor desc;
        auto st = desc.from_thrift(t_schemas, true);
        ASSERT_FALSE(st.ok()) << st.message();
        ASSERT_EQ("Non-repeated nodes in a LIST-annotated group are not supported.", st.message());
    }
}

} // namespace starrocks::parquet
