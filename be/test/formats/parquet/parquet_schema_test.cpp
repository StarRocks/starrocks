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
#include <vector>

#include "formats/parquet/schema.h"

namespace starrocks::parquet {

class ParquetSchemaTest : public testing::Test {
public:
    ParquetSchemaTest() = default;
    ~ParquetSchemaTest() override = default;
};

TEST_F(ParquetSchemaTest, EmptySchema) {
    std::vector<tparquet::SchemaElement> t_schemas;
    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, OnlyRoot) {
    std::vector<tparquet::SchemaElement> t_schemas;

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
    std::vector<tparquet::SchemaElement> t_schemas;

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
        t_schema.__set_type(tparquet::Type::INT32);
        t_schema.name = "col1";
        t_schema.__set_num_children(0);
        t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    }
    // BYTE_ARRAY
    {
        auto& t_schema = t_schemas[2];
        t_schema.__set_type(tparquet::Type::BYTE_ARRAY);
        t_schema.name = "col2";
        t_schema.__set_num_children(0);
        t_schema.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_TRUE(st.ok());

    {
        auto idx = desc.get_field_pos_by_column_name("col1");
        ASSERT_EQ(0, idx);
        auto field = desc.get_stored_column_by_idx(0);
        ASSERT_STREQ("col1", field->name.c_str());
        ASSERT_EQ(0, field->physical_column_index);
        ASSERT_EQ(1, field->max_def_level());
        ASSERT_EQ(0, field->max_rep_level());
        ASSERT_EQ(true, field->is_nullable);
    }

    {
        auto idx = desc.get_field_pos_by_column_name("col2");
        ASSERT_EQ(1, idx);
        auto field = desc.get_stored_column_by_idx(1);
        ASSERT_STREQ("col2", field->name.c_str());
        ASSERT_EQ(1, field->physical_column_index);
        ASSERT_EQ(0, field->max_def_level());
        ASSERT_EQ(0, field->max_rep_level());
        ASSERT_EQ(false, field->is_nullable);
    }
    LOG(INFO) << desc.debug_string();
}

TEST_F(ParquetSchemaTest, NestedType) {
    std::vector<tparquet::SchemaElement> t_schemas;

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
        t_schema.__set_type(tparquet::Type::INT32);
        t_schema.name = "col1";
        t_schema.__set_num_children(0);
        t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    }

    // col2: LIST(BYTE_ARRAY)
    {
        int idx_base = 2;

        // level-1
        {
            auto& t_schema = t_schemas[idx_base + 0];
            t_schema.name = "col2";
            t_schema.__set_num_children(1);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(tparquet::ConvertedType::LIST);
        }

        // level-2
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "list";
            t_schema.__set_num_children(1);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
        }

        // level-3
        {
            auto& t_schema = t_schemas[idx_base + 2];
            t_schema.__set_type(tparquet::Type::BYTE_ARRAY);
            t_schema.name = "element";
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        }
    }

    // col3: STRUCT{ a: INT32, b: BYTE_ARRAY}
    {
        int idx_base = 5;

        {
            auto& t_schema = t_schemas[idx_base + 0];
            t_schema.name = "col3";
            t_schema.__set_num_children(2);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        }

        // field-a
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.name = "a";
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        }

        // field-b
        {
            auto& t_schema = t_schemas[idx_base + 2];
            t_schema.__set_type(tparquet::Type::BYTE_ARRAY);
            t_schema.name = "b";
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
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
    std::vector<tparquet::SchemaElement> t_schemas;

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
        t_schema.__set_type(tparquet::Type::INT32);
        t_schema.name = "col1";
        t_schema.__set_num_children(0);
        t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    }
    // BYTE_ARRAY
    {
        auto& t_schema = t_schemas[2];
        t_schema.__set_type(tparquet::Type::BYTE_ARRAY);
        t_schema.name = "col2";
        t_schema.__set_num_children(0);
        t_schema.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, InvalidCase2) {
    std::vector<tparquet::SchemaElement> t_schemas;

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
        t_schema.__set_type(tparquet::Type::INT32);
        t_schema.name = "col1";
        t_schema.__set_num_children(0);
        t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    }
    // BYTE_ARRAY
    {
        auto& t_schema = t_schemas[2];
        t_schema.__set_type(tparquet::Type::BYTE_ARRAY);
        t_schema.name = "col2";
        t_schema.__set_num_children(0);
        t_schema.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, InvalidList1) {
    std::vector<tparquet::SchemaElement> t_schemas;

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
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
            t_schema.__set_converted_type(tparquet::ConvertedType::LIST);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, InvalidList2) {
    std::vector<tparquet::SchemaElement> t_schemas;

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
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(tparquet::ConvertedType::LIST);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, InvalidList3) {
    std::vector<tparquet::SchemaElement> t_schemas;

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
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(tparquet::ConvertedType::LIST);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, InvalidList4) {
    std::vector<tparquet::SchemaElement> t_schemas;

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
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(tparquet::ConvertedType::LIST);
        }

        // level-2
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "list";
            t_schema.__set_num_children(1);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, SimpleArray) {
    std::vector<tparquet::SchemaElement> t_schemas;

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
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
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
    std::vector<tparquet::SchemaElement> t_schemas;

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
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(tparquet::ConvertedType::LIST);
        }

        // level-2
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "field";
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
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
    std::vector<tparquet::SchemaElement> t_schemas;

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
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(tparquet::ConvertedType::MAP);
        }

        // level-2
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "key_value";
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(2);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
        }
        // key
        {
            auto& t_schema = t_schemas[idx_base + 2];
            t_schema.name = "key";
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
        }
        // value
        {
            auto& t_schema = t_schemas[idx_base + 3];
            t_schema.name = "value";
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
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
    std::vector<tparquet::SchemaElement> t_schemas;

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
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(tparquet::ConvertedType::MAP);
        }

        // level-2
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "key_value";
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(2);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, InvalidMap2) {
    std::vector<tparquet::SchemaElement> t_schemas;

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
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(tparquet::ConvertedType::MAP);
        }

        // key
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "key";
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
        }
        // value
        {
            auto& t_schema = t_schemas[idx_base + 2];
            t_schema.name = "value";
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, InvalidMap3) {
    std::vector<tparquet::SchemaElement> t_schemas;

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
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
            t_schema.__set_converted_type(tparquet::ConvertedType::MAP);
        }

        // key
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "key";
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
        }
        // value
        {
            auto& t_schema = t_schemas[idx_base + 2];
            t_schema.name = "value";
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, InvalidMap4) {
    std::vector<tparquet::SchemaElement> t_schemas;

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
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(tparquet::ConvertedType::MAP);
        }

        // level-2
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "key_value";
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(2);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        }
        // key
        {
            auto& t_schema = t_schemas[idx_base + 2];
            t_schema.name = "key";
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
        }
        // value
        {
            auto& t_schema = t_schemas[idx_base + 3];
            t_schema.name = "value";
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, InvalidMap5) {
    std::vector<tparquet::SchemaElement> t_schemas;

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
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(tparquet::ConvertedType::MAP);
        }

        // level-2
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "key_value";
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(2);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
        }
        // key
        {
            auto& t_schema = t_schemas[idx_base + 2];
            t_schema.name = "key";
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        }
        // value
        {
            auto& t_schema = t_schemas[idx_base + 3];
            t_schema.name = "value";
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetSchemaTest, InvalidMap6) {
    std::vector<tparquet::SchemaElement> t_schemas;

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
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
            t_schema.__set_converted_type(tparquet::ConvertedType::MAP);
        }

        // level-2
        {
            auto& t_schema = t_schemas[idx_base + 1];
            t_schema.name = "key_value";
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(3);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
        }
        // key
        {
            auto& t_schema = t_schemas[idx_base + 2];
            t_schema.name = "key";
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        }
        // value
        {
            auto& t_schema = t_schemas[idx_base + 3];
            t_schema.name = "value";
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        }
        // value3
        {
            auto& t_schema = t_schemas[idx_base + 4];
            t_schema.name = "value2";
            t_schema.__set_type(tparquet::Type::INT32);
            t_schema.__set_num_children(0);
            t_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        }
    }

    SchemaDescriptor desc;
    auto st = desc.from_thrift(t_schemas, true);
    ASSERT_FALSE(st.ok());
}

} // namespace starrocks::parquet
