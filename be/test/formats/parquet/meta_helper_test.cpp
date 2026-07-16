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

#include "formats/parquet/meta_helper.h"

#include <gtest/gtest.h>

#include "formats/parquet/schema.h"
#include "gen_cpp/Descriptors_types.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks::parquet {

// LakeMetaHelperTest is a friend of LakeMetaHelper so it can drive the private
// _is_valid_type() directly. That function is where a crash was reported when the
// FE ships an incomplete nested lake schema (e.g. Paimon complex columns before
// #66784): the FE only carried the top-level field id/name and no children, while
// the parquet file still described the full nesting, so the ARRAY/MAP branch used
// the file's children count as the loop bound and then indexed the (empty) FE
// children vector out of bounds, producing a null pointer that was dereferenced
// one level down in the STRUCT branch (SIGSEGV @0x30).
class LakeMetaHelperTest : public testing::Test {
public:
    LakeMetaHelperTest() = default;
    ~LakeMetaHelperTest() override = default;

protected:
    // Build the parquet-file view of ARRAY<STRUCT<a VARCHAR, b VARCHAR>>. The file
    // schema is always complete, so every level has its children populated.
    static ParquetField make_array_of_struct_parquet_field() {
        ParquetField a;
        a.name = "a";
        a.type = ColumnType::SCALAR;
        a.field_id = 101;

        ParquetField b;
        b.name = "b";
        b.type = ColumnType::SCALAR;
        b.field_id = 102;

        ParquetField element;
        element.name = "element";
        element.type = ColumnType::STRUCT;
        element.field_id = 100;
        element.children = {a, b};

        ParquetField array;
        array.name = "col";
        array.type = ColumnType::ARRAY;
        array.field_id = 1;
        array.children = {element};
        return array;
    }

    // The matching slot type: ARRAY<STRUCT<a VARCHAR, b VARCHAR>>.
    static TypeDescriptor make_array_of_struct_type() {
        TypeDescriptor varchar = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        TypeDescriptor struct_type = TypeDescriptor::create_struct_type({"a", "b"}, {varchar, varchar});
        return TypeDescriptor::create_array_type(struct_type);
    }

    // Helper to call the private method under test on a minimally-constructed helper.
    static bool is_valid_type(const TIcebergSchema& schema, const ParquetField& field,
                              const TIcebergSchemaField& top_field, const TypeDescriptor& type) {
        LakeMetaHelper helper(/*file_metadata=*/nullptr, /*case_sensitive=*/false, &schema);
        return helper._is_valid_type(&field, &top_field, &type);
    }
};

// Regression: an incomplete lake schema (top-level field only, no nested children)
// must be reported as an invalid/unsupported type instead of crashing.
TEST_F(LakeMetaHelperTest, IncompleteNestedSchemaIsRejectedInsteadOfCrashing) {
    // FE lake schema carrying only the top-level ARRAY column: field id + name, no children.
    TIcebergSchemaField top_field;
    top_field.__set_field_id(1);
    top_field.__set_name("col");
    // children intentionally left empty -- this is the pre-#66784 Paimon shape.
    EXPECT_TRUE(top_field.children.empty());

    TIcebergSchema schema;
    schema.__set_fields({top_field});

    ParquetField parquet_field = make_array_of_struct_parquet_field();
    TypeDescriptor type = make_array_of_struct_type();

    // Before the fix this dereferenced a null pointer and crashed; now it simply
    // reports the nested type as invalid so the caller can skip the column.
    EXPECT_FALSE(is_valid_type(schema, parquet_field, top_field, type));
}

// A complete lake schema (children recursively filled, as FE ships after #66784)
// still validates the nested type as usable.
TEST_F(LakeMetaHelperTest, CompleteNestedSchemaIsValid) {
    TIcebergSchemaField a;
    a.__set_field_id(101);
    a.__set_name("a");

    TIcebergSchemaField b;
    b.__set_field_id(102);
    b.__set_name("b");

    TIcebergSchemaField element;
    element.__set_field_id(100);
    element.__set_name("element");
    element.__set_children({a, b});

    TIcebergSchemaField top_field;
    top_field.__set_field_id(1);
    top_field.__set_name("col");
    top_field.__set_children({element});

    TIcebergSchema schema;
    schema.__set_fields({top_field});

    ParquetField parquet_field = make_array_of_struct_parquet_field();
    TypeDescriptor type = make_array_of_struct_type();

    EXPECT_TRUE(is_valid_type(schema, parquet_field, top_field, type));
}

// A scalar column never enters the complex-type branches and is always valid,
// regardless of the (empty) children vector.
TEST_F(LakeMetaHelperTest, ScalarTypeIsAlwaysValid) {
    TIcebergSchemaField top_field;
    top_field.__set_field_id(1);
    top_field.__set_name("col");

    TIcebergSchema schema;
    schema.__set_fields({top_field});

    ParquetField scalar;
    scalar.name = "col";
    scalar.type = ColumnType::SCALAR;
    scalar.field_id = 1;

    TypeDescriptor type = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);

    EXPECT_TRUE(is_valid_type(schema, scalar, top_field, type));
}

} // namespace starrocks::parquet
