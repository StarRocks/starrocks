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

#include <map>
#include <memory>

#include "base/testutil/assert.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/schema.h"
#include "formats/scan_context.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/parquet_types.h"
#include "runtime/descriptors.h"
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
        TypeDescriptor struct_type =
                TypeDescriptor::create_struct_type({"a", "b"}, {TYPE_VARCHAR_DESC, TYPE_VARCHAR_DESC});
        return TypeDescriptor::create_array_type(struct_type);
    }

    // Build the parquet-file view of MAP<VARCHAR, VARCHAR>: two children (key, value).
    static ParquetField make_map_parquet_field() {
        ParquetField key;
        key.name = "key";
        key.type = ColumnType::SCALAR;
        key.field_id = 101;

        ParquetField value;
        value.name = "value";
        value.type = ColumnType::SCALAR;
        value.field_id = 102;

        ParquetField map;
        map.name = "col";
        map.type = ColumnType::MAP;
        map.field_id = 1;
        map.children = {key, value};
        return map;
    }

    static TypeDescriptor make_map_type() {
        return TypeDescriptor::create_map_type(TYPE_VARCHAR_DESC, TYPE_VARCHAR_DESC);
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

// Regression: a partial MAP lake schema carrying only the key child must be rejected.
// The downstream reader reads lake_schema_field->children[1] (the value) unconditionally,
// so accepting a one-child MAP would still crash even though the key alone validates.
TEST_F(LakeMetaHelperTest, PartialMapSchemaWithOnlyKeyIsRejected) {
    TIcebergSchemaField key;
    key.__set_field_id(101);
    key.__set_name("key");

    // Only the key child is present -- the value child is missing.
    TIcebergSchemaField top_field;
    top_field.__set_field_id(1);
    top_field.__set_name("col");
    top_field.__set_children({key});

    TIcebergSchema schema;
    schema.__set_fields({top_field});

    ParquetField parquet_field = make_map_parquet_field();
    TypeDescriptor type = make_map_type();

    EXPECT_FALSE(is_valid_type(schema, parquet_field, top_field, type));
}

// A complete MAP lake schema (both key and value children) validates.
TEST_F(LakeMetaHelperTest, CompleteMapSchemaIsValid) {
    TIcebergSchemaField key;
    key.__set_field_id(101);
    key.__set_name("key");

    TIcebergSchemaField value;
    value.__set_field_id(102);
    value.__set_name("value");

    TIcebergSchemaField top_field;
    top_field.__set_field_id(1);
    top_field.__set_name("col");
    top_field.__set_children({key, value});

    TIcebergSchema schema;
    schema.__set_fields({top_field});

    ParquetField parquet_field = make_map_parquet_field();
    TypeDescriptor type = make_map_type();

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

    EXPECT_TRUE(is_valid_type(schema, scalar, top_field, TYPE_VARCHAR_DESC));
}

// A struct subfield without a field id (-1 placeholder in field_ids) is matched by its name.
TEST(ParquetMetaHelperTest, StructSubfieldWithoutFieldIdIsMatchedByName) {
    ParquetField a;
    a.name = "a";
    a.type = ColumnType::SCALAR;
    a.field_id = 1;

    ParquetField b;
    b.name = "b";
    b.type = ColumnType::SCALAR;
    b.field_id = 2;

    ParquetField col;
    col.name = "col";
    col.type = ColumnType::STRUCT;
    col.field_id = 10;
    col.children = {a, b};

    TypeDescriptor int_type = TYPE_INT_DESC;
    ParquetMetaHelper helper(/*file_metadata=*/nullptr, /*case_sensitive=*/true);

    // x (id 7) is not in the file, b has no id and is matched by name.
    TypeDescriptor type = TypeDescriptor::create_struct_type({"x", "b"}, {int_type, int_type});
    type.field_ids = {7, -1};
    EXPECT_TRUE(helper._is_valid_type(&col, &type));

    // Neither subfield is in the file.
    type.field_names = {"x", "y"};
    EXPECT_FALSE(helper._is_valid_type(&col, &type));
}

// MAP<STRUCT<a>, VARCHAR> in the file, read as MAP<VARCHAR, (pruned)> (e.g. `SELECT map_keys(m)`): the column is kept,
// so that the reader factory reports the unsupported group key instead of the column being read as NULL.
TEST(ParquetMetaHelperTest, IncompatibleGroupMapKeyIsKeptForTheReaderToReject) {
    ParquetField a;
    a.name = "a";
    a.type = ColumnType::SCALAR;
    ParquetField key;
    key.name = "key";
    key.type = ColumnType::STRUCT;
    key.children = {a};
    ParquetField value;
    value.name = "value";
    value.type = ColumnType::SCALAR;
    ParquetField map;
    map.name = "m";
    map.type = ColumnType::MAP;
    map.children = {key, value};

    ParquetMetaHelper helper(/*file_metadata=*/nullptr, /*case_sensitive=*/false);
    // incompatible key read, value pruned: kept for the reader to reject
    const TypeDescriptor key_only = TypeDescriptor::create_map_type(TYPE_VARCHAR_DESC, TYPE_UNKNOWN_DESC);
    EXPECT_TRUE(helper._is_valid_type(&map, &key_only));
    // key pruned, value read: valid as before
    const TypeDescriptor value_only = TypeDescriptor::create_map_type(TYPE_UNKNOWN_DESC, TYPE_VARCHAR_DESC);
    EXPECT_TRUE(helper._is_valid_type(&map, &value_only));
    // nothing read
    const TypeDescriptor none = TypeDescriptor::create_map_type(TYPE_UNKNOWN_DESC, TYPE_UNKNOWN_DESC);
    EXPECT_FALSE(helper._is_valid_type(&map, &none));
}

// A file without field ids stores c1 and c2 under the names they had before the table renamed them; the name mapping
// lists those names, so the columns are read from them instead of being missing.
TEST(ParquetMetaHelperTest, RenamedColumnWithoutFieldIdsIsReadThroughMappedNames) {
    tparquet::FileMetaData t_meta;
    std::vector<tparquet::SchemaElement> schema;
    tparquet::SchemaElement root;
    root.__set_name("table");
    root.__set_num_children(5);
    schema.push_back(root);
    for (const char* name : {"id", "c1", "c2", "c3", "A"}) {
        tparquet::SchemaElement col;
        col.__set_name(name);
        col.__set_type(tparquet::Type::INT32);
        col.__set_num_children(0);
        schema.push_back(col);
    }
    t_meta.__set_schema(schema);
    FileMetaData file_meta;
    ASSERT_OK(file_meta.init(t_meta, /*case_sensitive=*/false));
    ASSERT_FALSE(file_meta.schema().exist_filed_id());

    auto make_field = [](int32_t id, const std::string& name, std::vector<std::string> mapped_names) {
        TIcebergSchemaField field;
        field.__set_field_id(id);
        field.__set_name(name);
        if (!mapped_names.empty()) {
            field.__set_mapped_names(mapped_names);
        }
        return field;
    };
    // c1 -> c1_new; c2 -> c2_new; c3 -> c3_new and a new c3 added, so the file's c3 belongs to the new column;
    // a -> p, then A added and renamed to q: the file's A is q's only, the mapping tells a and A apart (the scan is
    // case-insensitive, so the file's column name is lowercased in the schema)
    TIcebergSchema lake;
    lake.__set_fields({make_field(1, "id", {}), make_field(2, "c1_new", {"c1"}), make_field(3, "c2_new", {"c2"}),
                       make_field(4, "c3_new", {"c3"}), make_field(5, "c3", {}), make_field(6, "c4", {}),
                       make_field(7, "p", {"a"}), make_field(8, "q", {"A"})});

    std::vector<std::unique_ptr<SlotDescriptor>> slots;
    for (const char* name : {"id", "c1_new", "c2_new", "c3_new", "c3", "c4", "p", "q"}) {
        slots.emplace_back(std::make_unique<SlotDescriptor>(static_cast<SlotId>(slots.size() + 1), name,
                                                            TypeDescriptor::from_logical_type(TYPE_INT)));
    }
    std::vector<FormatColumnInfo> columns;
    for (size_t i = 0; i < slots.size(); i++) {
        columns.push_back({static_cast<int>(i), slots[i].get(), true});
    }

    auto read = [&](const TIcebergSchema* lake_schema) {
        ParquetMetaHelper helper(&file_meta, /*case_sensitive=*/false, lake_schema);
        std::vector<GroupReaderParam::Column> read_cols;
        std::unordered_set<std::string> existed;
        helper.prepare_read_columns(columns, nullptr, read_cols, existed);
        std::map<std::string, int32_t> result;
        for (const auto& col : read_cols) {
            result.emplace(std::string(col.slot_desc->col_name()), col.idx_in_parquet);
            // a column found through the mapping carries its Iceberg field (e.g. for GEOGRAPHY metadata)
            bool by_mapping = col.slot_desc->col_name() == "c1_new" || col.slot_desc->col_name() == "c2_new" ||
                              col.slot_desc->col_name() == "q";
            EXPECT_EQ(by_mapping, col.t_lake_schema_field != nullptr) << col.slot_desc->col_name();
        }
        return result;
    };

    std::map<std::string, int32_t> expected{{"id", 0}, {"c1_new", 1}, {"c2_new", 2}, {"c3", 3}, {"q", 4}};
    EXPECT_EQ(expected, read(&lake));
    // without the lake schema only the current names match
    std::map<std::string, int32_t> by_name{{"id", 0}, {"c3", 3}};
    EXPECT_EQ(by_name, read(nullptr));
}

} // namespace starrocks::parquet
