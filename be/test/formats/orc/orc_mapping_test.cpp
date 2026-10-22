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

#include "formats/orc/orc_mapping.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "common/statusor.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptors.h"

namespace starrocks {

static TIcebergSchemaField make_iceberg_field(int32_t id, const std::string& name,
                                              std::vector<TIcebergSchemaField> children = {}) {
    TIcebergSchemaField field;
    field.__set_field_id(id);
    field.__set_name(name);
    if (!children.empty()) {
        field.__set_children(std::move(children));
    }
    return field;
}

// Iceberg resolves ORC columns by the `iceberg.id` attribute, so a renamed column (top-level or struct subfield)
// still reads its data, and a dropped-then-re-added column with the same name does not read the old data.
TEST(OrcMappingTest, IcebergFieldIdMapping) {
    std::unique_ptr<orc::Type> root =
            orc::Type::buildTypeFromString("struct<c1_old:int,dropped:int,s:struct<a_old:int,b:string>>");
    // getSubtype() is const; the writer sets these attributes on its own schema.
    auto set_id = [](const orc::Type* type, const char* id) {
        const_cast<orc::Type*>(type)->setAttribute("iceberg.id", id);
    };
    set_id(root->getSubtype(0), "1");
    set_id(root->getSubtype(1), "2");
    set_id(root->getSubtype(2), "3");
    set_id(root->getSubtype(2)->getSubtype(0), "4");
    set_id(root->getSubtype(2)->getSubtype(1), "5");

    // Current schema: c1_old renamed to c1, `dropped` dropped and re-added (new id 6), a_old renamed to a.
    TIcebergSchema schema;
    schema.__set_fields({make_iceberg_field(1, "c1"), make_iceberg_field(6, "dropped"),
                         make_iceberg_field(3, "s", {make_iceberg_field(4, "a"), make_iceberg_field(5, "b")})});

    auto names = OrcMappingFactory::iceberg_column_names(*root, schema);
    ASSERT_TRUE(names.has_value());
    EXPECT_EQ((std::vector<std::string>{"c1", "", "s"}), *names);

    TypeDescriptor struct_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
    struct_type.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    struct_type.field_names.emplace_back("a");
    SlotDescriptor c1(0, "c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    SlotDescriptor dropped(1, "dropped", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    SlotDescriptor s(2, "s", struct_type);
    std::vector<SlotDescriptor*> slots{&c1, &dropped, &s};

    OrcMappingOptions options{"test.orc", false, true};
    options.iceberg_schema = &schema;
    auto mapping = OrcMappingFactory::build_mapping(slots, *root, false, &*names, options);
    ASSERT_OK(mapping.status());
    ASSERT_TRUE((*mapping)->contains(0));
    EXPECT_EQ(root->getSubtype(0), (*mapping)->get_orc_type_child_mapping(0).orc_type);
    EXPECT_FALSE((*mapping)->contains(1));
    ASSERT_TRUE((*mapping)->contains(2));
    const auto& struct_mapping = (*mapping)->get_orc_type_child_mapping(2);
    EXPECT_EQ(root->getSubtype(2)->getSubtype(0), struct_mapping.orc_mapping->get_orc_type_child_mapping(0).orc_type);

    // Without the schema, the struct subfield is matched by name and the renamed one is not found.
    options.iceberg_schema = nullptr;
    EXPECT_FALSE(OrcMappingFactory::build_mapping(slots, *root, false, &*names, options).ok());

    // A file without field ids keeps the name-based resolution.
    std::unique_ptr<orc::Type> no_ids = orc::Type::buildTypeFromString("struct<c1:int>");
    EXPECT_FALSE(OrcMappingFactory::iceberg_column_names(*no_ids, schema).has_value());
}

// A struct subfield added to an Iceberg table after the file was written is missing from the file: it maps to no
// ORC type and carries its initial-default, instead of failing the scan.
TEST(OrcMappingTest, IcebergMissingStructSubfield) {
    std::unique_ptr<orc::Type> root = orc::Type::buildTypeFromString("struct<s:struct<x:string,a:int>>");
    const_cast<orc::Type*>(root->getSubtype(0))->setAttribute("iceberg.id", "3");
    const_cast<orc::Type*>(root->getSubtype(0)->getSubtype(0))->setAttribute("iceberg.id", "7");
    const_cast<orc::Type*>(root->getSubtype(0)->getSubtype(1))->setAttribute("iceberg.id", "4");
    const orc::Type* orc_a = root->getSubtype(0)->getSubtype(1);

    TIcebergSchemaField b = make_iceberg_field(5, "b");
    b.__set_initial_default("7");
    TIcebergSchema schema;
    schema.__set_fields({make_iceberg_field(3, "s", {make_iceberg_field(7, "x"), make_iceberg_field(4, "a"), b})});
    std::vector<std::string> names{"s"};

    auto struct_of = [](const std::vector<std::string>& fields) {
        TypeDescriptor type = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
        for (const auto& name : fields) {
            type.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
            type.field_names.emplace_back(name);
        }
        return type;
    };
    OrcMappingOptions options{"test.orc", false, true};
    options.iceberg_schema = &schema;

    {
        SlotDescriptor s(0, "s", struct_of({"a", "b"}));
        std::vector<SlotDescriptor*> slots{&s};
        auto mapping = OrcMappingFactory::build_mapping(slots, *root, false, &names, options);
        ASSERT_OK(mapping.status());
        const auto& struct_mapping = (*mapping)->get_orc_type_child_mapping(0).orc_mapping;
        EXPECT_EQ(orc_a, struct_mapping->get_orc_type_child_mapping(0).orc_type);
        EXPECT_EQ(nullptr, struct_mapping->get_orc_type_child_mapping(1).orc_type);
        EXPECT_EQ("7", struct_mapping->get_orc_type_child_mapping(1).initial_default.value_or(""));

        std::list<uint64_t> column_ids;
        ASSERT_OK((*mapping)->set_include_column_id(0, s.type(), &column_ids));
        EXPECT_EQ((std::list<uint64_t>{orc_a->getColumnId()}), column_ids);
    }
    {
        // Only the missing subfield is used: the fixed-width stored subfield `a`, not the string `x` nor the whole
        // struct, is read for the struct's nulls.
        SlotDescriptor s(0, "s", struct_of({"b"}));
        std::vector<SlotDescriptor*> slots{&s};
        auto mapping = OrcMappingFactory::build_mapping(slots, *root, false, &names, options);
        ASSERT_OK(mapping.status());
        std::list<uint64_t> column_ids;
        ASSERT_OK((*mapping)->set_include_column_id(0, s.type(), &column_ids));
        EXPECT_EQ((std::list<uint64_t>{orc_a->getColumnId()}), column_ids);
    }
    {
        // Without the Iceberg schema a missing subfield is still an error.
        options.iceberg_schema = nullptr;
        SlotDescriptor s(0, "s", struct_of({"a", "b"}));
        std::vector<SlotDescriptor*> slots{&s};
        EXPECT_FALSE(OrcMappingFactory::build_mapping(slots, *root, false, &names, options).ok());
    }
}

} // namespace starrocks
