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

#include <optional>

#include "base/testutil/assert.h"
#include "column/column_access_path.h"
#include "column/field.h"
#include "gtest/gtest.h"
#include "types/struct_type_info.h"

namespace starrocks {

namespace {

TColumnAccessPath make_thrift_path(TAccessPathType::type type, bool from_predicate = false, bool extended = false,
                                   std::optional<TypeDescriptor> value_type = std::nullopt) {
    TColumnAccessPath path;
    path.__set_type(type);
    path.__set_from_predicate(from_predicate);
    path.__set_extended(extended);
    if (value_type.has_value()) {
        path.__set_type_desc(value_type->to_thrift());
    }
    return path;
}

} // namespace

TEST(ColumnAccessPathCoreTest, createFromThriftUsesResolverForWholeTree) {
    auto leaf = make_thrift_path(TAccessPathType::FIELD, true, true, TypeDescriptor(TYPE_VARCHAR));
    auto child = make_thrift_path(TAccessPathType::FIELD);
    child.__set_children({leaf});
    auto root_path = make_thrift_path(TAccessPathType::ROOT);
    root_path.__set_children({child});

    std::vector<std::string> resolved_paths = {"root", "profile", "name"};
    size_t resolve_index = 0;
    auto resolver = [&](const TColumnAccessPath&) -> StatusOr<std::string> {
        DCHECK_LT(resolve_index, resolved_paths.size());
        return resolved_paths[resolve_index++];
    };

    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(root_path, resolver));
    ASSERT_EQ(3, resolve_index);

    EXPECT_TRUE(root->is_root());
    EXPECT_EQ("root", root->path());
    EXPECT_EQ("root", root->absolute_path());
    ASSERT_EQ(1, root->children().size());

    auto* child_path = root->children()[0].get();
    EXPECT_TRUE(child_path->is_field());
    EXPECT_EQ("profile", child_path->path());
    EXPECT_EQ("root.profile", child_path->absolute_path());
    ASSERT_EQ(1, child_path->children().size());

    auto* leaf_path = child_path->children()[0].get();
    EXPECT_TRUE(leaf_path->is_field());
    EXPECT_TRUE(leaf_path->is_from_predicate());
    EXPECT_TRUE(leaf_path->is_extended());
    EXPECT_EQ("name", leaf_path->path());
    EXPECT_EQ("root.profile.name", leaf_path->absolute_path());
    EXPECT_EQ(TYPE_VARCHAR, leaf_path->value_type().type);
    EXPECT_EQ("root.profile.name", root->linear_path());
    EXPECT_EQ(TYPE_VARCHAR, root->leaf_value_type().type);
    EXPECT_EQ(1, root->leaf_size());
}

TEST(ColumnAccessPathCoreTest, insertJsonPathAndConvertByIndex) {
    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(TAccessPathType::ROOT, "root", 0));
    ColumnAccessPath::insert_json_path(root.get(), TYPE_BIGINT, "payload.value");

    ASSERT_EQ(1, root->children().size());
    auto* payload = root->children()[0].get();
    ASSERT_EQ(1, payload->children().size());
    auto* value = payload->children()[0].get();

    EXPECT_EQ("payload", payload->path());
    EXPECT_EQ("root.payload", payload->absolute_path());
    EXPECT_EQ("value", value->path());
    EXPECT_EQ("root.payload.value", value->absolute_path());
    EXPECT_EQ(TYPE_BIGINT, value->value_type().type);

    Field payload_field(12, "payload", get_struct_type_info({get_type_info(TYPE_BIGINT)}), true);
    payload_field.set_sub_fields({Field(13, "value", TYPE_BIGINT, true)});

    Field root_field(10, "root", get_struct_type_info({get_type_info(TYPE_INT), payload_field.type()}), true);
    root_field.set_sub_fields({Field(11, "ignored", TYPE_INT, true), payload_field});

    ASSIGN_OR_ABORT(auto converted, root->convert_by_index(&root_field, 7));
    EXPECT_EQ(7, converted->index());
    ASSERT_EQ(1, converted->children().size());
    auto* converted_payload = converted->children()[0].get();
    EXPECT_EQ(1, converted_payload->index());
    ASSERT_EQ(1, converted_payload->children().size());
    EXPECT_EQ(0, converted_payload->children()[0]->index());
}

TEST(ColumnAccessPathCoreTest, createReturnsResolverErrors) {
    auto root_path = make_thrift_path(TAccessPathType::ROOT);
    auto resolver = [](const TColumnAccessPath&) -> StatusOr<std::string> {
        return Status::InternalError("path resolver failed");
    };

    auto result = ColumnAccessPath::create(root_path, resolver);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().message().find("path resolver failed") != std::string::npos);
}

} // namespace starrocks
