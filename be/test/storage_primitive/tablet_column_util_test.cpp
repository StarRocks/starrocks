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

#include "storage/primitive/tablet_column_util.h"

#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "gtest/gtest.h"

namespace starrocks {

namespace {

TTypeNode create_scalar_type_node(TPrimitiveType::type type, int32_t length = 0) {
    TScalarType scalar_type;
    scalar_type.__set_type(type);
    scalar_type.__set_len(length);

    TTypeNode type_node;
    type_node.__set_type(TTypeNodeType::SCALAR);
    type_node.__set_scalar_type(scalar_type);
    return type_node;
}

TTypeDesc create_scalar_type_desc(TPrimitiveType::type type, int32_t length = 0) {
    TTypeDesc type_desc;
    type_desc.types.push_back(create_scalar_type_node(type, length));
    return type_desc;
}

} // namespace

TEST(TabletColumnUtilTest, ConvertToNewVersionBuildsTypeDescFromLegacyColumnType) {
    TColumnType column_type;
    column_type.__set_type(TPrimitiveType::VARCHAR);
    column_type.__set_len(32);
    column_type.__set_index_len(12);

    TColumn column;
    column.__set_column_type(column_type);

    ASSERT_FALSE(column.__isset.type_desc);
    convert_to_new_version(&column);

    ASSERT_TRUE(column.__isset.type_desc);
    ASSERT_EQ(1, column.type_desc.types.size());
    EXPECT_EQ(TTypeNodeType::SCALAR, column.type_desc.types[0].type);
    EXPECT_EQ(TPrimitiveType::VARCHAR, column.type_desc.types[0].scalar_type.type);
    EXPECT_EQ(32, column.type_desc.types[0].scalar_type.len);
    EXPECT_EQ(12, column.index_len);
}

TEST(TabletColumnUtilTest, ConvertScalarTColumnToColumnPbPreservesMetadata) {
    TColumn column;
    column.__set_column_name("value_col");
    column.__set_is_key(false);
    column.__set_is_allow_null(false);
    column.__set_has_bitmap_index(true);
    column.__set_is_auto_increment(true);
    column.__set_aggregation_type(TAggregationType::REPLACE_IF_NOT_NULL);
    column.__set_index_len(7);
    column.__set_default_value("fallback");
    column.__set_type_desc(create_scalar_type_desc(TPrimitiveType::VARCHAR, 16));

    ColumnPB column_pb;
    ASSERT_TRUE(t_column_to_pb_column(42, column, &column_pb).ok());

    EXPECT_EQ(42, column_pb.unique_id());
    EXPECT_EQ("value_col", column_pb.name());
    EXPECT_EQ("VARCHAR", column_pb.type());
    EXPECT_EQ(20, column_pb.length());
    EXPECT_EQ(7, column_pb.index_length());
    EXPECT_FALSE(column_pb.is_key());
    EXPECT_FALSE(column_pb.is_nullable());
    EXPECT_TRUE(column_pb.has_bitmap_index());
    EXPECT_TRUE(column_pb.is_auto_increment());
    EXPECT_EQ("replace_if_not_null", column_pb.aggregation());
    EXPECT_EQ("fallback", column_pb.default_value());
}

TEST(TabletColumnUtilTest, ConvertArrayTColumnToColumnPbBuildsChildColumn) {
    TTypeDesc type_desc;
    TTypeNode array_type;
    array_type.__set_type(TTypeNodeType::ARRAY);
    type_desc.types.push_back(array_type);
    type_desc.types.push_back(create_scalar_type_node(TPrimitiveType::INT));

    TColumn column;
    column.__set_column_name("array_col");
    column.__set_is_key(false);
    column.__set_is_allow_null(true);
    column.__set_aggregation_type(TAggregationType::NONE);
    column.__set_type_desc(type_desc);

    ColumnPB column_pb;
    ASSERT_TRUE(t_column_to_pb_column(8, column, &column_pb).ok());

    EXPECT_EQ(8, column_pb.unique_id());
    EXPECT_EQ("array_col", column_pb.name());
    EXPECT_EQ("ARRAY", column_pb.type());
    ASSERT_EQ(1, column_pb.children_columns_size());
    EXPECT_EQ("element", column_pb.children_columns(0).name());
    EXPECT_EQ("INT", column_pb.children_columns(0).type());
    EXPECT_EQ(-1, column_pb.children_columns(0).unique_id());
    EXPECT_TRUE(column_pb.children_columns(0).is_nullable());
}

} // namespace starrocks
