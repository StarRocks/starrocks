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

#include "storage/primitive/tablet_column.h"

#include <string>
#include <utility>
#include <vector>

#include "gen_cpp/tablet_schema.pb.h"
#include "gtest/gtest.h"
#include "storage/primitive/aggregate_type.h"
#include "types/agg_state_desc.h"
#include "types/type_descriptor.h"

namespace starrocks {

namespace {

ColumnPB make_column_pb(int32_t unique_id, const std::string& name, LogicalType type,
                        StorageAggregateType aggregation = STORAGE_AGGREGATE_NONE, bool nullable = true,
                        int32_t length = 4) {
    ColumnPB column;
    column.set_unique_id(unique_id);
    column.set_name(name);
    column.set_type(logical_type_to_string(type));
    column.set_is_key(false);
    column.set_is_nullable(nullable);
    column.set_aggregation(get_string_by_aggregation_type(aggregation));
    column.set_length(length);
    column.set_index_length(length);
    return column;
}

AggStateDesc make_agg_state_desc() {
    auto return_type = TypeDescriptor::from_logical_type(TYPE_BIGINT);
    std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_BIGINT)};
    return AggStateDesc("sum", return_type, std::move(arg_types), false, 1);
}

void expect_agg_state_desc_eq(const AggStateDesc& actual, const AggStateDesc& expected) {
    EXPECT_EQ(expected.get_func_name(), actual.get_func_name());
    EXPECT_EQ(expected.get_return_type(), actual.get_return_type());
    EXPECT_EQ(expected.get_arg_types(), actual.get_arg_types());
    EXPECT_EQ(expected.is_result_nullable(), actual.is_result_nullable());
    EXPECT_EQ(expected.get_func_version(), actual.get_func_version());
}

} // namespace

TEST(TabletColumnTest, InitFromPbAndRoundTripPreserveScalarMetadata) {
    auto column_pb = make_column_pb(7, "value", TYPE_VARCHAR, STORAGE_AGGREGATE_REPLACE_IF_NOT_NULL, false, 32);
    column_pb.set_index_length(12);
    column_pb.set_default_value("fallback");
    column_pb.set_precision(18);
    column_pb.set_frac(3);
    column_pb.set_is_bf_column(true);
    column_pb.set_has_bitmap_index(true);
    column_pb.set_is_auto_increment(true);

    TabletColumn column(column_pb);

    EXPECT_EQ(7, column.unique_id());
    EXPECT_EQ("value", std::string(column.name()));
    EXPECT_EQ(TYPE_VARCHAR, column.type());
    EXPECT_FALSE(column.is_nullable());
    EXPECT_EQ(STORAGE_AGGREGATE_REPLACE_IF_NOT_NULL, column.aggregation());
    EXPECT_EQ(32, column.length());
    EXPECT_EQ(12, column.index_length());
    ASSERT_TRUE(column.has_default_value());
    EXPECT_EQ("fallback", column.default_value());
    ASSERT_TRUE(column.has_precision());
    EXPECT_EQ(18, column.precision());
    ASSERT_TRUE(column.has_scale());
    EXPECT_EQ(3, column.scale());
    EXPECT_TRUE(column.is_bf_column());
    EXPECT_TRUE(column.has_bitmap_index());
    EXPECT_TRUE(column.is_auto_increment());

    ColumnPB round_trip;
    column.to_schema_pb(&round_trip);

    EXPECT_EQ(column_pb.unique_id(), round_trip.unique_id());
    EXPECT_EQ(column_pb.name(), round_trip.name());
    EXPECT_EQ(column_pb.type(), round_trip.type());
    EXPECT_EQ(column_pb.is_nullable(), round_trip.is_nullable());
    EXPECT_EQ(column_pb.aggregation(), round_trip.aggregation());
    EXPECT_EQ(column_pb.length(), round_trip.length());
    EXPECT_EQ(column_pb.index_length(), round_trip.index_length());
    EXPECT_EQ(column_pb.default_value(), round_trip.default_value());
    EXPECT_EQ(column_pb.precision(), round_trip.precision());
    EXPECT_EQ(column_pb.frac(), round_trip.frac());
    EXPECT_EQ(column_pb.is_bf_column(), round_trip.is_bf_column());
    EXPECT_EQ(column_pb.has_bitmap_index(), round_trip.has_bitmap_index());
    EXPECT_EQ(column_pb.is_auto_increment(), round_trip.is_auto_increment());
}

TEST(TabletColumnTest, NestedSubcolumnsArePreserved) {
    auto root = make_column_pb(10, "payload", TYPE_STRUCT, STORAGE_AGGREGATE_NONE, true, 1);

    auto* numbers = root.add_children_columns();
    *numbers = make_column_pb(11, "numbers", TYPE_ARRAY, STORAGE_AGGREGATE_NONE, true, 24);
    *numbers->add_children_columns() = make_column_pb(-1, "element", TYPE_INT);

    auto* attributes = root.add_children_columns();
    *attributes = make_column_pb(12, "attributes", TYPE_MAP, STORAGE_AGGREGATE_NONE, true, 24);
    *attributes->add_children_columns() = make_column_pb(-1, "key", TYPE_VARCHAR, STORAGE_AGGREGATE_NONE, false, 16);
    *attributes->add_children_columns() = make_column_pb(-1, "value", TYPE_JSON, STORAGE_AGGREGATE_NONE, true, 16);

    TabletColumn column(root);

    ASSERT_EQ(2, column.subcolumn_count());
    EXPECT_EQ("numbers", std::string(column.subcolumn(0).name()));
    EXPECT_EQ(TYPE_ARRAY, column.subcolumn(0).type());
    ASSERT_EQ(1, column.subcolumn(0).subcolumn_count());
    EXPECT_EQ("element", std::string(column.subcolumn(0).subcolumn(0).name()));
    EXPECT_EQ(TYPE_INT, column.subcolumn(0).subcolumn(0).type());

    EXPECT_EQ("attributes", std::string(column.subcolumn(1).name()));
    EXPECT_EQ(TYPE_MAP, column.subcolumn(1).type());
    ASSERT_EQ(2, column.subcolumn(1).subcolumn_count());
    EXPECT_EQ("key", std::string(column.subcolumn(1).subcolumn(0).name()));
    EXPECT_EQ("value", std::string(column.subcolumn(1).subcolumn(1).name()));

    ColumnPB round_trip;
    column.to_schema_pb(&round_trip);
    ASSERT_EQ(2, round_trip.children_columns_size());
    EXPECT_EQ("numbers", round_trip.children_columns(0).name());
    ASSERT_EQ(1, round_trip.children_columns(0).children_columns_size());
    EXPECT_EQ("element", round_trip.children_columns(0).children_columns(0).name());
    EXPECT_EQ("attributes", round_trip.children_columns(1).name());
    ASSERT_EQ(2, round_trip.children_columns(1).children_columns_size());
    EXPECT_EQ("key", round_trip.children_columns(1).children_columns(0).name());
    EXPECT_EQ("value", round_trip.children_columns(1).children_columns(1).name());
}

TEST(TabletColumnTest, CopyAndMovePreserveOwnedExtraState) {
    auto column_pb = make_column_pb(20, "state", TYPE_VARCHAR, STORAGE_AGGREGATE_AGG_STATE_UNION, true, 16);
    column_pb.set_default_value("default-state");
    *column_pb.add_children_columns() = make_column_pb(-1, "nested", TYPE_INT);

    const auto expected_desc = make_agg_state_desc();
    expected_desc.to_protobuf(column_pb.mutable_agg_state_desc());

    TabletColumn original(column_pb);
    TabletColumn copied(original);

    ASSERT_TRUE(copied.has_default_value());
    EXPECT_EQ("default-state", copied.default_value());
    ASSERT_EQ(1, copied.subcolumn_count());
    EXPECT_EQ("nested", std::string(copied.subcolumn(0).name()));
    ASSERT_TRUE(copied.has_agg_state_desc());
    ASSERT_TRUE(original.has_agg_state_desc());
    EXPECT_NE(original.get_agg_state_desc(), copied.get_agg_state_desc());
    expect_agg_state_desc_eq(*copied.get_agg_state_desc(), expected_desc);

    TabletColumn moved(std::move(copied));
    ASSERT_TRUE(moved.has_default_value());
    EXPECT_EQ("default-state", moved.default_value());
    ASSERT_EQ(1, moved.subcolumn_count());
    EXPECT_EQ("nested", std::string(moved.subcolumn(0).name()));
    ASSERT_TRUE(moved.has_agg_state_desc());
    expect_agg_state_desc_eq(*moved.get_agg_state_desc(), expected_desc);
}

TEST(TabletColumnTest, ChecksumSupportRecursesIntoSubcolumns) {
    auto supported_array = make_column_pb(30, "numbers", TYPE_ARRAY, STORAGE_AGGREGATE_NONE, true, 24);
    *supported_array.add_children_columns() = make_column_pb(-1, "element", TYPE_INT);

    EXPECT_TRUE(TabletColumn(supported_array).is_support_checksum());

    auto unsupported_struct = make_column_pb(31, "payload", TYPE_STRUCT, STORAGE_AGGREGATE_NONE, true, 1);
    *unsupported_struct.add_children_columns() = make_column_pb(32, "id", TYPE_INT);
    *unsupported_struct.add_children_columns() =
            make_column_pb(33, "body", TYPE_JSON, STORAGE_AGGREGATE_NONE, true, 16);

    EXPECT_FALSE(TabletColumn(unsupported_struct).is_support_checksum());
}

} // namespace starrocks
