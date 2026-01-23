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

#include "storage/lake/tablet_range_helper.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "gen_cpp/AgentService_types.h"
#include "runtime/types.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"

namespace starrocks::lake {

TEST(TabletRangeHelperTest, test_create_sst_seek_range_from) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(PRIMARY_KEYS);

    // c0, c1 are keys
    auto c0 = schema_pb.add_column();
    c0->set_name("c0");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);

    auto c1 = schema_pb.add_column();
    c1->set_name("c1");
    c1->set_type("INT");
    c1->set_is_key(true);
    c1->set_is_nullable(false);

    auto c2 = schema_pb.add_column();
    c2->set_name("c2");
    c2->set_type("INT");
    c2->set_is_key(false);
    c2->set_is_nullable(true);

    // Case 1: sort keys are the same as pk keys (c0, c1) -> index (0, 1)
    schema_pb.clear_sort_key_idxes();
    schema_pb.add_sort_key_idxes(0);
    schema_pb.add_sort_key_idxes(1);

    auto tablet_schema = TabletSchema::create(schema_pb);

    TabletRangePB range_pb;
    auto lower = range_pb.mutable_lower_bound();
    auto v0 = lower->add_values();
    TypeDescriptor type_int(TYPE_INT);
    v0->mutable_type()->CopyFrom(type_int.to_protobuf());
    v0->set_value("1");
    auto v1 = lower->add_values();
    v1->mutable_type()->CopyFrom(type_int.to_protobuf());
    v1->set_value("2");
    range_pb.set_lower_bound_included(true);

    auto res = TabletRangeHelper::create_sst_seek_range_from(range_pb, tablet_schema);
    ASSERT_OK(res.status());
    ASSERT_FALSE(res.value().seek_key.empty());

    // Case 2: different order -> Should return InternalError
    schema_pb.clear_sort_key_idxes();
    schema_pb.add_sort_key_idxes(1);
    schema_pb.add_sort_key_idxes(0);
    auto tablet_schema_wrong = TabletSchema::create(schema_pb);
    auto res2 = TabletRangeHelper::create_sst_seek_range_from(range_pb, tablet_schema_wrong);
    ASSERT_FALSE(res2.ok());
    ASSERT_TRUE(res2.status().is_internal_error());
    ASSERT_THAT(res2.status().to_string(), testing::HasSubstr("Sort key index 0 must be 0, but is 1"));
}

TEST(TabletRangeHelperTest, test_non_nullable_key_rejects_null_range) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(PRIMARY_KEYS);

    auto c0 = schema_pb.add_column();
    c0->set_name("c0");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);

    schema_pb.clear_sort_key_idxes();
    schema_pb.add_sort_key_idxes(0);
    auto tablet_schema = TabletSchema::create(schema_pb);

    TabletRangePB range_pb;
    auto lower = range_pb.mutable_lower_bound();
    auto v0 = lower->add_values();
    TypeDescriptor type_int(TYPE_INT);
    v0->mutable_type()->CopyFrom(type_int.to_protobuf());
    v0->set_variant_type(VariantTypePB::NULL_VALUE);
    range_pb.set_lower_bound_included(true);

    auto res = TabletRangeHelper::create_sst_seek_range_from(range_pb, tablet_schema);
    ASSERT_FALSE(res.ok());
    ASSERT_TRUE(res.status().is_invalid_argument());
    ASSERT_EQ("Non-nullable primary key contains NULL in tablet range", res.status().message());
}

TEST(TabletRangeHelperTest, test_convert_t_range_to_pb_range_full_range) {
    TTabletRange t_range;

    // Set lower bound with multiple values
    TRangeLiteral lower_bound;

    TExprValue value1;
    TTypeDesc type_desc1;
    type_desc1.types.resize(1);
    type_desc1.types[0].type = TTypeNodeType::SCALAR;
    type_desc1.types[0].scalar_type.type = TPrimitiveType::INT;
    value1.__set_type(type_desc1);
    value1.__set_value("100");
    value1.__set_variant_type(TLiteralVariantType::NORMAL_VALUE);
    lower_bound.values.push_back(value1);

    TExprValue value2;
    TTypeDesc type_desc2;
    type_desc2.types.resize(1);
    type_desc2.types[0].type = TTypeNodeType::SCALAR;
    type_desc2.types[0].scalar_type.type = TPrimitiveType::VARCHAR;
    type_desc2.types[0].scalar_type.len = 10;
    value2.__set_type(type_desc2);
    value2.__set_value("test_str");
    value2.__set_variant_type(TLiteralVariantType::NORMAL_VALUE);
    lower_bound.values.push_back(value2);

    t_range.__set_lower_bound(lower_bound);

    // Set upper bound with multiple values
    TRangeLiteral upper_bound;

    TExprValue value3;
    value3.__set_type(type_desc1);
    value3.__set_value("200");
    value3.__set_variant_type(TLiteralVariantType::NORMAL_VALUE);
    upper_bound.values.push_back(value3);

    TExprValue value4;
    value4.__set_type(type_desc2);
    value4.__set_value("test_end");
    value4.__set_variant_type(TLiteralVariantType::NORMAL_VALUE);
    upper_bound.values.push_back(value4);

    t_range.__set_upper_bound(upper_bound);
    t_range.__set_lower_bound_included(true);
    t_range.__set_upper_bound_included(false);

    // Convert and verify
    TabletRangePB pb_range = TabletRangeHelper::convert_t_range_to_pb_range(t_range);

    ASSERT_TRUE(pb_range.has_lower_bound());
    ASSERT_EQ(2, pb_range.lower_bound().values_size());
    ASSERT_EQ("100", pb_range.lower_bound().values(0).value());
    ASSERT_EQ(VariantTypePB::NORMAL_VALUE, pb_range.lower_bound().values(0).variant_type());
    ASSERT_EQ("test_str", pb_range.lower_bound().values(1).value());

    ASSERT_TRUE(pb_range.has_upper_bound());
    ASSERT_EQ(2, pb_range.upper_bound().values_size());
    ASSERT_EQ("200", pb_range.upper_bound().values(0).value());
    ASSERT_EQ("test_end", pb_range.upper_bound().values(1).value());

    ASSERT_TRUE(pb_range.has_lower_bound_included());
    ASSERT_TRUE(pb_range.lower_bound_included());
    ASSERT_TRUE(pb_range.has_upper_bound_included());
    ASSERT_FALSE(pb_range.upper_bound_included());
}

TEST(TabletRangeHelperTest, test_convert_t_range_to_pb_range_with_nulls) {
    TTabletRange t_range;

    // Lower bound with NULL value
    TRangeLiteral lower_bound;

    TExprValue null_value;
    TTypeDesc type_desc;
    type_desc.types.resize(1);
    type_desc.types[0].type = TTypeNodeType::SCALAR;
    type_desc.types[0].scalar_type.type = TPrimitiveType::INT;
    null_value.__set_type(type_desc);
    null_value.__set_variant_type(TLiteralVariantType::NULL_VALUE);
    lower_bound.values.push_back(null_value);

    t_range.__set_lower_bound(lower_bound);
    t_range.__set_lower_bound_included(true);

    // Convert and verify
    TabletRangePB pb_range = TabletRangeHelper::convert_t_range_to_pb_range(t_range);

    ASSERT_TRUE(pb_range.has_lower_bound());
    ASSERT_EQ(1, pb_range.lower_bound().values_size());
    ASSERT_EQ(VariantTypePB::NULL_VALUE, pb_range.lower_bound().values(0).variant_type());
    ASSERT_FALSE(pb_range.lower_bound().values(0).has_value());
    ASSERT_TRUE(pb_range.has_lower_bound_included());
    ASSERT_TRUE(pb_range.lower_bound_included());
    ASSERT_FALSE(pb_range.has_upper_bound());
}

TEST(TabletRangeHelperTest, test_convert_t_range_to_pb_range_min_max_values) {
    TTabletRange t_range;

    // Lower bound with MIN_VALUE
    TRangeLiteral lower_bound;
    TExprValue min_value;
    TTypeDesc type_desc;
    type_desc.types.resize(1);
    type_desc.types[0].type = TTypeNodeType::SCALAR;
    type_desc.types[0].scalar_type.type = TPrimitiveType::BIGINT;
    min_value.__set_type(type_desc);
    min_value.__set_variant_type(TLiteralVariantType::MIN_VALUE);
    lower_bound.values.push_back(min_value);
    t_range.__set_lower_bound(lower_bound);

    // Upper bound with MAX_VALUE
    TRangeLiteral upper_bound;
    TExprValue max_value;
    max_value.__set_type(type_desc);
    max_value.__set_variant_type(TLiteralVariantType::MAX_VALUE);
    upper_bound.values.push_back(max_value);
    t_range.__set_upper_bound(upper_bound);

    // Convert and verify
    TabletRangePB pb_range = TabletRangeHelper::convert_t_range_to_pb_range(t_range);

    ASSERT_TRUE(pb_range.has_lower_bound());
    ASSERT_EQ(1, pb_range.lower_bound().values_size());
    ASSERT_EQ(VariantTypePB::MIN_VALUE, pb_range.lower_bound().values(0).variant_type());

    ASSERT_TRUE(pb_range.has_upper_bound());
    ASSERT_EQ(1, pb_range.upper_bound().values_size());
    ASSERT_EQ(VariantTypePB::MAX_VALUE, pb_range.upper_bound().values(0).variant_type());
}

TEST(TabletRangeHelperTest, test_convert_t_range_to_pb_range_only_lower_bound) {
    TTabletRange t_range;

    TRangeLiteral lower_bound;
    TExprValue value;
    TTypeDesc type_desc;
    type_desc.types.resize(1);
    type_desc.types[0].type = TTypeNodeType::SCALAR;
    type_desc.types[0].scalar_type.type = TPrimitiveType::DOUBLE;
    value.__set_type(type_desc);
    value.__set_value("3.14159");
    value.__set_variant_type(TLiteralVariantType::NORMAL_VALUE);
    lower_bound.values.push_back(value);

    t_range.__set_lower_bound(lower_bound);
    t_range.__set_lower_bound_included(false);

    TabletRangePB pb_range = TabletRangeHelper::convert_t_range_to_pb_range(t_range);

    ASSERT_TRUE(pb_range.has_lower_bound());
    ASSERT_EQ(1, pb_range.lower_bound().values_size());
    ASSERT_EQ("3.14159", pb_range.lower_bound().values(0).value());
    ASSERT_TRUE(pb_range.has_lower_bound_included());
    ASSERT_FALSE(pb_range.lower_bound_included());
    ASSERT_FALSE(pb_range.has_upper_bound());
    ASSERT_FALSE(pb_range.has_upper_bound_included());
}

TEST(TabletRangeHelperTest, test_convert_t_range_to_pb_range_only_upper_bound) {
    TTabletRange t_range;

    TRangeLiteral upper_bound;
    TExprValue value;
    TTypeDesc type_desc;
    type_desc.types.resize(1);
    type_desc.types[0].type = TTypeNodeType::SCALAR;
    type_desc.types[0].scalar_type.type = TPrimitiveType::DATE;
    value.__set_type(type_desc);
    value.__set_value("2024-01-01");
    value.__set_variant_type(TLiteralVariantType::NORMAL_VALUE);
    upper_bound.values.push_back(value);

    t_range.__set_upper_bound(upper_bound);
    t_range.__set_upper_bound_included(true);

    TabletRangePB pb_range = TabletRangeHelper::convert_t_range_to_pb_range(t_range);

    ASSERT_FALSE(pb_range.has_lower_bound());
    ASSERT_TRUE(pb_range.has_upper_bound());
    ASSERT_EQ(1, pb_range.upper_bound().values_size());
    ASSERT_EQ("2024-01-01", pb_range.upper_bound().values(0).value());
    ASSERT_TRUE(pb_range.has_upper_bound_included());
    ASSERT_TRUE(pb_range.upper_bound_included());
    ASSERT_FALSE(pb_range.has_lower_bound_included());
}

TEST(TabletRangeHelperTest, test_convert_t_range_to_pb_range_empty_range) {
    TTabletRange t_range;

    TabletRangePB pb_range = TabletRangeHelper::convert_t_range_to_pb_range(t_range);

    ASSERT_FALSE(pb_range.has_lower_bound());
    ASSERT_FALSE(pb_range.has_upper_bound());
    ASSERT_FALSE(pb_range.has_lower_bound_included());
    ASSERT_FALSE(pb_range.has_upper_bound_included());
}

TEST(TabletRangeHelperTest, test_convert_t_range_to_pb_range_empty_values_list) {
    TTabletRange t_range;

    // Lower bound with empty values list
    TRangeLiteral lower_bound;
    t_range.__set_lower_bound(lower_bound);

    // Upper bound with empty values list
    TRangeLiteral upper_bound;
    t_range.__set_upper_bound(upper_bound);

    t_range.__set_lower_bound_included(true);
    t_range.__set_upper_bound_included(false);

    TabletRangePB pb_range = TabletRangeHelper::convert_t_range_to_pb_range(t_range);

    ASSERT_TRUE(pb_range.has_lower_bound());
    ASSERT_EQ(0, pb_range.lower_bound().values_size());
    ASSERT_TRUE(pb_range.has_upper_bound());
    ASSERT_EQ(0, pb_range.upper_bound().values_size());
    ASSERT_TRUE(pb_range.has_lower_bound_included());
    ASSERT_TRUE(pb_range.lower_bound_included());
    ASSERT_TRUE(pb_range.has_upper_bound_included());
    ASSERT_FALSE(pb_range.upper_bound_included());
}

TEST(TabletRangeHelperTest, test_convert_t_range_to_pb_range_complex_types) {
    TTabletRange t_range;

    TRangeLiteral lower_bound;

    // DECIMAL type
    TExprValue decimal_value;
    TTypeDesc decimal_type;
    decimal_type.types.resize(1);
    decimal_type.types[0].type = TTypeNodeType::SCALAR;
    decimal_type.types[0].scalar_type.type = TPrimitiveType::DECIMAL128;
    decimal_type.types[0].scalar_type.precision = 10;
    decimal_type.types[0].scalar_type.scale = 2;
    decimal_value.__set_type(decimal_type);
    decimal_value.__set_value("123.45");
    decimal_value.__set_variant_type(TLiteralVariantType::NORMAL_VALUE);
    lower_bound.values.push_back(decimal_value);

    // DATETIME type
    TExprValue datetime_value;
    TTypeDesc datetime_type;
    datetime_type.types.resize(1);
    datetime_type.types[0].type = TTypeNodeType::SCALAR;
    datetime_type.types[0].scalar_type.type = TPrimitiveType::DATETIME;
    datetime_value.__set_type(datetime_type);
    datetime_value.__set_value("2024-01-01 12:00:00");
    datetime_value.__set_variant_type(TLiteralVariantType::NORMAL_VALUE);
    lower_bound.values.push_back(datetime_value);

    t_range.__set_lower_bound(lower_bound);

    TabletRangePB pb_range = TabletRangeHelper::convert_t_range_to_pb_range(t_range);

    ASSERT_TRUE(pb_range.has_lower_bound());
    ASSERT_EQ(2, pb_range.lower_bound().values_size());

    // Verify DECIMAL type conversion
    const auto& pb_decimal_val = pb_range.lower_bound().values(0);
    ASSERT_EQ("123.45", pb_decimal_val.value());
    ASSERT_EQ(PrimitiveType::TYPE_DECIMAL128, pb_decimal_val.type().type());
    ASSERT_EQ(10, pb_decimal_val.type().precision());
    ASSERT_EQ(2, pb_decimal_val.type().scale());

    // Verify DATETIME type conversion
    const auto& pb_datetime_val = pb_range.lower_bound().values(1);
    ASSERT_EQ("2024-01-01 12:00:00", pb_datetime_val.value());
    ASSERT_EQ(PrimitiveType::TYPE_DATETIME, pb_datetime_val.type().type());
}

TEST(TabletRangeHelperTest, test_convert_t_range_to_pb_range_partial_fields) {
    TTabletRange t_range;

    TRangeLiteral lower_bound;

    // Value with only variant_type set (no type, no value)
    TExprValue value1;
    value1.__set_variant_type(TLiteralVariantType::MIN_VALUE);
    lower_bound.values.push_back(value1);

    // Value with only value set (no type, no variant_type)
    TExprValue value2;
    value2.__set_value("some_value");
    lower_bound.values.push_back(value2);

    // Value with only type set
    TExprValue value3;
    TTypeDesc type_desc;
    type_desc.types.resize(1);
    type_desc.types[0].type = TTypeNodeType::SCALAR;
    type_desc.types[0].scalar_type.type = TPrimitiveType::INT;
    value3.__set_type(type_desc);
    lower_bound.values.push_back(value3);

    t_range.__set_lower_bound(lower_bound);

    TabletRangePB pb_range = TabletRangeHelper::convert_t_range_to_pb_range(t_range);

    ASSERT_TRUE(pb_range.has_lower_bound());
    ASSERT_EQ(3, pb_range.lower_bound().values_size());

    // Check first value (only variant_type)
    const auto& val1 = pb_range.lower_bound().values(0);
    ASSERT_EQ(VariantTypePB::MIN_VALUE, val1.variant_type());
    ASSERT_FALSE(val1.has_value());
    ASSERT_FALSE(val1.has_type());

    // Check second value (only value)
    const auto& val2 = pb_range.lower_bound().values(1);
    ASSERT_EQ("some_value", val2.value());
    ASSERT_FALSE(val2.has_type());
    ASSERT_FALSE(val2.has_variant_type());

    // Check third value (only type)
    const auto& val3 = pb_range.lower_bound().values(2);
    ASSERT_TRUE(val3.has_type());
    ASSERT_EQ(PrimitiveType::TYPE_INT, val3.type().type());
    ASSERT_FALSE(val3.has_value());
    ASSERT_FALSE(val3.has_variant_type());
}

} // namespace starrocks::lake
