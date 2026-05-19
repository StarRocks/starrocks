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

#include "base/testutil/assert.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/raw_data_visitor.h"
#include "gen_cpp/AgentService_types.h"
#include "storage/chunk_helper.h"
#include "storage/primary_key_encoder.h"
#include "storage/tablet_range.h"
#include "storage/tablet_schema.h"
#include "types/type_descriptor.h"

namespace starrocks::lake {

using google::protobuf::RepeatedPtrField;

namespace {

static VariantTuple make_int_tuple(int32_t value) {
    VariantTuple tuple;
    tuple.append(DatumVariant(get_type_info(LogicalType::TYPE_INT), Datum(value)));
    return tuple;
}

static TuplePB make_int_tuple_pb(int32_t value) {
    TuplePB tuple_pb;
    make_int_tuple(value).to_proto(&tuple_pb);
    return tuple_pb;
}

} // namespace

TEST(TabletRangeHelperTest, test_tablet_range_intersect) {
    TabletRange lhs(make_int_tuple(1), make_int_tuple(10), true, false);
    TabletRange rhs(make_int_tuple(5), make_int_tuple(12), true, false);
    ASSIGN_OR_ABORT(auto r1, lhs.intersect(rhs));
    EXPECT_TRUE(r1.lower_bound_included());
    EXPECT_FALSE(r1.upper_bound_included());
    EXPECT_EQ(0, r1.lower_bound().compare(make_int_tuple(5)));
    EXPECT_EQ(0, r1.upper_bound().compare(make_int_tuple(10)));

    TabletRange rhs2(make_int_tuple(10), make_int_tuple(12), true, false);
    ASSIGN_OR_ABORT(auto r2, lhs.intersect(rhs2));
    EXPECT_TRUE(r2.is_empty());
    EXPECT_EQ(0, r2.lower_bound().compare(make_int_tuple(10)));
    EXPECT_EQ(0, r2.upper_bound().compare(make_int_tuple(10)));
    EXPECT_TRUE(r2.lower_bound_included());
    EXPECT_FALSE(r2.upper_bound_included());

    TabletRange lhs_all;
    TabletRange rhs3(make_int_tuple(3), make_int_tuple(4), true, false);
    ASSIGN_OR_ABORT(auto r3, lhs_all.intersect(rhs3));
    EXPECT_EQ(0, r3.lower_bound().compare(make_int_tuple(3)));
    EXPECT_EQ(0, r3.upper_bound().compare(make_int_tuple(4)));
}

TEST(TabletRangeHelperTest, test_tablet_range_intersect_branch_coverage) {
    // rhs has minimum lower bound.
    TabletRange rhs_min;
    TabletRangePB rhs_min_pb;
    rhs_min_pb.mutable_upper_bound()->CopyFrom(make_int_tuple_pb(8));
    rhs_min_pb.set_upper_bound_included(false);
    ASSERT_OK(rhs_min.from_proto(rhs_min_pb));
    TabletRange lhs1(make_int_tuple(3), make_int_tuple(10), true, false);
    ASSIGN_OR_ABORT(auto r1, lhs1.intersect(rhs_min));
    EXPECT_EQ(0, r1.lower_bound().compare(make_int_tuple(3)));
    EXPECT_EQ(0, r1.upper_bound().compare(make_int_tuple(8)));
    EXPECT_TRUE(r1.lower_bound_included());
    EXPECT_FALSE(r1.upper_bound_included());

    // lower cmp > 0, upper cmp > 0.
    TabletRange lhs2(make_int_tuple(5), make_int_tuple(12), true, false);
    TabletRange rhs2(make_int_tuple(3), make_int_tuple(8), true, true);
    ASSIGN_OR_ABORT(auto r2, lhs2.intersect(rhs2));
    EXPECT_EQ(0, r2.lower_bound().compare(make_int_tuple(5)));
    EXPECT_EQ(0, r2.upper_bound().compare(make_int_tuple(8)));
    EXPECT_TRUE(r2.lower_bound_included());
    EXPECT_TRUE(r2.upper_bound_included());

    // lower cmp < 0, upper cmp < 0.
    TabletRange lhs3(make_int_tuple(1), make_int_tuple(7), true, false);
    TabletRange rhs3(make_int_tuple(3), make_int_tuple(9), false, true);
    ASSIGN_OR_ABORT(auto r3, lhs3.intersect(rhs3));
    EXPECT_EQ(0, r3.lower_bound().compare(make_int_tuple(3)));
    EXPECT_EQ(0, r3.upper_bound().compare(make_int_tuple(7)));
    EXPECT_FALSE(r3.lower_bound_included());
    EXPECT_FALSE(r3.upper_bound_included());

    // lower cmp == 0.
    TabletRange lhs4(make_int_tuple(5), make_int_tuple(10), true, false);
    TabletRange rhs4(make_int_tuple(5), make_int_tuple(12), false, true);
    ASSIGN_OR_ABORT(auto r4, lhs4.intersect(rhs4));
    EXPECT_EQ(0, r4.lower_bound().compare(make_int_tuple(5)));
    EXPECT_FALSE(r4.lower_bound_included());

    // rhs has maximum upper bound.
    TabletRange rhs_max;
    TabletRangePB rhs_max_pb;
    rhs_max_pb.mutable_lower_bound()->CopyFrom(make_int_tuple_pb(6));
    rhs_max_pb.set_lower_bound_included(true);
    ASSERT_OK(rhs_max.from_proto(rhs_max_pb));
    TabletRange lhs5(make_int_tuple(4), make_int_tuple(9), true, true);
    ASSIGN_OR_ABORT(auto r5, lhs5.intersect(rhs_max));
    EXPECT_EQ(0, r5.upper_bound().compare(make_int_tuple(9)));
    EXPECT_TRUE(r5.upper_bound_included());

    // upper cmp == 0.
    TabletRange lhs6(make_int_tuple(1), make_int_tuple(10), true, true);
    TabletRange rhs6(make_int_tuple(2), make_int_tuple(10), true, false);
    ASSIGN_OR_ABORT(auto r6, lhs6.intersect(rhs6));
    EXPECT_EQ(0, r6.upper_bound().compare(make_int_tuple(10)));
    EXPECT_FALSE(r6.upper_bound_included());

    // Canonicalize empty range.
    TabletRange lhs7(make_int_tuple(10), make_int_tuple(20), true, false);
    TabletRange rhs7(make_int_tuple(1), make_int_tuple(5), true, false);
    ASSIGN_OR_ABORT(auto r7, lhs7.intersect(rhs7));
    EXPECT_TRUE(r7.is_empty());
    EXPECT_EQ(0, r7.lower_bound().compare(make_int_tuple(10)));
    EXPECT_EQ(0, r7.upper_bound().compare(make_int_tuple(10)));
    EXPECT_TRUE(r7.lower_bound_included());
    EXPECT_FALSE(r7.upper_bound_included());
}

TEST(TabletRangeHelperTest, test_create_sst_seek_range_from) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(PRIMARY_KEYS);
    schema_pb.set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);

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

// NULL on a non-nullable PK column is treated as type-minimum (MIN sentinel from FE).
TEST(TabletRangeHelperTest, test_non_nullable_key_null_fills_type_min) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(PRIMARY_KEYS);
    schema_pb.set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);

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

    // Should succeed — NULL is filled with INT_MIN.
    auto res = TabletRangeHelper::create_sst_seek_range_from(range_pb, tablet_schema);
    ASSERT_OK(res.status());
    ASSERT_FALSE(res.value().seek_key.empty());
}

TEST(TabletRangeHelperTest, test_create_sst_seek_range_requires_v2_encoding) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(PRIMARY_KEYS);
    schema_pb.set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V1);

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
    v0->set_value("1");
    range_pb.set_lower_bound_included(true);

    auto res = TabletRangeHelper::create_sst_seek_range_from(range_pb, tablet_schema);
    ASSERT_FALSE(res.ok());
    ASSERT_TRUE(res.status().is_invalid_argument());
    ASSERT_THAT(res.status().to_string(),
                testing::HasSubstr("Big-endian encoding is required for range-distribution table in share data mode"));
}

TEST(TabletRangeHelperTest, test_convert_t_range_to_pb_range_full_range) {
    TTabletRange t_range;

    // Set lower bound with multiple values
    TTuple lower_bound;

    TVariant value1;
    TTypeDesc type_desc1;
    type_desc1.types.resize(1);
    type_desc1.__isset.types = true;
    type_desc1.types[0].type = TTypeNodeType::SCALAR;
    type_desc1.types[0].scalar_type.__set_type(TPrimitiveType::INT);
    type_desc1.types[0].__isset.scalar_type = true;
    value1.__set_type(type_desc1);
    value1.__set_value("100");
    value1.__set_variant_type(TVariantType::NORMAL_VALUE);
    lower_bound.values.push_back(value1);

    TVariant value2;
    TTypeDesc type_desc2;
    type_desc2.types.resize(1);
    type_desc2.__isset.types = true;
    type_desc2.types[0].type = TTypeNodeType::SCALAR;
    type_desc2.types[0].scalar_type.__set_type(TPrimitiveType::VARCHAR);
    type_desc2.types[0].scalar_type.__set_len(10);
    type_desc2.types[0].__isset.scalar_type = true;
    value2.__set_type(type_desc2);
    value2.__set_value("test_str");
    value2.__set_variant_type(TVariantType::NORMAL_VALUE);
    lower_bound.values.push_back(value2);

    t_range.__set_lower_bound(lower_bound);

    // Set upper bound with multiple values
    TTuple upper_bound;

    TVariant value3;
    value3.__set_type(type_desc1);
    value3.__set_value("200");
    value3.__set_variant_type(TVariantType::NORMAL_VALUE);
    upper_bound.values.push_back(value3);

    TVariant value4;
    value4.__set_type(type_desc2);
    value4.__set_value("test_end");
    value4.__set_variant_type(TVariantType::NORMAL_VALUE);
    upper_bound.values.push_back(value4);

    t_range.__set_upper_bound(upper_bound);
    t_range.__set_lower_bound_included(true);
    t_range.__set_upper_bound_included(false);

    // Convert and verify
    auto res = TabletRangeHelper::convert_t_range_to_pb_range(t_range);
    ASSERT_OK(res.status());
    TabletRangePB pb_range = res.value();

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
    TTuple lower_bound;

    TVariant null_value;
    TTypeDesc type_desc;
    type_desc.types.resize(1);
    type_desc.__isset.types = true;
    type_desc.types[0].type = TTypeNodeType::SCALAR;
    type_desc.types[0].scalar_type.__set_type(TPrimitiveType::INT);
    type_desc.types[0].__isset.scalar_type = true;
    null_value.__set_type(type_desc);
    null_value.__set_variant_type(TVariantType::NULL_VALUE);
    lower_bound.values.push_back(null_value);

    t_range.__set_lower_bound(lower_bound);
    t_range.__set_lower_bound_included(true);

    // Convert and verify
    auto res = TabletRangeHelper::convert_t_range_to_pb_range(t_range);
    ASSERT_OK(res.status());
    TabletRangePB pb_range = res.value();

    ASSERT_TRUE(pb_range.has_lower_bound());
    ASSERT_EQ(1, pb_range.lower_bound().values_size());
    ASSERT_EQ(VariantTypePB::NULL_VALUE, pb_range.lower_bound().values(0).variant_type());
    ASSERT_FALSE(pb_range.lower_bound().values(0).has_value());
    ASSERT_TRUE(pb_range.has_lower_bound_included());
    ASSERT_TRUE(pb_range.lower_bound_included());
    ASSERT_FALSE(pb_range.has_upper_bound());
}

// MINIMUM/MAXIMUM in TTabletRange should be rejected at conversion time.
TEST(TabletRangeHelperTest, test_convert_t_range_to_pb_range_rejects_min_max_in_lower) {
    TTabletRange t_range;
    TTuple lower_bound;
    TVariant min_value;
    TTypeDesc type_desc;
    type_desc.types.resize(1);
    type_desc.__isset.types = true;
    type_desc.types[0].type = TTypeNodeType::SCALAR;
    type_desc.types[0].scalar_type.__set_type(TPrimitiveType::BIGINT);
    type_desc.types[0].__isset.scalar_type = true;
    min_value.__set_type(type_desc);
    min_value.__set_variant_type(TVariantType::MINIMUM);
    lower_bound.values.push_back(min_value);
    t_range.__set_lower_bound(lower_bound);

    auto res = TabletRangeHelper::convert_t_range_to_pb_range(t_range);
    ASSERT_FALSE(res.ok());
    ASSERT_TRUE(res.status().is_invalid_argument());
    ASSERT_THAT(res.status().to_string(),
                testing::HasSubstr("MINIMUM/MAXIMUM variant is not supported in tablet range"));
}

TEST(TabletRangeHelperTest, test_convert_t_range_to_pb_range_only_lower_bound) {
    TTabletRange t_range;

    TTuple lower_bound;
    TVariant value;
    TTypeDesc type_desc;
    type_desc.types.resize(1);
    type_desc.__isset.types = true;
    type_desc.types[0].type = TTypeNodeType::SCALAR;
    type_desc.types[0].scalar_type.__set_type(TPrimitiveType::DOUBLE);
    type_desc.types[0].__isset.scalar_type = true;
    value.__set_type(type_desc);
    value.__set_value("3.14159");
    value.__set_variant_type(TVariantType::NORMAL_VALUE);
    lower_bound.values.push_back(value);

    t_range.__set_lower_bound(lower_bound);
    t_range.__set_lower_bound_included(false);

    auto res = TabletRangeHelper::convert_t_range_to_pb_range(t_range);
    ASSERT_OK(res.status());
    TabletRangePB pb_range = res.value();

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

    TTuple upper_bound;
    TVariant value;
    TTypeDesc type_desc;
    type_desc.types.resize(1);
    type_desc.__isset.types = true;
    type_desc.types[0].type = TTypeNodeType::SCALAR;
    type_desc.types[0].scalar_type.__set_type(TPrimitiveType::DATE);
    type_desc.types[0].__isset.scalar_type = true;
    value.__set_type(type_desc);
    value.__set_value("2024-01-01");
    value.__set_variant_type(TVariantType::NORMAL_VALUE);
    upper_bound.values.push_back(value);

    t_range.__set_upper_bound(upper_bound);
    t_range.__set_upper_bound_included(true);

    auto res = TabletRangeHelper::convert_t_range_to_pb_range(t_range);
    ASSERT_OK(res.status());
    TabletRangePB pb_range = res.value();

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

    auto res = TabletRangeHelper::convert_t_range_to_pb_range(t_range);
    ASSERT_OK(res.status());
    TabletRangePB pb_range = res.value();

    ASSERT_FALSE(pb_range.has_lower_bound());
    ASSERT_FALSE(pb_range.has_upper_bound());
    ASSERT_FALSE(pb_range.has_lower_bound_included());
    ASSERT_FALSE(pb_range.has_upper_bound_included());
}

TEST(TabletRangeHelperTest, test_convert_t_range_to_pb_range_empty_values_list) {
    TTabletRange t_range;

    // Lower bound with empty values list
    TTuple lower_bound;
    t_range.__set_lower_bound(lower_bound);

    // Upper bound with empty values list
    TTuple upper_bound;
    t_range.__set_upper_bound(upper_bound);

    t_range.__set_lower_bound_included(true);
    t_range.__set_upper_bound_included(false);

    auto res = TabletRangeHelper::convert_t_range_to_pb_range(t_range);
    ASSERT_OK(res.status());
    TabletRangePB pb_range = res.value();

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

    TTuple lower_bound;

    // DECIMAL type
    TVariant decimal_value;
    TTypeDesc decimal_type;
    decimal_type.types.resize(1);
    decimal_type.__isset.types = true;
    decimal_type.types[0].type = TTypeNodeType::SCALAR;
    decimal_type.types[0].scalar_type.__set_type(TPrimitiveType::DECIMAL128);
    decimal_type.types[0].scalar_type.__set_precision(10);
    decimal_type.types[0].scalar_type.__set_scale(2);
    decimal_type.types[0].__isset.scalar_type = true;
    decimal_value.__set_type(decimal_type);
    decimal_value.__set_value("123.45");
    decimal_value.__set_variant_type(TVariantType::NORMAL_VALUE);
    lower_bound.values.push_back(decimal_value);

    // DATETIME type
    TVariant datetime_value;
    TTypeDesc datetime_type;
    datetime_type.types.resize(1);
    datetime_type.__isset.types = true;
    datetime_type.types[0].type = TTypeNodeType::SCALAR;
    datetime_type.types[0].scalar_type.__set_type(TPrimitiveType::DATETIME);
    datetime_type.types[0].__isset.scalar_type = true;
    datetime_value.__set_type(datetime_type);
    datetime_value.__set_value("2024-01-01 12:00:00");
    datetime_value.__set_variant_type(TVariantType::NORMAL_VALUE);
    lower_bound.values.push_back(datetime_value);

    t_range.__set_lower_bound(lower_bound);

    auto res = TabletRangeHelper::convert_t_range_to_pb_range(t_range);
    ASSERT_OK(res.status());
    TabletRangePB pb_range = res.value();

    ASSERT_TRUE(pb_range.has_lower_bound());
    ASSERT_EQ(2, pb_range.lower_bound().values_size());

    // Verify DECIMAL type conversion
    const auto& pb_decimal_val = pb_range.lower_bound().values(0);
    ASSERT_EQ("123.45", pb_decimal_val.value());
    ASSERT_EQ(TPrimitiveType::DECIMAL128, pb_decimal_val.type().types(0).scalar_type().type());
    ASSERT_EQ(10, pb_decimal_val.type().types(0).scalar_type().precision());
    ASSERT_EQ(2, pb_decimal_val.type().types(0).scalar_type().scale());

    // Verify DATETIME type conversion
    const auto& pb_datetime_val = pb_range.lower_bound().values(1);
    ASSERT_EQ("2024-01-01 12:00:00", pb_datetime_val.value());
    ASSERT_EQ(TPrimitiveType::DATETIME, pb_datetime_val.type().types(0).scalar_type().type());
}

// MINIMUM/MAXIMUM variants should be rejected — FE must map them to NULL_VALUE.
TEST(TabletRangeHelperTest, test_convert_t_range_to_pb_range_rejects_min_max) {
    TTypeDesc type_desc;
    type_desc.types.resize(1);
    type_desc.__isset.types = true;
    type_desc.types[0].type = TTypeNodeType::SCALAR;
    type_desc.types[0].scalar_type.__set_type(TPrimitiveType::INT);
    type_desc.types[0].__isset.scalar_type = true;

    {
        TTabletRange t_range;
        TTuple lower_bound;
        TVariant min_val;
        min_val.__set_type(type_desc);
        min_val.__set_variant_type(TVariantType::MINIMUM);
        lower_bound.values.push_back(min_val);
        t_range.__set_lower_bound(lower_bound);

        auto res = TabletRangeHelper::convert_t_range_to_pb_range(t_range);
        ASSERT_FALSE(res.ok());
        ASSERT_TRUE(res.status().is_invalid_argument());
        ASSERT_THAT(res.status().to_string(),
                    testing::HasSubstr("MINIMUM/MAXIMUM variant is not supported in tablet range"));
    }

    {
        TTabletRange t_range;
        TTuple upper_bound;
        TVariant max_val;
        max_val.__set_type(type_desc);
        max_val.__set_variant_type(TVariantType::MAXIMUM);
        upper_bound.values.push_back(max_val);
        t_range.__set_upper_bound(upper_bound);

        auto res = TabletRangeHelper::convert_t_range_to_pb_range(t_range);
        ASSERT_FALSE(res.ok());
        ASSERT_TRUE(res.status().is_invalid_argument());
        ASSERT_THAT(res.status().to_string(),
                    testing::HasSubstr("MINIMUM/MAXIMUM variant is not supported in tablet range"));
    }
}

TEST(TabletRangeHelperTest, test_convert_t_range_to_pb_range_missing_fields) {
    TTypeDesc type_int;
    type_int.types.resize(1);
    type_int.__isset.types = true;
    type_int.types[0].type = TTypeNodeType::SCALAR;
    type_int.types[0].scalar_type.__set_type(TPrimitiveType::INT);
    type_int.types[0].__isset.scalar_type = true;

    {
        // Missing type
        TTabletRange t_range;
        TTuple lower_bound;
        TVariant value;
        value.__set_value("100");
        value.__set_variant_type(TVariantType::NORMAL_VALUE);
        lower_bound.values.push_back(value);
        t_range.__set_lower_bound(lower_bound);
        auto res = TabletRangeHelper::convert_t_range_to_pb_range(t_range);
        ASSERT_FALSE(res.ok());
        ASSERT_TRUE(res.status().is_invalid_argument());
        ASSERT_THAT(res.status().to_string(), testing::HasSubstr("TVariant type is required"));
    }

    {
        // Missing value
        TTabletRange t_range;
        TTuple lower_bound;
        TVariant value;
        value.__set_type(type_int);
        value.__set_variant_type(TVariantType::NORMAL_VALUE);
        lower_bound.values.push_back(value);
        t_range.__set_lower_bound(lower_bound);
        auto res = TabletRangeHelper::convert_t_range_to_pb_range(t_range);
        ASSERT_FALSE(res.ok());
        ASSERT_TRUE(res.status().is_invalid_argument());
        ASSERT_THAT(res.status().to_string(),
                    testing::HasSubstr("TVariant value is required for NORMAL_VALUE variant"));
    }

    {
        // Missing variant_type
        TTabletRange t_range;
        TTuple lower_bound;
        TVariant value;
        value.__set_type(type_int);
        value.__set_value("100");
        lower_bound.values.push_back(value);
        t_range.__set_lower_bound(lower_bound);
        auto res = TabletRangeHelper::convert_t_range_to_pb_range(t_range);
        ASSERT_FALSE(res.ok());
        ASSERT_TRUE(res.status().is_invalid_argument());
        ASSERT_THAT(res.status().to_string(), testing::HasSubstr("TVariant variant_type is required"));
    }
}

TEST(TabletRangeHelperTest, test_convert_t_range_to_pb_range_invalid_type) {
    TTabletRange t_range;
    TTuple lower_bound;
    TVariant value;
    TTypeDesc type_desc;
    // type_desc.types is empty
    type_desc.__isset.types = true;
    value.__set_type(type_desc);
    value.__set_value("");
    value.__set_variant_type(TVariantType::NORMAL_VALUE);
    lower_bound.values.push_back(value);
    t_range.__set_lower_bound(lower_bound);

    auto res = TabletRangeHelper::convert_t_range_to_pb_range(t_range);
    ASSERT_FALSE(res.ok());
    ASSERT_TRUE(res.status().is_invalid_argument());
    ASSERT_THAT(res.status().to_string(), testing::HasSubstr("TVariant type is set but types list is empty"));
}

// NULL on a non-nullable PK column is treated as type-minimum (MIN sentinel from FE).
TEST(TabletRangeHelperTest, test_sst_seek_range_null_as_min_on_non_nullable_pk) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(PRIMARY_KEYS);
    schema_pb.set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);

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

    schema_pb.clear_sort_key_idxes();
    schema_pb.add_sort_key_idxes(0);
    schema_pb.add_sort_key_idxes(1);
    auto tablet_schema = TabletSchema::create(schema_pb);

    TypeDescriptor type_int(TYPE_INT);

    // Build range [(100, NULL), (200, NULL)) where NULL represents MIN from FE.
    TabletRangePB range_pb;
    {
        auto* lower = range_pb.mutable_lower_bound();
        auto* v0 = lower->add_values();
        v0->mutable_type()->CopyFrom(type_int.to_protobuf());
        v0->set_value("100");
        v0->set_variant_type(VariantTypePB::NORMAL_VALUE);
        auto* v1 = lower->add_values();
        v1->mutable_type()->CopyFrom(type_int.to_protobuf());
        v1->set_variant_type(VariantTypePB::NULL_VALUE);
        range_pb.set_lower_bound_included(true);
    }
    {
        auto* upper = range_pb.mutable_upper_bound();
        auto* v0 = upper->add_values();
        v0->mutable_type()->CopyFrom(type_int.to_protobuf());
        v0->set_value("200");
        v0->set_variant_type(VariantTypePB::NORMAL_VALUE);
        auto* v1 = upper->add_values();
        v1->mutable_type()->CopyFrom(type_int.to_protobuf());
        v1->set_variant_type(VariantTypePB::NULL_VALUE);
        range_pb.set_upper_bound_included(false);
    }

    // Should succeed — NULL on non-nullable column is filled with type-min.
    ASSIGN_OR_ABORT(auto sst_seek_range, TabletRangeHelper::create_sst_seek_range_from(range_pb, tablet_schema));

    // Build expected: encode (100, INT_MIN) and (200, INT_MIN).
    std::vector<ColumnId> pk_columns = {0, 1};
    auto pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);

    auto encode_key = [&](int32_t v1, int32_t v2) {
        auto chunk = std::make_unique<Chunk>();
        auto col1 = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false);
        col1->append_datum(Datum(v1));
        chunk->append_column(std::move(col1), (SlotId)0);
        auto col2 = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false);
        col2->append_datum(Datum(v2));
        chunk->append_column(std::move(col2), (SlotId)1);

        MutableColumnPtr pk_column;
        EXPECT_OK(
                PrimaryKeyEncoder::create_column(pkey_schema, &pk_column, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2));
        PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, 1, pk_column.get(),
                                  PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
        if (pk_column->is_binary()) {
            return down_cast<BinaryColumn*>(pk_column.get())->get_slice(0).to_string();
        } else {
            RawDataVisitor visitor;
            EXPECT_OK(pk_column->accept(&visitor));
            return std::string(reinterpret_cast<const char*>(visitor.result()), pk_column->type_size());
        }
    };

    ASSERT_EQ(sst_seek_range.seek_key, encode_key(100, std::numeric_limits<int32_t>::lowest()));
    ASSERT_EQ(sst_seek_range.stop_key, encode_key(200, std::numeric_limits<int32_t>::lowest()));
}

// Exercise datum_from_type_min() for every PK-supported type.
// Each case builds a 1-column PK schema, puts NULL in the range bound,
// and verifies that create_sst_seek_range_from succeeds (NULL → type-min fill).
TEST(TabletRangeHelperTest, test_sst_seek_range_null_as_min_all_pk_types) {
    struct TypeCase {
        const char* type_name;
        LogicalType logical_type;
    };
    // All types from APPLY_FOR_ALL_PK_SUPPORT_TYPE (logical_type_infra.h).
    std::vector<TypeCase> cases = {
            {"BOOLEAN", TYPE_BOOLEAN}, {"TINYINT", TYPE_TINYINT},   {"SMALLINT", TYPE_SMALLINT},
            {"INT", TYPE_INT},         {"BIGINT", TYPE_BIGINT},     {"LARGEINT", TYPE_LARGEINT},
            {"DATE", TYPE_DATE},       {"DATETIME", TYPE_DATETIME}, {"VARCHAR", TYPE_VARCHAR},
    };

    for (const auto& tc : cases) {
        SCOPED_TRACE(tc.type_name);

        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(PRIMARY_KEYS);
        schema_pb.set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
        auto c0 = schema_pb.add_column();
        c0->set_name("c0");
        c0->set_type(tc.type_name);
        c0->set_is_key(true);
        c0->set_is_nullable(false);
        schema_pb.add_sort_key_idxes(0);
        auto tablet_schema = TabletSchema::create(schema_pb);

        TabletRangePB range_pb;
        auto* lower = range_pb.mutable_lower_bound();
        auto* v0 = lower->add_values();
        TypeDescriptor type_desc(tc.logical_type);
        v0->mutable_type()->CopyFrom(type_desc.to_protobuf());
        v0->set_variant_type(VariantTypePB::NULL_VALUE);
        range_pb.set_lower_bound_included(true);

        auto res = TabletRangeHelper::create_sst_seek_range_from(range_pb, tablet_schema);
        ASSERT_TRUE(res.ok()) << "type=" << tc.type_name << " error=" << res.status().to_string();
    }
}

// Unsupported type should return NotSupported, not crash.
TEST(TabletRangeHelperTest, test_sst_seek_range_null_as_min_unsupported_type) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(PRIMARY_KEYS);
    schema_pb.set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    auto c0 = schema_pb.add_column();
    c0->set_name("c0");
    c0->set_type("FLOAT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);
    schema_pb.add_sort_key_idxes(0);
    auto tablet_schema = TabletSchema::create(schema_pb);

    TabletRangePB range_pb;
    auto* lower = range_pb.mutable_lower_bound();
    auto* v0 = lower->add_values();
    TypeDescriptor type_desc(TYPE_FLOAT);
    v0->mutable_type()->CopyFrom(type_desc.to_protobuf());
    v0->set_variant_type(VariantTypePB::NULL_VALUE);
    range_pb.set_lower_bound_included(true);

    auto res = TabletRangeHelper::create_sst_seek_range_from(range_pb, tablet_schema);
    ASSERT_FALSE(res.ok());
    ASSERT_TRUE(res.status().is_not_supported());
    ASSERT_THAT(res.status().to_string(), testing::HasSubstr("unsupported type for PK min datum"));
}

// =============================================================================
// validate_new_tablet_ranges: structural (schema-free) validator used by the
// external-boundaries path. These tests exercise every rejection branch directly so coverage
// does not rely on routing through compute_split_ranges_from_external_boundaries.
// =============================================================================

namespace {

// Builds a closed-open [lower, upper) range with INT bounds. nullopt skips
// the corresponding bound — used to construct half-bounded edge ranges.
static TabletRangePB make_int_range_pb(std::optional<int32_t> lower, std::optional<int32_t> upper) {
    TabletRangePB r;
    if (lower.has_value()) {
        *r.mutable_lower_bound() = make_int_tuple_pb(*lower);
        r.set_lower_bound_included(true);
    }
    if (upper.has_value()) {
        *r.mutable_upper_bound() = make_int_tuple_pb(*upper);
        r.set_upper_bound_included(false);
    }
    return r;
}

static RepeatedPtrField<TabletRangePB> as_pb_list(std::initializer_list<TabletRangePB> ranges) {
    RepeatedPtrField<TabletRangePB> out;
    for (const auto& r : ranges) {
        *out.Add() = r;
    }
    return out;
}

} // namespace

TEST(TabletRangeHelperTest, validate_new_tablet_ranges_empty_list_rejected) {
    TabletRangePB parent = make_int_range_pb(0, 100);
    auto ranges = as_pb_list({});
    auto s = TabletRangeHelper::validate_new_tablet_ranges(parent, ranges);
    ASSERT_FALSE(s.ok());
    ASSERT_TRUE(s.is_invalid_argument()) << s;
    ASSERT_THAT(s.to_string(), testing::HasSubstr("new_tablet_ranges is empty"));
}

TEST(TabletRangeHelperTest, validate_new_tablet_ranges_zero_width_range_rejected) {
    TabletRangePB parent = make_int_range_pb(0, 100);
    // Synthesize a single range whose lower and upper bounds are byte-equal.
    TabletRangePB r;
    *r.mutable_lower_bound() = make_int_tuple_pb(50);
    *r.mutable_upper_bound() = make_int_tuple_pb(50);
    r.set_lower_bound_included(true);
    r.set_upper_bound_included(false);
    auto ranges = as_pb_list({r});
    auto s = TabletRangeHelper::validate_new_tablet_ranges(parent, ranges);
    ASSERT_FALSE(s.ok());
    ASSERT_THAT(s.to_string(), testing::HasSubstr("zero-width"));
}

TEST(TabletRangeHelperTest, validate_new_tablet_ranges_first_lower_must_be_inclusive) {
    TabletRangePB parent = make_int_range_pb(0, 100);
    TabletRangePB r = make_int_range_pb(0, 100);
    r.set_lower_bound_included(false); // Violation: first.lower must be inclusive when set.
    auto ranges = as_pb_list({r});
    auto s = TabletRangeHelper::validate_new_tablet_ranges(parent, ranges);
    ASSERT_FALSE(s.ok());
    // The per-range guard (validate_tablet_range) fires before the positional
    // "first.lower_bound must be inclusive" check and emits the generic message.
    ASSERT_THAT(s.to_string(), testing::HasSubstr("Lower bound is exclusive"));
}

TEST(TabletRangeHelperTest, validate_new_tablet_ranges_first_lower_mismatch_rejected) {
    TabletRangePB parent = make_int_range_pb(0, 100);
    TabletRangePB r = make_int_range_pb(10, 100); // first.lower=10 != parent.lower=0
    auto ranges = as_pb_list({r});
    auto s = TabletRangeHelper::validate_new_tablet_ranges(parent, ranges);
    ASSERT_FALSE(s.ok());
    ASSERT_THAT(s.to_string(), testing::HasSubstr("first.lower_bound != old_tablet_range.lower_bound"));
}

TEST(TabletRangeHelperTest, validate_new_tablet_ranges_last_upper_mismatch_rejected) {
    TabletRangePB parent = make_int_range_pb(0, 100);
    TabletRangePB r = make_int_range_pb(0, 99); // last.upper=99 != parent.upper=100
    auto ranges = as_pb_list({r});
    auto s = TabletRangeHelper::validate_new_tablet_ranges(parent, ranges);
    ASSERT_FALSE(s.ok());
    ASSERT_THAT(s.to_string(), testing::HasSubstr("last.upper_bound != old_tablet_range.upper_bound"));
}

TEST(TabletRangeHelperTest, validate_new_tablet_ranges_last_upper_must_be_exclusive) {
    TabletRangePB parent = make_int_range_pb(0, 100);
    TabletRangePB r = make_int_range_pb(0, 100);
    r.set_upper_bound_included(true); // Violation: last.upper must be exclusive when set.
    auto ranges = as_pb_list({r});
    auto s = TabletRangeHelper::validate_new_tablet_ranges(parent, ranges);
    ASSERT_FALSE(s.ok());
    // The per-range guard (validate_tablet_range) fires before the positional
    // "last.upper_bound must be exclusive" check and emits the generic message.
    ASSERT_THAT(s.to_string(), testing::HasSubstr("Upper bound is inclusive"));
}

TEST(TabletRangeHelperTest, validate_new_tablet_ranges_interior_gap_missing_bound_rejected) {
    TabletRangePB parent = make_int_range_pb(0, 100);
    TabletRangePB first = make_int_range_pb(0, 50);
    TabletRangePB second = make_int_range_pb(std::nullopt, 100); // missing lower
    auto ranges = as_pb_list({first, second});
    auto s = TabletRangeHelper::validate_new_tablet_ranges(parent, ranges);
    ASSERT_FALSE(s.ok());
    ASSERT_THAT(s.to_string(), testing::HasSubstr("gap at boundary 0"));
}

TEST(TabletRangeHelperTest, validate_new_tablet_ranges_interior_bound_flags_rejected) {
    TabletRangePB parent = make_int_range_pb(0, 100);
    TabletRangePB first = make_int_range_pb(0, 50);
    first.set_upper_bound_included(true); // Violation: left boundary must be exclusive.
    TabletRangePB second = make_int_range_pb(50, 100);
    auto ranges = as_pb_list({first, second});
    auto s = TabletRangeHelper::validate_new_tablet_ranges(parent, ranges);
    ASSERT_FALSE(s.ok());
    // The per-range guard (validate_tablet_range) fires before the positional
    // "invalid bound flags" check and emits the generic message.
    ASSERT_THAT(s.to_string(), testing::HasSubstr("Upper bound is inclusive"));
}

TEST(TabletRangeHelperTest, validate_new_tablet_ranges_adjacency_gap_rejected) {
    TabletRangePB parent = make_int_range_pb(0, 100);
    TabletRangePB first = make_int_range_pb(0, 40);
    TabletRangePB second = make_int_range_pb(50, 100); // gap: first.upper=40 != second.lower=50
    auto ranges = as_pb_list({first, second});
    auto s = TabletRangeHelper::validate_new_tablet_ranges(parent, ranges);
    ASSERT_FALSE(s.ok());
    ASSERT_THAT(s.to_string(), testing::HasSubstr("gap or overlap at boundary 0"));
}

TEST(TabletRangeHelperTest, validate_new_tablet_ranges_happy_path_two_adjacent_ranges) {
    TabletRangePB parent = make_int_range_pb(0, 100);
    TabletRangePB first = make_int_range_pb(0, 50);
    TabletRangePB second = make_int_range_pb(50, 100);
    auto ranges = as_pb_list({first, second});
    auto s = TabletRangeHelper::validate_new_tablet_ranges(parent, ranges);
    ASSERT_TRUE(s.ok()) << s;
}

TEST(TabletRangeHelperTest, validate_new_tablet_ranges_happy_path_unbounded_parent) {
    // Parent is (-inf, +inf); 3-way split with explicit interior boundaries.
    TabletRangePB parent;
    auto ranges = as_pb_list(
            {make_int_range_pb(std::nullopt, 50), make_int_range_pb(50, 100), make_int_range_pb(100, std::nullopt)});
    auto s = TabletRangeHelper::validate_new_tablet_ranges(parent, ranges);
    ASSERT_TRUE(s.ok()) << s;
}

} // namespace starrocks::lake
