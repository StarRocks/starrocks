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

#include "storage/query/range_group_task.h"

#include <gtest/gtest.h>

#include <limits>

#include "gen_cpp/PlanNodes_types.h"
#include "testutil/assert.h"

namespace starrocks {
namespace {

TabletSchemaPB schema_pb() {
    TabletSchemaPB pb;
    pb.set_keys_type(DUP_KEYS);
    pb.set_num_short_key_columns(1);
    for (int i = 0; i < 4; ++i) {
        auto* col = pb.add_column();
        col->set_unique_id(100 + i);
        col->set_name("c" + std::to_string(i));
        col->set_type("BIGINT");
        col->set_is_key(i < 3);
        col->set_is_nullable(i == 3); // Nullable DISTINCT argument is allowed.
        col->set_aggregation("NONE");
        if (i < 3) pb.add_sort_key_idxes(i);
    }
    return pb;
}

TTuple tuple(std::initializer_list<int64_t> values) {
    TTuple result;
    for (auto v : values) {
        TScalarType scalar;
        scalar.__set_type(TPrimitiveType::BIGINT);
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        node.__set_scalar_type(scalar);
        TTypeDesc type;
        type.__set_types({node});
        type.__isset.types = true; // Match Thrift read(); the generated setter does not mark presence.
        TVariant value;
        value.__set_type(type);
        value.__set_variant_type(TVariantType::NORMAL_VALUE);
        value.__set_value(std::to_string(v));
        result.values.push_back(value);
    }
    return result;
}

TGroupedRangeTask full_task() {
    TGroupedRangeTask wire;
    wire.__set_group_id(0);
    wire.__set_key_range(TTabletRange{});
    wire.__set_sort_key_unique_ids({100, 101, 102});
    return wire;
}

TGroupedRangeTask finite_task() {
    auto wire = full_task();
    wire.key_range.__set_lower_bound(tuple({-1, 2, 3}));
    wire.key_range.__set_upper_bound(tuple({4, 5, 6}));
    wire.key_range.__set_lower_bound_included(true);
    wire.key_range.__set_upper_bound_included(false);
    return wire;
}

} // namespace

TEST(RangeGroupTaskTest, FullRangeAndFiniteClip) {
    auto schema = TabletSchema::create(schema_pb());
    ASSIGN_OR_ABORT(auto full, decode_range_group_task(full_task()));
    ASSIGN_OR_ABORT(auto full_seek, decode_range_group_seek(*full, schema));
    EXPECT_TRUE(full_seek.all_range());
    ASSIGN_OR_ABORT(auto finite, decode_range_group_task(finite_task()));
    ASSIGN_OR_ABORT(auto seek, decode_range_group_seek(*finite, schema));
    EXPECT_TRUE(seek.inclusive_lower());
    EXPECT_FALSE(seek.inclusive_upper());
    ASSERT_EQ(3, seek.lower().columns());
    ASSERT_EQ(3, seek.upper().columns());
    EXPECT_EQ(-1, seek.lower().get(0).get_int64());
    EXPECT_EQ(2, seek.lower().get(1).get_int64());
    EXPECT_EQ(3, seek.lower().get(2).get_int64());
    EXPECT_EQ(4, seek.upper().get(0).get_int64());
    EXPECT_EQ(5, seek.upper().get(1).get_int64());
    EXPECT_EQ(6, seek.upper().get(2).get_int64());
    EXPECT_EQ(0, finite->group_id);
}

TEST(RangeGroupTaskTest, BigintExtremesAndOneSidedInfinity) {
    auto wire = finite_task();
    wire.key_range.__set_lower_bound(tuple({std::numeric_limits<int64_t>::min(), 0, 0}));
    wire.key_range.__set_upper_bound(tuple({std::numeric_limits<int64_t>::max(), 0, 0}));
    auto schema = TabletSchema::create(schema_pb());
    ASSIGN_OR_ABORT(auto task, decode_range_group_task(wire));
    ASSIGN_OR_ABORT(auto seek, decode_range_group_seek(*task, schema));
    EXPECT_EQ(std::numeric_limits<int64_t>::min(), seek.lower().get(0).get_int64());
    EXPECT_EQ(std::numeric_limits<int64_t>::max(), seek.upper().get(0).get_int64());
    wire.key_range.__isset.lower_bound = false;
    ASSIGN_OR_ABORT(task, decode_range_group_task(wire));
    ASSIGN_OR_ABORT(seek, decode_range_group_seek(*task, schema));
    EXPECT_TRUE(seek.lower().empty());
    EXPECT_EQ(std::numeric_limits<int64_t>::max(), seek.upper().get(0).get_int64());
    wire.key_range.__set_lower_bound(tuple({0, 0, 0}));
    wire.key_range.__isset.upper_bound = false;
    ASSIGN_OR_ABORT(task, decode_range_group_task(wire));
    ASSIGN_OR_ABORT(seek, decode_range_group_seek(*task, schema));
    EXPECT_EQ(0, seek.lower().get(0).get_int64());
    EXPECT_TRUE(seek.upper().empty());
}

TEST(RangeGroupTaskTest, CompareAllThreeBigintPositions) {
    auto wire = finite_task();
    wire.key_range.__set_lower_bound(tuple({7, 8, 9}));
    wire.key_range.__set_upper_bound(tuple({7, 8, 10}));
    EXPECT_TRUE(decode_range_group_task(wire).ok());
    wire.key_range.__set_upper_bound(tuple({7, 8, 8}));
    EXPECT_FALSE(decode_range_group_task(wire).ok());
    wire.key_range.__set_upper_bound(tuple({7, 9, std::numeric_limits<int64_t>::min()}));
    EXPECT_TRUE(decode_range_group_task(wire).ok());
}

TEST(RangeGroupTaskTest, RequiredFieldsAndNonnegativeGroup) {
    auto wire = full_task();
    wire.__isset.group_id = false;
    EXPECT_FALSE(decode_range_group_task(wire).ok());
    wire.__set_group_id(-1);
    EXPECT_FALSE(decode_range_group_task(wire).ok());
    wire = full_task();
    wire.__isset.key_range = false;
    EXPECT_FALSE(decode_range_group_task(wire).ok());
    wire = full_task();
    wire.__isset.sort_key_unique_ids = false;
    EXPECT_FALSE(decode_range_group_task(wire).ok());
}

TEST(RangeGroupTaskTest, UniqueIdArityAndDuplicates) {
    for (const std::vector<int32_t>& ids :
         {std::vector<int32_t>{}, {100, 101}, {100, 101, 102, 103}, {100, 100, 102}}) {
        auto wire = full_task();
        wire.__set_sort_key_unique_ids(ids);
        EXPECT_FALSE(decode_range_group_task(wire).ok());
    }
}

TEST(RangeGroupTaskTest, RejectMissingAndWrongHalfOpenFlags) {
    for (bool lower : {false, true}) {
        auto wire = finite_task();
        if (lower)
            wire.key_range.__isset.lower_bound_included = false;
        else
            wire.key_range.__isset.upper_bound_included = false;
        EXPECT_FALSE(decode_range_group_task(wire).ok());
        wire = finite_task();
        if (lower)
            wire.key_range.__set_lower_bound_included(false);
        else
            wire.key_range.__set_upper_bound_included(true);
        EXPECT_FALSE(decode_range_group_task(wire).ok());
    }
}

TEST(RangeGroupTaskTest, RejectEqualAndReversedBounds) {
    auto wire = finite_task();
    wire.key_range.__set_upper_bound(wire.key_range.lower_bound);
    EXPECT_FALSE(decode_range_group_task(wire).ok());
    wire.key_range.__set_upper_bound(tuple({-2, 9, 9}));
    EXPECT_FALSE(decode_range_group_task(wire).ok());
}

TEST(RangeGroupTaskTest, RejectPartialAndOversizedBounds) {
    for (auto bound : {tuple({}), tuple({0, 1}), tuple({0, 1, 2, 3})}) {
        auto wire = finite_task();
        wire.key_range.__set_lower_bound(bound);
        EXPECT_FALSE(decode_range_group_task(wire).ok());
        wire = finite_task();
        wire.key_range.__set_upper_bound(bound);
        EXPECT_FALSE(decode_range_group_task(wire).ok());
    }
}

TEST(RangeGroupTaskTest, RejectFiniteSentinelsAndWrongPrimitive) {
    for (auto variant : {TVariantType::NULL_VALUE, TVariantType::MINIMUM, TVariantType::MAXIMUM}) {
        for (bool lower : {false, true}) {
            auto wire = finite_task();
            auto& value = (lower ? wire.key_range.lower_bound : wire.key_range.upper_bound).values[0];
            value.__set_variant_type(variant);
            EXPECT_FALSE(decode_range_group_task(wire).ok());
        }
    }
    auto wire = finite_task();
    wire.key_range.lower_bound.values[0].type.types[0].scalar_type.__set_type(TPrimitiveType::INT);
    EXPECT_FALSE(decode_range_group_task(wire).ok());
}

TEST(RangeGroupTaskTest, RejectMissingValueMetadataAndInvalidBigint) {
    for (int field = 0; field < 4; ++field) {
        auto wire = finite_task();
        auto& value = wire.key_range.lower_bound.values[0];
        if (field == 0) value.__isset.type = false;
        if (field == 1) value.__isset.variant_type = false;
        if (field == 2) value.__isset.value = false;
        if (field == 3) value.__set_value("9223372036854775808");
        EXPECT_FALSE(decode_range_group_task(wire).ok());
    }
}

TEST(RangeGroupTaskTest, RejectInvalidOneSidedBigintLiterals) {
    for (const auto& invalid : {"", "garbage", "1tail", "9223372036854775808", "-9223372036854775809"}) {
        for (bool lower : {false, true}) {
            auto wire = finite_task();
            if (lower)
                wire.key_range.__isset.upper_bound = false;
            else
                wire.key_range.__isset.lower_bound = false;
            auto& value = (lower ? wire.key_range.lower_bound : wire.key_range.upper_bound).values[0];
            value.__set_value(invalid);
            EXPECT_FALSE(decode_range_group_task(wire).ok()) << invalid;
        }
    }
}

TEST(RangeGroupTaskTest, RejectMalformedTypeTrees) {
    for (int shape = 0; shape < 4; ++shape) {
        auto wire = finite_task();
        auto& type = wire.key_range.lower_bound.values[0].type;
        if (shape == 0) type.types.clear();
        if (shape == 1) type.__isset.types = false;
        if (shape == 2) type.types[0].__isset.scalar_type = false;
        if (shape == 3) type.types[0].__set_type(TTypeNodeType::ARRAY);
        EXPECT_FALSE(decode_range_group_task(wire).ok()) << shape;
    }
}

TEST(RangeGroupTaskTest, ValidateSchemaAndArchivedUniqueIdOrder) {
    ASSIGN_OR_ABORT(auto task, decode_range_group_task(finite_task()));
    ASSERT_OK(validate_range_group_schema(*task, *TabletSchema::create(schema_pb())));
    for (int change = 0; change < 7; ++change) {
        auto pb = schema_pb();
        if (change == 0) pb.set_keys_type(PRIMARY_KEYS);
        if (change == 1) pb.mutable_column(1)->set_is_nullable(true);
        if (change == 2) pb.mutable_column(1)->set_type("INT");
        if (change == 3) pb.mutable_column(1)->set_unique_id(999);
        if (change == 4) {
            pb.set_sort_key_idxes(0, 1);
            pb.set_sort_key_idxes(1, 0);
        }
        if (change == 5) pb.mutable_sort_key_idxes()->RemoveLast();
        if (change == 6) pb.add_sort_key_idxes(3);
        auto archived = TabletSchema::create(pb);
        EXPECT_FALSE(validate_range_group_schema(*task, *archived).ok()) << change;
        EXPECT_FALSE(decode_range_group_seek(*task, archived).ok()) << change;
    }
    EXPECT_FALSE(decode_range_group_seek(*task, nullptr).ok());
}

// Hand-built immutable tasks must retain the same typed contract as wire-decoded tasks.
TEST(RangeGroupTaskTest, RejectMalformedProtoClipType) {
    ASSIGN_OR_ABORT(auto decoded, decode_range_group_task(finite_task()));
    auto schema = TabletSchema::create(schema_pb());
    for (int change = 0; change < 3; ++change) {
        RangeGroupTask task = *decoded;
        auto* value = task.key_range.mutable_lower_bound()->mutable_values(0);
        if (change == 0) value->clear_type();
        if (change == 1) value->mutable_type()->clear_types();
        if (change == 2) value->mutable_type()->mutable_types(0)->mutable_scalar_type()->set_type(TPrimitiveType::INT);
        EXPECT_FALSE(validate_range_group_schema(task, *schema).ok()) << change;
    }
}

} // namespace starrocks
