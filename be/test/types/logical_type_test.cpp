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

#include "types/logical_type.h"

#include <gtest/gtest.h>

#include <set>
#include <utility>

#include "types/olap_type_infra.h"

namespace starrocks {

TEST(LogicalTypeTest, StringToLogicalType) {
    EXPECT_EQ(TYPE_INT, string_to_logical_type("INT"));
    EXPECT_EQ(TYPE_INT, string_to_logical_type("int"));
    EXPECT_EQ(TYPE_DATE, string_to_logical_type("DATE"));
    EXPECT_EQ(TYPE_DATE, string_to_logical_type("DATE_V2"));
    EXPECT_EQ(TYPE_VARBINARY, string_to_logical_type("VARBINARY"));
    EXPECT_EQ(TYPE_UNKNOWN, string_to_logical_type("not_a_type"));
}

TEST(LogicalTypeTest, LogicalTypeToString) {
    EXPECT_STREQ("INT", logical_type_to_string(TYPE_INT));
    EXPECT_STREQ("DATE_V2", logical_type_to_string(TYPE_DATE));
    EXPECT_STREQ("TIMESTAMP", logical_type_to_string(TYPE_DATETIME));
    EXPECT_STREQ("UNKNOWN", logical_type_to_string(TYPE_UNKNOWN));
}

TEST(LogicalTypeTest, TypePredicates) {
    EXPECT_TRUE(is_integer_type(TYPE_TINYINT));
    EXPECT_TRUE(is_integer_type(TYPE_BIGINT));
    EXPECT_FALSE(is_integer_type(TYPE_DOUBLE));

    EXPECT_TRUE(is_string_type(TYPE_CHAR));
    EXPECT_TRUE(is_string_type(TYPE_VARCHAR));
    EXPECT_FALSE(is_string_type(TYPE_BINARY));

    EXPECT_TRUE(is_binary_type(TYPE_BINARY));
    EXPECT_TRUE(is_binary_type(TYPE_VARBINARY));
    EXPECT_FALSE(is_binary_type(TYPE_VARCHAR));
}

namespace {

// Materializes the full membership of a compile-time type guard, so a test can assert the exact set
// instead of spot-checking a few members and missing the ones that were accidentally let in.
template <template <LogicalType> class Guard, size_t... Is>
std::set<LogicalType> guard_members(std::index_sequence<Is...>) {
    std::set<LogicalType> members;
    auto add = [&members](LogicalType type, bool in_guard) {
        if (in_guard) {
            members.insert(type);
        }
    };
    (add(static_cast<LogicalType>(Is), Guard<static_cast<LogicalType>(Is)>::value), ...);
    return members;
}

template <template <LogicalType> class Guard>
std::set<LogicalType> guard_members() {
    return guard_members<Guard>(std::make_index_sequence<TYPE_MAX_VALUE>{});
}

} // namespace

// Axis 1 of the taxonomy in logical_type.h: how values are laid out in a Column.
TEST(LogicalTypeTest, PhysicalShapeGuards) {
    EXPECT_EQ((std::set<LogicalType>{TYPE_HLL, TYPE_OBJECT, TYPE_PERCENTILE, TYPE_JSON, TYPE_VARIANT}),
              guard_members<lt_is_object_family_struct>());
    EXPECT_EQ((std::set<LogicalType>{TYPE_ARRAY, TYPE_MAP, TYPE_STRUCT}), guard_members<lt_is_collection_struct>());

    // VARIANT joins the nested types here because a shredded VariantColumn keeps its rows outside
    // ObjectColumn::_pool, which is what ColumnViewer<TYPE_VARIANT> would read. JSON must stay out:
    // JsonColumn always stores rows in _pool, so the viewer/builder path remains valid and faster.
    EXPECT_EQ((std::set<LogicalType>{TYPE_ARRAY, TYPE_MAP, TYPE_STRUCT, TYPE_VARIANT}),
              guard_members<lt_is_row_wise_append_struct>());
    EXPECT_FALSE(lt_is_row_wise_append<TYPE_JSON>);
}

// Axis 2 of the taxonomy: what the type means to the user. The two guards partition the object
// family -- logical_type.h static_asserts that, this pins down which side each type lands on.
TEST(LogicalTypeTest, SemanticGuards) {
    EXPECT_EQ((std::set<LogicalType>{TYPE_JSON, TYPE_VARIANT}), guard_members<lt_is_semi_structured_struct>());
    EXPECT_EQ((std::set<LogicalType>{TYPE_HLL, TYPE_OBJECT, TYPE_PERCENTILE}),
              guard_members<lt_is_opaque_sketch_struct>());
}

// Semi-structured values have a per-replica physical encoding (flat JSON / variant shredding), so
// ordering their on-disk bytes is meaningless and a checksum mismatch would not mean divergence.
TEST(LogicalTypeTest, SemiStructuredTypesAreNotZoneMapKeysNorChecksummable) {
    for (auto type : {TYPE_JSON, TYPE_VARIANT}) {
        EXPECT_FALSE(is_zone_map_key_type(type)) << type;
        EXPECT_FALSE(is_support_checksum_type(type)) << type;
    }
    EXPECT_TRUE(is_zone_map_key_type(TYPE_INT));
    EXPECT_TRUE(is_support_checksum_type(TYPE_INT));
}

namespace {

struct DispatchLogicalTypeFunctor {
    template <LogicalType field_type>
    LogicalType operator()(LogicalType* observed) const {
        *observed = field_type;
        return field_type;
    }
};

struct DispatchStatusFunctor {
    template <LogicalType field_type>
    Status operator()(LogicalType* observed) const {
        *observed = field_type;
        return Status::OK();
    }
};

struct DispatchBoolFunctor {
    template <LogicalType field_type>
    bool operator()(LogicalType* observed) const {
        *observed = field_type;
        return true;
    }
};

} // namespace

TEST(LogicalTypeInfraTest, FieldTypeDispatchBasic) {
    LogicalType observed = TYPE_UNKNOWN;
    EXPECT_EQ(TYPE_INT, field_type_dispatch_basic(TYPE_INT, DispatchLogicalTypeFunctor(), &observed));
    EXPECT_EQ(TYPE_INT, observed);
}

TEST(LogicalTypeInfraTest, FieldTypeDispatchBloomFilterRejectsTinyInt) {
    LogicalType observed = TYPE_UNKNOWN;
    auto status = field_type_dispatch_bloomfilter(TYPE_TINYINT, DispatchStatusFunctor(), &observed);
    EXPECT_TRUE(status.is_not_supported());
    EXPECT_EQ(TYPE_UNKNOWN, observed);
}

TEST(LogicalTypeInfraTest, FieldTypeDispatchColumnPredicateUsesDefaultForUnsupportedType) {
    LogicalType observed = TYPE_UNKNOWN;
    EXPECT_FALSE(field_type_dispatch_column_predicate(TYPE_ARRAY, false, DispatchBoolFunctor(), &observed));
    EXPECT_EQ(TYPE_UNKNOWN, observed);

    EXPECT_TRUE(field_type_dispatch_column_predicate(TYPE_INT, false, DispatchBoolFunctor(), &observed));
    EXPECT_EQ(TYPE_INT, observed);
}

} // namespace starrocks
