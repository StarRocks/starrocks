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

#include "column/column_helper.h"

#include "base/testutil/assert.h"
#include "column/adaptive_nullable_column.h"
#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/geo_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "gtest/gtest.h"
#include "gutil/casts.h"
#include "testutil/column_test_helper.h"

namespace starrocks {

class ColumnHelperTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

protected:
    static TypeDescriptor geo_type(LogicalType primitive) {
        const bool geography = primitive == TYPE_GEOGRAPHY;
        return TypeDescriptor::create_geo_type(
                primitive,
                GeoTypeDescriptor{geography ? GEO_LOGICAL_TYPE_GEOGRAPHY : GEO_LOGICAL_TYPE_GEOMETRY,
                                  geography ? GEO_COORDINATE_SYSTEM_SPHERICAL : GEO_COORDINATE_SYSTEM_CARTESIAN,
                                  geography ? GEO_EDGE_ALGORITHM_KARNEY : GEO_EDGE_ALGORITHM_PLANAR, "custom:geo-crs",
                                  4326});
    }

    MutableColumnPtr create_column() {
        ColumnBuilder<TYPE_VARCHAR> builder(1);
        builder.append(Slice("v1"));
        return builder.build(false);
    }

    MutableColumnPtr create_nullable_column() {
        ColumnBuilder<TYPE_VARCHAR> builder(1);
        builder.append(Slice("v1"), true);
        return builder.build(false);
    }

    MutableColumnPtr create_const_column() {
        ColumnBuilder<TYPE_VARCHAR> builder(1);
        builder.append(Slice("v1"));
        return builder.build(true);
    }

    MutableColumnPtr create_only_null_column() {
        ColumnBuilder<TYPE_VARCHAR> builder(1);
        builder.append_null();
        return builder.build(true);
    }
};

TEST_F(ColumnHelperTest, cast_to_nullable_column) {
    auto col = ColumnHelper::cast_to_nullable_column(create_column());
    ASSERT_TRUE(col->is_nullable());
    ASSERT_FALSE(col->is_constant());

    col = ColumnHelper::cast_to_nullable_column(create_nullable_column());
    ASSERT_TRUE(col->is_nullable());
    ASSERT_FALSE(col->is_constant());
}

TEST_F(ColumnHelperTest, align_return_type) {
    auto nullable_column = create_nullable_column();
    auto not_null_column = create_column();
    auto col_size = not_null_column->size();
    nullable_column->append(*ColumnHelper::align_return_type(
            std::move(not_null_column), TypeDescriptor::from_logical_type(TYPE_VARCHAR), col_size, true));
    ASSERT_TRUE(nullable_column->is_nullable());
    ASSERT_FALSE(nullable_column->is_constant());
}

TEST_F(ColumnHelperTest, get_data_column_by_type) {
    auto column = create_nullable_column();
    const auto* data_column = ColumnHelper::get_data_column_by_type<TYPE_VARCHAR>(column.get());
    ASSERT_TRUE(data_column->is_binary());

    column = create_column();
    data_column = ColumnHelper::get_data_column_by_type<TYPE_VARCHAR>(column.get());
    ASSERT_TRUE(data_column->is_binary());

    column = create_const_column();
    data_column = ColumnHelper::get_data_column_by_type<TYPE_VARCHAR>(column.get());
    ASSERT_TRUE(data_column->is_binary());
}

TEST_F(ColumnHelperTest, get_null_column) {
    auto column = create_nullable_column();
    const auto* null_column = ColumnHelper::get_null_column(column.get());
    ASSERT_EQ(null_column->get_name(), "integral-1");

    column = create_column();
    null_column = ColumnHelper::get_null_column(column.get());
    ASSERT_TRUE(null_column == nullptr);

    column = create_const_column();
    null_column = ColumnHelper::get_null_column(column.get());
    ASSERT_EQ(null_column->get_name(), "integral-1");
}

// GetContainer::get_data
TEST_F(ColumnHelperTest, get_container_fixed_length) {
    auto col = ColumnTestHelper::build_column<int32_t>({1, 2, 3});
    auto data = GetContainer<TYPE_INT>::get_data(col.get());
    ASSERT_EQ(data.size(), 3);
    EXPECT_EQ(data[0], 1);
    EXPECT_EQ(data[1], 2);
    EXPECT_EQ(data[2], 3);
}

TEST_F(ColumnHelperTest, get_container_varchar) {
    auto col = ColumnTestHelper::build_column<Slice>({"hello", "world"});
    auto data = GetContainer<TYPE_VARCHAR>::get_data(col.get());
    ASSERT_EQ(data.size(), 2);
    EXPECT_EQ(data[0].to_string(), "hello");
    EXPECT_EQ(data[1].to_string(), "world");
}

TEST_F(ColumnHelperTest, get_container_large_binary) {
    auto col = LargeBinaryColumn::create();
    col->append_string("abc");
    col->append_string("def");
    auto data = GetContainer<TYPE_VARCHAR>::get_data(col.get());
    ASSERT_EQ(data.size(), 2);
    EXPECT_EQ(data[0].to_string(), "abc");
    EXPECT_EQ(data[1].to_string(), "def");
}

// GetContainer::get_data(column, row)
TEST_F(ColumnHelperTest, get_container_get_data_with_row_fixed_length) {
    auto col = ColumnTestHelper::build_column<int32_t>({10, 20, 30});
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(col.get(), 0), 10);
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(col.get(), 1), 20);
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(col.get(), 2), 30);
}

TEST_F(ColumnHelperTest, get_container_get_data_with_row_const_column) {
    // ConstColumn: is_constant() == true, index is always 0
    auto inner = ColumnTestHelper::build_column<int32_t>({42});
    auto const_col = ConstColumn::create(std::move(inner), 5);
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(const_col.get(), 0), 42);
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(const_col.get(), 3), 42);
}

TEST_F(ColumnHelperTest, get_container_get_data_with_row_nullable) {
    // NullableColumn: get_data_column unwraps it, row index used as-is
    auto col = ColumnTestHelper::build_nullable_column<int32_t>({10, 20, 30});
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(col.get(), 0), 10);
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(col.get(), 1), 20);
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(col.get(), 2), 30);
}

TEST_F(ColumnHelperTest, get_data_column_from_const_nullable_column) {
    auto data_column = BinaryColumn::create();
    data_column->append(Slice("v1"));
    auto null_column = NullColumn::create();
    null_column->append(0);
    ColumnPtr nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
    ColumnPtr const_nullable_column = ConstColumn::create(nullable_column, 4);

    const Column* const_data = ColumnHelper::get_data_column(const_nullable_column.get());
    ASSERT_FALSE(const_data->is_nullable());
    ASSERT_FALSE(const_data->is_constant());
    ASSERT_TRUE(const_data->is_binary());

    const auto* typed_data = ColumnHelper::get_data_column_by_type<TYPE_VARCHAR>(const_nullable_column.get());
    ASSERT_TRUE(typed_data->is_binary());
    ASSERT_EQ(typed_data->get_slice(0).to_string(), "v1");
}

// ColumnHelper::append_column_value
TEST_F(ColumnHelperTest, append_column_value_int32) {
    auto col = Int32Column::create();
    ColumnHelper::append_column_value<TYPE_INT>(col.get(), 42);
    ColumnHelper::append_column_value<TYPE_INT>(col.get(), 99);
    EXPECT_EQ(col->debug_string(), "[42, 99]");
}

TEST_F(ColumnHelperTest, append_column_value_varchar) {
    auto col = BinaryColumn::create();
    ColumnHelper::append_column_value<TYPE_VARCHAR>(col.get(), Slice("hello"));
    ColumnHelper::append_column_value<TYPE_VARCHAR>(col.get(), Slice("world"));
    EXPECT_EQ(col->debug_string(), "['hello', 'world']");
}

TEST_F(ColumnHelperTest, append_column_value_large_binary) {
    auto col = LargeBinaryColumn::create();
    ColumnHelper::append_column_value<TYPE_VARBINARY>(col.get(), Slice("foo"));
    ColumnHelper::append_column_value<TYPE_VARBINARY>(col.get(), Slice("bar"));
    EXPECT_EQ(col->debug_string(), "['foo', 'bar']");
}

// ColumnHelper::build_slices
TEST_F(ColumnHelperTest, build_slices_binary_column) {
    auto col = ColumnTestHelper::build_column<Slice>({"foo", "bar", "baz"});

    Buffer<Slice> slices;
    ColumnHelper::build_slices(col.get(), slices);
    ASSERT_EQ(slices.size(), 3);
    EXPECT_EQ(slices[0].to_string(), "foo");
    EXPECT_EQ(slices[1].to_string(), "bar");
    EXPECT_EQ(slices[2].to_string(), "baz");
}

TEST_F(ColumnHelperTest, build_slices_large_binary_column) {
    auto col = LargeBinaryColumn::create();
    col->append_string("hello");
    col->append_string("world");

    Buffer<Slice> slices;
    ColumnHelper::build_slices(col.get(), slices);
    ASSERT_EQ(slices.size(), 2);
    EXPECT_EQ(slices[0].to_string(), "hello");
    EXPECT_EQ(slices[1].to_string(), "world");
}

TEST_F(ColumnHelperTest, build_slices_nullable_column) {
    auto nullable = ColumnTestHelper::build_nullable_column<Slice>({"a", "bb"});

    Buffer<Slice> slices;
    ColumnHelper::build_slices(nullable.get(), slices);
    ASSERT_EQ(slices.size(), 2);
    EXPECT_EQ(slices[0].to_string(), "a");
    EXPECT_EQ(slices[1].to_string(), "bb");
}

TEST_F(ColumnHelperTest, build_slices_const_column) {
    auto inner = BinaryColumn::create();
    inner->append(Slice("const"));
    ColumnPtr const_col = ConstColumn::create(std::move(inner), 3);

    Buffer<Slice> slices;
    ColumnHelper::build_slices(const_col.get(), slices);
    ASSERT_EQ(slices.size(), 1);
    EXPECT_EQ(slices[0].to_string(), "const");
}

// GetStorageContainer::get_data
TEST_F(ColumnHelperTest, get_storage_container_fixed_length) {
    auto col = ColumnTestHelper::build_column<int32_t>({1, 2, 3});
    auto data = GetStorageContainer<TYPE_INT>::get_data(col.get());
    ASSERT_EQ(data.size(), 3);
    EXPECT_EQ(data[0], 1);
    EXPECT_EQ(data[1], 2);
    EXPECT_EQ(data[2], 3);
}

TEST_F(ColumnHelperTest, get_storage_container_nullable) {
    auto col = ColumnTestHelper::build_nullable_column<int32_t>({10, 20, 30});
    auto data = GetStorageContainer<TYPE_INT>::get_data(col.get());
    ASSERT_EQ(data.size(), 3);
    EXPECT_EQ(data[0], 10);
    EXPECT_EQ(data[1], 20);
    EXPECT_EQ(data[2], 30);
}

TEST_F(ColumnHelperTest, get_storage_container_const_column) {
    auto inner = ColumnTestHelper::build_column<int32_t>({42});
    auto const_col = ConstColumn::create(std::move(inner), 3);
    auto data = GetStorageContainer<TYPE_INT>::get_data(const_col.get());
    ASSERT_EQ(data.size(), 1);
    EXPECT_EQ(data[0], 42);
}

TEST_F(ColumnHelperTest, get_storage_container_varchar) {
    auto col = ColumnTestHelper::build_column<Slice>({"hello", "world"});
    auto data = GetStorageContainer<TYPE_VARCHAR>::get_data(col.get());
    ASSERT_EQ(data.size(), 2);
    EXPECT_EQ(data[0].to_string(), "hello");
    EXPECT_EQ(data[1].to_string(), "world");
}

TEST_F(ColumnHelperTest, get_storage_container_large_binary) {
    auto col = LargeBinaryColumn::create();
    col->append_string("abc");
    col->append_string("def");
    auto data = GetStorageContainer<TYPE_VARCHAR>::get_data(col.get());
    ASSERT_EQ(data.size(), 2);
    EXPECT_EQ(data[0].to_string(), "abc");
    EXPECT_EQ(data[1].to_string(), "def");
}

TEST_F(ColumnHelperTest, get_storage_container_nullable_varchar) {
    auto col = ColumnTestHelper::build_nullable_column<Slice>({"foo", "bar"});
    auto data = GetStorageContainer<TYPE_VARCHAR>::get_data(col.get());
    ASSERT_EQ(data.size(), 2);
    EXPECT_EQ(data[0].to_string(), "foo");
    EXPECT_EQ(data[1].to_string(), "bar");
}

TEST_F(ColumnHelperTest, get_storage_container_mutable_column_ptr) {
    auto col = ColumnTestHelper::build_column<int32_t>({5, 6, 7});
    MutableColumnPtr mutable_col = std::move(col);
    auto data = GetStorageContainer<TYPE_INT>::get_data(mutable_col);
    ASSERT_EQ(data.size(), 3);
    EXPECT_EQ(data[0], 5);
    EXPECT_EQ(data[1], 6);
    EXPECT_EQ(data[2], 7);
}

// GetStorageContainer::get_data(column, row)
TEST_F(ColumnHelperTest, get_storage_container_get_data_with_row_fixed_length) {
    auto col = ColumnTestHelper::build_column<int32_t>({10, 20, 30});
    EXPECT_EQ(GetStorageContainer<TYPE_INT>::get_data(col.get(), 0), 10);
    EXPECT_EQ(GetStorageContainer<TYPE_INT>::get_data(col.get(), 1), 20);
    EXPECT_EQ(GetStorageContainer<TYPE_INT>::get_data(col.get(), 2), 30);
}

TEST_F(ColumnHelperTest, get_storage_container_get_data_with_row_const_column) {
    auto inner = ColumnTestHelper::build_column<int32_t>({99});
    auto const_col = ConstColumn::create(std::move(inner), 5);
    EXPECT_EQ(GetStorageContainer<TYPE_INT>::get_data(const_col.get(), 0), 99);
    EXPECT_EQ(GetStorageContainer<TYPE_INT>::get_data(const_col.get(), 4), 99);
}

TEST_F(ColumnHelperTest, get_storage_container_get_data_with_row_nullable) {
    auto col = ColumnTestHelper::build_nullable_column<int32_t>({10, 20, 30});
    EXPECT_EQ(GetStorageContainer<TYPE_INT>::get_data(col.get(), 0), 10);
    EXPECT_EQ(GetStorageContainer<TYPE_INT>::get_data(col.get(), 1), 20);
    EXPECT_EQ(GetStorageContainer<TYPE_INT>::get_data(col.get(), 2), 30);
}

// Verify update_nested_has_null recurses into STRUCT subfield columns.
// Mirrors the post-write step a STRUCT-returning Java UDF goes through:
// the Java side memcpys raw null bytes into subfield bitmaps without touching
// each NullableColumn's `_has_null` cache, and update_nested_has_null is
// expected to refresh the cache field-by-field.
TEST_F(ColumnHelperTest, update_nested_has_null_struct) {
    TypeDescriptor td(TYPE_STRUCT);
    td.children.emplace_back(TYPE_INT);
    td.children.emplace_back(TYPE_VARCHAR);
    td.field_names = {"a", "b"};
    auto col = ColumnHelper::create_column(td, true);
    auto* outer = down_cast<NullableColumn*>(col.get());
    outer->resize(3);

    auto* sc = down_cast<StructColumn*>(outer->data_column_raw_ptr());
    ASSERT_EQ(sc->fields_size(), 2);
    auto* a_col = down_cast<NullableColumn*>(sc->field_column_raw_ptr(0));
    auto* b_col = down_cast<NullableColumn*>(sc->field_column_raw_ptr(1));

    // Drop a null straight into subfield a's bitmap, bypassing the bookkeeping
    // that maintains _has_null. Pre-condition is the stale `false`.
    a_col->null_column_data()[1] = 1;
    ASSERT_FALSE(a_col->has_null());

    ASSERT_OK(ColumnHelper::update_nested_has_null(col.get()));

    EXPECT_TRUE(a_col->has_null());
    EXPECT_FALSE(b_col->has_null());
}

TEST_F(ColumnHelperTest, create_geo_placeholder_and_copy) {
    for (const auto primitive : {TYPE_GEOGRAPHY, TYPE_GEOMETRY}) {
        const auto type = geo_type(primitive);
        auto column = ColumnHelper::create_column(type, false, false, 3);
        auto* geo = dynamic_cast<GeoColumn*>(column.get());
        ASSERT_NE(nullptr, geo);
        EXPECT_EQ(3, geo->size());
        EXPECT_FALSE(geo->is_binary());
        // Descriptor-aware generic creation is a separate follow-up. These are
        // untyped physical placeholders, not silently coerced native values.
        EXPECT_EQ(GeoTypeDescriptor{}, geo->descriptor().type);
        EXPECT_EQ(GEO_ENCODING_WKB, geo->descriptor().storage.encoding);
        EXPECT_EQ(GEO_DIMENSION_UNKNOWN, geo->descriptor().storage.dimension);
        EXPECT_EQ(GEO_VALIDATION_STATE_UNVALIDATED, geo->descriptor().storage.validation_state);
        for (size_t i = 0; i < geo->size(); ++i) {
            EXPECT_TRUE(geo->get_wkb(i).empty());
        }

        // Physical ingestion copies opaque bytes; it does not infer geometry semantics.
        geo->append_wkb(Slice("payload"));
        auto copy = geo->clone();
        const auto* copied_geo = dynamic_cast<const GeoColumn*>(copy.get());
        ASSERT_NE(nullptr, copied_geo);
        EXPECT_EQ(geo->descriptor(), copied_geo->descriptor());
        EXPECT_EQ("payload", copied_geo->get_wkb(3).to_string());

        auto destination = ColumnHelper::create_column(type, false);
        destination->append(*column, 3, 1);
        const auto* destination_geo = dynamic_cast<const GeoColumn*>(destination.get());
        ASSERT_NE(nullptr, destination_geo);
        EXPECT_EQ(geo->descriptor(), destination_geo->descriptor());
        EXPECT_EQ("payload", destination_geo->get_wkb(0).to_string());
    }
}

TEST_F(ColumnHelperTest, create_geo_does_not_infer_metadata) {
    for (const auto primitive : {TYPE_GEOGRAPHY, TYPE_GEOMETRY}) {
        TypeDescriptor type(primitive);
        auto absent = ColumnHelper::create_column(type, false);
        const auto* absent_geo = dynamic_cast<const GeoColumn*>(absent.get());
        ASSERT_NE(nullptr, absent_geo);
        const auto& descriptor = absent_geo->descriptor().type;
        EXPECT_EQ(GEO_LOGICAL_TYPE_UNKNOWN, descriptor.logical_type);
        EXPECT_EQ(GEO_COORDINATE_SYSTEM_UNKNOWN, descriptor.coordinate_system);
        EXPECT_EQ(GEO_EDGE_ALGORITHM_UNKNOWN, descriptor.edge_algorithm);
        EXPECT_TRUE(descriptor.crs.empty());
        EXPECT_FALSE(descriptor.srid.has_value());

        type.geo_type = GeoTypeDescriptor{};
        auto empty = ColumnHelper::create_column(type, false);
        const auto* empty_geo = dynamic_cast<const GeoColumn*>(empty.get());
        ASSERT_NE(nullptr, empty_geo);
        EXPECT_EQ(*type.geo_type, empty_geo->descriptor().type);
        EXPECT_EQ(GEO_LOGICAL_TYPE_UNKNOWN, empty_geo->descriptor().type.logical_type);
    }
}

TEST_F(ColumnHelperTest, create_geo_nullable_const_and_adaptive) {
    for (const auto primitive : {TYPE_GEOGRAPHY, TYPE_GEOMETRY}) {
        const auto type = geo_type(primitive);
        for (const bool adaptive : {false, true}) {
            auto column = ColumnHelper::create_column(type, true, false, 3, adaptive);
            auto* nullable = dynamic_cast<NullableColumn*>(column.get());
            ASSERT_NE(nullptr, nullable);
            EXPECT_EQ(adaptive, dynamic_cast<AdaptiveNullableColumn*>(column.get()) != nullptr);
            EXPECT_EQ(3, nullable->size());
            EXPECT_TRUE(nullable->is_null(0));
            EXPECT_TRUE(nullable->is_null(2));
            const auto* geo = dynamic_cast<const GeoColumn*>(nullable->data_column().get());
            ASSERT_NE(nullptr, geo);
            EXPECT_EQ(GeoTypeDescriptor{}, geo->descriptor().type);
            EXPECT_EQ(3, geo->size());
            column->check_or_die();
        }

        auto constant = ColumnHelper::create_column(type, false, true, 3);
        ASSERT_TRUE(constant->is_constant());
        EXPECT_EQ(3, constant->size());
        const auto* constant_geo = dynamic_cast<const GeoColumn*>(ColumnHelper::get_data_column(constant.get()));
        ASSERT_NE(nullptr, constant_geo);
        EXPECT_EQ(1, constant_geo->size());
        EXPECT_EQ(GeoTypeDescriptor{}, constant_geo->descriptor().type);
        constant->check_or_die();

        auto nulls = ColumnHelper::create_column(type, true, true, 3);
        ASSERT_TRUE(nulls->only_null());
        auto aligned = ColumnHelper::align_return_type(std::move(nulls), type, 3, true);
        ASSERT_TRUE(aligned->is_nullable());
        EXPECT_EQ(3, aligned->size());
        EXPECT_TRUE(aligned->is_null(0));
        EXPECT_TRUE(aligned->is_null(2));
        const auto* aligned_geo = dynamic_cast<const GeoColumn*>(ColumnHelper::get_data_column(aligned.get()));
        ASSERT_NE(nullptr, aligned_geo);
        EXPECT_EQ(GeoTypeDescriptor{}, aligned_geo->descriptor().type);
        aligned->check_or_die();
    }
}

TEST_F(ColumnHelperTest, create_nested_geo_columns) {
    for (const auto primitive : {TYPE_GEOGRAPHY, TYPE_GEOMETRY}) {
        const auto type = geo_type(primitive);
        const auto array_type = TypeDescriptor::create_array_type(type);
        const auto map_type = TypeDescriptor::create_map_type(TypeDescriptor(TYPE_INT), array_type);
        const auto struct_type =
                TypeDescriptor::create_struct_type({"direct", "array", "map"}, {type, array_type, map_type});
        auto column = ColumnHelper::create_column(struct_type, false);
        auto* structure = dynamic_cast<StructColumn*>(column.get());
        ASSERT_NE(nullptr, structure);
        ASSERT_EQ(3, structure->fields_size());
        const auto* direct =
                dynamic_cast<const GeoColumn*>(ColumnHelper::get_data_column(structure->field_column_raw_ptr(0)));
        ASSERT_NE(nullptr, direct);
        EXPECT_EQ(GeoTypeDescriptor{}, direct->descriptor().type);

        const auto* array =
                dynamic_cast<const ArrayColumn*>(ColumnHelper::get_data_column(structure->field_column_raw_ptr(1)));
        ASSERT_NE(nullptr, array);
        const auto* array_geo =
                dynamic_cast<const GeoColumn*>(ColumnHelper::get_data_column(array->elements_column_raw_ptr()));
        ASSERT_NE(nullptr, array_geo);
        EXPECT_EQ(direct->descriptor(), array_geo->descriptor());

        const auto* map =
                dynamic_cast<const MapColumn*>(ColumnHelper::get_data_column(structure->field_column_raw_ptr(2)));
        ASSERT_NE(nullptr, map);
        const auto* values =
                dynamic_cast<const ArrayColumn*>(ColumnHelper::get_data_column(map->values_column_raw_ptr()));
        ASSERT_NE(nullptr, values);
        const auto* map_geo =
                dynamic_cast<const GeoColumn*>(ColumnHelper::get_data_column(values->elements_column_raw_ptr()));
        ASSERT_NE(nullptr, map_geo);
        EXPECT_EQ(direct->descriptor(), map_geo->descriptor());
        column->check_or_die();
    }
}

} // namespace starrocks
