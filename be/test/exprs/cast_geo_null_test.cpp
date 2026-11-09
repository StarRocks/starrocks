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

#include <gtest/gtest.h>

#include <algorithm>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/const_column.h"
#include "column/geo_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "exprs/cast_expr.h"
#include "exprs/column_ref.h"
#include "testutil/column_test_helper.h"
#include "testutil/type_test_helper.h"

namespace starrocks {
namespace {

class GeoNullCastTest : public ::testing::TestWithParam<LogicalType> {
protected:
    TypeDescriptor geo_type() const {
        bool geography = GetParam() == TYPE_GEOGRAPHY;
        return TypeDescriptor::create_geo_type(
                GetParam(), {geography ? GEO_LOGICAL_TYPE_GEOGRAPHY : GEO_LOGICAL_TYPE_GEOMETRY,
                             geography ? GEO_COORDINATE_SYSTEM_SPHERICAL : GEO_COORDINATE_SYSTEM_CARTESIAN,
                             geography ? GEO_EDGE_ALGORITHM_KARNEY : GEO_EDGE_ALGORITHM_PLANAR,
                             geography ? "OGC:CRS84" : "EPSG:3857", geography ? 4326 : 3857});
    }

    static auto offsets(std::initializer_list<uint32_t> values) {
        auto result = UInt32Column::create();
        result->append_numbers(values.begin(), values.size() * sizeof(uint32_t));
        return result;
    }

    static void check_offsets(const UInt32Column& column, std::initializer_list<uint32_t> values) {
        auto data = column.get_data();
        EXPECT_TRUE(std::equal(values.begin(), values.end(), data.begin(), data.end()));
    }

    static auto nullable(MutableColumnPtr data, std::initializer_list<uint8_t> flags) {
        auto mask = NullColumn::create();
        mask->append_numbers(flags.begin(), flags.size());
        return NullableColumn::create(std::move(data), std::move(mask));
    }

    static auto nulls(size_t size) {
        auto result = ColumnHelper::create_column(TYPE_BOOLEAN_DESC, true);
        result->append_nulls(size);
        return result;
    }

    static TypeDescriptor struct_type(const TypeDescriptor& field) {
        TypeDescriptor result(TYPE_STRUCT);
        result.children = {field};
        result.field_names = {"g"};
        return result;
    }

    static StatusOr<ColumnPtr> cast(ColumnPtr input, const TypeDescriptor& from, const TypeDescriptor& to,
                                    bool clone = false) {
        ObjectPool pool;
        ASSIGN_OR_RETURN(auto* expr, VectorizedCastExprFactory::create_cast_expr(&pool, from, to, false));
        pool.add(expr);
        expr->add_child(pool.add(new ColumnRef(from, 0)));
        Chunk chunk;
        chunk.append_column(std::move(input), 0);
        return (clone ? Expr::copy(&pool, expr) : expr)->evaluate_checked(nullptr, &chunk);
    }

    static auto struct_with_hidden_boolean() {
        MutableColumns fields;
        fields.emplace_back(nullable(BooleanColumn::create(3, 0), {1, 0, 1}));
        return nullable(StructColumn::create(std::move(fields), std::vector<std::string>{"g"}), {0, 1, 0});
    }

    void check_geo(const ColumnPtr& column) const {
        ASSERT_TRUE(column->is_nullable());
        auto* data = dynamic_cast<const GeoColumn*>(ColumnHelper::get_data_column(column.get()));
        ASSERT_NE(nullptr, data);
        EXPECT_EQ(*geo_type().geo_type, data->descriptor().type);
        EXPECT_EQ(column->size(), ColumnHelper::count_nulls(column));
    }
};

TEST_P(GeoNullCastTest, ScalarNull) {
    for (auto from : {TYPE_NULL, TYPE_BOOLEAN}) {
        for (size_t size : {0, 1, 3}) {
            auto result = cast(nulls(size), TypeDescriptor(from), geo_type(), true);
            ASSERT_TRUE(result.ok()) << result.status();
            EXPECT_EQ(size, result.value()->size());
            EXPECT_TRUE(result.value()->is_constant());
            check_geo(result.value());
        }
        auto result = cast(ConstColumn::create(nulls(1), 5), TypeDescriptor(from), geo_type());
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(5, result.value()->size());
        check_geo(result.value());
    }
}

TEST_P(GeoNullCastTest, ArrayNulls) {
    auto from = array_type(TYPE_BOOLEAN);
    auto to = array_type(geo_type());
    auto input = nullable(ArrayColumn::create(nulls(3), offsets({0, 2, 2, 2, 3})), {0, 0, 1, 0});
    auto result = cast(input, from, to, true);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(4, result.value()->size());
    EXPECT_EQ(1, ColumnHelper::count_nulls(result.value()));
    EXPECT_TRUE(result.value()->is_null(2));
    auto* data = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(result.value().get()));
    check_offsets(data->offsets(), {0, 2, 2, 2, 3});
    EXPECT_EQ(3, data->elements_column()->size());
    check_geo(data->elements_column());

    result = cast(ArrayColumn::create(nulls(0), offsets({0, 0})), from, to);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(1, result.value()->size());
    data = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(result.value().get()));
    EXPECT_EQ(0, data->elements_column()->size());
    check_geo(data->elements_column());

    auto constant = ConstColumn::create(ArrayColumn::create(nulls(2), offsets({0, 2})), 5);
    result = cast(constant, from, to);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(5, result.value()->size());
    EXPECT_TRUE(result.value()->is_constant());
    data = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(result.value().get()));
    check_offsets(data->offsets(), {0, 2});
    check_geo(data->elements_column());

    result = cast(ColumnHelper::create_const_null_column(3), from, to);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(3, result.value()->size());
    EXPECT_TRUE(result.value()->only_null());
    auto materialized = ColumnHelper::unfold_const_column(to, 3, result.value());
    data = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(materialized.get()));
    check_geo(data->elements_column());
}

TEST_P(GeoNullCastTest, MapValue) {
    for (auto key : {TYPE_TINYINT, TYPE_INT}) {
        TypeDescriptor from(TYPE_MAP);
        from.children = {TypeDescriptor(key), TYPE_BOOLEAN_DESC};
        TypeDescriptor to(TYPE_MAP);
        to.children = {TYPE_INT_DESC, geo_type()};
        auto keys = key == TYPE_INT ? ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 3})
                                    : ColumnTestHelper::build_nullable_column<int8_t>({1, 2, 3});
        auto input = nullable(MapColumn::create(std::move(keys), nulls(3), offsets({0, 2, 2, 2, 3})), {0, 0, 1, 0});
        auto result = cast(input, from, to);
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(4, result.value()->size());
        EXPECT_EQ(1, ColumnHelper::count_nulls(result.value()));
        EXPECT_TRUE(result.value()->is_null(2));
        auto* data = down_cast<const MapColumn*>(ColumnHelper::get_data_column(result.value().get()));
        check_offsets(data->offsets(), {0, 2, 2, 2, 3});
        EXPECT_EQ(3, data->values_column()->size());
        check_geo(data->values_column());
    }
}

TEST_P(GeoNullCastTest, StructField) {
    MutableColumns fields;
    fields.emplace_back(nulls(3));
    auto input = nullable(StructColumn::create(std::move(fields), std::vector<std::string>{"g"}), {0, 1, 0});
    auto result = cast(input, struct_type(TYPE_BOOLEAN_DESC), struct_type(geo_type()));
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(3, result.value()->size());
    EXPECT_TRUE(result.value()->is_null(1));
    auto* data = down_cast<const StructColumn*>(ColumnHelper::get_data_column(result.value().get()));
    EXPECT_EQ(std::vector<std::string>{"g"}, data->field_names());
    check_geo(data->fields()[0]);
}

TEST_P(GeoNullCastTest, NestedArrayStructArray) {
    MutableColumns fields;
    fields.emplace_back(nullable(ArrayColumn::create(nulls(3), offsets({0, 0, 1, 3})), {0, 1, 0}));
    auto structs = nullable(StructColumn::create(std::move(fields), std::vector<std::string>{"g"}), {0, 0, 1});
    auto input = ArrayColumn::create(std::move(structs), offsets({0, 2, 3}));
    auto result = cast(input, array_type(struct_type(array_type(TYPE_BOOLEAN))),
                       array_type(struct_type(array_type(geo_type()))), true);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(2, result.value()->size());
    auto* outer = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(result.value().get()));
    check_offsets(outer->offsets(), {0, 2, 3});
    EXPECT_TRUE(outer->elements_column()->is_null(2));
    auto* structure = down_cast<const StructColumn*>(ColumnHelper::get_data_column(outer->elements_column().get()));
    EXPECT_TRUE(structure->fields()[0]->is_null(1));
    auto* inner = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(structure->fields()[0].get()));
    check_offsets(inner->offsets(), {0, 0, 1, 3});
    check_geo(inner->elements_column());
}

TEST_P(GeoNullCastTest, NullStructWithHiddenBoolean) {
    auto result = cast(struct_with_hidden_boolean(), struct_type(TYPE_BOOLEAN_DESC), struct_type(geo_type()));
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(3, result.value()->size());
    EXPECT_EQ(1, ColumnHelper::count_nulls(result.value()));
    EXPECT_TRUE(result.value()->is_null(1));
    auto* data = down_cast<const StructColumn*>(ColumnHelper::get_data_column(result.value().get()));
    check_geo(data->fields()[0]);
}

TEST_P(GeoNullCastTest, ArrayWithNullStructElement) {
    auto input = ArrayColumn::create(struct_with_hidden_boolean(), offsets({0, 2, 3}));
    auto result = cast(input, array_type(struct_type(TYPE_BOOLEAN_DESC)), array_type(struct_type(geo_type())));
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(2, result.value()->size());
    auto* array = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(result.value().get()));
    check_offsets(array->offsets(), {0, 2, 3});
    EXPECT_EQ(1, ColumnHelper::count_nulls(array->elements_column()));
    EXPECT_TRUE(array->elements_column()->is_null(1));
    auto* data = down_cast<const StructColumn*>(ColumnHelper::get_data_column(array->elements_column().get()));
    check_geo(data->fields()[0]);
}

INSTANTIATE_TEST_SUITE_P(NativeGeo, GeoNullCastTest, ::testing::Values(TYPE_GEOGRAPHY, TYPE_GEOMETRY));

} // namespace
} // namespace starrocks
