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

#include "exprs/function_helper.h"

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <string_view>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/decimalv3_column.h"
#include "column/geo_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/base_aggregate_test.h"
#include "exprs/agg/map_agg.h"
#include "exprs/struct_functions.h"
#include "testutil/type_test_helper.h"

namespace starrocks {
struct FunctionHelperTest : public ::testing::Test {};
TEST_F(FunctionHelperTest, testMergeOnlyNullColumnMergeWithNullColumn) {
    auto only_null_column = ColumnHelper::create_const_null_column(10);
    auto null_column = NullColumn::create();
    null_column->reserve(10);
    for (int i = 0; i < 10; ++i) {
        null_column->append(i % 2 == 0 ? DATUM_NOT_NULL : DATUM_NULL);
    }
    auto result_column =
            FunctionHelper::merge_column_and_null_column(std::move(only_null_column), std::move(null_column));
    ASSERT_TRUE(result_column->only_null());
    ASSERT_EQ(result_column->size(), 10);
}

TEST_F(FunctionHelperTest, testMergeConstColumnMergeWithNullColumn) {
    auto data_column = Decimal128Column::create(38, 10);
    data_column->append((int128_t)111);
    auto const_column = ConstColumn::create(std::move(data_column), 10);
    auto null_column = NullColumn::create();
    null_column->reserve(10);
    for (int i = 0; i < 10; ++i) {
        null_column->append(i % 2 == 0 ? DATUM_NOT_NULL : DATUM_NULL);
    }
    auto result_column = FunctionHelper::merge_column_and_null_column(std::move(const_column), std::move(null_column));
    ASSERT_TRUE(result_column->is_nullable());
    ASSERT_EQ(result_column->size(), 10);
    auto* result_nullable_column = down_cast<const NullableColumn*>(result_column.get());
    auto* result_data_column = down_cast<const Decimal128Column*>(result_nullable_column->data_column().get());
    auto& result_data = result_data_column->get_data();
    for (int i = 0; i < 10; ++i) {
        if (i % 2 == 0) {
            ASSERT_FALSE(result_nullable_column->is_null(i));
            ASSERT_EQ(result_data[i], (int128_t)111);
        } else {
            ASSERT_TRUE(result_nullable_column->is_null(i));
        }
    }
}

TEST_F(FunctionHelperTest, testMergeNullableColumnMergeWithNullColumn) {
    auto nullable_column = NullableColumn::create(Decimal128Column::create(38, 10), NullColumn::create());
    nullable_column->reserve(10);
    for (int i = 0; i < 10; ++i) {
        if (i % 3 == 0) {
            nullable_column->append_nulls(1);
        } else {
            Datum datum((int128_t)111);
            nullable_column->append_datum(datum);
        }
    }
    auto null_column = NullColumn::create();
    null_column->reserve(10);
    for (int i = 0; i < 10; ++i) {
        null_column->append(i % 2 == 0 ? DATUM_NOT_NULL : DATUM_NULL);
    }
    auto result_column =
            FunctionHelper::merge_column_and_null_column(std::move(nullable_column), std::move(null_column));
    ASSERT_TRUE(result_column->is_nullable());
    ASSERT_EQ(result_column->size(), 10);
    auto* result_nullable_column = down_cast<const NullableColumn*>(result_column.get());
    auto* result_data_column = down_cast<const Decimal128Column*>(result_nullable_column->data_column().get());
    auto& result_data = result_data_column->get_data();
    for (int i = 0; i < 10; ++i) {
        if (i % 2 == 0 && i % 3 != 0) {
            ASSERT_FALSE(result_nullable_column->is_null(i));
            ASSERT_EQ(result_data[i], (int128_t)111);
        } else {
            ASSERT_TRUE(result_nullable_column->is_null(i));
        }
    }
}
TEST_F(FunctionHelperTest, testMergeNotNullableColumnMergeWithNullColumn) {
    auto data_column = Decimal128Column::create(38, 10);
    data_column->reserve(10);
    for (int i = 0; i < 10; ++i) {
        data_column->append((int128_t)111);
    }
    auto null_column = NullColumn::create();
    null_column->reserve(10);
    for (int i = 0; i < 10; ++i) {
        null_column->append(i % 2 == 0 ? DATUM_NOT_NULL : DATUM_NULL);
    }
    auto result_column = FunctionHelper::merge_column_and_null_column(std::move(data_column), std::move(null_column));
    ASSERT_TRUE(result_column->is_nullable());
    ASSERT_EQ(result_column->size(), 10);
    auto* result_nullable_column = down_cast<const NullableColumn*>(result_column.get());
    auto* result_data_column = down_cast<const Decimal128Column*>(result_nullable_column->data_column().get());
    auto& result_data = result_data_column->get_data();
    for (int i = 0; i < 10; ++i) {
        if (i % 2 == 0) {
            ASSERT_FALSE(result_nullable_column->is_null(i));
            ASSERT_EQ(result_data[i], (int128_t)111);
        } else {
            ASSERT_TRUE(result_nullable_column->is_null(i));
        }
    }
}

TEST_F(FunctionHelperTest, testCreateColumnScalarNullableAndNonNullable) {
    TypeDescriptor type_desc(TYPE_INT);

    auto non_nullable = FunctionHelper::create_column(type_desc, false);
    ASSERT_NE(non_nullable, nullptr);
    ASSERT_FALSE(non_nullable->is_nullable());
    ASSERT_TRUE(non_nullable->is_numeric());

    auto nullable = FunctionHelper::create_column(type_desc, true);
    ASSERT_NE(nullable, nullptr);
    ASSERT_TRUE(nullable->is_nullable());
    auto* nullable_column = down_cast<NullableColumn*>(nullable.get());
    ASSERT_TRUE(nullable_column->data_column()->is_numeric());
}

TEST_F(FunctionHelperTest, testCreateColumnArray) {
    auto array_column = FunctionHelper::create_column(TYPE_ARRAY_INT_DESC, false);
    ASSERT_NE(array_column, nullptr);
    ASSERT_TRUE(array_column->is_array());

    auto* col = down_cast<ArrayColumn*>(array_column.get());
    ASSERT_TRUE(col->elements().is_nullable());
    ASSERT_TRUE(ColumnHelper::get_data_column(col->elements_column_raw_ptr())->is_numeric());
    ASSERT_EQ(col->size(), 0);
    ASSERT_EQ(col->offsets().size(), 1);
    ASSERT_EQ(col->offsets().immutable_data().back(), 0);
}

TEST_F(FunctionHelperTest, testCreateColumnStruct) {
    std::vector<std::string> field_names{"c1", "c2"};
    std::vector<TypeDescriptor> field_types{TypeDescriptor(TYPE_INT), TYPE_ARRAY_INT_DESC};
    TypeDescriptor struct_type = TypeDescriptor::create_struct_type(field_names, field_types);

    auto struct_column = FunctionHelper::create_column(struct_type, false);
    ASSERT_NE(struct_column, nullptr);
    ASSERT_TRUE(struct_column->is_struct());

    auto* col = down_cast<StructColumn*>(struct_column.get());
    ASSERT_EQ(col->fields_size(), 2);
    ASSERT_TRUE(col->field_column_raw_ptr(0)->is_nullable());
    ASSERT_TRUE(ColumnHelper::get_data_column(col->field_column_raw_ptr(0))->is_numeric());
    ASSERT_TRUE(col->field_column_raw_ptr(1)->is_nullable());
    ASSERT_TRUE(ColumnHelper::get_data_column(col->field_column_raw_ptr(1))->is_array());
}

static const GeoColumn* check_geo_leaf(const Column* column, const GeoTypeDescriptor& expected) {
    column = ColumnHelper::get_data_column(column);
    if (column->is_array())
        return check_geo_leaf(down_cast<const ArrayColumn*>(column)->elements_column_raw_ptr(), expected);
    if (column->is_map()) return check_geo_leaf(down_cast<const MapColumn*>(column)->values_column_raw_ptr(), expected);
    if (column->is_struct())
        return check_geo_leaf(down_cast<const StructColumn*>(column)->field_column_raw_ptr(0), expected);
    const auto* geo = down_cast<const GeoColumn*>(column);
    EXPECT_EQ(expected, geo->descriptor().type);
    return geo;
}

TEST_F(FunctionHelperTest, testCreateColumnSpecialTypes) {
    for (const auto& type :
         {TypeDescriptor(TYPE_NULL),
          TypeDescriptor::create_map_type(TypeDescriptor(TYPE_UNKNOWN), TypeDescriptor(TYPE_UNKNOWN)),
          TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 35, 7), TypeDescriptor(TYPE_GEOGRAPHY),
          TypeDescriptor(TYPE_GEOMETRY)}) {
        for (bool nullable : {false, true}) {
            auto column = FunctionHelper::create_column(type, nullable);
            EXPECT_EQ(nullable, column->is_nullable());
            EXPECT_EQ(0, column->size());
            auto* data = ColumnHelper::get_data_column(column.get());
            if (type.type == TYPE_MAP) {
                auto* map = down_cast<MapColumn*>(data);
                for (auto* child : {map->keys_column_raw_ptr(), map->values_column_raw_ptr()}) {
                    EXPECT_TRUE(child->is_nullable());
                    EXPECT_NE(nullptr, dynamic_cast<NullColumn*>(ColumnHelper::get_data_column(child)));
                }
            } else if (type.type == TYPE_NULL) {
                EXPECT_NE(nullptr, dynamic_cast<NullColumn*>(data));
            } else if (type.type == TYPE_DECIMAL128) {
                EXPECT_EQ(35, down_cast<Decimal128Column*>(data)->precision());
                EXPECT_EQ(7, down_cast<Decimal128Column*>(data)->scale());
            } else {
                check_geo_leaf(column.get(), {});
            }
            column->check_or_die();
        }
    }
}

class GeoFunctionHelperTest : public FunctionHelperTest, public ::testing::WithParamInterface<LogicalType> {
protected:
    TypeDescriptor geo_type() const {
        const bool geography = GetParam() == TYPE_GEOGRAPHY;
        return TypeDescriptor::create_geo_type(
                GetParam(),
                {geography ? GEO_LOGICAL_TYPE_GEOGRAPHY : GEO_LOGICAL_TYPE_GEOMETRY,
                 geography ? GEO_COORDINATE_SYSTEM_SPHERICAL : GEO_COORDINATE_SYSTEM_CARTESIAN,
                 geography ? GEO_EDGE_ALGORITHM_SPHERICAL : GEO_EDGE_ALGORITHM_PLANAR,
                 geography ? "OGC:CRS84" : "EPSG:3857", geography ? std::nullopt : std::optional<int32_t>(3857)});
    }
    auto context(const TypeDescriptor& result, const std::vector<TypeDescriptor>& args) {
        return std::unique_ptr<FunctionContext>(FunctionContext::create_context(nullptr, pool.get(), result, args));
    }
    MutableColumnPtr input(const TypeDescriptor& arg, bool nullable = false) {
        auto column = ColumnHelper::create_column(arg, nullable);
        column->append_datum(arg.type == TYPE_ARRAY ? Datum(DatumArray{Datum(Slice(point)), Datum()})
                                                    : Datum(Slice(point)));
        return column;
    }
    MutableColumnPtr physical_input(const TypeDescriptor& arg, int mode) {
        auto geo = GeoColumn::create(GeoColumnDescriptor{
                *type.geo_type, {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_STRUCTURALLY_VALIDATED}});
        geo->append_wkb(Slice(point));
        auto column = ColumnHelper::create_column(arg, true);
        if (mode == 4) return column; // Empty, with the factory's physical placeholder.
        if (mode == 3 && arg.type == TYPE_ARRAY) {
            column->append_nulls(3); // No geo elements: retain the factory's physical placeholder.
            return column;
        }
        if (arg.type == TYPE_ARRAY) {
            auto* array = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(column.get()));
            array->elements_column_raw_ptr()->append(*geo, 0, 1);
            array->offsets_column_raw_ptr()->append(1);
            down_cast<NullableColumn*>(column.get())->null_column_data().push_back(0);
        } else {
            column->append(*geo, 0, 1);
        }
        if (mode == 3) {
            column->resize(0);
            column->append_nulls(3);
        } else if (mode == 2) {
            return ConstColumn::create(std::move(column), 3);
        } else {
            auto copy = column->clone();
            if (mode == 1)
                column->append_nulls(1);
            else
                column->append(*copy, 0, 1);
            column->append(*copy, 0, 1);
        }
        if (mode == 0) return down_cast<NullableColumn*>(column.get())->data_column()->as_mutable_ptr();
        return column;
    }
    void check_array_conversion() {
        for (const auto& arg : {type, array_type}) {
            auto result_type = TypeDescriptor::create_array_type(arg);
            auto intermediate_type = TypeDescriptor::create_struct_type({"g"}, {result_type});
            auto ctx = context(result_type, {arg});
            auto* function = get_aggregate_function("array_agg2", arg.type, TYPE_ARRAY, false);
            ASSERT_NE(nullptr, function);
            for (int mode : {0, 1, 2, 3, 4}) {
                SCOPED_TRACE(::testing::Message() << arg.type << ":" << mode);
                auto source = physical_input(arg, mode);
                auto result = ColumnHelper::create_column(intermediate_type, true);
                function->convert_to_serialize_format(ctx.get(), {source}, source->size(), result);
                EXPECT_EQ(check_geo_leaf(source.get(), *type.geo_type)->descriptor(),
                          check_geo_leaf(result.get(), *type.geo_type)->descriptor());
                auto* array = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(
                        down_cast<const StructColumn*>(ColumnHelper::get_data_column(result.get()))
                                ->field_column_raw_ptr(0)));
                EXPECT_EQ(source->size(), result->size());
                EXPECT_EQ(source->size(), array->elements().size());
                for (size_t i = 0; i < source->size(); ++i) {
                    EXPECT_EQ(i + 1, array->offsets().get(i + 1).get_uint32());
                    EXPECT_EQ(source->is_null(i), array->elements().is_null(i));
                }
                if (mode < 3) {
                    EXPECT_EQ(point, check_geo_leaf(result.get(), *type.geo_type)->get_wkb(0).to_string());
                }
                result->check_or_die();
                auto merged = ManagedAggrState::create(ctx.get(), function);
                for (size_t i = 0; i < result->size(); ++i)
                    function->merge(ctx.get(), result.get(), merged->state(), i);
                auto finalized = ColumnHelper::create_column(result_type, false);
                function->finalize_to_column(ctx.get(), merged->state(), finalized.get());
                EXPECT_EQ(check_geo_leaf(source.get(), *type.geo_type)->descriptor(),
                          check_geo_leaf(finalized.get(), *type.geo_type)->descriptor());
                finalized->check_or_die();
            }
            auto wrong_type = intermediate_type;
            auto* leaf = &wrong_type;
            while (!leaf->children.empty()) leaf = &leaf->children[0];
            leaf->geo_type->crs = "custom:incompatible";
            auto wrong = ColumnHelper::create_column(wrong_type, true);
            auto source = physical_input(arg, 0);
            EXPECT_THROW(function->convert_to_serialize_format(ctx.get(), {source}, source->size(), wrong),
                         std::invalid_argument);
        }
    }
    void check_aggregate(const char* name) {
        const bool array_agg = name == std::string_view("array_agg2");
        auto result_type = array_agg ? TypeDescriptor::create_array_type(array_type) : array_type;
        auto ctx = context(result_type, {array_type});
        auto source = input(array_type);
        const Column* columns[] = {source.get()};
        auto* function = get_aggregate_function(name, TYPE_ARRAY, result_type.type, false);
        ASSERT_NE(nullptr, function);
        auto state = ManagedAggrState::create(ctx.get(), function);
        function->update(ctx.get(), columns, state->state(), 0);
        auto intermediate = ColumnHelper::create_column(
                array_agg ? TypeDescriptor::create_struct_type({"g"}, {result_type}) : result_type, false);
        function->serialize_to_column(ctx.get(), state->state(), intermediate.get());
        auto merged = ManagedAggrState::create(ctx.get(), function);
        function->merge(ctx.get(), intermediate.get(), merged->state(), 0);
        auto result = ColumnHelper::create_column(result_type, false);
        function->finalize_to_column(ctx.get(), merged->state(), result.get());
        ASSERT_FALSE(ctx->has_error());
        ASSERT_EQ(1, result->size());
        const auto* geo = check_geo_leaf(result.get(), *type.geo_type);
        EXPECT_EQ(point, geo->get_wkb(0).to_string());
        EXPECT_EQ(2, geo->size());
        auto* array = down_cast<ArrayColumn*>(result.get());
        if (array_agg) array = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(array->elements_column_raw_ptr()));
        EXPECT_TRUE(array->elements().is_null(1));
        result->check_or_die();
    }
    const TypeDescriptor type = geo_type();
    const TypeDescriptor array_type = TypeDescriptor::create_array_type(type);
    std::unique_ptr<MemPool> pool = std::make_unique<MemPool>();
    // ISO WKB, little-endian POINT(1 2).
    const std::string point{
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f"
            "\x00\x00\x00\x00\x00\x00\x00\x40",
            21};
};

TEST_P(GeoFunctionHelperTest, CreateColumns) {
    for (const auto& nested :
         {type, array_type, TypeDescriptor::create_map_type(TypeDescriptor(TYPE_INT), type),
          TypeDescriptor::create_struct_type({"g"}, {type}), TypeDescriptor::create_array_type(array_type)}) {
        for (bool nullable : {false, true}) {
            auto column = FunctionHelper::create_column(nested, nullable);
            EXPECT_EQ(nullable, column->is_nullable());
            EXPECT_EQ(0, column->size());
            const auto* geo = check_geo_leaf(column.get(), *type.geo_type);
            EXPECT_EQ((GeoStorageDescriptor{GEO_ENCODING_WKB, GEO_DIMENSION_UNKNOWN, GEO_VALIDATION_STATE_UNVALIDATED}),
                      geo->descriptor().storage);
            column->check_or_die();
        }
    }
    auto source = GeoColumn::create(GeoColumnDescriptor{
            *type.geo_type, {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_STRUCTURALLY_VALIDATED}});
    source->append_wkb(Slice(point));
    auto result = FunctionHelper::create_column(type, false);
    result->append(*source);
    EXPECT_EQ(source->descriptor(), check_geo_leaf(result.get(), *type.geo_type)->descriptor());
    EXPECT_EQ(point, check_geo_leaf(result.get(), *type.geo_type)->get_wkb(0).to_string());
    auto other = type;
    other.geo_type->crs = "custom:incompatible";
    auto incompatible = ColumnHelper::create_column(other, false);
    EXPECT_THROW(incompatible->append(*result), std::invalid_argument);
}

TEST_P(GeoFunctionHelperTest, ArrayAgg) {
    check_aggregate("array_agg2");
}
TEST_P(GeoFunctionHelperTest, ArrayV2ConvertCopy) {
    check_array_conversion();
}
TEST_P(GeoFunctionHelperTest, AnyValue) {
    check_aggregate("any_value");
}

TEST_P(GeoFunctionHelperTest, ArrayAggWindow) {
    auto ctx = context(array_type, {type});
    auto function = std::make_unique<ArrayAggAggregateFunctionV2<ArrayAggWindowStateV2>>();
    auto state = ManagedAggrState::create(ctx.get(), function.get());
    check_geo_leaf(reinterpret_cast<ArrayAggWindowStateV2*>(state->state())->result_column.get(), *type.geo_type);
    auto source = input(type);
    const Column* columns[] = {source.get()};
    function->update(ctx.get(), columns, state->state(), 0);
    auto result = ColumnHelper::create_column(array_type, true);
    function->get_values(ctx.get(), state->state(), result.get(), 0, 2);
    ASSERT_EQ(2, result->size());
    const auto* geo = check_geo_leaf(result.get(), *type.geo_type);
    EXPECT_EQ(point, geo->get_wkb(0).to_string());
    EXPECT_EQ(point, geo->get_wkb(1).to_string());
    result->check_or_die();
}

TEST_P(GeoFunctionHelperTest, MapAgg) {
    auto map_type = TypeDescriptor::create_map_type(TypeDescriptor(TYPE_INT), type);
    auto ctx = context(map_type, {TypeDescriptor(TYPE_INT), type});
    auto function = std::make_unique<MapAggAggregateFunction<TYPE_INT>>();
    auto state = ManagedAggrState::create(ctx.get(), function.get());
    auto* value_column = reinterpret_cast<MapAggAggregateFunctionState<TYPE_INT>*>(state->state())->value_column.get();
    check_geo_leaf(value_column, *type.geo_type);
    auto keys = Int32Column::create();
    keys->append(7);
    keys->append(8);
    auto values = input(type, true);
    values->append_nulls(1);
    const Column* columns[] = {keys.get(), values.get()};
    function->update(ctx.get(), columns, state->state(), 0);
    function->update(ctx.get(), columns, state->state(), 1);
    auto result = ColumnHelper::create_column(map_type, false);
    function->finalize_to_column(ctx.get(), state->state(), result.get());
    ASSERT_EQ(1, result->size());
    EXPECT_EQ(point, check_geo_leaf(result.get(), *type.geo_type)->get_wkb(0).to_string());
    EXPECT_EQ(7, down_cast<MapColumn*>(result.get())->keys().get(0).get_int32());
    EXPECT_TRUE(down_cast<MapColumn*>(result.get())->values().is_null(1));
    result->check_or_die();
}

TEST_P(GeoFunctionHelperTest, StructCallers) {
    auto result_type = TypeDescriptor::create_struct_type({"g"}, {array_type});
    // Varying, nullable, constant, all-NULL, zero rows.
    for (int mode : {0, 1, 2, 3, 4}) {
        auto source = mode >= 3 ? ColumnHelper::create_column(array_type, true) : input(array_type, mode == 1);
        if (mode == 0) source->append_datum(DatumArray{});
        if (mode == 1 || mode == 3) source->append_nulls(mode == 3 ? 2 : 1);
        if (mode == 2) source = ConstColumn::create(std::move(source), 2);
        for (bool named : {false, true}) {
            auto ctx =
                    context(result_type, named ? std::vector<TypeDescriptor>{TypeDescriptor(TYPE_VARCHAR), array_type}
                                               : std::vector<TypeDescriptor>{array_type});
            auto name = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("g"), source->size());
            auto result = named ? StructFunctions::named_struct(ctx.get(), {name, source})
                                : StructFunctions::new_struct(ctx.get(), {source});
            ASSERT_TRUE(result.ok()) << result.status();
            EXPECT_EQ(source->size(), result.value()->size());
            const auto* geo = check_geo_leaf(result.value().get(), *type.geo_type);
            if (mode < 3) {
                EXPECT_EQ(point, geo->get_wkb(0).to_string());
            }
            if (mode == 2) {
                EXPECT_EQ(point, geo->get_wkb(2).to_string());
            }
            auto* field = down_cast<const StructColumn*>(result.value().get())->field_column_raw_ptr(0);
            if (mode == 1) {
                EXPECT_TRUE(field->is_null(1));
            }
            if (mode == 3) {
                EXPECT_EQ(2, down_cast<const NullableColumn*>(field)->null_count());
            }
            result.value()->check_or_die();
        }
    }
}

TEST_F(FunctionHelperTest, ArrayV2ColumnCopyControls) {
    MemPool pool;
    ArrayAggAggregateFunctionV2<ArrayAggAggregateStateV2> current;
    for (const auto& arg : {TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR),
                            TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT))}) {
        for (int mode : {0, 1, 2, 3, 4, 5}) {
            SCOPED_TRACE(::testing::Message() << arg.type << ":" << mode);
            auto source = ColumnHelper::create_column(arg, mode != 0 && mode != 2);
            if (mode < 3) {
                for (int i = 11; i <= (mode == 2 ? 11 : 33); i += 11) {
                    Datum value = arg.type == TYPE_INT
                                          ? Datum(i)
                                          : arg.type == TYPE_VARCHAR
                                                    ? Datum(Slice(i == 11 ? "one" : i == 22 ? "two" : "three"))
                                                    : Datum(DatumArray{Datum(i)});
                    if (mode == 1 && i == 22)
                        source->append_nulls(1);
                    else
                        source->append_datum(value);
                }
            } else if (mode == 3)
                source->append_nulls(3);
            else if (mode == 4)
                source = ColumnHelper::create_const_null_column(3);
            if (mode == 2) source = ConstColumn::create(std::move(source), 3);
            auto expected = [&](size_t i) {
                return source->is_constant() ? down_cast<const ConstColumn*>(source.get())->data_column()->debug_item(0)
                                             : source->debug_item(i);
            };
            auto array_type = TypeDescriptor::create_array_type(arg);
            auto intermediate =
                    ColumnHelper::create_column(TypeDescriptor::create_struct_type({"v"}, {array_type}), false);
            auto array_ctx = std::unique_ptr<FunctionContext>(
                    FunctionContext::create_context(nullptr, &pool, array_type, {arg}));
            current.convert_to_serialize_format(array_ctx.get(), {source}, source->size(), intermediate);
            const auto* array = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(
                    down_cast<const StructColumn*>(intermediate.get())->field_column_raw_ptr(0)));
            ASSERT_EQ(source->size(), array->size());
            for (size_t i = 0; i < source->size(); ++i) EXPECT_EQ(expected(i), array->elements().debug_item(i));
            intermediate->check_or_die();
        }
    }
}
INSTANTIATE_TEST_SUITE_P(FunctionHelper, GeoFunctionHelperTest, ::testing::Values(TYPE_GEOGRAPHY, TYPE_GEOMETRY));

} // namespace starrocks
