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

#include <optional>
#include <string>
#include <vector>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/geo_column.h"
#include "column/nullable_column.h"
#include "exprs/table_function/table_function_factory.h"
#include "exprs/table_function/table_function_harness.h"
#include "gutil/strings/escaping.h"

namespace starrocks {
namespace {

// Fixture indexes; -1 is a NULL element, nullopt is a NULL array.
using Row = std::optional<std::vector<int>>;
using Rows = std::vector<Row>;

class UnnestGeoTest : public testing::TestWithParam<LogicalType> {
protected:
    void SetUp() override {
        const bool geography = GetParam() == TYPE_GEOGRAPHY;
        _type = TypeDescriptor::create_geo_type(
                GetParam(), {geography ? GEO_LOGICAL_TYPE_GEOGRAPHY : GEO_LOGICAL_TYPE_GEOMETRY,
                             geography ? GEO_COORDINATE_SYSTEM_SPHERICAL : GEO_COORDINATE_SYSTEM_CARTESIAN,
                             geography ? GEO_EDGE_ALGORITHM_VINCENTY : GEO_EDGE_ALGORITHM_PLANAR,
                             geography ? "OGC:CRS84" : "EPSG:3857", geography ? 4326 : 3857});
        _function = get_table_function("unnest", {TYPE_ARRAY}, {GetParam()});
        ASSERT_NE(nullptr, _function);
        EXPECT_EQ(get_table_function("unnest", {TYPE_ARRAY}, {TYPE_INT}), _function);
        ASSERT_TRUE(_function->init(TFunction(), &_state).ok());
        ASSERT_TRUE(_function->prepare(_state).ok());
        ASSERT_TRUE(_function->open(&_runtime, _state).ok());
        _runtime.set_chunk_size(2);
    }

    void TearDown() override {
        if (_state != nullptr) {
            EXPECT_TRUE(_function->close(&_runtime, _state).ok());
        }
    }

    ColumnPtr make_array(const Rows& rows, bool dirty = false) {
        auto elements = NullableColumn::create(GeoColumn::create(_type, 0), NullColumn::create());
        auto offsets = UInt32Column::create();
        auto nulls = NullColumn::create();
        offsets->append(0);
        for (const auto& row : rows) {
            if (row) {
                for (int index : *row) {
                    if (index < 0) {
                        elements->append_nulls(1);
                    } else {
                        elements->append_datum(Datum(Slice(_wkb[index])));
                    }
                }
            } else if (dirty) {
                elements->append_datum(Datum(Slice(_wkb[0])));
            }
            offsets->append(elements->size());
            nulls->append(!row.has_value());
        }
        return NullableColumn::create(ArrayColumn::create(std::move(elements), std::move(offsets)), std::move(nulls));
    }

    void check(const ColumnPtr& input, const Rows& rows, bool dirty = false) {
        const auto* array = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(input.get()));
        const auto* source = down_cast<const GeoColumn*>(ColumnHelper::get_data_column(array->elements_column().get()));
        for (bool left : {false, true}) {
            SCOPED_TRACE(testing::Message() << "dirty=" << dirty << " left=" << left);
            _state->set_params({input});
            _state->set_is_left_join(left);
            _state->set_is_required(true);
            auto [columns, offsets] = _function->process(&_runtime, _state);
            ASSERT_EQ(1u, columns.size());
            ASSERT_NE(nullptr, offsets);
            ASSERT_EQ(rows.size() + 1, offsets->size());
            ASSERT_EQ(offsets->immutable_data().back(), columns[0]->size());
            EXPECT_EQ(!dirty, columns[0].get() == array->elements_column().get());
            if (!dirty) {
                EXPECT_EQ(array->offsets_column().get(), offsets.get());
            }
            const auto* geo = dynamic_cast<const GeoColumn*>(ColumnHelper::get_data_column(columns[0].get()));
            ASSERT_NE(nullptr, geo);
            EXPECT_EQ(source->descriptor().type, geo->descriptor().type);
            EXPECT_EQ(*_type.geo_type, geo->descriptor().type);
            EXPECT_EQ(rows.size(), _state->processed_rows());

            std::vector<std::string> expected_rows;
            std::vector<int> expected_values;
            std::vector<uint32_t> expected_offsets{0};
            for (size_t i = 0; i < rows.size(); ++i) {
                if (rows[i] && !rows[i]->empty()) {
                    expected_values.insert(expected_values.end(), rows[i]->begin(), rows[i]->end());
                    for (size_t j = 0; j < rows[i]->size(); ++j) {
                        expected_rows.emplace_back(table_function_test::render_row(i, {}));
                    }
                } else {
                    if (left) {
                        expected_rows.emplace_back(table_function_test::render_row(i, {}));
                    }
                    if (left && dirty) {
                        expected_values.push_back(-1);
                    }
                }
                expected_offsets.push_back(expected_values.size());
            }
            EXPECT_EQ(expected_offsets,
                      std::vector<uint32_t>(offsets->immutable_data().begin(), offsets->immutable_data().end()));
            ASSERT_EQ(expected_values.size(), columns[0]->size());
            for (size_t i = 0; i < expected_values.size(); ++i) {
                ASSERT_EQ(expected_values[i] < 0, columns[0]->is_null(i)) << "element=" << i;
                if (expected_values[i] >= 0) {
                    EXPECT_EQ(_wkb[expected_values[i]], geo->get_wkb(i).to_string());
                }
            }

            // GeoColumn forbids debug_item(): drive counts only, check the real payload above.
            _state->set_params({input});
            _state->set_is_required(false);
            const auto result = table_function_test::drive(*_function, &_runtime, _state);
            EXPECT_EQ(expected_rows, result.rows);
        }
    }

    // ISO WKB fixtures, authored here: little-endian POINT (1 2), POINT EMPTY with
    // distinct NaN payloads, big-endian POINT Z (1 2 3), and a self-crossing ring
    // (0 0, 2 2, 0 2, 2 0, 0 0). UNNEST must not parse or normalize any of them.
    const std::vector<std::string> _wkb{a2b_hex("0101000000000000000000f03f0000000000000040"),
                                        a2b_hex("0101000000010000000000f87f020000000000f87f"),
                                        a2b_hex("00000003e93ff000000000000040000000000000004008000000000000"),
                                        a2b_hex("01030000000100000005000000"
                                                "00000000000000000000000000000000"
                                                "00000000000000400000000000000040"
                                                "00000000000000000000000000000040"
                                                "00000000000000400000000000000000"
                                                "00000000000000000000000000000000")};
    TypeDescriptor _type;
    const TableFunction* _function = nullptr;
    TableFunctionState* _state = nullptr;
    RuntimeState _runtime;
};

TEST_P(UnnestGeoTest, BothPathsPreserveRowsAndPayload) {
    const Rows rows{std::nullopt,        std::vector<int>{0, -1}, std::vector<int>{},
                    std::vector<int>{1}, std::vector<int>{2, 3},  std::nullopt,
                    std::vector<int>{0}};
    for (bool dirty : {false, true}) {
        check(make_array(rows, dirty), rows, dirty);
    }
}

TEST_P(UnnestGeoTest, EmptyAndAllNullArrays) {
    for (const Rows& rows : {Rows{std::vector<int>{}, std::vector<int>{}}, Rows{std::nullopt, std::nullopt}}) {
        for (bool dirty : {false, true}) {
            // An empty array cannot carry hidden payload, so only NULL arrays rebuild.
            const bool rebuild = dirty && !rows[0].has_value();
            check(make_array(rows, rebuild), rows, rebuild);
        }
    }
}

TEST_P(UnnestGeoTest, MaterializedConstantArrays) {
    for (const Row& row : {Row{std::vector<int>{0, -1, 1}}, Row{std::vector<int>{}}, Row{std::nullopt}}) {
        ColumnPtr constant = ConstColumn::create(make_array({row}), 3);
        const auto input = ColumnHelper::unpack_and_duplicate_const_column(3, constant);
        ASSERT_FALSE(input->is_constant());
        check(input, Rows(3, row));
    }
}

TEST_P(UnnestGeoTest, NestedArrays) {
    const Rows rows{std::vector<int>{0, -1}, std::nullopt, std::vector<int>{}, std::vector<int>{1, 2, 3}};
    auto inner = make_array(rows, true);
    auto offsets = UInt32Column::create();
    for (uint32_t offset : {0u, 1u, 4u}) {
        offsets->append(offset);
    }
    auto outer = ArrayColumn::create(inner, offsets);
    const auto* function = get_table_function("unnest", {TYPE_ARRAY}, {TYPE_ARRAY});
    ASSERT_NE(nullptr, function);
    _state->set_params({outer});
    _state->set_is_required(true);
    auto [columns, result_offsets] = function->process(&_runtime, _state);
    ASSERT_EQ(1u, columns.size());
    ASSERT_NE(nullptr, result_offsets);
    EXPECT_EQ(offsets.get(), result_offsets.get());
    EXPECT_EQ(inner.get(), columns[0].get());
    ASSERT_EQ(result_offsets->immutable_data().back(), columns[0]->size());
    check(columns[0], rows, true);
}

INSTANTIATE_TEST_SUITE_P(NativeGeo, UnnestGeoTest, testing::Values(TYPE_GEOGRAPHY, TYPE_GEOMETRY));

} // namespace
} // namespace starrocks
