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

#include "column/flat_json/json_merger.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <utility>
#include <vector>

#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "types/json_value.h"
#include "types/logical_type.h"

namespace starrocks {

namespace {

Columns make_flat_columns() {
    auto k1 = NullableColumn::create(Int64Column::create(), NullColumn::create());
    auto k2 = NullableColumn::create(Int64Column::create(), NullColumn::create());
    k1->append_datum(Datum(int64_t{1}));
    k1->append_datum(Datum(int64_t{3}));
    k2->append_datum(Datum(int64_t{2}));
    k2->append_datum(Datum(int64_t{4}));

    Columns columns;
    columns.emplace_back(std::move(k1));
    columns.emplace_back(std::move(k2));
    return columns;
}

} // namespace

TEST(JsonMergerTest, DoesNotRetainNonNullableResult) {
    JsonMerger merger({"k1", "k2"}, {TYPE_BIGINT, TYPE_BIGINT});

    auto result = merger.merge(make_flat_columns());

    ASSERT_EQ(2, result->size());
    EXPECT_EQ(JsonValue::parse(R"({"k1": 1, "k2": 2})").value(), *result->get(0).get_json());
    EXPECT_EQ(JsonValue::parse(R"({"k1": 3, "k2": 4})").value(), *result->get(1).get_json());
    EXPECT_EQ(1, result->use_count());
}

TEST(JsonMergerTest, DoesNotRetainNullableResult) {
    JsonMerger merger({"k1", "k2"}, {TYPE_BIGINT, TYPE_BIGINT});
    merger.set_output_nullable(true);

    auto result = merger.merge(make_flat_columns());

    ASSERT_TRUE(result->is_nullable());
    ASSERT_EQ(2, result->size());
    EXPECT_EQ(JsonValue::parse(R"({"k1": 1, "k2": 2})").value(), *result->get(0).get_json());
    EXPECT_EQ(JsonValue::parse(R"({"k1": 3, "k2": 4})").value(), *result->get(1).get_json());
    EXPECT_EQ(1, result->use_count());
}

} // namespace starrocks
