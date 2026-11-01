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
#include "column/json_column.h"
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

TEST(JsonMergerTest, NonObjectRootDoesNotMatchParentPath) {
    const std::vector<std::string> values = {"null", "[]", "[1,2]", "1", "1.5", R"("text")", "true", "false"};
    for (const auto& path : {"o", "o.n", "\"o.x\""}) {
        auto leaf = NullableColumn::create(Int64Column::create(), NullColumn::create());
        auto remain = JsonColumn::create();
        for (const auto& value : values) {
            leaf->append_nulls(1);
            auto wrapped = JsonValue::parse(R"({"value":)" + value + "}").value();
            remain->append(JsonValue(wrapped.to_vslice().get("value")));
        }
        JsonMerger merger({std::string(path) + ".v"}, {TYPE_BIGINT}, true);
        merger.set_root_path(path);
        merger.set_output_nullable(true);
        auto result = merger.merge({leaf, remain});
        ASSERT_EQ(values.size(), result->size());
        for (size_t i = 0; i < values.size(); ++i) {
            EXPECT_TRUE(result->is_null(i)) << path << ": " << values[i];
        }
    }
}

TEST(JsonMergerTest, WholeDocumentPreservesNonObjectRoots) {
    const std::vector<std::string> values = {"null", "[]", "[1,2]", "1", "1.5", R"("text")", "true", "false"};
    auto leaf = NullableColumn::create(Int64Column::create(), NullColumn::create());
    auto remain = JsonColumn::create();
    for (const auto& value : values) {
        leaf->append_nulls(1);
        auto wrapped = JsonValue::parse(R"({"value":)" + value + "}").value();
        remain->append(JsonValue(wrapped.to_vslice().get("value")));
    }
    for (bool nullable : {false, true}) {
        JsonMerger merger({"o.v"}, {TYPE_BIGINT}, true);
        merger.set_output_nullable(nullable);
        auto result = merger.merge({leaf, remain});
        ASSERT_EQ(values.size(), result->size());
        for (size_t i = 0; i < values.size(); ++i) {
            ASSERT_FALSE(result->is_null(i));
            EXPECT_EQ(*remain->get_object(i), *result->get(i).get_json());
        }
    }
}

TEST(JsonMergerTest, ParentObjectMergesFlatAndRemainingFields) {
    auto leaf = NullableColumn::create(Int64Column::create(), NullColumn::create());
    auto remain = JsonColumn::create();
    leaf->append_datum(Datum(int64_t{10}));
    remain->append(JsonValue::parse(R"({"o":{"s":"x"},"other":1})").value());
    leaf->append_nulls(1);
    remain->append(JsonValue::parse(R"({"other":2})").value());
    JsonMerger merger({"o.v"}, {TYPE_BIGINT}, true);
    merger.set_root_path("o");
    merger.set_output_nullable(true);
    auto result = merger.merge({leaf, remain});
    ASSERT_EQ(2, result->size());
    ASSERT_FALSE(result->is_null(0));
    EXPECT_EQ(JsonValue::parse(R"({"v":10,"s":"x"})").value(), *result->get(0).get_json());
    EXPECT_TRUE(result->is_null(1));
}

TEST(JsonMergerTest, ParentPathPreservesRemainingValue) {
    const std::vector<std::string> values = {"{}",        "[]",   "[1,2]", R"([{"v":1},null])", "null", "123", "1.5",
                                             R"("text")", "true", "false", R"({"s":"x"})"};
    for (const auto& path : {"o", "o.n", "\"o.x\""}) {
        auto leaf = NullableColumn::create(Int64Column::create(), NullColumn::create());
        auto other = NullableColumn::create(Int64Column::create(), NullColumn::create());
        auto remain = JsonColumn::create();
        for (const auto& value : values) {
            leaf->append_nulls(1);
            other->append_datum(Datum(int64_t{99}));
            std::string json;
            if (std::string_view(path) == "o.n") {
                json = R"({"o":{"n":)" + value + "}}";
            } else if (std::string_view(path) == "\"o.x\"") {
                json = R"({"o.x":)" + value + "}";
            } else {
                json = R"({"o":)" + value + "}";
            }
            remain->append(JsonValue::parse(json).value());
        }
        JsonMerger merger({std::string(path) + ".v", "other"}, {TYPE_BIGINT, TYPE_BIGINT}, true);
        merger.set_root_path(path);
        merger.set_output_nullable(true);
        auto result = merger.merge({leaf, other, remain});
        ASSERT_EQ(values.size(), result->size());
        for (size_t i = 0; i < values.size(); ++i) {
            SCOPED_TRACE(std::string(path) + ": " + values[i]);
            EXPECT_FALSE(result->is_null(i));
            if (!result->is_null(i)) {
                auto expected = JsonValue::parse(R"({"value":)" + values[i] + "}").value();
                EXPECT_EQ(JsonValue(expected.to_vslice().get("value")), *result->get(i).get_json());
            }
        }
    }
}

TEST(JsonMergerTest, ParentPathMergesValuesWithoutRemainingParent) {
    auto leaf = NullableColumn::create(Int64Column::create(), NullColumn::create());
    auto remain = JsonColumn::create();
    leaf->append_datum(Datum(int64_t{10}));
    remain->append(JsonValue::parse(R"({"other":1})").value());
    leaf->append_nulls(1);
    remain->append(JsonValue::parse(R"({"other":2})").value());
    leaf->append_datum(Datum(int64_t{20}));
    remain->append(JsonValue::parse(R"({"o":{}})").value());
    leaf->append_datum(Datum(int64_t{30}));
    remain->append_default();
    JsonMerger merger({"o.v"}, {TYPE_BIGINT}, true);
    merger.set_root_path("o");
    merger.set_output_nullable(true);
    auto result = merger.merge({leaf, remain});
    ASSERT_EQ(4, result->size());
    ASSERT_FALSE(result->is_null(0));
    EXPECT_EQ(JsonValue::parse(R"({"v":10})").value(), *result->get(0).get_json());
    EXPECT_TRUE(result->is_null(1));
    ASSERT_FALSE(result->is_null(2));
    EXPECT_EQ(JsonValue::parse(R"({"v":20})").value(), *result->get(2).get_json());
    ASSERT_FALSE(result->is_null(3));
    EXPECT_EQ(JsonValue::parse(R"({"v":30})").value(), *result->get(3).get_json());
}

TEST(JsonMergerTest, ParentPathDoesNotCrossNonObjectPrefix) {
    const std::vector<std::string> values = {"[]", "[1,2]", "null", "123", R"("text")", "true", "{}"};
    auto leaf = NullableColumn::create(Int64Column::create(), NullColumn::create());
    auto remain = JsonColumn::create();
    for (const auto& value : values) {
        leaf->append_nulls(1);
        remain->append(JsonValue::parse(R"({"o":)" + value + "}").value());
    }
    JsonMerger merger({"o.n.v"}, {TYPE_BIGINT}, true);
    merger.set_root_path("o.n");
    merger.set_output_nullable(true);
    auto result = merger.merge({leaf, remain});
    ASSERT_EQ(values.size(), result->size());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_TRUE(result->is_null(i)) << values[i];
    }
}

TEST(JsonMergerTest, ParentPathWithoutRemainPreservesMissingValues) {
    for (const auto& path : {"o", "o.n", "\"o.x\""}) {
        auto leaf = NullableColumn::create(Int64Column::create(), NullColumn::create());
        leaf->append_datum(Datum(int64_t{10}));
        leaf->append_nulls(1);
        JsonMerger merger({std::string(path) + ".v"}, {TYPE_BIGINT}, false);
        merger.set_root_path(path);
        merger.set_output_nullable(true);
        auto result = merger.merge({leaf});
        ASSERT_EQ(2, result->size());
        ASSERT_FALSE(result->is_null(0));
        EXPECT_EQ(JsonValue::parse(R"({"v":10})").value(), *result->get(0).get_json());
        EXPECT_TRUE(result->is_null(1));
    }
}

TEST(JsonMergerTest, ParentPathUsesSameRepeatedKeyAsWholeJson) {
    auto leaf = NullableColumn::create(Int64Column::create(), NullColumn::create());
    leaf->append_nulls(1);
    auto remain = JsonColumn::create();
    auto json = JsonValue::parse(R"({"o":{"s":"x"},"o":{"s":"y"}})").value();
    remain->append(json);
    JsonMerger merger({"o.v"}, {TYPE_BIGINT}, true);
    merger.set_root_path("o");
    merger.set_output_nullable(true);
    auto result = merger.merge({leaf, remain});
    ASSERT_FALSE(result->is_null(0));
    EXPECT_EQ(JsonValue(json.to_vslice().get("o")), *result->get(0).get_json());
}

TEST(JsonMergerTest, WholeDocumentPreservesNoneMarker) {
    auto leaf = NullableColumn::create(Int64Column::create(), NullColumn::create());
    leaf->append_nulls(1);
    auto remain = JsonColumn::create();
    remain->append(JsonValue::from_none());
    JsonMerger merger({"o.v"}, {TYPE_BIGINT}, true);
    merger.set_output_nullable(true);
    auto result = merger.merge({leaf, remain});
    ASSERT_FALSE(result->is_null(0));
    EXPECT_TRUE(result->get(0).get_json()->is_none());
}

TEST(JsonMergerTest, EmptyRootPathPreservesWholeDocument) {
    auto leaf = NullableColumn::create(Int64Column::create(), NullColumn::create());
    leaf->append_datum(Datum(int64_t{10}));
    auto remain = JsonColumn::create();
    remain->append(JsonValue::parse(R"({"o":{"s":"x"},"other":[1,2]})").value());
    JsonMerger merger({"o.v"}, {TYPE_BIGINT}, true);
    merger.set_root_path("");
    merger.set_output_nullable(true);
    auto result = merger.merge({leaf, remain});
    ASSERT_FALSE(result->is_null(0));
    EXPECT_EQ(JsonValue::parse(R"({"o":{"v":10,"s":"x"},"other":[1,2]})").value(), *result->get(0).get_json());
}

} // namespace starrocks
