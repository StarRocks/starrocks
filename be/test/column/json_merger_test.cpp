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
#include <string>
#include <utility>
#include <vector>

#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "gutil/casts.h"
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

// One flat leaf plus the remain sub-column, the shape a flat_json table has for a document like
// {"o":{"x":N}} once `o.x` has been extracted. Note what the flattener leaves in the remain: it
// walks the keys the document really has, and re-creates every object level it descends into, so a
// row that has `o` at all carries the skeleton {"o":{}} even when nothing under `o` stayed behind.
// A row without `o` carries no `o` in its remain. That asymmetry is the only thing that tells an
// empty object apart from a missing key once the leaves are gone.
Columns make_subtree_columns(const std::vector<std::pair<const int64_t*, std::string>>& rows) {
    auto leaf = NullableColumn::create(Int64Column::create(), NullColumn::create());
    auto remain = JsonColumn::create();
    for (const auto& [value, remain_json] : rows) {
        if (value == nullptr) {
            leaf->append_nulls(1);
        } else {
            leaf->append_datum(Datum(*value));
        }
        remain->append(JsonValue::parse(remain_json).value());
    }

    Columns columns;
    columns.emplace_back(std::move(leaf));
    columns.emplace_back(std::move(remain));
    return columns;
}

// The value a row holds regardless of its null flag. Reading it through the data column rather than
// through get() keeps a wrong null flag reportable instead of throwing, and it is what shows that
// the defect was a null flag alone: the row's data was {} all along, the flag just said NULL.
const JsonValue& value_at(const ColumnPtr& nullable_result, size_t row) {
    const auto* nullable = down_cast<const NullableColumn*>(nullable_result.get());
    return *down_cast<const JsonColumn*>(nullable->data_column().get())->get_object(row);
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

// A row whose value AT the requested path is {} was reported as SQL NULL. Once `o.x` is a
// sub-column, the merge of $.o for such a row adds nothing -- o.x is null for it and the remain
// only holds the skeleton -- and the empty result was read as "this row has no `o`".
TEST(JsonMergerTest, EmptyObjectAtRootPathIsAValueNotNull) {
    const int64_t one = 1;
    // documents, in order: {"o":{"x":1}} / {"o":{}} -- the defect / {"p":1}, no `o` at all, which
    // must stay NULL / {"o":{"z":9}}, answerable from the remain alone
    auto columns = make_subtree_columns({
            {&one, R"({"o":{}})"},
            {nullptr, R"({"o":{}})"},
            {nullptr, R"({"p":1})"},
            {nullptr, R"({"o":{"z":9}})"},
    });

    JsonMerger merger({"o.x"}, {TYPE_BIGINT}, true);
    merger.set_root_path("o");
    merger.set_output_nullable(true);

    auto result = merger.merge(columns);

    ASSERT_TRUE(result->is_nullable());
    ASSERT_EQ(4, result->size());
    EXPECT_FALSE(result->is_null(0));
    EXPECT_EQ(JsonValue::parse(R"({"x": 1})").value(), value_at(result, 0));
    EXPECT_FALSE(result->is_null(1));
    EXPECT_EQ(JsonValue::parse("{}").value(), value_at(result, 1));
    // the guard against over-correcting: no `o` in the remain and no flat value means no `o` at all
    EXPECT_TRUE(result->is_null(2));
    EXPECT_FALSE(result->is_null(3));
    EXPECT_EQ(JsonValue::parse(R"({"z": 9})").value(), value_at(result, 3));
}

// The same below the first level: the remain has to be descended level by level, and {} found at
// the end of that descent is just as much a value as one found at the top.
TEST(JsonMergerTest, EmptyObjectAtNestedRootPathIsAValueNotNull) {
    const int64_t one = 1;
    // documents, in order: {"a":{"b":{"y":1}}} / {"a":{"b":{}}} -- the defect / {"a":{"c":1}}, where
    // `a` is there but `a.b` is not / {"p":1}, where neither is
    auto columns = make_subtree_columns({
            {&one, R"({"a":{"b":{}}})"},
            {nullptr, R"({"a":{"b":{}}})"},
            {nullptr, R"({"a":{"c":1}})"},
            {nullptr, R"({"p":1})"},
    });

    JsonMerger merger({"a.b.y"}, {TYPE_BIGINT}, true);
    merger.set_root_path("a.b");
    merger.set_output_nullable(true);

    auto result = merger.merge(columns);

    ASSERT_EQ(4, result->size());
    EXPECT_FALSE(result->is_null(0));
    EXPECT_EQ(JsonValue::parse(R"({"y": 1})").value(), value_at(result, 0));
    EXPECT_FALSE(result->is_null(1));
    EXPECT_EQ(JsonValue::parse("{}").value(), value_at(result, 1));
    EXPECT_TRUE(result->is_null(2));
    EXPECT_TRUE(result->is_null(3));
}

// Merging the whole column never re-roots the tree, and there is then nothing that says whether a
// key exists, so an empty merge result still has to be NULL. unflatten(), compaction and the merge
// iterator all take this path, and none of them may change.
TEST(JsonMergerTest, WholeColumnMergeStillNullsAnEmptyResult) {
    const int64_t one = 1;
    auto columns = make_subtree_columns({
            {&one, R"({"o":{}})"},
            {nullptr, R"({"o":{}})"},
            {nullptr, "{}"},
    });

    JsonMerger merger({"o.x"}, {TYPE_BIGINT}, true);
    merger.set_output_nullable(true);

    auto result = merger.merge(columns);

    ASSERT_EQ(3, result->size());
    EXPECT_FALSE(result->is_null(0));
    EXPECT_EQ(JsonValue::parse(R"({"o": {"x": 1}})").value(), value_at(result, 0));
    // the whole document is rebuilt, skeleton included, which is why reading the column always
    // showed {"o": {}} for the very row the subtree read called NULL
    EXPECT_FALSE(result->is_null(1));
    EXPECT_EQ(JsonValue::parse(R"({"o": {}})").value(), value_at(result, 1));
    EXPECT_TRUE(result->is_null(2));
}

} // namespace starrocks
