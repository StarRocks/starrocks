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

#include "storage/flexible_row_merger.h"

#include <gtest/gtest.h>

#include <array>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/field.h"
#include "column/schema.h"
#include "common/flexible_partial_update.h"
#include "gen_cpp/Types_types.h"
#include "types/datum.h"

namespace starrocks {

// Memtable schema: c0 INT key, c1..c3 INT nullable, "__cset__", "__op".
class FlexibleRowMergerTest : public testing::Test {
protected:
    struct In {
        int key;
        std::vector<std::string> cols;
        std::array<int, 3> vals{};
        bool del = false;
    };

    struct Out {
        int key;
        // The value of each of c1..c3, nullopt for NULL.
        std::array<std::optional<int>, 3> vals;
        std::set<std::string> declared;
        bool del;
    };

    void SetUp() override {
        Fields fields;
        auto c0 = std::make_shared<Field>(0, "c0", TYPE_INT, false);
        c0->set_is_key(true);
        fields.emplace_back(std::move(c0));
        for (int c = 1; c <= 3; ++c) {
            fields.emplace_back(std::make_shared<Field>(c, "c" + std::to_string(c), TYPE_INT, true));
        }
        fields.emplace_back(std::make_shared<Field>(4, LOAD_CSET_COLUMN, TYPE_SMALLINT, false));
        fields.emplace_back(std::make_shared<Field>(5, "__op", TYPE_TINYINT, false));
        _schema = std::make_unique<Schema>(std::move(fields), PRIMARY_KEYS, std::vector<ColumnId>{0});
        _dict = FlexiblePartialUpdateRegistry::instance()->retain(kTxnId);
        // An id of the load that no row uses, so that the load's ids and the writer's ids differ.
        ASSERT_EQ(0, _dict->intern({"c0", "c3"}));
        _merger = std::make_unique<FlexibleRowMerger>(kTxnId, *_schema, kCsetColumn, kOpColumn);
    }

    void TearDown() override {
        _dict.reset();
        FlexiblePartialUpdateRegistry::instance()->erase(kTxnId);
    }

    // |rows| must be sorted by key, the rows of a key in load order. An undeclared cell is NULL, as the json
    // scanner fills it.
    ChunkPtr build(const std::vector<In>& rows) {
        ChunkPtr chunk = ChunkFactory::new_chunk(*_schema, rows.size());
        for (const auto& row : rows) {
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(row.key));
            for (int c = 0; c < 3; ++c) {
                const std::string name = "c" + std::to_string(c + 1);
                const bool declared = std::find(row.cols.begin(), row.cols.end(), name) != row.cols.end();
                auto* column = chunk->get_column_raw_ptr_by_index(c + 1);
                if (declared) {
                    column->append_datum(Datum(row.vals[c]));
                } else {
                    column->append_nulls(1);
                }
            }
            auto names = row.cols;
            names.emplace_back("c0");
            const auto set_id = _dict->intern(names);
            chunk->get_column_raw_ptr_by_index(kCsetColumn)->append_datum(Datum(static_cast<int16_t>(set_id)));
            chunk->get_column_raw_ptr_by_index(kOpColumn)->append_datum(
                    Datum(static_cast<int8_t>(row.del ? TOpType::DELETE : TOpType::UPSERT)));
        }
        return chunk;
    }

    std::vector<Out> read(const ChunkPtr& chunk) {
        const auto sets = _merger->column_sets();
        std::vector<Out> rows;
        for (size_t i = 0; i < chunk->num_rows(); ++i) {
            Out out;
            out.key = chunk->get_column_by_index(0)->get(i).get_int32();
            for (int c = 0; c < 3; ++c) {
                const auto datum = chunk->get_column_by_index(c + 1)->get(i);
                if (!datum.is_null()) {
                    out.vals[c] = datum.get_int32();
                }
            }
            const auto set_id = chunk->get_column_by_index(kCsetColumn)->get(i).get_int16();
            EXPECT_GE(set_id, 0);
            EXPECT_LT(static_cast<size_t>(set_id), sets.size());
            if (set_id >= 0 && static_cast<size_t>(set_id) < sets.size()) {
                for (auto column : sets[set_id]) {
                    out.declared.emplace(_schema->field(column)->name());
                }
            }
            out.del = chunk->get_column_by_index(kOpColumn)->get(i).get_int8() == TOpType::DELETE;
            rows.push_back(std::move(out));
        }
        return rows;
    }

    static constexpr int64_t kTxnId = 9102030405;
    static constexpr int kCsetColumn = 4;
    static constexpr int kOpColumn = 5;

    std::unique_ptr<Schema> _schema;
    ColumnSetDictPtr _dict;
    std::unique_ptr<FlexibleRowMerger> _merger;
};

// Without a repeated key the rows stay as they are, with this writer's set ids.
TEST_F(FlexibleRowMergerTest, unique_keys_keep_their_rows) {
    auto chunk = build({{1, {"c1"}, {10, 0, 0}}, {2, {"c2", "c3"}, {0, 20, 30}}, {3, {"c1"}, {11, 0, 0}}});
    ASSERT_OK(_merger->merge(&chunk));
    const auto rows = read(chunk);
    ASSERT_EQ(3, rows.size());
    EXPECT_EQ(10, rows[0].vals[0]);
    EXPECT_EQ((std::set<std::string>{"c0", "c1"}), rows[0].declared);
    EXPECT_EQ(20, rows[1].vals[1]);
    EXPECT_EQ(30, rows[1].vals[2]);
    EXPECT_EQ((std::set<std::string>{"c0", "c2", "c3"}), rows[1].declared);
    EXPECT_EQ(11, rows[2].vals[0]);
    EXPECT_EQ(rows[0].declared, rows[2].declared);
    // The load's set {c0, c3} is used by no row, so the writer has only the two sets its rows use.
    EXPECT_EQ(2, _merger->column_sets().size());
}

// Every column of a repeated key comes from the last row that declares it; the merged row declares the union.
TEST_F(FlexibleRowMergerTest, repeated_key_takes_each_column_from_the_last_row_declaring_it) {
    auto chunk = build({
            {1, {"c1"}, {10, 0, 0}},
            {1, {"c2"}, {0, 20, 0}}, // disjoint
            {2, {"c1", "c2"}, {1, 2, 0}},
            {2, {"c2", "c3"}, {0, 22, 23}}, // overlapping
            {3, {"c1"}, {31, 0, 0}},
            {3, {"c1", "c2", "c3"}, {311, 312, 313}}, // superset
            {4, {"c1"}, {41, 0, 0}},
            {4, {"c2"}, {0, 42, 0}},
            {4, {"c1"}, {411, 0, 0}}, // three rows
            {5, {}, {0, 0, 0}},
            {5, {"c3"}, {0, 0, 53}}, // after a row declaring only the key
            {6, {"c3"}, {0, 0, 63}},
            {6, {}, {0, 0, 0}}, // declaring only the key
    });
    ASSERT_OK(_merger->merge(&chunk));
    const auto rows = read(chunk);
    ASSERT_EQ(6, rows.size());

    EXPECT_EQ(1, rows[0].key);
    EXPECT_EQ(10, rows[0].vals[0]);
    EXPECT_EQ(20, rows[0].vals[1]);
    EXPECT_EQ((std::set<std::string>{"c0", "c1", "c2"}), rows[0].declared);

    EXPECT_EQ(1, rows[1].vals[0]);
    EXPECT_EQ(22, rows[1].vals[1]);
    EXPECT_EQ(23, rows[1].vals[2]);
    EXPECT_EQ((std::set<std::string>{"c0", "c1", "c2", "c3"}), rows[1].declared);

    EXPECT_EQ(311, rows[2].vals[0]);
    EXPECT_EQ(312, rows[2].vals[1]);
    EXPECT_EQ(313, rows[2].vals[2]);

    EXPECT_EQ(411, rows[3].vals[0]);
    EXPECT_EQ(42, rows[3].vals[1]);
    EXPECT_FALSE(rows[3].vals[2].has_value());
    EXPECT_EQ((std::set<std::string>{"c0", "c1", "c2"}), rows[3].declared);

    EXPECT_EQ(53, rows[4].vals[2]);
    EXPECT_EQ((std::set<std::string>{"c0", "c3"}), rows[4].declared);

    EXPECT_EQ(63, rows[5].vals[2]);
    EXPECT_EQ((std::set<std::string>{"c0", "c3"}), rows[5].declared);
    for (const auto& row : rows) {
        EXPECT_FALSE(row.del) << row.key;
    }
}

// A key whose last row is a delete is deleted; a delete followed by more rows is dropped with the rows before
// it, as the memtable of a plain load drops them.
TEST_F(FlexibleRowMergerTest, deletes_keep_the_semantics_of_a_plain_load) {
    auto chunk = build({
            {1, {"c1"}, {10, 0, 0}},
            {1, {}, {0, 0, 0}, true},
            {2, {"c1"}, {20, 0, 0}},
            {2, {}, {0, 0, 0}, true},
            {2, {"c2"}, {0, 22, 0}},
            {3, {}, {0, 0, 0}, true},
            {3, {"c3"}, {0, 0, 33}},
            {3, {"c2"}, {0, 32, 0}},
            {4, {"c1"}, {40, 0, 0}},
            {4, {"c2"}, {0, 42, 0}},
            {4, {}, {0, 0, 0}, true},
    });
    ASSERT_OK(_merger->merge(&chunk));
    const auto rows = read(chunk);
    ASSERT_EQ(4, rows.size());

    EXPECT_TRUE(rows[0].del);

    EXPECT_FALSE(rows[1].del);
    EXPECT_FALSE(rows[1].vals[0].has_value());
    EXPECT_EQ(22, rows[1].vals[1]);
    EXPECT_EQ((std::set<std::string>{"c0", "c2"}), rows[1].declared);

    EXPECT_FALSE(rows[2].del);
    EXPECT_EQ(32, rows[2].vals[1]);
    EXPECT_EQ(33, rows[2].vals[2]);
    EXPECT_EQ((std::set<std::string>{"c0", "c2", "c3"}), rows[2].declared);

    EXPECT_TRUE(rows[3].del);
}

// The load's dictionary holds its source column names, which FE matched to the columns ignoring case; a name
// that is not a column of the memtable declares nothing.
TEST_F(FlexibleRowMergerTest, names_match_ignoring_case) {
    auto chunk = build({{1, {}, {0, 0, 0}}});
    const auto set_id = _dict->intern({"C0", "C2", "not_a_column"});
    chunk->get_column_raw_ptr_by_index(kCsetColumn)->resize(0);
    chunk->get_column_raw_ptr_by_index(kCsetColumn)->append_datum(Datum(static_cast<int16_t>(set_id)));
    ASSERT_OK(_merger->merge(&chunk));
    const auto rows = read(chunk);
    ASSERT_EQ(1, rows.size());
    EXPECT_EQ((std::set<std::string>{"c0", "c2"}), rows[0].declared);
}

// A set id that the load's dictionary on this node does not have cannot be decoded, and fails the merge.
TEST_F(FlexibleRowMergerTest, unknown_set_id_fails) {
    auto chunk = build({{1, {"c1"}, {10, 0, 0}}});
    const auto unknown = static_cast<int16_t>(_dict->size());
    chunk->get_column_raw_ptr_by_index(kCsetColumn)->resize(0);
    chunk->get_column_raw_ptr_by_index(kCsetColumn)->append_datum(Datum(unknown));
    auto st = _merger->merge(&chunk);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.message().find("column-set dictionary") != std::string::npos) << st;
}

} // namespace starrocks
