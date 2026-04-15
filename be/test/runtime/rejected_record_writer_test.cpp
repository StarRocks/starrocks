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

#include "runtime/rejected_record_writer.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "rapidjson/document.h"
#include "base/string/slice.h"

namespace starrocks {

namespace {

// Small helper: build a 3-column chunk with one row.
// Columns: int64, varchar, int64. Slot ids 1/2/3.
std::shared_ptr<Chunk> build_one_row_chunk() {
    auto id_col = Int64Column::create();
    id_col->append(10001);
    auto name_col = BinaryColumn::create();
    name_col->append("Alice");
    auto amount_col = Int64Column::create();
    amount_col->append(99);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(id_col), 1);
    chunk->append_column(std::move(name_col), 2);
    chunk->append_column(std::move(amount_col), 3);
    return chunk;
}

// Parse the JSON and return the Document. Assumes valid JSON (helper also
// asserts that).
rapidjson::Document parse(const std::string& json) {
    rapidjson::Document doc;
    EXPECT_FALSE(doc.Parse(json.c_str(), json.size()).HasParseError()) << "bad json: " << json;
    return doc;
}

} // namespace

TEST(RejectedRecordWriterCoreTest, BuildJsonFromChunkEmitsColumnNameKeyedObject) {
    auto chunk = build_one_row_chunk();
    std::vector<std::string> col_names = {"order_id", "customer_name", "amount"};

    std::string json = RejectedRecordWriter::build_json_from_chunk(*chunk, /*row_idx=*/0, col_names);
    rapidjson::Document doc = parse(json);

    ASSERT_TRUE(doc.IsObject());
    ASSERT_TRUE(doc.HasMember("order_id"));
    ASSERT_TRUE(doc.HasMember("customer_name"));
    ASSERT_TRUE(doc.HasMember("amount"));

    // Binary columns are emitted without the debug_item single-quote wrapping
    // so the JSON string matches exactly what was loaded.
    ASSERT_TRUE(doc["customer_name"].IsString());
    EXPECT_STREQ("Alice", doc["customer_name"].GetString());

    // Numeric columns fall back to debug_item() output, which for Int64Column
    // is just the decimal representation ("10001" / "99"). Replay uses
    // `CAST(raw_record->>'amount' AS BIGINT)` to recover the type.
    ASSERT_TRUE(doc["order_id"].IsString());
    EXPECT_STREQ("10001", doc["order_id"].GetString());
    EXPECT_STREQ("99", doc["amount"].GetString());
}

TEST(RejectedRecordWriterCoreTest, BuildJsonFromChunkHandlesPositionalNamesOnMismatch) {
    auto chunk = build_one_row_chunk();
    // Two names for a three-column chunk -- the third column must still show
    // up under a positional fallback rather than being silently dropped.
    std::vector<std::string> col_names = {"order_id", "customer_name"};

    std::string json = RejectedRecordWriter::build_json_from_chunk(*chunk, 0, col_names);
    rapidjson::Document doc = parse(json);

    EXPECT_TRUE(doc.HasMember("order_id"));
    EXPECT_TRUE(doc.HasMember("customer_name"));
    EXPECT_TRUE(doc.HasMember("col_2"));
    EXPECT_STREQ("99", doc["col_2"].GetString());
}

TEST(RejectedRecordWriterCoreTest, BuildJsonFromChunkHandlesEmptyColumnNamesVector) {
    auto chunk = build_one_row_chunk();
    std::vector<std::string> col_names;

    std::string json = RejectedRecordWriter::build_json_from_chunk(*chunk, 0, col_names);
    rapidjson::Document doc = parse(json);

    // All three columns must appear under col_0 / col_1 / col_2 rather than
    // disappearing because col_names was empty.
    EXPECT_TRUE(doc.HasMember("col_0"));
    EXPECT_TRUE(doc.HasMember("col_1"));
    EXPECT_TRUE(doc.HasMember("col_2"));
}

TEST(RejectedRecordWriterCoreTest, BuildJsonFromSlicesPreservesRawTextVerbatim) {
    // CSV parse failure path: per-column raw text slices are still
    // available but have not been appended to a Chunk. The writer must
    // emit them without any CSV-style quoting or escaping of its own --
    // rapidjson handles JSON escaping only.
    std::vector<Slice> col_values = {Slice("10001"), Slice("O'Malley, Jane"), Slice("not_a_number")};
    std::vector<std::string> col_names = {"order_id", "customer_name", "amount"};

    std::string json = RejectedRecordWriter::build_json_from_slices(col_values, col_names);
    rapidjson::Document doc = parse(json);

    EXPECT_STREQ("10001", doc["order_id"].GetString());
    EXPECT_STREQ("O'Malley, Jane", doc["customer_name"].GetString());
    EXPECT_STREQ("not_a_number", doc["amount"].GetString());
}

TEST(RejectedRecordWriterCoreTest, BuildJsonFromRawWrapsUnderReservedKey) {
    // Scanner parse failure path (e.g. CSV column-count mismatch) that
    // cannot split the row at all. The raw line is preserved verbatim under
    // `_raw` so replay consumers have something to work with.
    std::string raw = "10001,Alice,not_a_number\n";

    std::string json = RejectedRecordWriter::build_json_from_raw(raw);
    rapidjson::Document doc = parse(json);

    ASSERT_TRUE(doc.HasMember("_raw"));
    EXPECT_STREQ(raw.c_str(), doc["_raw"].GetString());
}

TEST(RejectedRecordWriterCoreTest, BuildJsonFromChunkEscapesJsonSpecialCharacters) {
    // Make sure embedded double quotes, backslashes, and newlines survive
    // the round-trip through rapidjson without breaking the output's JSON
    // framing.
    auto name_col = BinaryColumn::create();
    name_col->append("She said \"hi\"\\n");
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(name_col), 1);

    std::vector<std::string> col_names = {"message"};
    std::string json = RejectedRecordWriter::build_json_from_chunk(*chunk, 0, col_names);

    rapidjson::Document doc = parse(json);
    EXPECT_STREQ("She said \"hi\"\\n", doc["message"].GetString());
}

} // namespace starrocks
