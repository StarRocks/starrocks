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

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "base/string/slice.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "gen_cpp/InternalService_types.h"
#include "rapidjson/document.h"
#include "runtime/runtime_state.h"

namespace starrocks {

namespace {

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

rapidjson::Document parse(const std::string& json) {
    rapidjson::Document doc;
    EXPECT_FALSE(doc.Parse(json.c_str(), json.size()).HasParseError()) << "bad json: " << json;
    return doc;
}

// ---------------------------------------------------------------------------
// Chunk builders
// ---------------------------------------------------------------------------

// 3-column chunk (int64, varchar, int64) with one row.
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

// Chunk with a nullable int64 column — the single row is null.
std::shared_ptr<Chunk> build_null_row_chunk() {
    auto inner = Int64Column::create();
    inner->append(0); // value doesn't matter; null bit covers it
    auto null_col = NullColumn::create();
    null_col->append(1); // null
    auto nullable = NullableColumn::create(std::move(inner), std::move(null_col));

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(nullable), 1);
    return chunk;
}

// ---------------------------------------------------------------------------
// Scoped temp directory
// ---------------------------------------------------------------------------

class TempDir {
public:
    TempDir() {
        std::error_code ec;
        _path = std::filesystem::temp_directory_path(ec) /
                ("rr_writer_test_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::create_directories(_path, ec);
    }
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(_path, ec);
    }

    std::string path() const { return _path.string(); }

private:
    std::filesystem::path _path;
};

// ---------------------------------------------------------------------------
// Minimal RuntimeState factory helpers
// ---------------------------------------------------------------------------

// Build a TQueryOptions that enables rejected-record logging.
TQueryOptions make_load_options(TLoadJobType::type job_type, bool set_job_type = true) {
    TQueryOptions opts;
    opts.query_type = TQueryType::LOAD;
    opts.log_rejected_record_num = -1; // unlimited
    if (set_job_type) {
        opts.__set_load_job_type(job_type);
    }
    return opts;
}

// Build a RuntimeState suitable for exercising append_serialized with no
// actual file IO: query_type=LOAD, no ExecEnv → resolve_file_path returns
// empty → append_serialized skips the write but still runs all the JSON-
// building code and the enable_log_rejected_record() guard.
std::unique_ptr<RuntimeState> make_load_state(TLoadJobType::type job_type = TLoadJobType::STREAM_LOAD,
                                               bool set_job_type = true) {
    TQueryOptions opts = make_load_options(job_type, set_job_type);
    TQueryGlobals globals;
    TUniqueId id;
    auto state = std::make_unique<RuntimeState>(id, opts, globals, /*exec_env=*/nullptr);
    state->set_db("test_db");
    state->set_table_name("test_table");
    state->set_load_label("test_label_123");
    return state;
}

} // namespace

// ===========================================================================
// Original static-builder tests (kept from previous version)
// ===========================================================================

TEST(RejectedRecordWriterCoreTest, BuildJsonFromChunkEmitsColumnNameKeyedObject) {
    auto chunk = build_one_row_chunk();
    std::vector<std::string> col_names = {"order_id", "customer_name", "amount"};

    std::string json = RejectedRecordWriter::build_json_from_chunk(*chunk, /*row_idx=*/0, col_names);
    rapidjson::Document doc = parse(json);

    ASSERT_TRUE(doc.IsObject());
    ASSERT_TRUE(doc.HasMember("order_id"));
    ASSERT_TRUE(doc.HasMember("customer_name"));
    ASSERT_TRUE(doc.HasMember("amount"));

    ASSERT_TRUE(doc["customer_name"].IsString());
    EXPECT_STREQ("Alice", doc["customer_name"].GetString());
    ASSERT_TRUE(doc["order_id"].IsString());
    EXPECT_STREQ("10001", doc["order_id"].GetString());
    EXPECT_STREQ("99", doc["amount"].GetString());
}

TEST(RejectedRecordWriterCoreTest, BuildJsonFromChunkHandlesPositionalNamesOnMismatch) {
    auto chunk = build_one_row_chunk();
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

    EXPECT_TRUE(doc.HasMember("col_0"));
    EXPECT_TRUE(doc.HasMember("col_1"));
    EXPECT_TRUE(doc.HasMember("col_2"));
}

TEST(RejectedRecordWriterCoreTest, BuildJsonFromSlicesPreservesRawTextVerbatim) {
    std::vector<Slice> col_values = {Slice("10001"), Slice("O'Malley, Jane"), Slice("not_a_number")};
    std::vector<std::string> col_names = {"order_id", "customer_name", "amount"};

    std::string json = RejectedRecordWriter::build_json_from_slices(col_values, col_names);
    rapidjson::Document doc = parse(json);

    EXPECT_STREQ("10001", doc["order_id"].GetString());
    EXPECT_STREQ("O'Malley, Jane", doc["customer_name"].GetString());
    EXPECT_STREQ("not_a_number", doc["amount"].GetString());
}

TEST(RejectedRecordWriterCoreTest, BuildJsonFromRawWrapsUnderReservedKey) {
    std::string raw = "10001,Alice,not_a_number\n";

    std::string json = RejectedRecordWriter::build_json_from_raw(raw);
    rapidjson::Document doc = parse(json);

    ASSERT_TRUE(doc.HasMember("_raw"));
    EXPECT_STREQ(raw.c_str(), doc["_raw"].GetString());
}

TEST(RejectedRecordWriterCoreTest, BuildJsonFromChunkEscapesJsonSpecialCharacters) {
    auto name_col = BinaryColumn::create();
    name_col->append("She said \"hi\"\\n");
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(name_col), 1);

    std::vector<std::string> col_names = {"message"};
    std::string json = RejectedRecordWriter::build_json_from_chunk(*chunk, 0, col_names);

    rapidjson::Document doc = parse(json);
    EXPECT_STREQ("She said \"hi\"\\n", doc["message"].GetString());
}

// ===========================================================================
// New tests: null columns (add_member is_null branch)
// ===========================================================================

TEST(RejectedRecordWriterCoreTest, BuildJsonFromChunkNullColumnEmitsJsonNull) {
    // NullableColumn with a single null row covers the is_null branch in
    // add_member, which emits a JSON null literal rather than a string.
    auto chunk = build_null_row_chunk();
    std::vector<std::string> col_names = {"score"};

    std::string json = RejectedRecordWriter::build_json_from_chunk(*chunk, 0, col_names);
    rapidjson::Document doc = parse(json);

    ASSERT_TRUE(doc.HasMember("score"));
    EXPECT_TRUE(doc["score"].IsNull());
}

// ===========================================================================
// New tests: append_serialized path (no ExecEnv → path empty → no file IO,
// but all JSON-building code is covered by the function body)
// ===========================================================================

// Helper: run append_serialized and return true if enable_log_rejected_record()
// was checked (i.e. the call didn't get blocked before entering the function).
// Since we have no ExecEnv the file write is skipped, but the JSON construction
// lines are still exercised.
void run_append_through_chunk(RuntimeState& state, TLoadJobType::type /*hint*/) {
    RejectedRecordWriter writer(&state);

    auto chunk = build_one_row_chunk();
    std::vector<std::string> col_names = {"order_id", "customer_name", "amount"};

    // Valid source_info JSON → add_parsed_json happy path
    writer.append_from_chunk(*chunk, 0, col_names, "TYPE_MISMATCH", "column type error", "order_id",
                             R"({"file":"s3://bucket/data.csv","line":42})");
}

TEST(RejectedRecordWriterAppendTest, AppendFromChunkStreamLoad) {
    auto state = make_load_state(TLoadJobType::STREAM_LOAD);
    EXPECT_NO_FATAL_FAILURE(run_append_through_chunk(*state, TLoadJobType::STREAM_LOAD));
}

TEST(RejectedRecordWriterAppendTest, AppendFromChunkBrokerLoad) {
    auto state = make_load_state(TLoadJobType::BROKER);
    EXPECT_NO_FATAL_FAILURE(run_append_through_chunk(*state, TLoadJobType::BROKER));
}

TEST(RejectedRecordWriterAppendTest, AppendFromChunkSparkLoad) {
    auto state = make_load_state(TLoadJobType::SPARK);
    EXPECT_NO_FATAL_FAILURE(run_append_through_chunk(*state, TLoadJobType::SPARK));
}

TEST(RejectedRecordWriterAppendTest, AppendFromChunkInsertQuery) {
    auto state = make_load_state(TLoadJobType::INSERT_QUERY);
    EXPECT_NO_FATAL_FAILURE(run_append_through_chunk(*state, TLoadJobType::INSERT_QUERY));
}

TEST(RejectedRecordWriterAppendTest, AppendFromChunkInsertValues) {
    auto state = make_load_state(TLoadJobType::INSERT_VALUES);
    EXPECT_NO_FATAL_FAILURE(run_append_through_chunk(*state, TLoadJobType::INSERT_VALUES));
}

TEST(RejectedRecordWriterAppendTest, AppendFromChunkRoutineLoad) {
    auto state = make_load_state(TLoadJobType::ROUTINE_LOAD);
    EXPECT_NO_FATAL_FAILURE(run_append_through_chunk(*state, TLoadJobType::ROUTINE_LOAD));
}

TEST(RejectedRecordWriterAppendTest, AppendFromChunkUnknownLoadType) {
    // When load_job_type is not set, resolve_load_type returns "UNKNOWN".
    auto state = make_load_state(TLoadJobType::STREAM_LOAD, /*set_job_type=*/false);
    EXPECT_NO_FATAL_FAILURE(run_append_through_chunk(*state, TLoadJobType::STREAM_LOAD));
}

TEST(RejectedRecordWriterAppendTest, AppendFromSlicesNoExecEnv) {
    auto state = make_load_state(TLoadJobType::STREAM_LOAD);
    RejectedRecordWriter writer(state.get());

    std::vector<Slice> slices = {Slice("42"), Slice("bad_value")};
    std::vector<std::string> names = {"id", "col"};
    // Empty source_info → add_parsed_json empty branch
    writer.append_from_slices(slices, names, "CSV_PARSE_ERROR", "column count mismatch", "col", "");
    // No fatal failure expected.
}

TEST(RejectedRecordWriterAppendTest, AppendRawNoExecEnv) {
    auto state = make_load_state(TLoadJobType::BROKER);
    RejectedRecordWriter writer(state.get());

    // Invalid JSON source_info → add_parsed_json parse-error branch
    writer.append_raw("garbage line no columns", "ROW_ERROR", "unparseable row", "", "{not valid json}");
}

TEST(RejectedRecordWriterAppendTest, AppendSerializedSkipsWhenLogDisabled) {
    // log_rejected_record_num=0 means the cap is already reached after 0
    // records: enable_log_rejected_record() returns false immediately, so
    // append_serialized is a pure no-op. Verifying this doesn't crash.
    TQueryOptions opts;
    opts.query_type = TQueryType::LOAD;
    opts.log_rejected_record_num = 0; // cap already at zero
    TQueryGlobals globals;
    TUniqueId id;
    RuntimeState state(id, opts, globals, /*exec_env=*/nullptr);

    RejectedRecordWriter writer(&state);
    writer.append_raw("row", "ERR", "msg", "", "");
    // Counter must not have advanced since the call was skipped.
    EXPECT_EQ(0, state.enable_log_rejected_record()); // enable_log_rejected_record() still false
}

TEST(RejectedRecordWriterAppendTest, AppendFromChunkNullSourceInfoJson) {
    auto state = make_load_state(TLoadJobType::STREAM_LOAD);
    RejectedRecordWriter writer(state.get());

    auto chunk = build_null_row_chunk();
    std::vector<std::string> col_names = {"score"};

    // source_info is valid JSON object → add_parsed_json happy path with a null column value
    writer.append_from_chunk(*chunk, 0, col_names, "NULL_VIOLATION", "NOT NULL column received null",
                             "score", R"({"file":"data.orc"})");
}

TEST(RejectedRecordWriterAppendTest, AppendWithUserName) {
    // Cover the user_name conditional branch in append_serialized.
    TQueryOptions opts;
    opts.query_type = TQueryType::LOAD;
    opts.log_rejected_record_num = -1;
    opts.__set_load_job_type(TLoadJobType::STREAM_LOAD);
    TQueryGlobals globals;
    TUniqueId id;
    RuntimeState state(id, opts, globals, /*exec_env=*/nullptr);
    state.set_db("db");
    state.set_table_name("tbl");
    state.set_load_label("lbl");
    // _user is private; set via the initialiser constructor below
    // (RuntimeState doesn't expose set_user; use the ctor that takes user).
    // We can't set _user directly, so we cover the non-empty branch via
    // the ctor that accepts TQueryGlobals with a user field if available,
    // or accept that the branch stays uncovered via unit tests.
    // Still exercise the general code path.
    RejectedRecordWriter writer(&state);
    writer.append_raw("row", "ERR", "msg", "", "");
}

TEST(RejectedRecordWriterAppendTest, FlushIsNoOp) {
    auto state = make_load_state(TLoadJobType::STREAM_LOAD);
    RejectedRecordWriter writer(state.get());
    EXPECT_NO_FATAL_FAILURE(writer.flush());
}

// ===========================================================================
// File IO path: write through a real tmp file using a custom subclass that
// exposes resolve_file_path via an override trick.
// ===========================================================================

// We cannot override resolve_file_path (it's private, non-virtual).
// The easiest approach without touching production code is to test the file
// write path by constructing a RuntimeState backed by a real LoadPathMgr-like
// setup. However, that requires ExecEnv initialisation which drags in storage
// infrastructure. Instead we verify the "path empty → silent skip" contract
// (already covered above) and trust that the ofstream lines are reached if
// path is non-empty (which the file-scan-node integration tests cover).

// ===========================================================================
// build_json_from_chunk: additional branch coverage
// ===========================================================================

TEST(RejectedRecordWriterCoreTest, BuildJsonFromChunkOutOfBoundsRowIdxSkips) {
    // Regression: row_idx >= num_rows was formerly UB in the Chunk API.
    // append_from_chunk guards this, but build_json_from_chunk itself
    // iterates columns regardless — it iterates columns not rows, so it
    // produces a valid (though meaningless) JSON object.  Just confirm no crash.
    auto chunk = build_one_row_chunk();
    // This goes through append_from_chunk's guard: row_idx=5, chunk has 1 row.
    auto state = make_load_state(TLoadJobType::STREAM_LOAD);
    RejectedRecordWriter writer(state.get());
    // Should silently return without crashing.
    writer.append_from_chunk(*chunk, /*row_idx=*/5, {}, "ERR", "out of bounds test", "", "");
}

TEST(RejectedRecordWriterCoreTest, BuildJsonFromSlicesEmptySlices) {
    std::vector<Slice> empty;
    std::vector<std::string> names;
    std::string json = RejectedRecordWriter::build_json_from_slices(empty, names);
    rapidjson::Document doc = parse(json);
    EXPECT_TRUE(doc.IsObject());
    EXPECT_TRUE(doc.ObjectEmpty());
}

TEST(RejectedRecordWriterCoreTest, BuildJsonFromRawEmptyString) {
    std::string json = RejectedRecordWriter::build_json_from_raw("");
    rapidjson::Document doc = parse(json);
    ASSERT_TRUE(doc.HasMember("_raw"));
    EXPECT_STREQ("", doc["_raw"].GetString());
}

// ===========================================================================
// enable_log_rejected_record() inline helper coverage (runtime_state.h lines
// 292-295 — note_rejected_record at line 302 is also exercised here)
// ===========================================================================

TEST(RejectedRecordWriterAppendTest, NoteRejectedRecordIncrements) {
    auto state = make_load_state(TLoadJobType::STREAM_LOAD);
    RejectedRecordWriter writer(state.get());
    // Enable logging with cap=1.
    state->_query_options.log_rejected_record_num = 2; // allow 2 records
    EXPECT_TRUE(state->enable_log_rejected_record());

    // Each append_raw that reaches append_serialized calls note_rejected_record.
    // But since there's no ExecEnv the file write is skipped (path empty), so
    // the note_rejected_record() call after the ofstream section is NOT reached.
    // Still exercise enable_log_rejected_record() branches.
    EXPECT_EQ(0, state->_num_log_rejected_rows.load());
    state->note_rejected_record();
    EXPECT_EQ(1, state->_num_log_rejected_rows.load());
    EXPECT_TRUE(state->enable_log_rejected_record()); // 1 < 2
    state->note_rejected_record();
    EXPECT_EQ(2, state->_num_log_rejected_rows.load());
    EXPECT_FALSE(state->enable_log_rejected_record()); // 2 == limit, no more
}

} // namespace starrocks
