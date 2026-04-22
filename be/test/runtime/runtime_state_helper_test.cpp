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

// Tests for the rejected-record helpers in runtime_state_helper.cpp
// (lines 132-178):
//   * RuntimeStateHelper::rejected_record_writer(state) lazy accessor
//   * RuntimeStateHelper::append_rejected_record_to_file dispatcher

#include "runtime/runtime_state_helper.h"

#include <gtest/gtest.h>

#include <memory>

#include "gen_cpp/InternalService_types.h"
#include "runtime/rejected_record_writer.h"
#include "runtime/runtime_state.h"

namespace starrocks {

namespace {

// Build a RuntimeState with query_type=LOAD and log_rejected_record_num=-1
// (unlimited) so rejected_record_writer() constructs a writer.
std::unique_ptr<RuntimeState> make_load_state() {
    TQueryOptions opts;
    opts.query_type = TQueryType::LOAD;
    opts.log_rejected_record_num = -1;
    opts.__set_load_job_type(TLoadJobType::STREAM_LOAD);
    TQueryGlobals globals;
    TUniqueId id;
    auto state = std::make_unique<RuntimeState>(id, opts, globals, /*exec_env=*/nullptr);
    state->set_db("test_db");
    state->set_table_name("test_table");
    state->set_load_label("test_label");
    return state;
}

// Build a RuntimeState with query_type=SELECT (not a load query).
std::unique_ptr<RuntimeState> make_select_state() {
    TQueryOptions opts;
    opts.query_type = TQueryType::SELECT;
    opts.log_rejected_record_num = -1;
    TQueryGlobals globals;
    TUniqueId id;
    return std::make_unique<RuntimeState>(id, opts, globals, /*exec_env=*/nullptr);
}

} // namespace

// ===========================================================================
// rejected_record_writer() accessor tests (lines 164-179)
// ===========================================================================

TEST(RuntimeStateHelperRejectedRecordWriterTest, NullStateReturnsNullptr) {
    // Covers the early null guard on lines 165-167.
    RejectedRecordWriter* w = RuntimeStateHelper::rejected_record_writer(nullptr);
    EXPECT_EQ(nullptr, w);
}

TEST(RuntimeStateHelperRejectedRecordWriterTest, SelectQueryReturnsNullptr) {
    // Covers lines 171-173: query_type != LOAD → return nullptr.
    auto state = make_select_state();
    RejectedRecordWriter* w = RuntimeStateHelper::rejected_record_writer(state.get());
    EXPECT_EQ(nullptr, w);
}

TEST(RuntimeStateHelperRejectedRecordWriterTest, LoadQueryReturnsNonNullWriter) {
    // Covers lines 174-178: lazy construction under lock.
    auto state = make_load_state();
    EXPECT_EQ(nullptr, state->rejected_record_writer_or_null()) << "writer should not exist before first call";

    RejectedRecordWriter* w = RuntimeStateHelper::rejected_record_writer(state.get());
    ASSERT_NE(nullptr, w);
    EXPECT_EQ(w, state->rejected_record_writer_or_null()) << "writer should be stored on the state after construction";
}

TEST(RuntimeStateHelperRejectedRecordWriterTest, LoadQueryReturnsSameInstanceOnSecondCall) {
    // Covers the lazy-singleton contract: repeated calls return the same ptr.
    auto state = make_load_state();
    RejectedRecordWriter* w1 = RuntimeStateHelper::rejected_record_writer(state.get());
    RejectedRecordWriter* w2 = RuntimeStateHelper::rejected_record_writer(state.get());
    ASSERT_NE(nullptr, w1);
    EXPECT_EQ(w1, w2) << "should return the same writer on subsequent calls";
}

// ===========================================================================
// append_rejected_record_to_file() dispatcher tests (lines 123-162)
// ===========================================================================

TEST(RuntimeStateHelperAppendRejectedRecordTest, SelectQueryIsNoOp) {
    // Lines 129-131: query_type != LOAD → early return.
    auto state = make_select_state();
    // Should silently do nothing.
    EXPECT_NO_FATAL_FAILURE(RuntimeStateHelper::append_rejected_record_to_file(state.get(), "raw record", "error msg",
                                                                               "source_file.csv"));
    // No writer should have been created.
    EXPECT_EQ(nullptr, state->rejected_record_writer_or_null());
}

TEST(RuntimeStateHelperAppendRejectedRecordTest, LoadQueryCreatesWriterAndDelegates) {
    // Lines 132-162: writer is lazily created and append_raw is called.
    // Since there's no ExecEnv the file write is skipped, but the path to
    // resolve_file_path and the writer construction is exercised.
    auto state = make_load_state();

    EXPECT_NO_FATAL_FAILURE(RuntimeStateHelper::append_rejected_record_to_file(state.get(), "10001,Alice,bad_value",
                                                                               "column type mismatch", "data.csv"));

    // Writer must have been constructed as a side-effect.
    EXPECT_NE(nullptr, state->rejected_record_writer_or_null());
}

TEST(RuntimeStateHelperAppendRejectedRecordTest, LoadQueryEmptySourceIsWrappedWithNoJson) {
    // Lines 150-160: when source is empty, source_info_json stays empty and
    // the writer's append_raw is called with an empty source_info string.
    auto state = make_load_state();

    EXPECT_NO_FATAL_FAILURE(
            RuntimeStateHelper::append_rejected_record_to_file(state.get(), "raw_row", "some error", /*source=*/""));
    EXPECT_NE(nullptr, state->rejected_record_writer_or_null());
}

TEST(RuntimeStateHelperAppendRejectedRecordTest, LoadQueryNonEmptySourceBuildsJsonSourceInfo) {
    // Lines 151-160: when source is non-empty, a JSON object
    // {"source": "<source>"} is constructed and passed as source_info.
    auto state = make_load_state();

    EXPECT_NO_FATAL_FAILURE(RuntimeStateHelper::append_rejected_record_to_file(state.get(), "10001,Alice", "type error",
                                                                               "hdfs://nn/data/part0000.csv"));
    EXPECT_NE(nullptr, state->rejected_record_writer_or_null());
}

} // namespace starrocks
