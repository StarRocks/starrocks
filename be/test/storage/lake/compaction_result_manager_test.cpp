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

#include "storage/lake/compaction_result_manager.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <vector>

#include "base/testutil/assert.h"
#include "common/config.h"
#include "fs/fs_util.h"
#include "storage/lake/join_path.h"

namespace starrocks::lake {

class CompactionResultManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use a unique temp dir per test
        _root = std::filesystem::temp_directory_path() /
                ("crm_test_" + std::to_string(::testing::UnitTest::GetInstance()->random_seed()) + "_" +
                 std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::create_directories(_root);
        _saved_cap = config::lake_autonomous_compaction_local_result_dir_max_bytes;
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(_root, ec);
        config::lake_autonomous_compaction_local_result_dir_max_bytes = _saved_cap;
    }

    static CompactionResultPB make_result(int64_t tablet_id, int64_t base_version, int64_t result_id,
                                          std::vector<uint32_t> input_rowsets) {
        CompactionResultPB r;
        r.set_tablet_id(tablet_id);
        r.set_base_version(base_version);
        r.set_result_id(result_id);
        r.set_finish_time_ms(123);
        auto* op = r.mutable_op_compaction();
        for (auto rid : input_rowsets) op->add_input_rowsets(rid);
        op->set_compact_version(base_version);
        return r;
    }

    std::filesystem::path _root;
    int64_t _saved_cap = 0;
};

TEST_F(CompactionResultManagerTest, append_load_delete) {
    CompactionResultManager mgr({_root.string()});
    ASSERT_OK(mgr.scan_on_startup());

    auto r1 = make_result(100, 10, mgr.next_result_id(100), {1, 2, 3});
    auto r2 = make_result(100, 11, mgr.next_result_id(100), {4, 5});
    auto r3 = make_result(200, 10, mgr.next_result_id(200), {7});

    ASSERT_OK(mgr.append_result(r1));
    ASSERT_OK(mgr.append_result(r2));
    ASSERT_OK(mgr.append_result(r3));

    EXPECT_EQ(3u, mgr.result_count());
    auto p1 = mgr.pending_inputs(100);
    EXPECT_EQ(5u, p1.size());
    EXPECT_TRUE(p1.count(1));
    EXPECT_TRUE(p1.count(5));
    auto p2 = mgr.pending_inputs(200);
    EXPECT_EQ(1u, p2.size());

    // load_results filters by upper_bound_version
    auto loaded_or = mgr.load_results(100, 10);
    ASSERT_TRUE(loaded_or.ok());
    auto loaded = std::move(loaded_or).value();
    EXPECT_EQ(1u, loaded.size());
    EXPECT_EQ(10, loaded[0].base_version());

    auto loaded_all_or = mgr.load_results(100, 100);
    ASSERT_TRUE(loaded_all_or.ok());
    EXPECT_EQ(2u, loaded_all_or.value().size());

    // delete one
    ASSERT_OK(mgr.delete_results(100, {r1.result_id()}));
    EXPECT_EQ(2u, mgr.result_count());
    auto p1_after = mgr.pending_inputs(100);
    EXPECT_EQ(2u, p1_after.size());
    EXPECT_FALSE(p1_after.count(1));
    EXPECT_TRUE(p1_after.count(4));
}

TEST_F(CompactionResultManagerTest, scan_on_startup_rebuilds) {
    {
        CompactionResultManager mgr({_root.string()});
        ASSERT_OK(mgr.scan_on_startup());
        ASSERT_OK(mgr.append_result(make_result(100, 10, 0, {1, 2})));
        ASSERT_OK(mgr.append_result(make_result(100, 11, 1, {3})));
        ASSERT_OK(mgr.append_result(make_result(200, 5, 0, {7})));
    }
    // Simulate restart with a fresh manager pointed at the same dirs.
    CompactionResultManager mgr2({_root.string()});
    ASSERT_OK(mgr2.scan_on_startup());
    EXPECT_EQ(3u, mgr2.result_count());
    auto p100 = mgr2.pending_inputs(100);
    EXPECT_EQ(3u, p100.size());
    EXPECT_TRUE(p100.count(1));
    EXPECT_TRUE(p100.count(2));
    EXPECT_TRUE(p100.count(3));
    // next_result_id continues past the loaded ids
    EXPECT_EQ(2, mgr2.next_result_id(100));
    EXPECT_EQ(1, mgr2.next_result_id(200));
}

TEST_F(CompactionResultManagerTest, corrupt_file_skipped_and_renamed) {
    auto subdir = _root / CompactionResultManager::kSubDir;
    std::filesystem::create_directories(subdir);
    // Write a file with a parsable name but corrupt body.
    auto bad_path = subdir / "100_10_0.pb";
    {
        std::ofstream of(bad_path);
        of << "garbage_not_a_proto";
    }

    CompactionResultManager mgr({_root.string()});
    ASSERT_OK(mgr.scan_on_startup());
    EXPECT_EQ(0u, mgr.result_count());
    EXPECT_FALSE(std::filesystem::exists(bad_path));
    EXPECT_TRUE(std::filesystem::exists(bad_path.string() + ".corrupt"));
}

TEST_F(CompactionResultManagerTest, capacity_limit_rejects_new_results) {
    config::lake_autonomous_compaction_local_result_dir_max_bytes = 1; // very small

    CompactionResultManager mgr({_root.string()});
    ASSERT_OK(mgr.scan_on_startup());
    auto first_or = mgr.append_result(make_result(100, 10, 0, {1, 2, 3}));
    // Either succeeds (first write may exceed cap during append) or rejected.
    if (first_or.ok()) {
        auto second = mgr.append_result(make_result(100, 11, 1, {4}));
        EXPECT_FALSE(second.ok());
    }
}

TEST_F(CompactionResultManagerTest, merge_results_to_txn_log_single) {
    std::vector<CompactionResultPB> results;
    results.push_back(make_result(123, 10, 0, {1, 2, 3}));
    results[0].mutable_op_compaction()->mutable_output_rowset()->set_id(20);

    auto log = merge_results_to_txn_log(results, 123, 999);
    ASSERT_TRUE(log != nullptr);
    EXPECT_EQ(123, log->tablet_id());
    EXPECT_EQ(999, log->txn_id());
    ASSERT_TRUE(log->has_op_parallel_compaction());
    const auto& op = log->op_parallel_compaction();
    ASSERT_EQ(1, op.subtask_compactions_size());
    EXPECT_EQ(0, op.subtask_compactions(0).subtask_id());
    EXPECT_EQ(20, op.subtask_compactions(0).output_rowset().id());
    ASSERT_EQ(1, op.success_subtask_ids_size());
    EXPECT_EQ(0, op.success_subtask_ids(0));
}

TEST_F(CompactionResultManagerTest, merge_results_to_txn_log_multiple_reassigns_subtask_ids) {
    std::vector<CompactionResultPB> results;
    results.push_back(make_result(123, 10, 0, {1, 2, 3}));
    results.push_back(make_result(123, 11, 1, {4, 5}));
    results.push_back(make_result(123, 12, 2, {6}));
    // Pre-existing subtask_ids should be overwritten with 0..N-1.
    results[0].mutable_op_compaction()->set_subtask_id(99);
    results[1].mutable_op_compaction()->set_subtask_id(99);
    results[2].mutable_op_compaction()->set_subtask_id(99);

    auto log = merge_results_to_txn_log(results, 123, 555);
    const auto& op = log->op_parallel_compaction();
    ASSERT_EQ(3, op.subtask_compactions_size());
    EXPECT_EQ(0, op.subtask_compactions(0).subtask_id());
    EXPECT_EQ(1, op.subtask_compactions(1).subtask_id());
    EXPECT_EQ(2, op.subtask_compactions(2).subtask_id());
    ASSERT_EQ(3, op.success_subtask_ids_size());
    EXPECT_EQ(0, op.success_subtask_ids(0));
    EXPECT_EQ(1, op.success_subtask_ids(1));
    EXPECT_EQ(2, op.success_subtask_ids(2));
    // Inputs preserved.
    EXPECT_EQ(3, op.subtask_compactions(0).input_rowsets_size());
    EXPECT_EQ(2, op.subtask_compactions(1).input_rowsets_size());
    EXPECT_EQ(1, op.subtask_compactions(2).input_rowsets_size());
}

TEST_F(CompactionResultManagerTest, persist_helper_from_txn_log) {
    CompactionResultManager mgr({_root.string()});
    ASSERT_OK(mgr.scan_on_startup());

    TxnLogPB txn_log;
    txn_log.set_tablet_id(123);
    txn_log.set_txn_id(999);
    auto* op = txn_log.mutable_op_compaction();
    op->add_input_rowsets(11);
    op->add_input_rowsets(12);
    op->set_compact_version(20);

    ASSERT_OK(persist_compaction_result_from_txn_log(&mgr, 123, 20, txn_log));
    EXPECT_EQ(1u, mgr.result_count());
    auto p = mgr.pending_inputs(123);
    EXPECT_EQ(2u, p.size());
}

// TODO(Phase 2.3 follow-up): integration tests requiring a full Lake fixture
// (TabletManager + UpdateManager + real TabletMetadata) — tracked separately:
//  1. Mixed batch: 50 tablets with results + 50 without -> all 100 reach new_version,
//     first 50 apply OpParallelCompaction, latter 50 take ignore_txn_log path.
//  2. OpParallelCompaction graceful skip: subtask whose input_rowsets are not in
//     base_metadata -> _check_input_rowsets_exist returns false, that subtask
//     skipped, rest applied.
//  3. Idempotency: repeat the same COLLECT_AND_PUBLISH request after a successful
//     publish -> second call is no-op (results already deleted, force_publish
//     handles missing txn_log).
//  4. PK table cross-version (5+ versions): compact_version=v_c < base_version=v_b,
//     OpWrite imports interleaved -> apply via try_replace + max_src_rssid stays
//     correct; output rowset visible after publish.
//  5. PK table partial column update: dcg.versions > op.compact_version ->
//     conflict_check returns true -> op skipped via apply_opcompaction_with_conflict.
//  6. light_publish on/off (enable_light_pk_compaction_publish): identical post-state.

} // namespace starrocks::lake
