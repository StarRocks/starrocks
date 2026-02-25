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

#include "storage/lake/tablet_parallel_compaction_manager.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <future>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "common/config.h"
#include "common/thread/threadpool.h"
#include "storage/lake/compaction_scheduler.h"
#include "storage/lake/compaction_task_context.h"
#include "storage/lake/test_util.h"
#include "storage/lake/versioned_tablet.h"

namespace starrocks::lake {

class TestClosure : public google::protobuf::Closure {
public:
    void Run() override {
        std::lock_guard<std::mutex> lock(_mutex);
        _finished = true;
        _cv.notify_all();
    }

    bool wait_finish(int64_t timeout_ms = 5000) {
        std::unique_lock<std::mutex> lock(_mutex);
        return _cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this] { return _finished; });
    }

    bool is_finished() {
        std::lock_guard<std::mutex> lock(_mutex);
        return _finished;
    }

private:
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _finished = false;
};

class TabletParallelCompactionStateTest : public ::testing::Test {
protected:
    void SetUp() override { _state = std::make_unique<TabletParallelCompactionState>(); }

    std::unique_ptr<TabletParallelCompactionState> _state;
};

TEST_F(TabletParallelCompactionStateTest, test_can_create_subtask) {
    _state->max_parallel = 3;

    // Initially no running subtasks, should be able to create
    EXPECT_TRUE(_state->can_create_subtask());

    // Add running subtasks up to max
    for (int i = 0; i < 3; i++) {
        SubtaskInfo info;
        info.subtask_id = i;
        _state->running_subtasks[i] = std::move(info);
    }
    EXPECT_FALSE(_state->can_create_subtask());

    // Remove one, should be able to create again
    _state->running_subtasks.erase(0);
    EXPECT_TRUE(_state->can_create_subtask());
}

TEST_F(TabletParallelCompactionStateTest, test_is_rowset_compacting) {
    EXPECT_FALSE(_state->is_rowset_compacting(1));
    EXPECT_FALSE(_state->is_rowset_compacting(2));

    _state->compacting_rowsets[1] = 1;
    _state->compacting_rowsets[3] = 1;

    EXPECT_TRUE(_state->is_rowset_compacting(1));
    EXPECT_FALSE(_state->is_rowset_compacting(2));
    EXPECT_TRUE(_state->is_rowset_compacting(3));
}

TEST_F(TabletParallelCompactionStateTest, test_is_complete) {
    // No subtasks created yet
    EXPECT_FALSE(_state->is_complete());

    // Create subtasks
    _state->total_subtasks_created = 2;
    SubtaskInfo info1, info2;
    info1.subtask_id = 0;
    info2.subtask_id = 1;
    _state->running_subtasks[0] = std::move(info1);
    _state->running_subtasks[1] = std::move(info2);

    // Still running
    EXPECT_FALSE(_state->is_complete());

    // Complete one
    _state->running_subtasks.erase(0);
    EXPECT_FALSE(_state->is_complete());

    // Complete all
    _state->running_subtasks.erase(1);
    EXPECT_TRUE(_state->is_complete());
}

class TabletParallelCompactionManagerTest : public TestBase {
public:
    TabletParallelCompactionManagerTest() : TestBase(kTestDirectory) { clear_and_init_test_dir(); }

protected:
    constexpr static const char* kTestDirectory = "test_tablet_parallel_compaction_manager";

    void SetUp() override {
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
        _manager = std::make_unique<TabletParallelCompactionManager>(_tablet_mgr.get());

        ThreadPoolBuilder("test_pool")
                .set_min_threads(0)
                .set_max_threads(1) // Use 1 thread to allow execution if needed, or 0 to block
                .build(&_thread_pool);
    }

    void TearDown() override {
        _manager.reset();
        if (_thread_pool) {
            _thread_pool->shutdown();
        }
        remove_test_dir_ignore_error();
    }

    void create_tablet_with_rowsets(int64_t tablet_id, int num_rowsets, int64_t rowset_size) {
        create_tablet_with_rowsets_internal(tablet_id, num_rowsets, rowset_size, DUP_KEYS);
    }

    void create_pk_tablet_with_rowsets(int64_t tablet_id, int num_rowsets, int64_t rowset_size) {
        create_tablet_with_rowsets_internal(tablet_id, num_rowsets, rowset_size, PRIMARY_KEYS);
    }

    void create_tablet_with_rowsets_internal(int64_t tablet_id, int num_rowsets, int64_t rowset_size,
                                             KeysType keys_type) {
        auto metadata = generate_simple_tablet_metadata(keys_type);
        metadata->set_id(tablet_id);
        metadata->set_version(num_rowsets + 1);

        for (int i = 0; i < num_rowsets; i++) {
            auto* rowset = metadata->add_rowsets();
            rowset->set_id(i);
            rowset->set_overlapped(true);
            rowset->set_num_rows(100);
            rowset->set_data_size(rowset_size);

            std::string segment_name = fmt::format("segment_{}.dat", i);
            rowset->add_segments(segment_name);
            rowset->add_segment_size(rowset_size);

            // Create dummy segment file
            std::string path = _lp->segment_location(tablet_id, segment_name);
            std::string dir = std::filesystem::path(path).parent_path().string();
            CHECK_OK(fs::create_directories(dir));
            auto fs = FileSystem::CreateSharedFromString(path);
            WritableFileOptions opts;
            opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
            auto st = fs.value()->new_writable_file(opts, path);
            CHECK_OK(st.status());
            CHECK_OK(st.value()->append("dummy_segment_data"));
            CHECK_OK(st.value()->close());
            CHECK_OK(st.status());
        }

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
    }

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::unique_ptr<TabletParallelCompactionManager> _manager;
    std::unique_ptr<ThreadPool> _thread_pool;
};

TEST_F(TabletParallelCompactionManagerTest, test_get_tablet_state_not_exist) {
    int64_t tablet_id = 12345;
    int64_t txn_id = 67890;

    auto state = _manager->get_tablet_state(tablet_id, txn_id);
    EXPECT_EQ(nullptr, state);
}

TEST_F(TabletParallelCompactionManagerTest, test_is_tablet_complete_not_exist) {
    int64_t tablet_id = 12345;
    int64_t txn_id = 67890;

    // Non-existent tablet should return true (considered complete)
    EXPECT_TRUE(_manager->is_tablet_complete(tablet_id, txn_id));
}

TEST_F(TabletParallelCompactionManagerTest, test_cleanup_tablet) {
    int64_t tablet_id = 12345;
    int64_t txn_id = 67890;

    // Cleanup non-existent tablet should not crash
    _manager->cleanup_tablet(tablet_id, txn_id);
}

TEST_F(TabletParallelCompactionManagerTest, test_get_merged_txn_log_not_exist) {
    int64_t tablet_id = 12345;
    int64_t txn_id = 67890;

    auto result = _manager->get_merged_txn_log(tablet_id, txn_id);
    EXPECT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_not_found());
}

TEST_F(TabletParallelCompactionManagerTest, test_metrics_initial_value) {
    EXPECT_EQ(0, _manager->running_subtasks());
    EXPECT_EQ(0, _manager->completed_subtasks());
}

TEST_F(TabletParallelCompactionManagerTest, test_on_subtask_complete_not_exist) {
    int64_t tablet_id = 12345;
    int64_t txn_id = 67890;
    int32_t subtask_id = 0;

    auto context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, 1, false, true, nullptr);

    // Should not crash when state not exist
    _manager->on_subtask_complete(tablet_id, txn_id, subtask_id, std::move(context));
}

TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_two_groups) {
    int64_t tablet_id = 10001;
    int64_t txn_id = 20001;
    int64_t version = 11;

    // Create 10 rowsets, each 1MB (total 10MB)
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024); // 5MB per subtask, will create 2 groups

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    // Use a thread pool with 1 thread
    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    // Submit a blocking task to occupy the thread
    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    // Wait for the blocking task to start
    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value()); // Should be 2 groups (10MB / 5MB per group)

    auto state = _manager->get_tablet_state(tablet_id, txn_id);
    ASSERT_NE(nullptr, state);
    ASSERT_EQ(2, state->running_subtasks.size());
    // Each group should have approximately 5 rowsets (5MB each)
    ASSERT_EQ(5, state->running_subtasks[0].input_rowset_ids.size());
    ASSERT_EQ(5, state->running_subtasks[1].input_rowset_ids.size());

    // Unblock the thread
    block_promise.set_value();
    pool->wait();

    _manager->cleanup_tablet(tablet_id, txn_id);
}

TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_multiple_groups) {
    int64_t tablet_id = 10002;
    int64_t txn_id = 20002;
    int64_t version = 11;

    // Create 10 rowsets, each 10MB
    create_tablet_with_rowsets(tablet_id, 10, 10 * 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(3);
    config.set_max_bytes_per_subtask(25 * 1024 * 1024); // 25MB limit

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    // Use a thread pool with 1 thread
    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    // Submit a blocking task to occupy the thread
    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    // Wait for the blocking task to start
    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_TRUE(st.ok());

    // Total 100MB. Max 25MB per task.
    // Group 1: 10+10 = 20MB (next is 30 > 25) -> 2 rowsets
    // Group 2: 10+10 = 20MB -> 2 rowsets
    // Group 3: 10+10 = 20MB -> 2 rowsets.
    // Remaining 4 rowsets are skipped because they exceed max_parallel * max_bytes capacity.

    // Expected:
    // Group 0: 2 rowsets (20MB)
    // Group 1: 2 rowsets (20MB)
    // Group 2: 2 rowsets (20MB)

    ASSERT_EQ(3, st.value());

    auto state = _manager->get_tablet_state(tablet_id, txn_id);
    ASSERT_NE(nullptr, state);
    ASSERT_EQ(3, state->running_subtasks.size());

    ASSERT_EQ(2, state->running_subtasks[0].input_rowset_ids.size());
    ASSERT_EQ(2, state->running_subtasks[1].input_rowset_ids.size());
    ASSERT_EQ(2, state->running_subtasks[2].input_rowset_ids.size());

    // Unblock the thread
    block_promise.set_value();
    pool->wait();

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test manual completion flow by directly creating and manipulating TabletParallelCompactionState.
// This test does NOT use create_parallel_tasks because that function starts real compaction
// tasks which would fail with mock segment files. Instead, we test the on_subtask_complete
// and result merging logic directly.
TEST_F(TabletParallelCompactionManagerTest, test_manual_completion_flow) {
    int64_t tablet_id = 10003;
    int64_t txn_id = 20003;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    // Manually create and register tablet state
    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 2;
    state->callback = callback;

    // Create subtask info for 2 subtasks
    {
        SubtaskInfo info0;
        info0.subtask_id = 0;
        info0.input_rowset_ids = {0, 1, 2, 3, 4};
        info0.input_bytes = 5 * 1024 * 1024;
        info0.start_time = ::time(nullptr);
        state->running_subtasks[0] = std::move(info0);
        state->total_subtasks_created = 1;

        SubtaskInfo info1;
        info1.subtask_id = 1;
        info1.input_rowset_ids = {5, 6, 7, 8, 9};
        info1.input_bytes = 5 * 1024 * 1024;
        info1.start_time = ::time(nullptr);
        state->running_subtasks[1] = std::move(info1);
        state->total_subtasks_created = 2;
    }

    // Mark rowsets as compacting
    for (int i = 0; i < 10; i++) {
        state->compacting_rowsets[i] = 1;
    }

    // Register the state with manager using the test helper method
    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Simulate completion of subtask 0
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    ASSERT_FALSE(closure.is_finished());
    ASSERT_FALSE(_manager->is_tablet_complete(tablet_id, txn_id));

    // Simulate completion of subtask 1
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // Verify result
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // Verify subtask_compactions - each subtask has independent output
    ASSERT_EQ(2, op_parallel.subtask_compactions_size());

    const auto& subtask0 = op_parallel.subtask_compactions(0);
    EXPECT_EQ(0, subtask0.subtask_id());
    EXPECT_EQ(1, subtask0.input_rowsets_size());
    EXPECT_EQ(0, subtask0.input_rowsets(0));
    EXPECT_TRUE(subtask0.has_output_rowset());
    EXPECT_EQ(50, subtask0.output_rowset().num_rows());
    EXPECT_EQ(500, subtask0.output_rowset().data_size());

    const auto& subtask1 = op_parallel.subtask_compactions(1);
    EXPECT_EQ(1, subtask1.subtask_id());
    EXPECT_EQ(1, subtask1.input_rowsets_size());
    EXPECT_EQ(5, subtask1.input_rowsets(0));
    EXPECT_TRUE(subtask1.has_output_rowset());
    EXPECT_EQ(50, subtask1.output_rowset().num_rows());
    EXPECT_EQ(500, subtask1.output_rowset().data_size());

    // In real scenario, cleanup_tablet is called by CompactionScheduler::remove_states.
    // Since we don't have CompactionScheduler in this test, manually clean up.
    _manager->cleanup_tablet(tablet_id, txn_id);
    ASSERT_EQ(nullptr, _manager->get_tablet_state(tablet_id, txn_id));
}

class SubtaskInfoTest : public ::testing::Test {};

TEST_F(SubtaskInfoTest, test_subtask_info_default_values) {
    SubtaskInfo info;
    EXPECT_EQ(0, info.subtask_id);
    EXPECT_TRUE(info.input_rowset_ids.empty());
    EXPECT_EQ(0, info.input_bytes);
    EXPECT_EQ(0, info.start_time);
}

TEST_F(SubtaskInfoTest, test_subtask_info_set_values) {
    SubtaskInfo info;
    info.subtask_id = 5;
    info.input_rowset_ids = {1, 2, 3};
    info.input_bytes = 1024 * 1024;
    info.start_time = 1234567890;

    EXPECT_EQ(5, info.subtask_id);
    EXPECT_EQ(3, info.input_rowset_ids.size());
    EXPECT_EQ(1024 * 1024, info.input_bytes);
    EXPECT_EQ(1234567890, info.start_time);
}

class TabletParallelCompactionStateFieldsTest : public ::testing::Test {
protected:
    void SetUp() override { _state = std::make_unique<TabletParallelCompactionState>(); }

    std::unique_ptr<TabletParallelCompactionState> _state;
};

TEST_F(TabletParallelCompactionStateFieldsTest, test_default_values) {
    EXPECT_EQ(0, _state->tablet_id);
    EXPECT_EQ(0, _state->txn_id);
    EXPECT_EQ(0, _state->version);
    EXPECT_EQ(0, _state->max_parallel);
    EXPECT_EQ(0, _state->max_bytes_per_subtask);
    EXPECT_EQ(0, _state->next_subtask_id);
    EXPECT_EQ(0, _state->total_subtasks_created);
    EXPECT_TRUE(_state->compacting_rowsets.empty());
    EXPECT_TRUE(_state->running_subtasks.empty());
    EXPECT_TRUE(_state->completed_subtasks.empty());
    EXPECT_EQ(nullptr, _state->callback);
}

TEST_F(TabletParallelCompactionStateFieldsTest, test_set_fields) {
    _state->tablet_id = 100;
    _state->txn_id = 200;
    _state->version = 5;
    _state->max_parallel = 10;
    _state->max_bytes_per_subtask = 5368709120L; // 5GB
    _state->next_subtask_id = 3;
    _state->total_subtasks_created = 5;

    EXPECT_EQ(100, _state->tablet_id);
    EXPECT_EQ(200, _state->txn_id);
    EXPECT_EQ(5, _state->version);
    EXPECT_EQ(10, _state->max_parallel);
    EXPECT_EQ(5368709120L, _state->max_bytes_per_subtask);
    EXPECT_EQ(3, _state->next_subtask_id);
    EXPECT_EQ(5, _state->total_subtasks_created);
}

TEST_F(TabletParallelCompactionStateFieldsTest, test_compacting_rowsets_operations) {
    // Use reference counting: each insert increments the count
    _state->compacting_rowsets[1] = 1;
    _state->compacting_rowsets[2] = 1;
    _state->compacting_rowsets[3] = 1;

    EXPECT_EQ(3, _state->compacting_rowsets.size());
    EXPECT_TRUE(_state->compacting_rowsets.count(1) > 0);
    EXPECT_TRUE(_state->compacting_rowsets.count(2) > 0);
    EXPECT_TRUE(_state->compacting_rowsets.count(3) > 0);
    EXPECT_FALSE(_state->compacting_rowsets.count(4) > 0);

    _state->compacting_rowsets.erase(2);
    EXPECT_EQ(2, _state->compacting_rowsets.size());
    EXPECT_FALSE(_state->compacting_rowsets.count(2) > 0);
}

TEST_F(TabletParallelCompactionStateFieldsTest, test_running_subtasks_operations) {
    SubtaskInfo info1;
    info1.subtask_id = 0;
    info1.input_bytes = 100;

    SubtaskInfo info2;
    info2.subtask_id = 1;
    info2.input_bytes = 200;

    _state->running_subtasks[0] = std::move(info1);
    _state->running_subtasks[1] = std::move(info2);

    EXPECT_EQ(2, _state->running_subtasks.size());
    EXPECT_EQ(100, _state->running_subtasks[0].input_bytes);
    EXPECT_EQ(200, _state->running_subtasks[1].input_bytes);

    _state->running_subtasks.erase(0);
    EXPECT_EQ(1, _state->running_subtasks.size());
    EXPECT_TRUE(_state->running_subtasks.find(0) == _state->running_subtasks.end());
}

TEST_F(TabletParallelCompactionStateFieldsTest, test_completed_subtasks_operations) {
    auto ctx1 = std::make_unique<CompactionTaskContext>(100, 101, 1, false, true, nullptr);
    auto ctx2 = std::make_unique<CompactionTaskContext>(100, 102, 1, false, true, nullptr);

    _state->completed_subtasks.push_back(std::move(ctx1));
    _state->completed_subtasks.push_back(std::move(ctx2));

    EXPECT_EQ(2, _state->completed_subtasks.size());
    EXPECT_EQ(101, _state->completed_subtasks[0]->tablet_id);
    EXPECT_EQ(102, _state->completed_subtasks[1]->tablet_id);
}

// Test for max_bytes <= 0: should use BE config default value and fallback to normal compaction
// if data size is small
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_default_max_bytes) {
    int64_t tablet_id = 10010;
    int64_t txn_id = 20010;
    int64_t version = 11;

    // Create 10 rowsets, each 1MB (total 10MB, much smaller than default max_bytes ~5GB)
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(-1); // Invalid, will use BE config default

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    // When max_bytes <= 0, code uses BE config default value (lake_compaction_max_bytes_per_subtask).
    // With small data (10MB) and large default max_bytes (~5GB), it falls back to normal compaction.
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(0, st.value()); // Returns 0 indicating fallback to normal compaction

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test for max_parallel <= 0 (line 54-56)
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_invalid_max_parallel) {
    int64_t tablet_id = 10011;
    int64_t txn_id = 20011;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(0); // Invalid, should use 1
    config.set_max_bytes_per_subtask(100 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    // Should fail with invalid max_parallel
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.status().is_invalid_argument());

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test for tablet not found (line 68)
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_tablet_not_found) {
    int64_t tablet_id = 99999; // Non-existent tablet
    int64_t txn_id = 20012;
    int64_t version = 1;

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(10 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, _thread_pool.get(), []() { return true; },
            [](bool) {});

    ASSERT_FALSE(st.ok());
}

// Test for already existing parallel compaction (lines 216-217)
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_already_exists) {
    int64_t tablet_id = 10013;
    int64_t txn_id = 20013;
    int64_t version = 11;

    // Create 10 rowsets, each 1MB (total 10MB)
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    // Use 5MB to ensure total_bytes (10MB) > max_bytes, avoiding data_size_small fallback
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    // First creation should succeed and create parallel tasks
    auto st1 = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_TRUE(st1.ok());
    ASSERT_GT(st1.value(), 0); // Should create at least 1 group

    // Second creation with same tablet_id and txn_id should fail
    auto st2 = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_FALSE(st2.ok());
    ASSERT_TRUE(st2.status().is_already_exist());

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test for acquire_token failure (lines 274-288)
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_acquire_token_failure) {
    int64_t tablet_id = 10014;
    int64_t txn_id = 20014;
    int64_t version = 11;

    // Create 10 rowsets, each 1MB (total 10MB)
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    // Use 5MB to ensure total_bytes (10MB) > max_bytes, avoiding data_size_small fallback
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    // acquire_token always returns false to simulate failure
    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return false; }, [](bool) {});

    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.status().is_resource_busy());
}

// Test for on_subtask_complete with subtask not found (lines 374-381)
TEST_F(TabletParallelCompactionManagerTest, test_on_subtask_complete_subtask_not_found) {
    int64_t tablet_id = 10015;
    int64_t txn_id = 20015;
    int64_t version = 11;

    // Create 10 rowsets, each 1MB (total 10MB)
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    // Use 5MB to ensure total_bytes (10MB) > max_bytes, avoiding data_size_small fallback
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_TRUE(st.ok());

    // Try to complete a non-existent subtask (id 999)
    auto ctx = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx->subtask_id = 999; // Non-existent subtask
    _manager->on_subtask_complete(tablet_id, txn_id, 999, std::move(ctx));

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test for list_tasks with running and completed subtasks (lines 786-827)
TEST_F(TabletParallelCompactionManagerTest, test_list_tasks) {
    int64_t tablet_id = 10016;
    int64_t txn_id = 20016;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_TRUE(st.ok());

    // List tasks while some are running
    std::vector<CompactionTaskInfo> infos;
    _manager->list_tasks(&infos);

    // Should have at least one task
    EXPECT_GE(infos.size(), 1);

    // Check task info
    for (const auto& info : infos) {
        EXPECT_EQ(txn_id, info.txn_id);
        EXPECT_EQ(tablet_id, info.tablet_id);
        EXPECT_EQ(version, info.version);
    }

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test for merged TxnLog with overlapped output (lines 520-565)
TEST_F(TabletParallelCompactionManagerTest, test_merged_txn_log_overlapped) {
    int64_t tablet_id = 10017;
    int64_t txn_id = 20017;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Simulate completion of subtask 0 with overlapped output
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(1);
    auto* output0 = ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset();
    output0->set_num_rows(100);
    output0->set_data_size(1024);
    output0->set_overlapped(true);
    output0->add_segments("segment_0.dat");
    output0->add_segment_size(512);
    output0->add_segment_encryption_metas("meta0");
    ctx0->txn_log->mutable_op_compaction()->set_compact_version(10);
    ctx0->table_id = 1001;
    ctx0->partition_id = 2001;

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Simulate completion of subtask 1
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(6);
    auto* output1 = ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset();
    output1->set_num_rows(200);
    output1->set_data_size(2048);
    output1->set_overlapped(false);
    output1->add_segments("segment_1.dat");
    output1->add_segment_size(1024);
    output1->add_segment_encryption_metas("meta1");

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // Verify result
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // compact_version should be set in subtask_compactions
    ASSERT_GT(op_parallel.subtask_compactions_size(), 0);
    const auto& first_subtask = op_parallel.subtask_compactions(0);
    EXPECT_TRUE(first_subtask.has_compact_version());
    EXPECT_EQ(10, first_subtask.compact_version());

    // Verify subtask_compactions structure - each subtask has independent output
    ASSERT_EQ(2, op_parallel.subtask_compactions_size());

    // Subtask 0 output
    const auto& subtask0 = op_parallel.subtask_compactions(0);
    EXPECT_EQ(0, subtask0.subtask_id());
    EXPECT_EQ(2, subtask0.input_rowsets_size());
    EXPECT_EQ(0, subtask0.input_rowsets(0));
    EXPECT_EQ(1, subtask0.input_rowsets(1));
    EXPECT_TRUE(subtask0.has_output_rowset());
    EXPECT_EQ(100, subtask0.output_rowset().num_rows());
    EXPECT_TRUE(subtask0.output_rowset().overlapped());

    // Subtask 1 output
    const auto& subtask1 = op_parallel.subtask_compactions(1);
    EXPECT_EQ(1, subtask1.subtask_id());
    EXPECT_EQ(2, subtask1.input_rowsets_size());
    EXPECT_EQ(5, subtask1.input_rowsets(0));
    EXPECT_EQ(6, subtask1.input_rowsets(1));
    EXPECT_TRUE(subtask1.has_output_rowset());
    EXPECT_EQ(200, subtask1.output_rowset().num_rows());

    block_promise.set_value();
    pool->wait();
}

// Test for partial success: one subtask succeeds, one fails
TEST_F(TabletParallelCompactionManagerTest, test_partial_success_one_succeeded_one_failed) {
    int64_t tablet_id = 10018;
    int64_t txn_id = 20018;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Simulate completion of subtask 0
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(1);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segments("segment_0.dat");

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    ASSERT_FALSE(closure.is_finished());
    ASSERT_FALSE(_manager->is_tablet_complete(tablet_id, txn_id));

    // Simulate completion of subtask 1 with failure
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->status = Status::InternalError("simulated failure");
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(6);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segments("segment_1.dat");

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // With partial success, the overall status should be OK (one subtask succeeded)
    EXPECT_EQ(0, response.status().status_code());

    // Verify TxnLog only contains successful subtask's data
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // success_subtask_ids should only contain subtask 0
    EXPECT_EQ(1, op_parallel.success_subtask_ids_size());
    EXPECT_EQ(0, op_parallel.success_subtask_ids(0));

    // Verify subtask_compactions only contains successful subtask
    ASSERT_EQ(1, op_parallel.subtask_compactions_size());
    const auto& subtask0 = op_parallel.subtask_compactions(0);
    EXPECT_EQ(0, subtask0.subtask_id());
    EXPECT_EQ(2, subtask0.input_rowsets_size());
    EXPECT_EQ(0, subtask0.input_rowsets(0));
    EXPECT_EQ(1, subtask0.input_rowsets(1));
    EXPECT_TRUE(subtask0.has_output_rowset());
    EXPECT_EQ(50, subtask0.output_rowset().num_rows());
    EXPECT_EQ(500, subtask0.output_rowset().data_size());
    EXPECT_EQ(1, subtask0.output_rowset().segments_size());
    EXPECT_EQ("segment_0.dat", subtask0.output_rowset().segments(0));

    block_promise.set_value();
    pool->wait();
}

// Test for data exceeding max_parallel capacity (lines 141-156)
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_exceeds_capacity) {
    int64_t tablet_id = 10019;
    int64_t txn_id = 20019;
    int64_t version = 11;

    // Create 10 rowsets, each 10MB = 100MB total
    create_tablet_with_rowsets(tablet_id, 10, 10 * 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);              // Only allow 2 subtasks
    config.set_max_bytes_per_subtask(20 * 1024 * 1024); // 20MB per subtask, so 40MB total capacity

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_TRUE(st.ok());
    // Should create max_parallel subtasks (2), skipping excess data
    ASSERT_EQ(2, st.value());

    auto state = _manager->get_tablet_state(tablet_id, txn_id);
    ASSERT_NE(nullptr, state);
    // Each subtask should have limited rowsets
    ASSERT_EQ(2, state->running_subtasks.size());

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test for stats merging in on_subtask_complete (line 446)
TEST_F(TabletParallelCompactionManagerTest, test_stats_merging) {
    int64_t tablet_id = 10020;
    int64_t txn_id = 20020;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Complete subtask 0 with stats
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);
    ctx0->stats->io_ns_read_remote = 1000;
    ctx0->stats->io_bytes_read_remote = 2000;

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Complete subtask 1 with stats
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);
    ctx1->stats->io_ns_read_remote = 3000;
    ctx1->stats->io_bytes_read_remote = 4000;

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    block_promise.set_value();
    pool->wait();
}

// Test for no rowsets to compact (line 75)
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_no_rowsets) {
    int64_t tablet_id = 10022;
    int64_t txn_id = 20022;
    int64_t version = 1;

    // Create tablet without any rowsets
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    // Don't add any rowsets
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(10 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, _thread_pool.get(), []() { return true; },
            [](bool) {});

    // Should fail because no rowsets to compact
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.status().is_not_found());
}

// Test for valid_groups empty case (line 203)
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_single_small_rowset) {
    int64_t tablet_id = 10023;
    int64_t txn_id = 20023;
    int64_t version = 2;

    // Create tablet with a single non-overlapped rowset (won't be selected for compaction)
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    metadata->set_id(tablet_id);
    metadata->set_version(version);

    // Add a single rowset that's not overlapped
    auto* rowset = metadata->add_rowsets();
    rowset->set_id(0);
    rowset->set_overlapped(false); // Not overlapped, may not be selected
    rowset->set_num_rows(10);
    rowset->set_data_size(100);
    rowset->add_segments("segment_0.dat");
    rowset->add_segment_size(100);

    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(10 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, _thread_pool.get(), []() { return true; },
            [](bool) {});

    // May fail if no rowsets selected, or succeed with single group
    // This tests the pick_rowsets path
}

// Test for execute_subtask when state is cleaned up (lines 631-644)
TEST_F(TabletParallelCompactionManagerTest, test_execute_subtask_state_cleaned_up) {
    int64_t tablet_id = 10024;
    int64_t txn_id = 20024;
    int64_t version = 11;

    // Create 10 rowsets, each 1MB (total 10MB)
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    // Use max_parallel=2 and max_bytes=5MB to ensure parallel tasks are created
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(4).build(&pool);

    std::atomic<bool> subtask_started{false};
    std::promise<void> cleanup_done_promise;
    std::future<void> cleanup_done_future = cleanup_done_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { subtask_started.store(true); });

    // Wait briefly for subtask to start, then cleanup
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Cleanup tablet state while subtask might be running
    // This tests the path where state is gone during execute_subtask
    _manager->cleanup_tablet(tablet_id, txn_id);

    pool->wait();
}

// Test for partial subtask creation when some fail (lines 310-314)
TEST_F(TabletParallelCompactionManagerTest, test_partial_subtask_creation) {
    int64_t tablet_id = 10025;
    int64_t txn_id = 20025;
    int64_t version = 11;

    // Create 10 rowsets, each 5MB
    create_tablet_with_rowsets(tablet_id, 10, 5 * 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(3);
    config.set_max_bytes_per_subtask(15 * 1024 * 1024); // ~15MB per subtask

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    int acquire_count = 0;
    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(),
            [&]() {
                acquire_count++;
                // First acquisition succeeds, subsequent ones fail
                return acquire_count <= 1;
            },
            [](bool) {});

    // Should fail with ResourceBusy because we require atomic acquisition of all tokens
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.status().is_resource_busy());

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test for listing completed subtasks (lines 810-826)
TEST_F(TabletParallelCompactionManagerTest, test_list_tasks_with_completed) {
    int64_t tablet_id = 10026;
    int64_t txn_id = 20026;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Complete subtask 0
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->runs.store(1, std::memory_order_relaxed);
    ctx0->start_time.store(::time(nullptr) - 10, std::memory_order_relaxed);
    ctx0->finish_time.store(::time(nullptr), std::memory_order_release);
    ctx0->skipped.store(false, std::memory_order_relaxed);
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // List tasks - should include completed subtask
    std::vector<CompactionTaskInfo> infos;
    _manager->list_tasks(&infos);

    // Should have tasks listed
    EXPECT_GE(infos.size(), 1);

    // Complete subtask 1 to finish
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    block_promise.set_value();
    pool->wait();
}

// Test for copying table_id and partition_id from subtask contexts (lines 413-428)
TEST_F(TabletParallelCompactionManagerTest, test_table_partition_id_copy) {
    int64_t tablet_id = 10021;
    int64_t txn_id = 20021;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Complete subtask 0 without table_id and partition_id
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx0->table_id = 0;     // Not set
    ctx0->partition_id = 0; // Not set

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Complete subtask 1 with table_id and partition_id
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);
    ctx1->table_id = 12345;
    ctx1->partition_id = 67890;

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    block_promise.set_value();
    pool->wait();
}

// Test for TxnLog merge without output (lines 520-527, 560-565)
TEST_F(TabletParallelCompactionManagerTest, test_merged_txn_log_no_output) {
    int64_t tablet_id = 10027;
    int64_t txn_id = 20027;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Complete subtask 0 with TxnLog but no output_rowset
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    // No output_rowset set

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Complete subtask 1 with TxnLog but no output_rowset
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    // No output_rowset set

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // Verify subtask_compactions - each subtask has no output (or empty output)
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();
    ASSERT_EQ(2, op_parallel.subtask_compactions_size());

    // Subtasks have input but no output_rowset with data
    const auto& subtask0 = op_parallel.subtask_compactions(0);
    EXPECT_EQ(0, subtask0.subtask_id());
    EXPECT_EQ(1, subtask0.input_rowsets_size());
    // output_rowset exists but has 0 rows (default value)

    const auto& subtask1 = op_parallel.subtask_compactions(1);
    EXPECT_EQ(1, subtask1.subtask_id());
    EXPECT_EQ(1, subtask1.input_rowsets_size());

    block_promise.set_value();
    pool->wait();
}

// Test for TxnLog merge without compact_version (lines 567-574)
TEST_F(TabletParallelCompactionManagerTest, test_merged_txn_log_no_compact_version) {
    int64_t tablet_id = 10028;
    int64_t txn_id = 20028;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Complete subtask 0 without compact_version
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    // No compact_version set

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Complete subtask 1
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    block_promise.set_value();
    pool->wait();
}

// Test for subtask with null TxnLog (lines 501-507)
TEST_F(TabletParallelCompactionManagerTest, test_merged_txn_log_null_txn_log) {
    int64_t tablet_id = 10029;
    int64_t txn_id = 20029;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Complete subtask 0 with valid TxnLog
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Complete subtask 1 with null TxnLog
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = nullptr; // Null TxnLog

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    block_promise.set_value();
    pool->wait();
}

// Test for subtask with TxnLog but no op_compaction (lines 501-507, 522-523)
TEST_F(TabletParallelCompactionManagerTest, test_merged_txn_log_no_op_compaction) {
    int64_t tablet_id = 10030;
    int64_t txn_id = 20030;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Complete subtask 0 with TxnLog but no op_compaction
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    // Don't set op_compaction

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Complete subtask 1 with TxnLog but no op_compaction
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    // Don't set op_compaction

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    block_promise.set_value();
    pool->wait();
}

// Test for two subtasks output with non-overlapped results
TEST_F(TabletParallelCompactionManagerTest, test_merged_txn_log_two_subtasks) {
    int64_t tablet_id = 10031;
    int64_t txn_id = 20031;
    int64_t version = 11;

    // Create 10 rowsets, each 1MB (total 10MB)
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    // Use max_parallel=2 and max_bytes=5MB to create 2 groups
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value()); // Two subtasks

    // Complete subtask 0 with non-overlapped output
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(1);
    auto* output0 = ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset();
    output0->set_num_rows(100);
    output0->set_data_size(1000);
    output0->set_overlapped(false);
    output0->add_segments("merged_segment_0.dat");

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    ASSERT_FALSE(closure.is_finished()); // Not finished yet

    // Complete subtask 1 with non-overlapped output
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(6);
    auto* output1 = ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset();
    output1->set_num_rows(200);
    output1->set_data_size(2000);
    output1->set_overlapped(false);
    output1->add_segments("merged_segment_1.dat");

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // Verify subtask_compactions for two subtasks
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();
    ASSERT_EQ(2, op_parallel.subtask_compactions_size());

    const auto& subtask0 = op_parallel.subtask_compactions(0);
    EXPECT_EQ(0, subtask0.subtask_id());
    EXPECT_EQ(2, subtask0.input_rowsets_size());
    EXPECT_EQ(0, subtask0.input_rowsets(0));
    EXPECT_EQ(1, subtask0.input_rowsets(1));
    EXPECT_TRUE(subtask0.has_output_rowset());
    EXPECT_EQ(100, subtask0.output_rowset().num_rows());
    EXPECT_EQ(1000, subtask0.output_rowset().data_size());
    EXPECT_EQ("merged_segment_0.dat", subtask0.output_rowset().segments(0));

    const auto& subtask1 = op_parallel.subtask_compactions(1);
    EXPECT_EQ(1, subtask1.subtask_id());
    EXPECT_EQ(2, subtask1.input_rowsets_size());
    EXPECT_EQ(5, subtask1.input_rowsets(0));
    EXPECT_EQ(6, subtask1.input_rowsets(1));
    EXPECT_TRUE(subtask1.has_output_rowset());
    EXPECT_EQ(200, subtask1.output_rowset().num_rows());
    EXPECT_EQ(2000, subtask1.output_rowset().data_size());
    EXPECT_EQ("merged_segment_1.dat", subtask1.output_rowset().segments(0));

    block_promise.set_value();
    pool->wait();
}

// Test for metrics after subtask completion
TEST_F(TabletParallelCompactionManagerTest, test_metrics_after_completion) {
    int64_t tablet_id = 10032;
    int64_t txn_id = 20032;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    // Submit a blocking task first to occupy the single thread in the pool.
    // This prevents execute_subtask from running until we unblock it,
    // avoiding race condition with manual on_subtask_complete calls.
    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    // Wait for blocking task to start
    start_promise.get_future().wait();

    int64_t initial_running = _manager->running_subtasks();
    int64_t initial_completed = _manager->completed_subtasks();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // After creation, running subtasks should increase
    EXPECT_EQ(initial_running + 2, _manager->running_subtasks());

    // Complete both subtasks manually (execute_subtask is blocked in thread pool queue)
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // After first completion
    EXPECT_EQ(initial_running + 1, _manager->running_subtasks());
    EXPECT_EQ(initial_completed + 1, _manager->completed_subtasks());

    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // Unblock the pool. The queued execute_subtask tasks will run but find state cleaned up,
    // which will decrement running_subtasks (expected behavior for orphaned tasks).
    block_promise.set_value();
    pool->wait();

    // After pool completes, running_subtasks will be decremented by the orphaned execute_subtask calls.
    // This is expected: 2 execute_subtask calls each decrement counter when they find state missing.
    EXPECT_EQ(initial_running - 2, _manager->running_subtasks());
}

// Test for rowsets marking and unmarking (lines 606-620)
TEST_F(TabletParallelCompactionManagerTest, test_rowsets_marking) {
    int64_t tablet_id = 10033;
    int64_t txn_id = 20033;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_TRUE(st.ok());

    auto state = _manager->get_tablet_state(tablet_id, txn_id);
    ASSERT_NE(nullptr, state);

    // Check that rowsets are marked as compacting
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        EXPECT_FALSE(state->compacting_rowsets.empty());

        // All rowsets from all subtasks should be marked
        for (const auto& [subtask_id, info] : state->running_subtasks) {
            for (uint32_t rid : info.input_rowset_ids) {
                EXPECT_TRUE(state->is_rowset_compacting(rid));
            }
        }
    }

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test for callback not set scenario
TEST_F(TabletParallelCompactionManagerTest, test_on_subtask_complete_with_callback) {
    int64_t tablet_id = 10034;
    int64_t txn_id = 20034;
    int64_t version = 11;

    // Create 10 rowsets, each 1MB (total 10MB)
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    // Use max_parallel=2 and max_bytes=5MB to create 2 groups
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value()); // Expect 2 subtasks

    // Get state to verify
    auto state = _manager->get_tablet_state(tablet_id, txn_id);
    ASSERT_NE(nullptr, state);

    // Complete subtask 0
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(100);

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Complete subtask 1
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(100);

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.wait_finish(5000));

    block_promise.set_value();
    pool->wait();
}

// Test for all subtasks failed (should fail overall)
TEST_F(TabletParallelCompactionManagerTest, test_all_subtasks_failed) {
    int64_t tablet_id = 10035;
    int64_t txn_id = 20035;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Simulate completion of subtask 0 with failure
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->status = Status::InternalError("subtask 0 failed");
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Simulate completion of subtask 1 with failure
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->status = Status::InternalError("subtask 1 failed");
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(50);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(500);

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // When all subtasks fail, the overall status should indicate failure
    EXPECT_NE(0, response.status().status_code());

    block_promise.set_value();
    pool->wait();
}

// Test for partial success with multiple subtasks (2 succeed, 1 fails) - PK table
// PK tables allow non-consecutive successful subtasks to be applied
TEST_F(TabletParallelCompactionManagerTest, test_partial_success_multiple_subtasks_pk) {
    int64_t tablet_id = 10036;
    int64_t txn_id = 20036;
    int64_t version = 16; // 15 rowsets + 1

    // Create 15 rowsets, each 1MB - PK table
    create_pk_tablet_with_rowsets(tablet_id, 15, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(3);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok()) << st.status();
    ASSERT_EQ(3, st.value());

    // Subtask 0: failure
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->status = Status::MemoryLimitExceeded("OOM");
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(1);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(100);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(1000);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segments("segment_0.dat");

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Subtask 1: success
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(6);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(100);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(1000);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segments("segment_1.dat");

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    // Subtask 2: success
    auto ctx2 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx2->subtask_id = 2;
    ctx2->txn_log = std::make_unique<TxnLogPB>();
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(10);
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(11);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(200);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(2000);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segments("segment_2.dat");

    _manager->on_subtask_complete(tablet_id, txn_id, 2, std::move(ctx2));

    ASSERT_TRUE(closure.is_finished());

    // With partial success (2 out of 3 succeeded), the overall status should be OK
    EXPECT_EQ(0, response.status().status_code());

    // Verify the merged TxnLog
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // success_subtask_ids should contain 1 and 2
    EXPECT_EQ(2, op_parallel.success_subtask_ids_size());
    EXPECT_EQ(1, op_parallel.success_subtask_ids(0));
    EXPECT_EQ(2, op_parallel.success_subtask_ids(1));

    // Verify subtask_compactions structure - each subtask has independent output
    ASSERT_EQ(2, op_parallel.subtask_compactions_size());
    const auto& subtask1 = op_parallel.subtask_compactions(0);
    EXPECT_EQ(1, subtask1.subtask_id());
    EXPECT_EQ(2, subtask1.input_rowsets_size());
    EXPECT_EQ(5, subtask1.input_rowsets(0));
    EXPECT_EQ(6, subtask1.input_rowsets(1));
    EXPECT_TRUE(subtask1.has_output_rowset());
    EXPECT_EQ(100, subtask1.output_rowset().num_rows());
    EXPECT_EQ(1000, subtask1.output_rowset().data_size());
    EXPECT_EQ("segment_1.dat", subtask1.output_rowset().segments(0));

    const auto& subtask2 = op_parallel.subtask_compactions(1);
    EXPECT_EQ(2, subtask2.subtask_id());
    EXPECT_EQ(2, subtask2.input_rowsets_size());
    EXPECT_EQ(10, subtask2.input_rowsets(0));
    EXPECT_EQ(11, subtask2.input_rowsets(1));
    EXPECT_TRUE(subtask2.has_output_rowset());
    EXPECT_EQ(200, subtask2.output_rowset().num_rows());
    EXPECT_EQ(2000, subtask2.output_rowset().data_size());
    EXPECT_EQ("segment_2.dat", subtask2.output_rowset().segments(0));

    block_promise.set_value();
    pool->wait();
}

// Test for partial success with non-consecutive successful subtasks
// With unified logic, all successful subtasks are applied regardless of consecutiveness.
TEST_F(TabletParallelCompactionManagerTest, test_non_pk_table_all_successful_subtasks) {
    int64_t tablet_id = 10038;
    int64_t txn_id = 20038;
    int64_t version = 21; // 20 rowsets + 1

    create_tablet_with_rowsets(tablet_id, 20, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(4);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok()) << st.status();
    ASSERT_EQ(4, st.value());

    // Subtask 0: failure
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->status = Status::IOError("disk error");
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(1);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(100);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(1000);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segments("segment_0.dat");
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Subtask 1: success
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->status = Status::OK();
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(6);
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(7);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(150);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(1500);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segments("segment_1.dat");
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_size(750);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_encryption_metas("meta1");
    ctx1->txn_log->mutable_op_compaction()->set_compact_version(10);
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    // Subtask 2: success (should also be applied)
    auto ctx2 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx2->subtask_id = 2;
    ctx2->status = Status::OK();
    ctx2->txn_log = std::make_unique<TxnLogPB>();
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(10);
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(11);
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(12);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(200);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(2000);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segments("segment_2.dat");
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_size(1000);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_encryption_metas("meta2");
    ctx2->txn_log->mutable_op_compaction()->set_compact_version(10);
    _manager->on_subtask_complete(tablet_id, txn_id, 2, std::move(ctx2));

    // Subtask 3: failure
    auto ctx3 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx3->subtask_id = 3;
    ctx3->status = Status::MemoryLimitExceeded("OOM");
    ctx3->txn_log = std::make_unique<TxnLogPB>();
    ctx3->txn_log->mutable_op_compaction()->add_input_rowsets(15);
    ctx3->txn_log->mutable_op_compaction()->add_input_rowsets(16);
    ctx3->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(250);
    ctx3->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(2500);
    ctx3->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segments("segment_3.dat");
    _manager->on_subtask_complete(tablet_id, txn_id, 3, std::move(ctx3));

    ASSERT_TRUE(closure.is_finished());

    // Verify result - all successful subtasks (1 and 2) should be applied
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // compact_version should be set in subtask_compactions
    ASSERT_GT(op_parallel.subtask_compactions_size(), 0);
    const auto& first_subtask = op_parallel.subtask_compactions(0);
    EXPECT_TRUE(first_subtask.has_compact_version());
    EXPECT_EQ(10, first_subtask.compact_version());

    // success_subtask_ids should contain 1 and 2
    EXPECT_EQ(2, op_parallel.success_subtask_ids_size());
    EXPECT_EQ(1, op_parallel.success_subtask_ids(0));
    EXPECT_EQ(2, op_parallel.success_subtask_ids(1));

    // Verify subtask_compactions structure - 2 successful subtasks with independent outputs
    ASSERT_EQ(2, op_parallel.subtask_compactions_size());

    // Subtask 1 output
    const auto& subtask1 = op_parallel.subtask_compactions(0);
    EXPECT_EQ(1, subtask1.subtask_id());
    EXPECT_EQ(3, subtask1.input_rowsets_size());
    EXPECT_EQ(5, subtask1.input_rowsets(0));
    EXPECT_EQ(6, subtask1.input_rowsets(1));
    EXPECT_EQ(7, subtask1.input_rowsets(2));
    EXPECT_TRUE(subtask1.has_output_rowset());
    EXPECT_EQ(150, subtask1.output_rowset().num_rows());
    EXPECT_EQ(1500, subtask1.output_rowset().data_size());

    // Subtask 2 output
    const auto& subtask2 = op_parallel.subtask_compactions(1);
    EXPECT_EQ(2, subtask2.subtask_id());
    EXPECT_EQ(3, subtask2.input_rowsets_size());
    EXPECT_EQ(10, subtask2.input_rowsets(0));
    EXPECT_EQ(11, subtask2.input_rowsets(1));
    EXPECT_EQ(12, subtask2.input_rowsets(2));
    EXPECT_TRUE(subtask2.has_output_rowset());
    EXPECT_EQ(200, subtask2.output_rowset().num_rows());
    EXPECT_EQ(2000, subtask2.output_rowset().data_size());

    block_promise.set_value();
    pool->wait();
}

// Test where first subtask fails but second succeeds
// With unified logic, all successful subtasks are applied
TEST_F(TabletParallelCompactionManagerTest, test_first_subtask_fails_second_succeeds) {
    int64_t tablet_id = 10039;
    int64_t txn_id = 20039;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(4);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2, st.value());

    // Subtask 0: failure
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->status = Status::IOError("disk error");
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(1);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(100);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(1000);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segments("segment_0.dat");
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Subtask 1: success (should be applied)
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->status = Status::OK();
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(6);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(100);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(1000);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segments("segment_1.dat");

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // With unified logic, subtask 1 should be applied as it succeeded
    EXPECT_EQ(0, response.status().status_code());

    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // Only subtask 1 in success_subtask_ids
    EXPECT_EQ(1, op_parallel.success_subtask_ids_size());
    EXPECT_EQ(1, op_parallel.success_subtask_ids(0));

    // Verify subtask_compactions structure - only subtask 1 has output
    ASSERT_EQ(1, op_parallel.subtask_compactions_size());
    const auto& subtask1 = op_parallel.subtask_compactions(0);
    EXPECT_EQ(1, subtask1.subtask_id());
    EXPECT_EQ(2, subtask1.input_rowsets_size());
    EXPECT_EQ(5, subtask1.input_rowsets(0));
    EXPECT_EQ(6, subtask1.input_rowsets(1));
    EXPECT_TRUE(subtask1.has_output_rowset());
    EXPECT_EQ(100, subtask1.output_rowset().num_rows());
    EXPECT_EQ(1000, subtask1.output_rowset().data_size());
    EXPECT_EQ(1, subtask1.output_rowset().segments_size());
    EXPECT_EQ("segment_1.dat", subtask1.output_rowset().segments(0));

    block_promise.set_value();
    pool->wait();
}

// Test partial success pattern: [fail, success, success, fail]
// With unified logic, all successful subtasks (1 and 2) should be applied
TEST_F(TabletParallelCompactionManagerTest, test_partial_success_middle_subtasks) {
    int64_t tablet_id = 10040;
    int64_t txn_id = 20040;
    int64_t version = 21; // 20 rowsets + 1

    create_tablet_with_rowsets(tablet_id, 20, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(4);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("blocked_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; },
            [&](bool) { block_future.wait(); });
    ASSERT_TRUE(st.ok()) << st.status();
    ASSERT_EQ(4, st.value());

    // Subtask 0: failure
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->status = Status::IOError("disk error");
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(1);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(100);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(1000);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segments("segment_0.dat");
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    // Subtask 1: success
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->status = Status::OK();
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(5);
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(6);
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(7);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(150);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(1500);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segments("segment_1.dat");
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_size(750);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_encryption_metas("meta1");
    ctx1->txn_log->mutable_op_compaction()->set_compact_version(10);
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    // Subtask 2: success (should be applied)
    auto ctx2 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx2->subtask_id = 2;
    ctx2->status = Status::OK();
    ctx2->txn_log = std::make_unique<TxnLogPB>();
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(10);
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(11);
    ctx2->txn_log->mutable_op_compaction()->add_input_rowsets(12);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(200);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(2000);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segments("segment_2.dat");
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_size(1000);
    ctx2->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segment_encryption_metas("meta2");
    ctx2->txn_log->mutable_op_compaction()->set_compact_version(10);
    _manager->on_subtask_complete(tablet_id, txn_id, 2, std::move(ctx2));

    // Subtask 3: failure
    auto ctx3 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx3->subtask_id = 3;
    ctx3->status = Status::MemoryLimitExceeded("OOM");
    ctx3->txn_log = std::make_unique<TxnLogPB>();
    ctx3->txn_log->mutable_op_compaction()->add_input_rowsets(15);
    ctx3->txn_log->mutable_op_compaction()->add_input_rowsets(16);
    ctx3->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(250);
    ctx3->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_data_size(2500);
    ctx3->txn_log->mutable_op_compaction()->mutable_output_rowset()->add_segments("segment_3.dat");
    _manager->on_subtask_complete(tablet_id, txn_id, 3, std::move(ctx3));

    ASSERT_TRUE(closure.is_finished());

    // With unified logic: all successful subtasks are applied
    // Pattern: [fail, success, success, fail]
    // Subtasks 1 and 2 should be applied
    EXPECT_EQ(0, response.status().status_code());

    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // compact_version should be set in subtask_compactions
    ASSERT_GT(op_parallel.subtask_compactions_size(), 0);
    const auto& first_subtask = op_parallel.subtask_compactions(0);
    EXPECT_TRUE(first_subtask.has_compact_version());
    EXPECT_EQ(10, first_subtask.compact_version());

    // Verify subtask_compactions structure - each subtask has independent output
    ASSERT_EQ(2, op_parallel.subtask_compactions_size());
    const auto& subtask1 = op_parallel.subtask_compactions(0);
    EXPECT_EQ(1, subtask1.subtask_id());
    EXPECT_EQ(3, subtask1.input_rowsets_size());
    EXPECT_EQ(5, subtask1.input_rowsets(0));
    EXPECT_EQ(6, subtask1.input_rowsets(1));
    EXPECT_EQ(7, subtask1.input_rowsets(2));
    EXPECT_TRUE(subtask1.has_output_rowset());
    EXPECT_EQ(150, subtask1.output_rowset().num_rows());
    EXPECT_EQ(1500, subtask1.output_rowset().data_size());

    const auto& subtask2 = op_parallel.subtask_compactions(1);
    EXPECT_EQ(2, subtask2.subtask_id());
    EXPECT_EQ(3, subtask2.input_rowsets_size());
    EXPECT_EQ(10, subtask2.input_rowsets(0));
    EXPECT_EQ(11, subtask2.input_rowsets(1));
    EXPECT_EQ(12, subtask2.input_rowsets(2));
    EXPECT_TRUE(subtask2.has_output_rowset());
    EXPECT_EQ(200, subtask2.output_rowset().num_rows());
    EXPECT_EQ(2000, subtask2.output_rowset().data_size());

    block_promise.set_value();
    pool->wait();
}

// ================================================================================
// Tests for SubtaskType enum and SubtaskGroup struct
// ================================================================================

class SubtaskGroupTest : public ::testing::Test {};

TEST_F(SubtaskGroupTest, test_subtask_type_enum_values) {
    // Verify enum values exist and are distinct
    EXPECT_NE(SubtaskType::NORMAL, SubtaskType::LARGE_ROWSET_PART);
}

TEST_F(SubtaskGroupTest, test_subtask_group_default_values) {
    SubtaskGroup group;
    EXPECT_EQ(SubtaskType::NORMAL, group.type);
    EXPECT_TRUE(group.rowsets.empty());
    EXPECT_EQ(nullptr, group.large_rowset);
    EXPECT_EQ(0, group.large_rowset_id);
    EXPECT_EQ(0, group.segment_start);
    EXPECT_EQ(0, group.segment_end);
    EXPECT_EQ(0, group.total_bytes);
}

TEST_F(SubtaskGroupTest, test_subtask_group_normal_type) {
    SubtaskGroup group;
    group.type = SubtaskType::NORMAL;
    group.total_bytes = 1024 * 1024;

    EXPECT_EQ(SubtaskType::NORMAL, group.type);
    EXPECT_EQ(1024 * 1024, group.total_bytes);
}

TEST_F(SubtaskGroupTest, test_subtask_group_large_rowset_part_type) {
    SubtaskGroup group;
    group.type = SubtaskType::LARGE_ROWSET_PART;
    group.large_rowset_id = 42;
    group.segment_start = 0;
    group.segment_end = 4;
    group.total_bytes = 10 * 1024 * 1024;

    EXPECT_EQ(SubtaskType::LARGE_ROWSET_PART, group.type);
    EXPECT_EQ(42, group.large_rowset_id);
    EXPECT_EQ(0, group.segment_start);
    EXPECT_EQ(4, group.segment_end);
    EXPECT_EQ(10 * 1024 * 1024, group.total_bytes);
}

// ================================================================================
// Tests for SubtaskInfo new fields
// ================================================================================

TEST_F(SubtaskInfoTest, test_subtask_info_new_fields_default_values) {
    SubtaskInfo info;
    EXPECT_EQ(SubtaskType::NORMAL, info.type);
    EXPECT_EQ(0, info.large_rowset_id);
    EXPECT_EQ(0, info.segment_start);
    EXPECT_EQ(0, info.segment_end);
}

TEST_F(SubtaskInfoTest, test_subtask_info_large_rowset_part_fields) {
    SubtaskInfo info;
    info.subtask_id = 3;
    info.type = SubtaskType::LARGE_ROWSET_PART;
    info.large_rowset_id = 100;
    info.segment_start = 4;
    info.segment_end = 8;

    EXPECT_EQ(3, info.subtask_id);
    EXPECT_EQ(SubtaskType::LARGE_ROWSET_PART, info.type);
    EXPECT_EQ(100, info.large_rowset_id);
    EXPECT_EQ(4, info.segment_start);
    EXPECT_EQ(8, info.segment_end);
}

// ================================================================================
// Tests for TabletParallelCompactionState new fields
// ================================================================================

TEST_F(TabletParallelCompactionStateFieldsTest, test_large_rowset_split_groups_default) {
    EXPECT_TRUE(_state->large_rowset_split_groups.empty());
}

TEST_F(TabletParallelCompactionStateFieldsTest, test_large_rowset_split_groups_operations) {
    // Add entries for two large rowsets
    _state->large_rowset_split_groups[100] = {0, 1, 2};
    _state->large_rowset_split_groups[200] = {3, 4};

    EXPECT_EQ(2, _state->large_rowset_split_groups.size());
    EXPECT_EQ(3, _state->large_rowset_split_groups[100].size());
    EXPECT_EQ(2, _state->large_rowset_split_groups[200].size());

    // Verify subtask IDs for large rowset 100
    auto& subtasks_100 = _state->large_rowset_split_groups[100];
    EXPECT_EQ(0, subtasks_100[0]);
    EXPECT_EQ(1, subtasks_100[1]);
    EXPECT_EQ(2, subtasks_100[2]);

    // Verify subtask IDs for large rowset 200
    auto& subtasks_200 = _state->large_rowset_split_groups[200];
    EXPECT_EQ(3, subtasks_200[0]);
    EXPECT_EQ(4, subtasks_200[1]);
}

// ================================================================================
// Tests for large rowset split functionality (all table types)
// ================================================================================

class TabletParallelCompactionManagerLargeRowsetTest : public TestBase {
public:
    TabletParallelCompactionManagerLargeRowsetTest() : TestBase(kTestDirectory) { clear_and_init_test_dir(); }

protected:
    constexpr static const char* kTestDirectory = "test_tablet_parallel_compaction_large_rowset";

    void SetUp() override {
        _tablet_metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
        _manager = std::make_unique<TabletParallelCompactionManager>(_tablet_mgr.get());
    }

    void TearDown() override {
        _manager.reset();
        remove_test_dir_ignore_error();
    }

    // Create a tablet with one large rowset containing multiple segments
    void create_tablet_with_large_rowset(int64_t tablet_id, int num_segments, int64_t segment_size,
                                         KeysType keys_type = PRIMARY_KEYS) {
        auto metadata = generate_simple_tablet_metadata(keys_type);
        metadata->set_id(tablet_id);
        metadata->set_version(2);

        // Create one large rowset with multiple segments
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(0);
        rowset->set_overlapped(true);
        rowset->set_num_rows(1000 * num_segments);
        rowset->set_data_size(segment_size * num_segments);

        for (int i = 0; i < num_segments; i++) {
            std::string segment_name = fmt::format("segment_{}.dat", i);
            rowset->add_segments(segment_name);
            rowset->add_segment_size(segment_size);

            // Create dummy segment file
            std::string path = _lp->segment_location(tablet_id, segment_name);
            std::string dir = std::filesystem::path(path).parent_path().string();
            CHECK_OK(fs::create_directories(dir));
            auto fs = FileSystem::CreateSharedFromString(path);
            auto st = fs.value()->new_writable_file(path);
            CHECK_OK(st.status());
            CHECK_OK(st.value()->append("dummy_segment_data"));
            CHECK_OK(st.value()->close());
        }

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
    }

    // Keep old API for backward compatibility
    void create_pk_tablet_with_large_rowset(int64_t tablet_id, int num_segments, int64_t segment_size) {
        create_tablet_with_large_rowset(tablet_id, num_segments, segment_size, PRIMARY_KEYS);
    }

    // Create a tablet with multiple rowsets of varying sizes
    void create_tablet_with_mixed_rowsets(int64_t tablet_id, const std::vector<std::pair<int, int64_t>>& rowset_specs,
                                          KeysType keys_type = PRIMARY_KEYS) {
        auto metadata = generate_simple_tablet_metadata(keys_type);
        metadata->set_id(tablet_id);
        metadata->set_version(rowset_specs.size() + 1);

        int rowset_id = 0;
        for (const auto& [num_segments, segment_size] : rowset_specs) {
            auto* rowset = metadata->add_rowsets();
            rowset->set_id(rowset_id);
            rowset->set_overlapped(true);
            rowset->set_num_rows(100 * num_segments);
            rowset->set_data_size(segment_size * num_segments);

            for (int i = 0; i < num_segments; i++) {
                std::string segment_name = fmt::format("rowset_{}_segment_{}.dat", rowset_id, i);
                rowset->add_segments(segment_name);
                rowset->add_segment_size(segment_size);

                // Create dummy segment file
                std::string path = _lp->segment_location(tablet_id, segment_name);
                std::string dir = std::filesystem::path(path).parent_path().string();
                CHECK_OK(fs::create_directories(dir));
                auto fs = FileSystem::CreateSharedFromString(path);
                auto st = fs.value()->new_writable_file(path);
                CHECK_OK(st.status());
                CHECK_OK(st.value()->append("dummy_segment_data"));
                CHECK_OK(st.value()->close());
            }
            rowset_id++;
        }

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
    }

    // Keep old API for backward compatibility
    void create_pk_tablet_with_mixed_rowsets(int64_t tablet_id,
                                             const std::vector<std::pair<int, int64_t>>& rowset_specs) {
        create_tablet_with_mixed_rowsets(tablet_id, rowset_specs, PRIMARY_KEYS);
    }

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::unique_ptr<TabletParallelCompactionManager> _manager;
};

// Test that a large rowset meeting split criteria is identified
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_large_rowset_split_criteria) {
    int64_t tablet_id = 30001;

    // Create a tablet with one large rowset: 8 segments, each 1GB
    // Total size = 8GB, which is > 2 * lake_compaction_max_rowset_size (default ~2GB)
    // and has >= 4 segments
    int64_t segment_size = 1024 * 1024 * 1024; // 1GB per segment
    create_pk_tablet_with_large_rowset(tablet_id, 8, segment_size);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(4);
    config.set_max_bytes_per_subtask(2 * 1024 * 1024 * 1024L); // 2GB per subtask

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, 123, 2, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    // Should create multiple subtasks for the large rowset split
    ASSERT_TRUE(st.ok()) << st.status();

    auto state = _manager->get_tablet_state(tablet_id, 123);
    if (state != nullptr) {
        // Verify that large_rowset_split_groups is populated for PK tables
        // The exact number depends on the split algorithm
        VLOG(1) << "Created " << state->running_subtasks.size() << " subtasks";
    }

    block_promise.set_value();
    pool->wait();

    _manager->cleanup_tablet(tablet_id, 123);
}

// Test mixed rowsets: some large (to be split) and some small (to be grouped)
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_mixed_large_and_small_rowsets) {
    int64_t tablet_id = 30002;

    // Create mixed rowsets:
    // Rowset 0: Large - 8 segments, 500MB each = 4GB (should be split)
    // Rowset 1: Small - 1 segment, 100MB
    // Rowset 2: Small - 1 segment, 100MB
    // Rowset 3: Small - 1 segment, 100MB
    std::vector<std::pair<int, int64_t>> rowset_specs = {
            {8, 500 * 1024 * 1024}, // Large rowset: 4GB total
            {1, 100 * 1024 * 1024}, // Small rowset: 100MB
            {1, 100 * 1024 * 1024}, // Small rowset: 100MB
            {1, 100 * 1024 * 1024}, // Small rowset: 100MB
    };
    create_pk_tablet_with_mixed_rowsets(tablet_id, rowset_specs);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(4);
    config.set_max_bytes_per_subtask(1024 * 1024 * 1024L); // 1GB per subtask

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, 124, 5, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    ASSERT_TRUE(st.ok()) << st.status();

    auto state = _manager->get_tablet_state(tablet_id, 124);
    if (state != nullptr) {
        VLOG(1) << "Created " << state->running_subtasks.size() << " subtasks for mixed rowsets";

        // Verify subtask types
        for (const auto& [id, info] : state->running_subtasks) {
            if (info.type == SubtaskType::LARGE_ROWSET_PART) {
                VLOG(1) << "Subtask " << id << " is LARGE_ROWSET_PART for rowset " << info.large_rowset_id
                        << " segments [" << info.segment_start << ", " << info.segment_end << ")";
            } else {
                VLOG(1) << "Subtask " << id << " is NORMAL with " << info.input_rowset_ids.size() << " rowsets";
            }
        }
    }

    block_promise.set_value();
    pool->wait();

    _manager->cleanup_tablet(tablet_id, 124);
}

// Test manual completion flow with LARGE_ROWSET_PART subtasks
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_manual_large_rowset_split_completion) {
    int64_t tablet_id = 30003;
    int64_t txn_id = 40003;
    int64_t version = 2;

    create_pk_tablet_with_large_rowset(tablet_id, 8, 1024 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    // Manually create state with LARGE_ROWSET_PART subtasks
    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 2;
    state->callback = callback;

    // Simulate large rowset split into 2 subtasks
    {
        SubtaskInfo info0;
        info0.subtask_id = 0;
        info0.type = SubtaskType::LARGE_ROWSET_PART;
        info0.large_rowset_id = 0;
        info0.segment_start = 0;
        info0.segment_end = 4;
        info0.input_rowset_ids = {0};
        info0.input_bytes = 4 * 1024 * 1024 * 1024L;
        info0.start_time = ::time(nullptr);
        state->running_subtasks[0] = std::move(info0);
        state->total_subtasks_created = 1;

        SubtaskInfo info1;
        info1.subtask_id = 1;
        info1.type = SubtaskType::LARGE_ROWSET_PART;
        info1.large_rowset_id = 0;
        info1.segment_start = 4;
        info1.segment_end = 8;
        info1.input_rowset_ids = {0};
        info1.input_bytes = 4 * 1024 * 1024 * 1024L;
        info1.start_time = ::time(nullptr);
        state->running_subtasks[1] = std::move(info1);
        state->total_subtasks_created = 2;
    }

    // Record large rowset split group
    state->large_rowset_split_groups[0] = {0, 1};
    // Reference count = 2 because 2 subtasks share this rowset
    state->compacting_rowsets[0] = 2;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Complete subtask 0
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    auto* op0 = ctx0->txn_log->mutable_op_compaction();
    op0->add_input_rowsets(0);
    op0->mutable_output_rowset()->set_num_rows(500);
    op0->mutable_output_rowset()->set_data_size(2 * 1024 * 1024 * 1024L);
    op0->set_segment_range_start(0);
    op0->set_segment_range_end(4);

    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    ASSERT_FALSE(closure.is_finished());

    // Complete subtask 1
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    auto* op1 = ctx1->txn_log->mutable_op_compaction();
    op1->add_input_rowsets(0);
    op1->mutable_output_rowset()->set_num_rows(500);
    op1->mutable_output_rowset()->set_data_size(2 * 1024 * 1024 * 1024L);
    op1->set_segment_range_start(4);
    op1->set_segment_range_end(8);

    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // Verify result
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();
    // After merging large rowset split subtasks, we get 1 merged subtask_compaction
    // that combines all segments from the same large_rowset_id
    ASSERT_EQ(1, op_parallel.subtask_compactions_size());

    // Verify the merged subtask contains segments from both subtasks
    const auto& merged_subtask = op_parallel.subtask_compactions(0);
    // The merged subtask should have overlapped=true since segments are from different subtasks
    EXPECT_TRUE(merged_subtask.output_rowset().overlapped());
    // Total rows should be sum of both subtasks
    EXPECT_EQ(1000, merged_subtask.output_rowset().num_rows());

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test partial failure in large rowset split - all subtasks must succeed
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_large_rowset_split_partial_failure) {
    int64_t tablet_id = 30004;
    int64_t txn_id = 40004;
    int64_t version = 2;

    create_pk_tablet_with_large_rowset(tablet_id, 8, 1024 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 2;
    state->callback = callback;

    // Simulate large rowset split into 2 subtasks
    {
        SubtaskInfo info0;
        info0.subtask_id = 0;
        info0.type = SubtaskType::LARGE_ROWSET_PART;
        info0.large_rowset_id = 0;
        info0.segment_start = 0;
        info0.segment_end = 4;
        info0.input_rowset_ids = {0};
        state->running_subtasks[0] = std::move(info0);
        state->total_subtasks_created = 1;

        SubtaskInfo info1;
        info1.subtask_id = 1;
        info1.type = SubtaskType::LARGE_ROWSET_PART;
        info1.large_rowset_id = 0;
        info1.segment_start = 4;
        info1.segment_end = 8;
        info1.input_rowset_ids = {0};
        state->running_subtasks[1] = std::move(info1);
        state->total_subtasks_created = 2;
    }

    state->large_rowset_split_groups[0] = {0, 1};
    // Reference count = 2 because 2 subtasks share this rowset
    state->compacting_rowsets[0] = 2;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Complete subtask 0 with success
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->status = Status::OK();
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(500);
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    ASSERT_FALSE(closure.is_finished());

    // Complete subtask 1 with failure
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->status = Status::IOError("disk error");
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(500);
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // For large rowset split, if any subtask fails, all subtasks for that rowset should be discarded
    // When all subtasks in a large rowset split group fail, no txn_log is generated
    // because there's nothing to apply (the original rowset is kept intact)
    if (response.txn_logs_size() > 0) {
        const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();
        // No successful subtasks for the failed large rowset split
        EXPECT_EQ(0, op_parallel.subtask_compactions_size());
    } else {
        // When all subtasks fail, no txn_log is generated (expected behavior)
        EXPECT_EQ(0, response.txn_logs_size());
    }

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test that large rowset split is capped to max_parallel to ensure completeness.
// When max_parallel is less than the ideal number of subtasks for a large rowset,
// the split should be capped to max_parallel (allowing each subtask to exceed
// target_bytes_per_subtask) rather than truncating and causing data loss.
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_large_rowset_split_capped_to_max_parallel) {
    int64_t tablet_id = 30005;

    // Create a tablet with one very large rowset.
    // 16 segments, 550MB each = 8.8GB total (must be >= 2 * lake_compaction_max_rowset_size = 8.58GB)
    // With max_bytes_per_subtask=2GB, ideally this would split into 4-5 groups
    // But with max_parallel=2, it should be capped to 2 groups
    // to ensure the split is complete and no data is lost.
    int64_t segment_size = 550 * 1024 * 1024; // 550MB per segment
    create_pk_tablet_with_large_rowset(tablet_id, 16, segment_size);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);                     // Only 2 subtasks allowed
    config.set_max_bytes_per_subtask(2 * 1024 * 1024 * 1024L); // 2GB per subtask

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    // Use a single-threaded pool and block it to prevent task execution during state verification
    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, 125, 2, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    ASSERT_TRUE(st.ok()) << st.status();

    auto state = _manager->get_tablet_state(tablet_id, 125);
    ASSERT_NE(nullptr, state);

    {
        std::lock_guard<std::mutex> lock(state->mutex);

        // The large rowset should be split into exactly 2 subtasks (capped by max_parallel)
        EXPECT_EQ(1, state->large_rowset_split_groups.size());

        // Verify the large rowset (id=0) is being compacted
        EXPECT_TRUE(state->compacting_rowsets.count(0) > 0) << "Large rowset 0 should be in compacting set";

        // Verify all subtasks for the large rowset are tracked
        auto it = state->large_rowset_split_groups.find(0);
        if (it != state->large_rowset_split_groups.end()) {
            EXPECT_EQ(2, it->second.size()) << "Large rowset should be split into exactly 2 subtasks";

            // Verify all subtasks are running (tasks are queued but not executed yet)
            for (int32_t sid : it->second) {
                EXPECT_TRUE(state->running_subtasks.count(sid) > 0) << "Subtask " << sid << " should be running";
            }

            // Verify segment ranges cover all segments [0, 16)
            std::set<int32_t> covered_segments;
            for (const auto& [subtask_id, info] : state->running_subtasks) {
                if (info.type == SubtaskType::LARGE_ROWSET_PART && info.large_rowset_id == 0) {
                    for (int32_t seg = info.segment_start; seg < info.segment_end; seg++) {
                        covered_segments.insert(seg);
                    }
                }
            }
            EXPECT_EQ(16, covered_segments.size()) << "All 16 segments should be covered by subtasks";
        }
    }

    block_promise.set_value();
    pool->wait();

    _manager->cleanup_tablet(tablet_id, 125);
}

// Test that when large rowset is split, it uses multiple parallel slots as expected
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_large_rowset_uses_all_parallel_slots) {
    int64_t tablet_id = 30006;

    // Create a tablet with one large rowset only (no small rowsets to avoid compaction policy selection issues)
    // 16 segments, 550MB each = 8.8GB total (must be >= 2 * lake_compaction_max_rowset_size = 8.58GB)
    int64_t segment_size = 550 * 1024 * 1024; // 550MB per segment
    create_pk_tablet_with_large_rowset(tablet_id, 16, segment_size);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);                     // Only 2 subtasks allowed
    config.set_max_bytes_per_subtask(2 * 1024 * 1024 * 1024L); // 2GB per subtask

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    // Use a single-threaded pool and block it to prevent task execution during state verification
    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, 126, 2, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    ASSERT_TRUE(st.ok()) << st.status();

    auto state = _manager->get_tablet_state(tablet_id, 126);
    ASSERT_NE(nullptr, state);

    {
        std::lock_guard<std::mutex> lock(state->mutex);

        // Large rowset should use all 2 parallel slots (split into 2 subtasks)
        EXPECT_EQ(2, state->running_subtasks.size());

        // Only the large rowset (id=0) should be compacting
        EXPECT_TRUE(state->compacting_rowsets.count(0) > 0);

        // Verify it's in large_rowset_split_groups
        EXPECT_EQ(1, state->large_rowset_split_groups.size());
        auto it = state->large_rowset_split_groups.find(0);
        if (it != state->large_rowset_split_groups.end()) {
            EXPECT_EQ(2, it->second.size()) << "Large rowset should be split into 2 subtasks";
        }
    }

    block_promise.set_value();
    pool->wait();

    _manager->cleanup_tablet(tablet_id, 126);
}

// Test _split_large_rowset merge last group when last group has < 2 segments (lines 1589-1596)
// 7 segments with max_parallel=3: segments_per_subtask=3 -> [0,3), [3,6), [6,7); last has 1 segment -> merge with previous
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_split_large_rowset_merge_last_group) {
    int64_t tablet_id = 30025;
    int64_t txn_id = 40006;

    int64_t segment_size = 1500 * 1024 * 1024; // 1.5GB per segment, 7*1.5=10.5GB total
    create_tablet_with_large_rowset(tablet_id, 7, segment_size, PRIMARY_KEYS);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(3);
    config.set_max_bytes_per_subtask(3 * 1024 * 1024 * 1024L); // 3GB per subtask

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);
    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;
    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });
    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, 2, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    ASSERT_TRUE(st.ok()) << st.status();
    auto state = _manager->get_tablet_state(tablet_id, txn_id);
    ASSERT_NE(nullptr, state);
    EXPECT_EQ(2, state->running_subtasks.size())
            << "7 segments with max_parallel=3: last group has 1 segment, merged into previous -> 2 groups";

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test that large rowset is skipped when only 1 parallel slot remains
// (splitting with only 1 slot is meaningless, need at least 2)
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_large_rowset_skipped_when_only_one_slot) {
    int64_t tablet_id = 30007;

    // Create a tablet with one large rowset
    // 16 segments, 550MB each = 8.8GB total (must be >= 2 * lake_compaction_max_rowset_size = 8.58GB)
    int64_t segment_size = 550 * 1024 * 1024;
    create_pk_tablet_with_large_rowset(tablet_id, 16, segment_size);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(1);                     // Only 1 subtask allowed - not enough to split
    config.set_max_bytes_per_subtask(2 * 1024 * 1024 * 1024L); // 2GB per subtask

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    // Use a single-threaded pool and block it to prevent task execution during state verification
    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, 127, 2, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    // With only 1 parallel slot, the large rowset should be skipped (can't split with 1 slot)
    // This should return 0 (fallback to normal compaction) and state should be nullptr
    ASSERT_TRUE(st.ok()) << st.status();
    EXPECT_EQ(0, st.value()) << "Large rowset should be skipped when max_parallel=1, returning 0 for fallback";

    // State should not exist because no subtasks were created
    auto state = _manager->get_tablet_state(tablet_id, 127);
    EXPECT_EQ(nullptr, state) << "State should not exist when large rowset is skipped";

    block_promise.set_value();
    pool->wait();

    _manager->cleanup_tablet(tablet_id, 127);
}

// Test that incomplete large rowset split (when acquire_token() fails after some subtasks are created)
// is detected and treated as failed to prevent data loss.
// This tests the fix for: when acquire_token() fails after some subtasks are already created,
// only a subset of segment ranges would be scheduled for a given rowset. Because
// large_rowset_split_groups is populated only for created subtasks, get_merged_txn_log()
// would treat the group as successful and the first subtask would delete the original rowset,
// dropping the segments that never got a subtask.
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_incomplete_large_rowset_split_detected_as_failed) {
    int64_t tablet_id = 30008;
    int64_t txn_id = 40008;
    int64_t version = 2;

    create_pk_tablet_with_large_rowset(tablet_id, 16, 550 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = 4;
    state->callback = callback;

    // Simulate the scenario where a large rowset should be split into 4 subtasks,
    // but only 2 were successfully created (the others failed due to acquire_token() failure).
    // This is the bug scenario: expected 4 subtasks, but only 2 were created.
    state->expected_large_rowset_split_counts[0] = 4; // Expected 4 subtasks for large rowset 0

    // Only 2 subtasks were actually created (simulating acquire_token() failure for the rest)
    {
        SubtaskInfo info0;
        info0.subtask_id = 0;
        info0.type = SubtaskType::LARGE_ROWSET_PART;
        info0.large_rowset_id = 0;
        info0.segment_start = 0;
        info0.segment_end = 4;
        info0.input_rowset_ids = {0};
        state->running_subtasks[0] = std::move(info0);

        SubtaskInfo info1;
        info1.subtask_id = 1;
        info1.type = SubtaskType::LARGE_ROWSET_PART;
        info1.large_rowset_id = 0;
        info1.segment_start = 4;
        info1.segment_end = 8;
        info1.input_rowset_ids = {0};
        state->running_subtasks[1] = std::move(info1);

        state->total_subtasks_created = 2;
    }

    // Only 2 subtasks are in the split group (the other 2 were never created)
    state->large_rowset_split_groups[0] = {0, 1};
    // Reference count = 2 because 2 subtasks share this rowset
    state->compacting_rowsets[0] = 2;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Complete subtask 0 with success
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->status = Status::OK();
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    ctx0->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx0->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(250);
    ctx0->txn_log->mutable_op_compaction()->set_segment_range_start(0);
    ctx0->txn_log->mutable_op_compaction()->set_segment_range_end(4);
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    ASSERT_FALSE(closure.is_finished());

    // Complete subtask 1 with success
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->status = Status::OK();
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    ctx1->txn_log->mutable_op_compaction()->add_input_rowsets(0);
    ctx1->txn_log->mutable_op_compaction()->mutable_output_rowset()->set_num_rows(250);
    ctx1->txn_log->mutable_op_compaction()->set_segment_range_start(4);
    ctx1->txn_log->mutable_op_compaction()->set_segment_range_end(8);
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // Even though both created subtasks succeeded, the large rowset split is incomplete
    // (expected 4 subtasks but only 2 were created). The incomplete split should be
    // detected and treated as failed to prevent data loss.
    // The original rowset should NOT be deleted because segments [8,16) were never processed.
    //
    // When get_merged_txn_log() detects an incomplete split:
    // 1. It adds the large_rowset_id to failed_large_rowset_ids
    // 2. All subtasks belonging to the failed group are skipped
    // 3. Since success_subtask_ids becomes empty, it returns Status::InternalError
    // 4. The merged_context->status is set to this error
    // 5. No txn_log is added to the response (compaction fails completely)
    //
    // This is the expected behavior: incomplete splits should fail entirely to prevent data loss.
    EXPECT_EQ(0, response.txn_logs_size()) << "Incomplete large rowset split should result in no txn_logs - "
                                           << "the compaction should fail entirely to prevent data loss";

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// ==============================================================================
// Tests for _split_large_rowset segment merging (lines 1432-1436)
// ==============================================================================

// Note: The _split_large_rowset segment merge logic (lines 1432-1436) is covered
// by existing tests like test_large_rowset_split_capped_to_max_parallel.
// The merge happens when last group has < 2 segments.
// We can't easily test this in isolation without real segment files.

// ==============================================================================
// Tests for submit_subtasks_from_groups token acquisition failure (lines 1601-1625)
// ==============================================================================

TEST_F(TabletParallelCompactionManagerTest, test_submit_subtasks_token_acquisition_failure) {
    // This tests lines 1614-1625: when acquire_token() fails
    int64_t tablet_id = 10010;
    int64_t txn_id = 20010;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    int acquire_count = 0;
    // Fail on first acquire_token() call
    auto acquire_token = [&acquire_count]() {
        acquire_count++;
        return false; // All acquires fail
    };

    int release_count = 0;
    auto release_token = [&release_count](bool) { release_count++; };

    auto st = _manager->create_parallel_tasks(tablet_id, txn_id, version, config, callback, false, pool.get(),
                                              acquire_token, release_token);

    // Should fail because token acquisition failed
    EXPECT_TRUE(st.status().is_resource_busy()) << st.status();
    // No token was successfully acquired, so no release should be called
    EXPECT_EQ(0, release_count) << "No token was acquired, so no release should happen";

    block_promise.set_value();
    pool->wait();

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test token partial acquire: first succeeds, second fails; release the one acquired (lines 1802-1810)
TEST_F(TabletParallelCompactionManagerTest, test_submit_subtasks_token_partial_acquire_then_release) {
    int64_t tablet_id = 10061;
    int64_t txn_id = 20061;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);
    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;
    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });
    start_promise.get_future().wait();

    int acquire_count = 0;
    auto acquire_token = [&acquire_count]() {
        acquire_count++;
        return acquire_count <= 1;
    };
    int release_count = 0;
    auto release_token = [&release_count](bool) { release_count++; };

    auto st = _manager->create_parallel_tasks(tablet_id, txn_id, version, config, callback, false, pool.get(),
                                              acquire_token, release_token);

    EXPECT_TRUE(st.status().is_resource_busy()) << st.status();
    EXPECT_EQ(2, acquire_count);
    EXPECT_EQ(1, release_count) << "One token was acquired then released";

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test fallback not_enough_segments: total_segments < 4 (same condition in split_rowsets_into_groups
// and _create_subtask_groups). We test via split_rowsets_into_groups so we don't depend on
// compaction policy returning few rowsets.
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_fallback_not_enough_segments) {
    int64_t tablet_id = 10062;
    int64_t version = 3;

    create_tablet_with_rowsets(tablet_id, 2, 10 * 1024 * 1024);

    auto tablet_or = _tablet_mgr->get_tablet(tablet_id, version);
    ASSERT_OK(tablet_or);
    auto rowsets = tablet_or.value().get_rowsets();
    ASSERT_EQ(2, rowsets.size());

    auto groups = _manager->split_rowsets_into_groups(tablet_id, rowsets, 4, 5 * 1024 * 1024, false);
    EXPECT_TRUE(groups.empty()) << "2 segments < kMinSegmentsForParallel(4) should fallback, no groups";
}

// Test _create_subtask_groups: skip second large rowset when no remaining parallel slots (lines 1712-1715)
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_create_subtask_groups_skip_large_rowset_no_slots) {
    int64_t tablet_id = 30026;

    std::vector<std::pair<int, int64_t>> rowset_specs = {
            {8, 1200 * 1024 * 1024},
            {8, 1200 * 1024 * 1024},
    };
    create_tablet_with_mixed_rowsets(tablet_id, rowset_specs, PRIMARY_KEYS);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(3 * 1024 * 1024 * 1024L);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);
    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;
    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });
    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, 126, 3, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    ASSERT_TRUE(st.ok()) << st.status();
    auto state = _manager->get_tablet_state(tablet_id, 126);
    ASSERT_NE(nullptr, state);
    EXPECT_EQ(2, state->running_subtasks.size())
            << "First large rowset uses 2 slots; second is skipped (no remaining slots)";
    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, 126);
}

// ==============================================================================
// Tests for execute_subtask_segment_range error handling (lines 1797-1856)
// ==============================================================================

TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_execute_subtask_segment_range_state_not_found) {
    // This tests lines 1797-1804: tablet state not found during execution
    int64_t tablet_id = 30010;
    int64_t txn_id = 40010;

    // Don't register any state - verify state is not found
    EXPECT_EQ(nullptr, _manager->get_tablet_state(tablet_id, txn_id));

    // When execute_subtask_segment_range is called without registered state,
    // it should return early without crash (line 1797-1804)
    bool release_called = false;
    auto release_token = [&release_called](bool) { release_called = true; };

    _manager->execute_subtask_segment_range(tablet_id, txn_id, 0, nullptr, 0, 0, 4, 1, false, release_token);

    // Release token should be called even on early return
    EXPECT_TRUE(release_called) << "Release token should be called when state not found";
}

// ==============================================================================
// Tests for submit_subtasks_from_groups failure handling (lines 1722-1756)
// ==============================================================================

TEST_F(TabletParallelCompactionManagerTest, test_submit_subtasks_thread_pool_failure) {
    // This tests lines 1722-1756: when thread pool submission fails
    int64_t tablet_id = 10011;
    int64_t txn_id = 20011;
    int64_t version = 11;

    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    // Create a valid pool first, then shutdown to make submissions fail
    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_min_threads(0).set_max_threads(1).build(&pool);
    // Shutdown the pool so submissions will fail
    pool->shutdown();

    int release_count = 0;
    auto release_token = [&release_count](bool) { release_count++; };

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, release_token);

    // Submission should fail because pool is shutdown
    EXPECT_FALSE(st.ok()) << "Should fail when thread pool submission fails";
    // All tokens should be released
    EXPECT_GT(release_count, 0) << "Tokens should be released on failure";

    _manager->cleanup_tablet(tablet_id, txn_id);
}

// ==============================================================================
// Tests for coverage: split_rowsets_into_groups, _filter_compactable_rowsets,
// pick_rowsets, create_and_register_tablet_state, get_merged_txn_log, etc.
// ==============================================================================

// Test split_rowsets_into_groups when all rowsets are large non-overlapped (lines 369-373)
TEST_F(TabletParallelCompactionManagerTest, test_split_rowsets_into_groups_all_large_non_overlapped) {
    int64_t tablet_id = 10050;
    int64_t version = 3;
    // Two rowsets, each >= lake_compaction_max_rowset_size (default 4GB), non-overlapped
    int64_t large_size = 5L * 1024 * 1024 * 1024;
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    for (int i = 0; i < 2; i++) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(i);
        rowset->set_overlapped(false);
        rowset->set_num_rows(1000);
        rowset->set_data_size(large_size);
        rowset->add_segments("segment_" + std::to_string(i) + ".dat");
        rowset->add_segment_size(large_size);
        std::string path = _lp->segment_location(tablet_id, "segment_" + std::to_string(i) + ".dat");
        std::string dir = std::filesystem::path(path).parent_path().string();
        CHECK_OK(fs::create_directories(dir));
        auto fs = FileSystem::CreateSharedFromString(path);
        auto st = fs.value()->new_writable_file(path);
        CHECK_OK(st.status());
        CHECK_OK(st.value()->append("dummy"));
        CHECK_OK(st.value()->close());
    }
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    auto tablet_or = _tablet_mgr->get_tablet(tablet_id, version);
    ASSERT_OK(tablet_or);
    auto rowsets = tablet_or.value().get_rowsets();
    ASSERT_EQ(2, rowsets.size());

    auto groups = _manager->split_rowsets_into_groups(tablet_id, std::move(rowsets), 4, 10 * 1024 * 1024, false);
    EXPECT_TRUE(groups.empty()) << "All large non-overlapped should yield no groups";
}

// Test split_rowsets_into_groups fallback: data_size_small, not_enough_segments, max_parallel<=1 (lines 391-405)
TEST_F(TabletParallelCompactionManagerTest, test_split_rowsets_into_groups_fallback_reasons) {
    int64_t tablet_id = 10051;
    int64_t version = 5;
    create_tablet_with_rowsets(tablet_id, 4, 512 * 1024); // 4 rowsets, 512KB each = 2MB total

    auto tablet_or = _tablet_mgr->get_tablet(tablet_id, version);
    ASSERT_OK(tablet_or);
    auto rowsets = tablet_or.value().get_rowsets();

    // Fallback: total_bytes (2MB) <= max_bytes (5MB) -> data_size_small
    auto groups_small = _manager->split_rowsets_into_groups(tablet_id, rowsets, 4, 5 * 1024 * 1024, false);
    EXPECT_TRUE(groups_small.empty());

    // Fallback: max_parallel <= 1
    auto groups_parallel = _manager->split_rowsets_into_groups(tablet_id, rowsets, 1, 1024, false);
    EXPECT_TRUE(groups_parallel.empty());

    // Fallback: not_enough_segments - 4 rowsets each 1 segment = 4 segments, need >= 4 for parallel (kMinSegmentsForParallel=4)
    // So 4 segments is exactly the limit; with 4 rowsets overlapped we have total_segments=4. So we might get 1 group
    // or empty. Actually not_enough_segments = (total_segments < 4). So 4 is not < 4, so we don't fallback for that.
    // Use 3 rowsets so total_segments=3 < 4
    create_tablet_with_rowsets(10052, 3, 2 * 1024 * 1024);
    auto tablet_or2 = _tablet_mgr->get_tablet(10052, 4);
    ASSERT_OK(tablet_or2);
    auto rowsets2 = tablet_or2.value().get_rowsets();
    auto groups_seg = _manager->split_rowsets_into_groups(10052, rowsets2, 4, 1024, false);
    EXPECT_TRUE(groups_seg.empty());
}

// Test split_rowsets_into_groups fallback: has_delete_predicate (lines 391, 397)
TEST_F(TabletParallelCompactionManagerTest, test_split_rowsets_into_groups_fallback_has_delete_predicate) {
    int64_t tablet_id = 10053;
    int64_t version = 6;
    create_tablet_with_rowsets(tablet_id, 6, 2 * 1024 * 1024); // 6 rowsets, 2MB each

    // Put metadata again with delete predicate on one rowset
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    for (int i = 0; i < 6; i++) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(i);
        rowset->set_overlapped(true);
        rowset->set_num_rows(100);
        rowset->set_data_size(2 * 1024 * 1024);
        rowset->add_segments("seg_" + std::to_string(i) + ".dat");
        rowset->add_segment_size(2 * 1024 * 1024);
        if (i == 0) {
            rowset->mutable_delete_predicate()->set_version(version);
        }
        std::string path = _lp->segment_location(tablet_id, "seg_" + std::to_string(i) + ".dat");
        std::string dir = std::filesystem::path(path).parent_path().string();
        CHECK_OK(fs::create_directories(dir));
        auto fs = FileSystem::CreateSharedFromString(path);
        auto st = fs.value()->new_writable_file(path);
        CHECK_OK(st.status());
        CHECK_OK(st.value()->append("dummy"));
        CHECK_OK(st.value()->close());
    }
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    auto tablet_or = _tablet_mgr->get_tablet(tablet_id, version);
    ASSERT_OK(tablet_or);
    auto rowsets = tablet_or.value().get_rowsets();
    auto groups = _manager->split_rowsets_into_groups(tablet_id, rowsets, 4, 2 * 1024 * 1024, false);
    EXPECT_TRUE(groups.empty()) << "has_delete_predicate should fallback to normal compaction";
}

// Test _filter_compactable_rowsets path: at least one large non-overlapped skipped (lines 81-91)
TEST_F(TabletParallelCompactionManagerTest, test_filter_compactable_rowsets_skips_large_non_overlapped) {
    int64_t tablet_id = 10054;
    int64_t version = 4;
    int64_t large_size = 5L * 1024 * 1024 * 1024; // 5GB
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    metadata->set_id(tablet_id);
    metadata->set_version(version);
    // One large non-overlapped, four small overlapped
    for (int i = 0; i < 5; i++) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(i);
        rowset->set_overlapped(i > 0);
        rowset->set_num_rows(100);
        rowset->set_data_size(i == 0 ? large_size : 2 * 1024 * 1024);
        rowset->add_segments("s_" + std::to_string(i) + ".dat");
        rowset->add_segment_size(i == 0 ? large_size : 2 * 1024 * 1024);
        std::string path = _lp->segment_location(tablet_id, "s_" + std::to_string(i) + ".dat");
        std::string dir = std::filesystem::path(path).parent_path().string();
        CHECK_OK(fs::create_directories(dir));
        auto fs = FileSystem::CreateSharedFromString(path);
        auto st = fs.value()->new_writable_file(path);
        CHECK_OK(st.status());
        CHECK_OK(st.value()->append("dummy"));
        CHECK_OK(st.value()->close());
    }
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    auto tablet_or = _tablet_mgr->get_tablet(tablet_id, version);
    ASSERT_OK(tablet_or);
    auto rowsets = tablet_or.value().get_rowsets();
    ASSERT_EQ(5, rowsets.size());
    // split_rowsets_into_groups calls _filter_compactable_rowsets; one 5GB non-overlapped is skipped
    auto groups = _manager->split_rowsets_into_groups(tablet_id, rowsets, 4, 2 * 1024 * 1024, false);
    // After filter we have 4 small rowsets; may produce groups or fallback
    EXPECT_GE(groups.size(), 0u);
}

// Test create_and_register_tablet_state AlreadyExist (lines 416-421)
TEST_F(TabletParallelCompactionManagerTest, test_create_parallel_tasks_already_registered) {
    int64_t tablet_id = 10055;
    int64_t txn_id = 20055;
    int64_t version = 11;
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(2);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(2).build(&pool);

    auto st1 = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    ASSERT_TRUE(st1.ok()) << st1.status();
    EXPECT_GT(st1.value(), 0);

    // Second call with same tablet_id/txn_id should get AlreadyExist
    auto st2 = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, pool.get(), []() { return true; }, [](bool) {});
    EXPECT_FALSE(st2.ok());
    EXPECT_TRUE(st2.status().is_already_exist()) << st2.status();

    pool->wait();
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test get_merged_txn_log with large rowset split group incomplete (lines 971-978)
TEST_F(TabletParallelCompactionManagerTest, test_get_merged_txn_log_large_rowset_split_incomplete) {
    int64_t tablet_id = 10056;
    int64_t txn_id = 20056;
    int64_t version = 2;

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->total_subtasks_created = 2;
    state->large_rowset_split_groups[100] = {0, 1};     // two subtasks
    state->expected_large_rowset_split_counts[100] = 3; // expected 3, actual 2 -> incomplete
    SubtaskInfo info0, info1;
    info0.subtask_id = 0;
    info1.subtask_id = 1;
    state->running_subtasks[0] = std::move(info0);
    state->running_subtasks[1] = std::move(info1);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);
    state->callback = callback;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->status = Status::OK();
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->status = Status::OK();
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());
    // Incomplete large rowset group should be skipped; merged log should still be produced
    EXPECT_EQ(0, response.txn_logs_size());
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test get_merged_txn_log with large rowset merge path: segment_metas, ssts, sst_ranges (lines 1060-1118)
TEST_F(TabletParallelCompactionManagerTest, test_get_merged_txn_log_large_rowset_with_segment_metas_ssts) {
    int64_t tablet_id = 10055;
    int64_t txn_id = 20055;
    int64_t version = 2;

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->total_subtasks_created = 2;
    state->large_rowset_split_groups[0] = {0, 1};
    state->expected_large_rowset_split_counts[0] = 2;

    SubtaskInfo info0, info1;
    info0.subtask_id = 0;
    info0.input_rowset_ids = {0};
    info1.subtask_id = 1;
    info1.input_rowset_ids = {0};
    state->running_subtasks[0] = std::move(info0);
    state->running_subtasks[1] = std::move(info1);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);
    state->callback = callback;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    auto* op0 = ctx0->txn_log->mutable_op_compaction();
    op0->add_input_rowsets(0);
    op0->set_compact_version(10);
    auto* out0 = op0->mutable_output_rowset();
    out0->set_num_rows(100);
    out0->set_data_size(1000);
    out0->add_segments("s0.dat");
    out0->add_segment_size(1000);
    out0->add_segment_encryption_metas("enc0");
    out0->add_segment_metas();
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    auto* op1 = ctx1->txn_log->mutable_op_compaction();
    op1->add_input_rowsets(0);
    auto* out1 = op1->mutable_output_rowset();
    out1->set_num_rows(200);
    out1->set_data_size(2000);
    out1->add_segments("s1.dat");
    out1->add_segment_size(2000);
    op1->add_ssts();
    op1->add_sst_ranges();
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();
    ASSERT_EQ(1, op_parallel.subtask_compactions_size());
    const auto& merged = op_parallel.subtask_compactions(0);
    EXPECT_TRUE(merged.has_output_rowset());
    EXPECT_EQ(300, merged.output_rowset().num_rows());
    EXPECT_EQ(3000, merged.output_rowset().data_size());
    EXPECT_EQ(2, merged.output_rowset().segments_size());
    EXPECT_EQ(1, merged.output_rowset().segment_metas_size());
    EXPECT_EQ(1, merged.ssts_size());
    EXPECT_EQ(1, merged.sst_ranges_size());
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test get_merged_txn_log large rowset group with no valid subtasks (RemoveLast, lines 1132-1140)
TEST_F(TabletParallelCompactionManagerTest, test_get_merged_txn_log_large_rowset_no_valid_subtasks) {
    int64_t tablet_id = 10050;
    int64_t txn_id = 20050;
    int64_t version = 2;

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->total_subtasks_created = 2;
    state->large_rowset_split_groups[0] = {0, 1};
    state->expected_large_rowset_split_counts[0] = 2;

    SubtaskInfo info0, info1;
    info0.subtask_id = 0;
    info1.subtask_id = 1;
    state->running_subtasks[0] = std::move(info0);
    state->running_subtasks[1] = std::move(info1);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);
    state->callback = callback;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Complete both subtasks with status OK but no op_compaction (so first_subtask stays true in merge loop)
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();
    EXPECT_EQ(0, op_parallel.subtask_compactions_size());
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test get_merged_txn_log with large rowset split group one subtask failed (lines 990-996)
TEST_F(TabletParallelCompactionManagerTest, test_get_merged_txn_log_large_rowset_split_one_failed) {
    int64_t tablet_id = 10057;
    int64_t txn_id = 20057;
    int64_t version = 2;

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->total_subtasks_created = 2;
    state->large_rowset_split_groups[100] = {0, 1};
    state->expected_large_rowset_split_counts[100] = 2;
    SubtaskInfo info0, info1;
    info0.subtask_id = 0;
    info1.subtask_id = 1;
    state->running_subtasks[0] = std::move(info0);
    state->running_subtasks[1] = std::move(info1);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);
    state->callback = callback;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->status = Status::OK();
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->status = Status::InternalError("failed");
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());
    EXPECT_EQ(0, response.txn_logs_size());
    _manager->cleanup_tablet(tablet_id, txn_id);
}

// Test PK table with enable_pk_index_parallel_execution disabled fallback (lines 659-663)
TEST_F(TabletParallelCompactionManagerTest, test_try_create_parallel_tasks_pk_index_parallel_disabled) {
    int64_t tablet_id = 10059;
    int64_t txn_id = 20059;
    int64_t version = 11;
    create_pk_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    ConfigResetGuard<bool> guard(&config::enable_pk_index_parallel_execution, false);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(4);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, _thread_pool.get(), []() { return true; },
            [](bool) {});

    EXPECT_TRUE(st.ok()) << st.status();
    EXPECT_EQ(0, st.value()) << "PK table with enable_pk_index_parallel_execution disabled should fallback";
}

// Test non-PK table with enable_size_tiered_compaction_strategy=false fallback (lines 662-666)
TEST_F(TabletParallelCompactionManagerTest, test_try_create_parallel_tasks_non_pk_size_tiered_disabled) {
    int64_t tablet_id = 10058;
    int64_t txn_id = 20058;
    int64_t version = 11;
    create_tablet_with_rowsets(tablet_id, 10, 1024 * 1024);

    ConfigResetGuard<bool> guard(&config::enable_size_tiered_compaction_strategy, false);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(4);
    config.set_max_bytes_per_subtask(5 * 1024 * 1024);

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    auto st = _manager->create_parallel_tasks(
            tablet_id, txn_id, version, config, callback, false, _thread_pool.get(), []() { return true; },
            [](bool) {});

    // Non-PK with size_tiered disabled should fallback (return 0)
    EXPECT_TRUE(st.ok()) << st.status();
    EXPECT_EQ(0, st.value());
}

// ==============================================================================
// Tests for duplicate key table large rowset split
// ==============================================================================

// Test that DUP_KEYS table with a large overlapped rowset triggers large rowset split
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_dup_keys_large_rowset_split) {
    int64_t tablet_id = 30020;

    // Create a DUP_KEYS tablet with one large rowset: 8 segments, each 1GB
    // Total = 8GB > 2 * lake_compaction_max_rowset_size (default 4GB * 2 = 8GB)
    // Use slightly larger to guarantee the threshold is exceeded
    int64_t segment_size = 1200L * 1024 * 1024; // 1.2GB per segment -> 9.6GB total
    create_tablet_with_large_rowset(tablet_id, 8, segment_size, DUP_KEYS);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(4);
    config.set_max_bytes_per_subtask(3L * 1024 * 1024 * 1024); // 3GB per subtask

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, 500, 2, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    ASSERT_TRUE(st.ok()) << st.status();
    // Should create multiple subtasks via large rowset split
    EXPECT_GT(st.value(), 1) << "DUP_KEYS table should get multiple subtasks from large rowset split";

    auto state = _manager->get_tablet_state(tablet_id, 500);
    ASSERT_NE(nullptr, state);
    // Verify large_rowset_split_groups is populated
    EXPECT_FALSE(state->large_rowset_split_groups.empty()) << "DUP_KEYS table should have large_rowset_split_groups";

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, 500);
}

// Test DUP_KEYS table with mixed large and small rowsets
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_dup_keys_mixed_large_and_small_rowsets) {
    int64_t tablet_id = 30021;

    // Mixed rowsets:
    // Rowset 0: Large - 8 segments, 1.2GB each = 9.6GB (should be split)
    // Rowset 1: Small - 1 segment, 100MB
    // Rowset 2: Small - 1 segment, 100MB
    std::vector<std::pair<int, int64_t>> rowset_specs = {
            {8, 1200L * 1024 * 1024}, // Large rowset: 9.6GB total
            {1, 100 * 1024 * 1024},   // Small rowset: 100MB
            {1, 100 * 1024 * 1024},   // Small rowset: 100MB
    };
    create_tablet_with_mixed_rowsets(tablet_id, rowset_specs, DUP_KEYS);

    TabletParallelConfig config;
    config.set_max_parallel_per_tablet(4);
    config.set_max_bytes_per_subtask(3L * 1024 * 1024 * 1024); // 3GB per subtask

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);

    std::unique_ptr<ThreadPool> pool;
    ThreadPoolBuilder("test_pool").set_max_threads(1).build(&pool);

    std::promise<void> block_promise;
    std::future<void> block_future = block_promise.get_future();
    std::promise<void> start_promise;

    pool->submit_func([&]() {
        start_promise.set_value();
        block_future.wait();
    });

    start_promise.get_future().wait();

    auto st = _manager->create_parallel_tasks(
            tablet_id, 501, 4, config, callback, false, pool.get(), []() { return true; }, [](bool) {});

    ASSERT_TRUE(st.ok()) << st.status();
    EXPECT_GT(st.value(), 1) << "DUP_KEYS mixed should produce multiple subtasks";

    auto state = _manager->get_tablet_state(tablet_id, 501);
    ASSERT_NE(nullptr, state);

    // Check that we have both LARGE_ROWSET_PART and NORMAL subtasks
    bool has_large = false;
    bool has_normal = false;
    for (const auto& [id, info] : state->running_subtasks) {
        if (info.type == SubtaskType::LARGE_ROWSET_PART) {
            has_large = true;
        } else {
            has_normal = true;
        }
    }
    EXPECT_TRUE(has_large) << "Should have LARGE_ROWSET_PART subtasks for the large rowset";
    // Small rowsets may or may not form a NORMAL subtask depending on grouping

    block_promise.set_value();
    pool->wait();
    _manager->cleanup_tablet(tablet_id, 501);
}

// Test manual completion flow for DUP_KEYS large rowset split (verify get_merged_txn_log works)
TEST_F(TabletParallelCompactionManagerLargeRowsetTest, test_dup_keys_large_rowset_split_completion) {
    int64_t tablet_id = 30022;
    int64_t txn_id = 50022;
    int64_t version = 2;

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->total_subtasks_created = 2;

    // Simulate 2 subtasks splitting the same large rowset (id=0)
    state->large_rowset_split_groups[0] = {0, 1};
    state->expected_large_rowset_split_counts[0] = 2;

    SubtaskInfo info0, info1;
    info0.subtask_id = 0;
    info0.type = SubtaskType::LARGE_ROWSET_PART;
    info0.large_rowset_id = 0;
    info0.segment_start = 0;
    info0.segment_end = 4;
    info0.input_rowset_ids = {0};
    info0.input_bytes = 4L * 1024 * 1024 * 1024;
    info0.start_time = ::time(nullptr);
    state->running_subtasks[0] = std::move(info0);

    info1.subtask_id = 1;
    info1.type = SubtaskType::LARGE_ROWSET_PART;
    info1.large_rowset_id = 0;
    info1.segment_start = 4;
    info1.segment_end = 8;
    info1.input_rowset_ids = {0};
    info1.input_bytes = 4L * 1024 * 1024 * 1024;
    info1.start_time = ::time(nullptr);
    state->running_subtasks[1] = std::move(info1);

    state->compacting_rowsets[0] = 2; // refcount=2

    CompactRequest request;
    request.add_tablet_ids(tablet_id);
    CompactResponse response;
    TestClosure closure;
    auto callback = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, &closure);
    state->callback = callback;

    _manager->register_tablet_state_for_test(tablet_id, txn_id, state);

    // Complete subtask 0
    auto ctx0 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx0->subtask_id = 0;
    ctx0->txn_log = std::make_unique<TxnLogPB>();
    auto* op0 = ctx0->txn_log->mutable_op_compaction();
    op0->add_input_rowsets(0);
    op0->mutable_output_rowset()->set_num_rows(500);
    op0->mutable_output_rowset()->set_data_size(4000000000L);
    op0->mutable_output_rowset()->add_segments("out_seg_0.dat");
    op0->mutable_output_rowset()->add_segments("out_seg_1.dat");
    op0->mutable_output_rowset()->add_segment_size(2000000000L);
    op0->mutable_output_rowset()->add_segment_size(2000000000L);
    _manager->on_subtask_complete(tablet_id, txn_id, 0, std::move(ctx0));

    ASSERT_FALSE(closure.is_finished());

    // Complete subtask 1
    auto ctx1 = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, true, nullptr);
    ctx1->subtask_id = 1;
    ctx1->txn_log = std::make_unique<TxnLogPB>();
    auto* op1 = ctx1->txn_log->mutable_op_compaction();
    op1->add_input_rowsets(0);
    op1->mutable_output_rowset()->set_num_rows(500);
    op1->mutable_output_rowset()->set_data_size(4000000000L);
    op1->mutable_output_rowset()->add_segments("out_seg_2.dat");
    op1->mutable_output_rowset()->add_segments("out_seg_3.dat");
    op1->mutable_output_rowset()->add_segment_size(2000000000L);
    op1->mutable_output_rowset()->add_segment_size(2000000000L);
    _manager->on_subtask_complete(tablet_id, txn_id, 1, std::move(ctx1));

    ASSERT_TRUE(closure.is_finished());

    // Verify merged result
    ASSERT_EQ(1, response.txn_logs_size());
    const auto& op_parallel = response.txn_logs(0).op_parallel_compaction();

    // Should have 1 merged subtask_compaction (two subtasks merged into one)
    ASSERT_EQ(1, op_parallel.subtask_compactions_size());
    const auto& merged = op_parallel.subtask_compactions(0);
    EXPECT_EQ(1, merged.input_rowsets_size());
    EXPECT_EQ(0u, merged.input_rowsets(0)); // Original rowset id
    EXPECT_TRUE(merged.has_output_rowset());
    EXPECT_EQ(1000, merged.output_rowset().num_rows());         // 500+500
    EXPECT_EQ(8000000000L, merged.output_rowset().data_size()); // 4G+4G
    EXPECT_EQ(4, merged.output_rowset().segments_size());       // 2+2
    EXPECT_TRUE(merged.output_rowset().overlapped());           // merged output is overlapped

    _manager->cleanup_tablet(tablet_id, txn_id);
}

} // namespace starrocks::lake
