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

#include "storage/lake/compaction_scheduler.h"

#include <atomic>
#include <new>
#include <system_error>
#include <thread>

#include "base/bthreads/util.h"
#include "base/concurrency/countdown_latch.h"
#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "base/utility/scoped_cleanup.h"
#include "common/config_compaction_fwd.h"
#include "common/thread/threadpool.h"
#include "gen_cpp/lake_service.pb.h"
#include "runtime/descriptors.h"
#include "storage/lake/compaction_task_context.h"
#include "storage/lake/test_util.h"

namespace starrocks::lake {

inline void notify_and_wait_latch(const std::shared_ptr<CountDownLatch>& l1,
                                  const std::shared_ptr<CountDownLatch>& l2) {
    l1->count_down();
    l2->wait();
}

inline void notify(const std::shared_ptr<CountDownLatch>& latch) {
    latch->count_down();
}

// Wrapper functions for NewCallback compatibility (takes by value to match NewCallback's template deduction)
// NOLINTNEXTLINE(performance-unnecessary-value-param)
inline void notify_and_wait_latch_for_callback(std::shared_ptr<CountDownLatch> l1, std::shared_ptr<CountDownLatch> l2) {
    notify_and_wait_latch(l1, l2);
}

// NOLINTNEXTLINE(performance-unnecessary-value-param)
inline void notify_for_callback(std::shared_ptr<CountDownLatch> latch) {
    notify(latch);
}

class LakeCompactionSchedulerTest : public TestBase {
public:
    LakeCompactionSchedulerTest() : TestBase(kTestDirectory), _compaction_scheduler(_tablet_mgr.get()) {
        clear_and_init_test_dir();
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
        CHECK_OK(_tablet_mgr->put_tablet_metadata(_tablet_metadata));
    }

protected:
    constexpr static const char* kTestDirectory = "test_compaction_scheduler";

    CompactionScheduler _compaction_scheduler;
    std::shared_ptr<TabletMetadata> _tablet_metadata;
};

TEST_F(LakeCompactionSchedulerTest, test_task_queue) {
    CompactionScheduler::WrapTaskQueues queue(10);
    auto ctx = std::make_unique<CompactionTaskContext>(100 /* txn_id */, 101 /* tablet_id */, 1 /* version */,
                                                       false /* force_base_compaction */, false, nullptr);
    queue.set_target_size(5);
    ASSERT_EQ(5, queue.target_size());
    queue.put_by_txn_id(ctx->txn_id, ctx);

    std::vector<std::unique_ptr<CompactionTaskContext>> v;
    auto ctx2 = std::make_unique<CompactionTaskContext>(101 /* txn_id */, 102 /* tablet_id */, 1 /* version */,
                                                        false /* force_base_compaction */, false, nullptr);
    v.emplace_back(std::move(ctx2));
    queue.put_by_txn_id(101 /* txn_id */, v);
}

TEST_F(LakeCompactionSchedulerTest, test_list_tasks) {
    std::vector<CompactionTaskInfo> tasks;
    _compaction_scheduler.list_tasks(&tasks);
    EXPECT_EQ(0, tasks.size());

    auto t0 = ::time(nullptr);
    auto txn_id = next_id();
    auto l1 = std::make_shared<CountDownLatch>(1); // Used to notify that compaction task has finished
    auto l2 = std::make_shared<CountDownLatch>(1); // Used to notify that CompactionScheduler::list_tasks() has finished
    CompactRequest request;
    CompactResponse response;
    request.add_tablet_ids(_tablet_metadata->id());
    request.set_timeout_ms(/*1 minute=*/60 * 1000);
    request.set_txn_id(txn_id);
    request.set_version(1);
    ASSIGN_OR_ABORT(auto tid, bthreads::start_bthread([&, l1, l2]() {
                        auto cb = ::google::protobuf::NewCallback(notify_and_wait_latch_for_callback, l1, l2);
                        _compaction_scheduler.compact(nullptr, &request, &response, cb);
                    }));

    // Wait until the compaction task finished
    l1->wait();
    _compaction_scheduler.list_tasks(&tasks);
    // Notify the compaction thread to exit
    l2->count_down();

    auto t1 = ::time(nullptr);
    ASSERT_EQ(1, tasks.size());
    EXPECT_EQ(txn_id, tasks[0].txn_id);
    EXPECT_EQ(_tablet_metadata->id(), tasks[0].tablet_id);
    EXPECT_OK(tasks[0].status);
    EXPECT_GE(tasks[0].start_time, t0);
    EXPECT_LE(tasks[0].start_time, tasks[0].finish_time);
    EXPECT_LE(tasks[0].finish_time, t1);
    EXPECT_EQ(1, tasks[0].runs);
    EXPECT_EQ(100, tasks[0].progress);
    EXPECT_FALSE(tasks[0].skipped);
    EXPECT_EQ(-1, tasks[0].subtask_id);

    bthread_join(tid, nullptr);
}

TEST_F(LakeCompactionSchedulerTest, test_list_tasks_hides_parallel_merged_context) {
    auto context = std::make_unique<CompactionTaskContext>(100, 101, 1, false, false, nullptr);
    context->is_parallel_merged = true;
    _compaction_scheduler._contexts.Append(context.get());

    std::vector<CompactionTaskInfo> tasks;
    _compaction_scheduler.list_tasks(&tasks);
    EXPECT_TRUE(tasks.empty());

    context->RemoveFromList();
}

TEST_F(LakeCompactionSchedulerTest, test_abort_all) {
    // set to single thread mode, so all the tasks will be in the same thread
    _compaction_scheduler.update_compact_threads(1);
    std::vector<CompactionTaskInfo> tasks;
    _compaction_scheduler.list_tasks(&tasks);
    EXPECT_EQ(0, tasks.size());

    int num_tasks = 16;
    auto l0 = std::make_shared<CountDownLatch>(1);
    auto l1 = std::make_shared<CountDownLatch>(num_tasks);
    auto l2 = std::make_shared<CountDownLatch>(1);
    auto l3 = std::make_shared<CountDownLatch>(num_tasks);
    EXPECT_EQ(num_tasks, l1->count());

    std::vector<bthread_t> tids;
    // preserve requests and responses life time
    std::vector<std::shared_ptr<CompactRequest>> requests;
    std::vector<std::shared_ptr<CompactResponse>> responses;
    { // task 0: block the execution done until l2.count_down()
        auto txn_id = next_id();
        auto request = std::make_shared<CompactRequest>();
        requests.emplace_back(request);
        auto response = std::make_shared<CompactResponse>();
        responses.emplace_back(response);
        auto meta = generate_simple_tablet_metadata(DUP_KEYS);
        CHECK_OK(_tablet_mgr->put_tablet_metadata(meta));
        request->add_tablet_ids(meta->id());
        request->set_timeout_ms(60 * 1000); // 60 seconds
        request->set_txn_id(txn_id);
        request->set_version(1);
        // wait l2, count down l0
        ASSIGN_OR_ABORT(auto tid, bthreads::start_bthread([&, l1, l2, request, response]() {
                            auto cb = ::google::protobuf::NewCallback(notify_and_wait_latch_for_callback, l0, l2);
                            _compaction_scheduler.compact(nullptr, request.get(), response.get(), cb);
                        }));
        tids.emplace_back(tid);
    }
    // Wait for task0 complete
    l0->wait();
    // repeatedly submit num_tasks into the queue, make the thread busy before stop() invoked.
    for (int i = 0; i < num_tasks; ++i) {
        auto txn_id = next_id();
        auto request = std::make_shared<CompactRequest>();
        requests.emplace_back(request);
        auto response = std::make_shared<CompactResponse>();
        responses.emplace_back(response);
        auto meta = generate_simple_tablet_metadata(DUP_KEYS);
        CHECK_OK(_tablet_mgr->put_tablet_metadata(meta));
        request->add_tablet_ids(meta->id());
        request->set_timeout_ms(60 * 1000); // 60 seconds
        request->set_txn_id(txn_id);
        request->set_version(1);
        // wait l2, count down l1
        ASSIGN_OR_ABORT(auto tid, bthreads::start_bthread([&, l1, l2, l3, request, response]() {
                            auto cb = ::google::protobuf::NewCallback(notify_and_wait_latch_for_callback, l1, l2);
                            l3->count_down();
                            _compaction_scheduler.compact(nullptr, request.get(), response.get(), cb);
                        }));
        tids.emplace_back(tid);
    }
    // wait until all bthreads run
    l3->wait();
    // Allow all tasks to be executed
    // because the first task is blocked by the l2 countdown, rest are all in task queue.
    l2->count_down();
    // expect remain tasks in task queue aborted during stop
    _compaction_scheduler.stop();
    // l1 should be properly count down by all the tasks
    l1->wait();

    for (const auto& tid : tids) {
        bthread_join(tid, nullptr);
    }

    int aborted = 0;
    for (const auto& response : responses) {
        if (response->status().status_code() == TStatusCode::ABORTED) {
            ++aborted;
        }
    }
    // total num_tasks + 1 compact tasks submitted.
    // expect the first one success, and then the remain `num_tasks` aborted between [1, num_tasks]
    EXPECT_GE(aborted, 1);
    EXPECT_LE(aborted, num_tasks);
}

TEST_F(LakeCompactionSchedulerTest, test_submit_compact_after_stop) {
    _compaction_scheduler.stop();
    auto l1 = std::make_shared<CountDownLatch>(1);
    CompactRequest request;
    CompactResponse response;
    request.add_tablet_ids(_tablet_metadata->id());
    request.set_timeout_ms(60 * 1000);
    request.set_txn_id(next_id());
    request.set_version(1);
    auto cb = ::google::protobuf::NewCallback(notify_for_callback, l1);
    _compaction_scheduler.compact(nullptr, &request, &response, cb);
    l1->wait();
    EXPECT_EQ(response.status().status_code(), TStatusCode::ABORTED);
}

TEST_F(LakeCompactionSchedulerTest, test_compaction_cancel) {
    CompactRequest request;
    CompactResponse response;

    // has error
    {
        auto cb = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, nullptr);
        CompactionTaskContext ctx(100 /* txn_id */, 101 /* tablet_id */, 1 /* version */,
                                  false /* force_base_compaction */, false /* skip_write_txnlog */, cb);
        cb->update_status(Status::Aborted("aborted for test"));
        EXPECT_FALSE(compaction_should_cancel(&ctx).ok());
    }

    // not valid time interval, should return early
    {
        auto check_interval = config::lake_compaction_check_valid_interval_minutes;
        config::lake_compaction_check_valid_interval_minutes = -1;
        auto cb = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, nullptr);
        CompactionTaskContext ctx(100 /* txn_id */, 101 /* tablet_id */, 1 /* version */,
                                  false /* force_base_compaction */, false /* skip_write_txnlog */, cb);
        EXPECT_TRUE(compaction_should_cancel(&ctx).ok());
        config::lake_compaction_check_valid_interval_minutes = check_interval;
    }

    // try_lock succeed and check time not satisfied
    {
        auto check_interval = config::lake_compaction_check_valid_interval_minutes;
        config::lake_compaction_check_valid_interval_minutes = 24 * 60; // set to a big value
        auto cb = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, nullptr);
        CompactionTaskContext ctx(100 /* txn_id */, 101 /* tablet_id */, 1 /* version */,
                                  false /* force_base_compaction */, false /* skip_write_txnlog */, cb);

        cb->set_last_check_time(time(nullptr));
        EXPECT_TRUE(compaction_should_cancel(&ctx).ok());
        config::lake_compaction_check_valid_interval_minutes = check_interval;

        // give another try, should acquire the lock successfully
        // try_lock succeed and check time satisfied, should cancel succeed
        check_interval = config::lake_compaction_check_valid_interval_minutes;
        auto last_check_time_val = time(nullptr) - 60 * check_interval;
        cb->set_last_check_time(last_check_time_val);
        EXPECT_TRUE(compaction_should_cancel(&ctx).ok());
        // make sure _last_check_time value is updated
        EXPECT_TRUE(cb->TEST_get_last_check_time() > last_check_time_val);
    }
}

// https://github.com/StarRocks/starrocks/issues/44136
TEST_F(LakeCompactionSchedulerTest, test_issue44136) {
    SyncPoint::GetInstance()->LoadDependency(
            {{"lake::CompactionScheduler::abort:unlock:1", "lake::CompactionTaskCallback::finish_task:finish_task"},
             {"lake::CompactionTaskCallback::finish_task:finish_task", "lake::CompactionScheduler::abort:unlock:2"}});
    SyncPoint::GetInstance()->EnableProcessing();
    SCOPED_CLEANUP({ SyncPoint::GetInstance()->DisableProcessing(); });

    auto txn_id = next_id();
    auto latch = std::make_shared<CountDownLatch>(1);
    auto request = CompactRequest{};
    auto response = CompactResponse{};
    request.add_tablet_ids(_tablet_metadata->id());
    request.set_timeout_ms(/*1 minute=*/60 * 1000);
    request.set_txn_id(txn_id);
    request.set_version(1);
    auto cb = ::google::protobuf::NewCallback(notify_for_callback, latch);
    _compaction_scheduler.compact(nullptr, &request, &response, cb);

    _compaction_scheduler.abort(txn_id);

    latch->wait();
}

TEST_F(LakeCompactionSchedulerTest, test_abort_with_not_write_txnlog) {
    auto txn_id = next_id();
    auto latch = std::make_shared<CountDownLatch>(1);
    auto request = CompactRequest{};
    auto response = CompactResponse{};
    request.add_tablet_ids(_tablet_metadata->id());
    request.set_timeout_ms(/*1 minute=*/60 * 1000);
    request.set_txn_id(txn_id);
    request.set_version(1);
    request.set_skip_write_txnlog(true);
    auto cb = ::google::protobuf::NewCallback(notify_for_callback, latch);
    TEST_ENABLE_ERROR_POINT("VerticalCompactionTask::execute::1", Status::IOError("injected error"));
    TEST_ENABLE_ERROR_POINT("HorizontalCompactionTask::execute::1", Status::IOError("injected error"));
    TEST_ENABLE_ERROR_POINT("CloudNativeIndexCompactionTask::execute::1", Status::IOError("injected error"));
    SyncPoint::GetInstance()->EnableProcessing();
    _compaction_scheduler.compact(nullptr, &request, &response, cb);
    latch->wait();
    TEST_DISABLE_ERROR_POINT("VerticalCompactionTask::execute::1");
    TEST_DISABLE_ERROR_POINT("HorizontalCompactionTask::execute::1");
    TEST_DISABLE_ERROR_POINT("CloudNativeIndexCompactionTask::execute::1");
    SyncPoint::GetInstance()->DisableProcessing();
}

// Test for process_parallel_compaction (lines 299-369 in compaction_scheduler.cpp)
TEST_F(LakeCompactionSchedulerTest, test_parallel_compaction_basic) {
    // Create a tablet with multiple rowsets for parallel compaction
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    metadata->set_id(next_id());
    metadata->set_version(11);

    // Add rowsets
    for (int i = 0; i < 10; i++) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(i);
        rowset->set_overlapped(true);
        rowset->set_num_rows(100);
        rowset->set_data_size(1024 * 1024); // 1MB each
        auto* segment_meta = rowset->add_segment_metas();
        segment_meta->set_filename(fmt::format("segment_{}.dat", i));
        segment_meta->set_size(1024 * 1024);
    }

    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    auto txn_id = next_id();
    auto latch = std::make_shared<CountDownLatch>(1);

    CompactRequest request;
    CompactResponse response;
    request.add_tablet_ids(metadata->id());
    request.set_timeout_ms(60 * 1000);
    request.set_txn_id(txn_id);
    request.set_version(11);

    // Enable parallel compaction
    auto* parallel_config = request.mutable_parallel_config();
    parallel_config->set_enable_parallel(true);
    parallel_config->set_max_parallel_per_tablet(3);
    parallel_config->set_max_bytes_per_subtask(5 * 1024 * 1024); // 5MB limit

    auto cb = ::google::protobuf::NewCallback(notify_for_callback, latch);
    _compaction_scheduler.compact(nullptr, &request, &response, cb);
    latch->wait();

    // The response should have some result (success or failure with txn_log)
    // Since we're testing the code path, we verify it didn't crash
}

// Test parallel compaction with single tablet fallback on failure
TEST_F(LakeCompactionSchedulerTest, test_parallel_compaction_fallback) {
    // Create a tablet with rowsets
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    metadata->set_id(next_id());
    metadata->set_version(11);

    // Add rowsets
    for (int i = 0; i < 5; i++) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(i);
        rowset->set_overlapped(true);
        rowset->set_num_rows(100);
        rowset->set_data_size(1024 * 1024);
        auto* segment_meta = rowset->add_segment_metas();
        segment_meta->set_filename(fmt::format("segment_{}.dat", i));
        segment_meta->set_size(1024 * 1024);
    }

    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    auto txn_id = next_id();
    auto latch = std::make_shared<CountDownLatch>(1);

    CompactRequest request;
    CompactResponse response;
    request.add_tablet_ids(metadata->id());
    request.set_timeout_ms(60 * 1000);
    request.set_txn_id(txn_id);
    request.set_version(11);

    // Enable parallel compaction with very small max_bytes to create multiple groups
    auto* parallel_config = request.mutable_parallel_config();
    parallel_config->set_enable_parallel(true);
    parallel_config->set_max_parallel_per_tablet(2);
    parallel_config->set_max_bytes_per_subtask(2 * 1024 * 1024); // 2MB limit

    auto cb = ::google::protobuf::NewCallback(notify_for_callback, latch);
    _compaction_scheduler.compact(nullptr, &request, &response, cb);
    latch->wait();
}

// Test parallel compaction with multiple tablets
TEST_F(LakeCompactionSchedulerTest, test_parallel_compaction_multiple_tablets) {
    std::vector<int64_t> tablet_ids;

    // Create multiple tablets with rowsets
    for (int t = 0; t < 3; t++) {
        auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
        metadata->set_id(next_id());
        metadata->set_version(11);
        tablet_ids.push_back(metadata->id());

        for (int i = 0; i < 5; i++) {
            auto* rowset = metadata->add_rowsets();
            rowset->set_id(i);
            rowset->set_overlapped(true);
            rowset->set_num_rows(100);
            rowset->set_data_size(1024 * 1024);
            auto* segment_meta = rowset->add_segment_metas();
            segment_meta->set_filename(fmt::format("segment_{}.dat", i));
            segment_meta->set_size(1024 * 1024);
        }

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));
    }

    auto txn_id = next_id();
    auto latch = std::make_shared<CountDownLatch>(1);

    CompactRequest request;
    CompactResponse response;
    for (auto tablet_id : tablet_ids) {
        request.add_tablet_ids(tablet_id);
    }
    request.set_timeout_ms(60 * 1000);
    request.set_txn_id(txn_id);
    request.set_version(11);

    // Enable parallel compaction
    auto* parallel_config = request.mutable_parallel_config();
    parallel_config->set_enable_parallel(true);
    parallel_config->set_max_parallel_per_tablet(2);
    parallel_config->set_max_bytes_per_subtask(3 * 1024 * 1024);

    auto cb = ::google::protobuf::NewCallback(notify_for_callback, latch);
    _compaction_scheduler.compact(nullptr, &request, &response, cb);
    latch->wait();
}

// Regression test for issue #76882: CompactionScheduler::compact() must NOT run the IO-heavy
// process_parallel_compaction() (tablet-metadata load + StarletFileSystem creation) on the calling
// brpc bthread. Running blocking filesystem IO on a bthread can make a pthread rwlock (StarOSWorker's
// std::shared_mutex _cache_mtx) return EDEADLK on an unrelated bthread sharing the same worker pthread,
// which surfaces as an uncaught std::system_error("Resource deadlock avoided") and aborts the CN. The
// fix offloads process_parallel_compaction() to the dedicated _threads pthread pool. This test pins the
// offload: it asserts process_parallel_compaction() executes on a thread different from the caller.
// (The other parallel tests above only prove "did not crash / did complete"; they pass against the old
// inline code too, so they cannot catch a regression back to on-bthread execution.)
TEST_F(LakeCompactionSchedulerTest, test_parallel_compaction_runs_off_caller_thread) {
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    metadata->set_id(next_id());
    metadata->set_version(11);
    for (int i = 0; i < 10; i++) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(i);
        rowset->set_overlapped(true);
        rowset->set_num_rows(100);
        rowset->set_data_size(1024 * 1024);
        auto* segment_meta = rowset->add_segment_metas();
        segment_meta->set_filename(fmt::format("segment_{}.dat", i));
        segment_meta->set_size(1024 * 1024);
    }
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    const auto caller_id = std::this_thread::get_id();
    // Compute the "ran off the caller thread" result inside the callback (where both ids are known) and
    // publish it via an atomic, so the main thread never reads a std::thread::id written on another
    // thread. caller_id is set before compact() and only read here, so it is not racy.
    std::atomic<bool> fired{false};
    std::atomic<bool> ran_off_caller_thread{false};
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack("CompactionScheduler::process_parallel_compaction:enter", [&](void* /*arg*/) {
        ran_off_caller_thread.store(std::this_thread::get_id() != caller_id);
        fired.store(true);
    });
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearCallBack("CompactionScheduler::process_parallel_compaction:enter");
        sync_point->DisableProcessing();
    });

    auto txn_id = next_id();
    auto latch = std::make_shared<CountDownLatch>(1);
    CompactRequest request;
    CompactResponse response;
    request.add_tablet_ids(metadata->id());
    request.set_timeout_ms(60 * 1000);
    request.set_txn_id(txn_id);
    request.set_version(11);
    auto* parallel_config = request.mutable_parallel_config();
    parallel_config->set_enable_parallel(true);
    parallel_config->set_max_parallel_per_tablet(3);
    parallel_config->set_max_bytes_per_subtask(5 * 1024 * 1024);

    auto cb = ::google::protobuf::NewCallback(notify_for_callback, latch);
    _compaction_scheduler.compact(nullptr, &request, &response, cb);
    // done->Run() (hence latch) only fires after process_parallel_compaction has entered, so the hook is
    // guaranteed to have run by the time wait() returns.
    latch->wait();

    EXPECT_TRUE(fired.load());
    // The offloaded task runs on a _threads pool worker, never on the caller (gtest) thread.
    EXPECT_TRUE(ran_off_caller_thread.load());
}

// Regression test for the #76882 fix's shutdown-safety guarantee. When the deferred parallel-compaction
// task is cancelled in the thread pool (as happens when stop()/shutdown() drains the queue), the
// CancellableRunnable's canceller MUST still complete the RPC (run `done`), otherwise the FE's compact RPC
// hangs forever and the closure leaks. A plain submit_func() runnable has a no-op cancel() and would
// exhibit exactly that hang; this test pins the CancellableRunnable choice.
//
// The "ThreadPool::do_submit:replace_task" sync point runs its callback synchronously inside submit() on
// the caller thread, so we cancel the just-submitted dispatcher there (and swap in a no-op runnable). This
// makes the whole test deterministic: `done` runs during compact() via the canceller, so latch->wait()
// returns without depending on any background thread timing.
TEST_F(LakeCompactionSchedulerTest, test_parallel_compaction_cancelled_completes_rpc) {
    class MockRunnable : public Runnable {
    public:
        void run() override {}
        void cancel() override {}
    };

    auto* sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack("ThreadPool::do_submit:replace_task", [](void* arg) {
        auto ptr = (*(std::shared_ptr<Runnable>*)arg);
        ptr->cancel(); // invoke the dispatcher's canceller (must reject_request + run `done`)
        (*(std::shared_ptr<Runnable>*)arg) = std::make_shared<MockRunnable>();
    });
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearCallBack("ThreadPool::do_submit:replace_task");
        sync_point->DisableProcessing();
    });

    auto txn_id = next_id();
    auto latch = std::make_shared<CountDownLatch>(1);
    CompactRequest request;
    CompactResponse response;
    request.add_tablet_ids(_tablet_metadata->id());
    request.set_timeout_ms(60 * 1000);
    request.set_txn_id(txn_id);
    request.set_version(1);
    auto* parallel_config = request.mutable_parallel_config();
    parallel_config->set_enable_parallel(true);
    parallel_config->set_max_parallel_per_tablet(3);
    parallel_config->set_max_bytes_per_subtask(5 * 1024 * 1024);

    auto cb = ::google::protobuf::NewCallback(notify_for_callback, latch);
    _compaction_scheduler.compact(nullptr, &request, &response, cb);
    // The canceller ran `done` synchronously during submit(); wait() must not hang.
    latch->wait();

    // The RPC was completed (not leaked) with a non-OK status.
    EXPECT_NE(0, response.status().status_code());
}

// Third and last `done`-exactly-once path of the #76882 fix: when submitting the deferred
// parallel-compaction task to the pool FAILS, the task was never enqueued (so neither run() nor cancel()
// will ever fire) and compact() itself must complete the RPC, surfacing the real submit error. If it
// didn't, the FE's compact RPC would hang forever.
//
// "ThreadPool::do_submit:1" hands the callback a pointer to the pool's computed capacity_remaining;
// forcing it to 0 makes submit() return ServiceUnavailable deterministically.
TEST_F(LakeCompactionSchedulerTest, test_parallel_compaction_submit_failure_completes_rpc) {
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack("ThreadPool::do_submit:1", [](void* arg) { *(int64_t*)arg = 0; });
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearCallBack("ThreadPool::do_submit:1");
        sync_point->DisableProcessing();
    });

    auto txn_id = next_id();
    auto latch = std::make_shared<CountDownLatch>(1);
    CompactRequest request;
    CompactResponse response;
    request.add_tablet_ids(_tablet_metadata->id());
    request.set_timeout_ms(60 * 1000);
    request.set_txn_id(txn_id);
    request.set_version(1);
    auto* parallel_config = request.mutable_parallel_config();
    parallel_config->set_enable_parallel(true);
    parallel_config->set_max_parallel_per_tablet(3);
    parallel_config->set_max_bytes_per_subtask(5 * 1024 * 1024);

    auto cb = ::google::protobuf::NewCallback(notify_for_callback, latch);
    _compaction_scheduler.compact(nullptr, &request, &response, cb);
    // compact() ran `done` inline on the submit-failure path; wait() must not hang.
    latch->wait();

    // The real submit error is surfaced, not a generic shutdown status.
    EXPECT_NE(0, response.status().status_code());
}

// Once the IO-heavy task creation is offloaded to `_threads`, an exception escaping it stops being a crash
// and becomes a silent hang: ThreadPool::dispatch_thread only logs an escaping exception (it does not run
// the runnable's cancel()), compact() has already released the ClosureGuard, and ~CompactionTaskCallback()
// does not run `done`. Nothing would ever complete the RPC, so the FE would block until its compact timeout.
//
// Inject the #76882 exception itself -- std::system_error("Resource deadlock avoided"), what a pthread rwlock
// returning EDEADLK raises through std::shared_mutex -- into create_parallel_tasks() and assert the RPC still
// completes. process_parallel_compaction() must absorb it and fall back to normal compaction, which keeps one
// finish_task() per tablet id and therefore still runs `done`. Both handlers are exercised: the std::exception
// one and the catch-all that covers a foreign exception thrown through the same call.
TEST_F(LakeCompactionSchedulerTest, test_parallel_compaction_exception_completes_rpc) {
    struct ForeignException {};

    for (bool foreign : {false, true}) {
        std::atomic<bool> injected{false};
        auto* sync_point = SyncPoint::GetInstance();
        sync_point->SetCallBack(
                "CompactionScheduler::process_parallel_compaction:create_parallel_tasks", [&](void* /*arg*/) {
                    injected.store(true);
                    if (foreign) {
                        throw ForeignException{};
                    }
                    throw std::system_error(std::make_error_code(std::errc::resource_deadlock_would_occur),
                                            "Resource deadlock avoided");
                });
        sync_point->EnableProcessing();
        SCOPED_CLEANUP({
            sync_point->ClearCallBack("CompactionScheduler::process_parallel_compaction:create_parallel_tasks");
            sync_point->DisableProcessing();
        });

        auto txn_id = next_id();
        auto latch = std::make_shared<CountDownLatch>(1);
        CompactRequest request;
        CompactResponse response;
        request.add_tablet_ids(_tablet_metadata->id());
        request.set_timeout_ms(60 * 1000);
        request.set_txn_id(txn_id);
        request.set_version(1);
        auto* parallel_config = request.mutable_parallel_config();
        parallel_config->set_enable_parallel(true);
        parallel_config->set_max_parallel_per_tablet(3);
        parallel_config->set_max_bytes_per_subtask(5 * 1024 * 1024);

        auto cb = ::google::protobuf::NewCallback(notify_for_callback, latch);
        _compaction_scheduler.compact(nullptr, &request, &response, cb);
        // The exception was absorbed into the fallback path, which completes the RPC; wait() must not hang.
        latch->wait();

        EXPECT_TRUE(injected.load());
    }
}

// Companion to the test above, for the dangerous arm it cannot reach. That test injects its throw at the
// ":create_parallel_tasks" hook, which fires *before* create_parallel_tasks() runs, so no subtask has been
// submitted yet and falling back to normal compaction is safe.
//
// Here the throw lands *after* a subtask was already submitted and is running (the real shape of a
// std::bad_alloc from the per-group bookkeeping in submit_subtasks_from_groups). That subtask already owns
// the tablet's single finish_task(): TabletParallelCompactionState::is_complete() is
// `running_subtasks.empty() && total_subtasks_created > 0`, so it fires when the subtask completes. If the
// tablet were *also* routed into the fallback path, finish_task() would run twice for one tablet id, push
// _contexts past tablet_ids_size(), and dereference the `_response` the first completion already nulled ->
// SIGSEGV. So the RPC must complete exactly once.
TEST_F(LakeCompactionSchedulerTest, test_parallel_compaction_exception_after_submit_completes_rpc_once) {
    auto metadata = generate_simple_tablet_metadata(DUP_KEYS);
    metadata->set_id(next_id());
    metadata->set_version(11);
    for (int i = 0; i < 10; i++) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(i);
        rowset->set_overlapped(true);
        rowset->set_num_rows(100);
        rowset->set_data_size(1024 * 1024);
        auto* segment_meta = rowset->add_segment_metas();
        segment_meta->set_filename(fmt::format("segment_{}.dat", i));
        segment_meta->set_size(1024 * 1024);
    }
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*metadata));

    // Throw only on the first submitted subtask, so at least one subtask stays in flight.
    std::atomic<bool> thrown{false};
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack("TabletParallelCompactionManager::submit_subtasks_from_groups:after_submit",
                            [&](void* /*arg*/) {
                                if (!thrown.exchange(true)) {
                                    throw std::bad_alloc();
                                }
                            });
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearCallBack("TabletParallelCompactionManager::submit_subtasks_from_groups:after_submit");
        sync_point->DisableProcessing();
    });

    auto txn_id = next_id();
    auto latch = std::make_shared<CountDownLatch>(1);
    CompactRequest request;
    CompactResponse response;
    request.add_tablet_ids(metadata->id());
    request.set_timeout_ms(60 * 1000);
    request.set_txn_id(txn_id);
    request.set_version(11);
    auto* parallel_config = request.mutable_parallel_config();
    parallel_config->set_enable_parallel(true);
    parallel_config->set_max_parallel_per_tablet(3);
    parallel_config->set_max_bytes_per_subtask(5 * 1024 * 1024);

    auto cb = ::google::protobuf::NewCallback(notify_for_callback, latch);
    _compaction_scheduler.compact(nullptr, &request, &response, cb);
    // The in-flight subtask completes the RPC; wait() must not hang.
    latch->wait();

    EXPECT_TRUE(thrown.load());
    // Exactly one finish_task() ran for the single tablet: finish_task() appends one compact_stat per call,
    // so a second (crashing) completion would show up here as an extra entry.
    EXPECT_EQ(1, response.compact_stats_size());
}

} // namespace starrocks::lake
