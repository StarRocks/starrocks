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

#include "storage/lake/compaction_task_context.h"
#include "storage/lake/test_util.h"
#include "testutil/assert.h"
#include "util/bthreads/util.h"
#include "util/countdown_latch.h"
#include "util/scoped_cleanup.h"

namespace starrocks::lake {

inline void notify_and_wait_latch(std::shared_ptr<CountDownLatch> l1, std::shared_ptr<CountDownLatch> l2) {
    l1->count_down();
    l2->wait();
}

inline void notify(std::shared_ptr<CountDownLatch> latch) {
    latch->count_down();
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
                                                       false /* force_base_compaction */, nullptr);
    queue.set_target_size(5);
    ASSERT_EQ(5, queue.target_size());
    queue.put_by_txn_id(ctx->txn_id, ctx);

    std::vector<std::unique_ptr<CompactionTaskContext>> v;
    auto ctx2 = std::make_unique<CompactionTaskContext>(101 /* txn_id */, 102 /* tablet_id */, 1 /* version */,
                                                        false /* force_base_compaction */, nullptr);
    v.push_back(std::move(ctx2));
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
                        auto cb = ::google::protobuf::NewCallback(notify_and_wait_latch, l1, l2);
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

    bthread_join(tid, nullptr);
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
        requests.push_back(request);
        auto response = std::make_shared<CompactResponse>();
        responses.push_back(response);
        auto meta = generate_simple_tablet_metadata(DUP_KEYS);
        CHECK_OK(_tablet_mgr->put_tablet_metadata(meta));
        request->add_tablet_ids(meta->id());
        request->set_timeout_ms(60 * 1000); // 60 seconds
        request->set_txn_id(txn_id);
        request->set_version(1);
        // wait l2, count down l0
        ASSIGN_OR_ABORT(auto tid, bthreads::start_bthread([&, l1, l2, request, response]() {
                            auto cb = ::google::protobuf::NewCallback(notify_and_wait_latch, l0, l2);
                            _compaction_scheduler.compact(nullptr, request.get(), response.get(), cb);
                        }));
        tids.push_back(tid);
    }
    // Wait for task0 complete
    l0->wait();
    // repeatedly submit num_tasks into the queue, make the thread busy before stop() invoked.
    for (int i = 0; i < num_tasks; ++i) {
        auto txn_id = next_id();
        auto request = std::make_shared<CompactRequest>();
        requests.push_back(request);
        auto response = std::make_shared<CompactResponse>();
        responses.push_back(response);
        auto meta = generate_simple_tablet_metadata(DUP_KEYS);
        CHECK_OK(_tablet_mgr->put_tablet_metadata(meta));
        request->add_tablet_ids(meta->id());
        request->set_timeout_ms(60 * 1000); // 60 seconds
        request->set_txn_id(txn_id);
        request->set_version(1);
        // wait l2, count down l1
        ASSIGN_OR_ABORT(auto tid, bthreads::start_bthread([&, l1, l2, l3, request, response]() {
                            auto cb = ::google::protobuf::NewCallback(notify_and_wait_latch, l1, l2);
                            l3->count_down();
                            _compaction_scheduler.compact(nullptr, request.get(), response.get(), cb);
                        }));
        tids.push_back(tid);
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

    for (auto tid : tids) {
        bthread_join(tid, nullptr);
    }

    int aborted = 0;
    for (auto response : responses) {
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
    auto cb = ::google::protobuf::NewCallback(notify, l1);
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
                                  false /* force_base_compaction */, cb);
        cb->update_status(Status::Aborted("aborted for test"));
        EXPECT_FALSE(compaction_should_cancel(&ctx).ok());
    }

    // not valid time interval, should return early
    {
        auto check_interval = config::lake_compaction_check_valid_interval_minutes;
        config::lake_compaction_check_valid_interval_minutes = -1;
        auto cb = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, nullptr);
        CompactionTaskContext ctx(100 /* txn_id */, 101 /* tablet_id */, 1 /* version */,
                                  false /* force_base_compaction */, cb);
        EXPECT_TRUE(compaction_should_cancel(&ctx).ok());
        config::lake_compaction_check_valid_interval_minutes = check_interval;
    }

    // try_lock succeed and check time not satisfied
    {
        auto check_interval = config::lake_compaction_check_valid_interval_minutes;
        config::lake_compaction_check_valid_interval_minutes = 24 * 60; // set to a big value
        auto cb = std::make_shared<CompactionTaskCallback>(nullptr, &request, &response, nullptr);
        CompactionTaskContext ctx(100 /* txn_id */, 101 /* tablet_id */, 1 /* version */,
                                  false /* force_base_compaction */, cb);

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
    auto cb = ::google::protobuf::NewCallback(notify, latch);
    _compaction_scheduler.compact(nullptr, &request, &response, cb);

    _compaction_scheduler.abort(txn_id);

    latch->wait();
}

} // namespace starrocks::lake
