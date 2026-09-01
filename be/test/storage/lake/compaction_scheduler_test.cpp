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

#include <chrono>

#include "base/bthreads/util.h"
#include "base/concurrency/countdown_latch.h"
#include "base/metrics.h"
#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "base/utility/scoped_cleanup.h"
#include "common/config_compaction_fwd.h"
#include "common/config_storage_fwd.h"
#include "gen_cpp/lake_service.pb.h"
#include "runtime/descriptors.h"
#include "storage/lake/compaction_task_context.h"
#include "storage/lake/metacache.h"
#include "storage/lake/test_util.h"
#include "storage/storage_metrics.h"

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
    LakeCompactionSchedulerTest()
            : TestBase(kTestDirectory), _compaction_scheduler(*_tablet_mgr->compaction_scheduler()) {
        clear_and_init_test_dir();
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
        CHECK_OK(_tablet_mgr->put_tablet_metadata(_tablet_metadata));
    }

protected:
    constexpr static const char* kTestDirectory = "test_compaction_scheduler";

    CompactionScheduler& _compaction_scheduler;
    std::shared_ptr<TabletMetadata> _tablet_metadata;
};

TEST_F(LakeCompactionSchedulerTest, test_task_queue) {
    CompactionScheduler::WrapTaskQueues queue(10);
    EXPECT_EQ(0, queue.queued_tasks());

    auto ctx = std::make_unique<CompactionTaskContext>(100 /* txn_id */, 101 /* tablet_id */, 1 /* version */,
                                                       false /* force_base_compaction */, false, nullptr);
    queue.set_target_size(5);
    ASSERT_EQ(5, queue.target_size());
    queue.put_by_txn_id(ctx->txn_id, ctx);
    EXPECT_EQ(1, queue.queued_tasks());

    std::vector<std::unique_ptr<CompactionTaskContext>> v;
    auto ctx2 = std::make_unique<CompactionTaskContext>(101 /* txn_id */, 102 /* tablet_id */, 1 /* version */,
                                                        false /* force_base_compaction */, false, nullptr);
    v.emplace_back(std::move(ctx2));
    queue.put_by_txn_id(101 /* txn_id */, v);
    EXPECT_EQ(2, queue.queued_tasks());

    std::unique_ptr<CompactionTaskContext> dequeued;
    ASSERT_TRUE(queue.try_get(0, &dequeued));
    EXPECT_EQ(1, queue.queued_tasks());
    dequeued.reset();

    queue.steal_task(0, &dequeued);
    ASSERT_NE(nullptr, dequeued);
    EXPECT_EQ(0, queue.queued_tasks());
}

TEST_F(LakeCompactionSchedulerTest, test_register_lake_compaction_hook_before_install) {
    ConfigResetGuard<int32_t> compact_threads_guard(&config::compact_threads, 2);
    MetricRegistry registry("test_registry");
    StorageMetrics metrics;
    CompactionScheduler scheduler(_tablet_mgr.get());
    scheduler.stop();

    ASSERT_TRUE(metrics.register_lake_compaction_hook(&scheduler));
    DeferOp hook_guard([&metrics] { metrics.deregister_lake_compaction_hook(); });

    metrics.install(&registry);
    metrics.lake_compaction_max_concurrency.set_value(123);
    registry.trigger_hook();

    auto* metric = registry.get_metric("lake_compaction_max_concurrency");
    ASSERT_NE(nullptr, metric);
    EXPECT_EQ("2", metric->to_string());
}

TEST_F(LakeCompactionSchedulerTest, test_deregister_lake_compaction_hook_before_install) {
    ConfigResetGuard<int32_t> compact_threads_guard(&config::compact_threads, 2);
    MetricRegistry registry("test_registry");
    StorageMetrics metrics;
    CompactionScheduler scheduler(_tablet_mgr.get());
    scheduler.stop();

    ASSERT_TRUE(metrics.register_lake_compaction_hook(&scheduler));
    metrics.deregister_lake_compaction_hook();

    metrics.install(&registry);
    DeferOp hook_guard([&metrics] { metrics.deregister_lake_compaction_hook(); });
    metrics.lake_compaction_max_concurrency.set_value(123);
    registry.trigger_hook();

    auto* metric = registry.get_metric("lake_compaction_max_concurrency");
    ASSERT_NE(nullptr, metric);
    EXPECT_EQ("123", metric->to_string());
}

TEST_F(LakeCompactionSchedulerTest, test_task_metrics) {
    ConfigResetGuard<int32_t> compact_threads_guard(&config::compact_threads, 2);
    MetricRegistry registry("test_registry");
    StorageMetrics metrics(&registry);
    auto metric_value = [&registry](const char* name) {
        auto* metric = registry.get_metric(name);
        CHECK(metric != nullptr) << name;
        return std::stoll(metric->to_string());
    };
    auto metric_value_by_mode = [&registry](const char* name, const char* mode) {
        auto* metric = registry.get_metric(name, MetricLabels().add("mode", mode));
        CHECK(metric != nullptr) << name << ":" << mode;
        return std::stoll(metric->to_string());
    };

    metrics.lake_compaction_max_concurrency.set_value(123);
    {
        CompactionScheduler scheduler(_tablet_mgr.get());
        scheduler.stop();
        const bool hook_registered = metrics.register_lake_compaction_hook(&scheduler);
        DeferOp hook_guard([&metrics, hook_registered] {
            if (hook_registered) {
                metrics.deregister_lake_compaction_hook();
            }
        });
        ASSERT_TRUE(hook_registered);
        EXPECT_EQ(123, metric_value("lake_compaction_max_concurrency"));

        ASSERT_TRUE(scheduler._limiter.acquire());
        scheduler._limiter.memory_limit_exceeded();
        registry.trigger_hook();
        EXPECT_EQ(2, metric_value("lake_compaction_max_concurrency"));
        EXPECT_EQ(1, metric_value("lake_compaction_effective_concurrency"));
    }

    config::compact_threads = 1;
    CompactionScheduler scheduler(_tablet_mgr.get());
    const bool hook_registered = metrics.register_lake_compaction_hook(&scheduler);
    DeferOp hook_guard([&metrics, hook_registered] {
        if (hook_registered) {
            metrics.deregister_lake_compaction_hook();
        }
    });
    ASSERT_TRUE(hook_registered);

    auto second_metadata = generate_simple_tablet_metadata(DUP_KEYS);
    second_metadata->set_id(next_id());
    CHECK_OK(_tablet_mgr->put_tablet_metadata(second_metadata));

    auto entered = std::make_shared<CountDownLatch>(1);
    auto release = std::make_shared<CountDownLatch>(1);
    SyncPoint::GetInstance()->SetCallBack("CompactionScheduler::do_compaction:before_execute_task", [&](void*) {
        entered->count_down();
        release->wait();
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp sync_point_guard([&]() {
        release->count_down();
        SyncPoint::GetInstance()->ClearCallBack("CompactionScheduler::do_compaction:before_execute_task");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    auto wait_for_latch = [&](const std::shared_ptr<CountDownLatch>& latch) {
        if (latch->wait_for(std::chrono::seconds(10))) {
            return true;
        }
        // Make fatal assertion cleanup safe: unblock and join the scheduler
        // before request/response objects captured by callbacks are destroyed.
        release->count_down();
        scheduler.stop();
        return false;
    };

    CompactRequest request1;
    CompactResponse response1;
    request1.add_tablet_ids(_tablet_metadata->id());
    request1.set_timeout_ms(60 * 1000);
    request1.set_txn_id(next_id());
    request1.set_version(1);
    auto done1 = std::make_shared<CountDownLatch>(1);
    scheduler.compact(nullptr, &request1, &response1, ::google::protobuf::NewCallback(notify_for_callback, done1));

    ASSERT_TRUE(wait_for_latch(entered));
    registry.trigger_hook();
    EXPECT_EQ(1, metric_value_by_mode("lake_compaction_running_tasks", "non_parallel"));
    EXPECT_EQ(0, metric_value_by_mode("lake_compaction_running_tasks", "parallel"));
    EXPECT_EQ(0, metric_value("lake_compaction_queued_tasks"));

    CompactRequest request2;
    CompactResponse response2;
    request2.add_tablet_ids(second_metadata->id());
    request2.set_timeout_ms(60 * 1000);
    request2.set_txn_id(next_id());
    request2.set_version(1);
    auto done2 = std::make_shared<CountDownLatch>(1);
    scheduler.compact(nullptr, &request2, &response2, ::google::protobuf::NewCallback(notify_for_callback, done2));

    registry.trigger_hook();
    EXPECT_EQ(1, metric_value_by_mode("lake_compaction_running_tasks", "non_parallel"));
    EXPECT_EQ(0, metric_value_by_mode("lake_compaction_running_tasks", "parallel"));
    EXPECT_EQ(1, metric_value("lake_compaction_queued_tasks"));

    release->count_down();
    ASSERT_TRUE(wait_for_latch(done1));
    ASSERT_TRUE(wait_for_latch(done2));

    CompactRequest request3;
    CompactResponse response3;
    request3.add_tablet_ids(next_id());
    request3.set_timeout_ms(60 * 1000);
    request3.set_txn_id(next_id());
    request3.set_version(1);
    auto done3 = std::make_shared<CountDownLatch>(1);
    scheduler.compact(nullptr, &request3, &response3, ::google::protobuf::NewCallback(notify_for_callback, done3));
    ASSERT_TRUE(wait_for_latch(done3));

    // finish_task() invokes the RPC callback before do_compaction() returns. stop()
    // joins the worker and therefore waits for the limiter slot to be released.
    scheduler.stop();

    registry.trigger_hook();
    EXPECT_EQ(0, metric_value_by_mode("lake_compaction_running_tasks", "non_parallel"));
    EXPECT_EQ(0, metric_value_by_mode("lake_compaction_running_tasks", "parallel"));
    EXPECT_EQ(0, metric_value("lake_compaction_queued_tasks"));
    EXPECT_EQ(2, metric_value_by_mode("lake_compaction_task_success_total", "non_parallel"));
    EXPECT_EQ(0, metric_value_by_mode("lake_compaction_task_success_total", "parallel"));
    EXPECT_EQ(1, metric_value_by_mode("lake_compaction_task_failure_total", "non_parallel"));
    EXPECT_EQ(0, metric_value_by_mode("lake_compaction_task_failure_total", "parallel"));
    EXPECT_EQ(0, metric_value("lake_compaction_parallel_fallback_total"));
    EXPECT_EQ(0, metric_value("lake_compaction_running_subtasks"));
    EXPECT_EQ(0, metric_value("lake_compaction_subtask_success_total"));
    EXPECT_EQ(0, metric_value("lake_compaction_subtask_failure_total"));

    metrics.deregister_lake_compaction_hook();
    metrics.lake_compaction_non_parallel_running_tasks.set_value(123);
    registry.trigger_hook();
    EXPECT_EQ(123, metric_value_by_mode("lake_compaction_running_tasks", "non_parallel"));
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

    EXPECT_EQ(0, _compaction_scheduler.non_parallel_task_success_total());
    EXPECT_EQ(1, _compaction_scheduler.non_parallel_task_failure_total());

    // A task that failed before producing a txn log must not leave anything behind in the metacache.
    EXPECT_EQ(nullptr,
              _tablet_mgr->metacache()->lookup_txn_log(_tablet_mgr->txn_log_location(_tablet_metadata->id(), txn_id)));
}

// skip_write_txnlog is the aggregate/file-bundling path: the compaction worker returns the txn log
// inline instead of persisting it, and the aggregator folds every tablet's log into one combined
// `<txn_id>.logs` object. The worker must still populate the metacache under the per-tablet txn log
// key, because that is what the publish path (`load_txn_log()` in transactions.cpp) probes before
// falling back to a remote read of the combined log.
TEST_F(LakeCompactionSchedulerTest, test_skip_write_txnlog_fills_metacache) {
    auto txn_id = next_id();
    auto latch = std::make_shared<CountDownLatch>(1);
    auto request = CompactRequest{};
    auto response = CompactResponse{};
    request.add_tablet_ids(_tablet_metadata->id());
    request.set_timeout_ms(/*1 minute=*/60 * 1000);
    request.set_txn_id(txn_id);
    request.set_version(1);
    request.set_skip_write_txnlog(true);

    auto log_path = _tablet_mgr->txn_log_location(_tablet_metadata->id(), txn_id);
    ASSERT_EQ(nullptr, _tablet_mgr->metacache()->lookup_txn_log(log_path));

    auto cb = ::google::protobuf::NewCallback(notify_for_callback, latch);
    _compaction_scheduler.compact(nullptr, &request, &response, cb);
    latch->wait();

    ASSERT_EQ(0, response.failed_tablets_size());
    ASSERT_EQ(1, response.txn_logs_size());

    // The log was not written to object storage ...
    EXPECT_FALSE(fs::path_exist(log_path));
    // ... but the publish path can still find it without a remote read.
    auto cached = _tablet_mgr->metacache()->lookup_txn_log(log_path);
    ASSERT_NE(nullptr, cached);
    EXPECT_EQ(_tablet_metadata->id(), cached->tablet_id());
    EXPECT_EQ(txn_id, cached->txn_id());
    EXPECT_TRUE(cached->has_op_compaction());
    EXPECT_EQ(response.txn_logs(0).op_compaction().compact_version(), cached->op_compaction().compact_version());
}

// Test for process_parallel_compaction (lines 299-369 in compaction_scheduler.cpp)
TEST_F(LakeCompactionSchedulerTest, test_parallel_compaction_basic) {
    MetricRegistry registry("parallel_compaction_basic_metrics");
    StorageMetrics metrics(&registry);
    ASSERT_TRUE(metrics.register_lake_compaction_hook(&_compaction_scheduler));
    DeferOp hook_guard([&metrics] { metrics.deregister_lake_compaction_hook(); });
    auto metric_value = [&registry](const char* name) {
        auto* metric = registry.get_metric(name);
        CHECK(metric != nullptr) << name;
        return std::stoll(metric->to_string());
    };
    auto metric_value_by_mode = [&registry](const char* name, const char* mode) {
        auto* metric = registry.get_metric(name, MetricLabels().add("mode", mode));
        CHECK(metric != nullptr) << name << ":" << mode;
        return std::stoll(metric->to_string());
    };

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

    registry.trigger_hook();
    EXPECT_EQ(0, metric_value_by_mode("lake_compaction_running_tasks", "parallel"));
    EXPECT_EQ(0, metric_value_by_mode("lake_compaction_task_success_total", "parallel"));
    EXPECT_EQ(1, metric_value_by_mode("lake_compaction_task_failure_total", "parallel"));
    EXPECT_EQ(0, metric_value_by_mode("lake_compaction_task_success_total", "non_parallel"));
    EXPECT_EQ(0, metric_value_by_mode("lake_compaction_task_failure_total", "non_parallel"));
    EXPECT_EQ(0, metric_value("lake_compaction_running_subtasks"));
    EXPECT_EQ(0, metric_value("lake_compaction_subtask_success_total"));
    EXPECT_EQ(2, metric_value("lake_compaction_subtask_failure_total"));
    EXPECT_EQ(0, metric_value("lake_compaction_parallel_fallback_total"));
}

// Test parallel compaction fallback is counted for each tablet task.
TEST_F(LakeCompactionSchedulerTest, test_parallel_compaction_fallback) {
    MetricRegistry registry("parallel_compaction_fallback_metrics");
    StorageMetrics metrics(&registry);
    ASSERT_TRUE(metrics.register_lake_compaction_hook(&_compaction_scheduler));
    DeferOp hook_guard([&metrics] { metrics.deregister_lake_compaction_hook(); });
    auto metric_value = [&registry](const char* name) {
        auto* metric = registry.get_metric(name);
        CHECK(metric != nullptr) << name;
        return std::stoll(metric->to_string());
    };

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
    auto second_metadata = std::make_shared<TabletMetadata>(*metadata);
    second_metadata->set_id(next_id());
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*second_metadata));

    auto txn_id = next_id();
    auto latch = std::make_shared<CountDownLatch>(1);

    CompactRequest request;
    CompactResponse response;
    request.add_tablet_ids(metadata->id());
    request.add_tablet_ids(second_metadata->id());
    request.set_timeout_ms(60 * 1000);
    request.set_txn_id(txn_id);
    request.set_version(11);

    // Force parallel task creation to fail so both tablets fall back to the regular task queue.
    auto* parallel_config = request.mutable_parallel_config();
    parallel_config->set_enable_parallel(true);
    parallel_config->set_max_parallel_per_tablet(0);
    parallel_config->set_max_bytes_per_subtask(2 * 1024 * 1024); // 2MB limit

    auto cb = ::google::protobuf::NewCallback(notify_for_callback, latch);
    _compaction_scheduler.compact(nullptr, &request, &response, cb);
    latch->wait();

    EXPECT_EQ(2, _compaction_scheduler.parallel_fallback_total());
    EXPECT_EQ(2, _compaction_scheduler.non_parallel_task_success_total() +
                         _compaction_scheduler.non_parallel_task_failure_total());
    EXPECT_EQ(0, _compaction_scheduler.parallel_task_success_total());
    EXPECT_EQ(0, _compaction_scheduler.parallel_task_failure_total());
    EXPECT_EQ(0, _compaction_scheduler.running_subtasks());
    EXPECT_EQ(0, _compaction_scheduler.subtask_success_total());
    EXPECT_EQ(0, _compaction_scheduler.subtask_failure_total());

    registry.trigger_hook();
    EXPECT_EQ(2, metric_value("lake_compaction_parallel_fallback_total"));
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

TEST(LakeCompactionLimiterTest, test_adapt_to_task_queue_size_shrink_with_reserved) {
    CompactionScheduler::Limiter limiter(8);
    // Two tasks finished with memory limit exceeded, two tokens get reserved.
    ASSERT_TRUE(limiter.acquire());
    limiter.memory_limit_exceeded();
    ASSERT_TRUE(limiter.acquire());
    limiter.memory_limit_exceeded();
    ASSERT_EQ(6, limiter.concurrency());

    // Shrink the total from 8 to 4: the reserved tokens are scaled down
    // proportionally (2 * 4/8 = 1) instead of being inflated.
    limiter.adapt_to_task_queue_size(4);
    ASSERT_EQ(3, limiter.concurrency());
    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(limiter.acquire());
    }
    ASSERT_FALSE(limiter.acquire());
}

TEST(LakeCompactionLimiterTest, test_adapt_to_task_queue_size_preserves_inflight_tokens) {
    CompactionScheduler::Limiter limiter(8);
    // Three tasks are running.
    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(limiter.acquire());
    }

    limiter.adapt_to_task_queue_size(4);
    ASSERT_EQ(4, limiter.concurrency());
    // Only one more token can be granted while the three tasks are still running.
    ASSERT_TRUE(limiter.acquire());
    ASSERT_FALSE(limiter.acquire());

    // After all running tasks finished, the concurrency is still bounded by the new total.
    for (int i = 0; i < 4; i++) {
        limiter.no_memory_limit_exceeded();
    }
    for (int i = 0; i < 4; i++) {
        ASSERT_TRUE(limiter.acquire());
    }
    ASSERT_FALSE(limiter.acquire());
}

TEST(LakeCompactionLimiterTest, test_adapt_to_task_queue_size_shrink_keeps_one_token) {
    CompactionScheduler::Limiter limiter(4);
    ASSERT_TRUE(limiter.acquire());
    limiter.memory_limit_exceeded();
    ASSERT_TRUE(limiter.acquire());
    limiter.memory_limit_exceeded();
    ASSERT_EQ(2, limiter.concurrency());

    // Shrinking to 1 must keep one grantable token instead of zeroing the concurrency.
    limiter.adapt_to_task_queue_size(1);
    ASSERT_EQ(1, limiter.concurrency());
    ASSERT_TRUE(limiter.acquire());
    ASSERT_FALSE(limiter.acquire());
}

TEST(LakeCompactionLimiterTest, test_adapt_to_task_queue_size_grow) {
    CompactionScheduler::Limiter limiter(4);
    ASSERT_TRUE(limiter.acquire());
    limiter.memory_limit_exceeded();
    ASSERT_EQ(3, limiter.concurrency());

    limiter.adapt_to_task_queue_size(8);
    ASSERT_EQ(7, limiter.concurrency());
    for (int i = 0; i < 7; i++) {
        ASSERT_TRUE(limiter.acquire());
    }
    ASSERT_FALSE(limiter.acquire());
}

} // namespace starrocks::lake
