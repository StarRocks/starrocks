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

#include "storage/lake/combined_txn_log_writer.h"

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <set>

#include "base/failpoint/fail_point.h"
#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "common/config_ingest_fwd.h"
#include "common/thread/threadpool.h"
#include "gen_cpp/lake_types.pb.h"
#include "runtime/runtime_env.h"

namespace starrocks::lake {

class WriteCombinedTxnLogTest : public testing::Test {
public:
    WriteCombinedTxnLogTest() {}
};

TEST_F(WriteCombinedTxnLogTest, test_write_combined_txn_log_parallel) {
    std::map<int64_t, CombinedTxnLogPB> txn_log_map;
    size_t N = 2;
    for (int64_t i = 0; i < N; i++) {
        CombinedTxnLogPB combinde_txn_log_pb;
        txn_log_map.insert(std::make_pair(i, std::move(combinde_txn_log_pb)));
    }
    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    auto fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get("put_combined_txn_log_success");
    fp->setMode(trigger_mode);
    ASSERT_TRUE(write_combined_txn_log_parallel(txn_log_map).ok());
    trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
    fp->setMode(trigger_mode);

    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get("put_combined_txn_log_fail");
    fp->setMode(trigger_mode);
    ASSERT_FALSE(write_combined_txn_log_parallel(txn_log_map).ok());
    trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
    fp->setMode(trigger_mode);
}

namespace {
CombinedTxnLogPB make_logs(int64_t partition_id, const std::vector<int64_t>& tablet_ids) {
    CombinedTxnLogPB logs;
    for (int64_t tablet_id : tablet_ids) {
        auto* log = logs.add_txn_logs();
        log->set_tablet_id(tablet_id);
        log->set_txn_id(1000);
        log->set_partition_id(partition_id);
    }
    return logs;
}
} // namespace

// A combined txn log missing an entry makes the transaction permanently unpublishable once it
// commits: publish resolves every tablet inside that object and has no per-tablet fallback. The
// write must be refused instead of leaving a partial object behind.
TEST_F(WriteCombinedTxnLogTest, test_refuse_incomplete_combined_txn_log) {
    // The partition owns tablets {1, 2, 3}; only two of them made it into the collected log.
    std::map<int64_t, CombinedTxnLogPB> txn_log_map;
    txn_log_map.emplace(100, make_logs(100, {1, 2}));
    ExpectedTabletsByPartition expected;
    expected.emplace(100, std::set<int64_t>{1, 2, 3});

    auto st = write_combined_txn_log_parallel(txn_log_map, expected);
    ASSERT_FALSE(st.ok());
    // The parallel path must hand back the rejection itself, not a re-wrapped IOError: a caller
    // that retries on IO errors would otherwise keep retrying an invariant violation.
    ASSERT_TRUE(st.is_internal_error()) << st.to_string();
    const auto msg = st.to_string();
    ASSERT_NE(msg.find("refuse to write incomplete combined txn log"), std::string::npos) << msg;
    ASSERT_NE(msg.find("missing 1 of 3"), std::string::npos) << msg;
    ASSERT_NE(msg.find('3'), std::string::npos) << msg;

    // The single-partition entry point must reject it too.
    auto single = write_combined_txn_log(make_logs(100, {1, 2}), std::set<int64_t>{1, 2, 3});
    ASSERT_FALSE(single.ok());
    ASSERT_NE(single.to_string().find("refuse to write incomplete combined txn log"), std::string::npos)
            << single.to_string();
}

// A complete log, and the legacy no-expectation call, must both get past the coverage check.
// The failpoint stands in for the object-store write that follows it.
TEST_F(WriteCombinedTxnLogTest, test_complete_combined_txn_log_passes_coverage) {
    std::map<int64_t, CombinedTxnLogPB> txn_log_map;
    txn_log_map.emplace(100, make_logs(100, {1, 2, 3}));
    ExpectedTabletsByPartition expected;
    expected.emplace(100, std::set<int64_t>{1, 2, 3});

    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    auto fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get("put_combined_txn_log_success");
    fp->setMode(trigger_mode);

    ASSERT_TRUE(write_combined_txn_log_parallel(txn_log_map, expected).ok());
    // No expectation supplied -> unchanged behaviour for callers that have no independent source.
    ASSERT_TRUE(write_combined_txn_log_parallel(txn_log_map).ok());

    trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
    fp->setMode(trigger_mode);
}

namespace {

void set_fail_point(const char* name, bool enable) {
    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(enable ? FailPointTriggerModeType::ENABLE : FailPointTriggerModeType::DISABLE);
    starrocks::failpoint::FailPointRegistry::GetInstance()->get(name)->setMode(trigger_mode);
}

// Counts the write attempts and holds the ones a test picks until it releases them.
struct AttemptHook {
    std::mutex mutex;
    std::condition_variable cond;
    int started = 0;
    bool released = false;
    std::function<bool(int attempt)> hold = [](int) { return false; };
};

} // namespace

// Covers write_combined_txn_logs(), which bounds every partition's write by
// lake_put_combined_txn_log_attempt_timeout_ms and lake_put_combined_txn_log_max_attempts.
class BoundedCombinedTxnLogWriteTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_timeout_ms = config::lake_put_combined_txn_log_attempt_timeout_ms;
        _saved_max_attempts = config::lake_put_combined_txn_log_max_attempts;
        _saved_parallel = config::enable_put_combinded_txn_log_parallel;
        SyncPoint::GetInstance()->EnableProcessing();
        install_hook([](int) { return false; });
    }

    void TearDown() override {
        release_held_attempts();
        SyncPoint::GetInstance()->ClearCallBack("write_combined_txn_logs:attempt");
        SyncPoint::GetInstance()->DisableProcessing();
        set_fail_point("put_combined_txn_log_success", false);
        set_fail_point("put_combined_txn_log_fail", false);
        config::lake_put_combined_txn_log_attempt_timeout_ms = _saved_timeout_ms;
        config::lake_put_combined_txn_log_max_attempts = _saved_max_attempts;
        config::enable_put_combinded_txn_log_parallel = _saved_parallel;
    }

    // Counts attempts from scratch, holding those for which |hold| returns true.
    void install_hook(std::function<bool(int attempt)> hold) {
        _hook = std::make_shared<AttemptHook>();
        _hook->hold = std::move(hold);
        SyncPoint::GetInstance()->SetCallBack("write_combined_txn_logs:attempt", [hook = _hook](void*) {
            std::unique_lock l(hook->mutex);
            const int attempt = ++hook->started;
            if (hook->hold(attempt)) {
                hook->cond.wait(l, [&] { return hook->released; });
            }
        });
    }

    // An attempt the writer gave up on may still be held or queued. Let it go and wait for the pool to drain, so that
    // no attempt reaches put_combined_txn_log() after a test switched the fail points off.
    void release_held_attempts() {
        {
            std::lock_guard l(_hook->mutex);
            _hook->released = true;
            _hook->cond.notify_all();
        }
        RuntimeEnv::GetInstance()->put_combined_txn_log_thread_pool()->wait();
    }

    int started_attempts() {
        std::lock_guard l(_hook->mutex);
        return _hook->started;
    }

    std::shared_ptr<AttemptHook> _hook;
    int64_t _saved_timeout_ms = 0;
    int32_t _saved_max_attempts = 0;
    bool _saved_parallel = false;
};

TEST_F(BoundedCombinedTxnLogWriteTest, retries_a_failed_write) {
    config::lake_put_combined_txn_log_attempt_timeout_ms = 10000;
    config::lake_put_combined_txn_log_max_attempts = 3;
    set_fail_point("put_combined_txn_log_fail", true);

    std::map<int64_t, CombinedTxnLogPB> txn_log_map;
    txn_log_map.emplace(100, make_logs(100, {1, 2}));
    auto st = write_combined_txn_logs(txn_log_map, {});
    ASSERT_TRUE(st.is_internal_error()) << st;
    ASSERT_NE(st.to_string().find("write combined_txn_log_fail"), std::string::npos) << st;
    ASSERT_EQ(3, started_attempts());
}

// A PUT that hangs for seconds must not hold the load for seconds: once an attempt overruns its timeout another one
// is started next to it, and the first to succeed wins.
TEST_F(BoundedCombinedTxnLogWriteTest, starts_another_attempt_when_one_hangs) {
    config::lake_put_combined_txn_log_attempt_timeout_ms = 100;
    config::lake_put_combined_txn_log_max_attempts = 3;
    set_fail_point("put_combined_txn_log_success", true);
    for (bool parallel : {false, true}) {
        config::enable_put_combinded_txn_log_parallel = parallel;
        // Only the very first attempt hangs; with two partitions the other one is written at once.
        install_hook([](int attempt) { return attempt == 1; });

        std::map<int64_t, CombinedTxnLogPB> txn_log_map;
        txn_log_map.emplace(100, make_logs(100, {1, 2}));
        if (parallel) {
            txn_log_map.emplace(200, make_logs(200, {3, 4}));
        }
        const auto begin = std::chrono::steady_clock::now();
        ASSERT_OK(write_combined_txn_logs(txn_log_map, {}));
        const auto elapsed = std::chrono::steady_clock::now() - begin;
        ASSERT_LT(elapsed, std::chrono::seconds(5)) << "parallel=" << parallel;
        // The first attempt is still held, so the write can only have succeeded through a later one.
        ASSERT_GE(started_attempts(), static_cast<int>(txn_log_map.size()) + 1) << "parallel=" << parallel;
        release_held_attempts();
    }
}

TEST_F(BoundedCombinedTxnLogWriteTest, gives_up_when_no_attempt_finishes) {
    config::lake_put_combined_txn_log_attempt_timeout_ms = 50;
    config::lake_put_combined_txn_log_max_attempts = 2;
    install_hook([](int) { return true; });
    set_fail_point("put_combined_txn_log_success", true);

    std::map<int64_t, CombinedTxnLogPB> txn_log_map;
    txn_log_map.emplace(100, make_logs(100, {1, 2}));
    const auto begin = std::chrono::steady_clock::now();
    auto st = write_combined_txn_logs(txn_log_map, {});
    const auto elapsed = std::chrono::steady_clock::now() - begin;
    ASSERT_TRUE(st.is_time_out()) << st;
    ASSERT_NE(st.to_string().find("partition 100 txn 1000 not written after 2 attempts of 50 ms"), std::string::npos)
            << st;
    ASSERT_GE(elapsed, std::chrono::milliseconds(100));
    ASSERT_EQ(2, started_attempts());
}

// An incomplete log is an invariant violation, not a storage error: it must be refused before any attempt, and the
// rejection must come back as it is.
TEST_F(BoundedCombinedTxnLogWriteTest, does_not_retry_an_incomplete_log) {
    config::lake_put_combined_txn_log_attempt_timeout_ms = 100;
    config::lake_put_combined_txn_log_max_attempts = 3;

    std::map<int64_t, CombinedTxnLogPB> txn_log_map;
    txn_log_map.emplace(100, make_logs(100, {1, 2}));
    ExpectedTabletsByPartition expected;
    expected.emplace(100, std::set<int64_t>{1, 2, 3});
    auto st = write_combined_txn_logs(txn_log_map, expected);
    ASSERT_TRUE(st.is_internal_error()) << st;
    ASSERT_NE(st.to_string().find("refuse to write incomplete combined txn log"), std::string::npos) << st;
    ASSERT_EQ(0, started_attempts());
}

// One attempt without a time limit is the former behaviour, written by the former code path.
TEST_F(BoundedCombinedTxnLogWriteTest, single_unbounded_attempt_keeps_the_former_path) {
    config::lake_put_combined_txn_log_attempt_timeout_ms = -1;
    config::lake_put_combined_txn_log_max_attempts = 1;
    set_fail_point("put_combined_txn_log_success", true);

    std::map<int64_t, CombinedTxnLogPB> txn_log_map;
    txn_log_map.emplace(100, make_logs(100, {1, 2}));
    for (bool parallel : {false, true}) {
        config::enable_put_combinded_txn_log_parallel = parallel;
        ASSERT_OK(write_combined_txn_logs(txn_log_map, {}));
    }
    ASSERT_EQ(0, started_attempts());
}

} // namespace starrocks::lake
