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

#include <fmt/format.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

#include "base/concurrency/countdown_latch.h"
#include "base/testutil/sync_point.h"
#include "base/time/time.h"
#include "common/config_ingest_fwd.h"
#include "common/thread/threadpool.h"
#include "runtime/runtime_env.h"
#include "storage/lake/tablet_manager.h"
#include "storage/storage_env.h"

namespace starrocks {

Status write_combined_txn_log(const CombinedTxnLogPB& logs, const std::set<int64_t>& expected_tablet_ids) {
    auto tablet_mgr = StorageEnv::GetInstance()->lake_tablet_manager();
    return tablet_mgr->put_combined_txn_log(logs, expected_tablet_ids);
}

void borrow_shard_infos(const ::google::protobuf::RepeatedPtrField<std::string>& shard_infos) {
    auto tablet_mgr = StorageEnv::GetInstance()->lake_tablet_manager();
    if (tablet_mgr == nullptr) {
        return;
    }
    for (const auto& info : shard_infos) {
        tablet_mgr->borrow_shard_info(info);
    }
}

namespace {

void mark_failure(const Status& status, std::atomic<bool>* has_error, Status* final_status) {
    if (!has_error->load()) {
        if (!has_error->exchange(true)) {
            if (final_status->ok()) {
                *final_status = status;
            }
        }
    }
}

std::function<void()> create_txn_log_task(const CombinedTxnLogPB* logs, const std::set<int64_t>* expected_tablet_ids,
                                          lake::TabletManager* tablet_mgr, std::atomic<bool>* has_error,
                                          Status* final_status, CountDownLatch* latch) {
    return [logs, expected_tablet_ids, tablet_mgr, has_error, final_status, latch]() {
        try {
            static const std::set<int64_t> kNoExpectation;
            Status status = tablet_mgr->put_combined_txn_log(
                    *logs, expected_tablet_ids != nullptr ? *expected_tablet_ids : kNoExpectation);
            if (!status.ok()) {
                // Report the status as it came back. Throwing here instead would route it through
                // the handler below and re-wrap it as an IOError, so an incomplete-coverage
                // rejection -- an invariant violation, not a transient object-store failure --
                // would reach the caller indistinguishable from one, both in code and in message.
                mark_failure(status, has_error, final_status);
            }
        } catch (const std::exception& e) {
            mark_failure(Status::IOError(e.what()), has_error, final_status);
        } catch (...) {
            mark_failure(Status::Unknown("Unknown exception in write combined txn log task"), has_error, final_status);
        }
        latch->count_down();
    };
}

} // namespace

Status write_combined_txn_log_parallel(const std::map<int64_t, CombinedTxnLogPB>& txn_log_map,
                                       const ExpectedTabletsByPartition& expected_by_partition) {
    CountDownLatch latch(txn_log_map.size());
    std::atomic<bool> has_error(false);
    Status final_status;
    {
        std::vector<std::shared_ptr<CancellableRunnable>> tasks;
        for (const auto& [partition_id, logs] : txn_log_map) {
            auto expected_it = expected_by_partition.find(partition_id);
            const std::set<int64_t>* expected =
                    expected_it != expected_by_partition.end() ? &expected_it->second : nullptr;
            auto task_logic = create_txn_log_task(&logs, expected, StorageEnv::GetInstance()->lake_tablet_manager(),
                                                  &has_error, &final_status, &latch);
            auto task =
                    std::make_shared<CancellableRunnable>(std::move(task_logic), [&latch, &has_error, &final_status]() {
                        Status st = Status::Cancelled("Task cancelled before execution");
                        mark_failure(st, &has_error, &final_status);
                        latch.count_down();
                    });
            tasks.emplace_back(std::move(task));
        }
        bool submit_failed = false;
        for (const auto& task : tasks) {
            if (submit_failed) {
                latch.count_down(); // Skip further tasks if one has already failed
                continue;
            }

            Status submit_status = RuntimeEnv::GetInstance()->put_combined_txn_log_thread_pool()->submit(task);
            if (!submit_status.ok()) {
                submit_failed = true;
                mark_failure(submit_status, &has_error, &final_status);
                latch.count_down();
            }
        }
    }

    latch.wait();
    return has_error.load() ? final_status : Status::OK();
}

namespace {

// The attempts at one partition's combined txn log. The writer can stop waiting while an attempt keeps running, so
// the attempts own their input and report through this shared state, never through the writer's stack.
struct PartitionTxnLogWrite {
    int64_t partition_id = 0;
    std::shared_ptr<const CombinedTxnLogPB> logs;
    std::shared_ptr<const std::set<int64_t>> expected_tablet_ids;
    int started = 0;
    int running = 0;
    // When another attempt is started if none of the running ones has finished by then.
    int64_t next_attempt_ns = std::numeric_limits<int64_t>::max();
    bool succeeded = false;
    Status last_error;
};

struct TxnLogWriteState {
    std::mutex mutex;
    std::condition_variable cond;
    std::vector<PartitionTxnLogWrite> writes;
};

// Called with state->mutex held.
void start_attempt(const std::shared_ptr<TxnLogWriteState>& state, size_t index, int64_t now_ns, int64_t timeout_ns) {
    auto& write = state->writes[index];
    ++write.started;
    ++write.running;
    write.next_attempt_ns = timeout_ns > 0 ? now_ns + timeout_ns : std::numeric_limits<int64_t>::max();
    auto submit_st = RuntimeEnv::GetInstance()->put_combined_txn_log_thread_pool()->submit_func([state, index]() {
        auto& write = state->writes[index];
        Status st;
        try {
            TEST_SYNC_POINT_CALLBACK("write_combined_txn_logs:attempt", nullptr);
            st = StorageEnv::GetInstance()->lake_tablet_manager()->put_combined_txn_log(*write.logs,
                                                                                        *write.expected_tablet_ids);
        } catch (const std::exception& e) {
            st = Status::IOError(e.what());
        } catch (...) {
            st = Status::Unknown("Unknown exception in write combined txn log task");
        }
        std::lock_guard l(state->mutex);
        --write.running;
        if (st.ok()) {
            write.succeeded = true;
        } else {
            write.last_error = std::move(st);
        }
        state->cond.notify_all();
    });
    if (!submit_st.ok()) {
        --write.running;
        write.last_error = std::move(submit_st);
    }
}

// Writes every partition of |writes| at the same time; see write_combined_txn_logs().
Status write_with_bounded_attempts(std::vector<PartitionTxnLogWrite> writes) {
    const int max_attempts = std::max(1, config::lake_put_combined_txn_log_max_attempts);
    const int64_t timeout_ms = config::lake_put_combined_txn_log_attempt_timeout_ms;
    const int64_t timeout_ns = timeout_ms > 0 ? timeout_ms * 1000000 : 0;
    auto state = std::make_shared<TxnLogWriteState>();
    state->writes = std::move(writes);

    std::unique_lock lock(state->mutex);
    for (size_t i = 0; i < state->writes.size(); ++i) {
        start_attempt(state, i, MonotonicNanos(), timeout_ns);
    }
    while (true) {
        const int64_t now_ns = MonotonicNanos();
        bool all_succeeded = true;
        int64_t wake_ns = std::numeric_limits<int64_t>::max();
        for (size_t i = 0; i < state->writes.size(); ++i) {
            auto& write = state->writes[i];
            if (write.succeeded) {
                continue;
            }
            all_succeeded = false;
            const bool failed = write.running == 0;
            if (failed || now_ns >= write.next_attempt_ns) {
                const int64_t txn_id = write.logs->txn_logs(0).txn_id();
                if (write.started >= max_attempts) {
                    if (failed) {
                        return write.last_error;
                    }
                    return Status::TimedOut(fmt::format(
                            "combined txn log of partition {} txn {} not written after {} attempts of {} ms{}",
                            write.partition_id, txn_id, write.started, timeout_ms,
                            write.last_error.ok() ? "" : ", last error: " + write.last_error.to_string()));
                }
                LOG(WARNING) << "Retry writing the combined txn log, partition_id=" << write.partition_id
                             << ", txn_id=" << txn_id << ", attempt=" << write.started + 1 << "/" << max_attempts
                             << ", reason="
                             << (failed ? write.last_error.to_string()
                                        : fmt::format("no attempt finished within {} ms", timeout_ms));
                start_attempt(state, i, now_ns, timeout_ns);
            }
            wake_ns = std::min(wake_ns, write.next_attempt_ns);
        }
        if (all_succeeded) {
            return Status::OK();
        }
        if (wake_ns == std::numeric_limits<int64_t>::max()) {
            state->cond.wait(lock);
        } else {
            state->cond.wait_for(lock, std::chrono::nanoseconds(std::max<int64_t>(wake_ns - MonotonicNanos(), 0)));
        }
    }
}

} // namespace

Status write_combined_txn_logs(const std::map<int64_t, CombinedTxnLogPB>& txn_log_map,
                               const ExpectedTabletsByPartition& expected_by_partition) {
    static const std::set<int64_t> kNoExpectation;
    auto expected_of = [&](int64_t partition_id) -> const std::set<int64_t>& {
        auto it = expected_by_partition.find(partition_id);
        return it != expected_by_partition.end() ? it->second : kNoExpectation;
    };

    if (config::lake_put_combined_txn_log_max_attempts <= 1 &&
        config::lake_put_combined_txn_log_attempt_timeout_ms <= 0) {
        if (config::enable_put_combinded_txn_log_parallel) {
            return write_combined_txn_log_parallel(txn_log_map, expected_by_partition);
        }
        for (const auto& [partition_id, logs] : txn_log_map) {
            RETURN_IF_ERROR(write_combined_txn_log(logs, expected_of(partition_id)));
        }
        return Status::OK();
    }

    std::vector<PartitionTxnLogWrite> writes;
    writes.reserve(txn_log_map.size());
    for (const auto& [partition_id, logs] : txn_log_map) {
        const auto& expected = expected_of(partition_id);
        // An incomplete log is an invariant violation, not a storage error: refuse it before anything is written,
        // and never retry it.
        RETURN_IF_ERROR(lake::TabletManager::check_combined_txn_log_coverage(logs, expected));
        if (logs.txn_logs_size() == 0) {
            return Status::InvalidArgument("empty CombinedTxnLogPB");
        }
        auto& write = writes.emplace_back();
        write.partition_id = partition_id;
        write.logs = std::make_shared<const CombinedTxnLogPB>(logs);
        write.expected_tablet_ids = std::make_shared<const std::set<int64_t>>(expected);
    }
    if (config::enable_put_combinded_txn_log_parallel) {
        return write_with_bounded_attempts(std::move(writes));
    }
    for (auto& write : writes) {
        RETURN_IF_ERROR(write_with_bounded_attempts({std::move(write)}));
    }
    return Status::OK();
}

} // namespace starrocks
