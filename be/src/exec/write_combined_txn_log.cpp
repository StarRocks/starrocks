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

#include "exec/write_combined_txn_log.h"

#include <vector>

#include "runtime/exec_env.h"
#include "storage/lake/tablet_manager.h"
#include "util/countdown_latch.h"

namespace starrocks {

Status write_combined_txn_log(const CombinedTxnLogPB& logs, const std::set<int64_t>& expected_tablet_ids) {
    auto tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
    return tablet_mgr->put_combined_txn_log(logs, expected_tablet_ids);
}

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
            auto task_logic = create_txn_log_task(&logs, expected, ExecEnv::GetInstance()->lake_tablet_manager(),
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

            Status submit_status = ExecEnv::GetInstance()->put_combined_txn_log_thread_pool()->submit(task);
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

} // namespace starrocks