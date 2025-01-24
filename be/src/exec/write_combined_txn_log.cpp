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

Status write_combined_txn_log(const CombinedTxnLogPB& logs) {
    auto tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
    return tablet_mgr->put_combined_txn_log(logs);
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

std::function<void()> create_txn_log_task(const CombinedTxnLogPB* logs, lake::TabletManager* tablet_mgr,
                                          std::atomic<bool>* has_error, Status* final_status) {
    return [logs, tablet_mgr, has_error, final_status]() {
        try {
            Status status = tablet_mgr->put_combined_txn_log(*logs);
            if (!status.ok()) {
                throw std::runtime_error("Txn log write failed");
            }
        } catch (const std::exception& e) {
            mark_failure(Status::IOError(e.what()), has_error, final_status);
        } catch (...) {
            mark_failure(Status::Unknown("Unknown exception in write combined txn log task"), has_error, final_status);
        }
    };
}

Status write_combined_txn_log_parallel(const std::map<int64_t, CombinedTxnLogPB>& txn_log_map) {
    CountDownLatch latch(txn_log_map.size());
    std::atomic<bool> has_error(false);
    Status final_status;
    {
        std::vector<std::shared_ptr<AutoCleanRunnable>> tasks;
        for (const auto& [partition_id, logs] : txn_log_map) {
            auto task_logic = create_txn_log_task(&logs, ExecEnv::GetInstance()->lake_tablet_manager(), &has_error,
                                                  &final_status);
            auto task = std::make_shared<AutoCleanRunnable>(std::move(task_logic), [&latch]() { latch.count_down(); });
            tasks.emplace_back(std::move(task));
        }
        for (const auto& task : tasks) {
            Status submit_status = ExecEnv::GetInstance()->put_combined_txn_log_thread_pool()->submit(task);
            if (!submit_status.ok()) {
                mark_failure(submit_status, &has_error, &final_status);
                break;
            }
        }
    }

    latch.wait();
    return has_error.load() ? final_status : Status::OK();
}

} // namespace starrocks