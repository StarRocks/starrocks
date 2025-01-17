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

#include <future>
#include <vector>

#include "runtime/exec_env.h"
#include "storage/lake/tablet_manager.h"

namespace starrocks {

class TxnLogTask : public Runnable {
public:
    TxnLogTask(const CombinedTxnLogPB* logs, lake::TabletManager* tablet_mgr, std::promise<Status> promise)
            : _logs(logs), _tablet_mgr(tablet_mgr), _promise(std::move(promise)) {}

    void run() override {
        try {
            Status status = _tablet_mgr->put_combined_txn_log(*_logs);
            if (!status.ok()) {
                throw std::runtime_error("Log write failed");
            }
            _promise.set_value(Status::OK());
        } catch (const std::exception& e) {
            _promise.set_value(Status::IOError(e.what()));
        }
    }

private:
    const CombinedTxnLogPB* _logs;
    lake::TabletManager* _tablet_mgr;
    std::promise<Status> _promise;
};

Status write_combined_txn_log(const CombinedTxnLogPB& logs) {
    auto tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
    return tablet_mgr->put_combined_txn_log(logs);
}

Status write_combined_txn_log(const std::map<int64_t, CombinedTxnLogPB>& txn_log_map) {
    std::vector<std::future<Status>> futures;
    std::vector<std::shared_ptr<Runnable>> tasks;

    for (const auto& [partition_id, logs] : txn_log_map) {
        (void)partition_id;
        std::promise<Status> promise;
        futures.push_back(promise.get_future());
        std::shared_ptr<Runnable> r(
                std::make_shared<TxnLogTask>(&logs, ExecEnv::GetInstance()->lake_tablet_manager(), std::move(promise)));
        tasks.emplace_back(std::move(r));
    }

    for (auto& task : tasks) {
        RETURN_IF_ERROR(ExecEnv::GetInstance()->put_combined_txn_log_thread_pool()->submit(task));
    }

    Status st;
    for (auto& future : futures) {
        Status status = future.get();
        if (st.ok() && !status.ok()) {
            st = status;
        }
    }

    return st;
}

} // namespace starrocks