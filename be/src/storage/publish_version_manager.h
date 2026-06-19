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

#pragma once

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "common/status.h"
#include "common/thread/threadpool.h"
#include "gen_cpp/MasterService_types.h"

namespace starrocks {

using FinishTaskRequestPtr = std::shared_ptr<TFinishTaskRequest>;

class PublishVersionManager {
public:
    Status init();
    ~PublishVersionManager();
    void start();
    void stop();
    void wait_publish_task_apply_finish(std::vector<TFinishTaskRequest> finish_task_requests);

    size_t finish_task_requests_size();
    size_t waitting_finish_task_requests_size();

private:
    bool _all_task_applied(const TFinishTaskRequest& finish_task_request);
    bool _left_task_applied(const TFinishTaskRequest& finish_task_request);
    bool _has_pending_task_unlocked() const;
    void _finish_publish_version_task_unlocked();
    void _finish_publish_version_thread_callback();
    void _update_tablet_version(TFinishTaskRequest& finish_task_request);

private:
    mutable std::mutex _lock;
    std::condition_variable _finish_publish_version_cv;
    std::atomic<bool> _stopped{true};
    std::thread _finish_publish_version_thread;

    std::map<int64_t, TFinishTaskRequest> _finish_task_requests;
    std::map<int64_t, TFinishTaskRequest> _waitting_finish_task_requests;
    std::map<int64_t, std::set<std::pair<int64_t, int64_t>>> _unapplied_tablet_by_txn;
    std::unique_ptr<ThreadPool> _finish_publish_version_thread_pool;
};

} // namespace starrocks
