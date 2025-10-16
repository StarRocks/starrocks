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

#ifdef USE_STAROS

#pragma once

#include <future>
#include <memory>

#include "common/status.h"

namespace starrocks {
class ThreadPool;
} // namespace starrocks

namespace starrocks::lake {

class TabletManager;

struct TabletCacheStatsContext {
    const int64_t tablet_id;
    const int64_t tablet_version;
    std::promise<Status> promise;
    std::future<Status> future;
    int64_t cached_bytes;
    int64_t total_bytes;

    TabletCacheStatsContext(int64_t id, int64_t version)
            : tablet_id(id), tablet_version(version), future(promise.get_future()), cached_bytes(0), total_bytes(0) {}

    void fail(Status status) { promise.set_value(std::move(status)); }

    void done(int64_t cached_bytes, int64_t total_bytes) {
        this->cached_bytes = cached_bytes;
        this->total_bytes = total_bytes;
        promise.set_value(Status::OK());
    }

    Status get() { // TODO: add timeout
        return future.get();
    }
};

class TabletCacheStatsManager {
public:
    explicit TabletCacheStatsManager(TabletManager* tablet_mgr);
    ~TabletCacheStatsManager();

    void init();

    void stop();

    ThreadPool* thread_pool() { return _thread_pool.get(); }

    std::shared_ptr<TabletCacheStatsContext> submit_get_tablet_cache_stats(int64_t tablet_id, int64_t tablet_version);
    std::shared_ptr<TabletCacheStatsContext> get_tablet_cache_stats(int64_t tablet_id, int64_t tablet_version);

    Status update_max_threads(int max_threads);

private:
    void _get_tablet_cache_stats(std::shared_ptr<TabletCacheStatsContext>& ctx);

    TabletManager* _tablet_mgr;
    std::unique_ptr<ThreadPool> _thread_pool;
    std::atomic<bool> _stopped = true;
};

} // namespace starrocks::lake

#endif
