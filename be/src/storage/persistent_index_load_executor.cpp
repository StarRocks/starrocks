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

#include "storage/persistent_index_load_executor.h"

#include "storage/storage_engine.h"
#include "storage/update_manager.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

class PersistentIndexLoadTask final : public Runnable {
public:
    PersistentIndexLoadTask(TabletSharedPtr tablet, std::function<void()> cb)
            : _tablet(std::move(tablet)), _cb(std::move(cb)) {}

    ~PersistentIndexLoadTask() override {
        if (_cb) {
            _cb();
        }
    }

    void run() override {
        auto tablet_id = _tablet->tablet_id();
        auto manager = StorageEngine::instance()->update_manager();
        auto& index_cache = manager->index_cache();
        auto index_entry = index_cache.get_or_create(tablet_id);
        auto st = index_entry->value().load(_tablet.get());
        index_entry->update_expire_time(MonotonicMillis() + manager->get_index_cache_expire_ms(*_tablet));
        index_cache.update_object_size(index_entry, index_entry->value().memory_usage());
        if (st.ok()) {
            VLOG(2) << "Load primary index success. tablet: " << tablet_id;
            index_cache.release(index_entry);
        } else {
            LOG(WARNING) << "Load primary index error: " << st << ", tablet: " << tablet_id;
            index_cache.remove(index_entry);
        }
    }

private:
    TabletSharedPtr _tablet;
    std::function<void()> _cb;
};

Status PersistentIndexLoadExecutor::init() {
    int max_threads = std::max<int>(1, config::pindex_load_thread_pool_num_max);
    RETURN_IF_ERROR(
            ThreadPoolBuilder("pindex_load").set_min_threads(0).set_max_threads(max_threads).build(&_load_pool));
    REGISTER_THREAD_POOL_METRICS(pindex_load, _load_pool);
    return Status::OK();
}

void PersistentIndexLoadExecutor::shutdown() {
    if (_load_pool != nullptr) {
        _load_pool->shutdown();
    }
}

Status PersistentIndexLoadExecutor::refresh_max_thread_num() {
    if (_load_pool != nullptr) {
        int max_threads = std::max<int>(1, config::pindex_load_thread_pool_num_max);
        return _load_pool->update_max_threads(max_threads);
    } else {
        return Status::InternalError("Thread pool not exist");
    }
}

Status PersistentIndexLoadExecutor::submit_task_and_wait_for(const TabletSharedPtr& tablet, int32_t wait_seconds) {
    auto tablet_id = tablet->tablet_id();
    if (tablet->updates() == nullptr) {
        return Status::InvalidArgument(fmt::format("Tablet {} is not primary key", tablet_id));
    }

    auto latch_or = submit_task(tablet);
    if (latch_or.ok()) {
        auto finished = latch_or.value()->wait_for(std::chrono::seconds(wait_seconds));
        if (!finished) {
            auto msg = fmt::format("Persistent index is still loading, already wait {} seconds. tablet: {}",
                                   wait_seconds, tablet_id);
            LOG(INFO) << msg;
            return Status::TimedOut(msg);
        }
    } else {
        auto msg = fmt::format("Fail to submit persistent index load task. tablet: {}, error: {}", tablet_id,
                               latch_or.status().to_string(false));
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }

    return Status::OK();
}

StatusOr<std::shared_ptr<CountDownLatch>> PersistentIndexLoadExecutor::submit_task(const TabletSharedPtr& tablet) {
    if (_load_pool == nullptr) {
        return Status::Uninitialized("Persistent index load executor is not initialized");
    }

    auto tablet_id = tablet->tablet_id();

    std::lock_guard<std::mutex> lock(_lock);
    auto it = _running_tablets.find(tablet_id);
    if (it != _running_tablets.end()) {
        return it->second;
    }

    auto latch = std::make_shared<CountDownLatch>(1);
    auto task = std::make_shared<PersistentIndexLoadTask>(tablet, [this, latch, tablet]() {
        latch->count_down();
        std::lock_guard<std::mutex> lock(_lock);
        _running_tablets.erase(tablet->tablet_id());
    });
    RETURN_IF_ERROR(_load_pool->submit(std::move(task)));
    _running_tablets.emplace(tablet_id, latch);
    return std::move(latch);
}

} // namespace starrocks
