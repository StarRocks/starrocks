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

#include "storage/persistent_index_rebuild_executor.h"

#include "storage/storage_engine.h"
#include "storage/update_manager.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

class PersistentIndexRebuildTask final : public Runnable {
public:
    PersistentIndexRebuildTask(TabletSharedPtr tablet, std::function<void()> cb)
            : _tablet(std::move(tablet)),
              _cb(std::move(cb)) {}

    ~PersistentIndexRebuildTask() override {
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
            VLOG(2) << "load primary index success. tablet: " << tablet_id;
            index_cache.release(index_entry);
        } else {
            LOG(WARNING) << "load primary index error: " << st << ", tablet: " << tablet_id;
            index_cache.remove(index_entry);
        }
    }

private:
    TabletSharedPtr _tablet;
    std::function<void()> _cb;
};

Status PersistentIndexRebuildExecutor::init() {
    int max_threads = std::max<int>(1, config::pk_index_rebuild_thread_pool_num_max);
    RETURN_IF_ERROR(ThreadPoolBuilder("pindex_rebuild")
            .set_min_threads(0)
            .set_max_threads(max_threads)
            .build(&_rebuild_pool));
    REGISTER_THREAD_POOL_METRICS(pindex_rebuild, _rebuild_pool);
    return Status::OK();
}

void PersistentIndexRebuildExecutor::shutdown() {
    if (_rebuild_pool != nullptr) {
        _rebuild_pool->shutdown();
    }
}

Status PersistentIndexRebuildExecutor::refresh_max_thread_num() {
    if (_rebuild_pool != nullptr) {
        int max_threads = std::max<int>(1, config::pk_index_rebuild_thread_pool_num_max);
        return _rebuild_pool->update_max_threads(max_threads);
    } else {
        return Status::InternalError("Thread pool not exist");
    }
}

StatusOr<std::shared_ptr<CountDownLatch>> PersistentIndexRebuildExecutor::submit_task(TabletSharedPtr tablet) {
    auto tablet_id = tablet->tablet_id();
    if (tablet->updates() == nullptr) {
        return Status::InvalidArgument(fmt::format("tablet {} is not primary key", tablet_id));
    }

    std::lock_guard<std::mutex> lock(_lock);
    auto it = _running_tablets.find(tablet_id);
    if (it != _running_tablets.end()) {
        return std::move(it->second);
    }

    auto latch = std::make_shared<CountDownLatch>(1);
    auto task = std::make_shared<PersistentIndexRebuildTask>(tablet, [this, latch, tablet]() {
        latch->count_down();
        std::lock_guard<std::mutex> lock(_lock);
        _running_tablets.erase(tablet->tablet_id());
    });
    RETURN_IF_ERROR(_rebuild_pool->submit(std::move(task)));
    _running_tablets.emplace(tablet_id, latch);
    return std::move(latch);
}

} // namespace starrocks
