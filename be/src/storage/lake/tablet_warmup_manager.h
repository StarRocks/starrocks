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

#ifdef USE_STAROS
#include <atomic>
#include <future>
#include <list>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <unordered_set>

#include "base/time/time.h"
#include "common/status.h"
#include "gutil/macros.h"
#include "storage/fixed_fifo_cache.h"
#include "storage/olap_common.h"
#include "worker.pb.h"

namespace staros::starlet {
struct ShardInfo;
}

namespace starrocks {
class ThreadPool;

namespace lake {
class TabletManager;

// Module to manage tablets warmup in shared-data mode.
//
// It is a component of TabletManager to handle all the warmup tasks
//
// The basic workflow for a warmup is as follows
// 1. (StartUp) register add_shard_listener to listen add_shard event from StarOSWorker
// 2. (Listener Handler) put the tablet id into a queue waiting for warmup
// 3. (EventLoop) batch process tablet ids in the queue, dedup tablet ids, exclude tablets with no caching.
// 4. (EventLoop) batch fetch visible versions from FE
// 5. (EventLoop) Generate warmup tasks and add into queue
// 6. (ThreadPool) Fetch tasks from queue and perform the warmup op
// 7. (ThreadPool) Notify StarOSWorker that the warmup task is done
class TabletWarmupManager {
protected:
    struct WarmupStats {
        int64_t pending_ts = 0;
        int64_t before_version_ts = 0;
        int64_t get_version_ts = 0;
        int64_t start_ts = 0;
        int64_t finish_ts = 0; // abort or done ts

        int64_t io_bytes_read_remote = 0;
        int64_t io_ms_read_remote = 0;
        int64_t io_ms_write_local = 0;

        std::string to_json_str();
    };

    struct WarmupContext {
        int64_t _tablet_id;
        std::promise<Status> _promise;
        std::shared_future<Status> _future;
        int64_t _version;
        WarmupStats _stats;

        WarmupContext(int64_t id) : _tablet_id(id), _future(_promise.get_future()), _version(-1) {
            _stats.pending_ts = UnixMillis();
        }
        ~WarmupContext();

        void record_version(int64_t version) {
            _version = version;
            _stats.get_version_ts = UnixMillis();
        }

        void record_io_stats(const OlapReaderStatistics& stat) {
            _stats.io_bytes_read_remote = stat.compressed_bytes_read_remote;
            _stats.io_ms_read_remote = stat.io_ns_remote / 1000000;
            _stats.io_ms_write_local = stat.io_ns_write_local_disk / 1000000;
        }

        void record_start() { _stats.start_ts = UnixMillis(); }

        void abort(Status status) {
            _promise.set_value(std::move(status));
            _stats.finish_ts = UnixMillis();
        }
        void done() {
            _promise.set_value(Status::OK());
            _stats.finish_ts = UnixMillis();
        }
    };

public:
    explicit TabletWarmupManager(TabletManager* tablet_mgr);

    ~TabletWarmupManager();

    DISALLOW_COPY_AND_MOVE(TabletWarmupManager);

    void init();

    void stop();

    void warmup_tablet(uint64_t tablet_id);

    std::shared_future<Status> warmup_tablet2(uint64_t tablet_id);

    Status update_max_threads(int max_threads);

    // For test only
    FixedFIFOCache<int64_t, int64_t>* TEST_partition_version_cache() { return &_partition_version_cache; }
    void TEST_set_schedule_sleep_ms(size_t interval) { _schedule_sleep_ms = interval; }

private:
    void loop_schedule();

    void batch_prepare_warmup();
    void get_tablet_visible_version(const std::shared_ptr<WarmupContext>& ctx);
    void add_tablet_id_pending_visible_version(const std::shared_ptr<WarmupContext>& ctx);
    void batch_get_partitions_meta_from_frontend(
            const std::unordered_map<int64_t, std::shared_ptr<WarmupContext>>& tablet_pending_version);
    void batch_report_tablet_replica_status(const std::vector<uint64_t>& tablet_ids);
    void do_warmup_tablet(const std::shared_ptr<WarmupContext>& ctx);
    void abort_warmup(int64_t tablet_id, Status status);
    void done_warmup(int64_t tablet_id, staros::WarmupLevel level, bool report);

    static int64_t get_partition_id_from_shard_info(staros::starlet::ShardInfo& info);

private:
    static constexpr size_t _FIXED_FIFO_CACHE_SIZE = 1024;
    static constexpr size_t _FXIED_FIFO_CACHE_EXPIRE_MS = 5000; // 5000ms

    size_t _schedule_sleep_ms = 200; // 200ms

    TabletManager* _tablet_mgr;
    std::unique_ptr<ThreadPool> _thread_pool;

    // protect accessing to _tablet_pending
    std::mutex _mutex_pending;
    std::list<std::shared_ptr<WarmupContext>> _tablet_pending;

    // protect accessng to _tablet_in_progress
    std::mutex _mutex_in_progress;
    std::unordered_map<int64_t, std::shared_ptr<WarmupContext>> _tablet_in_progress;

    std::thread _schedule_thread;

    std::atomic<bool> _stopped = true;
    std::atomic<bool> _fe_leader_exist = false;

    FixedFIFOCache<int64_t, int64_t> _partition_version_cache;

    // protect accessing to _tablet_pending_version
    std::mutex _mutex_pending_version;
    std::unordered_map<int64_t, std::shared_ptr<WarmupContext>> _tablet_pending_version;

    // protect accessing to _tablet_id_report
    std::mutex _mutex_batch_report;
    std::set<uint64_t> _tablet_id_report;
};
} // namespace lake
} // namespace starrocks
#endif
