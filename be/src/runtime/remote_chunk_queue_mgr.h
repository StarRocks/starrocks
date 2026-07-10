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
#include <limits>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "base/concurrency/blocking_queue.hpp"
#include "base/concurrency/spinlock.h"
#include "base/hash/hash_std.hpp"
#include "base/uid_util.h"
#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/data.pb.h"

namespace starrocks {

class MetricRegistry;

class RemoteChunkQueue {
public:
    RemoteChunkQueue(uint32_t max_elements, int64_t max_bytes = std::numeric_limits<int64_t>::max())
            : _high_watermark(max_elements), _bytes_high_watermark(max_bytes) {}

    Status status();
    void update_status(const Status& status);

    // Pops the next element and releases its buffered bytes (see is_full()). fetch() routes through
    // this so the byte counter is maintained on every data pop.
    bool blocking_get(std::shared_ptr<ChunkPB>* result) {
        bool ok = _queue.blocking_get(result);
        if (ok && *result != nullptr) {
            _num_buffered_bytes.fetch_sub((*result)->data().size(), std::memory_order_relaxed);
        }
        return ok;
    }
    bool put(const std::shared_ptr<ChunkPB>& val) {
        bool ok = _queue.put(val);
        if (ok && val != nullptr) {
            _num_buffered_bytes.fetch_add(val->data().size(), std::memory_order_relaxed);
        }
        return ok;
    }
    // Soft backpressure watermark: full once the queued item count OR the buffered serialized-chunk
    // bytes reach their limits. Byte bounding guards against OOM when chunks are large (an item cap
    // alone is not enough). The nullptr EOS marker contributes zero bytes.
    bool is_full() const {
        return _queue.get_size() >= _high_watermark ||
               _num_buffered_bytes.load(std::memory_order_relaxed) >= _bytes_high_watermark;
    }
    void shutdown() { _queue.shutdown(); }
    // True once the queue has been shut down (cancel / EOS teardown). Lets fetch_chunk tell a
    // shut-down queue apart from a fetch() that merely rejected a stale/out-of-order packet_seq.
    bool is_shutdown() const { return _queue.is_shutdown(); }

    // Fetch the chunk for `packet_seq`, idempotent under brpc auto-retry: an exact re-send of the
    // last served packet_seq replays the cached data chunk instead of destructively popping the
    // next one (which would silently drop rows); a strictly-older packet_seq is rejected. Serializes
    // concurrent fetches for this
    // fragment via _fetch_lock so a retried request that arrives while the first is still blocked
    // cannot double-pop. Returns blocking_get's success; *eos / *result are set accordingly.
    bool fetch(int64_t packet_seq, std::shared_ptr<ChunkPB>* result, bool* eos);

private:
    UnboundedBlockingQueue<std::shared_ptr<ChunkPB>> _queue;
    const uint32_t _high_watermark;
    const int64_t _bytes_high_watermark;
    std::atomic<int64_t> _num_buffered_bytes{0};
    SpinLock _status_lock;
    Status _status;

    // Replay cache for idempotent fetch (see fetch()). _last_chunk holds the last served data
    // chunk; the EOS sentinel is not cached (an EOS retry hits the gone-queue / completed-token
    // path instead).
    std::mutex _fetch_lock;
    int64_t _last_packet_seq = -1;
    std::shared_ptr<ChunkPB> _last_chunk;
};

using RemoteChunkQueueSharedPtr = std::shared_ptr<RemoteChunkQueue>;

class RemoteChunkQueueMgr {
public:
    explicit RemoteChunkQueueMgr(MetricRegistry* metrics = nullptr);
    ~RemoteChunkQueueMgr() = default;

    void create_queue(const TUniqueId& fragment_instance_id, RemoteChunkQueueSharedPtr* queue);
    Status fetch_chunk(const TUniqueId& fragment_instance_id, int64_t packet_seq, ChunkPB* chunk, bool* eos);
    Status cancel(const TUniqueId& fragment_instance_id);
    void update_queue_status(const TUniqueId& fragment_instance_id, const Status& status);

private:
    std::mutex _lock;
    std::unordered_map<TUniqueId, RemoteChunkQueueSharedPtr> _fragment_queue_map;
};

} // namespace starrocks
