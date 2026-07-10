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

namespace arrow {

class RecordBatch;
class Schema;
} // namespace arrow

namespace starrocks {

class MetricRegistry;

// RemoteArrowQueue caches the arrow_flight remote-scan results (Arrow RecordBatch) and the query
// status for a single fragment instance. It is the arrow-flight counterpart of RemoteChunkQueue.
// blocking_get waits for data when the queue is empty. Writes use a soft high watermark for
// pipeline backpressure (is_full), but are never rejected just because the watermark is reached.
class RemoteArrowQueue {
public:
    RemoteArrowQueue(uint32_t max_elements, int64_t max_bytes = std::numeric_limits<int64_t>::max())
            : _high_watermark(max_elements), _bytes_high_watermark(max_bytes) {}

    Status status() {
        std::lock_guard<SpinLock> l(_status_lock);
        return _status;
    }
    void update_status(const Status& status);

    // Defined out-of-line: computing an Arrow RecordBatch's byte size needs the full arrow headers,
    // which we keep out of this header. Both maintain _num_buffered_bytes (see is_full()).
    bool blocking_get(std::shared_ptr<arrow::RecordBatch>* result);
    bool put(const std::shared_ptr<arrow::RecordBatch>& val);
    // Soft backpressure watermark: full once the queued item count OR the buffered RecordBatch bytes
    // reach their limits. Byte bounding guards against OOM when batches are large (an item cap alone
    // is not enough). The nullptr EOS marker contributes zero bytes.
    bool is_full() const {
        return _queue.get_size() >= _high_watermark ||
               _num_buffered_bytes.load(std::memory_order_relaxed) >= _bytes_high_watermark;
    }
    void shutdown() { _queue.shutdown(); }

private:
    UnboundedBlockingQueue<std::shared_ptr<arrow::RecordBatch>> _queue;
    const uint32_t _high_watermark;
    const int64_t _bytes_high_watermark;
    std::atomic<int64_t> _num_buffered_bytes{0};
    SpinLock _status_lock;
    Status _status;
};

using RemoteArrowQueueSharedPtr = std::shared_ptr<RemoteArrowQueue>;

class RemoteArrowQueueMgr {
public:
    explicit RemoteArrowQueueMgr(MetricRegistry* metrics = nullptr);
    ~RemoteArrowQueueMgr() = default;

    Status fetch_result(const TUniqueId& fragment_instance_id, std::shared_ptr<arrow::RecordBatch>* result, bool* eos);
    void create_queue(const TUniqueId& fragment_instance_id, RemoteArrowQueueSharedPtr* queue);

    void set_arrow_schema(const TUniqueId& fragment_instance_id, const std::shared_ptr<arrow::Schema>& arrow_schema);
    std::shared_ptr<arrow::Schema> get_arrow_schema(const TUniqueId& fragment_instance_id);

    Status cancel(const TUniqueId& fragment_instance_id);
    void update_queue_status(const TUniqueId& fragment_instance_id, const Status& status);

private:
    std::mutex _lock;
    std::unordered_map<TUniqueId, RemoteArrowQueueSharedPtr> _fragment_queue_map;
    std::unordered_map<TUniqueId, std::shared_ptr<arrow::Schema>> _arrow_schema_map;
};

} // namespace starrocks
