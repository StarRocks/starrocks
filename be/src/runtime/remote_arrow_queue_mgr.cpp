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

#include "runtime/remote_arrow_queue_mgr.h"

#include <arrow/record_batch.h>
#include <arrow/util/byte_size.h>

#include "common/config_exec_flow_fwd.h"
#include "common/status.h"
#include "gutil/compiler_util.h"
#include "runtime/runtime_metrics.h"

namespace starrocks {

void RemoteArrowQueue::update_status(const Status& status) {
    if (status.ok()) {
        return;
    }
    std::lock_guard<SpinLock> l(_status_lock);
    if (_status.ok()) {
        _status = status;
    }
}

bool RemoteArrowQueue::put(const std::shared_ptr<arrow::RecordBatch>& val) {
    bool ok = _queue.put(val);
    if (ok && val != nullptr) {
        _num_buffered_bytes.fetch_add(arrow::util::TotalBufferSize(*val), std::memory_order_relaxed);
    }
    return ok;
}

bool RemoteArrowQueue::blocking_get(std::shared_ptr<arrow::RecordBatch>* result) {
    bool ok = _queue.blocking_get(result);
    if (ok && *result != nullptr) {
        _num_buffered_bytes.fetch_sub(arrow::util::TotalBufferSize(**result), std::memory_order_relaxed);
    }
    return ok;
}

RemoteArrowQueueMgr::RemoteArrowQueueMgr(MetricRegistry* metrics) {
    // Each queue has a soft high watermark (default config::max_memory_sink_batch_count), so the
    // actual element count is not tracked here.
    if (metrics != nullptr) {
        REGISTER_GAUGE_RUNTIME_METRIC(metrics, remote_scan_arrow_queue_count, [this]() {
            std::lock_guard<std::mutex> l(_lock);
            return _fragment_queue_map.size();
        });
    }
}

Status RemoteArrowQueueMgr::fetch_result(const TUniqueId& fragment_instance_id,
                                         std::shared_ptr<arrow::RecordBatch>* result, bool* eos) {
    RemoteArrowQueueSharedPtr queue;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto iter = _fragment_queue_map.find(fragment_instance_id);
        if (_fragment_queue_map.end() != iter) {
            queue = iter->second;
        } else {
            return Status::InternalError("fragment_instance_id does not exist");
        }
    }
    // check queue status before get result
    RETURN_IF_ERROR(queue->status());
    bool success = queue->blocking_get(result);
    if (success) {
        // sentinel nullptr indicates scan end
        if (*result == nullptr) {
            *eos = true;
            // re-put sentinel for consistency, avoid repeated fetch when there is no more batch
            queue->put(nullptr);
        } else {
            *eos = false;
        }
    } else {
        *eos = true;
        // blocking_get() leaves *result untouched when the queue is shut down. Callers (the Arrow
        // Flight reader) reuse the same shared_ptr across ReadNext(), so clear any stale batch here;
        // otherwise the *result == nullptr check below would return OK and report a clean EOS over a
        // cancelled/truncated stream instead of surfacing the queue status.
        *result = nullptr;
    }
    if (UNLIKELY(*result == nullptr)) {
        // return the status of the queue, in case the status is updated during the blocking_get()
        return queue->status();
    } else {
        return Status::OK();
    }
}

void RemoteArrowQueueMgr::create_queue(const TUniqueId& fragment_instance_id, RemoteArrowQueueSharedPtr* queue) {
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _fragment_queue_map.find(fragment_instance_id);
    if (iter != _fragment_queue_map.end()) {
        *queue = iter->second;
    } else {
        auto tmp = std::make_shared<RemoteArrowQueue>(config::max_memory_sink_batch_count,
                                                      config::remote_scan_result_queue_mem_limit);
        _fragment_queue_map.emplace(fragment_instance_id, tmp);
        *queue = std::move(tmp);
    }
}

void RemoteArrowQueueMgr::set_arrow_schema(const TUniqueId& fragment_instance_id,
                                           const std::shared_ptr<arrow::Schema>& arrow_schema) {
    std::lock_guard<std::mutex> l(_lock);
    _arrow_schema_map[fragment_instance_id] = arrow_schema;
}

std::shared_ptr<arrow::Schema> RemoteArrowQueueMgr::get_arrow_schema(const TUniqueId& fragment_instance_id) {
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _arrow_schema_map.find(fragment_instance_id);
    if (iter == _arrow_schema_map.end()) {
        return nullptr;
    }
    return iter->second;
}

Status RemoteArrowQueueMgr::cancel(const TUniqueId& fragment_instance_id) {
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _fragment_queue_map.find(fragment_instance_id);
    if (iter != _fragment_queue_map.end()) {
        // Poison the status BEFORE shutting down so a consumer blocked in fetch_result observes a
        // Cancelled error instead of a clean EOS over a truncated/reclaimed stream (mirrors
        // RemoteChunkQueueMgr::cancel). Then shut the queue down to wake it and drop it.
        iter->second->update_status(Status::Cancelled("remote scan arrow queue has been cancelled"));
        iter->second->shutdown();
        _fragment_queue_map.erase(fragment_instance_id);
    }
    _arrow_schema_map.erase(fragment_instance_id);
    return Status::OK();
}

void RemoteArrowQueueMgr::update_queue_status(const TUniqueId& fragment_instance_id, const Status& status) {
    if (status.ok()) {
        return;
    }
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _fragment_queue_map.find(fragment_instance_id);
    if (iter != _fragment_queue_map.end()) {
        iter->second->update_status(status);
    }
}

} // namespace starrocks
