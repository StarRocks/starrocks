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

#include "runtime/remote_chunk_queue_mgr.h"

#include "common/config_exec_flow_fwd.h"
#include "runtime/runtime_metrics.h"

namespace starrocks {

Status RemoteChunkQueue::status() {
    std::lock_guard<SpinLock> l(_status_lock);
    return _status;
}

void RemoteChunkQueue::update_status(const Status& status) {
    if (status.ok()) {
        return;
    }
    std::lock_guard<SpinLock> l(_status_lock);
    if (_status.ok()) {
        _status = status;
    }
}

bool RemoteChunkQueue::fetch(int64_t packet_seq, std::shared_ptr<ChunkPB>* result, bool* eos) {
    std::lock_guard<std::mutex> l(_fetch_lock);
    // Exact re-send of the last served sequence (a brpc auto-retry re-sends the same packet_seq):
    // replay the cached chunk instead of popping the next one.
    if (packet_seq == _last_packet_seq && _last_chunk != nullptr) {
        *result = _last_chunk;
        *eos = false;
        return true;
    }
    // A strictly-older packet_seq is a stale/out-of-order retry of a sequence the stream already
    // advanced past: that chunk is gone and _last_chunk now belongs to a NEWER sequence. Serving it
    // would corrupt the result, so report failure (the caller surfaces an error) rather than replay
    // the wrong chunk. Not reachable from today's synchronous single consumer; this guards the
    // replay contract regardless.
    if (packet_seq < _last_packet_seq) {
        *eos = false;
        return false;
    }
    bool success = blocking_get(result);
    if (success && *result != nullptr) {
        _last_packet_seq = packet_seq;
        _last_chunk = *result;
        *eos = false;
    } else {
        *eos = true;
    }
    return success;
}

RemoteChunkQueueMgr::RemoteChunkQueueMgr(MetricRegistry* metrics) {
    if (metrics != nullptr) {
        REGISTER_GAUGE_RUNTIME_METRIC(metrics, remote_scan_chunk_queue_count, [this]() {
            std::lock_guard<std::mutex> l(_lock);
            return _fragment_queue_map.size();
        });
    }
}

void RemoteChunkQueueMgr::create_queue(const TUniqueId& fragment_instance_id, RemoteChunkQueueSharedPtr* queue) {
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _fragment_queue_map.find(fragment_instance_id);
    if (iter != _fragment_queue_map.end()) {
        *queue = iter->second;
    } else {
        auto tmp = std::make_shared<RemoteChunkQueue>(config::max_memory_sink_batch_count,
                                                      config::remote_scan_result_queue_mem_limit);
        _fragment_queue_map.emplace(fragment_instance_id, tmp);
        *queue = std::move(tmp);
    }
}

Status RemoteChunkQueueMgr::fetch_chunk(const TUniqueId& fragment_instance_id, int64_t packet_seq, ChunkPB* chunk,
                                        bool* eos) {
    RemoteChunkQueueSharedPtr queue;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto iter = _fragment_queue_map.find(fragment_instance_id);
        if (iter == _fragment_queue_map.end()) {
            return Status::NotFound("remote scan fragment instance id does not exist");
        }
        queue = iter->second;
    }

    // queue->fetch() is idempotent under brpc retry: a duplicate packet_seq replays the cached
    // last data chunk instead of popping the next one.
    std::shared_ptr<ChunkPB> result;
    bool success = queue->fetch(packet_seq, &result, eos);
    if (success) {
        if (result == nullptr) {
            // EOS sentinel: drop the queue. A retried EOS fetch finds the queue gone (NotFound),
            // which the RPC handler maps to a clean EOS for a completed token.
            std::lock_guard<std::mutex> l(_lock);
            _fragment_queue_map.erase(fragment_instance_id);
        } else {
            // Copy (not Swap): the chunk stays owned by the queue's replay cache.
            chunk->CopyFrom(*result);
        }
    } else {
        if (!queue->is_shutdown()) {
            // fetch() rejected a stale/out-of-order packet_seq (older than the last served one).
            // The queue itself is still healthy, so reject only this request — do NOT erase the
            // queue and tear the stream down (that would fail the whole scan).
            return Status::InvalidArgument("stale or out-of-order remote scan packet_seq");
        }
        {
            std::lock_guard<std::mutex> l(_lock);
            _fragment_queue_map.erase(fragment_instance_id);
        }
        Status status = queue->status();
        if (status.ok()) {
            return Status::Cancelled("remote scan chunk queue has been shutdown");
        }
        return status;
    }

    if (result == nullptr) {
        return queue->status();
    }
    return Status::OK();
}

Status RemoteChunkQueueMgr::cancel(const TUniqueId& fragment_instance_id) {
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _fragment_queue_map.find(fragment_instance_id);
    if (iter != _fragment_queue_map.end()) {
        iter->second->update_status(Status::Cancelled("remote scan chunk queue has been cancelled"));
        iter->second->shutdown();
        _fragment_queue_map.erase(iter);
    }
    return Status::OK();
}

void RemoteChunkQueueMgr::update_queue_status(const TUniqueId& fragment_instance_id, const Status& status) {
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
