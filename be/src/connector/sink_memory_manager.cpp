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

#include "connector/sink_memory_manager.h"

#include "runtime/exec_env.h"

namespace starrocks::connector {

void SinkOperatorMemoryManager::init(std::map<PartitionKey, PartitionChunkWriterPtr>* partition_chunk_writers,
                                     AsyncFlushStreamPoller* io_poller, CommitFunc commit_func) {
    _candidates = partition_chunk_writers;
    _commit_func = std::move(commit_func);
    _io_poller = io_poller;
}

bool SinkOperatorMemoryManager::kill_victim() {
    if (_candidates->empty()) {
        return false;
    }

    // Find a target file writer to flush.
    // For buffered partition writer, choose the the writer with the largest file size.
    // For spillable partition writer, choose the the writer with the largest memory size that can be spilled.
    PartitionChunkWriterPtr victim = nullptr;
    for (auto& [key, writer] : *_candidates) {
        int64_t flushable_bytes = writer->get_flushable_bytes();
        if (flushable_bytes == 0) {
            continue;
        }
        if (victim && flushable_bytes < victim->get_flushable_bytes()) {
            continue;
        }
        victim = writer;
    }
    if (victim == nullptr) {
        return false;
    }

    // The flush will decrease the writer flushable memory bytes, so it usually
    // will not be choosed in a short time.
    const auto filename = victim->out_stream()->filename();
    size_t flush_bytes = victim->get_flushable_bytes();
    const auto result = victim->flush();
    LOG(INFO) << "kill victim: " << filename << ", result: " << result << ", flushable_bytes: " << flush_bytes;
    return true;
}

int64_t SinkOperatorMemoryManager::update_releasable_memory() {
    int64_t releasable_memory = _io_poller->releasable_memory();
    _releasable_memory.store(releasable_memory);
    return releasable_memory;
}

int64_t SinkOperatorMemoryManager::update_writer_occupied_memory() {
    int64_t writer_occupied_memory = 0;
    for (auto& [_, writer] : *_candidates) {
        writer_occupied_memory += writer->get_flushable_bytes();
    }
    _writer_occupied_memory.store(writer_occupied_memory);
    return _writer_occupied_memory;
}

SinkMemoryManager::SinkMemoryManager(MemTracker* query_pool_tracker, MemTracker* query_tracker)
        : _query_pool_tracker(query_pool_tracker), _query_tracker(query_tracker) {
    _process_tracker = GlobalEnv::GetInstance()->process_mem_tracker();
    _high_watermark_ratio = config::connector_sink_mem_high_watermark_ratio;
    _low_watermark_ratio = config::connector_sink_mem_low_watermark_ratio;
    _urgent_space_ratio = config::connector_sink_mem_urgent_space_ratio;
}

SinkOperatorMemoryManager* SinkMemoryManager::create_child_manager() {
    _children.push_back(std::make_unique<SinkOperatorMemoryManager>());
    auto* p = _children.back().get();
    DCHECK(p != nullptr);
    return p;
}

bool SinkMemoryManager::can_accept_more_input(SinkOperatorMemoryManager* child_manager) {
    if (!_apply_on_mem_tracker(child_manager, _process_tracker)) {
        return false;
    }
    if (!_apply_on_mem_tracker(child_manager, _query_pool_tracker)) {
        return false;
    }
    if (!_apply_on_mem_tracker(child_manager, _query_tracker)) {
        return false;
    }
    return true;
}

int64_t SinkMemoryManager::_total_releasable_memory() {
    int64_t total = 0;
    std::for_each(_children.begin(), _children.end(), [&](auto& child) { total += child->releasable_memory(); });
    return total;
}

int64_t SinkMemoryManager::_total_writer_occupied_memory() {
    int64_t total = 0;
    std::for_each(_children.begin(), _children.end(), [&](auto& child) { total += child->writer_occupied_memory(); });
    return total;
}

bool SinkMemoryManager::_apply_on_mem_tracker(SinkOperatorMemoryManager* child_manager, MemTracker* mem_tracker) {
    if (mem_tracker == nullptr || !mem_tracker->has_limit()) {
        return true;
    }

    auto available_memory = [&]() { return mem_tracker->limit() - mem_tracker->consumption(); };
    auto low_watermark = static_cast<int64_t>(mem_tracker->limit() * _low_watermark_ratio);
    int64_t flush_watermark = mem_tracker->limit() * _urgent_space_ratio;
    while (available_memory() <= low_watermark) {
        child_manager->update_writer_occupied_memory();
        int64_t total_occupied_memory = _total_writer_occupied_memory();
        LOG_EVERY_SECOND(INFO) << "consumption: " << mem_tracker->consumption()
                               << ", total_occupied_memory: " << total_occupied_memory
                               << ", flush_watermark: " << flush_watermark;
        if (total_occupied_memory < flush_watermark) {
            break;
        }
        bool found = child_manager->kill_victim();
        if (!found) {
            break;
        }
    }

    child_manager->update_releasable_memory();
    if (available_memory() <= low_watermark && _total_releasable_memory() > 0) {
        LOG_EVERY_SECOND(WARNING) << "memory usage is still high after flush, : available_memory" << available_memory()
                                  << ", memory_low_watermark: " << low_watermark
                                  << ", total_releasable_memory: " << _total_releasable_memory();
        return false;
    }
    return true;
}

} // namespace starrocks::connector
