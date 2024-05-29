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

namespace starrocks::connector {

void SinkOperatorMemoryManager::init(std::unordered_map<std::string, WriterAndStream>* writer_stream_pairs,
                                     AsyncFlushStreamPoller* io_poller, CommitFunc commit_func) {
    _candidates = writer_stream_pairs;
    _commit_func = commit_func;
    _io_poller = io_poller;
}

bool SinkOperatorMemoryManager::kill_victim() {
    if (_candidates->empty()) {
        return false;
    }

    // find file writer with the largest file size
    std::string partition;
    WriterAndStream* victim = nullptr;
    for (auto& [key, writer_and_stream] : *_candidates) {
        if (victim == nullptr || victim->first->get_written_bytes() <= writer_and_stream.first->get_written_bytes()) {
            partition = key;
            victim = &writer_and_stream;
        }
    }
    DCHECK(victim != nullptr); // silence warning

    auto result = victim->first->commit();
    _commit_func(result);
    LOG(INFO) << "kill victim: " << victim->second->filename() << " size: " << result.file_statistics.file_size;
    _candidates->erase(partition);
    return true;
}

int64_t SinkOperatorMemoryManager::update_releasable_memory() {
    int64_t releasable_memory = _io_poller->releasable_memory();
    _releasable_memory.store(releasable_memory);
    return releasable_memory;
}

SinkMemoryManager::SinkMemoryManager(MemTracker* mem_tracker) : _mem_tracker(mem_tracker) {
    if (_mem_tracker != nullptr && _mem_tracker->has_limit()) {
        _high_watermark_percent = config::connector_sink_mem_high_watermark_percent;
        _low_watermark_percent = config::connector_sink_mem_low_watermark_percent;
        _min_watermark_percent = config::connector_sink_mem_min_watermark_percent;
        _high_watermark_bytes = _mem_tracker->limit() * _high_watermark_percent / 100;
        _low_watermark_bytes = _mem_tracker->limit() * _low_watermark_percent / 100;
        _min_watermark_bytes = _mem_tracker->limit() * _min_watermark_percent / 100;
    }
}

SinkOperatorMemoryManager* SinkMemoryManager::create_child_manager() {
    _children.push_back(std::make_unique<SinkOperatorMemoryManager>());
    auto* p = _children.back().get();
    DCHECK(p != nullptr);
    return p;
}

bool SinkMemoryManager::can_accept_more_input(SinkOperatorMemoryManager* child_manager) {
    child_manager->update_releasable_memory();
    if (_mem_tracker == nullptr || !_mem_tracker->has_limit()) {
        return true;
    }
    LOG_EVERY_SECOND(INFO) << "query pool consumption: " << _mem_tracker->consumption()
                           << " releasable_memory: " << _total_releasable_memory();

    auto available_memory = [this]() { return _mem_tracker->limit() - _mem_tracker->consumption(); };

    if (available_memory() <= _low_watermark_bytes) {
        // trigger early close
        while (child_manager->has_victim() && available_memory() + _total_releasable_memory() < _high_watermark_bytes) {
            bool found = child_manager->kill_victim();
            DCHECK(found);
            child_manager->update_releasable_memory();
        }
    }

    if (available_memory() <= _min_watermark_bytes) {
        // block
        if (_total_releasable_memory() > 0) {
            LOG_EVERY_SECOND(INFO) << "stop accept chunk";
            return false;
        }
    }

    return true;
}

int64_t SinkMemoryManager::_total_releasable_memory() {
    int64_t total = 0;
    std::for_each(_children.begin(), _children.end(), [&](auto& child) { total += child->releasable_memory(); });
    return total;
}
} // namespace starrocks::connector
