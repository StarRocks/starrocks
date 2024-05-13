#pragma once

#include "runtime/mem_tracker.h"
#include "formats/file_writer.h"
#include "connector/connector_chunk_sink.h"
#include "async_io_poller.h"

namespace starrocks::connector {

/// manage memory of a single sink operator
/// not thread-safe except `releasable_memory()`
class SinkOperatorMemoryManager {
public:
    SinkOperatorMemoryManager() = default;

    void init(std::unordered_map<std::string, WriterAndStream>* writer_stream_pairs, IOStatusPoller* io_poller, CommitFunc commit_func) {
        _candidates = writer_stream_pairs;
        _commit_func = commit_func;
        _async_io_poller = io_poller;
    }

    bool kill_victim() {
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

    int64_t update_releasable_memory() {
        int64_t releasable_memory = _async_io_poller->releasable_memory();
        _releasable_memory.store(releasable_memory);
        return releasable_memory;
    }

    // thread-safe
    int64_t releasable_memory() {
        return _releasable_memory.load();
    }

private:
    std::unordered_map<std::string, WriterAndStream>* _candidates = nullptr; // owned by sink operator
    CommitFunc _commit_func;
    IOStatusPoller* _async_io_poller = nullptr;
    std::atomic_int64_t _releasable_memory{0};
};


/// 1. manage all sink operators in a query
/// 2. calculates releasable memory across all
/// 3. reject inputs if memory occupancy exceeds soft bound (hard_limit x high_watermark\%)
/// 4. kill (early-close) writers to enlarge releasable memory
class SinkMemoryManager {
public:
    SinkMemoryManager(MemTracker* mem_tracker) : _mem_tracker(mem_tracker) {
        if (_mem_tracker != nullptr && _mem_tracker->has_limit()) {
            _mem_soft_bound = _mem_tracker->limit() * _high_watermark / 100;
        }
    }

    SinkOperatorMemoryManager* create_child_manager() {
        _children.push_back(std::make_unique<SinkOperatorMemoryManager>());
        auto* p = _children.back().get();
        DCHECK(p != nullptr);
        return p;
    }

    // thread-safe
    bool can_accept_more_input(SinkOperatorMemoryManager* child_manager) {
        // may lower frequency if overhead is significant
        child_manager->update_releasable_memory();

        if (_mem_soft_bound < 0 || _mem_tracker->consumption() <= _mem_soft_bound) {
            return true;
        }

        while (_mem_tracker->consumption() - _total_releasable_memory() > _mem_soft_bound) {
            int64_t before = child_manager->releasable_memory();
            // should we set a lower bound to avoid kill writer of small size?
            bool found = child_manager->kill_victim();
            if (!found) {
                break;
            }
            // child_manager->update_releasable_memory();
            int64_t after = child_manager->update_releasable_memory();
            LOG(INFO) << "before: " << before << " after: " << after;
        }

        LOG_EVERY_SECOND(INFO) << "query pool consumption: " << _mem_tracker->consumption() << " releasable_memory: " << _total_releasable_memory() << " bound: " << _mem_soft_bound;
        LOG_EVERY_SECOND(INFO) << "stop accept chunk";
        return false;
    }

private:
    int64_t _total_releasable_memory() {
        int64_t total = 0;
        std::for_each(_children.begin(), _children.end(), [&](auto& child) {
            total += child->releasable_memory();
        });
        return total;
    }

    MemTracker* _mem_tracker = nullptr;
    int64_t _high_watermark{80};
    int64_t _mem_soft_bound{-1};

    std::vector<std::unique_ptr<SinkOperatorMemoryManager>> _children; // size of dop
};

} // starrocks::connector

