// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <string>

#include "gen_cpp/Types_types.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "util/uid_util.h"

namespace starrocks {

class TUniqueId;

class CurrentThread {
public:
    CurrentThread() = default;
    ~CurrentThread() { commit(); }

    void commit() {
        MemTracker* cur_tracker = mem_tracker();
        if (_cache_size != 0 && cur_tracker != nullptr) {
            cur_tracker->consume(_cache_size);
            _cache_size = 0;
        }
    }

    void set_query_id(const starrocks::TUniqueId& query_id) {
        _query_id = query_id;
        _str_query_id = starrocks::print_id(query_id);
    }

    const starrocks::TUniqueId& query_id() { return _query_id; }
    const std::string& query_id_string() { return _str_query_id; }

    // Return prev memory tracker.
    starrocks::MemTracker* set_mem_tracker(starrocks::MemTracker* mem_tracker) {
        commit();
        auto* prev = _mem_tracker;
        _mem_tracker = mem_tracker;
        return prev;
    }

    starrocks::MemTracker* mem_tracker() {
        if (UNLIKELY(_mem_tracker == nullptr)) {
            _mem_tracker = ExecEnv::GetInstance()->process_mem_tracker();
        }
        return _mem_tracker;
    }

    void mem_consume(int64_t size) {
        MemTracker* cur_tracker = mem_tracker();
        _cache_size += size;
        if (cur_tracker != nullptr && _cache_size >= BATCH_SIZE) {
            cur_tracker->consume(_cache_size);
            _cache_size = 0;
        }
    }

    void mem_consume_without_cache(int64_t size) {
        MemTracker* cur_tracker = mem_tracker();
        if (cur_tracker != nullptr && size != 0) {
            cur_tracker->consume(size);
        }
    }

    void mem_release(int64_t size) {
        MemTracker* cur_tracker = mem_tracker();
        _cache_size -= size;
        if (cur_tracker != nullptr && _cache_size <= -BATCH_SIZE) {
            cur_tracker->release(-_cache_size);
            _cache_size = 0;
        }
    }

    void mem_release_without_cache(int64_t size) {
        MemTracker* cur_tracker = mem_tracker();
        if (cur_tracker != nullptr && size != 0) {
            cur_tracker->release(size);
        }
    }

private:
    int64_t _cache_size = 0;
    MemTracker* _mem_tracker = nullptr;
    TUniqueId _query_id;
    std::string _str_query_id;

    const static int64_t BATCH_SIZE = 2 * 1024 * 1024;
};

inline thread_local CurrentThread tls_thread_status;
} // namespace starrocks