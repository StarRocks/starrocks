// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <string>

#include "gen_cpp/Types_types.h"
#include "gutil/macros.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "util/defer_op.h"
#include "util/uid_util.h"

#define SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(mem_tracker) \
    auto VARNAME_LINENUM(tracker_setter) = CurrentThreadMemTrackerSetter(mem_tracker)

namespace starrocks {

class TUniqueId;

class CurrentThread {
public:
    CurrentThread() = default;
    ~CurrentThread();

    void commit() {
        MemTracker* cur_tracker = mem_tracker();
        if (_cache_size != 0 && cur_tracker != nullptr) {
            cur_tracker->consume(_cache_size);
            _cache_size = 0;
        }
    }

    void set_query_id(const starrocks::TUniqueId& query_id) { _query_id = query_id; }

    const starrocks::TUniqueId& query_id() { return _query_id; }

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

    void set_exceed_mem_tracker(starrocks::MemTracker* mem_tracker) { _exceed_mem_tracker = mem_tracker; }

    starrocks::MemTracker* exceed_mem_tracker() { return _exceed_mem_tracker; }

    bool set_is_catched(bool is_catched) {
        bool old = _is_catched;
        _is_catched = is_catched;
        return old;
    }

    bool is_catched() { return _is_catched; }

    void mem_consume(int64_t size) {
        MemTracker* cur_tracker = mem_tracker();
        _cache_size += size;
        if (cur_tracker != nullptr && _cache_size >= BATCH_SIZE) {
            cur_tracker->consume(_cache_size);
            _cache_size = 0;
        }
    }

    bool try_mem_consume(int64_t size) {
        MemTracker* cur_tracker = mem_tracker();
        _cache_size += size;
        if (cur_tracker != nullptr && _cache_size >= BATCH_SIZE) {
            MemTracker* limit_tracker = cur_tracker->try_consume(_cache_size);
            if (LIKELY(limit_tracker == nullptr)) {
                _cache_size = 0;
                return true;
            } else {
                _cache_size -= size;
                _exceed_mem_tracker = limit_tracker;
                return false;
            }
        }
        return true;
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
    const static int64_t BATCH_SIZE = 2 * 1024 * 1024;

    int64_t _cache_size = 0;
    MemTracker* _mem_tracker = nullptr;
    MemTracker* _exceed_mem_tracker = nullptr;
    TUniqueId _query_id;
    bool _is_catched = false;
};

inline thread_local CurrentThread tls_thread_status;

class CurrentThreadMemTrackerSetter {
public:
    explicit CurrentThreadMemTrackerSetter(MemTracker* new_mem_tracker) {
        _old_mem_tracker = tls_thread_status.set_mem_tracker(new_mem_tracker);
    }

    ~CurrentThreadMemTrackerSetter() { (void)tls_thread_status.set_mem_tracker(_old_mem_tracker); }

    CurrentThreadMemTrackerSetter(const CurrentThreadMemTrackerSetter&) = delete;
    void operator=(const CurrentThreadMemTrackerSetter&) = delete;
    CurrentThreadMemTrackerSetter(CurrentThreadMemTrackerSetter&&) = delete;
    void operator=(CurrentThreadMemTrackerSetter&&) = delete;

private:
    MemTracker* _old_mem_tracker;
};

#define TRY_CATCH_BAD_ALLOC(stmt)                                                \
    do {                                                                         \
        try {                                                                    \
            bool prev = tls_thread_status.set_is_catched(true);                  \
            DeferOp op([&] { tls_thread_status.set_is_catched(prev); });         \
            { stmt; }                                                            \
        } catch (std::bad_alloc const&) {                                        \
            MemTracker* exceed_tracker = tls_thread_status.exceed_mem_tracker(); \
            tls_thread_status.set_exceed_mem_tracker(nullptr);                   \
            return Status::MemoryLimitExceeded(exceed_tracker->err_msg(""));     \
        }                                                                        \
    } while (0)

} // namespace starrocks
