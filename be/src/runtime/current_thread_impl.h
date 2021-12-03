// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <bthread/bthread.h>
#include <butil/thread_local.h>

#include "common/compiler_util.h"
#include "gen_cpp/Types_types.h"
#include "gutil/macros.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "service/mem_hook.h"
#include "util/uid_util.h"

namespace starrocks {
class TUniqueId;
} // namespace starrocks

namespace starrocks::current_thread::impl {

constexpr static int64_t BATCH_SIZE = 2 * 1024 * 1024;

__thread inline int64_t g_tls_cached_bytes = 0;

inline void clear_tls_cache() {
    auto tracker = ExecEnv::GetInstance()->process_mem_tracker();
    if ((g_tls_cached_bytes > 0) & (tracker != nullptr)) {
        tracker->consume(g_tls_cached_bytes);
        g_tls_cached_bytes = 0;
    }
}

inline void consume_tls_cache(size_t size) {
    g_tls_cached_bytes += size;
    if (g_tls_cached_bytes >= BATCH_SIZE) {
        clear_tls_cache();
    }
}

inline void release_tls_cache(size_t size) {
    g_tls_cached_bytes -= size;
    if (g_tls_cached_bytes <= -BATCH_SIZE) {
        clear_tls_cache();
    }
}

class ThreadCacheGuard {
public:
    ThreadCacheGuard() { butil::thread_atexit(clear_tls_cache); }
};

inline ThreadCacheGuard guard;

template <typename T>
class ThreadKey {
    static void destructor(void* p) {
        disable_memory_hook();
        delete static_cast<T*>(p);
        enable_memory_hook();
    }

public:
    ThreadKey() {
        if (int r = bthread_key_create(&_key, destructor); r != 0) {
            std::cerr << "Fail to call bthread_key_create: " << r;
            ::exit(-1);
        }
    }

    // ~ThreadKey() { (void)bthread_key_delete(_key); }

    DISALLOW_COPY_AND_ASSIGN(ThreadKey);

    bthread_key_t* operator->() { return &_key; }

    bthread_key_t& operator*() { return _key; }

private:
    bthread_key_t _key;
};

class ThreadLocalData {
public:
    ~ThreadLocalData() { commit(); }

    static ThreadLocalData* get();
    static ThreadLocalData* get_or_create();

    DISALLOW_COPY_AND_ASSIGN(ThreadLocalData);

    ALWAYS_INLINE void commit() {
        MemTracker* cur_tracker = mem_tracker();
        if (_cache_size != 0 && cur_tracker != nullptr) {
            cur_tracker->consume(_cache_size);
            _cache_size = 0;
        }
    }

    ALWAYS_INLINE void set_query_id(const TUniqueId& query_id) noexcept {
        _query_id_hi = query_id.hi;
        _query_id_lo = query_id.lo;
    }

    ALWAYS_INLINE UniqueId query_id() noexcept { return UniqueId(_query_id_hi, _query_id_lo); }

    // Return prev memory tracker.
    ALWAYS_INLINE MemTracker* set_mem_tracker(MemTracker* new_mem_tracker) {
        commit();
        auto* prev = mem_tracker();
        _mem_tracker = new_mem_tracker;
        return prev;
    }

    ALWAYS_INLINE MemTracker* mem_tracker() {
        return _mem_tracker ? _mem_tracker : ExecEnv::GetInstance()->process_mem_tracker();
    }

    ALWAYS_INLINE void mem_consume(int64_t size) {
        MemTracker* cur_tracker;
        _cache_size += size;
        if (_cache_size >= BATCH_SIZE && (cur_tracker = mem_tracker()) != nullptr) {
            cur_tracker->consume(_cache_size);
            _cache_size = 0;
        }
    }

    ALWAYS_INLINE void mem_consume_without_cache(int64_t size) {
        MemTracker* cur_tracker = mem_tracker();
        if (cur_tracker != nullptr && size != 0) {
            cur_tracker->consume(size);
        }
    }

    ALWAYS_INLINE void mem_release(int64_t size) {
        MemTracker* cur_tracker;
        _cache_size -= size;
        if (_cache_size <= -BATCH_SIZE && (cur_tracker = mem_tracker()) != nullptr) {
            cur_tracker->release(-_cache_size);
            _cache_size = 0;
        }
    }

    ALWAYS_INLINE void mem_release_without_cache(int64_t size) {
        MemTracker* cur_tracker = mem_tracker();
        if (cur_tracker != nullptr && size != 0) {
            cur_tracker->release(size);
        }
    }

private:
    ThreadLocalData() = default;

    static ThreadKey<ThreadLocalData> _thread_key;

    int64_t _cache_size = 0;
    MemTracker* _mem_tracker = nullptr;
    int64_t _query_id_hi = 0;
    int64_t _query_id_lo = 0;
};

inline ThreadKey<ThreadLocalData> ThreadLocalData::_thread_key{};

inline ThreadLocalData* ThreadLocalData::get() {
    return static_cast<ThreadLocalData*>(bthread_getspecific(*_thread_key));
}

inline ThreadLocalData* ThreadLocalData::get_or_create() {
    auto* data = static_cast<ThreadLocalData*>(bthread_getspecific(*_thread_key));
    if (data == nullptr) {
        disable_memory_hook();
        data = new ThreadLocalData();
        int r = bthread_setspecific(*_thread_key, data);
        if (UNLIKELY(r != 0)) {
            delete data;
            std::cerr << "bthread_setspecific failed: " << r;
            ::exit(-1);
        }
        enable_memory_hook();
    }
    return data;
}

} // namespace starrocks::current_thread::impl
