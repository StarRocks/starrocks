// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <bthread/bthread.h>
#include <gperftools/tcmalloc.h>

#include <exception>
#include <string>

#include "common/compiler_util.h"
#include "common/logging.h"
#include "gen_cpp/Types_types.h"
#include "gutil/macros.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "util/uid_util.h"
#include "service/mem_hook.h"

#define SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(mem_tracker) \
    auto VARNAME_LINENUM(tracker_setter) = CurrentThreadMemTrackerSetter(mem_tracker)

namespace starrocks {

class TUniqueId;

namespace internal {

class ThreadLocalData {
public:
    ThreadLocalData() = default;
    ~ThreadLocalData();

    DISALLOW_COPY_AND_ASSIGN(ThreadLocalData);

    ALWAYS_INLINE void commit() {
        MemTracker* cur_tracker = mem_tracker();
        if (_cache_size != 0 && cur_tracker != nullptr) {
            cur_tracker->consume(_cache_size);
            _cache_size = 0;
        }
    }

    ALWAYS_INLINE void set_query_id(const starrocks::TUniqueId& query_id) {
        _query_id_hi = query_id.hi;
        _query_id_lo = query_id.lo;
    }

    ALWAYS_INLINE UniqueId query_id() { return UniqueId(_query_id_hi, _query_id_lo); }

    // Return prev memory tracker.
    ALWAYS_INLINE starrocks::MemTracker* set_mem_tracker(starrocks::MemTracker* new_mem_tracker) {
        commit();
        auto* prev = mem_tracker();
        _mem_tracker = new_mem_tracker;
        return prev;
    }

    ALWAYS_INLINE starrocks::MemTracker* mem_tracker() {
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
    int64_t _cache_size = 0;
    MemTracker* _mem_tracker = nullptr;
    int64_t _query_id_hi = 0;
    int64_t _query_id_lo = 0;

    const static int64_t BATCH_SIZE = 2 * 1024 * 1024;
};

class ThreadLocalDataAccessor {
    static void destructor(void* p) {
        DISABLE_MEMTRACKER();
        delete static_cast<ThreadLocalData*>(p);
        ENABLE_MEMTRACKER();
    }

public:
    static ThreadLocalDataAccessor& Instance() {
        static ThreadLocalDataAccessor instance;
        return instance;
    }

    ~ThreadLocalDataAccessor() { (void)bthread_key_delete(_key); }

    ThreadLocalData* get() {
      return static_cast<ThreadLocalData*>(bthread_getspecific(_key));
    }

    ThreadLocalData* get_or_create() {
        auto* data = static_cast<ThreadLocalData*>(bthread_getspecific(_key));
        if (data == nullptr) {
            DISABLE_MEMTRACKER();
            data = new ThreadLocalData();
            CHECK_EQ(0, bthread_setspecific(_key, data));
            ENABLE_MEMTRACKER();
        }
        return data;
    }

    DISALLOW_COPY_AND_ASSIGN(ThreadLocalDataAccessor);

private:
    ThreadLocalDataAccessor() {
      if (int r = bthread_key_create(&_key, destructor); r != 0) {
        ::exit(-1);
      }
    }

    bthread_key_t _key;
};

__thread inline int64_t g_tls_cached_bytes = 0;

static void clear_cache() {
    ExecEnv::GetInstance()->process_mem_tracker()->consume(g_tls_cached_bytes);
    g_tls_cached_bytes = 0;
}

static void consume_cache(size_t size) {
    g_tls_cached_bytes += size;
    if (g_tls_cached_bytes >= 2 * 1024 * 1024) {
        clear_cache();
    }
}

static void release_cache(size_t size) {
    g_tls_cached_bytes -= size;
    if (g_tls_cached_bytes <= -2 * 1024 * 1024) {
        clear_cache();
    }
}

} // namespace internal

namespace CurrentThread {

inline void set_query_id(const TUniqueId& query_id) {
    internal::ThreadLocalData* data = internal::ThreadLocalDataAccessor::Instance().get_or_create();
    data->set_query_id(query_id);
}

inline UniqueId query_id() {
    internal::ThreadLocalData* data = internal::ThreadLocalDataAccessor::Instance().get();
    return data ? data->query_id() : UniqueId(0, 0);
}

// Return prev memory tracker.
inline MemTracker* set_mem_tracker(MemTracker* mem_tracker) {
    internal::clear_cache();
    internal::ThreadLocalData* data = internal::ThreadLocalDataAccessor::Instance().get_or_create();
    return data->set_mem_tracker(mem_tracker);
}

inline MemTracker* mem_tracker() {
    internal::ThreadLocalData* data = internal::ThreadLocalDataAccessor::Instance().get();
    return data ? data->mem_tracker() : ExecEnv::GetInstance()->process_mem_tracker();
}

inline void mem_consume(int64_t size) {
    internal::ThreadLocalData* data = internal::ThreadLocalDataAccessor::Instance().get();
    if (data) {
        data->mem_consume(size);
    } else {
        internal::consume_cache(size);
    }
}

inline void mem_release(int64_t size) {
    internal::ThreadLocalData* data = internal::ThreadLocalDataAccessor::Instance().get();
    if (data) {
        data->mem_release(size);
    } else {
      internal::release_cache(size);
    }
}

} // namespace CurrentThread

class CurrentThreadMemTrackerSetter {
public:
    explicit CurrentThreadMemTrackerSetter(MemTracker* new_mem_tracker)
        : _old_mem_tracker(CurrentThread::set_mem_tracker(new_mem_tracker)) {
    }

    ~CurrentThreadMemTrackerSetter() { (void)CurrentThread::set_mem_tracker(_old_mem_tracker); }

    CurrentThreadMemTrackerSetter(const CurrentThreadMemTrackerSetter&) = delete;
    void operator=(const CurrentThreadMemTrackerSetter&) = delete;
    CurrentThreadMemTrackerSetter(CurrentThreadMemTrackerSetter&&) = delete;
    void operator=(CurrentThreadMemTrackerSetter&&) = delete;

private:
    MemTracker* _old_mem_tracker;
};

} // namespace starrocks
