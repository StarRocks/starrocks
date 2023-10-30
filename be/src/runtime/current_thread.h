// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <string>

#include "fmt/format.h"
#include "gen_cpp/Types_types.h"
#include "gutil/macros.h"
#include "runtime/mem_tracker.h"
#include "util/defer_op.h"
#include "util/uid_util.h"

#define SCOPED_THREAD_LOCAL_MEM_SETTER(mem_tracker, check)                             \
    auto VARNAME_LINENUM(tracker_setter) = CurrentThreadMemTrackerSetter(mem_tracker); \
    auto VARNAME_LINENUM(check_setter) = CurrentThreadCheckMemLimitSetter(check);

#define SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(mem_tracker) \
    auto VARNAME_LINENUM(tracker_setter) = CurrentThreadMemTrackerSetter(mem_tracker)

#define SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(check) \
    auto VARNAME_LINENUM(check_setter) = CurrentThreadCheckMemLimitSetter(check)

#define CHECK_MEM_LIMIT(err_msg)                                                              \
    do {                                                                                      \
        if (tls_thread_status.check_mem_limit() && CurrentThread::mem_tracker() != nullptr) { \
            RETURN_IF_ERROR(CurrentThread::mem_tracker()->check_mem_limit(err_msg));          \
        }                                                                                     \
    } while (0)

namespace starrocks {

class TUniqueId;

inline thread_local MemTracker* tls_mem_tracker = nullptr;
inline thread_local MemTracker* tls_exceed_mem_tracker = nullptr;
inline thread_local bool tls_is_thread_status_init = false;

class CurrentThread {
public:
    CurrentThread() { tls_is_thread_status_init = true; }
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

    void set_fragment_instance_id(const starrocks::TUniqueId& fragment_instance_id) {
        _fragment_instance_id = fragment_instance_id;
    }
    const starrocks::TUniqueId& fragment_instance_id() { return _fragment_instance_id; }
    void set_pipeline_driver_id(int32_t driver_id) { _driver_id = driver_id; }
    int32_t get_driver_id() const { return _driver_id; }

    // Return prev memory tracker.
    starrocks::MemTracker* set_mem_tracker(starrocks::MemTracker* mem_tracker) {
        commit();
        auto* prev = tls_mem_tracker;
        tls_mem_tracker = mem_tracker;
        return prev;
    }

    bool set_check_mem_limit(bool check) {
        bool prev_check = _check;
        _check = check;
        return prev_check;
    }

    bool check_mem_limit() { return _check; }

    static starrocks::MemTracker* mem_tracker();

    static CurrentThread& current();

    static void set_exceed_mem_tracker(starrocks::MemTracker* mem_tracker) { tls_exceed_mem_tracker = mem_tracker; }

    bool set_is_catched(bool is_catched) {
        bool old = _is_catched;
        _is_catched = is_catched;
        return old;
    }

    bool is_catched() const { return _is_catched; }

    void mem_consume(int64_t size) {
        MemTracker* cur_tracker = mem_tracker();
        _cache_size += size;
        _total_consumed_bytes += size;
        if (cur_tracker != nullptr && _cache_size >= BATCH_SIZE) {
            cur_tracker->consume(_cache_size);
            _cache_size = 0;
        }
    }

    bool try_mem_consume(int64_t size) {
        MemTracker* cur_tracker = mem_tracker();
        _cache_size += size;
        _total_consumed_bytes += size;
        if (cur_tracker != nullptr && _cache_size >= BATCH_SIZE) {
            MemTracker* limit_tracker = cur_tracker->try_consume(_cache_size);
            if (LIKELY(limit_tracker == nullptr)) {
                _cache_size = 0;
                return true;
            } else {
                _cache_size -= size;
                _try_consume_mem_size = size;
                tls_exceed_mem_tracker = limit_tracker;
                return false;
            }
        }
        return true;
    }

    static void mem_consume_without_cache(int64_t size) {
        MemTracker* cur_tracker = mem_tracker();
        if (cur_tracker != nullptr && size != 0) {
            cur_tracker->consume(size);
        }
    }

    static bool try_mem_consume_without_cache(int64_t size) {
        MemTracker* cur_tracker = mem_tracker();
        if (cur_tracker != nullptr && size != 0) {
            MemTracker* limit_tracker = cur_tracker->try_consume(size);
            if (LIKELY(limit_tracker == nullptr)) {
                return true;
            } else {
                return false;
            }
        }
        return true;
    }

    void mem_release(int64_t size) {
        MemTracker* cur_tracker = mem_tracker();
        _cache_size -= size;
        if (cur_tracker != nullptr && _cache_size <= -BATCH_SIZE) {
            cur_tracker->release(-_cache_size);
            _cache_size = 0;
        }
    }

    static void mem_release_without_cache(int64_t size) {
        MemTracker* cur_tracker = mem_tracker();
        if (cur_tracker != nullptr && size != 0) {
            cur_tracker->release(size);
        }
    }

    // get last time try consume and reset
    int64_t try_consume_mem_size() {
        auto res = _try_consume_mem_size;
        _try_consume_mem_size = 0;
        return res;
    }

    int64_t get_consumed_bytes() const { return _total_consumed_bytes; }

private:
    const static int64_t BATCH_SIZE = 2 * 1024 * 1024;

    int64_t _cache_size = 0;           // Allocated but not committed memory bytes
    int64_t _total_consumed_bytes = 0; // Totally consumed memory bytes
    int64_t _try_consume_mem_size = 0; // Last time tried to consumed bytes
    // Store in TLS for diagnose coredump easier
    TUniqueId _query_id;
    TUniqueId _fragment_instance_id;
    int32_t _driver_id = 0;
    bool _is_catched = false;
    bool _check = true;
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

class CurrentThreadCheckMemLimitSetter {
public:
    explicit CurrentThreadCheckMemLimitSetter(bool check) {
        _prev_check = tls_thread_status.set_check_mem_limit(check);
    }

    ~CurrentThreadCheckMemLimitSetter() { (void)tls_thread_status.set_check_mem_limit(_prev_check); }

    CurrentThreadCheckMemLimitSetter(const CurrentThreadCheckMemLimitSetter&) = delete;
    void operator=(const CurrentThreadCheckMemLimitSetter&) = delete;
    CurrentThreadCheckMemLimitSetter(CurrentThreadCheckMemLimitSetter&&) = delete;
    void operator=(CurrentThreadCheckMemLimitSetter&&) = delete;

private:
    bool _prev_check;
};

class CurrentThreadCatchSetter {
public:
    explicit CurrentThreadCatchSetter(bool catched) { _prev_catched = tls_thread_status.set_is_catched(catched); }

    ~CurrentThreadCatchSetter() { (void)tls_thread_status.set_is_catched(_prev_catched); }

    CurrentThreadCatchSetter(const CurrentThreadCatchSetter&) = delete;
    void operator=(const CurrentThreadCatchSetter&) = delete;
    CurrentThreadCatchSetter(CurrentThreadCheckMemLimitSetter&&) = delete;
    void operator=(CurrentThreadCatchSetter&&) = delete;

private:
    bool _prev_catched;
};

#define SCOPED_SET_CATCHED(catched) auto VARNAME_LINENUM(catched_setter) = CurrentThreadCatchSetter(catched)

#define SCOPED_SET_TRACE_INFO(driver_id, query_id, fragment_instance_id)     \
    CurrentThread::current().set_pipeline_driver_id(driver_id);              \
    CurrentThread::current().set_query_id(query_id);                         \
    CurrentThread::current().set_fragment_instance_id(fragment_instance_id); \
    auto VARNAME_LINENUM(defer) = DeferOp([] {                               \
        CurrentThread::current().set_pipeline_driver_id(0);                  \
        CurrentThread::current().set_query_id({});                           \
        CurrentThread::current().set_fragment_instance_id({});               \
    });

#define TRY_CATCH_ALLOC_SCOPE_START() \
    try {                             \
        SCOPED_SET_CATCHED(true);

#define TRY_CATCH_ALLOC_SCOPE_END()                                                                                    \
    }                                                                                                                  \
    catch (std::bad_alloc const&) {                                                                                    \
        MemTracker* exceed_tracker = tls_exceed_mem_tracker;                                                           \
        tls_exceed_mem_tracker = nullptr;                                                                              \
        tls_thread_status.set_is_catched(false);                                                                       \
        if (LIKELY(exceed_tracker != nullptr)) {                                                                       \
            return Status::MemoryLimitExceeded(                                                                        \
                    exceed_tracker->err_msg(fmt::format("try consume:{}", tls_thread_status.try_consume_mem_size()))); \
        } else {                                                                                                       \
            return Status::MemoryLimitExceeded("Mem usage has exceed the limit of BE");                                \
        }                                                                                                              \
    }                                                                                                                  \
    catch (std::runtime_error const& e) {                                                                              \
        return Status::RuntimeError(fmt::format("Runtime error: {}", e.what()));                                       \
    }

#define TRY_CATCH_BAD_ALLOC(stmt)               \
    do {                                        \
        TRY_CATCH_ALLOC_SCOPE_START() { stmt; } \
        TRY_CATCH_ALLOC_SCOPE_END()             \
    } while (0)

// TRY_CATCH_ALL will not set catched=true, only used for catch unexpected crash,
// cannot be used to control memory usage.
#define TRY_CATCH_ALL(result, stmt)                                                      \
    do {                                                                                 \
        try {                                                                            \
            { result = stmt; }                                                           \
        } catch (std::runtime_error const& e) {                                          \
            result = Status::RuntimeError(fmt::format("Runtime error: {}", e.what()));   \
        } catch (std::exception const& e) {                                              \
            result = Status::InternalError(fmt::format("Internal error: {}", e.what())); \
        } catch (...) {                                                                  \
            result = Status::Unknown("Unknown error");                                   \
        }                                                                                \
    } while (0)
} // namespace starrocks
