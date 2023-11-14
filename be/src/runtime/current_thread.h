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

#pragma once

#include <cstdint>
#include <string>

#include "fmt/format.h"
#include "gen_cpp/Types_types.h"
#include "gutil/macros.h"
#include "runtime/mem_tracker.h"
#include "util/defer_op.h"
#include "util/uid_util.h"
#include "bthread/bthread.h"

#define SCOPED_THREAD_LOCAL_MEM_SETTER(mem_tracker, check)                             \
    /*LOG(INFO) << "set mem tracker";*/ \
    auto VARNAME_LINENUM(tracker_setter) = CurrentThreadMemTrackerSetter(mem_tracker); \
    auto VARNAME_LINENUM(check_setter) = CurrentThreadCheckMemLimitSetter(check);

#define SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(mem_tracker) \
    /*LOG(INFO) << "set mem tracker";*/ \
    auto VARNAME_LINENUM(tracker_setter) = CurrentThreadMemTrackerSetter(mem_tracker)

#define SCOPED_THREAD_LOCAL_OPERATOR_MEM_TRACKER_SETTER(operator) \
    /*LOG(INFO) << "set operator mem tracker";*/ \
    auto VARNAME_LINENUM(tracker_setter) = CurrentThreadOperatorMemTrackerSetter(operator->mem_tracker())

#define SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(check) \
    auto VARNAME_LINENUM(check_setter) = CurrentThreadCheckMemLimitSetter(check)

// @TODO change it
#define CHECK_MEM_LIMIT(err_msg)                                                              \
    do {                                                                                      \
        if (tls_thread_status.check_mem_limit() && CurrentThread::mem_tracker() != nullptr) { \
            RETURN_IF_ERROR(CurrentThread::mem_tracker()->check_mem_limit(err_msg));          \
        }                                                                                     \
    } while (0)

namespace starrocks {

class TUniqueId;

// inline thread_local MemTracker* tls_mem_tracker = nullptr;
// inline thread_local MemTracker* tls_operator_mem_tracker = nullptr;
// inline thread_local MemTracker* tls_exceed_mem_tracker = nullptr;
inline thread_local bool tls_is_thread_status_init = false;

// bthread and pthread should hold it by itself
// @TODO how about mem cache manager?
// @TODO maybe we don't need it
class RuntimeContext {
private:
    MemTracker* mem_tracker = nullptr;
    MemTracker* operator_mem_tracker = nullptr;
    MemTracker* tls_exceed_mem_tracker = nullptr;

    // trace info
    TUniqueId query_id;
    TUniqueId fragment_instance_id;
    int32_t pipeline_driver_id = 0;
    std::string custom_coredump_msg;
    bool is_catched = false;
    bool check = true;
};
inline thread_local RuntimeContext pthread_local_context;
// @TODO in bthread, set_mem_tracker should update BThreadContext
// @TODO in mem_hook, we check tls_mem_tracker, if bthread scheduled, it may changed
// @TODO how can we always use it
inline thread_local RuntimeContext* bthread_local_context;

// @TODO should take aware pthread and bthread
class CurrentThread {
private:
    class MemCacheManager {
    public:
        // @TODO can we bind CurrentThread directly
        // MemCacheManager(std::function<MemTracker*()>&& loader) : _loader(std::move(loader)) {}
        // MemCacheManager(std::function<CurrentThread&()>&& loader) : _loader(std::move(loader)) {}
        MemCacheManager(CurrentThread* current_thread, std::function<MemTracker*()>&& loader):
            _current_thread(current_thread), _loader(std::move(loader)) {}
        MemCacheManager(const MemCacheManager&) = delete;
        MemCacheManager(MemCacheManager&&) = delete;

        void consume(int64_t size) {
            size = _consume_from_reserved(size);
            _cache_size += size;
            _allocated_cache_size += size;
            _total_consumed_bytes += size;
            if (_cache_size >= BATCH_SIZE) {
                commit(false);
            }
        }

        bool try_mem_consume(int64_t size) {
            // MemTracker* cur_tracker = _loader();
            // auto& current_thread = _loader();
            // MemTracker* cur_tracker = current_thread._mem_tracker;
            MemTracker* cur_tracker = _loader();
            int64_t prev_reserved = _reserved_bytes;
            size = _consume_from_reserved(size);
            _cache_size += size;
            _allocated_cache_size += size;
            _total_consumed_bytes += size;
            if (cur_tracker != nullptr && _cache_size >= BATCH_SIZE) {
                MemTracker* limit_tracker = cur_tracker->try_consume(_cache_size);
                if (LIKELY(limit_tracker == nullptr)) {
                    _cache_size = 0;
                    return true;
                } else {
                    _reserved_bytes = prev_reserved;
                    _cache_size -= size;
                    _allocated_cache_size -= size;
                    _try_consume_mem_size = size;
                    // tls_exceed_mem_tracker = limit_tracker;
                    // current_thread._exceed_mem_tracker = limit_tracker;
                    _current_thread->_exceed_mem_tracker = limit_tracker;
                    return false;
                }
            }
            return true;
        }

        bool try_mem_consume_with_limited_tracker(int64_t size, MemTracker* tracker, int64_t limit) {
            // MemTracker* cur_tracker = _loader();
            // auto& current_thread = _loader();
            // MemTracker* cur_tracker = current_thread._mem_tracker;
            MemTracker* cur_tracker = _loader();
            _cache_size += size;
            _allocated_cache_size += size;
            _total_consumed_bytes += size;
            if (cur_tracker != nullptr && _cache_size >= BATCH_SIZE) {
                MemTracker* limit_tracker = cur_tracker->try_consume_with_limited(_cache_size, tracker, limit);
                if (LIKELY(limit_tracker == nullptr)) {
                    _cache_size = 0;
                    return true;
                } else {
                    _cache_size -= size;
                    _allocated_cache_size -= size;
                    _try_consume_mem_size = size;
                    // current_thread._exceed_mem_tracker = limit_tracker;
                    _current_thread->_exceed_mem_tracker = limit_tracker;
                    // tls_exceed_mem_tracker = limit_tracker;
                    return false;
                }
            }
            return true;
        }

        bool try_mem_reserve(int64_t reserve_bytes, MemTracker* tracker, int64_t limit) {
            DCHECK(_reserved_bytes == 0);
            DCHECK(reserve_bytes >= 0);
            if (try_mem_consume_with_limited_tracker(reserve_bytes, tracker, limit)) {
                _reserved_bytes = reserve_bytes;
                return true;
            }
            return false;
        }

        void release_reserved() {
            if (_reserved_bytes) {
                release(_reserved_bytes);
                _reserved_bytes = 0;
            }
        }

        void release(int64_t size) {
            _cache_size -= size;
            _deallocated_cache_size += size;
            if (_cache_size <= -BATCH_SIZE) {
                commit(false);
            }
        }

        void commit(bool is_ctx_shift) {
            // auto& current_thread = _loader();
            // MemTracker* cur_tracker = current_thread.get_mem_tracker();
            MemTracker* cur_tracker = _loader();
            if (cur_tracker != nullptr) {
                cur_tracker->consume(_cache_size);
            }
            _cache_size = 0;
            if (is_ctx_shift) {
                // Flush all cached info
                if (cur_tracker != nullptr) {
                    cur_tracker->update_allocation(_allocated_cache_size);
                    cur_tracker->update_deallocation(_deallocated_cache_size);
                }
                _allocated_cache_size = 0;
                _deallocated_cache_size = 0;
            }
        }

        int64_t try_consume_mem_size() {
            auto res = _try_consume_mem_size;
            _try_consume_mem_size = 0;
            return res;
        }

        int64_t get_consumed_bytes() const { return _total_consumed_bytes; }

    private:
        int64_t _consume_from_reserved(int64_t size) {
            if (_reserved_bytes > size) {
                _reserved_bytes -= size;
                size = 0;
            } else {
                size -= _reserved_bytes;
                _reserved_bytes = 0;
            }
            return size;
        }

        const static int64_t BATCH_SIZE = 2 * 1024 * 1024;

        // std::function<MemTracker*()> _loader;
        // std::function<CurrentThread&()> _loader;
        CurrentThread* _current_thread = nullptr;
        std::function<MemTracker*()> _loader;


        int64_t _reserved_bytes = 0;

        // Allocated or delocated but not committed memory bytes, can be negative
        int64_t _cache_size = 0;
        // Allocated but not committed memory bytes, always positive
        int64_t _allocated_cache_size = 0;
        // Deallocated but not committed memory bytes, always positive
        int64_t _deallocated_cache_size = 0;
        int64_t _total_consumed_bytes = 0; // Totally consumed memory bytes
        int64_t _try_consume_mem_size = 0; // Last time tried to consumed bytes
    };

public:
    CurrentThread();
    // CurrentThread() : _mem_cache_manager(CurrentThread::current), _operator_mem_cache_manager(CurrentThread::current) {
    //     if (LIKELY(GlobalEnv::is_init())) {
    //         _mem_tracker = GlobalEnv::GetInstance()->process_mem_tracker();
    //         _is_init = true;
    //     }
    //     // if(!bthread_self()) {
    //         // tls_is_thread_status_init = true;
    //     // }
    //     // if (bthread_self()) {
    //         // LOG(INFO) << "CurrentThread is create in bthread: " << bthread_self();
    //     // }
    // }
    ~CurrentThread();

    void mem_tracker_ctx_shift() { _mem_cache_manager.commit(true); }
    void operator_mem_tracker_ctx_shift() { _operator_mem_cache_manager.commit(true); }

    void set_query_id(const starrocks::TUniqueId& query_id) { _query_id = query_id; }
    const starrocks::TUniqueId& query_id() { return _query_id; }

    void set_fragment_instance_id(const starrocks::TUniqueId& fragment_instance_id) {
        _fragment_instance_id = fragment_instance_id;
    }
    const starrocks::TUniqueId& fragment_instance_id() { return _fragment_instance_id; }
    void set_pipeline_driver_id(int32_t driver_id) { _driver_id = driver_id; }
    // @TODO make get/set name consistent
    int32_t get_driver_id() const { return _driver_id; }

    void set_custom_coredump_msg(const std::string& custom_coredump_msg) { _custom_coredump_msg = custom_coredump_msg; }

    const std::string& get_custom_coredump_msg() const { return _custom_coredump_msg; }
    bool is_init() const {
        return _is_init;
    }
    void set_is_init(bool val) {
        _is_init = val;
    }
    void init();

    // Return prev memory tracker.
    starrocks::MemTracker* set_mem_tracker(starrocks::MemTracker* mem_tracker) {
        release_reserved();
        mem_tracker_ctx_shift();
        // auto* prev = tls_mem_tracker;
        // tls_mem_tracker = mem_tracker;
        auto* prev = _mem_tracker;
        _mem_tracker = mem_tracker;
        return prev;
    }

    // Return prev memory tracker.
    starrocks::MemTracker* set_operator_mem_tracker(starrocks::MemTracker* operator_mem_tracker) {
        operator_mem_tracker_ctx_shift();
        // auto* prev = tls_operator_mem_tracker;
        // tls_operator_mem_tracker = operator_mem_tracker;
        auto* prev = _operator_mem_tracker;
        _operator_mem_tracker = operator_mem_tracker;
        return prev;
    }

    bool set_check_mem_limit(bool check) {
        bool prev_check = _check;
        _check = check;
        return prev_check;
    }

    bool check_mem_limit() { return _check; }

    MemTracker* get_mem_tracker() {
        init();
        return _mem_tracker;
    }
    MemTracker* get_operator_mem_tracker() {
        return _operator_mem_tracker;
    }
    MemTracker* get_exceed_mem_tracker() const {
        return _exceed_mem_tracker;
    }
    void set_exceed_mem_tracker(MemTracker* mem_tracker) {
        _exceed_mem_tracker = mem_tracker;
    }

    static starrocks::MemTracker* mem_tracker();
    static starrocks::MemTracker* operator_mem_tracker();
    static bool is_check_mem_limit();

    // @TODO should consider bthread and pthread
    static CurrentThread& current();
    // @TODO test
    static CurrentThread* current_ptr();

    // @TODO consider bthread and pthread
    // static void set_exceed_mem_tracker(starrocks::MemTracker* mem_tracker);
    // {
    //     tls_exceed_mem_tracker = mem_tracker;
    // }

    bool set_is_catched(bool is_catched) {
        bool old = _is_catched;
        _is_catched = is_catched;
        return old;
    }

    bool is_catched() const { return _is_catched; }

    void mem_consume(int64_t size) {
        _mem_cache_manager.consume(size);
        _operator_mem_cache_manager.consume(size);
    }

    bool try_mem_consume(int64_t size) {
        if (_mem_cache_manager.try_mem_consume(size)) {
            _operator_mem_cache_manager.consume(size);
            return true;
        }
        return false;
    }

    bool try_mem_reserve(int64_t size, MemTracker* tracker, int64_t limit) {
        if (_mem_cache_manager.try_mem_reserve(size, tracker, limit)) {
            return true;
        }
        return false;
    }

    void release_reserved() { _mem_cache_manager.release_reserved(); }

    void mem_release(int64_t size) {
        _mem_cache_manager.release(size);
        _operator_mem_cache_manager.release(size);
    }

    static void mem_consume_without_cache(int64_t size) {
        MemTracker* cur_tracker = mem_tracker();
        // MemTracker* cur_tracker = current().mem_tracker();
        if (cur_tracker != nullptr && size != 0) {
            cur_tracker->consume(size);
        }
    }

    static bool try_mem_consume_without_cache(int64_t size) {
        // MemTracker* cur_tracker = current().mem_tracker();
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

    static void mem_release_without_cache(int64_t size) {
        MemTracker* cur_tracker = mem_tracker();
        if (cur_tracker != nullptr && size != 0) {
            cur_tracker->release(size);
        }
    }

    // get last time try consume and reset
    int64_t try_consume_mem_size() { return _mem_cache_manager.try_consume_mem_size(); }

    int64_t get_consumed_bytes() const { return _mem_cache_manager.get_consumed_bytes(); }

private:
    // In order to record operator level memory trace while keep up high performance, we need to
    // record the normal MemTracker's tree and operator's isolated MemTracker independently.
    // `tls_operator_mem_tracker` will be updated every time when `Operator::pull_chunk` or `Operator::push_chunk`
    // is invoked, the frequrency is a little bit high, but it does little harm to performance,
    // because operator's MemTracker, which is a dangling MemTracker(withouth parent), has no concurrency conflicts
    MemCacheManager _mem_cache_manager;
    MemCacheManager _operator_mem_cache_manager;

    MemTracker* _mem_tracker = nullptr;
    MemTracker* _operator_mem_tracker = nullptr;
    MemTracker* _exceed_mem_tracker = nullptr;
    // Store in TLS for diagnose coredump easier
    TUniqueId _query_id;
    TUniqueId _fragment_instance_id;
    std::string _custom_coredump_msg{};
    int32_t _driver_id = 0;
    bool _is_catched = false;
    bool _check = true;
    bool _is_init = false;

    // RuntimeContext* _ctx = nullptr;
};

// @TODO need a context, each bthread should know its context, before do other thing, set it to tls and when it switched, should set it back?
// used to set bthread specific data
extern bthread_key_t bthread_tls_key;
inline thread_local CurrentThread* bthread_tls_thread_status;
inline thread_local bthread_t bthread_tls_id;

inline thread_local CurrentThread tls_thread_status;

class ThreadLocalSwitcher {
public:
    static void switch_to_bthread_local() {
        if (bthread_self()) {
            // @TODO check if bthread is change?
            bthread_tls_thread_status = static_cast<CurrentThread*>(bthread_getspecific(bthread_tls_key));
            // @TODO first time to set
            if (bthread_tls_thread_status == nullptr) {
                // @TODO current thread may populate tls var, need refactor
                // tls_is_thread_status_init = false;
                // @TODO seems not set init?
                tls_thread_status.set_is_init(false);
                LOG(INFO) << "switch to bthread_local, create CurrentThread for bthread: " << bthread_self();
                bthread_tls_thread_status = new CurrentThread();
                // @TODO should set mem tracker and trace info outside it?
                CHECK_EQ(0, bthread_setspecific(bthread_tls_key, bthread_tls_thread_status));
                // tls_is_thread_status_init = true;
                tls_thread_status.set_is_init(true);
            } else {
                LOG(INFO) << "bthread tls already exists: " << bthread_self();
            }
            bthread_tls_id = bthread_self();
            // return true;
        }
        // return false;
    }

    static void switch_to_pthread_local() {
        if (bthread_self()) {
            if (bthread_equal(bthread_self(), bthread_tls_id) != 0) {
                // same bthread
                LOG(INFO) << "pthread not changed for bthread: " << bthread_self();
                bthread_tls_thread_status = static_cast<CurrentThread*>(bthread_getspecific(bthread_tls_key));
                DCHECK(bthread_tls_thread_status != nullptr);
            } else {
                LOG(INFO) << "pthread changed for bthread: " << bthread_self();
            }
            // @TODO what should we do
            LOG(INFO) << "switch to pthread_local, bthread_id: " << bthread_self();
            // return true;
        }
        // return false;
    }
};

class CurrentThreadMemTrackerSetter {
public:
    // @TODO record current thread?
    explicit CurrentThreadMemTrackerSetter(MemTracker* new_mem_tracker) {
        ThreadLocalSwitcher::switch_to_bthread_local();
        _current_thread = CurrentThread::current_ptr();
        _old_mem_tracker = _current_thread->get_mem_tracker();
        // _old_mem_tracker = CurrentThread::mem_tracker();
        // @TODO record thread status
        // _old_mem_tracker = tls_thread_status.mem_tracker();
        _is_same = (_old_mem_tracker == new_mem_tracker);
        if (!_is_same) {
            // LOG(INFO) << "old tracker: " << (_old_mem_tracker == nullptr ? "null" : _old_mem_tracker->label())
            //     << ", new tracker: " << (new_mem_tracker == nullptr ? "null" : new_mem_tracker->label())
            //     << ", bthread: " << bthread_self();
            // tls_thread_status.set_mem_tracker(new_mem_tracker);
            // CurrentThread::current().set_mem_tracker(new_mem_tracker);
            _current_thread->set_mem_tracker(new_mem_tracker);
        }
    }

    ~CurrentThreadMemTrackerSetter() {
        if (!_is_same) {
            // (void)tls_thread_status.set_mem_tracker(_old_mem_tracker);
            // CurrentThread::current().set_mem_tracker(_old_mem_tracker);
            _current_thread->set_mem_tracker(_old_mem_tracker);
        }
        ThreadLocalSwitcher::switch_to_pthread_local();
    }

    CurrentThreadMemTrackerSetter(const CurrentThreadMemTrackerSetter&) = delete;
    void operator=(const CurrentThreadMemTrackerSetter&) = delete;
    CurrentThreadMemTrackerSetter(CurrentThreadMemTrackerSetter&&) = delete;
    void operator=(CurrentThreadMemTrackerSetter&&) = delete;

private:
    MemTracker* _old_mem_tracker;
    bool _is_same;
    // @TODO record current thread
    CurrentThread* _current_thread = nullptr;
};


class CurrentThreadOperatorMemTrackerSetter {
public:
    explicit CurrentThreadOperatorMemTrackerSetter(MemTracker* new_mem_tracker) {
        // operator's mem tracker must have no parent
        DCHECK(new_mem_tracker == nullptr || new_mem_tracker->parent() == nullptr);
        // @TODO reduce function call
        ThreadLocalSwitcher::switch_to_bthread_local();
        _old_mem_tracker = CurrentThread::operator_mem_tracker();
        // _old_mem_tracker = tls_thread_status.operator_mem_tracker();
        _is_same = (_old_mem_tracker == new_mem_tracker);
        if (!_is_same) {
            // LOG(INFO) << "old op tracker: " << (_old_mem_tracker == nullptr ? "null" : _old_mem_tracker->label())
            //     << ", new op tracker: " << (new_mem_tracker == nullptr ? "null" : new_mem_tracker->label())
            //     << ", bthread: " << bthread_self();
            // tls_thread_status.set_operator_mem_tracker(new_mem_tracker);
            CurrentThread::current().set_operator_mem_tracker(new_mem_tracker);
        }
    }

    ~CurrentThreadOperatorMemTrackerSetter() {
        if (!_is_same) {
            // (void)tls_thread_status.set_operator_mem_tracker(_old_mem_tracker);
            CurrentThread::current().set_operator_mem_tracker(_old_mem_tracker);
        }
    }

    CurrentThreadOperatorMemTrackerSetter(const CurrentThreadOperatorMemTrackerSetter&) = delete;
    void operator=(const CurrentThreadOperatorMemTrackerSetter&) = delete;
    CurrentThreadOperatorMemTrackerSetter(CurrentThreadOperatorMemTrackerSetter&&) = delete;
    void operator=(CurrentThreadMemTrackerSetter&&) = delete;

private:
    MemTracker* _old_mem_tracker;
    bool _is_same;
};

class CurrentThreadCheckMemLimitSetter {
public:
    explicit CurrentThreadCheckMemLimitSetter(bool check) {
        _prev_check = CurrentThread::current().set_check_mem_limit(check);
        // _prev_check = tls_thread_status.set_check_mem_limit(check);
    }

    ~CurrentThreadCheckMemLimitSetter() {
        // (void)tls_thread_status.set_check_mem_limit(_prev_check);
        (void)CurrentThread::current().set_check_mem_limit(_prev_check);
    }

    CurrentThreadCheckMemLimitSetter(const CurrentThreadCheckMemLimitSetter&) = delete;
    void operator=(const CurrentThreadCheckMemLimitSetter&) = delete;
    CurrentThreadCheckMemLimitSetter(CurrentThreadCheckMemLimitSetter&&) = delete;
    void operator=(CurrentThreadCheckMemLimitSetter&&) = delete;

private:
    bool _prev_check;
};

// class CurrentBThreadCheckMemLimitSetter {
// public:
//     explicit CurrentBThreadCheckMemLimitSetter(bool check) {}
// };


class CurrentThreadCatchSetter {
public:
    explicit CurrentThreadCatchSetter(bool catched) {
        // _prev_catched = tls_thread_status.set_is_catched(catched);
        _prev_catched = CurrentThread::current().set_is_catched(catched);
    }

    ~CurrentThreadCatchSetter() {
        // (void)tls_thread_status.set_is_catched(_prev_catched);
        (void)CurrentThread::current().set_is_catched(_prev_catched);
    }

    CurrentThreadCatchSetter(const CurrentThreadCatchSetter&) = delete;
    void operator=(const CurrentThreadCatchSetter&) = delete;
    CurrentThreadCatchSetter(CurrentThreadCheckMemLimitSetter&&) = delete;
    void operator=(CurrentThreadCatchSetter&&) = delete;

private:
    bool _prev_catched;
};

#define SCOPED_SET_CATCHED(catched) auto VARNAME_LINENUM(catched_setter) = CurrentThreadCatchSetter(catched)

#define RELEASE_RESERVED_GUARD() \
    auto VARNAME_LINENUM(defer) = DeferOp([] { CurrentThread::current().release_reserved(); });

#define SET_TRACE_INFO(driver_id, query_id, fragment_instance_id) \
    CurrentThread::current().set_pipeline_driver_id(driver_id);   \
    CurrentThread::current().set_query_id(query_id);              \
    CurrentThread::current().set_fragment_instance_id(fragment_instance_id);

#define RESET_TRACE_INFO()                              \
    CurrentThread::current().set_pipeline_driver_id(0); \
    CurrentThread::current().set_query_id({});          \
    CurrentThread::current().set_fragment_instance_id({});

#define SCOPED_SET_TRACE_INFO(driver_id, query_id, fragment_instance_id) \
    SET_TRACE_INFO(driver_id, query_id, fragment_instance_id)            \
    auto VARNAME_LINENUM(defer) = DeferOp([] { RESET_TRACE_INFO() });

#define SCOPED_SET_CUSTOM_COREDUMP_MSG(custom_coredump_msg)                \
    CurrentThread::current().set_custom_coredump_msg(custom_coredump_msg); \
    auto VARNAME_LINENUM(defer) = DeferOp([] { CurrentThread::current().set_custom_coredump_msg({}); });

#define TRY_CATCH_ALLOC_SCOPE_START() \
    try {                             \
        SCOPED_SET_CATCHED(true);

#define TRY_CATCH_ALLOC_SCOPE_END()                                                                                    \
    }                                                                                                                  \
    catch (std::bad_alloc const&) {                                                                                    \
        CurrentThread& current_thread = CurrentThread::current(); \
        MemTracker* exceed_tracker = current_thread.get_exceed_mem_tracker(); \
        current_thread.set_exceed_mem_tracker(nullptr); \
        current_thread.set_is_catched(false); \
        if (LIKELY(exceed_tracker != nullptr)) {                                                                       \
            return Status::MemoryLimitExceeded(                                                                        \
                    exceed_tracker->err_msg(fmt::format("try consume:{}", current_thread.try_consume_mem_size()))); \
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
