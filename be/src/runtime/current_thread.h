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

namespace starrocks {

class TUniqueId;

inline thread_local MemTracker* tls_mem_tracker = nullptr;
// `tls_singleton_check_mem_tracker` is used when you want to separate the mem tracker and check tracker,
// you can add a new check tracker by set up `tls_singleton_check_mem_tracker`.
inline thread_local bool tls_is_thread_status_init = false;

class CurrentThread {
private:
    class MemCacheManager {
    public:
        MemCacheManager() {}
        MemCacheManager(const MemCacheManager&) = delete;
        MemCacheManager(MemCacheManager&&) = delete;

        void consume(int64_t size) {
            _cache_size += size;
            _total_consumed_bytes += size;
            if (_cache_size >= BATCH_SIZE) {
                commit(false);
            }
        }

        bool try_mem_consume(int64_t size) {
            MemTracker* cur_tracker = mem_tracker();
            _cache_size += size;
            _total_consumed_bytes += size;
            auto failure_handler = [&]() {
                _cache_size -= size;
                _total_consumed_bytes -= size;
            };
            if (_cache_size >= BATCH_SIZE) {
                if (cur_tracker != nullptr) {
                    MemTracker* limit_tracker = cur_tracker->try_consume(_cache_size);
                    if (LIKELY(limit_tracker == nullptr)) {
                        _cache_size = 0;
                        return true;
                    } else {
                        failure_handler();
                        return false;
                    }
                }
            }
            return true;
        }

        void release(int64_t size) {
            _cache_size -= size;
            _total_consumed_bytes -= size;
            if (_cache_size <= -BATCH_SIZE) {
                commit(false);
            }
        }

        void commit(bool is_ctx_shift) {
            MemTracker* cur_tracker = mem_tracker();
            if (cur_tracker != nullptr) {
                cur_tracker->consume(_cache_size);
            }
            _cache_size = 0;
        }

        int64_t get_consumed_bytes() const { return _total_consumed_bytes; }

    private:
        const static int64_t BATCH_SIZE = 2 * 1024 * 1024;

        // Allocated or delocated but not committed memory bytes, can be negative
        int64_t _cache_size = 0;
        int64_t _total_consumed_bytes = 0; // Totally consumed memory bytes
    };

public:
    CurrentThread() { tls_is_thread_status_init = true; }
    ~CurrentThread();

    void mem_tracker_ctx_shift() { _mem_cache_manager.commit(true); }

    // Return prev memory tracker.
    starrocks::MemTracker* set_mem_tracker(starrocks::MemTracker* mem_tracker) {
        mem_tracker_ctx_shift();
        auto* prev = tls_mem_tracker;
        tls_mem_tracker = mem_tracker;
        return prev;
    }

    bool check_mem_limit() { return _check; }

    static starrocks::MemTracker* mem_tracker();

    static CurrentThread& current();

    bool set_is_catched(bool is_catched) {
        bool old = _is_catched;
        _is_catched = is_catched;
        return old;
    }

    bool is_catched() const { return _is_catched; }

    void mem_consume(int64_t size) { _mem_cache_manager.consume(size); }

    bool try_mem_consume(int64_t size) {
        if (_mem_cache_manager.try_mem_consume(size)) {
            return true;
        }
        return false;
    }

    void mem_release(int64_t size) { _mem_cache_manager.release(size); }

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

    static void mem_release_without_cache(int64_t size) {
        MemTracker* cur_tracker = mem_tracker();
        if (cur_tracker != nullptr && size != 0) {
            cur_tracker->release(size);
        }
    }

    int64_t get_consumed_bytes() const { return _mem_cache_manager.get_consumed_bytes(); }

private:
    // In order to record operator level memory trace while keep up high performance, we need to
    // record the normal MemTracker's tree and operator's isolated MemTracker independently.
    // `tls_operator_mem_tracker` will be updated every time when `Operator::pull_chunk` or `Operator::push_chunk`
    // is invoked, the frequrency is a little bit high, but it does little harm to performance,
    // because operator's MemTracker, which is a dangling MemTracker(withouth parent), has no concurrency conflicts
    MemCacheManager _mem_cache_manager;
    bool _is_catched = false;
    bool _check = true;
};

inline thread_local CurrentThread tls_thread_status;

class CurrentThreadCatchSetter {
public:
    explicit CurrentThreadCatchSetter(bool catched) { _prev_catched = tls_thread_status.set_is_catched(catched); }

    ~CurrentThreadCatchSetter() { (void)tls_thread_status.set_is_catched(_prev_catched); }

    CurrentThreadCatchSetter(const CurrentThreadCatchSetter&) = delete;
    void operator=(const CurrentThreadCatchSetter&) = delete;
    void operator=(CurrentThreadCatchSetter&&) = delete;

private:
    bool _prev_catched;
};

#define SCOPED_SET_CATCHED(catched) auto VARNAME_LINENUM(catched_setter) = CurrentThreadCatchSetter(catched)

#define TRY_CATCH_ALLOC_SCOPE_START() \
    try {                             \
        SCOPED_SET_CATCHED(true);

#define TRY_CATCH_ALLOC_SCOPE_END()                                                 \
    }                                                                               \
    catch (std::bad_alloc const&) {                                                 \
        tls_thread_status.set_is_catched(false);                                    \
        return Status::MemoryLimitExceeded("Mem usage has exceed the limit of BE"); \
    }                                                                               \
    catch (std::runtime_error const& e) {                                           \
        return Status::RuntimeError(fmt::format("Runtime error: {}", e.what()));    \
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
