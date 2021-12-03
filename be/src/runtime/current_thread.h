// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "runtime/current_thread_impl.h"

#define SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(mem_tracker) \
    auto VARNAME_LINENUM(tracker_setter) = starrocks::CurrentThreadMemTrackerSetter(mem_tracker)

namespace starrocks {
namespace current_thread {

inline void set_query_id(const TUniqueId& query_id) {
    impl::ThreadLocalData::get_or_create()->set_query_id(query_id);
}

inline UniqueId query_id() {
    impl::ThreadLocalData* tls = impl::ThreadLocalData::get();
    return tls ? tls->query_id() : UniqueId(0, 0);
}

// Return prev memory tracker.
inline MemTracker* set_mem_tracker(MemTracker* mem_tracker) {
    impl::clear_tls_cache();
    return impl::ThreadLocalData::get_or_create()->set_mem_tracker(mem_tracker);
}

inline MemTracker* mem_tracker() {
    impl::ThreadLocalData* tls = impl::ThreadLocalData::get();
    return tls ? tls->mem_tracker() : ExecEnv::GetInstance()->process_mem_tracker();
}

inline void mem_consume(int64_t size) {
    impl::ThreadLocalData* tls = impl::ThreadLocalData::get();
    if (tls) {
        tls->mem_consume(size);
    } else {
        impl::consume_tls_cache(size);
    }
}

inline void mem_release(int64_t size) {
    impl::ThreadLocalData* tls = impl::ThreadLocalData::get();
    if (tls) {
        tls->mem_release(size);
    } else {
        impl::release_tls_cache(size);
    }
}

} // namespace current_thread

class CurrentThreadMemTrackerSetter {
public:
    explicit CurrentThreadMemTrackerSetter(MemTracker* new_mem_tracker)
            : _old_mem_tracker(current_thread::set_mem_tracker(new_mem_tracker)) {}

    ~CurrentThreadMemTrackerSetter() { (void)current_thread::set_mem_tracker(_old_mem_tracker); }

    CurrentThreadMemTrackerSetter(const CurrentThreadMemTrackerSetter&) = delete;
    void operator=(const CurrentThreadMemTrackerSetter&) = delete;
    CurrentThreadMemTrackerSetter(CurrentThreadMemTrackerSetter&&) = delete;
    void operator=(CurrentThreadMemTrackerSetter&&) = delete;

private:
    MemTracker* _old_mem_tracker;
};

} // namespace starrocks
