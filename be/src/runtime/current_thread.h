// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <string>

#include "gen_cpp/Types_types.h"
#include "util/uid_util.h"

namespace starrocks {
class MemTracker;
class TUniqueId;
} // namespace starrocks

namespace starrocks {

class CurrentThread {
public:
    static void set_query_id(const starrocks::TUniqueId& query_id);
    static const starrocks::TUniqueId& query_id();
    static const std::string& query_id_string();

    // Return old memory tracker.
    static starrocks::MemTracker* set_mem_tracker(starrocks::MemTracker* tracker);
    // Return current memory tracker in this thread.
    static starrocks::MemTracker* mem_tracker();

private:
    // `__thread` is faster than `thread_local`.
    static inline __thread starrocks::MemTracker* s_tls_mem_tracker{nullptr}; // NOLINT
    static inline thread_local starrocks::TUniqueId s_tls_query_id{};         // NOLINT
    static inline thread_local std::string s_tls_str_query_id{};              // NOLINT
};

inline void CurrentThread::set_query_id(const starrocks::TUniqueId& query_id) {
    s_tls_query_id = query_id;
    s_tls_str_query_id = starrocks::print_id(query_id);
}

inline const starrocks::TUniqueId& CurrentThread::query_id() {
    return s_tls_query_id;
}

inline const std::string& CurrentThread::query_id_string() {
    return s_tls_str_query_id;
}

inline starrocks::MemTracker* CurrentThread::set_mem_tracker(starrocks::MemTracker* tracker) {
    auto* r = s_tls_mem_tracker;
    s_tls_mem_tracker = tracker;
    return r;
}

inline starrocks::MemTracker* CurrentThread::mem_tracker() {
    return s_tls_mem_tracker;
}

} // namespace starrocks
