// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

inline __thread bool g_tls_enable_mem_tracker = true;

inline bool memory_hook_enabled() {
    return g_tls_enable_mem_tracker;
}
inline void enable_memory_hook() {
    g_tls_enable_mem_tracker = true;
}
inline void disable_memory_hook() {
    g_tls_enable_mem_tracker = false;
}
