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

#include "cache/mem_cache/pagecache_arena.h"

#include <cstring>

#include "glog/logging.h"
#include "jemalloc/jemalloc.h"

namespace starrocks {

PageCacheArena* PageCacheArena::instance() {
    static PageCacheArena arena;
    return &arena;
}

// Helper function for allocator to avoid incomplete type issues
PageCacheArena* get_pagecache_arena_instance() {
    return PageCacheArena::instance();
}

// Helper functions to check initialization and allocate/deallocate
// These are needed because allocator template is instantiated before PageCacheArena is fully defined
bool pagecache_arena_is_initialized(PageCacheArena* arena) {
    return arena->is_initialized();
}

void* pagecache_arena_allocate(PageCacheArena* arena, size_t size) {
    return arena->allocate(size);
}

void pagecache_arena_deallocate(PageCacheArena* arena, void* ptr, size_t size) {
    arena->deallocate(ptr, size);
}

bool PageCacheArena::init() {
    if (_initialized) {
        return true;
    }

    // Create a new arena
    size_t arena_index_size = sizeof(unsigned);
    int ret = je_mallctl("arenas.create", &_arena_index, &arena_index_size, nullptr, 0);
    if (ret != 0) {
        LOG(ERROR) << "Failed to create jemalloc arena for pagecache, error: " << ret;
        return false;
    }

    // Configure aggressive decay settings to minimize memory fragmentation
    // Use shorter decay times: 1000ms (1 second) for both dirty and muzzy pages
    // This is more aggressive than the default 5000ms
    uint64_t dirty_decay_ms = 1000;
    uint64_t muzzy_decay_ms = 1000;

    char dirty_decay_key[64];
    snprintf(dirty_decay_key, sizeof(dirty_decay_key), "arena.%u.dirty_decay_ms", _arena_index);
    size_t sz = sizeof(dirty_decay_ms);
    ret = je_mallctl(dirty_decay_key, nullptr, nullptr, &dirty_decay_ms, sz);
    if (ret != 0) {
        LOG(WARNING) << "Failed to set dirty_decay_ms for pagecache arena, error: " << ret;
    }

    char muzzy_decay_key[64];
    snprintf(muzzy_decay_key, sizeof(muzzy_decay_key), "arena.%u.muzzy_decay_ms", _arena_index);
    sz = sizeof(muzzy_decay_ms);
    ret = je_mallctl(muzzy_decay_key, nullptr, nullptr, &muzzy_decay_ms, sz);
    if (ret != 0) {
        LOG(WARNING) << "Failed to set muzzy_decay_ms for pagecache arena, error: " << ret;
    }

    // Enable background thread for automatic decay
    // This helps release memory more aggressively
    char background_thread_key[64];
    snprintf(background_thread_key, sizeof(background_thread_key), "arena.%u.background_thread", _arena_index);
    bool background_thread = true;
    sz = sizeof(background_thread);
    ret = je_mallctl(background_thread_key, nullptr, nullptr, &background_thread, sz);
    if (ret != 0) {
        LOG(WARNING) << "Failed to enable background_thread for pagecache arena, error: " << ret;
    }

    _initialized = true;
    LOG(INFO) << "PageCacheArena initialized with arena index " << _arena_index << ", dirty_decay_ms=" << dirty_decay_ms
              << ", muzzy_decay_ms=" << muzzy_decay_ms;
    return true;
}

void* PageCacheArena::allocate(size_t size) {
    if (!_initialized) {
        LOG(ERROR) << "PageCacheArena not initialized";
        return nullptr;
    }

    // Use MALLOCX_ARENA to allocate from the specific arena
    int flags = MALLOCX_ARENA(_arena_index) | MALLOCX_TCACHE_NONE;
    void* ptr = je_mallocx(size, flags);
    return ptr;
}

void PageCacheArena::deallocate(void* ptr, size_t size) {
    if (ptr == nullptr) {
        return;
    }

    if (!_initialized) {
        LOG(ERROR) << "PageCacheArena not initialized";
        return;
    }

    // Use je_free instead of je_dallocx to let jemalloc automatically detect
    // which arena the pointer belongs to. This is safer because:
    // 1. je_free automatically detects the arena from the pointer metadata
    // 2. je_dallocx with MALLOCX_ARENA requires the pointer to be from that specific arena,
    //    which may not always be true if allocation fell back to default jemalloc
    (void)size; // size parameter is not used by je_free
    je_free(ptr);
}

bool PageCacheArena::get_stats(ArenaStats* stats) {
    if (!_initialized || stats == nullptr) {
        return false;
    }

    // Update epoch to refresh stats
    uint64_t epoch = 1;
    size_t sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);

    // Get arena-specific stats
    char allocated_key[64];
    snprintf(allocated_key, sizeof(allocated_key), "stats.arenas.%u.allocated", _arena_index);
    sz = sizeof(stats->allocated);
    if (je_mallctl(allocated_key, &stats->allocated, &sz, nullptr, 0) != 0) {
        return false;
    }

    char active_key[64];
    snprintf(active_key, sizeof(active_key), "stats.arenas.%u.active", _arena_index);
    sz = sizeof(stats->active);
    if (je_mallctl(active_key, &stats->active, &sz, nullptr, 0) != 0) {
        return false;
    }

    char resident_key[64];
    snprintf(resident_key, sizeof(resident_key), "stats.arenas.%u.resident", _arena_index);
    sz = sizeof(stats->resident);
    if (je_mallctl(resident_key, &stats->resident, &sz, nullptr, 0) != 0) {
        return false;
    }

    char mapped_key[64];
    snprintf(mapped_key, sizeof(mapped_key), "stats.arenas.%u.mapped", _arena_index);
    sz = sizeof(stats->mapped);
    if (je_mallctl(mapped_key, &stats->mapped, &sz, nullptr, 0) != 0) {
        return false;
    }

    char retained_key[64];
    snprintf(retained_key, sizeof(retained_key), "stats.arenas.%u.retained", _arena_index);
    sz = sizeof(stats->retained);
    if (je_mallctl(retained_key, &stats->retained, &sz, nullptr, 0) != 0) {
        return false;
    }

    return true;
}

} // namespace starrocks
