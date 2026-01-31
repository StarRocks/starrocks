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

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "common/config.h"
#include "jemalloc/jemalloc.h"

namespace starrocks {

// Forward declaration
class PageCacheArena;

// Helper function declarations to avoid incomplete type issues in allocator
PageCacheArena* get_pagecache_arena_instance();
bool pagecache_arena_is_initialized(PageCacheArena* arena);
void* pagecache_arena_allocate(PageCacheArena* arena, size_t size);
void pagecache_arena_deallocate(PageCacheArena* arena, void* ptr, size_t size);

// Custom allocator for std::vector that uses the pagecache arena
template <typename T>
class PageCacheArenaAllocator {
public:
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    template <typename U>
    struct rebind {
        using other = PageCacheArenaAllocator<U>;
    };

    PageCacheArenaAllocator() = default;
    ~PageCacheArenaAllocator() = default;

    template <typename U>
    PageCacheArenaAllocator(const PageCacheArenaAllocator<U>&) {}

    pointer allocate(size_type n) {
        if (n == 0) {
            return nullptr;
        }

        const size_t bytes = n * sizeof(T);

        // Check config first - if arena is disabled, use default jemalloc
        if (!config::enable_pagecache_arena) {
            // Use jemalloc directly (which is the default allocator in StarRocks)
            void* ptr = je_malloc(bytes);
            return static_cast<pointer>(ptr);
        }

        // Arena is enabled, try to use pagecache arena
        auto* arena = get_pagecache_arena_instance();
        if (arena != nullptr && pagecache_arena_is_initialized(arena)) {
            void* ptr = pagecache_arena_allocate(arena, bytes);
            if (ptr != nullptr) {
                return static_cast<pointer>(ptr);
            }
            // If arena allocation fails, fallback to default jemalloc
        }

        // Fallback to default jemalloc
        void* ptr = je_malloc(bytes);
        return static_cast<pointer>(ptr);
    }

    void deallocate(pointer p, size_type n) {
        if (p == nullptr) {
            return;
        }

        const size_t bytes = n * sizeof(T);

        // Check config first - if arena is disabled, use default jemalloc
        if (!config::enable_pagecache_arena) {
            je_free(p);
            return;
        }

        // Arena is enabled, try to use pagecache arena deallocation
        // Note: We use je_free here instead of arena-specific deallocation because:
        // 1. je_free automatically detects which arena the pointer belongs to
        // 2. This handles the case where allocation fell back to je_malloc
        //    (when arena allocation failed or arena was not initialized at allocation time)
        // 3. This ensures correct deallocation regardless of where the pointer was allocated
        auto* arena = get_pagecache_arena_instance();
        if (arena != nullptr && pagecache_arena_is_initialized(arena)) {
            // Use arena deallocation, which internally uses je_free to auto-detect arena
            pagecache_arena_deallocate(arena, p, bytes);
            return;
        }

        // Fallback to default jemalloc if arena is not available
        // This handles cases where allocation fell back to je_malloc
        // (e.g., when arena allocation failed or arena was not initialized at allocation time)
        je_free(p);
    }

    template <typename U, typename... Args>
    void construct(U* p, Args&&... args) {
        ::new (static_cast<void*>(p)) U(std::forward<Args>(args)...);
    }

    template <typename U>
    void destroy(U* p) {
        p->~U();
    }

    bool operator==(const PageCacheArenaAllocator&) const { return true; }
    bool operator!=(const PageCacheArenaAllocator&) const { return false; }
};

// Type alias for vector using pagecache arena allocator.
// The allocator internally checks config::enable_pagecache_arena:
// - If enabled: uses pagecache arena for all allocations (initial and resize)
// - If disabled: uses default jemalloc for all allocations
// This ensures consistent allocation behavior throughout the vector's lifetime.
using PageCacheVector = std::vector<uint8_t, PageCacheArenaAllocator<uint8_t>>;

// Manages a separate jemalloc arena dedicated to pagecache memory allocations.
// This arena is configured with aggressive decay settings to minimize memory fragmentation.
class PageCacheArena {
public:
    // Get the singleton instance
    static PageCacheArena* instance();

    // Initialize the arena with aggressive decay settings
    // Returns true on success, false on failure
    bool init();

    // Get the arena index
    unsigned get_arena_index() const { return _arena_index; }

    // Check if the arena is initialized
    bool is_initialized() const { return _initialized; }

    // Allocate memory from the pagecache arena
    void* allocate(size_t size);

    // Free memory allocated from the pagecache arena
    void deallocate(void* ptr, size_t size);

    // Get arena statistics
    struct ArenaStats {
        size_t allocated = 0; // Bytes allocated
        size_t active = 0;    // Active bytes
        size_t resident = 0;  // Resident bytes
        size_t mapped = 0;    // Mapped bytes
        size_t retained = 0;  // Retained bytes
    };

    // Retrieve current arena statistics
    bool get_stats(ArenaStats* stats);

    // Delete copy constructor and assignment operator
    PageCacheArena(const PageCacheArena&) = delete;
    PageCacheArena& operator=(const PageCacheArena&) = delete;

private:
    PageCacheArena() = default;
    ~PageCacheArena() = default;

    unsigned _arena_index = 0;
    bool _initialized = false;
};

} // namespace starrocks
