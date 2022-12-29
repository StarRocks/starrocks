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
#include <utility>

#include "runtime/mem_pool.h"

namespace starrocks {
using AggDataPtr = uint8_t*;
struct HashTableKeyAllocator;

struct RawHashTableIterator {
    RawHashTableIterator(HashTableKeyAllocator* alloc_, size_t x_, int y_) : alloc(alloc_), x(x_), y(y_) {}
    bool operator==(const RawHashTableIterator& other) { return x == other.x && y == other.y; }
    bool operator!=(const RawHashTableIterator& other) { return !this->operator==(other); }
    inline void next();
    // return alloc[x]->states[y]
    inline uint8_t* value();
    HashTableKeyAllocator* alloc;
    size_t x;
    int y;
};

struct HashTableKeyAllocator {
    // number of states allocated consecutively in a single alloc
    static auto constexpr alloc_batch_size = 1024;
    // memory aligned when allocate
    static size_t constexpr aligned = 16;

    int aggregate_key_size = 0;
    std::vector<std::pair<void*, int>> vecs;
    MemPool* pool = nullptr;

    RawHashTableIterator begin() { return {this, 0, 0}; }

    RawHashTableIterator end() { return {this, vecs.size(), 0}; }

    AggDataPtr allocate() {
        if (vecs.empty() || vecs.back().second == alloc_batch_size) {
            uint8_t* mem = pool->allocate_aligned(alloc_batch_size * aggregate_key_size, aligned);
            vecs.emplace_back(mem, 0);
        }
        return static_cast<AggDataPtr>(vecs.back().first) + aggregate_key_size * vecs.back().second++;
    }

    uint8_t* allocate_null_key_data() { return pool->allocate_aligned(alloc_batch_size * aggregate_key_size, aligned); }

    void reset() { vecs.clear(); }
};

inline void RawHashTableIterator::next() {
    y++;
    if (y == alloc->vecs[x].second) {
        y = 0;
        x++;
    }
}

inline uint8_t* RawHashTableIterator::value() {
    return static_cast<uint8_t*>(alloc->vecs[x].first) + alloc->aggregate_key_size * y;
}

} // namespace starrocks