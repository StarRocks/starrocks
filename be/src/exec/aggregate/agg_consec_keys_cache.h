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
#include <cstring>
#include <string>

#include "base/string/slice.h"
#include "exprs/agg/aggregate.h"
#include "gutil/compiler_util.h"

namespace starrocks {

// Consecutive keys cache: for sorted/clustered input, consecutive rows
// may share the same GROUP BY (Map side) or DISTINCT (Set side) key.
// The cache remembers the last (key -> cached value) pair within a
// chunk and replays it when the next key matches, skipping the hash
// table probe.  Adaptive disable kicks in when the hit rate is too low.

// Configuration for adaptive cache behavior
struct ConsecutiveKeyCacheConfig {
    // Number of chunks to process before evaluating cache effectiveness
    static constexpr size_t EVAL_CHUNKS = 3;
    // Minimum hit rate (0.0 - 1.0) to keep cache enabled
    // 30% is conservative - even modest hit rates provide benefit
    static constexpr double MIN_HIT_RATE = 0.3;
    // Minimum samples before making a decision (avoid disabling on tiny datasets)
    static constexpr size_t MIN_SAMPLES = 1000;
};

// Shared adaptive enable/disable + counters.  The wrappers' cache types
// derive from this and the two-level conversion carry-over reads these
// fields directly (see _carry_over_post_init_state in agg_hash_variant.cpp).
struct ConsecutiveKeyCacheStats {
    bool cache_enabled = true;
    size_t hits = 0;
    size_t misses = 0;
    size_t chunks_processed = 0;

    ALWAYS_INLINE bool is_enabled() const { return cache_enabled; }
    size_t get_hits() const { return hits; }
    size_t get_misses() const { return misses; }

    // Permanently turn the cache off (session-var gate).
    void force_disable() { cache_enabled = false; }

protected:
    // Called by derived on_chunk_start() after resetting per-chunk state.
    void on_chunk_advance() {
        if (!cache_enabled) return;
        ++chunks_processed;
        if (chunks_processed >= ConsecutiveKeyCacheConfig::EVAL_CHUNKS) {
            evaluate_and_maybe_disable();
        }
    }

private:
    void evaluate_and_maybe_disable() {
        size_t total = hits + misses;
        if (total < ConsecutiveKeyCacheConfig::MIN_SAMPLES) {
            return;
        }
        double hit_rate = static_cast<double>(hits) / static_cast<double>(total);
        if (hit_rate < ConsecutiveKeyCacheConfig::MIN_HIT_RATE) {
            cache_enabled = false;
        }
    }
};

// Generic cache for any key type that supports equality comparison and
// trivial copy.  CachedValue is the per-row payload replayed on cache
// hit: AggDataPtr on the Map side (the resolved aggregate state), bool
// on the Set side (was this key in the set).
template <typename KeyType, typename CachedValue = AggDataPtr>
struct ConsecutiveKeyCache : public ConsecutiveKeyCacheStats {
    KeyType last_key{};
    CachedValue last_value{};
    bool has_cached_value = false;

    // Returns true on hit and writes the replayed value into out_value.
    ALWAYS_INLINE bool try_get(const KeyType& key, CachedValue& out_value) {
        if (has_cached_value && key == last_key) {
            out_value = last_value;
            ++hits;
            return true;
        }
        ++misses;
        return false;
    }

    ALWAYS_INLINE void update(const KeyType& key, CachedValue value) {
        last_key = key;
        last_value = value;
        has_cached_value = true;
    }

    void on_chunk_start() {
        has_cached_value = false;
        last_value = CachedValue{};
        this->on_chunk_advance();
    }

    void reset() {
        has_cached_value = false;
        last_value = CachedValue{};
        // Note: We don't reset cache_enabled, hits, misses, chunks_processed
        // Those persist across the lifetime of the hash table.
    }
};

// Slice specialization - the data backing the Slice (input column buffer)
// outlives the cache within a chunk, so we store the pointer without
// copying.  The on-chunk-start hook resets the Slice to a known-empty
// sentinel.
template <typename CachedValue>
struct ConsecutiveKeyCache<Slice, CachedValue> : public ConsecutiveKeyCacheStats {
    Slice last_key{};
    CachedValue last_value{};
    bool has_cached_value = false;

    ALWAYS_INLINE bool try_get(const Slice& key, CachedValue& out_value) {
        if (has_cached_value && last_key == key) {
            out_value = last_value;
            ++hits;
            return true;
        }
        ++misses;
        return false;
    }

    ALWAYS_INLINE void update(const Slice& key, CachedValue value) {
        last_key = key;
        last_value = value;
        has_cached_value = true;
    }

    void on_chunk_start() {
        has_cached_value = false;
        last_value = CachedValue{};
        last_key = Slice{};
        this->on_chunk_advance();
    }

    void reset() {
        has_cached_value = false;
        last_value = CachedValue{};
        last_key = Slice{};
    }
};

// Specialised cache for the SerializedKey wrappers: the serialization
// buffer is reused between rows/chunks, so we cannot borrow a Slice -
// we copy the bytes into an owned std::string instead.
template <typename CachedValue = AggDataPtr>
struct ConsecutiveSerializedKeyCache : public ConsecutiveKeyCacheStats {
    std::string last_key_data;
    CachedValue last_value{};
    bool has_cached_value = false;

    ALWAYS_INLINE bool try_get(const Slice& key, CachedValue& out_value) {
        if (has_cached_value && key.size == last_key_data.size() &&
            memcmp(key.data, last_key_data.data(), key.size) == 0) {
            out_value = last_value;
            ++hits;
            return true;
        }
        ++misses;
        return false;
    }

    ALWAYS_INLINE void update(const Slice& key, CachedValue value) {
        last_key_data.assign(reinterpret_cast<const char*>(key.data), key.size);
        last_value = value;
        has_cached_value = true;
    }

    void on_chunk_start() {
        has_cached_value = false;
        last_value = CachedValue{};
        this->on_chunk_advance();
    }

    void reset() {
        has_cached_value = false;
        last_value = CachedValue{};
        last_key_data.clear();
    }
};

} // namespace starrocks
