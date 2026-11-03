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

#include <memory>
#include <string_view>
#include <variant>

#include "gutil/macros.h"

namespace starrocks {
class Cache;
class CacheKey;
class DelVector;
class Segment;
class TabletSchema;
class TabletMetadataPB;
class TxnLogPB;
class CombinedTxnLogPB;
} // namespace starrocks

namespace starrocks::lake {

using CacheValue = std::variant<std::shared_ptr<const TabletMetadataPB>, std::shared_ptr<const TxnLogPB>,
                                std::shared_ptr<const TabletSchema>, std::shared_ptr<const DelVector>,
                                std::shared_ptr<Segment>, std::shared_ptr<const CombinedTxnLogPB>, bool>;

// Byte capacity of a metacache LRU sized at |percent| of |mem_limit|, with |percent| clamped to [0, 100].
int64_t metacache_capacity(int64_t mem_limit, int32_t percent);

// Two independent LRU caches behind one interface. Tablet metadata, txn logs and bundled metadata markers
// go to the tablet metadata cache; segments, delete vectors and tablet schemas go to the segment metadata
// cache. Segments are large and churn with every query and compaction, so keeping them apart stops them
// from evicting the small tablet metadata and txn logs that every load, publish and scan starts from.
class Metacache {
public:
    Metacache(int64_t tablet_metadata_capacity, int64_t segment_metadata_capacity);

    ~Metacache();

    DISALLOW_COPY_AND_MOVE(Metacache);

    std::shared_ptr<const TabletMetadataPB> lookup_tablet_metadata(std::string_view key);

    std::shared_ptr<const TxnLogPB> lookup_txn_log(std::string_view key);

    std::shared_ptr<const CombinedTxnLogPB> lookup_combined_txn_log(std::string_view key);

    std::shared_ptr<const TabletSchema> lookup_tablet_schema(std::string_view key);

    std::shared_ptr<Segment> lookup_segment(std::string_view key);

    std::shared_ptr<const DelVector> lookup_delvec(std::string_view key);

    bool lookup_bundled_metadata_marker(std::string_view key);

    void cache_tablet_metadata(std::string_view key, std::shared_ptr<const TabletMetadataPB> metadata);

    void cache_tablet_schema(std::string_view key, std::shared_ptr<const TabletSchema> schema, size_t size);

    void cache_txn_log(std::string_view key, std::shared_ptr<const TxnLogPB> log);

    void cache_combined_txn_log(std::string_view key, std::shared_ptr<const CombinedTxnLogPB> log);

    void cache_segment(std::string_view key, std::shared_ptr<Segment> segment);

    // Update the cached memory footprint of an already cached segment in place, without
    // rebuilding the cache entry. Does nothing and returns false when the key is absent
    // or no longer maps to `segment`.
    bool update_segment_cache_size(std::string_view key, size_t mem_cost, const Segment* segment);

    // Replace only the entry that still points to expected. A failed comparison
    // neither inserts replacement nor changes the existing entry or its charge.
    // Readers holding the old Segment remain valid after a successful replacement.
    bool replace_segment_if_matches(std::string_view key, const Segment* expected,
                                    const std::shared_ptr<Segment>& replacement);

    // cache the segment if the given key not exists in the cache, returns the segment shared_ptr stored in the cache.
    std::shared_ptr<Segment> cache_segment_if_absent(std::string_view key, const std::shared_ptr<Segment>& segment);

    void cache_delvec(std::string_view key, std::shared_ptr<const DelVector> delvec);

    void cache_bundled_metadata_marker(std::string_view key);

    // Erases |key| from both caches.
    void erase(std::string_view key);

    void update_tablet_metadata_capacity(size_t new_capacity);

    void update_segment_metadata_capacity(size_t new_capacity);

    void prune();

    size_t tablet_metadata_memory_usage() const;

    size_t tablet_metadata_capacity() const;

    size_t segment_metadata_memory_usage() const;

    size_t segment_metadata_capacity() const;

private:
    static void cache_value_deleter(const CacheKey& key, void* value);

    static void insert(Cache* cache, std::string_view key, CacheValue* ptr, size_t size);

    static void update_capacity(Cache* cache, size_t new_capacity);

    // No metacache-level lock: conditional replacement, the compare-and-insert needed by cache_segment_if_absent()
    // and the in-place charge update needed by update_segment_cache_size() are provided
    // atomically by the underlying sharded cache. Keeping the cache lock as the only lock
    // also keeps cached value destructors (notably ~Segment) off every lock, because
    // LRUCache always runs deleters after releasing its shard mutex.
    std::unique_ptr<Cache> _tablet_metadata_cache;
    std::unique_ptr<Cache> _segment_metadata_cache;
};

} // namespace starrocks::lake
