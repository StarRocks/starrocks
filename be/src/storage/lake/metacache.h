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
#include <mutex>
#include <string_view>
#include <variant>

#include "gutil/macros.h"

namespace starrocks {
class Cache;
class CacheKey;
class DelVector;
class Segment;
class TabletSchema;
} // namespace starrocks

namespace starrocks::lake {

class TabletMetadataPB;
class TxnLogPB;

using CacheValue =
        std::variant<std::shared_ptr<const TabletMetadataPB>, std::shared_ptr<const TxnLogPB>,
                     std::shared_ptr<const TabletSchema>, std::shared_ptr<const DelVector>, std::shared_ptr<Segment>>;

class Metacache {
public:
    explicit Metacache(int64_t cache_capacity);

    ~Metacache();

    DISALLOW_COPY_AND_MOVE(Metacache);

    std::shared_ptr<const TabletMetadataPB> lookup_tablet_metadata(std::string_view key);

    std::shared_ptr<const TxnLogPB> lookup_txn_log(std::string_view key);

    std::shared_ptr<const TabletSchema> lookup_tablet_schema(std::string_view key);

    std::shared_ptr<Segment> lookup_segment(std::string_view key);

    std::shared_ptr<const DelVector> lookup_delvec(std::string_view key);

    void cache_tablet_metadata(std::string_view key, std::shared_ptr<const TabletMetadataPB> metadata);

    void cache_tablet_schema(std::string_view key, std::shared_ptr<const TabletSchema> schema, size_t size);

    void cache_txn_log(std::string_view key, std::shared_ptr<const TxnLogPB> log);

    void cache_segment(std::string_view key, std::shared_ptr<Segment> segment);

    // cache the segment if the given key not exists in the cache, returns the segment shared_ptr stored in the cache.
    std::shared_ptr<Segment> cache_segment_if_absent(std::string_view key, std::shared_ptr<Segment> segment);

    void cache_delvec(std::string_view key, std::shared_ptr<const DelVector> delvec);

    void erase(std::string_view key);

    void update_capacity(size_t new_capacity);

    void prune();

    size_t memory_usage() const;

    size_t capacity() const;

private:
    static void cache_value_deleter(const CacheKey& /*key*/, void* value) { delete static_cast<CacheValue*>(value); }

    std::shared_ptr<Segment> _lookup_segment_no_lock(std::string_view key);
    void _cache_segment_no_lock(std::string_view key, std::shared_ptr<Segment> segment);

    void insert(std::string_view key, CacheValue* ptr, size_t size);

    std::unique_ptr<Cache> _cache;

    std::mutex _mutex;
};

} // namespace starrocks::lake
