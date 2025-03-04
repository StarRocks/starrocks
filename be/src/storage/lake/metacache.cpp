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

#include "storage/lake/metacache.h"

#include <bvar/bvar.h>

#include "gen_cpp/lake_types.pb.h"
#include "storage/del_vector.h"
#include "storage/lake/tablet_manager.h"
#include "storage/rowset/segment.h"
#include "util/lru_cache.h"

namespace starrocks::lake {

static bvar::Adder<uint64_t> g_metadata_cache_hit;
static bvar::Window<bvar::Adder<uint64_t>> g_metadata_cache_hit_minute("lake", "metadata_cache_hit_minute",
                                                                       &g_metadata_cache_hit, 60);

static bvar::Adder<uint64_t> g_metadata_cache_miss;
static bvar::Window<bvar::Adder<uint64_t>> g_metadata_cache_miss_minute("lake", "metadata_cache_miss_minute",
                                                                        &g_metadata_cache_miss, 60);

static bvar::Adder<uint64_t> g_txnlog_cache_hit;
static bvar::Window<bvar::Adder<uint64_t>> g_txnlog_cache_hit_minute("lake", "txn_log_cache_hit_minute",
                                                                     &g_txnlog_cache_hit, 60);

static bvar::Adder<uint64_t> g_txnlog_cache_miss;
static bvar::Window<bvar::Adder<uint64_t>> g_txnlog_cache_miss_minute("lake", "txn_log_cache_miss_minute",
                                                                      &g_txnlog_cache_miss, 60);

static bvar::Adder<uint64_t> g_schema_cache_hit;
static bvar::Window<bvar::Adder<uint64_t>> g_schema_cache_hit_minute("lake", "schema_cache_hit_minute",
                                                                     &g_schema_cache_hit, 60);

static bvar::Adder<uint64_t> g_schema_cache_miss;
static bvar::Window<bvar::Adder<uint64_t>> g_schema_cache_miss_minute("lake", "schema_cache_miss_minute",
                                                                      &g_schema_cache_miss, 60);

static bvar::Adder<uint64_t> g_dv_cache_hit;
static bvar::Window<bvar::Adder<uint64_t>> g_dv_cache_hit_minute("lake", "delvec_cache_hit_minute", &g_dv_cache_hit,
                                                                 60);

static bvar::Adder<uint64_t> g_dv_cache_miss;
static bvar::Window<bvar::Adder<uint64_t>> g_dv_cache_miss_minute("lake", "delvec_cache_miss_minute", &g_dv_cache_miss,
                                                                  60);

static bvar::Adder<uint64_t> g_segment_cache_hit;
static bvar::Window<bvar::Adder<uint64_t>> g_segment_cache_hit_minute("lake", "segment_cache_hit_minute",
                                                                      &g_segment_cache_hit, 60);

static bvar::Adder<uint64_t> g_segment_cache_miss;
static bvar::Window<bvar::Adder<uint64_t>> g_segment_cache_miss_minute("lake", "segment_cache_miss_minute",
                                                                       &g_segment_cache_miss, 60);

#ifndef BE_TEST
static Metacache* get_metacache() {
    auto mgr = ExecEnv::GetInstance()->lake_tablet_manager();
    return (mgr != nullptr) ? mgr->metacache() : nullptr;
}

static size_t get_metacache_capacity(void*) {
    auto cache = get_metacache();
    return (cache != nullptr) ? cache->capacity() : 0;
}

static size_t get_metacache_usage(void*) {
    auto cache = get_metacache();
    return (cache != nullptr) ? cache->memory_usage() : 0;
}

static bvar::PassiveStatus<size_t> g_metacache_capacity("lake", "metacache_capacity", get_metacache_capacity, nullptr);
static bvar::PassiveStatus<size_t> g_metacache_usage("lake", "metacache_usage", get_metacache_usage, nullptr);
#endif

Metacache::Metacache(int64_t cache_capacity) : _cache(new_lru_cache(cache_capacity)) {}

Metacache::~Metacache() = default;

void Metacache::insert(std::string_view key, CacheValue* ptr, size_t size) {
    Cache::Handle* handle = _cache->insert(CacheKey(key), ptr, size, size, cache_value_deleter);
    _cache->release(handle);
}

std::shared_ptr<const TabletMetadataPB> Metacache::lookup_tablet_metadata(std::string_view key) {
    auto handle = _cache->lookup(CacheKey(key));
    if (handle == nullptr) {
        g_metadata_cache_miss << 1;
        return nullptr;
    }
    DeferOp defer([this, handle]() { _cache->release(handle); });

    try {
        auto value = static_cast<CacheValue*>(_cache->value(handle));
        auto metadata = std::get<std::shared_ptr<const TabletMetadataPB>>(*value);
        g_metadata_cache_hit << 1;
        return metadata;
    } catch (const std::bad_variant_access& e) {
        return nullptr;
    }
}

std::shared_ptr<const TxnLogPB> Metacache::lookup_txn_log(std::string_view key) {
    auto handle = _cache->lookup(CacheKey(key));
    if (handle == nullptr) {
        g_txnlog_cache_miss << 1;
        return nullptr;
    }
    DeferOp defer([this, handle]() { _cache->release(handle); });

    try {
        auto value = static_cast<CacheValue*>(_cache->value(handle));
        auto log = std::get<std::shared_ptr<const TxnLogPB>>(*value);
        g_txnlog_cache_hit << 1;
        return log;
    } catch (const std::bad_variant_access& e) {
        return nullptr;
    }
}

std::shared_ptr<const CombinedTxnLogPB> Metacache::lookup_combined_txn_log(std::string_view key) {
    auto handle = _cache->lookup(CacheKey(key));
    if (handle == nullptr) {
        g_txnlog_cache_miss << 1;
        return nullptr;
    }
    DeferOp defer([this, handle]() { _cache->release(handle); });

    try {
        auto value = static_cast<CacheValue*>(_cache->value(handle));
        auto log = std::get<std::shared_ptr<const CombinedTxnLogPB>>(*value);
        g_txnlog_cache_hit << 1;
        return log;
    } catch (const std::bad_variant_access& e) {
        return nullptr;
    }
}

std::shared_ptr<const TabletSchema> Metacache::lookup_tablet_schema(std::string_view key) {
    auto handle = _cache->lookup(CacheKey(key));
    if (handle == nullptr) {
        g_schema_cache_miss << 1;
        return nullptr;
    }
    DeferOp defer([this, handle]() { _cache->release(handle); });

    try {
        auto value = static_cast<CacheValue*>(_cache->value(handle));
        auto schema = std::get<std::shared_ptr<const TabletSchema>>(*value);
        g_schema_cache_hit << 1;
        return schema;
    } catch (const std::bad_variant_access& e) {
        return nullptr;
    }
}

std::shared_ptr<Segment> Metacache::lookup_segment(std::string_view key) {
    std::lock_guard<std::mutex> lock(_mutex);
    return _lookup_segment_no_lock(key);
}

std::shared_ptr<Segment> Metacache::_lookup_segment_no_lock(std::string_view key) {
    auto handle = _cache->lookup(CacheKey(key));
    if (handle == nullptr) {
        g_segment_cache_miss << 1;
        return nullptr;
    }
    DeferOp defer([this, handle]() { _cache->release(handle); });

    try {
        auto value = static_cast<CacheValue*>(_cache->value(handle));
        auto segment = std::get<std::shared_ptr<Segment>>(*value);
        g_segment_cache_hit << 1;
        return segment;
    } catch (const std::bad_variant_access& e) {
        return nullptr;
    }
}

std::shared_ptr<const DelVector> Metacache::lookup_delvec(std::string_view key) {
    auto handle = _cache->lookup(CacheKey(key));
    if (handle == nullptr) {
        g_dv_cache_miss << 1;
        return nullptr;
    }
    DeferOp defer([this, handle]() { _cache->release(handle); });

    try {
        auto value = static_cast<CacheValue*>(_cache->value(handle));
        auto delvec = std::get<std::shared_ptr<const DelVector>>(*value);
        g_dv_cache_hit << 1;
        return delvec;
    } catch (const std::bad_variant_access& e) {
        return nullptr;
    }
}

void Metacache::cache_segment(std::string_view key, std::shared_ptr<Segment> segment) {
    std::lock_guard<std::mutex> lock(_mutex);
    _cache_segment_no_lock(key, std::move(segment));
}

void Metacache::_cache_segment_no_lock(std::string_view key, std::shared_ptr<Segment> segment) {
    auto mem_cost = segment->mem_usage();
    auto value = std::make_unique<CacheValue>(std::move(segment));
    insert(key, value.release(), mem_cost);
}

std::shared_ptr<Segment> Metacache::cache_segment_if_absent(std::string_view key, std::shared_ptr<Segment> segment) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto seg = _lookup_segment_no_lock(key);
    if (seg != nullptr) {
        // already exists, return the one in cache
        return seg;
    }
    _cache_segment_no_lock(key, std::move(segment));
    // it is possible that the `cache_segment` fails
    return _lookup_segment_no_lock(key);
}

void Metacache::cache_delvec(std::string_view key, std::shared_ptr<const DelVector> delvec) {
    auto mem_cost = delvec->memory_usage();
    auto value = std::make_unique<CacheValue>(std::move(delvec));
    insert(key, value.release(), mem_cost);
}

void Metacache::cache_tablet_metadata(std::string_view key, std::shared_ptr<const TabletMetadataPB> metadata) {
    auto value_ptr = std::make_unique<CacheValue>(metadata);
    insert(key, value_ptr.release(), metadata->SpaceUsedLong());
}

void Metacache::cache_txn_log(std::string_view key, std::shared_ptr<const TxnLogPB> log) {
    auto value_ptr = std::make_unique<CacheValue>(log);
    insert(key, value_ptr.release(), log->SpaceUsedLong());
}

void Metacache::cache_combined_txn_log(std::string_view key, std::shared_ptr<const CombinedTxnLogPB> log) {
    auto value_ptr = std::make_unique<CacheValue>(log);
    insert(key, value_ptr.release(), log->SpaceUsedLong());
}

void Metacache::cache_tablet_schema(std::string_view key, std::shared_ptr<const TabletSchema> schema, size_t size) {
    auto cache_value = std::make_unique<CacheValue>(schema);
    insert(key, cache_value.release(), size);
}

void Metacache::erase(std::string_view key) {
    _cache->erase(CacheKey(key));
}

void Metacache::update_capacity(size_t new_capacity) {
    size_t old_capacity = _cache->get_capacity();
    int64_t delta = (int64_t)new_capacity - (int64_t)old_capacity;
    if (delta != 0) {
        (void)_cache->adjust_capacity(delta);
        VLOG(5) << "Changed metadache capacity from " << old_capacity << " to " << _cache->get_capacity();
    }
}

void Metacache::prune() {
    _cache->prune();
}

size_t Metacache::memory_usage() const {
    return _cache->get_memory_usage();
}

size_t Metacache::capacity() const {
    return _cache->get_capacity();
}

} // namespace starrocks::lake
