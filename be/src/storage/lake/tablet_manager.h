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

#include <bthread/types.h>

#include <variant>

#include "common/statusor.h"
#include "gutil/macros.h"
#include "storage/lake/metadata_iterator.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/types_fwd.h"

namespace starrocks {
class Cache;
class CacheKey;
class Segment;
class TCreateTabletReq;
} // namespace starrocks

namespace starrocks::lake {

template <typename T>
class MetadataIterator;
class UpdateManager;
using TabletMetadataIter = MetadataIterator<TabletMetadataPtr>;
using TxnLogIter = MetadataIterator<TxnLogPtr>;

class TabletManager {
    friend class Tablet;

public:
    // Does NOT take the ownership of |location_provider| and |location_provider| must outlive
    // this TabletManager.
    // |cache_capacity| is the max number of bytes can be used by the
    // metadata cache.
    explicit TabletManager(LocationProvider* location_provider, UpdateManager* update_mgr, int64_t cache_capacity);

    ~TabletManager();

    DISALLOW_COPY_AND_MOVE(TabletManager);

    Status create_tablet(const TCreateTabletReq& req);

    StatusOr<Tablet> get_tablet(int64_t tablet_id);

    Status delete_tablet(int64_t tablet_id);

    // Returns the compaction score of the newly created tablet metadata
    StatusOr<double> publish_version(int64_t tablet_id, int64_t base_version, int64_t new_version, const int64_t* txns,
                                     int txns_size);

    void abort_txn(int64_t tablet_id, const int64_t* txns, int txns_size);

    StatusOr<CompactionTaskPtr> compact(int64_t tablet_id, int64_t version, int64_t txn_id);

    Status put_tablet_metadata(const TabletMetadata& metadata);

    Status put_tablet_metadata(TabletMetadataPtr metadata);

    StatusOr<TabletMetadataPtr> get_tablet_metadata(int64_t tablet_id, int64_t version);

    StatusOr<TabletMetadataPtr> get_tablet_metadata(const std::string& path, bool fill_cache = true);

    StatusOr<TabletMetadataIter> list_tablet_metadata(int64_t tablet_id, bool filter_tablet);

    Status delete_tablet_metadata(int64_t tablet_id, int64_t version);

    Status put_txn_log(const TxnLog& log);

    Status put_txn_log(TxnLogPtr log);

    StatusOr<TxnLogPtr> get_txn_log(int64_t tablet_id, int64_t txn_id);

    StatusOr<TxnLogPtr> get_txn_vlog(int64_t tablet_id, int64_t version);

    StatusOr<TxnLogPtr> get_txn_log(const std::string& path, bool fill_cache = true);

    StatusOr<TxnLogIter> list_txn_log(int64_t tablet_id, bool filter_tablet);

    Status delete_txn_log(int64_t tablet_id, int64_t txn_id);

    Status delete_txn_vlog(int64_t tablet_id, int64_t version);

    Status delete_segment(int64_t tablet_id, std::string_view segment_name);

    // Transform a txn log into versioned txn log(i.e., rename `{tablet_id}_{txn_id}.log` to `{tablet_id}_{log_version}.vlog`)
    Status publish_log_version(int64_t tablet_id, int64_t txn_id, int64 log_version);

    Status put_tablet_metadata_lock(int64_t tablet_id, int64_t version, int64_t expire_time);

    Status delete_tablet_metadata_lock(int64_t tablet_id, int64_t version, int64_t expire_time);

    // put tablet_metadata and delvec to meta file. Only in PK table
    Status put_tablet_metadata_delvec(const TabletMetadata& metadata,
                                      const std::vector<std::pair<std::string, DelVectorPtr>>& del_vecs);

    void prune_metacache();

    // TODO: remove this method
    LocationProvider* TEST_set_location_provider(LocationProvider* value) {
        auto ret = _location_provider;
        _location_provider = value;
        return ret;
    }

    std::string tablet_root_location(int64_t tablet_id) const;

    std::string tablet_metadata_root_location(int64_t tablet_id) const;

    std::string tablet_metadata_location(int64_t tablet_id, int64_t version) const;

    std::string txn_log_location(int64_t tablet_id, int64_t txn_id) const;

    std::string txn_vlog_location(int64_t tablet_id, int64_t version) const;

    std::string segment_location(int64_t tablet_id, std::string_view segment_name) const;

    std::string del_location(int64_t tablet_id, std::string_view del_name) const;

    std::string delvec_location(int64_t tablet_id, int64_t version) const;

    std::string tablet_metadata_lock_location(int64_t tablet_id, int64_t version, int64_t expire_time) const;

    const LocationProvider* location_provider() const { return _location_provider; }

    void start_gc();

    // Return a set of tablet that owned by this TabletManager.
    std::set<int64_t> owned_tablets();

    UpdateManager* update_mgr();

private:
    using CacheValue = std::variant<TabletMetadataPtr, TxnLogPtr, TabletSchemaPtr, SegmentPtr>;

    static std::string tablet_schema_cache_key(int64_t tablet_id);
    static void cache_value_deleter(const CacheKey& /*key*/, void* value) { delete static_cast<CacheValue*>(value); }

    StatusOr<TabletSchemaPtr> get_tablet_schema(int64_t tablet_id);

    StatusOr<TabletMetadataPtr> load_tablet_metadata(const std::string& metadata_location, bool fill_cache);
    StatusOr<TxnLogPtr> load_txn_log(const std::string& txn_log_location, bool fill_cache);

    /// Cache operations
    bool fill_metacache(std::string_view key, CacheValue* ptr, int size);
    void erase_metacache(std::string_view key);

    TabletMetadataPtr lookup_tablet_metadata(std::string_view key);
    TxnLogPtr lookup_txn_log(std::string_view key);
    TabletSchemaPtr lookup_tablet_schema(std::string_view key);
    SegmentPtr lookup_segment(std::string_view key);
    void cache_segment(std::string_view key, SegmentPtr segment);

    LocationProvider* _location_provider;
    std::unique_ptr<Cache> _metacache;
    UpdateManager* _update_mgr;

    bthread_t _gc_checker_tid;
};

} // namespace starrocks::lake
