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

#include <shared_mutex>
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
class TabletSchemaPB;
class TCreateTabletReq;
} // namespace starrocks

namespace starrocks::lake {

template <typename T>
class MetadataIterator;
class UpdateManager;
using TabletMetadataIter = MetadataIterator<TabletMetadataPtr>;
using TxnLogIter = MetadataIterator<TxnLogPtr>;

class CompactionScheduler;

class TabletManager {
    friend class Tablet;
    friend class MetaFileBuilder;
    friend class MetaFileReader;

public:
    // Does NOT take the ownership of |location_provider| and |location_provider| must outlive
    // this TabletManager.
    // |cache_capacity| is the max number of bytes can be used by the
    // metadata cache.
    explicit TabletManager(LocationProvider* location_provider, UpdateManager* update_mgr, int64_t cache_capacity);

    ~TabletManager();

    DISALLOW_COPY_AND_MOVE(TabletManager);

    [[nodiscard]] Status create_tablet(const TCreateTabletReq& req);

    StatusOr<Tablet> get_tablet(int64_t tablet_id);

    [[nodiscard]] Status delete_tablet(int64_t tablet_id);

    StatusOr<CompactionTaskPtr> compact(int64_t tablet_id, int64_t version, int64_t txn_id);

    [[nodiscard]] Status put_tablet_metadata(const TabletMetadata& metadata);

    [[nodiscard]] Status put_tablet_metadata(TabletMetadataPtr metadata);

    StatusOr<TabletMetadataPtr> get_tablet_metadata(int64_t tablet_id, int64_t version);

    StatusOr<TabletMetadataPtr> get_tablet_metadata(const std::string& path, bool fill_cache = true);

    TabletMetadataPtr get_latest_cached_tablet_metadata(int64_t tablet_id);

    StatusOr<TabletMetadataIter> list_tablet_metadata(int64_t tablet_id, bool filter_tablet);

    [[nodiscard]] Status delete_tablet_metadata(int64_t tablet_id, int64_t version);

    [[nodiscard]] Status put_txn_log(const TxnLog& log);

    [[nodiscard]] Status put_txn_log(TxnLogPtr log);

    StatusOr<TxnLogPtr> get_txn_log(int64_t tablet_id, int64_t txn_id);

    StatusOr<TxnLogPtr> get_txn_log(const std::string& path, bool fill_cache = true);

    StatusOr<TxnLogPtr> get_txn_vlog(int64_t tablet_id, int64_t version);

    StatusOr<TxnLogPtr> get_txn_vlog(const std::string& path, bool fill_cache = true);

<<<<<<< HEAD
    StatusOr<TxnLogIter> list_txn_log(int64_t tablet_id, bool filter_tablet);

    [[nodiscard]] Status delete_txn_log(int64_t tablet_id, int64_t txn_id);

    [[nodiscard]] Status delete_txn_vlog(int64_t tablet_id, int64_t version);

    [[nodiscard]] Status put_tablet_metadata_lock(int64_t tablet_id, int64_t version, int64_t expire_time);

    [[nodiscard]] Status delete_tablet_metadata_lock(int64_t tablet_id, int64_t version, int64_t expire_time);
=======
#ifdef USE_STAROS
    bool is_tablet_in_worker(int64_t tablet_id);
#endif // USE_STAROS
>>>>>>> 8e4e211b4c ([Enhancement] Support gc local persistent index in shared data (#32184))

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

    std::string delvec_location(int64_t tablet_id, std::string_view delvec_filename) const;

    std::string tablet_metadata_lock_location(int64_t tablet_id, int64_t version, int64_t expire_time) const;

    const LocationProvider* location_provider() const { return _location_provider; }

    UpdateManager* update_mgr();

    CompactionScheduler* compaction_scheduler() { return _compaction_scheduler.get(); }

    void update_metacache_limit(size_t limit);

    // The return value will never be null.
    Cache* metacache() { return _metacache.get(); }

    int64_t in_writing_data_size(int64_t tablet_id);

    void set_in_writing_data_size(int64_t tablet_id, int64_t txn_id, int64_t size);

    void remove_in_writing_data_size(int64_t tablet_id, int64_t txn_id);

    // only for TEST purpose
    void TEST_set_global_schema_cache(int64_t index_id, TabletSchemaPtr schema);

    void update_segment_cache_size(std::string_view key);

private:
    using CacheValue = std::variant<TabletMetadataPtr, TxnLogPtr, TabletSchemaPtr, SegmentPtr, DelVectorPtr>;

    static std::string global_schema_cache_key(int64_t index_id);
    static std::string tablet_schema_cache_key(int64_t tablet_id);
    static std::string tablet_latest_metadata_cache_key(int64_t tablet_id);
    static void cache_value_deleter(const CacheKey& /*key*/, void* value) { delete static_cast<CacheValue*>(value); }

    Status create_schema_file(int64_t tablet_id, const TabletSchemaPB& schema_pb);
    StatusOr<TabletSchemaPtr> load_and_parse_schema_file(const std::string& path);
    StatusOr<TabletSchemaPtr> get_tablet_schema(int64_t tablet_id, int64_t* version_hint = nullptr);
    StatusOr<TabletSchemaPtr> get_tablet_schema_by_index_id(int64_t tablet_id, int64_t index_id);

    StatusOr<TabletMetadataPtr> load_tablet_metadata(const std::string& metadata_location, bool fill_cache);
    StatusOr<TxnLogPtr> load_txn_log(const std::string& txn_log_location, bool fill_cache);

    /// Cache operations
    void fill_metacache(std::string_view key, CacheValue* ptr, size_t size);
    void erase_metacache(std::string_view key);

    TabletMetadataPtr lookup_tablet_metadata(std::string_view key);
    TxnLogPtr lookup_txn_log(std::string_view key);
    TabletSchemaPtr lookup_tablet_schema(std::string_view key);
    SegmentPtr lookup_segment(std::string_view key);
    void cache_segment(std::string_view key, SegmentPtr segment);
    DelVectorPtr lookup_delvec(std::string_view key);
    void cache_delvec(std::string_view key, DelVectorPtr delvec);
    // only store tablet's latest metadata
    TabletMetadataPtr lookup_tablet_latest_metadata(std::string_view key);
    void cache_tablet_latest_metadata(TabletMetadataPtr metadata);

    LocationProvider* _location_provider;
    std::unique_ptr<Cache> _metacache;
    std::unique_ptr<CompactionScheduler> _compaction_scheduler;
    UpdateManager* _update_mgr;

    std::shared_mutex _meta_lock;
    std::unordered_map<int64_t, std::unordered_map<int64_t, int64_t>> _tablet_in_writing_txn_size;
};

} // namespace starrocks::lake
