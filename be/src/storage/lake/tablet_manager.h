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
class Metacache;

class TabletManager {
    friend class Tablet;
    friend class MetaFileBuilder;

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

    [[nodiscard]] Status put_tablet_metadata(const TabletMetadataPtr& metadata);

    StatusOr<TabletMetadataPtr> get_tablet_metadata(int64_t tablet_id, int64_t version);

    StatusOr<TabletMetadataPtr> get_tablet_metadata(const std::string& path, bool fill_cache = true);

    TabletMetadataPtr get_latest_cached_tablet_metadata(int64_t tablet_id);

    StatusOr<TabletMetadataIter> list_tablet_metadata(int64_t tablet_id, bool filter_tablet);

    [[nodiscard]] Status delete_tablet_metadata(int64_t tablet_id, int64_t version);

    [[nodiscard]] Status put_txn_log(const TxnLog& log);

    [[nodiscard]] Status put_txn_log(const TxnLogPtr& log);

    StatusOr<TxnLogPtr> get_txn_log(int64_t tablet_id, int64_t txn_id);

    StatusOr<TxnLogPtr> get_txn_log(const std::string& path, bool fill_cache = true);

    StatusOr<TxnLogPtr> get_txn_vlog(int64_t tablet_id, int64_t version);

    StatusOr<TxnLogPtr> get_txn_vlog(const std::string& path, bool fill_cache = true);
    StatusOr<TxnLogIter> list_txn_log(int64_t tablet_id, bool filter_tablet);

    [[nodiscard]] Status delete_txn_log(int64_t tablet_id, int64_t txn_id);

    [[nodiscard]] Status delete_txn_vlog(int64_t tablet_id, int64_t version);

    [[nodiscard]] Status put_tablet_metadata_lock(int64_t tablet_id, int64_t version, int64_t expire_time);

    [[nodiscard]] Status delete_tablet_metadata_lock(int64_t tablet_id, int64_t version, int64_t expire_time);

#ifdef USE_STAROS
    bool is_tablet_in_worker(int64_t tablet_id);
#endif // USE_STAROS

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
    Metacache* metacache() { return _metacache.get(); }

    // only for TEST purpose
    void TEST_set_global_schema_cache(int64_t index_id, TabletSchemaPtr schema);

    void update_segment_cache_size(std::string_view key);

private:
    static std::string global_schema_cache_key(int64_t index_id);
    static std::string tablet_schema_cache_key(int64_t tablet_id);
    static std::string tablet_latest_metadata_cache_key(int64_t tablet_id);

    Status create_schema_file(int64_t tablet_id, const TabletSchemaPB& schema_pb);
    StatusOr<TabletSchemaPtr> load_and_parse_schema_file(const std::string& path);
    StatusOr<TabletSchemaPtr> get_tablet_schema(int64_t tablet_id, int64_t* version_hint = nullptr);

    StatusOr<TabletMetadataPtr> load_tablet_metadata(const std::string& metadata_location, bool fill_cache);
    StatusOr<TxnLogPtr> load_txn_log(const std::string& txn_log_location, bool fill_cache);

    LocationProvider* _location_provider;
    std::unique_ptr<Metacache> _metacache;
    std::unique_ptr<CompactionScheduler> _compaction_scheduler;
    UpdateManager* _update_mgr;

    std::shared_mutex _meta_lock;
};

} // namespace starrocks::lake
