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
#include "compaction_task_context.h"
#include "gutil/macros.h"
#include "storage/lake/metadata_iterator.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/types_fwd.h"
#include "storage/options.h"
#include "storage/rowset/base_rowset.h"
#include "util/bthreads/single_flight.h"

namespace starrocks {
struct FileInfo;
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
using TabletAndRowsets = std::tuple<std::shared_ptr<Tablet>, std::vector<BaseRowsetSharedPtr>>;

class CompactionScheduler;
class Metacache;
class VersionedTablet;

class TabletManager {
    friend class Tablet;
    friend class MetaFileBuilder;

public:
    // Does NOT take the ownership of |location_provider| and |location_provider| must outlive
    // this TabletManager.
    // |cache_capacity| is the max number of bytes can be used by the
    // metadata cache.
    explicit TabletManager(std::shared_ptr<LocationProvider> location_provider, UpdateManager* update_mgr,
                           int64_t cache_capacity);

    explicit TabletManager(std::shared_ptr<LocationProvider> location_provider, int64_t cache_capacity);

    ~TabletManager();

    DISALLOW_COPY_AND_MOVE(TabletManager);

    Status create_tablet(const TCreateTabletReq& req);

    StatusOr<Tablet> get_tablet(int64_t tablet_id);

    StatusOr<VersionedTablet> get_tablet(int64_t tablet_id, int64_t version);

    StatusOr<CompactionTaskPtr> compact(CompactionTaskContext* context);

    Status put_tablet_metadata(const TabletMetadata& metadata);

    Status put_tablet_metadata(const TabletMetadataPtr& metadata);

    // When using get_tablet_metadata to determine whether a new version exists in publish version,
    // a valid expected_gtid must be passed in.
    StatusOr<TabletMetadataPtr> get_tablet_metadata(int64_t tablet_id, int64_t version, bool fill_cache = true,
                                                    int64_t expected_gtid = 0,
                                                    const std::shared_ptr<FileSystem>& fs = nullptr);

    // Do not use this function except in a list dir
    StatusOr<TabletMetadataPtr> get_tablet_metadata(const std::string& path, bool fill_cache = true,
                                                    int64_t expected_gtid = 0,
                                                    const std::shared_ptr<FileSystem>& fs = nullptr);

    TabletMetadataPtr get_latest_cached_tablet_metadata(int64_t tablet_id);

    StatusOr<TabletMetadataIter> list_tablet_metadata(int64_t tablet_id);

    Status delete_tablet_metadata(int64_t tablet_id, int64_t version);

    Status put_txn_log(const TxnLog& log);

    Status put_txn_log(const TxnLogPtr& log);

    Status put_txn_log(const TxnLogPtr& log, const std::string& path);

    Status put_txn_slog(const TxnLogPtr& log);

    Status put_txn_slog(const TxnLogPtr& log, const std::string& path);

    Status put_txn_vlog(const TxnLogPtr& log, int64_t version);

    Status put_combined_txn_log(const CombinedTxnLogPB& logs);

    StatusOr<TxnLogPtr> get_txn_log(int64_t tablet_id, int64_t txn_id);

    StatusOr<TxnLogPtr> get_txn_log(const std::string& path, bool fill_cache = true);

    StatusOr<CombinedTxnLogPtr> get_combined_txn_log(const std::string& path, bool fill_cache = true);

    StatusOr<TxnLogPtr> get_txn_slog(int64_t tablet_id, int64_t txn_id);

    StatusOr<TxnLogPtr> get_txn_slog(const std::string& path, bool fill_cache = true);

    StatusOr<TxnLogPtr> get_txn_vlog(int64_t tablet_id, int64_t version);

    StatusOr<TxnLogPtr> get_txn_vlog(const std::string& path, bool fill_cache = true);

    StatusOr<TabletSchemaPtr> get_output_rowset_schema(std::vector<uint32_t>& input_rowset,
                                                       const TabletMetadata* metadata);

#ifdef USE_STAROS
#if !defined(BUILD_FORMAT_LIB)
    bool is_tablet_in_worker(int64_t tablet_id);
#else
    bool is_tablet_in_worker(int64_t tablet_id) { return true; }
#endif
#endif // USE_STAROS

    void prune_metacache();

    // TODO: remove this method
    std::shared_ptr<LocationProvider> TEST_set_location_provider(std::shared_ptr<LocationProvider> value) {
        auto ret = _location_provider;
        _location_provider = value;
        return ret;
    }

    std::string tablet_root_location(int64_t tablet_id) const;

    std::string tablet_metadata_root_location(int64_t tablet_id) const;

    std::string tablet_metadata_location(int64_t tablet_id, int64_t version) const;

    std::string tablet_initial_metadata_location(int64_t tablet_id) const;

    std::string txn_log_location(int64_t tablet_id, int64_t txn_id) const;

    std::string txn_slog_location(int64_t tablet_id, int64_t txn_id) const;

    std::string txn_vlog_location(int64_t tablet_id, int64_t version) const;

    std::string combined_txn_log_location(int64_t tablet_id, int64_t txn_id) const;

    std::string segment_location(int64_t tablet_id, std::string_view segment_name) const;

    std::string del_location(int64_t tablet_id, std::string_view del_name) const;

    std::string delvec_location(int64_t tablet_id, std::string_view delvec_filename) const;

    const std::shared_ptr<LocationProvider> location_provider() { return _location_provider; }
    std::string sst_location(int64_t tablet_id, std::string_view sst_filename) const;

    UpdateManager* update_mgr();

    CompactionScheduler* compaction_scheduler() { return _compaction_scheduler.get(); }

    void update_metacache_limit(size_t limit);

    // The return value will never be null.
    Metacache* metacache() { return _metacache.get(); }

    StatusOr<int64_t> get_tablet_data_size(int64_t tablet_id, int64_t* version_hint);

    StatusOr<int64_t> get_tablet_num_rows(int64_t tablet_id, int64_t version);

    int64_t in_writing_data_size(int64_t tablet_id);

    int64_t add_in_writing_data_size(int64_t tablet_id, int64_t size);

    void clean_in_writing_data_size();

    // only for TEST purpose
    void TEST_set_global_schema_cache(int64_t index_id, TabletSchemaPtr schema);

    // update cache size of the segment with the given key, optionally provide the segment address hint.
    // If segment_addr_hint is provided and it's non-zero, the cache size will be only updated when the
    // instance address matches the address provided by the segment_addr_hint. This is used to prevent
    // updating the cache size where the cached object is not the one as expected.
    void update_segment_cache_size(std::string_view key, intptr_t segment_addr_hint = 0);

    StatusOr<SegmentPtr> load_segment(const FileInfo& segment_info, int segment_id, size_t* footer_size_hint,
                                      const LakeIOOptions& lake_io_opts, bool fill_metadata_cache,
                                      TabletSchemaPtr tablet_schema);
    // for load segment parallel
    StatusOr<SegmentPtr> load_segment(const FileInfo& segment_info, int segment_id, const LakeIOOptions& lake_io_opts,
                                      bool fill_metadata_cache, TabletSchemaPtr tablet_schema);

    StatusOr<TabletSchemaPtr> get_tablet_schema(int64_t tablet_id, int64_t* version_hint = nullptr);

    Status create_schema_file(int64_t tablet_id, const TabletSchemaPB& schema_pb);
    StatusOr<TabletAndRowsets> capture_tablet_and_rowsets(int64_t tablet_id, int64_t from_version, int64_t to_version);

    int64_t get_average_row_size_from_latest_metadata(int64_t tablet_id);

    void stop();

private:
    static std::string global_schema_cache_key(int64_t index_id);
    static std::string tablet_schema_cache_key(int64_t tablet_id);
    static std::string tablet_latest_metadata_cache_key(int64_t tablet_id);
    static Status drop_local_cache(const std::string& path);

    StatusOr<TabletSchemaPtr> load_and_parse_schema_file(const std::string& path);
    StatusOr<TabletSchemaPtr> get_tablet_schema_by_id(int64_t tablet_id, int64_t schema_id);

    Status put_tablet_metadata(const TabletMetadataPtr& metadata, const std::string& metadata_location);
    StatusOr<TabletMetadataPtr> load_tablet_metadata(const std::string& metadata_location, bool fill_cache,
                                                     int64_t expected_gtid, const std::shared_ptr<FileSystem>& fs);
    StatusOr<TxnLogPtr> load_txn_log(const std::string& txn_log_location, bool fill_cache);
    StatusOr<CombinedTxnLogPtr> load_combined_txn_log(const std::string& path, bool fill_cache);

private:
    std::shared_ptr<LocationProvider> _location_provider;
    std::unique_ptr<Metacache> _metacache;
    std::unique_ptr<CompactionScheduler> _compaction_scheduler;
    UpdateManager* _update_mgr = nullptr;

    std::shared_mutex _meta_lock;
    std::unordered_map<int64_t, int64_t> _tablet_in_writing_size;

    bthreads::singleflight::Group<std::string, StatusOr<TabletSchemaPtr>> _schema_group;
    bthreads::singleflight::Group<std::string, StatusOr<CombinedTxnLogPtr>> _combined_txn_log_group;
};

} // namespace starrocks::lake
