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

#include <set>
#include <shared_mutex>
#include <variant>
#include <vector>

#include "common/statusor.h"
#include "compaction_task_context.h"
#include "gen_cpp/Types_types.h" // for PUniqueId
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
struct TabletBasicInfo;
class Segment;
class TabletSchemaPB;
class TCreateTabletReq;
} // namespace starrocks

namespace starrocks::lake {

struct CacheOptions;
template <typename T>
class MetadataIterator;
class UpdateManager;
using TabletMetadataIter = MetadataIterator<TabletMetadataPtr>;
using TxnLogIter = MetadataIterator<TxnLogPtr>;
using TabletAndRowsets = std::tuple<std::shared_ptr<Tablet>, std::vector<BaseRowsetSharedPtr>>;

class CompactionScheduler;
class Metacache;
class VersionedTablet;
class TableSchemaService;

class TabletManager {
    friend class Tablet;
    friend class MetaFileBuilder;
    friend class TableSchemaService;

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

    StatusOr<VersionedTablet> get_tablet(int64_t tablet_id, int64_t version, bool fill_meta_cache = true,
                                         bool fill_data_cache = true);

    StatusOr<CompactionTaskPtr> compact(CompactionTaskContext* context);

    // Durable tablet-metadata layout contract:
    //
    // When file_bundling is disabled:
    // - Version 1 is normally a standard lake protobuf file named with the tablet id and contains
    //   one complete TabletMetadataPB for that tablet. For example, tablet 42 uses
    //   `000000000000002A_0000000000000001.meta` with this content:
    //       [optional protobuf checksum header][serialized TabletMetadataPB(42, version 1)]
    // - Version 2 and later use the same per-tablet filename and standard TabletMetadataPB format.
    //   For example, tablet 42 at version 7 uses
    //   `000000000000002A_0000000000000007.meta` with this content:
    //       [optional protobuf checksum header][serialized TabletMetadataPB(42, version 7)]
    //
    // When file_bundling is enabled for normal table creation and publish:
    // - Version 1 is a single shared initial-metadata file for the physical partition. Its filename
    //   uses tablet id 0, but its payload is still one complete, standard TabletMetadataPB, not a
    //   BundleTabletMetadataPB. The embedded id can identify the tablet that wrote the shared file,
    //   so an id-based read normalizes it to the requested tablet id. The filename and content are:
    //       `0000000000000000_0000000000000001.meta`
    //       [optional protobuf checksum header][serialized writer TabletMetadataPB(version 1)]
    // - Version 2 and later use one bundle file per physical partition and version, also named with
    //   tablet id 0. The file contains serialized per-tablet metadata pages followed by a
    //   BundleTabletMetadataPB footer with page pointers, deduplicated schemas, and checksums. Each
    //   read extracts and returns one tablet's TabletMetadataPB from that bundle. For version 7:
    //       `0000000000000000_0000000000000007.meta`
    //       [TabletMetadataPB(42) page][TabletMetadataPB(43) page]...[BundleTabletMetadataPB]
    //       [optional bundle footer checksum][uint64 bundle footer size and flags]
    //
    // The actual version-1 write switch is TCreateTabletReq::enable_tablet_creation_optimization.
    // For normal table creation FE enables it for a file-bundling table, but a standalone config,
    // legacy data, or a specialized creation path can make the version-1 layout differ from the
    // table's current file_bundling property. Readers must therefore retain both version-1
    // fallbacks. The version-2+ bundled-metadata marker does not change the version-1 file encoding.
    Status put_tablet_metadata(const TabletMetadata& metadata);

    Status put_tablet_metadata(const TabletMetadataPtr& metadata);

    Status cache_tablet_metadata(const TabletMetadataPtr& metadata);

    // Persist every entry of |tablet_metas| into a single bundle metadata file named after the
    // version they all share.
    //
    // |expected_tablet_ids| is the set of tablets the caller was asked to produce metadata for, or
    // empty to skip the coverage check.
    //
    // The bundle is written with CREATE_OR_OPEN_WITH_TRUNCATE, i.e. the file IS the complete
    // metadata for that version: a map short by even one tablet silently yields a version whose
    // metadata is missing that tablet. That is unrecoverable once FE marks the version visible,
    // because every later publish must read exactly that version as its base -- retries can only
    // reproduce the same miss. So refuse to write anything unless the map covers the expected set.
    // Extra tablets beyond that set are tolerated -- only under-coverage loses data.
    //
    // The set is only worth passing when it comes from a source independent of whatever produced
    // |tablet_metas|; derived from the same place it would just compare that source against itself.
    // Callers with no such source pass empty -- which is also what the single-argument overload
    // does, leaving them exactly as they were.
    Status put_bundle_tablet_metadata(std::map<int64_t, TabletMetadataPB>& tablet_metas,
                                      const std::set<int64_t>& expected_tablet_ids);

    Status put_bundle_tablet_metadata(std::map<int64_t, TabletMetadataPB>& tablet_metas) {
        return put_bundle_tablet_metadata(tablet_metas, {});
    }

    // When using get_tablet_metadata to determine whether a new version exists in publish version,
    // a valid expected_gtid must be passed in.
    StatusOr<TabletMetadataPtr> get_tablet_metadata(int64_t tablet_id, int64_t version, bool fill_cache = true,
                                                    int64_t expected_gtid = 0,
                                                    const std::shared_ptr<FileSystem>& fs = nullptr);
    StatusOr<TabletMetadataPtr> get_tablet_metadata(int64_t tablet_id, int64_t version, bool fill_meta_cache,
                                                    bool fill_data_cache, int64_t expected_gtid = 0,
                                                    const std::shared_ptr<FileSystem>& fs = nullptr);
    StatusOr<TabletMetadataPtr> get_tablet_metadata(int64_t tablet_id, int64_t version, const CacheOptions& cache_opts,
                                                    int64_t expected_gtid = 0,
                                                    const std::shared_ptr<FileSystem>& fs = nullptr);

    // Reads the tablet metadata named by |path|, but resolves it through this TabletManager's own
    // LocationProvider: the tablet id comes from the filename, and a bundled partition is then read via
    // bundle_tablet_metadata_location(tablet_id, version). The bundle path, the
    // _bundle_tablet_metadata_group singleflight key, and the metacache key therefore all derive from
    // the local provider, not from |path|.
    //
    // Precondition: |path| must resolve into the same physical partition as that tablet id's metadata
    // root. Paths obtained by listing a metadata root satisfy this, including sibling tablets' files:
    // their virtual roots differ (staros://<shard>/meta) but real_location() maps them to one physical
    // partition. A path under a different physical partition does not qualify -- it would be served the
    // local partition's bundle on a tablet-id collision. Reads against storage this provider does not
    // describe belong in a reader that takes its root explicitly.
    StatusOr<TabletMetadataPtr> get_tablet_metadata(const std::string& path, bool fill_cache = true,
                                                    int64_t expected_gtid = 0,
                                                    const std::shared_ptr<FileSystem>& fs = nullptr);
    StatusOr<TabletMetadataPtr> get_tablet_metadata(const std::string& path, const CacheOptions& cache_opts,
                                                    int64_t expected_gtid = 0,
                                                    const std::shared_ptr<FileSystem>& fs = nullptr);

    StatusOr<TabletMetadataPtr> get_single_tablet_metadata(int64_t tablet_id, int64_t version, bool fill_cache = true,
                                                           int64_t expected_gtid = 0,
                                                           const std::shared_ptr<FileSystem>& fs = nullptr);
    StatusOr<TabletMetadataPtr> get_single_tablet_metadata(int64_t tablet_id, int64_t version,
                                                           const CacheOptions& cache_opts, int64_t expected_gtid = 0,
                                                           const std::shared_ptr<FileSystem>& fs = nullptr);

    static StatusOr<BundleTabletMetadataPtr> parse_bundle_tablet_metadata(const std::string& path,
                                                                          const std::string& serialized_string);

    // A lake tablet's metadata lives in exactly one of two remote locations, never in
    // both: its own metadata object, or a bundle file shared with the other tablets of
    // an aggregated partition. The two `_with_meter` helpers below are the only
    // sanctioned way to read either location; both record NotFound outcomes into
    // lake_tablet_metadata_get_not_found_total. Reading a tablet metadata file through
    // anything else makes that metric silently under-report.
    //
    // Note that txn logs deliberately do NOT go through these: they share the underlying
    // protobuf loader but are not tablet metadata, so they must stay unmetered.
    static Status load_tablet_metadata_file_with_meter(const std::string& path, TabletMetadataPB* metadata,
                                                       bool fill_cache, const std::shared_ptr<FileSystem>& fs);

    static StatusOr<std::string> read_bundle_metadata_file_with_meter(FileSystem* fs, const std::string& path,
                                                                      bool skip_fill_local_cache);

    static StatusOr<TabletMetadataPtrs> get_metas_from_bundle_tablet_metadata(const std::string& location,
                                                                              FileSystem* input_fs = nullptr);

    // NOTICE : latest_cached_tablet_metadata may contain a tablet meta that
    // is either older or newer than the FE visible version.
    TabletMetadataPtr get_latest_cached_tablet_metadata(int64_t tablet_id);

    StatusOr<TabletMetadataIter> list_tablet_metadata(int64_t tablet_id);

    Status delete_tablet_metadata(int64_t tablet_id, int64_t version);

    Status put_txn_log(const TxnLog& log);

    Status put_txn_log(const TxnLogPtr& log);

    Status put_txn_log(const TxnLogPtr& log, const std::string& path);

    Status put_txn_slog(const TxnLogPtr& log);

    Status put_txn_slog(const TxnLogPtr& log, const std::string& path);

    Status put_txn_vlog(const TxnLogPtr& log, int64_t version);

    // |expected_tablet_ids| is the set of tablets this combined txn log must cover. A combined txn
    // log is the only record of those tablets' rowset metadata: publish looks each tablet up inside
    // it and has no per-tablet fallback, so an object written short of an entry leaves the
    // transaction permanently unpublishable once it commits.
    //
    // Mirrors put_bundle_tablet_metadata(): the set is only worth passing when it comes from a
    // source independent of whatever produced |logs| -- deriving it from the collected logs would
    // just compare that source against itself. Callers with no such source pass empty, which is
    // what the single-argument overload does, leaving them exactly as they were.
    Status put_combined_txn_log(const CombinedTxnLogPB& logs, const std::set<int64_t>& expected_tablet_ids);

    Status put_combined_txn_log(const CombinedTxnLogPB& logs) { return put_combined_txn_log(logs, {}); }

    StatusOr<TxnLogPtr> get_txn_log(int64_t tablet_id, int64_t txn_id);

    StatusOr<TxnLogPtr> get_txn_log(int64_t tablet_id, int64_t txn_id, const PUniqueId& load_id);

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

    // Pick a tablet id from `candidates` that is already known to this worker's staros
    // shard cache (so that later location_provider calls can resolve it without issuing
    // a get-shard-info RPC to StarMgr). Falls back to the first candidate when none is
    // local or when USE_STAROS is not enabled. Callers must ensure `candidates` is not
    // empty.
    int64_t pick_local_anchor_tablet_id(const std::vector<int64_t>& candidates);

    Status drop_local_cache(const std::string& path);
    void prune_metacache();

    // TODO: remove this method
    std::shared_ptr<LocationProvider> TEST_set_location_provider(std::shared_ptr<LocationProvider> value) {
        auto ret = _location_provider;
        _location_provider = value;
        return ret;
    }

    std::string tablet_root_location(int64_t tablet_id) const;
    std::string real_tablet_root_location(int64_t tablet_id) const;

    std::string tablet_metadata_root_location(int64_t tablet_id) const;

    // Cache a process-local marker indicating that the tablet's physical partition uses bundled
    // metadata. The marker key uses the resolved storage path so every tablet in the partition
    // consults the same cache entry.
    void cache_bundled_metadata_partition_marker(int64_t tablet_id);

    // Check only the process-local metacache for the bundled-metadata marker. This does not inspect
    // the local data cache or remote storage.
    bool lookup_cached_bundled_metadata_partition_marker(int64_t tablet_id);

    std::string tablet_metadata_location(int64_t tablet_id, int64_t version) const;

    std::string tablet_initial_metadata_location(int64_t tablet_id) const;

    std::string bundle_tablet_metadata_location(int64_t tablet_id, int64_t version) const;

    std::string txn_log_location(int64_t tablet_id, int64_t txn_id) const;

    std::string txn_log_location(int64_t tablet_id, int64_t txn_id, const PUniqueId& load_id) const;

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

    TableSchemaService* table_schema_service() { return _table_schema_service.get(); }

    // The return value will never be null.
    Metacache* metacache() { return _metacache.get(); }

    StatusOr<int64_t> get_tablet_data_size(int64_t tablet_id, int64_t* version_hint);

    StatusOr<int64_t> get_tablet_num_rows(int64_t tablet_id, int64_t version);

    int64_t in_writing_data_size(int64_t tablet_id);

    int64_t add_in_writing_data_size(int64_t tablet_id, int64_t size);

    void clean_in_writing_data_size();

    // only for TEST purpose
    void TEST_set_global_schema_cache(int64_t index_id, TabletSchemaPtr schema);

    // Update the cache size of the segment with the given key. The update is applied only when the
    // key still maps to `segment`, so that a cache entry replaced by a different instance in the
    // meantime is not charged with this segment's memory cost.
    void update_segment_cache_size(std::string_view key, size_t mem_cost, const Segment* segment);

    StatusOr<SegmentPtr> load_segment(const FileInfo& segment_info, int segment_id, size_t* footer_size_hint,
                                      const LakeIOOptions& lake_io_opts, bool fill_meta_cache,
                                      TabletSchemaPtr tablet_schema);
    // for load segment parallel
    StatusOr<SegmentPtr> load_segment(const FileInfo& segment_info, int segment_id, const LakeIOOptions& lake_io_opts,
                                      bool fill_meta_cache, TabletSchemaPtr tablet_schema);

    StatusOr<TabletSchemaPtr> get_tablet_schema(int64_t tablet_id, int64_t* version_hint = nullptr);

    Status create_schema_file(int64_t tablet_id, const TabletSchemaPB& schema_pb);
    StatusOr<TabletAndRowsets> capture_tablet_and_rowsets(int64_t tablet_id, int64_t from_version, int64_t to_version);

    int64_t get_average_row_size_from_latest_metadata(int64_t tablet_id);

    void get_tablets_basic_info(int64_t table_id, int64_t partition_id, int64_t tablet_id,
                                const std::set<int64_t>& authorized_table_ids,
                                const std::unordered_map<int64_t, int64_t>& partition_versions,
                                std::vector<TabletBasicInfo>& tablet_infos);

    void stop();

    // Cache the schema into the metadata cache.
    void cache_schema(const TabletSchemaPtr& schema);

    // Get the schema from the metadata cache.
    // Return nullptr if not found.
    TabletSchemaPtr get_cached_schema(int64_t schema_id);

private:
    static std::string global_schema_cache_key(int64_t index_id);
    static std::string tablet_schema_cache_key(int64_t tablet_id);
    static std::string tablet_latest_metadata_cache_key(int64_t tablet_id);

    StatusOr<TabletSchemaPtr> load_and_parse_schema_file(const std::string& path);
    StatusOr<TabletSchemaPtr> get_tablet_schema_by_id(int64_t tablet_id, int64_t schema_id);

    Status put_tablet_metadata(const TabletMetadataPtr& metadata, const std::string& metadata_location);
    StatusOr<TabletMetadataPtr> load_tablet_metadata(const std::string& metadata_location, bool fill_data_cache,
                                                     int64_t expected_gtid, const std::shared_ptr<FileSystem>& fs);
    StatusOr<TxnLogPtr> load_txn_log(const std::string& txn_log_location, bool fill_cache);
    StatusOr<CombinedTxnLogPtr> load_combined_txn_log(const std::string& path, bool fill_cache);
    Status corrupted_tablet_meta_handler(const Status& s, const std::string& metadata_location);

#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
    StatusOr<TabletBasicInfo> get_tablet_basic_info(int64_t tablet_id, int64_t table_id, int64_t partition_id,
                                                    const std::set<int64_t>& authorized_table_ids,
                                                    const std::unordered_map<int64_t, int64_t>& partition_versions);
#endif // USE_STAROS

private:
    std::shared_ptr<LocationProvider> _location_provider;
    std::unique_ptr<Metacache> _metacache;
    std::unique_ptr<CompactionScheduler> _compaction_scheduler;
    UpdateManager* _update_mgr = nullptr;
    std::unique_ptr<TableSchemaService> _table_schema_service;

    std::shared_mutex _meta_lock;
    std::unordered_map<int64_t, int64_t> _tablet_in_writing_size;

    bthreads::singleflight::Group<std::string, StatusOr<TabletSchemaPtr>> _schema_group;
    bthreads::singleflight::Group<std::string, StatusOr<CombinedTxnLogPtr>> _combined_txn_log_group;
    bthreads::singleflight::Group<std::string, StatusOr<std::string>> _bundle_tablet_metadata_group;
};

} // namespace starrocks::lake
