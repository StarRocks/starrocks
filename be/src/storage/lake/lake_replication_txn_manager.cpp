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

#include "storage/lake/lake_replication_txn_manager.h"

#include <atomic>
#include <mutex>
#include <optional>
#include <unordered_set>

#include "base/coding.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "cache/dynamic_cache.h"
#include "common/config_lake_fwd.h"
#include "common/config_rowset_fwd.h"
#include "common/storage_define.h"
#include "common/system/master_info.h"
#include "common/thread/threadpool.h"
#include "compute_env/staros/starlet_filesystem.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "gen_cpp/lake_types.pb.h"
#include "persistent_index_sstable.h"
#include "platform/key_cache.h"
#include "replication_txn_manager.h"
#include "storage/del_file_stream_converter.h"
#include "storage/lake/filenames.h"
#include "storage/lake/join_path.h"
#include "storage/lake/lake_proto_normalizer.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/protobuf_file.h"
#include "storage/segment_stream_converter.h"
#include "storage/tablet_schema.h"
#include "storage/utils.h"
#include "storage_primitive/primary_key_encoding_types.h"
#include "types/logical_type.h"
#include "vacuum.h"

namespace starrocks::lake {
namespace {
std::unordered_set<std::string> collect_shared_file_names(const TabletMetadataPB& metadata) {
    std::unordered_set<std::string> files;
    for (const auto& rowset : metadata.rowsets()) {
        for (const auto& segment : rowset.segment_metas()) {
            if (segment.shared()) {
                files.emplace(segment.filename());
            }
        }
        for (const auto& del : rowset.del_files()) {
            if (del.shared()) {
                files.emplace(del.name());
            }
        }
    }
    for (const auto& sstable : metadata.sstable_meta().sstables()) {
        if (sstable.shared()) {
            files.emplace(sstable.filename());
        }
    }
    for (const auto& [_, file] : metadata.delvec_meta().version_to_file()) {
        if (file.shared()) {
            files.emplace(file.name());
        }
    }
    for (const auto& [_, dcg] : metadata.dcg_meta().dcgs()) {
        for (int i = 0; i < dcg.column_files_size(); ++i) {
            if (i < dcg.shared_files_size() && dcg.shared_files(i)) {
                files.emplace(dcg.column_files(i));
            }
        }
    }
    for (const auto& [_, idg] : metadata.idg_meta().idgs()) {
        for (const auto& entry : idg.entries()) {
            if (entry.shared_file() && entry.has_index_file() && !entry.index_file().empty()) {
                files.emplace(entry.index_file());
            }
        }
    }
    return files;
}

StatusOr<std::optional<size_t>> get_existing_file_size(const std::string& path) {
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(path));
    auto size = fs->get_file_size(path);
    std::optional<size_t> existing_size;
    if (size.ok()) {
        existing_size = static_cast<size_t>(*size);
    } else if (!size.status().is_not_found()) {
        return size.status();
    }
    TEST_SYNC_POINT_CALLBACK("LakeReplicationTxnManager::get_existing_file_size", &existing_size);
    return existing_size;
}

void remove_cleanup_file(std::vector<std::string>* files_to_delete, const std::string& path, std::mutex* mutex) {
    if (mutex != nullptr) {
        std::lock_guard lock(*mutex);
        std::erase(*files_to_delete, path);
    } else {
        std::erase(*files_to_delete, path);
    }
}

StatusOr<TabletMetadataPtr> load_source_standalone_tablet_metadata(const std::string& metadata_path,
                                                                   const std::shared_ptr<FileSystem>& source_fs) {
    RandomAccessFileOptions opts{.skip_fill_local_cache = true, .skip_disk_cache = true};
    ASSIGN_OR_RETURN(auto input_file, source_fs->new_random_access_file(opts, metadata_path));
    ASSIGN_OR_RETURN(auto content, input_file->read_all());

    auto metadata = std::make_shared<TabletMetadataPB>();
    RETURN_IF_ERROR(ProtobufFileWithHeader::load_from_buffer(metadata.get(), content, LAKE_META_HEADER_MAGIC_NUMBER,
                                                             /*allow_plain_protobuf_fallback=*/true));
    normalize_tablet_metadata_after_load(metadata.get());
    return metadata;
}

StatusOr<TabletMetadataPtr> load_source_bundle_tablet_metadata(int64_t tablet_id, int64_t version,
                                                               const std::string& meta_dir,
                                                               const std::shared_ptr<FileSystem>& source_fs) {
    const auto bundle_path = join_path(meta_dir, tablet_metadata_filename(0, version));
    RandomAccessFileOptions opts{.skip_fill_local_cache = true, .skip_disk_cache = true};
    ASSIGN_OR_RETURN(auto input_file, source_fs->new_random_access_file(opts, bundle_path));
    ASSIGN_OR_RETURN(auto file_size, input_file->get_size());

    constexpr size_t kSizeFieldSize = sizeof(uint64_t);
    if (file_size < kSizeFieldSize) {
        return Status::Corruption(
                fmt::format("Source metadata bundle {} is too small: {} bytes", bundle_path, file_size));
    }

    std::string size_field(kSizeFieldSize, '\0');
    RETURN_IF_ERROR(input_file->read_at_fully(file_size - kSizeFieldSize, size_field.data(), size_field.size()));
    const uint64_t raw_bundle_metadata_size = decode_fixed64_le(reinterpret_cast<const uint8_t*>(size_field.data()));
    const bool checksummed = (raw_bundle_metadata_size & LAKE_BUNDLE_META_CHECKSUM_FLAG) != 0;
    const uint64_t bundle_metadata_size = raw_bundle_metadata_size & ~LAKE_BUNDLE_META_CHECKSUM_FLAG;
    const size_t footer_suffix_size = kSizeFieldSize + (checksummed ? sizeof(uint32_t) : 0);
    if (file_size < footer_suffix_size || bundle_metadata_size == 0 ||
        bundle_metadata_size > static_cast<uint64_t>(file_size - footer_suffix_size)) {
        return Status::Corruption(
                fmt::format("Invalid source metadata bundle footer in {}, file_size={}, "
                            "bundle_metadata_size={}",
                            bundle_path, file_size, bundle_metadata_size));
    }

    const uint64_t bundle_metadata_offset = file_size - footer_suffix_size - bundle_metadata_size;
    std::string footer(bundle_metadata_size + footer_suffix_size, '\0');
    RETURN_IF_ERROR(input_file->read_at_fully(bundle_metadata_offset, footer.data(), footer.size()));
    ASSIGN_OR_RETURN(auto bundle, TabletManager::parse_bundle_tablet_metadata(bundle_path, footer));

    auto page_it = bundle->tablet_meta_pages().find(tablet_id);
    if (page_it == bundle->tablet_meta_pages().end()) {
        return Status::NotFound(
                fmt::format("Tablet {} is absent from source metadata bundle {}", tablet_id, bundle_path));
    }
    const uint64_t offset = page_it->second.offset();
    const uint32_t size = page_it->second.size();
    if (offset > bundle_metadata_offset || size > bundle_metadata_offset - offset) {
        return Status::Corruption(fmt::format("Invalid source tablet metadata page in {}, offset={}, size={}",
                                              bundle_path, offset, size));
    }

    std::string page(size, '\0');
    RETURN_IF_ERROR(input_file->read_at_fully(offset, page.data(), page.size()));
    auto checksum_it = bundle->tablet_meta_page_checksum().find(tablet_id);
    if (checksum_it != bundle->tablet_meta_page_checksum().end() &&
        olap_adler32(ADLER32_INIT, page.data(), page.size()) != checksum_it->second) {
        return Status::Corruption(
                fmt::format("Mismatched checksum for tablet {} metadata in {}", tablet_id, bundle_path));
    }

    auto metadata = std::make_shared<TabletMetadataPB>();
    if (!metadata->ParseFromArray(page.data(), page.size())) {
        return Status::Corruption(fmt::format("Failed to parse tablet {} metadata from {}", tablet_id, bundle_path));
    }
    if (metadata->id() != tablet_id) {
        return Status::Corruption(fmt::format("Tablet ID mismatch in {}, expected={}, actual={}", bundle_path,
                                              tablet_id, metadata->id()));
    }
    normalize_tablet_metadata_after_load(metadata.get());

    auto schema_id_it = bundle->tablet_to_schema().find(tablet_id);
    if (schema_id_it == bundle->tablet_to_schema().end()) {
        return Status::Corruption(
                fmt::format("Schema mapping for tablet {} is absent from {}", tablet_id, bundle_path));
    }
    auto schema_it = bundle->schemas().find(schema_id_it->second);
    if (schema_it == bundle->schemas().end()) {
        return Status::Corruption(
                fmt::format("Schema {} for tablet {} is absent from {}", schema_id_it->second, tablet_id, bundle_path));
    }
    metadata->mutable_schema()->CopyFrom(schema_it->second);
    (*metadata->mutable_historical_schemas())[schema_id_it->second].CopyFrom(schema_it->second);
    force_cloud_native_pk_persistent_index(metadata.get());

    for (const auto& [_, historical_schema_id] : metadata->rowset_to_schema()) {
        auto historical_schema_it = bundle->schemas().find(historical_schema_id);
        if (historical_schema_it == bundle->schemas().end()) {
            return Status::Corruption(fmt::format("Historical schema {} for tablet {} is absent from {}",
                                                  historical_schema_id, tablet_id, bundle_path));
        }
        (*metadata->mutable_historical_schemas())[historical_schema_id].CopyFrom(historical_schema_it->second);
    }
    return metadata;
}
} // namespace

#ifdef USE_STAROS
std::string convert_s3_path_to_starlet_uri(std::string_view s3_path, int64_t shard_id) {
    // S3 URI format: s3://bucket/path...
    // Starlet URI format: staros://shard_id/path...
    // We need to replace "s3://bucket/" with "staros://shard_id/"
    std::string_view path = s3_path;
    if (path.find("s3://") == 0) {
        // Remove "s3://" prefix
        path.remove_prefix(5);
        // Find the first "/" after bucket name and skip the bucket part
        size_t first_slash_pos = path.find('/');
        if (first_slash_pos != std::string_view::npos) {
            path.remove_prefix(first_slash_pos + 1);
        } else {
            path = std::string_view();
        }
        // Build starlet URI: staros://shard_id/path...
        return build_starlet_uri(shard_id, path);
    }

    // Not a valid S3 path - log warning
    LOG(WARNING) << "S3 path does not start with 's3://': " << s3_path;
    return build_starlet_uri(shard_id, path);
}
#endif // USE_STAROS

std::string remove_last_path_component(const std::string& path) {
    // Find the last "/" which separates the directory name (meta or data)
    size_t last_slash = path.find_last_of('/');
    if (last_slash == std::string::npos) {
        return path;
    }

    // Get the directory name (meta or data)
    std::string dir_name = path.substr(last_slash + 1);

    // Get the path before the directory name
    std::string base_path = path.substr(0, last_slash);

    // Find the second-to-last "/"
    size_t second_last_slash = base_path.find_last_of('/');
    if (second_last_slash == std::string::npos) {
        return path;
    }

    // Remove the component between second_last_slash and last_slash
    return base_path.substr(0, second_last_slash + 1) + dir_name;
}

std::string remove_db_id_component(const std::string& path, int64_t db_id) {
    std::string db_pattern = "/db" + std::to_string(db_id) + "/";
    size_t pos = path.find(db_pattern);
    if (pos == std::string::npos) {
        return path;
    }
    // Remove "db{db_id}/" but keep the "/" before it
    return path.substr(0, pos + 1) + path.substr(pos + db_pattern.length());
}

Status LakeReplicationTxnManager::replicate_lake_remote_storage(const TReplicateSnapshotRequest& request,
                                                                ThreadPool* replicate_file_thread_pool) {
    auto src_tablet_id = request.src_tablet_id;
    auto src_visible_version = request.src_visible_version;
    auto src_db_id = request.src_db_id;
    auto src_table_id = request.src_table_id;
    auto src_partition_id = request.src_partition_id;

    auto data_version = request.data_version;
    auto target_visible_version = request.visible_version;
    auto target_tablet_id = request.tablet_id;

    auto txn_id = request.transaction_id;
    auto virtual_tablet_id = request.virtual_tablet_id;

    // Check if FE provides full path for S3 storage type
    // - has_full_path=true: S3 storage type, FE provides full S3 path (supports partitioned prefix)
    // - has_full_path=false: Non-S3 storage type (OSS/Azure/HDFS/GFS), use RemoteStarletLocationProvider
    bool has_full_path = request.__isset.src_partition_full_path && !request.src_partition_full_path.empty();
    std::string src_partition_full_path = has_full_path ? request.src_partition_full_path : "";

    LOG(INFO) << "Start to replicate lake remote storage, txn_id: " << txn_id << ", tablet_id: " << target_tablet_id
              << ", src_tablet_id: " << src_tablet_id << ", src_db_id: " << src_db_id
              << ", src_table_id: " << src_table_id << ", src_partition_id: " << src_partition_id
              << ", visible_version: " << target_visible_version << ", data_version: " << data_version
              << ", virtual_tablet_id: " << virtual_tablet_id << ", src_visible_version: " << src_visible_version
              << ", has_full_path: " << has_full_path
              << (has_full_path ? ", src_partition_full_path: " + src_partition_full_path : "");

    // step 1: validate request and locate source tablet metadata/files.
    std::vector<Version> missed_versions;
    for (auto v = data_version + 1; v <= src_visible_version; ++v) {
        missed_versions.emplace_back(v, v);
    }
    if (UNLIKELY(missed_versions.empty())) {
        LOG(WARNING) << "Replicate lake remote storage skipped, no missing version"
                     << ", txn_id: " << txn_id << ", tablet_id: " << target_tablet_id
                     << ", src_tablet_id: " << src_tablet_id << ", visible_version: " << target_visible_version
                     << ", data_version: " << data_version << ", src_visible_version: " << src_visible_version;
        return Status::Corruption("No missing version");
    }

    std::string src_meta_dir;
    std::string src_data_dir;
    std::shared_ptr<FileSystem> shared_src_fs;
    TabletMetadataPtr src_tablet_meta;

#ifdef USE_STAROS
    if (has_full_path) {
        // S3 storage type: FE provides full S3 path (supports partitioned prefix feature)
        // Use S3 raw path mode - starlet will use the path as-is without normalize_path
        if (src_partition_full_path.find("s3://") != 0) {
            return Status::InvalidArgument(
                    fmt::format("Full path must be S3 type (start with 's3://'), got: {}", src_partition_full_path));
        }
        std::string src_partition_starlet_uri =
                convert_s3_path_to_starlet_uri(src_partition_full_path, virtual_tablet_id);
        TEST_SYNC_POINT_CALLBACK("LakeReplicationTxnManager::src_partition_starlet_uri", &src_partition_starlet_uri);

        // Append metadata and segment directory names
        src_meta_dir = join_path(src_partition_starlet_uri, kMetadataDirectoryName);
        src_data_dir = join_path(src_partition_starlet_uri, kSegmentDirectoryName);

        VLOG(3) << "S3 storage: converted S3 full path to starlet URI, original: " << src_partition_full_path
                << ", starlet_uri: " << src_partition_starlet_uri << ", meta_dir: " << src_meta_dir
                << ", data_dir: " << src_data_dir;

        // Create filesystem with S3 raw path mode enabled
        shared_src_fs = new_fs_starlet(virtual_tablet_id, true /* use_raw_path */);
        if (shared_src_fs == nullptr) {
            return Status::Corruption("Failed to create virtual starlet filesystem");
        }

        ASSIGN_OR_RETURN(src_tablet_meta,
                         try_build_source_tablet_meta_with_fallback(src_tablet_id, src_visible_version, src_db_id,
                                                                    txn_id, src_meta_dir, src_data_dir, shared_src_fs));
    } else {
        // Non-S3 storage type (OSS/Azure/HDFS/GFS): use RemoteStarletLocationProvider
        // Use normal mode - starlet will use normalize_path to combine sys.root with relative path
        src_meta_dir = _remote_location_provider->metadata_root_location(virtual_tablet_id, src_db_id, src_table_id,
                                                                         src_partition_id);
        src_data_dir = _remote_location_provider->segment_root_location(virtual_tablet_id, src_db_id, src_table_id,
                                                                        src_partition_id);
        TEST_SYNC_POINT_CALLBACK("LakeReplicationTxnManager::src_meta_dir", &src_meta_dir);

        LOG(INFO) << "Non-S3 storage: using RemoteStarletLocationProvider, meta_dir: " << src_meta_dir
                  << ", data_dir: " << src_data_dir;

        // Create filesystem with normal mode (no S3 raw path)
        shared_src_fs = new_fs_starlet(virtual_tablet_id, false /* use_raw_path */);
        if (shared_src_fs == nullptr) {
            return Status::Corruption("Failed to create virtual starlet filesystem");
        }
        ASSIGN_OR_RETURN(src_tablet_meta,
                         build_source_tablet_meta(src_tablet_id, src_visible_version, src_meta_dir, shared_src_fs));
    }
#else
    return Status::NotSupported("Lake replication remote storage requires build with shared-data support!");
#endif

    VLOG(3) << "Lake replicate storage task, built source meta and data dir, meta dir: " << src_meta_dir
            << ", data dir: " << src_data_dir << ", txn_id: " << txn_id << ", src_tablet_id: " << src_tablet_id
            << ", tablet_id: " << target_tablet_id;

    // step 2: build target metadata and file mappings.

    // `file_locations` is the mapping between source and target file locations,
    // it contains all files that need to replicate from source to target storage
    std::map<std::string, std::string> file_locations;
    // `filename_map` is another mapping between source and target file name,
    // and it's borrowed from lake::ReplicationTxnManager
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;
    SourceEncryptionMetaMap source_encryption_metas;
    // `segment_name_to_size_map` is the mapping between segment file name to its file size
    // we use the `segment_size` field in rowset metadata to get the file size.
    // for history reasons, the `segment_size` field is not always present, so the resulting map is not guaranteed to
    // have all segment file sizes.
    std::unordered_map<std::string, size_t> segment_name_to_size_map;

    auto txn_log = std::make_shared<TxnLog>();

    ASSIGN_OR_RETURN(auto target_tablet, _tablet_manager->get_tablet(target_tablet_id));
    ASSIGN_OR_RETURN(auto target_tablet_meta, target_tablet.get_metadata(target_visible_version));
    if (!src_tablet_meta->has_schema()) {
        LOG(WARNING) << "Failed to get source schema, source tablet: " << src_tablet_id
                     << ", target tablet: " << target_tablet_id;
        return Status::Corruption("Failed to get source schema");
    }
    const TabletSchemaPB& source_schema_pb = src_tablet_meta->schema();
    std::unordered_set<std::string> bundled_segment_names;
    for (const auto& rowset : src_tablet_meta->rowsets()) {
        for (const auto& segment : rowset.segment_metas()) {
            if (!segment.has_bundle_file_offset()) {
                continue;
            }
            bundled_segment_names.emplace(segment.filename());
        }
    }
    // Copy the rowsets, sstables etc. into tablet metadata on target cluster,
    // then replace file names and return `copied_target_tablet_meta` as the final target tablet metadata
    ASSIGN_OR_RETURN(
            auto copied_target_tablet_meta,
            convert_and_build_new_tablet_meta(src_tablet_meta, target_tablet_meta, src_tablet_id, target_tablet_id,
                                              txn_id, data_version, src_data_dir, segment_name_to_size_map,
                                              file_locations, filename_map, source_encryption_metas));
    SourceEncryptionInfoMap source_encryption_infos;
    source_encryption_infos.reserve(source_encryption_metas.size());
    for (const auto& [filename, encryption_meta] : source_encryption_metas) {
        if (encryption_meta.empty()) {
            continue;
        }
        ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta_without_cache(encryption_meta));
        source_encryption_infos.emplace(filename, std::move(info));
    }
    std::unordered_set<std::string> shared_file_names;
    if (src_tablet_meta->has_range() && target_tablet_meta->has_range()) {
        // Aligned range children retain the source shared-file flags and resolve to the same
        // partition data path, so their per-tablet replication tasks may target the same object.
        shared_file_names = collect_shared_file_names(*src_tablet_meta);
    }
    // calc column unique id to adapt for fast schema change
    std::unordered_map<uint32_t, uint32_t> column_unique_id_map;
    ReplicationUtils::calc_column_unique_id_map(source_schema_pb.column(), target_tablet_meta->schema().column(),
                                                &column_unique_id_map);

    if (column_unique_id_map.size() > 0) {
        LOG(INFO) << "Lake replicate storage task, need rebuild column unique id, txn_id: " << txn_id
                  << ", tablet_id: " << target_tablet_id << ", unique_id_map size: " << column_unique_id_map.size();
        if (!bundled_segment_names.empty()) {
            return Status::NotSupported(
                    "Fast schema conversion of bundled segments is not supported in lake replication");
        }
    }
    std::vector<std::string> files_to_delete;
    CancelableDefer clean_files([&files_to_delete]() { lake::delete_files_async(std::move(files_to_delete)); });

    // Compute PK encoding transcode context for .del files. prepare_del_transcode_context
    // validates PK column count/type match and rejects V2→V1 on byte-incompatible PK shapes.
    // For V1→V2, it returns the transcode context so build_file_converters can wire up
    // DelFileStreamConverter for .del files.
    ASSIGN_OR_RETURN(auto del_transcode_ctx,
                     lake::ReplicationTxnManager::prepare_del_transcode_context(*target_tablet_meta, source_schema_pb));

    auto file_converters = lake::ReplicationTxnManager::build_file_converters(
            _tablet_manager, request, filename_map, column_unique_id_map, files_to_delete,
            del_transcode_ctx.pkey_schema, del_transcode_ctx.source_encoding, del_transcode_ctx.target_encoding);

    // Track which segments have size changes
    std::unordered_map<std::string, size_t> segment_size_changes;

    // step 3: prepare copy mode and build per-file copy tasks.
    MonotonicStopWatch watch;
    watch.start();
    std::atomic<size_t> total_file_size{0};

    ThreadPool* repl_pool = replicate_file_thread_pool;
    bool use_parallel = should_use_parallel_copy(filename_map.size(), repl_pool);
    bool use_file_copy_pool = repl_pool != nullptr && repl_pool->max_threads() > 0 &&
                              config::lake_replication_parallel_copy_min_file_count > 0;
    size_t worker_count = 1;
    if (use_parallel) {
        worker_count = std::min<size_t>(filename_map.size(),
                                        std::max(1, config::lake_replication_max_parallel_files_per_tablet));
    }
    std::mutex mu;
    std::mutex* shared_mutex = nullptr;
    FileConverterCreatorFunc active_file_converters = file_converters;
    if (use_parallel) {
        LOG(INFO) << "Start parallel file copy, file_count: " << filename_map.size() << ", txn_id: " << txn_id
                  << ", tablet_id: " << target_tablet_id << ", worker_count: " << worker_count
                  << ", pool max_threads: " << repl_pool->max_threads()
                  << ", pool num_threads: " << repl_pool->num_threads()
                  << ", pool active_threads: " << repl_pool->active_threads()
                  << ", pool queued_tasks: " << repl_pool->num_queued_tasks();
        shared_mutex = &mu;
        active_file_converters = [&file_converters, &mu](
                                         const std::string& file_name,
                                         uint64_t file_size) -> StatusOr<std::unique_ptr<FileStreamConverter>> {
            std::lock_guard lock(mu);
            return file_converters(file_name, file_size);
        };
    }

    std::vector<ReplicationTask> tasks;
    tasks.reserve(filename_map.size());
    for (const auto& pair : filename_map) {
        const auto& src_file_name = pair.first;
        auto src_file_location = join_path(src_data_dir, src_file_name);
        auto it = file_locations.find(src_file_location);
        if (it == file_locations.end()) {
            return Status::Corruption("Found invalid file location, src file location: " + src_file_location);
        }
        const auto& target_file_location = it->second;
        size_t src_file_size = 0;
        auto size_it = segment_name_to_size_map.find(src_file_name);
        if (size_it != segment_name_to_size_map.end()) {
            src_file_size = size_it->second;
        }
        bool is_seg = is_segment(src_file_name);
        bool is_bundled_segment = is_seg && bundled_segment_names.contains(src_file_name);
        bool is_shared_file = is_bundled_segment || shared_file_names.contains(src_file_name);
        // Segments and .del files go through download_lake_file_with_converter + file_converters,
        // which routes .del files through DelFileStreamConverter when V1→V2 transcoding is needed.
        bool use_converter = is_seg || is_del(src_file_name);
        const auto& target_file_name = pair.second.first;
        FileEncryptionInfo target_encryption_info;
        if (config::enable_transparent_data_encryption) {
            target_encryption_info = pair.second.second.info;
        }
        FileEncryptionInfo source_encryption_info;
        auto source_encryption_it = source_encryption_infos.find(src_file_name);
        if (source_encryption_it != source_encryption_infos.end()) {
            source_encryption_info = source_encryption_it->second;
        }

        tasks.emplace_back([&, src_file_name, src_file_location, target_file_location, target_file_name, src_file_size,
                            is_seg, is_bundled_segment, is_shared_file, use_converter, target_encryption_info,
                            source_encryption_info]() -> Status {
            // Fast cancel: check right before each file copy starts.
            if (txn_id < get_master_info().min_active_txn_id) {
                LOG(WARNING) << "Lake replication task cancelled before file copy, transaction is aborted"
                             << ", txn_id: " << txn_id << ", tablet_id: " << target_tablet_id
                             << ", min_active_txn_id: " << get_master_info().min_active_txn_id
                             << ", src_file: " << src_file_name;
                return Status::Aborted("Lake replication cancelled, transaction is aborted");
            }
            TEST_SYNC_POINT_CALLBACK("LakeReplicationTxnManager::replicate_lake_remote_storage::before_copy", nullptr);

            LOG(INFO) << "Start replicate src file: " << src_file_location << ", target: " << target_file_location
                      << ", txn_id: " << txn_id << ", tablet_id: " << target_tablet_id;

            size_t final_file_size = 0;
            bool copy_needed = true;
            auto start_ts = butil::gettimeofday_us();
            if (is_shared_file) {
                // Reuse a sibling's completed copy. Remote shared-data filesystems expose the
                // final path after close/rename, rather than exposing an in-progress multipart or
                // temporary file at this path.
                ASSIGN_OR_RETURN(auto existing_size, get_existing_file_size(target_file_location));
                if (existing_size.has_value()) {
                    final_file_size = *existing_size;
                    copy_needed = false;
                    LOG(INFO) << "Skip copying an existing shared physical file, src: " << src_file_location
                              << ", target: " << target_file_location << ", txn_id: " << txn_id
                              << ", tablet_id: " << target_tablet_id << ", size: " << final_file_size;
                }
            }
            if (copy_needed && use_converter) {
                TEST_SYNC_POINT_CALLBACK("LakeReplicationTxnManager::replicate_task::download_segment",
                                         &final_file_size);
                if (final_file_size == 0) {
                    RandomAccessFileOptions source_opts{.encryption_info = source_encryption_info};
                    auto copy_status = ReplicationUtils::download_lake_file_with_converter(
                            src_file_location, src_file_name, src_file_size, shared_src_fs, source_opts,
                            active_file_converters, &final_file_size);
                    if (is_shared_file) {
                        remove_cleanup_file(&files_to_delete, target_file_location, shared_mutex);
                        if (copy_status.is_already_exist()) {
                            ASSIGN_OR_RETURN(auto existing_size, get_existing_file_size(target_file_location));
                            if (!existing_size.has_value()) {
                                return Status::Corruption("Shared physical file disappeared after concurrent copy: " +
                                                          target_file_location);
                            }
                            final_file_size = *existing_size;
                            copy_status = Status::OK();
                        }
                    }
                    RETURN_IF_ERROR(copy_status);
                }
                if (is_seg && !is_bundled_segment && final_file_size > 0 && final_file_size != src_file_size) {
                    if (shared_mutex != nullptr) {
                        std::lock_guard lock(*shared_mutex);
                        segment_size_changes[target_file_name] = final_file_size;
                    } else {
                        segment_size_changes[target_file_name] = final_file_size;
                    }
                    LOG(INFO) << "Segment file size changed after conversion, src_file: " << src_file_name
                              << ", target_file: " << target_file_name << ", original size: " << src_file_size
                              << ", final size: " << final_file_size;
                }
            } else if (copy_needed) {
                WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
                if (config::enable_transparent_data_encryption) {
                    opts.encryption_info = target_encryption_info;
                }
                int max_retry = std::max(1, config::lake_replication_max_file_copy_retry);
                TEST_SYNC_POINT_CALLBACK("LakeReplicationTxnManager::replicate_task::copy_non_segment",
                                         &final_file_size);
                if (final_file_size == 0) {
                    if (!is_shared_file) {
                        if (shared_mutex != nullptr) {
                            std::lock_guard lock(*shared_mutex);
                            files_to_delete.push_back(target_file_location);
                        } else {
                            files_to_delete.push_back(target_file_location);
                        }
                    }
                    SequentialFileOptions source_opts{.encryption_info = source_encryption_info};
                    auto copy_result = copy_non_segment_file_with_retry(src_file_location, shared_src_fs, source_opts,
                                                                        target_file_location, opts, max_retry);
                    if (!copy_result.ok() && is_shared_file && copy_result.status().is_already_exist()) {
                        ASSIGN_OR_RETURN(auto existing_size, get_existing_file_size(target_file_location));
                        if (!existing_size.has_value()) {
                            return Status::Corruption("Shared physical file disappeared after concurrent copy: " +
                                                      target_file_location);
                        }
                        final_file_size = *existing_size;
                    } else {
                        ASSIGN_OR_RETURN(final_file_size, std::move(copy_result));
                    }
                }
            }

            total_file_size.fetch_add(final_file_size, std::memory_order_relaxed);
            auto cost = butil::gettimeofday_us() - start_ts;
            auto is_slow = cost >= config::lake_replication_slow_log_ms * 1000;
            if (is_slow) {
                LOG(INFO) << "Finished replicate src file: " << src_file_location
                          << ", target: " << target_file_location << ", txn_id: " << txn_id
                          << ", tablet_id: " << target_tablet_id << ", size: " << final_file_size
                          << ", cost(s): " << cost / 1000. / 1000.;
            }
            return Status::OK();
        });
    }
    // step 4: execute tasks and collect copy metrics.
    if (use_file_copy_pool) {
        auto st = execute_file_copy_tasks(std::move(tasks), repl_pool, worker_count);
        if (!st.ok()) {
            LOG(WARNING) << "File copy through dedicated pool failed, txn_id: " << txn_id
                         << ", tablet_id: " << target_tablet_id << ", worker_count: " << worker_count
                         << ", pool max_threads: " << repl_pool->max_threads()
                         << ", pool num_threads: " << repl_pool->num_threads()
                         << ", pool active_threads: " << repl_pool->active_threads()
                         << ", pool queued_tasks: " << repl_pool->num_queued_tasks() << ", error: " << st;
            return st;
        }
    } else {
        for (const auto& task : tasks) {
            RETURN_IF_ERROR(task());
        }
    }
    double total_time_sec = watch.elapsed_time() / 1000. / 1000. / 1000.;
    double copy_rate = 0.0;
    if (total_time_sec > 0) {
        copy_rate = (total_file_size / 1024. / 1024.) / total_time_sec;
    }
    LOG(INFO) << "Replicated tablet file count: " << filename_map.size() << ", total bytes: " << total_file_size
              << ", cost: " << total_time_sec << "s, rate: " << copy_rate
              << "MB/s, file_copy_pool: " << (use_file_copy_pool ? "true" : "false")
              << ", parallel: " << (use_parallel ? "true" : "false") << ", worker_count: " << worker_count
              << ", txn_id: " << txn_id << ", tablet_id: " << target_tablet_id;

    // step 5: update metadata and write txn log.
    // Update segment sizes in tablet_metadata if there are any changes
    if (!segment_size_changes.empty()) {
        RETURN_IF_ERROR(update_tablet_metadata_segment_sizes(copied_target_tablet_meta, segment_size_changes));
    }
    txn_log->mutable_op_replication()->mutable_tablet_metadata()->CopyFrom(*copied_target_tablet_meta);

    // write txn log
    txn_log->set_tablet_id(target_tablet_id);
    txn_log->set_txn_id(txn_id);

    auto* txn_meta = txn_log->mutable_op_replication()->mutable_txn_meta();
    txn_meta->set_tablet_id(target_tablet_id);
    txn_meta->set_txn_id(txn_id);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_visible_version(target_visible_version);
    txn_meta->set_data_version(data_version);
    txn_meta->set_snapshot_version(src_visible_version);
    // mark full replication for shared-data cluster migration
    txn_meta->set_incremental_snapshot(false);

    RETURN_IF_ERROR(_tablet_manager->put_txn_log(txn_log));

    VLOG(3) << "Replicate lake remote files finished, txn_id: " << txn_id << ", tablet_id: " << target_tablet_id;

    clean_files.cancel();
    return Status::OK();
}

bool LakeReplicationTxnManager::should_use_parallel_copy(size_t file_count, const ThreadPool* thread_pool) {
    if (thread_pool == nullptr) {
        return false;
    }
    const int min_file_count = std::max(0, config::lake_replication_parallel_copy_min_file_count);
    if (min_file_count == 0) {
        return false;
    }
    if (config::lake_replication_max_parallel_files_per_tablet <= 1) {
        return false;
    }
    if (file_count < static_cast<size_t>(min_file_count)) {
        return false;
    }
    if (thread_pool->max_threads() <= 0) {
        return false;
    }
    return true;
}

Status LakeReplicationTxnManager::execute_file_copy_tasks(std::vector<ReplicationTask> tasks, ThreadPool* thread_pool,
                                                          size_t max_workers) {
    if (tasks.empty()) {
        return Status::OK();
    }
    if (thread_pool == nullptr || thread_pool->max_threads() <= 0) {
        return Status::InvalidArgument("Lake replication file copy thread pool is unavailable");
    }

    const size_t worker_count = std::min(tasks.size(), std::max<size_t>(1, max_workers));
    auto token = thread_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    std::atomic<size_t> next_task{0};
    std::vector<Status> task_results(tasks.size());
    Status submit_status;
    for (size_t i = 0; i < worker_count; ++i) {
        auto st = token->submit_func([&]() {
            while (true) {
                size_t task_index = next_task.fetch_add(1, std::memory_order_relaxed);
                if (task_index >= tasks.size()) {
                    return;
                }
                task_results[task_index] = tasks[task_index]();
            }
        });
        if (!st.ok() && submit_status.ok()) {
            submit_status = std::move(st);
        }
    }
    token->wait();
    RETURN_IF_ERROR(submit_status);
    for (const auto& result : task_results) {
        RETURN_IF_ERROR(result);
    }
    return Status::OK();
}

StatusOr<size_t> LakeReplicationTxnManager::copy_non_segment_file_with_retry(
        const std::string& src_file_location, const std::shared_ptr<FileSystem>& shared_src_fs,
        const SequentialFileOptions& src_opts, const std::string& target_file_location, const WritableFileOptions& opts,
        int max_retry) {
    ASSIGN_OR_RETURN(auto expected_size, shared_src_fs->get_file_size(src_file_location));

    const size_t buff_size = std::max<size_t>(
            std::min<size_t>(expected_size, config::lake_replication_read_buffer_size), 1 * 1024 * 1024);

    max_retry = std::max(1, max_retry);
    Status copy_status;
    size_t final_file_size = 0;
    for (int retry = 0; retry < max_retry; ++retry) {
        auto res = fs::copy_file(src_file_location, shared_src_fs, src_opts, target_file_location, nullptr, opts,
                                 buff_size);
        if (!res.ok()) {
            copy_status = res.status();
            LOG(WARNING) << "Failed to copy file " << src_file_location << " to " << target_file_location
                         << ", retry=" << retry << ", error: " << copy_status;
            continue;
        }
        final_file_size = *res;
        TEST_SYNC_POINT_CALLBACK("lake_replication_non_segment_copy_size", &final_file_size);
        if (static_cast<int64_t>(final_file_size) == static_cast<int64_t>(expected_size)) {
            return final_file_size;
        }
        copy_status = Status::Corruption(fmt::format("File size mismatch after copy: expected={}, actual={}, src={}",
                                                     expected_size, final_file_size, src_file_location));
        LOG(WARNING) << copy_status.message() << ", retry=" << retry;
    }
    return copy_status;
}

StatusOr<size_t> LakeReplicationTxnManager::copy_non_segment_file_with_retry(
        const std::string& src_file_location, const std::shared_ptr<FileSystem>& shared_src_fs,
        const std::string& target_file_location, const WritableFileOptions& opts, int max_retry) {
    return copy_non_segment_file_with_retry(src_file_location, shared_src_fs, SequentialFileOptions{},
                                            target_file_location, opts, max_retry);
}

StatusOr<TabletMetadataPtr> LakeReplicationTxnManager::build_source_tablet_meta(
        int64_t src_tablet_id, int64_t version, const std::string& meta_dir,
        const std::shared_ptr<FileSystem>& shared_src_fs) {
    LOG(INFO) << "Lake replicate storage task, building source tablet meta for tablet: " << src_tablet_id
              << ", version: " << version << ", meta_dir: " << meta_dir;

#ifdef BE_TEST
    TabletMetadataPtr injected_meta = nullptr;
    TEST_SYNC_POINT_CALLBACK("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                             static_cast<void*>(&injected_meta));
    if (injected_meta != nullptr) {
        return injected_meta;
    }
#endif

    const auto src_tablet_meta_path = join_path(meta_dir, tablet_metadata_filename(src_tablet_id, version));
    auto src_tablet_meta = load_source_standalone_tablet_metadata(src_tablet_meta_path, shared_src_fs);
    if (src_tablet_meta.ok()) {
        return src_tablet_meta;
    }
    if (!src_tablet_meta.status().is_not_found()) {
        VLOG(3) << "Lake replicate storage task, failed to build source tablet meta for version: " << version
                << ", src_tablet_id: " << src_tablet_id << ", error: " << src_tablet_meta.status();
        return src_tablet_meta;
    }
    return load_source_bundle_tablet_metadata(src_tablet_id, version, meta_dir, shared_src_fs);
}

StatusOr<TabletMetadataPtr> LakeReplicationTxnManager::try_build_source_tablet_meta_with_fallback(
        int64_t src_tablet_id, int64_t version, int64_t src_db_id, TTransactionId txn_id, std::string& src_meta_dir,
        std::string& src_data_dir, const std::shared_ptr<FileSystem>& shared_src_fs) {
    // Strategy: Try current format first, then fallback to legacy formats on NotFound error.
    const std::string original_meta_dir = src_meta_dir;
    const std::string original_data_dir = src_data_dir;

    // Attempt 1: Try current path format
    auto result = build_source_tablet_meta(src_tablet_id, version, src_meta_dir, shared_src_fs);
    if (result.ok()) {
        return result;
    }

    // If error is not NotFound, return immediately
    if (!result.status().is_not_found()) {
        LOG(WARNING) << "Lake replicate storage task, failed to build source tablet meta for version: " << version
                     << ", src_tablet_id: " << src_tablet_id << ", error: " << result.status();
        return result;
    }

    LOG(INFO) << "Source tablet meta not found with current path format, trying legacy format without db_id"
              << ", src_meta_dir: " << src_meta_dir << ", txn_id: " << txn_id;

    // Attempt 2: Try legacy format without db_id (keep partition_id)
    // Example: db56764/56970/63453/meta -> 56970/63453/meta
    std::string legacy_meta_dir = remove_db_id_component(original_meta_dir, src_db_id);
    std::string legacy_data_dir = remove_db_id_component(original_data_dir, src_db_id);

    result = build_source_tablet_meta(src_tablet_id, version, legacy_meta_dir, shared_src_fs);
    if (result.ok()) {
        LOG(INFO) << "Source tablet meta found with legacy format (without db_id)"
                  << ", updated meta_dir: " << legacy_meta_dir << ", data_dir: " << legacy_data_dir
                  << ", txn_id: " << txn_id;
        src_meta_dir = legacy_meta_dir;
        src_data_dir = legacy_data_dir;
        return result;
    }

    // If error is not NotFound, return immediately
    if (!result.status().is_not_found()) {
        LOG(WARNING) << "Lake replicate storage task, failed to build source tablet meta for version: " << version
                     << ", src_tablet_id: " << src_tablet_id << ", legacy_meta_dir: " << legacy_meta_dir
                     << ", error: " << result.status();
        return result;
    }

    LOG(INFO) << "Source tablet meta not found with legacy format without db_id, "
              << "trying very old format without db_id and partition_id"
              << ", legacy_meta_dir: " << legacy_meta_dir << ", txn_id: " << txn_id;

    // Attempt 3: Try very old format without db_id and partition_id
    // Remove partition_id from legacy1 path
    // Example: 56970/63453/meta -> 56970/meta
    std::string very_old_meta_dir = remove_last_path_component(legacy_meta_dir);
    std::string very_old_data_dir = remove_last_path_component(legacy_data_dir);

    result = build_source_tablet_meta(src_tablet_id, version, very_old_meta_dir, shared_src_fs);
    if (result.ok()) {
        LOG(INFO) << "Source tablet meta found with very old format (without db_id and partition_id)"
                  << ", updated meta_dir: " << very_old_meta_dir << ", data_dir: " << very_old_data_dir
                  << ", txn_id: " << txn_id;
        src_meta_dir = very_old_meta_dir;
        src_data_dir = very_old_data_dir;
        return result;
    }

    // All attempts failed, return the last error
    LOG(WARNING) << "Lake replicate storage task, failed to build source tablet meta after all fallback attempts"
                 << ", version: " << version << ", src_tablet_id: " << src_tablet_id << ", txn_id: " << txn_id
                 << ", error: " << result.status();
    return result;
}

Status LakeReplicationTxnManager::build_existed_filename_uuids_map(
        const TabletMetadataPtr& target_data_version_tablet_meta, ExistingFileMap& existed_filename_uuids,
        ExistingBundleSliceEncryptionMetaMap& bundle_slice_encryption_metas) {
    // Collect UUIDs from rowsets (segments and del files)
    for (const auto& rowset : target_data_version_tablet_meta->rowsets()) {
        for (const auto& segment_meta : rowset.segment_metas()) {
            const auto& segment_name = segment_meta.filename();
            const auto uuid = extract_uuid_from(segment_name);
            existed_filename_uuids.emplace(
                    uuid, ExistingFileInfo{segment_name,
                                           segment_meta.has_bundle_file_offset() ? "" : segment_meta.encryption_meta(),
                                           segment_meta.shared()});
            if (segment_meta.has_bundle_file_offset()) {
                auto& slice_metas = bundle_slice_encryption_metas[uuid];
                auto [it, inserted] =
                        slice_metas.emplace(segment_meta.bundle_file_offset(), segment_meta.encryption_meta());
                if (!inserted && it->second != segment_meta.encryption_meta()) {
                    return Status::Corruption(
                            fmt::format("Conflicting target bundle slice encryption metadata for UUID {} at offset {}",
                                        uuid, segment_meta.bundle_file_offset()));
                }
            }
        }
        for (const auto& del : rowset.del_files()) {
            const auto& del_filename = del.name();
            existed_filename_uuids.emplace(extract_uuid_from(del_filename),
                                           ExistingFileInfo{del_filename, del.encryption_meta(), del.shared()});
        }
    }

    // Collect UUIDs from SST files
    if (target_data_version_tablet_meta->has_sstable_meta()) {
        const auto& dest_meta = target_data_version_tablet_meta->sstable_meta();
        for (const auto& sst : dest_meta.sstables()) {
            const auto& sst_filename = sst.filename();
            existed_filename_uuids.emplace(extract_uuid_from(sst_filename),
                                           ExistingFileInfo{sst_filename, sst.encryption_meta(), sst.shared()});
        }
    }

    // Collect UUIDs from delvec files
    if (target_data_version_tablet_meta->has_delvec_meta()) {
        const auto& dest_meta = target_data_version_tablet_meta->delvec_meta();
        for (const auto& [_, file_meta_pb] : dest_meta.version_to_file()) {
            const auto& delvec_filename = file_meta_pb.name();
            existed_filename_uuids.emplace(
                    extract_uuid_from(delvec_filename),
                    ExistingFileInfo{delvec_filename, file_meta_pb.encryption_meta(), file_meta_pb.shared()});
        }
    }

    // Collect UUIDs from dcg files
    if (target_data_version_tablet_meta->has_dcg_meta()) {
        const auto& dcg_meta = target_data_version_tablet_meta->dcg_meta();
        for (const auto& [_, dcg_ver_pb] : dcg_meta.dcgs()) {
            bool has_encryption_meta = dcg_ver_pb.column_files_size() == dcg_ver_pb.encryption_metas_size();
            for (int i = 0; i < dcg_ver_pb.column_files_size(); ++i) {
                const auto& dcg_filename = dcg_ver_pb.column_files(i);
                const std::string encryption_meta = has_encryption_meta ? dcg_ver_pb.encryption_metas(i) : "";
                const bool shared = i < dcg_ver_pb.shared_files_size() && dcg_ver_pb.shared_files(i);
                existed_filename_uuids.emplace(extract_uuid_from(dcg_filename),
                                               ExistingFileInfo{dcg_filename, encryption_meta, shared});
            }
        }
    }

    // Collect UUIDs from idg (.idx) files so a repeated full-snapshot replication reuses the
    // already-replicated .idx (and its encryption meta) instead of re-copying it.
    if (target_data_version_tablet_meta->has_idg_meta()) {
        const auto& idg_meta = target_data_version_tablet_meta->idg_meta();
        for (const auto& [_, idg_ver_pb] : idg_meta.idgs()) {
            for (const auto& entry : idg_ver_pb.entries()) {
                if (!entry.has_index_file() || entry.index_file().empty()) {
                    continue;
                }
                existed_filename_uuids.emplace(
                        extract_uuid_from(entry.index_file()),
                        ExistingFileInfo{entry.index_file(), entry.encryption_meta(), entry.shared_file()});
            }
        }
    }

    return Status::OK();
}

StatusOr<std::shared_ptr<TabletMetadataPB>> LakeReplicationTxnManager::convert_and_build_new_tablet_meta(
        const TabletMetadataPtr& src_tablet_meta, const TabletMetadataPtr& target_tablet_meta, int64_t src_tablet_id,
        int64_t target_tablet_id, TTransactionId txn_id, int64_t data_version, const std::string& src_data_dir,
        std::unordered_map<std::string, size_t>& segment_name_to_size_map,
        std::map<std::string, std::string>& file_locations,
        std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>>& filename_map,
        SourceEncryptionMetaMap& source_encryption_metas) {
    VLOG(3) << "Lake replicate storage task, building new tablet meta for tablet: " << target_tablet_id
            << ", src_tablet_id: " << src_tablet_id << ", txn_id: " << txn_id << ", data_version: " << data_version;
    // find all files that already replicated to target storage in previous txns
    auto target_data_version_tablet_meta_or =
            _tablet_manager->get_tablet_metadata(target_tablet_id, data_version, false, 0, nullptr);
    TabletMetadataPtr target_data_version_tablet_meta;
    if (target_data_version_tablet_meta_or.ok()) {
        target_data_version_tablet_meta = std::move(target_data_version_tablet_meta_or).value();
    } else if (target_data_version_tablet_meta_or.status().is_not_found() && target_tablet_meta->has_range() &&
               target_tablet_meta->version() > data_version) {
        target_data_version_tablet_meta = target_tablet_meta;
    } else {
        return target_data_version_tablet_meta_or.status();
    }
    // `existed_filename_uuids` represented files that already replicated to target storage in previous txns
    // <uuid, destination filename/encryption/shared ownership>
    ExistingFileMap existed_filename_uuids;
    ExistingBundleSliceEncryptionMetaMap bundle_slice_encryption_metas;
    RETURN_IF_ERROR(build_existed_filename_uuids_map(target_data_version_tablet_meta, existed_filename_uuids,
                                                     bundle_slice_encryption_metas));

    const bool preserve_source_shared = src_tablet_meta->has_range() && target_tablet_meta->has_range();
    struct SourceFileDeclaration {
        std::string encryption_meta;
        bool shared_or_bundled = false;
    };
    std::unordered_map<std::string, SourceFileDeclaration> source_file_declarations;
    auto destination_shared = [&existed_filename_uuids, preserve_source_shared](const std::string& source_filename,
                                                                                bool source_shared,
                                                                                bool existed) -> StatusOr<bool> {
        // For aligned range tablets, a source-shared segment can contain rows for multiple
        // split children. The shared bit makes each child apply its tablet range while reading.
        // The corresponding target children also use one physical copied file, so preserve the
        // bit for all associated sidecars as well.
        if (preserve_source_shared && source_shared) {
            return true;
        }
        if (!existed) {
            return false;
        }
        auto it = existed_filename_uuids.find(extract_uuid_from(source_filename));
        if (it == existed_filename_uuids.end()) {
            return Status::Corruption("Existing replicated file disappeared from the UUID map: " + source_filename);
        }
        return it->second.shared;
    };
    auto record_source_encryption_declaration =
            [&](const std::string& source_filename, const std::string& encryption_meta, bool existed,
                bool destination_file_shared, bool source_bundled = false) -> Status {
        if (existed) {
            return Status::OK();
        }
        auto [it, inserted] = source_file_declarations.try_emplace(
                source_filename, SourceFileDeclaration{.encryption_meta = encryption_meta});
        if (!inserted && it->second.encryption_meta != encryption_meta) {
            return Status::Corruption(
                    fmt::format("Conflicting source encryption metadata for file: {}", source_filename));
        }
        it->second.shared_or_bundled |= destination_file_shared || source_bundled;
        return Status::OK();
    };

    VLOG(3) << "Lake replicate storage task, found " << existed_filename_uuids.size() << " existed files";
    // make new metadata
    std::shared_ptr<TabletMetadataPB> new_metadata = std::make_shared<TabletMetadataPB>(*target_tablet_meta);
    // Replace the tablet id with target tablet id
    new_metadata->mutable_rowsets()->Clear();
    new_metadata->mutable_dcg_meta()->mutable_dcgs()->clear();
    new_metadata->mutable_sstable_meta()->Clear();
    new_metadata->mutable_delvec_meta()->Clear();
    // Drop the target's pre-replication idg_meta. Without this the target's stale
    // per-segment IDG (.idx) entries would be carried into the replicated metadata:
    // their rssids no longer match the freshly replicated rowsets (dangling entries),
    // their .idx files leak (never orphaned/vacuumed since a retained entry still
    // "references" them), and the source's own indexes would be missing. The source's
    // idg_meta is rebuilt below, mirroring rowsets/dcg/sstable/delvec.
    new_metadata->mutable_idg_meta()->Clear();

    // deal with segments and dels
    for (const auto& src_rowset_meta : src_tablet_meta->rowsets()) {
        auto new_rowset_meta = new_metadata->add_rowsets();
        new_rowset_meta->CopyFrom(src_rowset_meta);
        new_rowset_meta->mutable_del_files()->Clear();
        // Replication produces a target-local rowset; any source-cluster uid carried by
        // CopyFrom belongs to a different uid space, so mint a fresh target uid.
        tablet_reshard_helper::set_rowset_uid(new_rowset_meta);

        // Convert rowset metadata. The copied segment_metas carry over per-segment attributes
        // (size, sort keys, num_rows, vector_index_ids, ...); rewrite only filename/encryption_meta.
        for (int i = 0; i < src_rowset_meta.segment_metas_size(); ++i) {
            const auto& src_seg_meta = src_rowset_meta.segment_metas(i);
            const auto& src_segment_filename = src_seg_meta.filename();
            std::string final_segment_filename;
            ASSIGN_OR_RETURN(auto is_existed,
                             determine_final_filename(src_segment_filename, txn_id, existed_filename_uuids,
                                                      final_segment_filename, target_tablet_id, src_data_dir,
                                                      file_locations, filename_map));
            auto* new_seg_meta = new_rowset_meta->mutable_segment_metas(i);
            new_seg_meta->set_filename(final_segment_filename);
            new_seg_meta->clear_encryption_meta();
            ASSIGN_OR_RETURN(auto destination_file_shared,
                             destination_shared(src_segment_filename, src_seg_meta.shared(), is_existed));
            new_seg_meta->set_shared(destination_file_shared);
            RETURN_IF_ERROR(record_source_encryption_declaration(src_segment_filename, src_seg_meta.encryption_meta(),
                                                                 is_existed, destination_file_shared,
                                                                 src_seg_meta.has_bundle_file_offset()));

            // Add encryption metadata for files
            if (!is_existed) {
                if (config::enable_transparent_data_encryption) {
                    // segment file doesn't exist, use the newly generated encryption metadata
                    const auto& pair = filename_map[src_segment_filename];
                    new_seg_meta->set_encryption_meta(pair.second.encryption_meta);
                }
            } else {
                // segment file already exists, use the existing encryption metadata from target tablet
                auto uuid = extract_uuid_from(src_segment_filename);
                if (src_seg_meta.has_bundle_file_offset()) {
                    auto uuid_it = bundle_slice_encryption_metas.find(uuid);
                    if (uuid_it == bundle_slice_encryption_metas.end()) {
                        return Status::Corruption(fmt::format(
                                "No existing target bundle slice encryption metadata found for UUID {}", uuid));
                    }
                    auto offset_it = uuid_it->second.find(src_seg_meta.bundle_file_offset());
                    if (offset_it == uuid_it->second.end()) {
                        return Status::Corruption(fmt::format(
                                "No existing target bundle slice encryption metadata found for UUID {} at offset {}",
                                uuid, src_seg_meta.bundle_file_offset()));
                    }
                    new_seg_meta->set_encryption_meta(offset_it->second);
                } else {
                    auto it = existed_filename_uuids.find(uuid);
                    if (it != existed_filename_uuids.end()) {
                        new_seg_meta->set_encryption_meta(it->second.encryption_meta);
                    } else {
                        // should never happend
                        return Status::Corruption(fmt::format("no existing encryption metadata found for file: {}",
                                                              src_segment_filename));
                    }
                }
            }

            // build segment_name_to_size_map, record the size of source segment file
            if (src_seg_meta.has_size() && !src_seg_meta.has_bundle_file_offset()) {
                segment_name_to_size_map.emplace(src_segment_filename, src_seg_meta.size());
            }
        }
        // update next_rowset_id
        new_metadata->set_next_rowset_id(src_tablet_meta->next_rowset_id());

        // Convert dels
        for (const DelfileWithRowsetId& src_del : src_rowset_meta.del_files()) {
            const auto& src_del_filename = src_del.name();
            std::string final_del_filename;
            ASSIGN_OR_RETURN(auto is_existed, determine_final_filename(src_del_filename, txn_id, existed_filename_uuids,
                                                                       final_del_filename, target_tablet_id,
                                                                       src_data_dir, file_locations, filename_map));
            auto* new_del = new_rowset_meta->add_del_files();
            new_del->CopyFrom(src_del);
            new_del->set_name(final_del_filename);
            // The replicated file is produced by the download, which routes .del files through
            // build_file_converters and may re-encode the payload (DelFileStreamConverter, PK encoding
            // V1->V2). The source checksum describes the source bytes, so keeping it would make the
            // target reject a perfectly valid del file. Drop it unconditionally rather than trying to
            // predict here whether this particular file gets transcoded: absent means "not recorded"
            // and readers skip verification, same as the shared-nothing replication path.
            new_del->clear_crc32c();
            ASSIGN_OR_RETURN(auto destination_file_shared,
                             destination_shared(src_del_filename, src_del.shared(), is_existed));
            new_del->set_shared(destination_file_shared);
            RETURN_IF_ERROR(record_source_encryption_declaration(src_del_filename, src_del.encryption_meta(),
                                                                 is_existed, destination_file_shared));
            new_del->clear_encryption_meta();

            if (!is_existed) {
                if (config::enable_transparent_data_encryption) {
                    // del doesn't exist, use the newly generated encryption metadata
                    const auto& pair = filename_map[src_del_filename];
                    new_del->set_encryption_meta(pair.second.encryption_meta);
                }
            } else {
                // del already exists, use the existing encryption metadata from target tablet
                auto uuid = extract_uuid_from(src_del_filename);
                auto it = existed_filename_uuids.find(uuid);
                if (it != existed_filename_uuids.end()) {
                    const std::string& existing_encryption_meta = it->second.encryption_meta;
                    new_del->set_encryption_meta(existing_encryption_meta);
                }
            }
        }
    }

    // deal with sstable
    if (src_tablet_meta->has_sstable_meta()) {
        PersistentIndexSstableMetaPB* dest_meta = new_metadata->mutable_sstable_meta();
        dest_meta->CopyFrom(src_tablet_meta->sstable_meta());
        for (PersistentIndexSstablePB& sst_ref : *dest_meta->mutable_sstables()) {
            PersistentIndexSstablePB* sst = &sst_ref;
            const auto src_sst_filename = sst->filename();
            const auto source_encryption_meta = sst->encryption_meta();
            const bool source_shared = sst->shared();
            std::string final_sst_filename;
            ASSIGN_OR_RETURN(auto is_existed, determine_final_filename(src_sst_filename, txn_id, existed_filename_uuids,
                                                                       final_sst_filename, target_tablet_id,
                                                                       src_data_dir, file_locations, filename_map));
            sst->set_filename(final_sst_filename);
            ASSIGN_OR_RETURN(auto destination_file_shared,
                             destination_shared(src_sst_filename, source_shared, is_existed));
            sst->set_shared(destination_file_shared);
            RETURN_IF_ERROR(record_source_encryption_declaration(src_sst_filename, source_encryption_meta, is_existed,
                                                                 destination_file_shared));
            sst->clear_encryption_meta();

            if (!is_existed) {
                if (config::enable_transparent_data_encryption) {
                    // sst doesn't exist, use the newly generated encryption metadata
                    const auto& pair = filename_map[src_sst_filename];
                    sst->set_encryption_meta(pair.second.encryption_meta);
                }
            } else {
                // sst already exists, use the existing encryption metadata from target tablet
                auto uuid = extract_uuid_from(src_sst_filename);
                auto it = existed_filename_uuids.find(uuid);
                if (it != existed_filename_uuids.end()) {
                    const std::string& existing_encryption_meta = it->second.encryption_meta;
                    sst->set_encryption_meta(existing_encryption_meta);
                }
            }
        }
    }

    // deal with delvec
    if (src_tablet_meta->has_delvec_meta()) {
        DelvecMetadataPB* dest_meta = new_metadata->mutable_delvec_meta();
        dest_meta->CopyFrom(src_tablet_meta->delvec_meta());
        for (const auto& [version, file_meta_pb] : dest_meta->version_to_file()) {
            auto src_delvec_filename = file_meta_pb.name();
            const auto source_encryption_meta = file_meta_pb.encryption_meta();
            std::string final_delvec_filename;
            ASSIGN_OR_RETURN(
                    auto is_existed,
                    determine_final_filename(src_delvec_filename, txn_id, existed_filename_uuids, final_delvec_filename,
                                             target_tablet_id, src_data_dir, file_locations, filename_map));
            auto& item = (*dest_meta->mutable_version_to_file())[version];
            item.set_name(final_delvec_filename);
            ASSIGN_OR_RETURN(auto destination_file_shared,
                             destination_shared(src_delvec_filename, file_meta_pb.shared(), is_existed));
            item.set_shared(destination_file_shared);
            RETURN_IF_ERROR(record_source_encryption_declaration(src_delvec_filename, source_encryption_meta,
                                                                 is_existed, destination_file_shared));
            item.clear_encryption_meta();

            if (!is_existed) {
                if (config::enable_transparent_data_encryption) {
                    // del file doesn't exist, use the newly generated encryption metadata
                    const auto& pair = filename_map[src_delvec_filename];
                    item.set_encryption_meta(pair.second.encryption_meta);
                }
            } else {
                // del file already exists, use the existing encryption metadata from target tablet
                auto uuid = extract_uuid_from(src_delvec_filename);
                auto it = existed_filename_uuids.find(uuid);
                if (it != existed_filename_uuids.end()) {
                    const std::string& existing_encryption_meta = it->second.encryption_meta;
                    item.set_encryption_meta(existing_encryption_meta);
                }
            }
        }
    }

    // deal with dcg
    if (src_tablet_meta->has_dcg_meta()) {
        DeltaColumnGroupMetadataPB* dest_meta = new_metadata->mutable_dcg_meta();
        dest_meta->CopyFrom(src_tablet_meta->dcg_meta());
        for (auto& [segment_id, dcg_ver_pb] : *dest_meta->mutable_dcgs()) {
            std::vector<bool> source_shared_files;
            source_shared_files.reserve(dcg_ver_pb.column_files_size());
            std::vector<std::string> source_dcg_encryption_metas;
            source_dcg_encryption_metas.reserve(dcg_ver_pb.column_files_size());
            for (int i = 0; i < dcg_ver_pb.column_files_size(); ++i) {
                source_shared_files.emplace_back(i < dcg_ver_pb.shared_files_size() && dcg_ver_pb.shared_files(i));
                source_dcg_encryption_metas.emplace_back(
                        i < dcg_ver_pb.encryption_metas_size() ? dcg_ver_pb.encryption_metas(i) : "");
            }
            dcg_ver_pb.clear_shared_files();
            dcg_ver_pb.clear_encryption_metas();
            for (int i = 0; i < dcg_ver_pb.column_files_size(); ++i) {
                auto src_dcg_filename = dcg_ver_pb.column_files(i);
                std::string final_dcg_filename;
                ASSIGN_OR_RETURN(
                        auto is_existed,
                        determine_final_filename(src_dcg_filename, txn_id, existed_filename_uuids, final_dcg_filename,
                                                 target_tablet_id, src_data_dir, file_locations, filename_map));
                dcg_ver_pb.set_column_files(i, final_dcg_filename);
                ASSIGN_OR_RETURN(auto destination_file_shared,
                                 destination_shared(src_dcg_filename, source_shared_files[i], is_existed));
                dcg_ver_pb.add_shared_files(destination_file_shared);
                RETURN_IF_ERROR(record_source_encryption_declaration(src_dcg_filename, source_dcg_encryption_metas[i],
                                                                     is_existed, destination_file_shared));

                std::string destination_encryption_meta;
                if (!is_existed && config::enable_transparent_data_encryption) {
                    // dcg file doesn't exist, use the newly generated encryption metadata
                    const auto& pair = filename_map[src_dcg_filename];
                    destination_encryption_meta = pair.second.encryption_meta;
                } else if (is_existed) {
                    // dcg file already exists, use the existing encryption metadata from target tablet
                    auto uuid = extract_uuid_from(src_dcg_filename);
                    auto it = existed_filename_uuids.find(uuid);
                    if (it != existed_filename_uuids.end()) {
                        destination_encryption_meta = it->second.encryption_meta;
                    }
                }
                dcg_ver_pb.add_encryption_metas(destination_encryption_meta);
            }
        }
    }

    // deal with idg_meta (per-segment Index Delta Group / .idx sidecar index files,
    // produced by the lake ADD INDEX fast path: BITMAP / NGRAMBF / bloom_filter_columns).
    // Mirror the sstable/dcg handling: copy the source IDG metadata, then rewrite each
    // .idx filename (and encryption meta) and register the file for copy via
    // determine_final_filename, so the source's fast-path indexes are actually replicated
    // to the target. The IDG map is keyed by rssid; rowset ids and next_rowset_id are
    // adopted verbatim from the source above (set_rowset_uid only mints a fresh 128-bit
    // uid, not the numeric rowset id), so the source's rssid keys stay valid against the
    // copied rowsets and need no remap. The target's own stale idg_meta was cleared above;
    // the publish-time applier orphans its now-unreferenced .idx via collect_idg_orphan_files.
    //
    // Fast-schema-change caveat: when the source and target tablets assign different column
    // unique ids to the same logical column, replication remaps the ids embedded in
    // segment/.cols footers via build_file_converters + column_unique_id_map. The IDG entry
    // keys (IndexKey.col_unique_id / dropped_keys) AND the col_unique_ids embedded inside the
    // .idx payload footer -- which IndexFileReader::find(col_unique_id, index_type) and the
    // scan probe (ScalarColumnIterator matches k.col_unique_id == opts.col_unique_id) look up
    // by the TARGET id -- are NOT converted here (build_file_converters only rewrites
    // is_segment()/is_cols() footers). Copying idg_meta + .idx verbatim under a divergent id
    // space would make the replica either silently ignore the index (target id misses the
    // source-keyed entry) or, on a unique-id collision, apply an index built for a different
    // column and prune rows wrongly. Until the .idx footer + IDG-key remap is implemented,
    // skip IDG replication whenever the id spaces diverge: leave idg_meta cleared (index
    // absent on the replica, to be rebuilt on the target) rather than publishing a
    // mismappable index. The common identical-schema CCR path (empty map) is unaffected.
    std::unordered_map<uint32_t, uint32_t> idg_column_unique_id_map;
    if (target_tablet_meta->has_schema()) {
        ReplicationUtils::calc_column_unique_id_map(src_tablet_meta->schema().column(),
                                                    target_tablet_meta->schema().column(), &idg_column_unique_id_map);
    }
    if (src_tablet_meta->has_idg_meta() && !idg_column_unique_id_map.empty()) {
        LOG(WARNING) << "Lake replicate storage task, skipping IDG (.idx) index replication because source/target "
                        "column unique ids diverge (fast schema change); the fast-path index will be absent on the "
                        "replica and must be rebuilt on the target. target_tablet_id: "
                     << target_tablet_id << ", txn_id: " << txn_id
                     << ", unique_id_map size: " << idg_column_unique_id_map.size();
    } else if (src_tablet_meta->has_idg_meta()) {
        IndexDeltaGroupMetadataPB* dest_meta = new_metadata->mutable_idg_meta();
        dest_meta->CopyFrom(src_tablet_meta->idg_meta());
        for (auto& [rssid, idg_ver] : *dest_meta->mutable_idgs()) {
            for (auto& entry : *idg_ver.mutable_entries()) {
                if (!entry.has_index_file() || entry.index_file().empty()) {
                    continue;
                }
                const auto src_idx_filename = entry.index_file();
                const auto source_encryption_meta = entry.encryption_meta();
                const bool source_shared = entry.shared_file();
                std::string final_idx_filename;
                ASSIGN_OR_RETURN(
                        auto is_existed,
                        determine_final_filename(src_idx_filename, txn_id, existed_filename_uuids, final_idx_filename,
                                                 target_tablet_id, src_data_dir, file_locations, filename_map));
                entry.set_index_file(final_idx_filename);
                ASSIGN_OR_RETURN(auto destination_file_shared,
                                 destination_shared(src_idx_filename, source_shared, is_existed));
                entry.set_shared_file(destination_file_shared);
                RETURN_IF_ERROR(record_source_encryption_declaration(src_idx_filename, source_encryption_meta,
                                                                     is_existed, destination_file_shared));
                // The source's encryption meta belongs to the source cluster; drop it and
                // re-derive against the target (matching the segment handling above).
                entry.clear_encryption_meta();

                if (!is_existed) {
                    if (config::enable_transparent_data_encryption) {
                        // .idx file doesn't exist on target, use the newly generated encryption metadata
                        const auto& pair = filename_map[src_idx_filename];
                        entry.set_encryption_meta(pair.second.encryption_meta);
                    }
                } else {
                    // .idx file already replicated in a previous txn, reuse its existing encryption metadata
                    auto uuid = extract_uuid_from(src_idx_filename);
                    auto it = existed_filename_uuids.find(uuid);
                    if (it != existed_filename_uuids.end()) {
                        entry.set_encryption_meta(it->second.encryption_meta);
                    }
                }
            }
        }
    }

    for (const auto& [source_filename, declaration] : source_file_declarations) {
        if (declaration.shared_or_bundled) {
            if (!declaration.encryption_meta.empty() || config::enable_transparent_data_encryption) {
                return Status::NotSupported("Copying new encrypted shared or bundled physical files is not supported");
            }
        } else {
            source_encryption_metas.emplace(source_filename, declaration.encryption_meta);
        }
    }

    return new_metadata;
}

StatusOr<bool> LakeReplicationTxnManager::determine_final_filename(
        const std::string& src_filename, TTransactionId txn_id, const ExistingFileMap& existed_filename_uuids,
        std::string& final_filename, const int64_t target_tablet_id, const std::string& src_data_dir,
        std::map<std::string, std::string>& file_locations,
        std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>>& filename_map) {
    auto uuid = extract_uuid_from(src_filename);
    auto it = existed_filename_uuids.find(uuid);
    if (it != existed_filename_uuids.end()) {
        // UUID exists, use the existing target filename
        final_filename = it->second.filename;
        LOG(INFO) << "File: " << src_filename
                  << " already exists on target cluster, use existing target filename: " << final_filename;
        return true;
    }

    // One physical bundle object can appear as multiple logical segment slices. Register and
    // copy it once, while every SegmentMetadataPB keeps its own bundle_file_offset and size.
    auto pending_it = filename_map.find(src_filename);
    if (pending_it != filename_map.end()) {
        final_filename = pending_it->second.first;
        return false;
    }

    // UUID not exists, generate new filename
    final_filename = gen_filename_from(txn_id, src_filename);
    if (UNLIKELY(final_filename.empty())) {
        return Status::Corruption("Failed to generate new filename from: " + src_filename);
    }

    // Build file_locations map
    auto target_file_path = _tablet_manager->segment_location(target_tablet_id, final_filename);
    file_locations.emplace(join_path(src_data_dir, src_filename), target_file_path);

    // Build filename_map
    FileEncryptionPair encryption_pair;
    if (config::enable_transparent_data_encryption) {
        ASSIGN_OR_RETURN(encryption_pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
    }
    auto pair = filename_map.emplace(src_filename, std::pair(final_filename, std::move(encryption_pair)));
    if (!pair.second) {
        return Status::Corruption("Duplicated file: " + pair.first->first);
    }
    return false;
}

Status LakeReplicationTxnManager::update_tablet_metadata_segment_sizes(
        const std::shared_ptr<TabletMetadataPB>& tablet_metadata,
        const std::unordered_map<std::string, size_t>& segment_size_changes) {
    if (segment_size_changes.empty()) {
        return Status::OK();
    }

    int updated_count = 0;

    // Iterate through all rowsets in the tablet metadata
    for (auto& rowset_ref : *tablet_metadata->mutable_rowsets()) {
        auto* rowset = &rowset_ref;

        // Update segment sizes if they changed
        for (auto& seg_meta_ref : *rowset->mutable_segment_metas()) {
            auto* segment_meta = &seg_meta_ref;
            // No segment size recorded, skip
            if (!segment_meta->has_size()) {
                continue;
            }
            const auto& segment_name = segment_meta->filename();
            auto it = segment_size_changes.find(segment_name);
            if (it != segment_size_changes.end()) {
                uint64_t old_size = segment_meta->size();
                uint64_t new_size = it->second;

                if (old_size != new_size) {
                    segment_meta->set_size(new_size);
                    updated_count++;

                    LOG(INFO) << "Updated segment size in tablet_metadata, rowset_id: " << rowset->id()
                              << ", segment: " << segment_name << ", old_size: " << old_size
                              << ", new_size: " << new_size;
                }
            }
        }
    }

    if (updated_count > 0) {
        LOG(INFO) << "Updated " << updated_count << " segment sizes in tablet_metadata";
    }

    return Status::OK();
}

} // namespace starrocks::lake
