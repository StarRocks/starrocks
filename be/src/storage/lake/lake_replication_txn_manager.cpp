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

#include "agent/master_info.h"
#include "fs/fs_starlet.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "gen_cpp/Types_constants.h"
#include "gen_cpp/lake_types.pb.h"
#include "persistent_index_sstable.h"
#include "replication_txn_manager.h"
#include "storage/lake/filenames.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/segment_stream_converter.h"
#include "util/dynamic_cache.h"
#include "vacuum.h"

namespace starrocks::lake {

Status LakeReplicationTxnManager::replicate_lake_remote_storage(const TReplicateSnapshotRequest& request) {
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

    LOG(INFO) << "Start to replicate lake remote storage, txn_id: " << txn_id << ", tablet_id: " << target_tablet_id
              << ", src_tablet_id: " << src_tablet_id << ", src_db_id: " << src_db_id
              << ", src_table_id: " << src_table_id << ", src_partition_id: " << src_partition_id
              << ", visible_version: " << target_visible_version << ", data_version: " << data_version
              << ", virtual_tablet_id: " << virtual_tablet_id << ", src_visible_version: " << src_visible_version;

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

#ifndef BE_TEST
    auto src_meta_dir =
            _remote_location_provider->metadata_root_location(src_tablet_id, src_db_id, src_table_id, src_partition_id);
    auto src_data_dir =
            _remote_location_provider->segment_root_location(src_tablet_id, src_db_id, src_table_id, src_partition_id);
    // `shared_src_fs` is used to access storage of src cluster
    auto shared_src_fs = new_fs_starlet(virtual_tablet_id);
    ASSIGN_OR_RETURN(auto src_tablet_meta,
                     build_source_tablet_meta(src_tablet_id, src_visible_version, src_meta_dir, shared_src_fs));
#else
    auto src_meta_dir = "test_lake_replication/meta";
    auto src_data_dir = "test_lake_replication/data";
    auto shared_src_fs_st_or = FileSystem::CreateSharedFromString(src_data_dir);
    auto shared_src_fs = shared_src_fs_st_or.value();
    ASSIGN_OR_RETURN(auto src_tablet_meta,
                     _tablet_manager->get_tablet_metadata(src_tablet_id, src_visible_version, false, 0, nullptr));
#endif

    LOG(INFO) << "Lake replicate storage task, built source meta and data dir, meta dir: " << src_meta_dir
              << ", data dir: " << src_data_dir << ", txn_id: " << txn_id << ", src_tablet_id: " << src_tablet_id
              << ", tablet_id: " << target_tablet_id;

    // `file_locations` is the mapping between source and target file locations,
    // it contains all files that need to replicate from source to target storage
    std::map<std::string, std::string> file_locations;
    // `filename_map` is another mapping between source and target file name,
    // and it's borrowed from lake::ReplicationTxnManager
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;
    // `segment_name_to_size_map` is the mapping between segment file name to its file size
    // we use the `segment_size` field in rowset metadata to get the file size.
    // for history reasons, the `segment_size` field is not always present, so the resulting map is not guaranteed to
    // have all segment file sizes.
    std::unordered_map<std::string, size_t> segment_name_to_size_map;

    auto txn_log = std::make_shared<TxnLog>();

    // fully copy the tablet meta of source cluster
    // only generate and replace file names to adapt for target storage
    ASSIGN_OR_RETURN(auto new_metadata, convert_and_build_new_tablet_meta(
                                                src_tablet_meta, src_tablet_id, target_tablet_id, txn_id, data_version,
                                                src_data_dir, segment_name_to_size_map, file_locations, filename_map));
    txn_log->mutable_op_replication()->mutable_tablet_metadata()->CopyFrom(*new_metadata);

    VLOG(3) << "Lake replicate storage task, have built new tablet meta, tablet_id: " << target_tablet_id
            << ", txn_id:" << txn_id << ", start calculate column unique id map..";
    // calc column unique id to adapt for fast schema change
    const TabletSchemaPB* source_schema_pb = &src_tablet_meta->schema();
    if (source_schema_pb == nullptr) {
        LOG(WARNING) << "Failed to get source schema, tablet source tablet: " << src_tablet_id
                     << ", target tablet: " << target_tablet_id;
        return Status::Corruption("Failed to get source schema");
    }
    std::unordered_map<uint32_t, uint32_t> column_unique_id_map;
    ASSIGN_OR_RETURN(auto target_tablet, _tablet_manager->get_tablet(target_tablet_id));
    ASSIGN_OR_RETURN(auto target_tablet_metadata, target_tablet.get_metadata(target_visible_version));
    ReplicationUtils::calc_column_unique_id_map(source_schema_pb->column(), target_tablet_metadata->schema().column(),
                                                &column_unique_id_map);

    VLOG(3) << "Lake replicate storage task, start to replicate files from src to target cluster, txn_id: " << txn_id;
    std::vector<std::string> files_to_delete;
    CancelableDefer clean_files([&files_to_delete]() { lake::delete_files_async(std::move(files_to_delete)); });

    auto file_converters = lake::ReplicationTxnManager::build_file_converters(_tablet_manager, request, filename_map,
                                                                              column_unique_id_map, files_to_delete);
    for (const auto& pair : filename_map) {
        const auto& src_file_name = pair.first;
        auto src_file_location = join_path(src_data_dir, src_file_name);
        auto it = file_locations.find(src_file_location);
        if (it == file_locations.end()) {
            return Status::Corruption("Found invalid file location, src file location: " + src_file_location);
        }
        const auto& target_file_location = it->second;

        VLOG(3) << "Lake replicate storage task, start to replicate file, src file: " << src_file_location
                << ", target: " << target_file_location;
        // check if need to convert segment file while downloading
        if (is_segment(src_file_name) && !column_unique_id_map.empty()) {
            // file_size might be empty, in that case we'll get it while downloading the file
            auto file_size = segment_name_to_size_map[src_file_name];
            RETURN_IF_ERROR(ReplicationUtils::download_lake_segment_file(src_file_location, src_file_name, file_size,
                                                                         shared_src_fs, file_converters));
        } else {
            // since we can't easily get the file size for non-segment files, here we just use fs::copy operation
            RETURN_IF_ERROR(
                    fs::copy_file(src_file_location, shared_src_fs, target_file_location, nullptr, 1024 * 1024));
        }
        LOG(INFO) << "Finished to replicate lake remote file, src file: " << src_file_location
                  << ", target: " << target_file_location;
    }

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
    // mak full replication for shared-data cluster migration
    txn_meta->set_incremental_snapshot(false);

    RETURN_IF_ERROR(_tablet_manager->put_txn_log(txn_log));

    LOG(INFO) << "Replicated lake remote files, txn_id: " << txn_id << ", tablet_id: " << target_tablet_id
              << ", src_tablet_id: " << src_tablet_id << ", src_db_id: " << src_db_id
              << ", src_table_id: " << src_table_id << ", src_partition_id: " << src_partition_id
              << ", visible_version: " << target_visible_version << ", data_version: " << data_version
              << ", virtual_tablet_id: " << virtual_tablet_id << ", src_visible_version: " << src_visible_version;

    clean_files.cancel();
    return Status::OK();
}

StatusOr<TabletMetadataPtr> LakeReplicationTxnManager::build_source_tablet_meta(
        int64_t src_tablet_id, int64_t version, const std::string& meta_dir,
        const std::shared_ptr<FileSystem>& shared_src_fs) {
    LOG(INFO) << "Lake replicate storage task, building source tablet meta for tablet: " << src_tablet_id
              << ", version: " << version;
    auto src_metadata_file_name = tablet_metadata_filename(src_tablet_id, version);
    auto src_tablet_meta_path = join_path(meta_dir, src_metadata_file_name);
    auto src_tablet_meta_or = _tablet_manager->get_tablet_metadata(src_tablet_meta_path, false, 0, shared_src_fs);
    if (!src_tablet_meta_or.ok()) {
        LOG(WARNING) << "Lake replicate storage task, failed to build source tablet meta for version: " << version
                     << ", src_tablet_id: " << src_tablet_id << ", error: " << src_tablet_meta_or;
        return src_tablet_meta_or;
    }
    return src_tablet_meta_or.value();
}

Status LakeReplicationTxnManager::build_existed_filename_uuids_map(
        const TabletMetadataPtr& target_data_version_tablet_meta,
        std::unordered_map<std::string, std::pair<std::string, std::string>>& existed_filename_uuids) {
    // Collect UUIDs from rowsets (segments and del files)
    for (const auto& rowset : target_data_version_tablet_meta->rowsets()) {
        // the condition is very strict, because currently encryption meta for each segment is not bind to the segment
        // we can only find the encryption meta by index, so the precondition is that the size of segment files
        // is strictly equal to the size of encryption metas
        bool has_encryption_meta = rowset.segments_size() == rowset.segment_encryption_metas_size();
        for (size_t i = 0; i < rowset.segments_size(); ++i) {
            const auto& segment_name = rowset.segments(i);
            if (has_encryption_meta) {
                existed_filename_uuids.emplace(extract_uuid_from(segment_name),
                                               std::make_pair(segment_name, rowset.segment_encryption_metas(i)));
            } else {
                existed_filename_uuids.emplace(extract_uuid_from(segment_name), std::make_pair(segment_name, ""));
            }
        }
        for (const auto& del : rowset.del_files()) {
            const auto& del_filename = del.name();
            existed_filename_uuids.emplace(extract_uuid_from(del_filename),
                                           std::make_pair(del_filename, del.encryption_meta()));
        }
    }

    // Collect UUIDs from SST files
    if (target_data_version_tablet_meta->has_sstable_meta()) {
        const auto& dest_meta = target_data_version_tablet_meta->sstable_meta();
        for (const auto& sst : dest_meta.sstables()) {
            const auto& sst_filename = sst.filename();
            existed_filename_uuids.emplace(extract_uuid_from(sst_filename),
                                           std::make_pair(sst_filename, sst.encryption_meta()));
        }
    }

    // Collect UUIDs from delvec files
    if (target_data_version_tablet_meta->has_delvec_meta()) {
        const auto& dest_meta = target_data_version_tablet_meta->delvec_meta();
        for (const auto& [_, file_meta_pb] : dest_meta.version_to_file()) {
            const auto& delvec_filename = file_meta_pb.name();
            // Note: delvec files don't have separate encryption metas in current implementation
            existed_filename_uuids.emplace(extract_uuid_from(delvec_filename), std::make_pair(delvec_filename, ""));
        }
    }

    // Collect UUIDs from dcg files
    if (target_data_version_tablet_meta->has_dcg_meta()) {
        const auto& dcg_meta = target_data_version_tablet_meta->dcg_meta();
        for (const auto& [_, dcg_ver_pb] : dcg_meta.dcgs()) {
            bool has_encryption_meta = dcg_ver_pb.column_files_size() == dcg_ver_pb.encryption_metas_size();
            for (int i = 0; i < dcg_ver_pb.column_files_size(); ++i) {
                const auto& dcg_filename = dcg_ver_pb.column_files(i);
                if (has_encryption_meta) {
                    existed_filename_uuids.emplace(extract_uuid_from(dcg_filename),
                                                   std::make_pair(dcg_filename, dcg_ver_pb.encryption_metas(i)));
                } else {
                    existed_filename_uuids.emplace(extract_uuid_from(dcg_filename), std::make_pair(dcg_filename, ""));
                }
            }
        }
    }

    return Status::OK();
}

StatusOr<std::shared_ptr<TabletMetadataPB>> LakeReplicationTxnManager::convert_and_build_new_tablet_meta(
        const TabletMetadataPtr& src_tablet_meta, int64_t src_tablet_id, int64_t target_tablet_id,
        TTransactionId txn_id, int64_t data_version, const std::string& src_data_dir,
        std::unordered_map<std::string, size_t>& segment_name_to_size_map,
        std::map<std::string, std::string>& file_locations,
        std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>>& filename_map) {
    LOG(INFO) << "Lake replicate storage task, building new tablet meta for tablet: " << target_tablet_id
              << ", src_tablet_id: " << src_tablet_id << ", txn_id: " << txn_id << ", data_version: " << data_version;
    // find all files that already replicated to target storage in previous txns
    ASSIGN_OR_RETURN(auto target_data_version_tablet_meta,
                     _tablet_manager->get_tablet_metadata(target_tablet_id, data_version, false, 0, nullptr));
    // `existed_filename_uuids` represented files that already replicated to target storage in previous txns
    // <uuid, pair<existed_filename, encryption_meta>>
    std::unordered_map<std::string, std::pair<std::string, std::string>> existed_filename_uuids;
    RETURN_IF_ERROR(build_existed_filename_uuids_map(target_data_version_tablet_meta, existed_filename_uuids));

    VLOG(3) << "Lake replicate storage task, found " << existed_filename_uuids.size() << " existed files";
    // make new metadata
    std::shared_ptr<TabletMetadataPB> new_metadata = std::make_shared<TabletMetadataPB>(*src_tablet_meta);
    // Replace the tablet id with target tablet id
    new_metadata->set_id(target_tablet_id);
    new_metadata->mutable_rowsets()->Clear();
    new_metadata->mutable_dcg_meta()->mutable_dcgs()->clear();
    new_metadata->mutable_sstable_meta()->Clear();

    // deal with segments and dels
    for (const auto& src_rowset_meta : src_tablet_meta->rowsets()) {
        auto new_rowset_meta = new_metadata->add_rowsets();
        new_rowset_meta->CopyFrom(src_rowset_meta);
        new_rowset_meta->mutable_segments()->Clear();
        new_rowset_meta->mutable_segment_encryption_metas()->Clear();
        // check if segment size is valid
        auto segment_size_size = src_rowset_meta.segment_size_size();
        if (segment_size_size > 0) {
            auto segment_file_size = src_rowset_meta.segments_size();
            // `segment_size_size` and `segment_file_size` should always be equal
            if (UNLIKELY(segment_size_size > 0 && segment_size_size != segment_file_size)) {
                return Status::Corruption(
                        fmt::format("found invalidate segment_size, src_tablet_id: {}, "
                                    "rowse_id: {}, segment_size_size: {}, segment_file_size: {}",
                                    src_tablet_id, src_rowset_meta.id(), segment_size_size, segment_file_size));
            }
        }

        // Convert rowset metadata
        for (int i = 0; i < src_rowset_meta.segments_size(); ++i) {
            const auto& src_segment_filename = src_rowset_meta.segments(i);
            std::string final_segment_filename;
            ASSIGN_OR_RETURN(auto is_existed,
                             determine_final_filename(src_segment_filename, txn_id, existed_filename_uuids,
                                                      final_segment_filename, target_tablet_id, src_data_dir,
                                                      file_locations, filename_map));
            new_rowset_meta->add_segments(final_segment_filename);

            // Add encryption metadata for files
            if (config::enable_transparent_data_encryption) {
                if (!is_existed) {
                    // segment file doesn't exist, use the newly generated encryption metadata
                    std::pair<std::string, FileEncryptionPair> pair = filename_map[src_segment_filename];
                    new_rowset_meta->add_segment_encryption_metas(pair.second.encryption_meta);
                } else {
                    // segment file already exists, use the existing encryption metadata from target tablet
                    auto uuid = extract_uuid_from(src_segment_filename);
                    auto it = existed_filename_uuids.find(uuid);
                    if (it != existed_filename_uuids.end()) {
                        const std::string& existing_encryption_meta = it->second.second;
                        new_rowset_meta->add_segment_encryption_metas(existing_encryption_meta);
                    } else {
                        // should never happend
                        return Status::Corruption(fmt::format("no existing encryption metadata found for file: {}",
                                                              src_segment_filename));
                    }
                }
            }

            // build segment_name_to_size_map
            if (segment_size_size > 0) {
                auto segment_size = src_rowset_meta.segment_size(i);
                segment_name_to_size_map.emplace(final_segment_filename, segment_size);
            }
        }

        // Convert dels
        for (int i = 0; i < src_rowset_meta.del_files_size(); ++i) {
            const DelfileWithRowsetId& src_del = src_rowset_meta.del_files(i);
            const auto& src_del_filename = src_del.name();
            std::string final_del_filename;
            ASSIGN_OR_RETURN(auto is_existed, determine_final_filename(src_del_filename, txn_id, existed_filename_uuids,
                                                                       final_del_filename, target_tablet_id,
                                                                       src_data_dir, file_locations, filename_map));
            auto* new_del = new_rowset_meta->add_del_files();
            new_del->CopyFrom(src_del);
            new_del->set_name(final_del_filename);

            if (config::enable_transparent_data_encryption) {
                if (!is_existed) {
                    // del doesn't exist, use the newly generated encryption metadata
                    std::pair<std::string, FileEncryptionPair> pair = filename_map[src_del_filename];
                    new_del->set_encryption_meta(pair.second.encryption_meta);
                } else {
                    // del already exists, use the existing encryption metadata from target tablet
                    auto uuid = extract_uuid_from(src_del_filename);
                    auto it = existed_filename_uuids.find(uuid);
                    if (it != existed_filename_uuids.end()) {
                        const std::string& existing_encryption_meta = it->second.second;
                        new_del->set_encryption_meta(existing_encryption_meta);
                    }
                }
            }
        }
    }

    // deal with sstable
    if (src_tablet_meta->has_sstable_meta()) {
        PersistentIndexSstableMetaPB* dest_meta = new_metadata->mutable_sstable_meta();
        dest_meta->CopyFrom(src_tablet_meta->sstable_meta());
        for (int i = 0; i < dest_meta->sstables_size(); ++i) {
            PersistentIndexSstablePB* sst = dest_meta->mutable_sstables(i);
            const auto& src_sst_filename = sst->filename();
            std::string final_sst_filename;
            ASSIGN_OR_RETURN(auto is_existed, determine_final_filename(src_sst_filename, txn_id, existed_filename_uuids,
                                                                       final_sst_filename, target_tablet_id,
                                                                       src_data_dir, file_locations, filename_map));
            sst->set_filename(final_sst_filename);

            if (config::enable_transparent_data_encryption) {
                if (!is_existed) {
                    // sst doesn't exist, use the newly generated encryption metadata
                    std::pair<std::string, FileEncryptionPair> pair = filename_map[src_sst_filename];
                    sst->set_encryption_meta(pair.second.encryption_meta);
                } else {
                    // sst already exists, use the existing encryption metadata from target tablet
                    auto uuid = extract_uuid_from(src_sst_filename);
                    auto it = existed_filename_uuids.find(uuid);
                    if (it != existed_filename_uuids.end()) {
                        const std::string& existing_encryption_meta = it->second.second;
                        sst->set_encryption_meta(existing_encryption_meta);
                    }
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
            std::string final_delvec_filename;
            ASSIGN_OR_RETURN(
                    auto is_existed,
                    determine_final_filename(src_delvec_filename, txn_id, existed_filename_uuids, final_delvec_filename,
                                             target_tablet_id, src_data_dir, file_locations, filename_map));
            auto& item = (*dest_meta->mutable_version_to_file())[version];
            item.set_name(final_delvec_filename);

            if (config::enable_transparent_data_encryption) {
                if (!is_existed) {
                    // del file doesn't exist, use the newly generated encryption metadata
                    std::pair<std::string, FileEncryptionPair> pair = filename_map[src_delvec_filename];
                    item.set_encryption_meta(pair.second.encryption_meta);
                } else {
                    // del file already exists, use the existing encryption metadata from target tablet
                    auto uuid = extract_uuid_from(src_delvec_filename);
                    auto it = existed_filename_uuids.find(uuid);
                    if (it != existed_filename_uuids.end()) {
                        const std::string& existing_encryption_meta = it->second.second;
                        item.set_encryption_meta(existing_encryption_meta);
                    }
                }
            }
        }
    }

    // deal with dcg
    if (src_tablet_meta->has_dcg_meta()) {
        DeltaColumnGroupMetadataPB* dest_meta = new_metadata->mutable_dcg_meta();
        dest_meta->CopyFrom(src_tablet_meta->dcg_meta());
        for (auto& [segment_id, dcg_ver_pb] : *dest_meta->mutable_dcgs()) {
            for (int i = 0; i < dcg_ver_pb.column_files_size(); ++i) {
                auto src_dcg_filename = dcg_ver_pb.column_files(i);
                std::string final_dcg_filename;
                ASSIGN_OR_RETURN(
                        auto is_existed,
                        determine_final_filename(src_dcg_filename, txn_id, existed_filename_uuids, final_dcg_filename,
                                                 target_tablet_id, src_data_dir, file_locations, filename_map));
                dcg_ver_pb.set_column_files(i, final_dcg_filename);

                if (config::enable_transparent_data_encryption) {
                    if (!is_existed) {
                        // dcg file doesn't exist, use the newly generated encryption metadata
                        std::pair<std::string, FileEncryptionPair> pair = filename_map[src_dcg_filename];
                        if (dcg_ver_pb.encryption_metas_size() > i) {
                            dcg_ver_pb.set_encryption_metas(i, pair.second.encryption_meta);
                        } else {
                            dcg_ver_pb.add_encryption_metas(pair.second.encryption_meta);
                        }
                    } else {
                        // dcg file already exists, use the existing encryption metadata from target tablet
                        auto uuid = extract_uuid_from(src_dcg_filename);
                        auto it = existed_filename_uuids.find(uuid);
                        if (it != existed_filename_uuids.end()) {
                            const std::string& existing_encryption_meta = it->second.second;
                            if (dcg_ver_pb.encryption_metas_size() > i) {
                                dcg_ver_pb.set_encryption_metas(i, existing_encryption_meta);
                            } else {
                                dcg_ver_pb.add_encryption_metas(existing_encryption_meta);
                            }
                        }
                    }
                }
            }
        }
    }

    return new_metadata;
}

StatusOr<bool> LakeReplicationTxnManager::determine_final_filename(
        const std::string& src_filename, TTransactionId txn_id,
        const std::unordered_map<std::string, std::pair<std::string, std::string>>& existed_filename_uuids,
        std::string& final_filename, const int64_t target_tablet_id, const std::string& src_data_dir,
        std::map<std::string, std::string>& file_locations,
        std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>>& filename_map) {
    auto uuid = extract_uuid_from(src_filename);
    auto it = existed_filename_uuids.find(uuid);
    if (it != existed_filename_uuids.end()) {
        // UUID exists, use the existing target filename
        final_filename = it->second.first; // pair.first is the filename
        LOG(INFO) << "File: " << src_filename
                  << " already exists on target cluster, use existing target filename: " << final_filename;
        return true;
    }

    // UUID not exists, generate new filename
    final_filename = gen_filename_from(txn_id, src_filename);
    if (UNLIKELY(final_filename.empty())) {
        return Status::Corruption("Failed to generate new filename from: " + src_filename);
    }
    LOG(INFO) << "Generated new file: " << final_filename << " for src file: " << src_filename;

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

} // namespace starrocks::lake
