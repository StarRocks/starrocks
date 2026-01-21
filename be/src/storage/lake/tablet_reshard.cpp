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

#include "storage/lake/tablet_reshard.h"

#include <span>

#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"

namespace starrocks::lake {

std::ostream& operator<<(std::ostream& out, const PublishTabletInfo& tablet_info) {
    if (tablet_info.get_publish_tablet_type() == PublishTabletInfo::PUBLISH_NORMAL) {
        return out << "{tablet_id: " << tablet_info.get_tablet_id_in_metadata() << '}';
    }

    return out << "{publish_tablet_type: " << (int)tablet_info.get_publish_tablet_type()
               << ", tablet_id_in_metadata: " << tablet_info.get_tablet_id_in_metadata()
               << ", tablet_id_in_txn_log: " << tablet_info.get_tablet_id_in_txn_log() << '}';
}

static void set_all_data_files_shared(RowsetMetadataPB* rowset_metadata) {
    // Set dat files shared
    auto* shared_segments = rowset_metadata->mutable_shared_segments();
    shared_segments->Clear();
    shared_segments->Resize(rowset_metadata->segments_size(), true);

    // Set del files shared
    for (auto& del : *rowset_metadata->mutable_del_files()) {
        del.set_shared(true);
    }
}

static void set_all_data_files_shared(TxnLogPB* txn_log) {
    if (txn_log->has_op_write()) {
        auto* op_write = txn_log->mutable_op_write();
        if (op_write->has_rowset()) {
            set_all_data_files_shared(op_write->mutable_rowset());
        }
    }

    if (txn_log->has_op_compaction()) {
        auto* op_compaction = txn_log->mutable_op_compaction();
        if (op_compaction->has_output_rowset()) {
            set_all_data_files_shared(op_compaction->mutable_output_rowset());
        }
        if (op_compaction->has_output_sstable()) {
            auto* sstable = op_compaction->mutable_output_sstable();
            sstable->set_shared(true);
        }
    }

    if (txn_log->has_op_schema_change()) {
        auto* op_schema_change = txn_log->mutable_op_schema_change();
        for (auto& rowset : *op_schema_change->mutable_rowsets()) {
            set_all_data_files_shared(&rowset);
        }
        if (op_schema_change->has_delvec_meta()) {
            for (auto& pair : *op_schema_change->mutable_delvec_meta()->mutable_version_to_file()) {
                pair.second.set_shared(true);
            }
        }
    }

    if (txn_log->has_op_replication()) {
        for (auto& op_write : *txn_log->mutable_op_replication()->mutable_op_writes()) {
            if (op_write.has_rowset()) {
                set_all_data_files_shared(op_write.mutable_rowset());
            }
        }
    }
}

static void set_all_data_files_shared(TabletMetadataPB* tablet_metadata) {
    // rowset (dat and del)
    for (auto& rowset_metadata : *tablet_metadata->mutable_rowsets()) {
        set_all_data_files_shared(&rowset_metadata);
    }

    // delvec
    if (tablet_metadata->has_delvec_meta()) {
        for (auto& pair : *tablet_metadata->mutable_delvec_meta()->mutable_version_to_file()) {
            pair.second.set_shared(true);
        }
    }

    // dcg
    if (tablet_metadata->has_dcg_meta()) {
        for (auto& dcg : *tablet_metadata->mutable_dcg_meta()->mutable_dcgs()) {
            auto* shared_files = dcg.second.mutable_shared_files();
            shared_files->Clear();
            shared_files->Resize(dcg.second.column_files_size(), true);
        }
    }

    // sst
    if (tablet_metadata->has_sstable_meta()) {
        for (auto& sstable : *tablet_metadata->mutable_sstable_meta()->mutable_sstables()) {
            sstable.set_shared(true);
        }
    }
}

static Status handle_splitting_tablet(TabletManager* tablet_manager, const SplittingTabletInfoPB& splitting_tablet,
                                      int64_t base_version, int64_t new_version, const TxnInfoPB& txn_info,
                                      std::unordered_map<int64_t, TabletMetadataPtr>& new_metadatas,
                                      std::unordered_map<int64_t, TabletRangePB>& tablet_ranges) {
    // Check tablet metadata cache
    {
        auto old_tablet_new_metadata_location =
                tablet_manager->tablet_metadata_location(splitting_tablet.old_tablet_id(), new_version);
        auto cached_old_tablet_new_metadata =
                tablet_manager->metacache()->lookup_tablet_metadata(old_tablet_new_metadata_location);
        if (cached_old_tablet_new_metadata == nullptr) {
            goto CONTINUE_HANDLE_SPLITTING_TABLET;
        }

        new_metadatas.emplace(splitting_tablet.old_tablet_id(), std::move(cached_old_tablet_new_metadata));

        for (auto new_tablet_id : splitting_tablet.new_tablet_ids()) {
            auto new_tablet_new_metadata_location =
                    tablet_manager->tablet_metadata_location(new_tablet_id, new_version);
            auto cached_new_tablet_new_metadata =
                    tablet_manager->metacache()->lookup_tablet_metadata(new_tablet_new_metadata_location);
            if (cached_new_tablet_new_metadata == nullptr) {
                new_metadatas.clear();
                tablet_ranges.clear();
                goto CONTINUE_HANDLE_SPLITTING_TABLET;
            }

            new_metadatas.emplace(new_tablet_id, std::move(cached_new_tablet_new_metadata));
            tablet_ranges.emplace(new_tablet_id, TabletRangePB());
        }

        // All new metadatas found in cache, return ok
        return Status::OK();
    }

CONTINUE_HANDLE_SPLITTING_TABLET:

    auto old_tablet_old_metadata_or =
            tablet_manager->get_tablet_metadata(splitting_tablet.old_tablet_id(), base_version, false);
    if (old_tablet_old_metadata_or.status().is_not_found()) {
        // Check new metadata
        auto old_tablet_new_metadata_or =
                tablet_manager->get_tablet_metadata(splitting_tablet.old_tablet_id(), new_version, txn_info.gtid());
        if (!old_tablet_new_metadata_or.ok()) {
            // Return old metadata not found error
            return old_tablet_old_metadata_or.status();
        }

        new_metadatas.emplace(splitting_tablet.old_tablet_id(), std::move(old_tablet_new_metadata_or.value()));

        for (auto new_tablet_id : splitting_tablet.new_tablet_ids()) {
            auto new_tablet_new_metadata_or =
                    tablet_manager->get_tablet_metadata(new_tablet_id, new_version, txn_info.gtid());
            if (!new_tablet_new_metadata_or.ok()) {
                // Return old metadata not found error
                return old_tablet_old_metadata_or.status();
            }

            new_metadatas.emplace(new_tablet_id, std::move(new_tablet_new_metadata_or.value()));
            tablet_ranges.emplace(new_tablet_id, TabletRangePB());
        }

        // All new metadatas found, return ok
        return Status::OK();
    }

    if (!old_tablet_old_metadata_or.ok()) {
        LOG(WARNING) << "Failed to get tablet: " << splitting_tablet.old_tablet_id() << ", version: " << base_version
                     << ", status: " << old_tablet_old_metadata_or.status();
        return old_tablet_old_metadata_or.status();
    }

    const auto& old_tablet_old_metadata = old_tablet_old_metadata_or.value();

    // Old tablet
    {
        auto old_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_old_metadata);
        old_tablet_new_metadata->set_version(new_version);
        old_tablet_new_metadata->set_commit_time(txn_info.commit_time());
        old_tablet_new_metadata->set_gtid(txn_info.gtid());
        set_all_data_files_shared(old_tablet_new_metadata.get());

        new_metadatas.emplace(splitting_tablet.old_tablet_id(), std::move(old_tablet_new_metadata));
    }

    // New tablets
    for (auto new_tablet_id : splitting_tablet.new_tablet_ids()) {
        auto new_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_old_metadata);
        new_tablet_new_metadata->set_id(new_tablet_id);
        new_tablet_new_metadata->set_version(new_version);
        new_tablet_new_metadata->set_commit_time(txn_info.commit_time());
        new_tablet_new_metadata->set_gtid(txn_info.gtid());
        set_all_data_files_shared(new_tablet_new_metadata.get());

        // Update num rows and data size for rowsets
        for (auto& rowset_metadata : *new_tablet_new_metadata->mutable_rowsets()) {
            rowset_metadata.set_num_rows(rowset_metadata.num_rows() / splitting_tablet.new_tablet_ids_size());
            rowset_metadata.set_data_size(rowset_metadata.data_size() / splitting_tablet.new_tablet_ids_size());
        }

        new_metadatas.emplace(new_tablet_id, std::move(new_tablet_new_metadata));
        // TODO: Get split point and construct new ranges
        tablet_ranges.emplace(new_tablet_id, TabletRangePB());
    }

    return Status::OK();
}

static Status handle_merging_tablet(TabletManager* tablet_manager, const MergingTabletInfoPB& merging_tablet,
                                    int64_t base_version, int64_t new_version, const TxnInfoPB& txn_info,
                                    std::unordered_map<int64_t, TabletMetadataPtr>& new_metadatas) {
    return Status::NotSupported("Tablet merging is not implemented");
}

static Status handle_identical_tablet(TabletManager* tablet_manager, const IdenticalTabletInfoPB& identical_tablet,
                                      int64_t base_version, int64_t new_version, const TxnInfoPB& txn_info,
                                      std::unordered_map<int64_t, TabletMetadataPtr>& new_metadatas) {
    // Check tablet metadata cache
    {
        auto old_tablet_new_metadata_location =
                tablet_manager->tablet_metadata_location(identical_tablet.old_tablet_id(), new_version);
        auto cached_old_tablet_new_metadata =
                tablet_manager->metacache()->lookup_tablet_metadata(old_tablet_new_metadata_location);
        if (cached_old_tablet_new_metadata == nullptr) {
            goto CONTINUE_HANDLE_IDENTICAL_TABLET;
        }

        new_metadatas.emplace(identical_tablet.old_tablet_id(), std::move(cached_old_tablet_new_metadata));

        auto new_tablet_new_metadata_location =
                tablet_manager->tablet_metadata_location(identical_tablet.new_tablet_id(), new_version);
        auto cached_new_tablet_new_metadata =
                tablet_manager->metacache()->lookup_tablet_metadata(new_tablet_new_metadata_location);
        if (cached_new_tablet_new_metadata == nullptr) {
            new_metadatas.clear();
            goto CONTINUE_HANDLE_IDENTICAL_TABLET;
        }

        new_metadatas.emplace(identical_tablet.new_tablet_id(), std::move(cached_new_tablet_new_metadata));

        // All new metadatas found in cache, return ok
        return Status::OK();
    }

CONTINUE_HANDLE_IDENTICAL_TABLET:

    auto old_tablet_old_metadata_or =
            tablet_manager->get_tablet_metadata(identical_tablet.old_tablet_id(), base_version, false);
    if (old_tablet_old_metadata_or.status().is_not_found()) {
        // Check new metadata
        auto old_tablet_new_metadata_or =
                tablet_manager->get_tablet_metadata(identical_tablet.old_tablet_id(), new_version, txn_info.gtid());
        if (!old_tablet_new_metadata_or.ok()) {
            // Return old metadata not found error
            return old_tablet_old_metadata_or.status();
        }

        new_metadatas.emplace(identical_tablet.old_tablet_id(), std::move(old_tablet_new_metadata_or.value()));

        auto new_tablet_new_metadata_or =
                tablet_manager->get_tablet_metadata(identical_tablet.new_tablet_id(), new_version, txn_info.gtid());
        if (!new_tablet_new_metadata_or.ok()) {
            // Return old metadata not found error
            return old_tablet_old_metadata_or.status();
        }

        new_metadatas.emplace(identical_tablet.new_tablet_id(), std::move(new_tablet_new_metadata_or.value()));

        // All new metadatas found, return ok
        return Status::OK();
    }

    if (!old_tablet_old_metadata_or.ok()) {
        LOG(WARNING) << "Failed to get tablet: " << identical_tablet.old_tablet_id() << ", version: " << base_version
                     << ", status: " << old_tablet_old_metadata_or.status();
        return old_tablet_old_metadata_or.status();
    }

    const auto& old_tablet_old_metadata = old_tablet_old_metadata_or.value();

    // Old tablet
    {
        auto old_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_old_metadata);
        old_tablet_new_metadata->set_version(new_version);
        old_tablet_new_metadata->set_commit_time(txn_info.commit_time());
        old_tablet_new_metadata->set_gtid(txn_info.gtid());
        set_all_data_files_shared(old_tablet_new_metadata.get());

        new_metadatas.emplace(identical_tablet.old_tablet_id(), std::move(old_tablet_new_metadata));
    }

    // New tablet
    {
        auto new_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_old_metadata);
        new_tablet_new_metadata->set_id(identical_tablet.new_tablet_id());
        new_tablet_new_metadata->set_version(new_version);
        new_tablet_new_metadata->set_commit_time(txn_info.commit_time());
        new_tablet_new_metadata->set_gtid(txn_info.gtid());
        // New tablet in identical tablet need not to share data files

        new_metadatas.emplace(identical_tablet.new_tablet_id(), std::move(new_tablet_new_metadata));
    }

    return Status::OK();
}

TxnLogPtr convert_txn_log(const TxnLogPtr& txn_log, const TabletMetadataPtr& base_tablet_metadata,
                          const PublishTabletInfo& publish_tablet_info) {
    if (publish_tablet_info.get_publish_tablet_type() == PublishTabletInfo::PUBLISH_NORMAL) {
        return txn_log;
    }

    // Copy a new txn log from original txn log
    auto new_txn_log = std::make_shared<TxnLogPB>(*txn_log);
    new_txn_log->set_tablet_id(publish_tablet_info.get_tablet_id_in_metadata());

    // For tablet splitting, set all data files shared in new txn log
    if (publish_tablet_info.get_publish_tablet_type() == PublishTabletInfo::SPLITTING_TABLET) {
        set_all_data_files_shared(new_txn_log.get());
    }

    return new_txn_log;
}

Status publish_resharding_tablet(TabletManager* tablet_manager, const ReshardingTabletInfoPB& resharding_tablet,
                                 int64_t base_version, int64_t new_version, const TxnInfoPB& txn_info,
                                 bool skip_write_tablet_metadata,
                                 std::unordered_map<int64_t, TabletMetadataPtr>& tablet_metadatas,
                                 std::unordered_map<int64_t, TabletRangePB>& tablet_ranges) {
    if (resharding_tablet.has_splitting_tablet_info()) {
        RETURN_IF_ERROR(handle_splitting_tablet(tablet_manager, resharding_tablet.splitting_tablet_info(), base_version,
                                                new_version, txn_info, tablet_metadatas, tablet_ranges));
    } else if (resharding_tablet.has_merging_tablet_info()) {
        RETURN_IF_ERROR(handle_merging_tablet(tablet_manager, resharding_tablet.merging_tablet_info(), base_version,
                                              new_version, txn_info, tablet_metadatas));
    } else if (resharding_tablet.has_identical_tablet_info()) {
        RETURN_IF_ERROR(handle_identical_tablet(tablet_manager, resharding_tablet.identical_tablet_info(), base_version,
                                                new_version, txn_info, tablet_metadatas));
    }

    for (const auto& [tablet_id, new_metadata] : tablet_metadatas) {
        if (!skip_write_tablet_metadata) {
            RETURN_IF_ERROR(tablet_manager->put_tablet_metadata(new_metadata));
        } else {
            RETURN_IF_ERROR(tablet_manager->cache_tablet_metadata(new_metadata));
            tablet_manager->metacache()->cache_aggregation_partition(
                    tablet_manager->tablet_metadata_root_location(tablet_id), true);
        }
    }

    return Status::OK();
}

} // namespace starrocks::lake
