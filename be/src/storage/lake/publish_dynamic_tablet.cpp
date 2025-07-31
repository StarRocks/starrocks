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

#include "storage/lake/publish_dynamic_tablet.h"

#include <span>

#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"

namespace starrocks::lake {

TxnLogPtr convert_txn_log_for_dynamic_tablet(const TxnLogPtr& original_txn_log,
                                             const TabletMetadataPtr& base_tablet_metadata,
                                             PublishTabletInfo::PublishTabletType publish_tablet_type) {
    if (publish_tablet_type == PublishTabletInfo::PUBLISH_NORMAL) {
        return original_txn_log;
    }

    // Copy a new txn log from original txn log
    auto new_txn_log = std::make_shared<TxnLogPB>(*original_txn_log);
    new_txn_log->set_tablet_id(base_tablet_metadata->id());

    if (publish_tablet_type == PublishTabletInfo::SPLITTING_TABLET) {
        const RecordPredicatePB* record_predicate = nullptr;

        const auto& rowsets = base_tablet_metadata->rowsets();
        for (auto it = rowsets.rbegin(); it != rowsets.rend(); ++it) {
            const auto& rowset = *it;
            if (rowset.has_record_predicate()) {
                record_predicate = &rowset.record_predicate();
                break;
            }
        }

        if (record_predicate != nullptr) {
            auto handle_rowset_metadata = [record_predicate](RowsetMetadataPB* rowset_metadata) {
                // Set record predicate
                rowset_metadata->mutable_record_predicate()->CopyFrom(*record_predicate);

                // Set dat files shared
                auto* shared_segments = rowset_metadata->mutable_shared_segments();
                shared_segments->Clear();
                shared_segments->Resize(rowset_metadata->segments_size(), true);

                // Set del files shared
                for (auto& del : *rowset_metadata->mutable_del_files()) {
                    del.set_shared(true);
                }
            };

            if (new_txn_log->has_op_write()) {
                auto* op_write = new_txn_log->mutable_op_write();
                if (op_write->has_rowset()) {
                    handle_rowset_metadata(op_write->mutable_rowset());
                }
            }

            if (new_txn_log->has_op_compaction()) {
                auto* op_compaction = new_txn_log->mutable_op_compaction();
                if (op_compaction->has_output_rowset()) {
                    handle_rowset_metadata(op_compaction->mutable_output_rowset());
                }
                if (op_compaction->has_output_sstable()) {
                    auto* sstable = op_compaction->mutable_output_sstable();
                    sstable->mutable_predicate()->mutable_record_predicate()->CopyFrom(*record_predicate);
                    sstable->set_shared(true);
                }
            }

            if (new_txn_log->has_op_schema_change()) {
                auto* op_schema_change = new_txn_log->mutable_op_schema_change();
                for (auto& rowset : *op_schema_change->mutable_rowsets()) {
                    handle_rowset_metadata(&rowset);
                }
                if (op_schema_change->has_delvec_meta()) {
                    for (auto& pair : *op_schema_change->mutable_delvec_meta()->mutable_version_to_file()) {
                        pair.second.set_shared(true);
                    }
                }
            }

            if (new_txn_log->has_op_replication()) {
                for (auto& op_write : *new_txn_log->mutable_op_replication()->mutable_op_writes()) {
                    if (op_write.has_rowset()) {
                        handle_rowset_metadata(op_write.mutable_rowset());
                    }
                }
            }
        }
    }

    return new_txn_log;
}

static void set_all_data_files_shared(TabletMetadataPB* tablet_metadata) {
    for (auto& rowset_metadata : *tablet_metadata->mutable_rowsets()) {
        // dat
        auto* shared_segments = rowset_metadata.mutable_shared_segments();
        shared_segments->Clear();
        shared_segments->Resize(rowset_metadata.segments_size(), true);

        // del
        for (auto& del : *rowset_metadata.mutable_del_files()) {
            del.set_shared(true);
        }
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

StatusOr<std::unordered_map<int64_t, TabletMetadataPtr>> publish_splitting_tablet(
        TabletManager* tablet_manager, const std::span<std::string>& distribution_columns,
        const SplittingTabletInfoPB& splitting_tablet, int64_t base_version, int64_t new_version,
        const TxnInfoPB& txn_info, bool skip_write_tablet_metadata) {
    std::unordered_map<int64_t, TabletMetadataPtr> new_metadatas;

    // Check tablet metadata cache
    do {
        auto old_tablet_new_metadata_location =
                tablet_manager->tablet_metadata_location(splitting_tablet.old_tablet_id(), new_version);
        auto cached_old_tablet_new_metadata =
                tablet_manager->metacache()->lookup_tablet_metadata(old_tablet_new_metadata_location);
        if (cached_old_tablet_new_metadata == nullptr) {
            break;
        }

        new_metadatas.emplace(splitting_tablet.old_tablet_id(), std::move(cached_old_tablet_new_metadata));

        for (auto new_tablet_id : splitting_tablet.new_tablet_ids()) {
            auto new_tablet_new_metadata_location =
                    tablet_manager->tablet_metadata_location(new_tablet_id, new_version);
            auto cached_new_tablet_new_metadata =
                    tablet_manager->metacache()->lookup_tablet_metadata(new_tablet_new_metadata_location);
            if (cached_new_tablet_new_metadata == nullptr) {
                break;
            }

            new_metadatas.emplace(new_tablet_id, std::move(cached_new_tablet_new_metadata));
        }

        return new_metadatas;
    } while (false);

    new_metadatas.clear();

    auto old_tablet_old_metadata_or =
            tablet_manager->get_tablet_metadata(splitting_tablet.old_tablet_id(), base_version, false);
    if (old_tablet_old_metadata_or.status().is_not_found()) {
        // Check new metadata
        auto old_tablet_new_metadata_or =
                tablet_manager->get_tablet_metadata(splitting_tablet.old_tablet_id(), new_version, txn_info.gtid());
        if (!old_tablet_new_metadata_or.ok()) {
            return old_tablet_old_metadata_or.status();
        }

        new_metadatas.emplace(splitting_tablet.old_tablet_id(), std::move(old_tablet_new_metadata_or.value()));

        for (auto new_tablet_id : splitting_tablet.new_tablet_ids()) {
            auto new_tablet_new_metadata_or =
                    tablet_manager->get_tablet_metadata(new_tablet_id, new_version, txn_info.gtid());
            if (!new_tablet_new_metadata_or.ok()) {
                return old_tablet_old_metadata_or.status();
            }

            new_metadatas.emplace(new_tablet_id, std::move(new_tablet_new_metadata_or.value()));
        }

        return new_metadatas;
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
    for (int64_t new_tablet_index = 0; auto new_tablet_id : splitting_tablet.new_tablet_ids()) {
        auto new_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_old_metadata);
        new_tablet_new_metadata->set_id(new_tablet_id);
        new_tablet_new_metadata->set_version(new_version);
        new_tablet_new_metadata->set_commit_time(txn_info.commit_time());
        new_tablet_new_metadata->set_gtid(txn_info.gtid());
        set_all_data_files_shared(new_tablet_new_metadata.get());

        auto set_record_predicate = [&](RecordPredicatePB* record_predicate) {
            record_predicate->set_type(RecordPredicatePB::COLUMN_HASH_IS_CONGRUENT);

            auto* column_hash_is_congruent = record_predicate->mutable_column_hash_is_congruent();
            if (column_hash_is_congruent->has_modulus()) {
                DCHECK(column_hash_is_congruent->has_remainder() &&
                       column_hash_is_congruent->column_names_size() == distribution_columns.size());
                column_hash_is_congruent->set_modulus(column_hash_is_congruent->modulus() *
                                                      splitting_tablet.new_tablet_ids_size());
                column_hash_is_congruent->set_remainder(column_hash_is_congruent->remainder() *
                                                                splitting_tablet.new_tablet_ids_size() +
                                                        new_tablet_index);
            } else {
                column_hash_is_congruent->set_modulus(splitting_tablet.new_tablet_ids_size());
                column_hash_is_congruent->set_remainder(new_tablet_index);
                for (const auto& column : distribution_columns) {
                    column_hash_is_congruent->add_column_names(column);
                }
            }
        };

        // Set record predicate for rowsets
        for (auto& rowset_metadata : *new_tablet_new_metadata->mutable_rowsets()) {
            set_record_predicate(rowset_metadata.mutable_record_predicate());
            rowset_metadata.set_num_rows(rowset_metadata.num_rows() / splitting_tablet.new_tablet_ids_size());
            rowset_metadata.set_data_size(rowset_metadata.data_size() / splitting_tablet.new_tablet_ids_size());
        }

        // Set record predicate for ssts
        if (new_tablet_new_metadata->has_sstable_meta()) {
            for (auto& sst : *new_tablet_new_metadata->mutable_sstable_meta()->mutable_sstables()) {
                set_record_predicate(sst.mutable_predicate()->mutable_record_predicate());
            }
        }

        new_metadatas.emplace(new_tablet_id, std::move(new_tablet_new_metadata));

        ++new_tablet_index;
    }

    for (const auto& [tablet_id, new_metadata] : new_metadatas) {
        if (!skip_write_tablet_metadata) {
            RETURN_IF_ERROR(tablet_manager->put_tablet_metadata(new_metadata));
        } else {
            RETURN_IF_ERROR(tablet_manager->cache_tablet_metadata(new_metadata));
            tablet_manager->metacache()->cache_aggregation_partition(
                    tablet_manager->tablet_metadata_root_location(tablet_id), true);
        }
    }

    return new_metadatas;
}

StatusOr<std::unordered_map<int64_t, TabletMetadataPtr>> publish_merging_tablet(
        TabletManager* tablet_manager, const std::span<std::string>& distribution_columns,
        const MergingTabletInfoPB& merging_tablet, int64_t base_version, int64_t new_version, const TxnInfoPB& txn_info,
        bool skip_write_tablet_metadata) {
    return Status::NotSupported("Not implemented");
}

StatusOr<std::unordered_map<int64_t, TabletMetadataPtr>> publish_identical_tablet(
        TabletManager* tablet_manager, const IdenticalTabletInfoPB& identical_tablet, int64_t base_version,
        int64_t new_version, const TxnInfoPB& txn_info, bool skip_write_tablet_metadata) {
    std::unordered_map<int64_t, TabletMetadataPtr> new_metadatas;

    // Check tablet metadata cache
    do {
        auto old_tablet_new_metadata_location =
                tablet_manager->tablet_metadata_location(identical_tablet.old_tablet_id(), new_version);
        auto cached_old_tablet_new_metadata =
                tablet_manager->metacache()->lookup_tablet_metadata(old_tablet_new_metadata_location);
        if (cached_old_tablet_new_metadata == nullptr) {
            break;
        }

        new_metadatas.emplace(identical_tablet.old_tablet_id(), std::move(cached_old_tablet_new_metadata));

        auto new_tablet_new_metadata_location =
                tablet_manager->tablet_metadata_location(identical_tablet.new_tablet_id(), new_version);
        auto cached_new_tablet_new_metadata =
                tablet_manager->metacache()->lookup_tablet_metadata(new_tablet_new_metadata_location);
        if (cached_new_tablet_new_metadata == nullptr) {
            break;
        }

        new_metadatas.emplace(identical_tablet.new_tablet_id(), std::move(cached_new_tablet_new_metadata));

        return new_metadatas;
    } while (false);

    new_metadatas.clear();

    auto old_tablet_old_metadata_or =
            tablet_manager->get_tablet_metadata(identical_tablet.old_tablet_id(), base_version, false);
    if (old_tablet_old_metadata_or.status().is_not_found()) {
        // Check new metadata
        auto old_tablet_new_metadata_or =
                tablet_manager->get_tablet_metadata(identical_tablet.old_tablet_id(), new_version, txn_info.gtid());
        if (!old_tablet_new_metadata_or.ok()) {
            return old_tablet_old_metadata_or.status();
        }

        new_metadatas.emplace(identical_tablet.old_tablet_id(), std::move(old_tablet_new_metadata_or.value()));

        auto new_tablet_new_metadata_or =
                tablet_manager->get_tablet_metadata(identical_tablet.new_tablet_id(), new_version, txn_info.gtid());
        if (!new_tablet_new_metadata_or.ok()) {
            return old_tablet_old_metadata_or.status();
        }

        new_metadatas.emplace(identical_tablet.new_tablet_id(), std::move(new_tablet_new_metadata_or.value()));

        return new_metadatas;
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

        new_metadatas.emplace(identical_tablet.new_tablet_id(), std::move(new_tablet_new_metadata));
    }

    for (const auto& [tablet_id, new_metadata] : new_metadatas) {
        if (!skip_write_tablet_metadata) {
            RETURN_IF_ERROR(tablet_manager->put_tablet_metadata(new_metadata));
        } else {
            RETURN_IF_ERROR(tablet_manager->cache_tablet_metadata(new_metadata));
            tablet_manager->metacache()->cache_aggregation_partition(
                    tablet_manager->tablet_metadata_root_location(tablet_id), true);
        }
    }

    return new_metadatas;
}

} // namespace starrocks::lake
