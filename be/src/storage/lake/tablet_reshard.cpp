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

#include <unordered_map>

#include "common/logging.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_merger.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/lake/tablet_splitter.h"

namespace starrocks::lake {

namespace {

std::ostream& operator<<(std::ostream& out, const std::vector<int64_t>& tablet_ids) {
    out << '[';
    for (size_t i = 0; i < tablet_ids.size(); ++i) {
        if (i > 0) {
            out << ", ";
        }
        out << tablet_ids[i];
    }
    out << ']';
    return out;
}

Status handle_splitting_tablet(TabletManager* tablet_manager, const SplittingTabletInfoPB& splitting_tablet,
                               int64_t base_version, int64_t new_version, const TxnInfoPB& txn_info,
                               std::unordered_map<int64_t, TabletMetadataPtr>& new_metadatas,
                               std::unordered_map<int64_t, TabletRangePB>& tablet_ranges) {
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
            tablet_ranges.emplace(new_tablet_id, cached_new_tablet_new_metadata->range());
            new_metadatas.emplace(new_tablet_id, std::move(cached_new_tablet_new_metadata));
        }
        return Status::OK();
    }

CONTINUE_HANDLE_SPLITTING_TABLET:
    auto old_tablet_old_metadata_or =
            tablet_manager->get_tablet_metadata(splitting_tablet.old_tablet_id(), base_version, false);
    if (old_tablet_old_metadata_or.status().is_not_found()) {
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
            auto& new_tablet_new_metadata = new_tablet_new_metadata_or.value();
            tablet_ranges.emplace(new_tablet_id, new_tablet_new_metadata->range());
            new_metadatas.emplace(new_tablet_id, std::move(new_tablet_new_metadata));
        }
        return Status::OK();
    }

    if (!old_tablet_old_metadata_or.ok()) {
        LOG(WARNING) << "Failed to get tablet: " << splitting_tablet.old_tablet_id() << ", version: " << base_version
                     << ", txn_id: " << txn_info.txn_id() << ", status: " << old_tablet_old_metadata_or.status();
        return old_tablet_old_metadata_or.status();
    }

    const auto& old_tablet_old_metadata = old_tablet_old_metadata_or.value();

    auto old_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_old_metadata);
    old_tablet_new_metadata->set_version(new_version);
    old_tablet_new_metadata->set_commit_time(txn_info.commit_time());
    old_tablet_new_metadata->set_gtid(txn_info.gtid());
    tablet_reshard_helper::set_all_data_files_shared(old_tablet_new_metadata.get());
    new_metadatas.emplace(splitting_tablet.old_tablet_id(), std::move(old_tablet_new_metadata));

    ASSIGN_OR_RETURN(auto split_new_metadatas,
                     split_tablet(tablet_manager, old_tablet_old_metadata, splitting_tablet, new_version, txn_info));
    for (auto& [tablet_id, tablet_metadata] : split_new_metadatas) {
        tablet_ranges.emplace(tablet_id, tablet_metadata->range());
        new_metadatas.emplace(tablet_id, std::move(tablet_metadata));
    }
    return Status::OK();
}

Status handle_merging_tablet(TabletManager* tablet_manager, const MergingTabletInfoPB& merging_tablet,
                             int64_t base_version, int64_t new_version, const TxnInfoPB& txn_info,
                             std::unordered_map<int64_t, TabletMetadataPtr>& new_metadatas,
                             std::unordered_map<int64_t, TabletRangePB>& tablet_ranges) {
    {
        for (auto old_tablet_id : merging_tablet.old_tablet_ids()) {
            auto old_tablet_new_metadata_location =
                    tablet_manager->tablet_metadata_location(old_tablet_id, new_version);
            auto cached_old_tablet_new_metadata =
                    tablet_manager->metacache()->lookup_tablet_metadata(old_tablet_new_metadata_location);
            if (cached_old_tablet_new_metadata == nullptr) {
                goto CONTINUE_HANDLE_MERGING_TABLET;
            }
            new_metadatas.emplace(old_tablet_id, std::move(cached_old_tablet_new_metadata));
        }

        auto new_tablet_new_metadata_location =
                tablet_manager->tablet_metadata_location(merging_tablet.new_tablet_id(), new_version);
        auto cached_new_tablet_new_metadata =
                tablet_manager->metacache()->lookup_tablet_metadata(new_tablet_new_metadata_location);
        if (cached_new_tablet_new_metadata == nullptr) {
            new_metadatas.clear();
            goto CONTINUE_HANDLE_MERGING_TABLET;
        }
        tablet_ranges.emplace(merging_tablet.new_tablet_id(), cached_new_tablet_new_metadata->range());
        new_metadatas.emplace(merging_tablet.new_tablet_id(), std::move(cached_new_tablet_new_metadata));
        return Status::OK();
    }

CONTINUE_HANDLE_MERGING_TABLET:
    std::vector<TabletMetadataPtr> old_tablet_metadatas;
    old_tablet_metadatas.reserve(merging_tablet.old_tablet_ids_size());
    for (auto old_tablet_id : merging_tablet.old_tablet_ids()) {
        auto old_tablet_old_metadata_or = tablet_manager->get_tablet_metadata(old_tablet_id, base_version, false);
        if (old_tablet_old_metadata_or.status().is_not_found()) {
            new_metadatas.clear();
            for (auto retry_tablet_id : merging_tablet.old_tablet_ids()) {
                auto old_tablet_new_metadata_or =
                        tablet_manager->get_tablet_metadata(retry_tablet_id, new_version, txn_info.gtid());
                if (!old_tablet_new_metadata_or.ok()) {
                    return old_tablet_old_metadata_or.status();
                }
                new_metadatas.emplace(retry_tablet_id, std::move(old_tablet_new_metadata_or.value()));
            }
            auto new_tablet_new_metadata_or =
                    tablet_manager->get_tablet_metadata(merging_tablet.new_tablet_id(), new_version, txn_info.gtid());
            if (!new_tablet_new_metadata_or.ok()) {
                return old_tablet_old_metadata_or.status();
            }
            auto& new_tablet_new_metadata = new_tablet_new_metadata_or.value();
            tablet_ranges.emplace(merging_tablet.new_tablet_id(), new_tablet_new_metadata->range());
            new_metadatas.emplace(merging_tablet.new_tablet_id(), std::move(new_tablet_new_metadata));
            return Status::OK();
        }
        if (!old_tablet_old_metadata_or.ok()) {
            LOG(WARNING) << "Failed to get tablet: " << old_tablet_id << ", version: " << base_version
                         << ", txn_id: " << txn_info.txn_id() << ", status: " << old_tablet_old_metadata_or.status();
            return old_tablet_old_metadata_or.status();
        }
        old_tablet_metadatas.emplace_back(std::move(old_tablet_old_metadata_or.value()));
    }

    for (const auto& old_tablet_old_metadata : old_tablet_metadatas) {
        auto old_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_old_metadata);
        old_tablet_new_metadata->set_version(new_version);
        old_tablet_new_metadata->set_commit_time(txn_info.commit_time());
        old_tablet_new_metadata->set_gtid(txn_info.gtid());
        tablet_reshard_helper::set_all_data_files_shared(old_tablet_new_metadata.get(), true);
        new_metadatas.emplace(old_tablet_old_metadata->id(), std::move(old_tablet_new_metadata));
    }

    ASSIGN_OR_RETURN(auto new_tablet_metadata,
                     merge_tablet(tablet_manager, old_tablet_metadatas, merging_tablet, new_version, txn_info));

    tablet_ranges.emplace(merging_tablet.new_tablet_id(), new_tablet_metadata->range());
    new_metadatas.emplace(merging_tablet.new_tablet_id(), std::move(new_tablet_metadata));
    return Status::OK();
}

Status handle_identical_tablet(TabletManager* tablet_manager, const IdenticalTabletInfoPB& identical_tablet,
                               int64_t base_version, int64_t new_version, const TxnInfoPB& txn_info,
                               std::unordered_map<int64_t, TabletMetadataPtr>& new_metadatas) {
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
        return Status::OK();
    }

CONTINUE_HANDLE_IDENTICAL_TABLET:
    auto old_tablet_old_metadata_or =
            tablet_manager->get_tablet_metadata(identical_tablet.old_tablet_id(), base_version, false);
    if (old_tablet_old_metadata_or.status().is_not_found()) {
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
        return Status::OK();
    }

    if (!old_tablet_old_metadata_or.ok()) {
        LOG(WARNING) << "Failed to get tablet: " << identical_tablet.old_tablet_id() << ", version: " << base_version
                     << ", txn_id: " << txn_info.txn_id() << ", status: " << old_tablet_old_metadata_or.status();
        return old_tablet_old_metadata_or.status();
    }

    const auto& old_tablet_old_metadata = old_tablet_old_metadata_or.value();

    auto old_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_old_metadata);
    old_tablet_new_metadata->set_version(new_version);
    old_tablet_new_metadata->set_commit_time(txn_info.commit_time());
    old_tablet_new_metadata->set_gtid(txn_info.gtid());
    tablet_reshard_helper::set_all_data_files_shared(old_tablet_new_metadata.get());
    new_metadatas.emplace(identical_tablet.old_tablet_id(), std::move(old_tablet_new_metadata));

    auto new_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_old_metadata);
    new_tablet_new_metadata->set_id(identical_tablet.new_tablet_id());
    new_tablet_new_metadata->set_version(new_version);
    new_tablet_new_metadata->set_commit_time(txn_info.commit_time());
    new_tablet_new_metadata->set_gtid(txn_info.gtid());
    new_tablet_new_metadata->clear_compaction_inputs();
    new_tablet_new_metadata->clear_orphan_files();
    new_tablet_new_metadata->clear_prev_garbage_version();
    new_metadatas.emplace(identical_tablet.new_tablet_id(), std::move(new_tablet_new_metadata));
    return Status::OK();
}

} // namespace

std::ostream& operator<<(std::ostream& out, const PublishTabletInfo& tablet_info) {
    if (tablet_info.get_publish_tablet_type() == PublishTabletInfo::PUBLISH_NORMAL) {
        return out << "{tablet_id: " << tablet_info.get_tablet_id_in_metadata() << '}';
    }
    return out << "{publish_tablet_type: " << static_cast<int>(tablet_info.get_publish_tablet_type())
               << ", tablet_id_in_metadata: " << tablet_info.get_tablet_id_in_metadata()
               << ", tablet_id_in_txn_log: " << tablet_info.get_tablet_ids_in_txn_logs() << '}';
}

StatusOr<TxnLogPtr> convert_txn_log(const TxnLogPtr& txn_log, const TabletMetadataPtr& base_tablet_metadata,
                                    const PublishTabletInfo& publish_tablet_info) {
    if (publish_tablet_info.get_publish_tablet_type() == PublishTabletInfo::PUBLISH_NORMAL) {
        return txn_log;
    }

    auto new_txn_log = std::make_shared<TxnLogPB>(*txn_log);
    new_txn_log->set_tablet_id(publish_tablet_info.get_tablet_id_in_metadata());

    if (publish_tablet_info.get_publish_tablet_type() == PublishTabletInfo::SPLITTING_TABLET) {
        tablet_reshard_helper::set_all_data_files_shared(new_txn_log.get());
        RETURN_IF_ERROR(tablet_reshard_helper::update_rowset_ranges(new_txn_log.get(), base_tablet_metadata->range()));
    }

    return new_txn_log;
}

Status publish_resharding_tablet(TabletManager* tablet_manager, const ReshardingTabletInfoPB& resharding_tablet,
                                 int64_t base_version, int64_t new_version, const TxnInfoPB& txn_info,
                                 bool skip_write_tablet_metadata,
                                 std::unordered_map<int64_t, TabletMetadataPtr>& tablet_metadatas,
                                 std::unordered_map<int64_t, TabletRangePB>& tablet_ranges) {
    LOG(INFO) << "Start publish resharding tablet"
              << ", resharding_tablet=" << resharding_tablet.DebugString() << ", txn_info=" << txn_info.DebugString()
              << ", base_version=" << base_version << ", new_version=" << new_version;

    if (resharding_tablet.has_splitting_tablet_info()) {
        RETURN_IF_ERROR(handle_splitting_tablet(tablet_manager, resharding_tablet.splitting_tablet_info(), base_version,
                                                new_version, txn_info, tablet_metadatas, tablet_ranges));
    } else if (resharding_tablet.has_merging_tablet_info()) {
        RETURN_IF_ERROR(handle_merging_tablet(tablet_manager, resharding_tablet.merging_tablet_info(), base_version,
                                              new_version, txn_info, tablet_metadatas, tablet_ranges));
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

    LOG(INFO) << "Finish publish resharding tablet"
              << ", resharding_tablet=" << resharding_tablet.DebugString() << ", txn_info=" << txn_info.DebugString()
              << ", base_version=" << base_version << ", new_version=" << new_version;
    return Status::OK();
}

} // namespace starrocks::lake
