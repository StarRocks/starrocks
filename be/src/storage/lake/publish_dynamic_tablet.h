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

#include <span>

#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"

namespace starrocks::lake {
class TabletManager;

class PublishTabletInfo {
public:
    enum PublishTabletType {
        PUBLISH_NORMAL,   // Publish normal tablet, a txn log will be applied to the corresponding tablet.
        SPLITTING_TABLET, // Cross publish splitting tablet, a txn log will be applied to multiple tablets.
        MERGING_TABLET,   // Cross publish merging tablet, multiple txn logs will be applied to a tablet.
        IDENTICAL_TABLET, // Cross publish identical tablet, a txn log will be applied to another tablet.
    };

public:
    // For publish normal transaction
    PublishTabletInfo(int64_t tablet_id)
            : publish_tablet_type(PUBLISH_NORMAL), tablet_id_in_txn_log(tablet_id), tablet_id_in_metadata(tablet_id) {}

    // For cross publish
    PublishTabletInfo(PublishTabletType publish_tablet_type, int64_t tablet_id_in_txn_log,
                      int64_t tablet_id_in_metadata)
            : publish_tablet_type(publish_tablet_type),
              tablet_id_in_txn_log(tablet_id_in_txn_log),
              tablet_id_in_metadata(tablet_id_in_metadata) {}

    PublishTabletType get_publish_tablet_type() const { return publish_tablet_type; }
    int64_t get_tablet_id_in_txn_log() const { return tablet_id_in_txn_log; }
    int64_t get_tablet_id_in_metadata() const { return tablet_id_in_metadata; }
    // A txn log will be applied to multiple tablets in splitting tablet, so it cannot be deleted after applied.
    bool can_delete_txn_log() const { return publish_tablet_type != SPLITTING_TABLET; }

private:
    PublishTabletType publish_tablet_type;
    int64_t tablet_id_in_txn_log;
    int64_t tablet_id_in_metadata;
};

TxnLogPtr convert_txn_log_for_dynamic_tablet(const TxnLogPtr& original_txn_log,
                                             const TabletMetadataPtr& base_tablet_metadata,
                                             PublishTabletInfo::PublishTabletType publish_tablet_type);

StatusOr<std::unordered_map<int64_t, TabletMetadataPtr>> publish_splitting_tablet(
        TabletManager* tablet_manager, const std::span<std::string>& distribution_columns,
        const SplittingTabletInfoPB& splitting_tablet, int64_t base_version, int64_t new_version,
        const TxnInfoPB& txn_info, bool skip_write_tablet_metadata);

StatusOr<std::unordered_map<int64_t, TabletMetadataPtr>> publish_merging_tablet(
        TabletManager* tablet_manager, const std::span<std::string>& distribution_columns,
        const MergingTabletInfoPB& merging_tablet, int64_t base_version, int64_t new_version, const TxnInfoPB& txn_info,
        bool skip_write_tablet_metadata);

StatusOr<std::unordered_map<int64_t, TabletMetadataPtr>> publish_identical_tablet(
        TabletManager* tablet_manager, const IdenticalTabletInfoPB& identical_tablet, int64_t base_version,
        int64_t new_version, const TxnInfoPB& txn_info, bool skip_write_tablet_metadata);

} // namespace starrocks::lake
