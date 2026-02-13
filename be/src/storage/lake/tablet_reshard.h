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
#include <vector>

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
            : publish_tablet_type(PUBLISH_NORMAL),
              tablet_ids_in_txn_logs({tablet_id}),
              tablet_id_in_metadata(tablet_id) {}

    // For publish cross transaction with only one txn log, including splitting tablet and identical tablet
    PublishTabletInfo(PublishTabletType publish_tablet_type, int64_t tablet_id_in_txn_log,
                      int64_t tablet_id_in_metadata)
            : publish_tablet_type(publish_tablet_type),
              tablet_ids_in_txn_logs(1, tablet_id_in_txn_log),
              tablet_id_in_metadata(tablet_id_in_metadata) {}

    // For cross publish merging tablet with multiple txn logs
    PublishTabletInfo(PublishTabletType publish_tablet_type, const std::span<const int64_t>& tablet_ids_in_txn_logs,
                      int64_t tablet_id_in_metadata)
            : publish_tablet_type(publish_tablet_type),
              tablet_ids_in_txn_logs(tablet_ids_in_txn_logs.begin(), tablet_ids_in_txn_logs.end()),
              tablet_id_in_metadata(tablet_id_in_metadata) {}

    PublishTabletType get_publish_tablet_type() const { return publish_tablet_type; }
    const std::vector<int64_t>& get_tablet_ids_in_txn_logs() const { return tablet_ids_in_txn_logs; }
    int64_t get_tablet_id_in_metadata() const { return tablet_id_in_metadata; }
    // A txn log will be applied to multiple tablets in splitting tablet, so it cannot be deleted after applied.
    bool can_delete_txn_log() const { return publish_tablet_type != SPLITTING_TABLET; }

private:
    PublishTabletType publish_tablet_type;
    std::vector<int64_t> tablet_ids_in_txn_logs;
    int64_t tablet_id_in_metadata;
};

std::ostream& operator<<(std::ostream& out, const PublishTabletInfo& tablet_info);

TxnLogPtr convert_txn_log(const TxnLogPtr& txn_log, const TabletMetadataPtr& base_tablet_metadata,
                          const PublishTabletInfo& publish_tablet_info);

Status publish_resharding_tablet(TabletManager* tablet_manager, const ReshardingTabletInfoPB& resharding_tablet,
                                 int64_t base_version, int64_t new_version, const TxnInfoPB& txn_info,
                                 bool skip_write_tablet_metadata,
                                 std::unordered_map<int64_t, TabletMetadataPtr>& tablet_metadatas,
                                 std::unordered_map<int64_t, TabletRangePB>& tablet_ranges);

} // namespace starrocks::lake
