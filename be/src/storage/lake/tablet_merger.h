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

#include <vector>

#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/tablet_metadata.h"

namespace starrocks::lake {

class TabletManager;

// |skip_sstable_merge| drops the primary-index sstable merge from the result. Only pass true for a
// read-only alias such as the query-side parent an ORDER BY != PK split keeps alive: rewriting the
// inherited multi-rssid sstables costs a parse+remap+reserialize of every index entry, and nothing
// that reads such an alias consults the persistent index -- scans need rowsets and delvecs only,
// while upserts go to the writable child layout. A real MERGE must never set it.
StatusOr<MutableTabletMetadataPtr> merge_tablet(TabletManager* tablet_manager,
                                                const std::vector<TabletMetadataPtr>& old_tablet_metadatas,
                                                const MergingTabletInfoPB& merging_tablet, int64_t new_version,
                                                const TxnInfoPB& txn_info, bool skip_sstable_merge = false);

} // namespace starrocks::lake
