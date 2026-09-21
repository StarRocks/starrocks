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

#include <optional>
#include <vector>

#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/pk_publish_config.h"

namespace starrocks::lake {

class TabletManager;

// A real MERGE: the output owns its rowsets, delete vectors, DCG/IDG projections and primary-index
// sstable, and a reshard transaction publishes it.
//
// The read-only parent alias an ORDER BY != PK split keeps alive is NOT built here -- see
// virtual_merge_for_read in tablet_virtual_merge.h.
//
// |publish_property| belongs to the publish request this merge runs under and reaches each source
// tablet's primary key index through the per-source flush. Deliberately without a default: that flush
// can rebuild a cold index, so a caller that has a request must not be able to forget it by saying
// nothing.
StatusOr<MutableTabletMetadataPtr> merge_tablet(TabletManager* tablet_manager,
                                                const std::vector<TabletMetadataPtr>& old_tablet_metadatas,
                                                const MergingTabletInfoPB& merging_tablet, int64_t new_version,
                                                const TxnInfoPB& txn_info,
                                                std::optional<PublishPropertyPBRef> publish_property);

} // namespace starrocks::lake
