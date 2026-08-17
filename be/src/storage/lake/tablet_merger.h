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

StatusOr<MutableTabletMetadataPtr> merge_tablet(TabletManager* tablet_manager,
                                                const std::vector<TabletMetadataPtr>& old_tablet_metadatas,
                                                const MergingTabletInfoPB& merging_tablet, int64_t new_version,
                                                const TxnInfoPB& txn_info);

} // namespace starrocks::lake
