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
#include "storage/lake/tablet_metadata.h"

namespace starrocks::lake {

class TabletManager;

StatusOr<TabletMetadataPtr> publish_version(TabletManager* tablet_mgr, int64_t tablet_id, int64_t base_version,
                                            int64_t new_version, std::span<const int64_t> txn_ids, int64_t commit_time);

// Transform a txn log into versioned txn log(i.e., rename `{tablet_id}_{txn_id}.log` to `{tablet_id}_{log_version}.vlog`)
Status publish_log_version(TabletManager* tablet_mgr, int64_t tablet_id, int64_t txn_id, int64 log_version);

void abort_txn(TabletManager* tablet_mgr, int64_t tablet_id, std::span<const int64_t> txn_ids);

} // namespace starrocks::lake
