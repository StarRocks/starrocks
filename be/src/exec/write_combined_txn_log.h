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

#include <set>

#include "common/status.h"

namespace starrocks {
class CombinedTxnLogPB;

// partition id -> the tablets that partition's combined txn log must cover. Supplying it makes
// put_combined_txn_log() refuse to write an object that is short of an entry; see the comment on
// TabletManager::put_combined_txn_log. Pass empty to keep the previous unchecked behaviour.
using ExpectedTabletsByPartition = std::map<int64_t, std::set<int64_t>>;

Status write_combined_txn_log(const CombinedTxnLogPB& logs, const std::set<int64_t>& expected_tablet_ids = {});
Status write_combined_txn_log_parallel(const std::map<int64_t, CombinedTxnLogPB>& txn_log_map,
                                       const ExpectedTabletsByPartition& expected_by_partition = {});

} // namespace starrocks
