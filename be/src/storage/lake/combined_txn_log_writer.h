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

#include <google/protobuf/repeated_field.h>

#include <cstdint>
#include <map>
#include <set>
#include <string>

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

// Writes the combined txn log of every partition in |txn_log_map|, as a load does when its sink closes: all partitions
// at once with enable_put_combinded_txn_log_parallel, one after another otherwise. The write of a partition is bounded
// by lake_put_combined_txn_log_attempt_timeout_ms and lake_put_combined_txn_log_max_attempts: an attempt that fails
// is retried, and one that has not finished in time gets another attempt alongside it. Must not be called from the
// put_combined_txn_log thread pool, which runs the attempts.
Status write_combined_txn_logs(const std::map<int64_t, CombinedTxnLogPB>& txn_log_map,
                               const ExpectedTabletsByPartition& expected_by_partition);

// Keeps the shard infos tablet owners returned with their txn logs (LakeTabletData.shard_infos), so
// that a combined txn log of a partition this node owns no tablet of is written without a starmgr RPC.
void borrow_shard_infos(const ::google::protobuf::RepeatedPtrField<std::string>& shard_infos);

} // namespace starrocks
