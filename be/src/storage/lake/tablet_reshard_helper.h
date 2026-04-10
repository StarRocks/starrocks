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

#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"

namespace starrocks::lake::tablet_reshard_helper {

void set_all_data_files_shared(RowsetMetadataPB* rowset_metadata);
void set_all_data_files_shared(TxnLogPB* txn_log);
void set_all_data_files_shared(TabletMetadataPB* tablet_metadata, bool skip_delvecs = false);

StatusOr<TabletRangePB> intersect_range(const TabletRangePB& lhs_pb, const TabletRangePB& rhs_pb);
StatusOr<TabletRangePB> union_range(const TabletRangePB& lhs_pb, const TabletRangePB& rhs_pb);
Status update_rowset_range(RowsetMetadataPB* rowset, const TabletRangePB& range);
Status update_rowset_ranges(TxnLogPB* txn_log, const TabletRangePB& range);

void update_rowset_data_stats(RowsetMetadataPB* rowset, int32_t split_count, int32_t split_index);
void update_txn_log_data_stats(TxnLogPB* txn_log, int32_t split_count, int32_t split_index);

} // namespace starrocks::lake::tablet_reshard_helper
