// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

// Bridge between the canonical structured fields (RowsetMetadataPB.segment_metas[],
// OpWrite.dels_meta[] / rewrite_segments_meta[]) and the deprecated legacy parallel
// arrays (deprecated_segments / deprecated_segment_size / ... / deprecated_dels / ...).
//
// These free functions are invoked at the lake metadata load/save choke points in
// TabletManager:
//   - AFTER LOAD: back-fill segment_metas / *_meta from the legacy arrays for old
//     rowsets written by a pre-feature BE, so the migrated readers (which only look
//     at segment_metas) work on pre-upgrade on-disk data.
//   - BEFORE SAVE: fill the legacy arrays from segment_metas / *_meta so a BE that is
//     rolled back to a pre-feature version can still read every segment. (Phase 2 will
//     gate the dual-write off once rollback below this version is unsupported.)

#include "common/status.h"
#include "gen_cpp/lake_types.pb.h"

namespace starrocks::lake {

// Walk every nested RowsetMetadataPB / OpWrite carrier in a TabletMetadataPB or TxnLogPB
// and normalize it. After-load merges the legacy arrays into the structured fields (structured
// wins; never errors). Before-save rebuilds the legacy arrays from the structured fields; it
// returns Status only to propagate failures and currently always succeeds.
void normalize_tablet_metadata_after_load(TabletMetadataPB* tablet_metadata);
Status normalize_tablet_metadata_before_save(TabletMetadataPB* tablet_metadata);

void normalize_txn_log_after_load(TxnLogPB* txn_log);
Status normalize_txn_log_before_save(TxnLogPB* txn_log);

// Lower-level entrypoints, exposed for targeted callers and unit tests.
void normalize_rowset_after_load(RowsetMetadataPB* rowset_metadata);
Status normalize_rowset_before_save(RowsetMetadataPB* rowset_metadata);
void normalize_op_write_after_load(TxnLogPB::OpWrite* op_write);
Status normalize_op_write_before_save(TxnLogPB::OpWrite* op_write);

} // namespace starrocks::lake
