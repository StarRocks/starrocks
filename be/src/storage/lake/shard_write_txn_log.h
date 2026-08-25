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

#include "common/status.h"
#include "gen_cpp/lake_types.pb.h"

namespace starrocks::lake {

// Fold |src| -- the txn log one shard-write node produced for a tablet -- into |dst|, the log being
// assembled for the same tablet from every node that wrote it in this transaction.
//
// Shard write (TOlapTableSink.enable_shard_write) has N compute nodes each write a disjoint part of
// one tablet's rows, so the transaction ends with N partial op_write logs for that tablet. They are
// folded together here, on the sender, BEFORE the combined {txn_id}.logs file is written, which is
// what keeps publish unchanged: the file it reads still holds exactly one log per tablet, and the
// tablet still gains exactly one rowset.
//
// The fold is a flat concatenation, mirroring TabletWriter::merge_other_writer (which does the same
// thing for the multi-threaded spill merge inside one node):
//   - segment_metas are appended and renumbered, so a segment's position IS its rowset-local index;
//   - ssts / sst_ranges / seg_delvecs are appended alongside them and stay positionally aligned,
//     which is what publish relies on (update_manager indexes op_write.ssts() by segment id and
//     stamps the resulting sstable's rssid from that position, so no rssid rewriting is needed);
//   - dels_meta are appended with their op_offset shifted by the segments already in |dst|, so a
//     delete keeps beating the upserts it followed on its own node;
//   - row and byte counters are summed.
//
// Both logs must describe the same tablet, txn and partition. Callers must run
// normalize_txn_log_after_load() on each input first: it makes the structured arrays canonical and
// drops the deprecated parallel arrays, which this function does not maintain.
//
// Ordering between two nodes' rows is NOT defined -- rows were spread round-robin, so a key upserted
// on one node and deleted on another has no arrival order left to honour. This is the documented
// contract of the feature, not an artifact of the merge.
Status merge_shard_write_txn_log(TxnLogPB* dst, TxnLogPB* src);

} // namespace starrocks::lake
