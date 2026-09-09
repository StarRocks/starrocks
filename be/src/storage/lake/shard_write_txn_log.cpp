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

#include "storage/lake/shard_write_txn_log.h"

#include <limits>

#include "fmt/format.h"

namespace starrocks::lake {

namespace {

constexpr uint32_t kUnknownDelOpOffset = std::numeric_limits<uint32_t>::max();

// A shard-write node contributes plain appended data and nothing else. Anything that ties the log to
// a per-node view of the tablet (partial-update rewrite plans, a compaction/schema-change body) means
// this transaction should never have been routed to several writers, so fail loudly instead of
// silently dropping the field during the fold.
Status check_mergeable(const TxnLogPB& log) {
    if (!log.has_op_write()) {
        return Status::NotSupported(fmt::format("shard write: txn log of tablet {} has no op_write", log.tablet_id()));
    }
    if (log.has_op_compaction() || log.has_op_schema_change() || log.has_op_replication() ||
        log.has_op_parallel_compaction()) {
        return Status::NotSupported(
                fmt::format("shard write: txn log of tablet {} carries a non-write operation", log.tablet_id()));
    }
    const auto& op_write = log.op_write();
    if (op_write.has_txn_meta() || op_write.rewrite_segments_meta_size() > 0) {
        return Status::NotSupported(
                fmt::format("shard write: partial update is not supported, tablet {}", log.tablet_id()));
    }
    return Status::OK();
}

// ssts are indexed BY SEGMENT at publish time (update_manager reads op_write.ssts(segment_id) and
// stamps the ingested sstable's rssid from that index), so a log either carries one sst per segment
// or none at all. A mix across nodes would shift every sst onto the wrong segment, corrupting the
// primary key index without any visible error, so reject it here.
Status check_sst_alignment(const TxnLogPB& log) {
    const auto& op_write = log.op_write();
    const int segments = op_write.rowset().segment_metas_size();
    if (op_write.ssts_size() != 0 && op_write.ssts_size() != segments) {
        return Status::Corruption(fmt::format("shard write: tablet {} has {} ssts for {} segments", log.tablet_id(),
                                              op_write.ssts_size(), segments));
    }
    if (op_write.sst_ranges_size() != 0 && op_write.sst_ranges_size() != op_write.ssts_size()) {
        return Status::Corruption(fmt::format("shard write: tablet {} has {} sst_ranges for {} ssts", log.tablet_id(),
                                              op_write.sst_ranges_size(), op_write.ssts_size()));
    }
    // del_ssts is indexed by del_id and is all-or-nothing (see the field's proto comment).
    if (op_write.del_ssts_size() != 0 && op_write.del_ssts_size() != op_write.dels_meta_size()) {
        return Status::Corruption(fmt::format("shard write: tablet {} has {} del_ssts for {} del files",
                                              log.tablet_id(), op_write.del_ssts_size(), op_write.dels_meta_size()));
    }
    return Status::OK();
}

} // namespace

Status merge_shard_write_txn_log(TxnLogPB* dst, TxnLogPB* src) {
    if (dst->tablet_id() != src->tablet_id() || dst->txn_id() != src->txn_id() ||
        dst->partition_id() != src->partition_id()) {
        return Status::InternalError(fmt::format(
                "shard write: refusing to merge txn logs of different targets, ({}, {}, {}) vs ({}, {}, {})",
                dst->tablet_id(), dst->txn_id(), dst->partition_id(), src->tablet_id(), src->txn_id(),
                src->partition_id()));
    }
    RETURN_IF_ERROR(check_mergeable(*dst));
    RETURN_IF_ERROR(check_mergeable(*src));
    RETURN_IF_ERROR(check_sst_alignment(*dst));
    RETURN_IF_ERROR(check_sst_alignment(*src));

    auto* dst_op = dst->mutable_op_write();
    auto* src_op = src->mutable_op_write();
    auto* dst_rowset = dst_op->mutable_rowset();
    auto* src_rowset = src_op->mutable_rowset();

    const int base_segments = dst_rowset->segment_metas_size();
    const int base_ssts = dst_op->ssts_size();
    // With both sides carrying segments, either both build the PK index eagerly (one sst per segment)
    // or neither does. Mixing them leaves fewer ssts than segments, and publish would then read every
    // sst under the wrong segment id. An empty side is fine: it contributes neither.
    if (base_segments > 0 && src_rowset->segment_metas_size() > 0 && (base_ssts > 0) != (src_op->ssts_size() > 0)) {
        return Status::Corruption(fmt::format(
                "shard write: tablet {} mixes eager-sst and no-sst txn logs, {} ssts for {} segments vs {} for {}",
                dst->tablet_id(), base_ssts, base_segments, src_op->ssts_size(), src_rowset->segment_metas_size()));
    }

    // Segments: a plain append. The position of a segment in this array is its rowset-local index and
    // therefore its rssid offset, so renumbering segment_idx is all that keeps the ids contiguous.
    for (auto& segment_meta : *src_rowset->mutable_segment_metas()) {
        auto* appended = dst_rowset->add_segment_metas();
        appended->Swap(&segment_meta);
        appended->set_segment_idx(dst_rowset->segment_metas_size() - 1);
    }

    // Del files: shift each del's op_offset past the segments |dst| already holds, so the delete still
    // follows exactly the upserts it followed on its own node and does not swallow rows another node
    // wrote earlier. del_op_offsets / del_num_rows / del_ssts / del_sst_ranges all stay positionally
    // aligned with dels_meta.
    const int base_dels = dst_op->dels_meta_size();
    const int src_dels = src_op->dels_meta_size();
    const bool dst_has_offsets = dst_op->del_op_offsets_size() == base_dels;
    const bool src_has_offsets = src_op->del_op_offsets_size() == src_dels;
    // A pre-built tombstone sstable is optional PER DEL FILE (an empty entry means "below the eager
    // threshold, fall back to the memtable erase path"), but the array itself is all-or-nothing. So a
    // contributor that built none is padded with empty entries rather than collapsing the whole array.
    const bool any_del_sst = dst_op->del_ssts_size() > 0 || src_op->del_ssts_size() > 0;
    while (dst_op->del_num_rows_size() < base_dels) {
        dst_op->add_del_num_rows(0);
    }
    if (any_del_sst) {
        while (dst_op->del_ssts_size() < base_dels) {
            dst_op->add_del_ssts();
        }
        while (dst_op->del_sst_ranges_size() < base_dels) {
            dst_op->add_del_sst_ranges();
        }
    }
    for (int i = 0; i < src_dels; ++i) {
        dst_op->add_dels_meta()->Swap(src_op->mutable_dels_meta(i));
        dst_op->add_del_num_rows(i < src_op->del_num_rows_size() ? src_op->del_num_rows(i) : 0);
        if (any_del_sst) {
            auto* del_sst = dst_op->add_del_ssts();
            if (i < src_op->del_ssts_size()) {
                del_sst->Swap(src_op->mutable_del_ssts(i));
            }
            auto* del_sst_range = dst_op->add_del_sst_ranges();
            if (i < src_op->del_sst_ranges_size()) {
                del_sst_range->Swap(src_op->mutable_del_sst_ranges(i));
            }
        }
    }
    if (dst_has_offsets && src_has_offsets) {
        for (int i = 0; i < src_op->del_op_offsets_size(); ++i) {
            const uint32_t offset = src_op->del_op_offsets(i);
            dst_op->add_del_op_offsets(offset == kUnknownDelOpOffset ? kUnknownDelOpOffset : offset + base_segments);
        }
    } else if (base_dels + src_dels > 0) {
        // Not every contributor recorded per-del offsets. Keeping a partial array would misalign it
        // with dels_meta; clearing it falls back to the legacy "all deletes after all upserts" reading,
        // which is exactly what a producer without the offsets already meant.
        dst_op->clear_del_op_offsets();
    }

    // ssts / sst_ranges / seg_delvecs are all indexed by segment, so they follow the segments verbatim.
    for (auto& sst : *src_op->mutable_ssts()) {
        dst_op->add_ssts()->Swap(&sst);
    }
    for (auto& sst_range : *src_op->mutable_sst_ranges()) {
        dst_op->add_sst_ranges()->Swap(&sst_range);
    }
    if (src_op->seg_delvecs_size() > 0) {
        // seg_delvecs is optional and indexed like ssts; pad the slots of contributors that emitted
        // none so the incoming entries land on their own segments.
        while (dst_op->seg_delvecs_size() < base_ssts) {
            dst_op->add_seg_delvecs();
        }
        for (auto& seg_delvec : *src_op->mutable_seg_delvecs()) {
            dst_op->add_seg_delvecs()->Swap(&seg_delvec);
        }
    }

    dst_rowset->set_num_rows(dst_rowset->num_rows() + src_rowset->num_rows());
    dst_rowset->set_data_size(dst_rowset->data_size() + src_rowset->data_size());
    // The merged rowset holds segments from several nodes whose key ranges freely overlap.
    dst_rowset->set_overlapped(dst_rowset->segment_metas_size() > 1);

    if (!dst_op->has_schema_key() && src_op->has_schema_key()) {
        dst_op->mutable_schema_key()->Swap(src_op->mutable_schema_key());
    }
    return Status::OK();
}

} // namespace starrocks::lake
