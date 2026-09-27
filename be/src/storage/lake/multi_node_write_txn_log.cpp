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

#include "storage/lake/multi_node_write_txn_log.h"

#include <google/protobuf/util/message_differencer.h>

#include <limits>

#include "fmt/format.h"

namespace starrocks::lake {

namespace {

constexpr uint32_t kUnknownDelOpOffset = std::numeric_limits<uint32_t>::max();

// A multi-node write node contributes plain appended data and nothing else. A compaction or
// schema-change body means this transaction should never have been routed to several writers, so fail
// loudly instead of silently dropping the field during the fold.
Status check_mergeable(const TxnLogPB& log) {
    if (!log.has_op_write()) {
        return Status::NotSupported(
                fmt::format("multi-node write: txn log of tablet {} has no op_write", log.tablet_id()));
    }
    if (log.has_op_compaction() || log.has_op_schema_change() || log.has_op_replication() ||
        log.has_op_parallel_compaction()) {
        return Status::NotSupported(
                fmt::format("multi-node write: txn log of tablet {} carries a non-write operation", log.tablet_id()));
    }
    return Status::OK();
}

// txn_meta is mostly a description of the LOAD -- which columns it writes, its partial-update mode, its
// merge condition, the offset of an auto-increment column it omits. Every writing node builds those from
// the same plan and the same write schema, so they must agree and folding them is keeping one copy.
//
// partial_rowset_footers is the exception and the reason this is not a plain equality check: it holds one
// entry per SEGMENT (appended in HorizontalPkTabletWriter::flush_segment_writer) and publish reads it BY
// SEGMENT ID, so each writer's entries belong to its own segments. They are excluded here and
// concatenated by the merge, exactly like the ssts.
//
// Compared with MessageDifferencer rather than by serialized bytes because column_to_expr_value is a
// proto map, whose serialization order is unspecified and would make equal metas look different.
Status check_txn_meta_agreement(const TxnLogPB& dst, const TxnLogPB& src) {
    const auto& a = dst.op_write();
    const auto& b = src.op_write();
    if (a.has_txn_meta() != b.has_txn_meta()) {
        return Status::Corruption(
                fmt::format("multi-node write: tablet {} mixes logs with and without txn_meta", dst.tablet_id()));
    }
    if (!a.has_txn_meta()) {
        return Status::OK();
    }
    RowsetTxnMetaPB load_scope_a = a.txn_meta();
    RowsetTxnMetaPB load_scope_b = b.txn_meta();
    load_scope_a.clear_partial_rowset_footers();
    load_scope_b.clear_partial_rowset_footers();
    if (!google::protobuf::util::MessageDifferencer::Equals(load_scope_a, load_scope_b)) {
        return Status::Corruption(
                fmt::format("multi-node write: tablet {} has writers disagreeing on txn_meta", dst.tablet_id()));
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
        return Status::Corruption(fmt::format("multi-node write: tablet {} has {} ssts for {} segments",
                                              log.tablet_id(), op_write.ssts_size(), segments));
    }
    if (op_write.sst_ranges_size() != 0 && op_write.sst_ranges_size() != op_write.ssts_size()) {
        return Status::Corruption(fmt::format("multi-node write: tablet {} has {} sst_ranges for {} ssts",
                                              log.tablet_id(), op_write.sst_ranges_size(), op_write.ssts_size()));
    }
    // partial_rowset_footers is one entry per segment and publish reads it by segment id
    // (rowset_update_state.cpp: txn_meta.partial_rowset_footers(segment_id)), so a short array would make
    // every later segment read another segment's footer.
    if (op_write.has_txn_meta() && op_write.txn_meta().partial_rowset_footers_size() != 0 &&
        op_write.txn_meta().partial_rowset_footers_size() != segments) {
        return Status::Corruption(
                fmt::format("multi-node write: tablet {} has {} partial rowset footers for {} segments",
                            log.tablet_id(), op_write.txn_meta().partial_rowset_footers_size(), segments));
    }
    // rewrite_segments_meta is one entry per segment, added by the partial-update and auto-increment
    // paths; column-mode partial update adds none. All-or-nothing, so a mix would shift the entries
    // onto the wrong segments.
    if (op_write.rewrite_segments_meta_size() != 0 && op_write.rewrite_segments_meta_size() != segments) {
        return Status::Corruption(fmt::format("multi-node write: tablet {} has {} rewrite segments for {} segments",
                                              log.tablet_id(), op_write.rewrite_segments_meta_size(), segments));
    }
    // del_ssts is indexed by del_id and is all-or-nothing (see the field's proto comment).
    if (op_write.del_ssts_size() != 0 && op_write.del_ssts_size() != op_write.dels_meta_size()) {
        return Status::Corruption(fmt::format("multi-node write: tablet {} has {} del_ssts for {} del files",
                                              log.tablet_id(), op_write.del_ssts_size(), op_write.dels_meta_size()));
    }
    return Status::OK();
}

} // namespace

Status merge_multi_node_write_txn_log(TxnLogPB* dst, TxnLogPB* src) {
    if (dst->tablet_id() != src->tablet_id() || dst->txn_id() != src->txn_id() ||
        dst->partition_id() != src->partition_id()) {
        return Status::InternalError(fmt::format(
                "multi-node write: refusing to merge txn logs of different targets, ({}, {}, {}) vs ({}, {}, {})",
                dst->tablet_id(), dst->txn_id(), dst->partition_id(), src->tablet_id(), src->txn_id(),
                src->partition_id()));
    }
    RETURN_IF_ERROR(check_mergeable(*dst));
    RETURN_IF_ERROR(check_mergeable(*src));
    RETURN_IF_ERROR(check_sst_alignment(*dst));
    RETURN_IF_ERROR(check_sst_alignment(*src));
    RETURN_IF_ERROR(check_txn_meta_agreement(*dst, *src));

    auto* dst_op = dst->mutable_op_write();
    auto* src_op = src->mutable_op_write();
    auto* dst_rowset = dst_op->mutable_rowset();
    auto* src_rowset = src_op->mutable_rowset();

    const int base_segments = dst_rowset->segment_metas_size();
    const int src_segments = src_rowset->segment_metas_size();

    // Segments: a plain append. The position of a segment in this array is its rowset-local index and
    // therefore its rssid offset, so renumbering segment_idx is all that keeps the ids contiguous.
    for (auto& segment_meta : *src_rowset->mutable_segment_metas()) {
        auto* appended = dst_rowset->add_segment_metas();
        appended->Swap(&segment_meta);
        appended->set_segment_idx(dst_rowset->segment_metas_size() - 1);
    }

    // Rewrite segments ride along with the segments they are named for, keeping the positional pairing
    // publish resolves them by. Both sides were checked to be either full or empty, and mixing the two
    // would leave the array shorter than the segment list -- which is why it is checked rather than
    // padded: an entry invented here would name a file no node ever wrote.
    if (src_op->rewrite_segments_meta_size() > 0 && dst_op->rewrite_segments_meta_size() == 0 && base_segments > 0) {
        return Status::Corruption(fmt::format(
                "multi-node write: tablet {} mixes logs with and without rewrite segments", dst->tablet_id()));
    }
    for (auto& rewrite_segment : *src_op->mutable_rewrite_segments_meta()) {
        dst_op->add_rewrite_segments_meta()->Swap(&rewrite_segment);
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

    // The per-segment half of txn_meta rides with the segments. The load-scope fields were already
    // checked to agree, so dst's copy of them stands for both.
    if (src_op->has_txn_meta() && src_op->txn_meta().partial_rowset_footers_size() > 0) {
        auto* dst_meta = dst_op->mutable_txn_meta();
        for (auto& footer : *src_op->mutable_txn_meta()->mutable_partial_rowset_footers()) {
            dst_meta->add_partial_rowset_footers()->Swap(&footer);
        }
    }

    // ssts / sst_ranges / seg_delvecs are all indexed by segment, so they follow the segments.
    //
    // Whether a segment gets its sstable built eagerly is decided PER SEGMENT, against a size
    // threshold, so one writer can contribute segments that carry one while another contributes
    // segments that do not. That is an ordinary load, not a corrupt one, so the side that emitted
    // none is padded with empty entries rather than refused. An empty name is publish's marker for
    // "no pre-built sstable for this segment" -- the same convention del_ssts already uses -- and
    // keeping the array segment-aligned is what lets publish read it per segment at all.
    const bool any_sst = dst_op->ssts_size() > 0 || src_op->ssts_size() > 0;
    if (any_sst) {
        while (dst_op->ssts_size() < base_segments) {
            dst_op->add_ssts();
        }
        while (dst_op->sst_ranges_size() < base_segments) {
            dst_op->add_sst_ranges();
        }
        for (int i = 0; i < src_segments; ++i) {
            auto* sst = dst_op->add_ssts();
            if (i < src_op->ssts_size()) {
                sst->Swap(src_op->mutable_ssts(i));
            }
            auto* sst_range = dst_op->add_sst_ranges();
            if (i < src_op->sst_ranges_size()) {
                sst_range->Swap(src_op->mutable_sst_ranges(i));
            }
        }
    }
    if (src_op->seg_delvecs_size() > 0) {
        // seg_delvecs is optional and indexed like ssts; pad the slots of contributors that emitted
        // none so the incoming entries land on their own segments.
        while (dst_op->seg_delvecs_size() < base_segments) {
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
