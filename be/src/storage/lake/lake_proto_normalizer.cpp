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

#include "storage/lake/lake_proto_normalizer.h"

#include <algorithm>

#include "common/status.h"
#include "fmt/format.h"

namespace starrocks::lake {

// ---- AFTER LOAD ---------------------------------------------------------------------------------
// Back-fill the structured fields from the deprecated legacy parallel arrays for data written by a
// pre-feature BE. `segment_metas` may ALREADY exist on an old rowset (it predates this refactor and
// carried sort_key/num_rows/segment_idx/vector_index_ids), but WITHOUT the new file-attr fields
// (filename/size/encryption_meta/shared/bundle_file_offset). So we merge per-index: only set a file
// attribute on segment_metas[i] when it is absent there and present in the legacy array.
//
// Once the back-fill is done, segment_metas is the sole canonical source in memory and nothing reads
// the legacy arrays again, so we CLEAR them: it avoids caching two copies of every segment's file
// attributes (filenames especially) for the lifetime of the cached metadata, and it keeps the legacy
// arrays from going stale relative to segment_metas (before-save rebuilds them from segment_metas on
// a throwaway copy, so disk dual-write for rollback is unaffected).

void normalize_rowset_after_load(RowsetMetadataPB* rowset_metadata) {
    const int segment_count =
            std::max(rowset_metadata->deprecated_segments_size(), rowset_metadata->segment_metas_size());
    while (rowset_metadata->segment_metas_size() < segment_count) {
        rowset_metadata->add_segment_metas();
    }
    for (int i = 0; i < segment_count; ++i) {
        auto* segment_metadata = rowset_metadata->mutable_segment_metas(i);
        if (!segment_metadata->has_filename() && i < rowset_metadata->deprecated_segments_size()) {
            segment_metadata->set_filename(rowset_metadata->deprecated_segments(i));
        }
        if (!segment_metadata->has_size() && i < rowset_metadata->deprecated_segment_size_size()) {
            segment_metadata->set_size(rowset_metadata->deprecated_segment_size(i));
        }
        // An empty encryption_meta means "unencrypted", same as absent; don't back-fill it (a
        // pre-feature BE stored "" for unencrypted segments).
        if (!segment_metadata->has_encryption_meta() &&
            i < rowset_metadata->deprecated_segment_encryption_metas_size() &&
            !rowset_metadata->deprecated_segment_encryption_metas(i).empty()) {
            segment_metadata->set_encryption_meta(rowset_metadata->deprecated_segment_encryption_metas(i));
        }
        if (!segment_metadata->has_shared() && i < rowset_metadata->deprecated_shared_segments_size()) {
            segment_metadata->set_shared(rowset_metadata->deprecated_shared_segments(i));
        }
        // bundle_file_offset: 0 is legitimate; presence is by has_bundle_file_offset() / array length.
        if (!segment_metadata->has_bundle_file_offset() && i < rowset_metadata->deprecated_bundle_file_offsets_size()) {
            segment_metadata->set_bundle_file_offset(rowset_metadata->deprecated_bundle_file_offsets(i));
        }
    }
    // segment_metas is now canonical; drop the legacy arrays so they aren't cached unread (or left stale).
    rowset_metadata->clear_deprecated_segments();
    rowset_metadata->clear_deprecated_segment_size();
    rowset_metadata->clear_deprecated_segment_encryption_metas();
    rowset_metadata->clear_deprecated_shared_segments();
    rowset_metadata->clear_deprecated_bundle_file_offsets();
}

void normalize_op_write_after_load(TxnLogPB::OpWrite* op_write) {
    if (op_write->has_rowset()) {
        normalize_rowset_after_load(op_write->mutable_rowset());
    }
    // dels_meta <- deprecated_dels / deprecated_del_encryption_metas / deprecated_shared_dels
    const int del_file_count = std::max(op_write->deprecated_dels_size(), op_write->dels_meta_size());
    while (op_write->dels_meta_size() < del_file_count) {
        op_write->add_dels_meta();
    }
    for (int i = 0; i < del_file_count; ++i) {
        auto* del_file_metadata = op_write->mutable_dels_meta(i);
        if (!del_file_metadata->has_name() && i < op_write->deprecated_dels_size()) {
            del_file_metadata->set_name(op_write->deprecated_dels(i));
        }
        if (!del_file_metadata->has_encryption_meta() && i < op_write->deprecated_del_encryption_metas_size()) {
            del_file_metadata->set_encryption_meta(op_write->deprecated_del_encryption_metas(i));
        }
        if (!del_file_metadata->has_shared() && i < op_write->deprecated_shared_dels_size()) {
            del_file_metadata->set_shared(op_write->deprecated_shared_dels(i));
        }
    }
    // rewrite_segments_meta <- deprecated_rewrite_segments
    const int rewrite_segment_count =
            std::max(op_write->deprecated_rewrite_segments_size(), op_write->rewrite_segments_meta_size());
    while (op_write->rewrite_segments_meta_size() < rewrite_segment_count) {
        op_write->add_rewrite_segments_meta();
    }
    for (int i = 0; i < rewrite_segment_count; ++i) {
        auto* rewrite_segment_metadata = op_write->mutable_rewrite_segments_meta(i);
        if (!rewrite_segment_metadata->has_name() && i < op_write->deprecated_rewrite_segments_size()) {
            rewrite_segment_metadata->set_name(op_write->deprecated_rewrite_segments(i));
        }
    }
    // dels_meta / rewrite_segments_meta are now canonical; drop the legacy arrays (the rowset's own
    // legacy arrays were already cleared by normalize_rowset_after_load above).
    op_write->clear_deprecated_dels();
    op_write->clear_deprecated_del_encryption_metas();
    op_write->clear_deprecated_shared_dels();
    op_write->clear_deprecated_rewrite_segments();
}

// ---- BEFORE SAVE --------------------------------------------------------------------------------
// segment_metas / dels_meta / rewrite_segments_meta are AUTHORITATIVE. Clear the deprecated legacy
// arrays and rebuild them from the structured fields so a BE rolled back to a pre-feature version
// reads exactly what the structured fields say (and stale legacy entries from a removed segment can
// never resurrect). Each legacy array stays all-or-nothing (size 0 or == segment count), matching
// what pre-feature producers wrote and what the legacy readers expect.

Status normalize_rowset_before_save(RowsetMetadataPB* rowset_metadata) {
    // If segment_metas is empty there is no authoritative source to rebuild from (e.g. a metadata
    // constructed directly from the legacy fields, or written by a pre-feature path); leave the
    // legacy arrays untouched rather than wiping them.
    if (rowset_metadata->segment_metas_size() == 0) {
        return Status::OK();
    }
    rowset_metadata->clear_deprecated_segments();
    rowset_metadata->clear_deprecated_segment_size();
    rowset_metadata->clear_deprecated_segment_encryption_metas();
    rowset_metadata->clear_deprecated_shared_segments();
    rowset_metadata->clear_deprecated_bundle_file_offsets();

    bool all_have_size = true;
    bool has_any_encryption_meta = false;
    bool has_any_shared = false;
    int bundle_file_offset_count = 0;
    for (const auto& segment_metadata : rowset_metadata->segment_metas()) {
        all_have_size &= segment_metadata.has_size();
        has_any_encryption_meta |= segment_metadata.has_encryption_meta();
        has_any_shared |= segment_metadata.has_shared();
        bundle_file_offset_count += segment_metadata.has_bundle_file_offset() ? 1 : 0;
    }
    // bundle_file_offset is all-or-nothing per rowset (a rowset is either fully bundled or fully
    // standalone). A mixed state cannot be encoded faithfully into the flat legacy array because 0 is
    // a valid offset, so an absent entry would read as "bundled at offset 0" on a rolled-back BE.
    if (bundle_file_offset_count != 0 && bundle_file_offset_count != rowset_metadata->segment_metas_size()) {
        return Status::Corruption(
                fmt::format("lake rowset has a mix of bundled and standalone segments: {} of {} segment_metas "
                            "carry bundle_file_offset",
                            bundle_file_offset_count, rowset_metadata->segment_metas_size()));
    }
    const bool all_have_bundle_file_offset = bundle_file_offset_count == rowset_metadata->segment_metas_size();
    // Rebuild each legacy array from segment_metas. The all_*/has_any_* flags keep every optional array
    // all-or-nothing (size 0 or == segment count), matching what the legacy readers expect.
    for (const auto& segment_metadata : rowset_metadata->segment_metas()) {
        rowset_metadata->add_deprecated_segments(segment_metadata.filename());
        if (all_have_size) {
            rowset_metadata->add_deprecated_segment_size(segment_metadata.size());
        }
        if (has_any_encryption_meta) {
            rowset_metadata->add_deprecated_segment_encryption_metas(segment_metadata.encryption_meta());
        }
        if (has_any_shared) {
            rowset_metadata->add_deprecated_shared_segments(segment_metadata.shared());
        }
        if (all_have_bundle_file_offset) {
            rowset_metadata->add_deprecated_bundle_file_offsets(segment_metadata.bundle_file_offset());
        }
    }
    return Status::OK();
}

Status normalize_op_write_before_save(TxnLogPB::OpWrite* op_write) {
    if (op_write->has_rowset()) {
        RETURN_IF_ERROR(normalize_rowset_before_save(op_write->mutable_rowset()));
    }

    // Only rebuild a legacy array group when its structured source is non-empty; otherwise leave the
    // legacy arrays untouched (see normalize_rowset_before_save for the same rationale).
    if (op_write->dels_meta_size() > 0) {
        op_write->clear_deprecated_dels();
        op_write->clear_deprecated_del_encryption_metas();
        op_write->clear_deprecated_shared_dels();
        bool has_any_encryption_meta = false;
        bool has_any_shared = false;
        for (const auto& del_file_metadata : op_write->dels_meta()) {
            has_any_encryption_meta |= del_file_metadata.has_encryption_meta();
            has_any_shared |= del_file_metadata.has_shared();
        }
        for (const auto& del_file_metadata : op_write->dels_meta()) {
            op_write->add_deprecated_dels(del_file_metadata.name());
            if (has_any_encryption_meta) {
                op_write->add_deprecated_del_encryption_metas(del_file_metadata.encryption_meta());
            }
            if (has_any_shared) {
                op_write->add_deprecated_shared_dels(del_file_metadata.shared());
            }
        }
    }

    if (op_write->rewrite_segments_meta_size() > 0) {
        op_write->clear_deprecated_rewrite_segments();
        for (const auto& rewrite_segment_metadata : op_write->rewrite_segments_meta()) {
            op_write->add_deprecated_rewrite_segments(rewrite_segment_metadata.name());
        }
    }
    return Status::OK();
}

// ---- Top-level walkers --------------------------------------------------------------------------

void normalize_tablet_metadata_after_load(TabletMetadataPB* tablet_metadata) {
    for (auto& rowset_metadata : *tablet_metadata->mutable_rowsets()) {
        normalize_rowset_after_load(&rowset_metadata);
    }
    for (auto& rowset_metadata : *tablet_metadata->mutable_compaction_inputs()) {
        normalize_rowset_after_load(&rowset_metadata);
    }
}

Status normalize_tablet_metadata_before_save(TabletMetadataPB* tablet_metadata) {
    for (auto& rowset_metadata : *tablet_metadata->mutable_rowsets()) {
        RETURN_IF_ERROR(normalize_rowset_before_save(&rowset_metadata));
    }
    for (auto& rowset_metadata : *tablet_metadata->mutable_compaction_inputs()) {
        RETURN_IF_ERROR(normalize_rowset_before_save(&rowset_metadata));
    }
    return Status::OK();
}

void normalize_txn_log_after_load(TxnLogPB* txn_log) {
    if (txn_log->has_op_write()) {
        normalize_op_write_after_load(txn_log->mutable_op_write());
    }
    if (txn_log->has_op_compaction() && txn_log->op_compaction().has_output_rowset()) {
        normalize_rowset_after_load(txn_log->mutable_op_compaction()->mutable_output_rowset());
    }
    if (txn_log->has_op_schema_change()) {
        for (auto& rowset_metadata : *txn_log->mutable_op_schema_change()->mutable_rowsets()) {
            normalize_rowset_after_load(&rowset_metadata);
        }
    }
    if (txn_log->has_op_parallel_compaction()) {
        for (auto& subtask_compaction : *txn_log->mutable_op_parallel_compaction()->mutable_subtask_compactions()) {
            if (subtask_compaction.has_output_rowset()) {
                normalize_rowset_after_load(subtask_compaction.mutable_output_rowset());
            }
        }
    }
    if (txn_log->has_op_replication()) {
        auto* op_replication = txn_log->mutable_op_replication();
        for (auto& op_write : *op_replication->mutable_op_writes()) {
            normalize_op_write_after_load(&op_write);
        }
        if (op_replication->has_tablet_metadata()) {
            normalize_tablet_metadata_after_load(op_replication->mutable_tablet_metadata());
        }
    }
}

Status normalize_txn_log_before_save(TxnLogPB* txn_log) {
    if (txn_log->has_op_write()) {
        RETURN_IF_ERROR(normalize_op_write_before_save(txn_log->mutable_op_write()));
    }
    if (txn_log->has_op_compaction() && txn_log->op_compaction().has_output_rowset()) {
        RETURN_IF_ERROR(normalize_rowset_before_save(txn_log->mutable_op_compaction()->mutable_output_rowset()));
    }
    if (txn_log->has_op_schema_change()) {
        for (auto& rowset_metadata : *txn_log->mutable_op_schema_change()->mutable_rowsets()) {
            RETURN_IF_ERROR(normalize_rowset_before_save(&rowset_metadata));
        }
    }
    if (txn_log->has_op_parallel_compaction()) {
        for (auto& subtask_compaction : *txn_log->mutable_op_parallel_compaction()->mutable_subtask_compactions()) {
            if (subtask_compaction.has_output_rowset()) {
                RETURN_IF_ERROR(normalize_rowset_before_save(subtask_compaction.mutable_output_rowset()));
            }
        }
    }
    if (txn_log->has_op_replication()) {
        auto* op_replication = txn_log->mutable_op_replication();
        for (auto& op_write : *op_replication->mutable_op_writes()) {
            RETURN_IF_ERROR(normalize_op_write_before_save(&op_write));
        }
        if (op_replication->has_tablet_metadata()) {
            RETURN_IF_ERROR(normalize_tablet_metadata_before_save(op_replication->mutable_tablet_metadata()));
        }
    }
    return Status::OK();
}

} // namespace starrocks::lake
