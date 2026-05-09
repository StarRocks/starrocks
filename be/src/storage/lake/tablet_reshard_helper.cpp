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

#include "storage/lake/tablet_reshard_helper.h"

#include <algorithm>
#include <numeric>

#include "common/logging.h"
#include "storage/lake/tablet_manager.h"
#include "storage/tablet_range.h"

namespace starrocks::lake::tablet_reshard_helper {

void allocate_proportionally(int64_t total, const std::vector<int64_t>& weights, std::vector<int64_t>* out) {
    DCHECK(out != nullptr);
    DCHECK_GE(total, 0);
    DCHECK_EQ(weights.size(), out->size());
    DCHECK(!weights.empty());
    for (auto w : weights) DCHECK_GE(w, 0);

    const size_t n = weights.size();

    if (total == 0) {
        std::fill(out->begin(), out->end(), 0);
        return;
    }

    __int128 sum_w = 0;
    for (auto w : weights) sum_w += static_cast<__int128>(w);

    // All-zero weights with non-zero total: deterministic uniform fallback.
    // Allocate floor(total/N) to each, +1 to the lowest indices to absorb the
    // remainder. Preserves Σ exactly without stranding any of the total.
    if (sum_w == 0) {
        const int64_t base = total / static_cast<int64_t>(n);
        const int64_t leftover = total - base * static_cast<int64_t>(n);
        for (size_t i = 0; i < n; ++i) {
            (*out)[i] = base + (static_cast<int64_t>(i) < leftover ? 1 : 0);
        }
        return;
    }

    // Largest-remainder method. base[i] = floor(total * w[i] / sum_w);
    // remainder[i] = (total * w[i]) mod sum_w. The total minus Σ base[i]
    // is the leftover (always in [0, n)) and is distributed +1 each to the
    // entries with the largest fractional remainders, breaking ties by
    // ascending index for determinism.
    //
    // remainder is stored as __int128 because rem can be in [0, sum_w) and
    // sum_w may itself exceed INT64_MAX when there are many large weights;
    // truncating to int64_t there would corrupt the largest-remainder ordering.
    // numerator and base also stay in __int128: numerator = total * weights[i]
    // ≤ INT64_MAX * INT64_MAX ≈ 8.5e37, well within __int128's ~1.7e38 limit.
    std::vector<__int128> remainders(n, 0);
    int64_t sum_base = 0;
    for (size_t i = 0; i < n; ++i) {
        if (weights[i] == 0) {
            (*out)[i] = 0;
            remainders[i] = 0;
            continue;
        }
        const __int128 numerator = static_cast<__int128>(total) * static_cast<__int128>(weights[i]);
        const __int128 base = numerator / sum_w;
        const __int128 rem = numerator - base * sum_w;
        (*out)[i] = static_cast<int64_t>(base);
        remainders[i] = rem;
        sum_base += static_cast<int64_t>(base);
    }
    int64_t leftover = total - sum_base;

    if (leftover > 0) {
        std::vector<size_t> order(n);
        std::iota(order.begin(), order.end(), 0);
        std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
            if (remainders[a] != remainders[b]) return remainders[a] > remainders[b];
            return a < b;
        });
        for (size_t k = 0; k < n && leftover > 0; ++k, --leftover) {
            (*out)[order[k]]++;
        }
    }

#ifndef NDEBUG
    int64_t actual_sum = 0;
    for (auto v : *out) actual_sum += v;
    DCHECK_EQ(actual_sum, total);
#endif
}

void cap_and_redistribute_dels(const std::vector<int64_t>& rows, std::vector<int64_t>* dels) {
    DCHECK(dels != nullptr);
    DCHECK_EQ(rows.size(), dels->size());
    for (auto r : rows) DCHECK_GE(r, 0);
    for (auto d : *dels) DCHECK_GE(d, 0);

    const size_t n = rows.size();

    // Cap each (*dels)[i] to rows[i]; accumulate the over-cap mass as overflow.
    int64_t overflow = 0;
    for (size_t i = 0; i < n; ++i) {
        if ((*dels)[i] > rows[i]) {
            overflow += (*dels)[i] - rows[i];
            (*dels)[i] = rows[i];
        }
    }
    if (overflow == 0) return;

    // Redistribute overflow to buckets with headroom, ordered by
    // (headroom desc, index asc) for deterministic output.
    std::vector<size_t> order(n);
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
        const int64_t ha = rows[a] - (*dels)[a];
        const int64_t hb = rows[b] - (*dels)[b];
        if (ha != hb) return ha > hb;
        return a < b;
    });
    for (size_t idx : order) {
        if (overflow <= 0) break;
        const int64_t headroom = rows[idx] - (*dels)[idx];
        if (headroom <= 0) break; // remaining are zero-headroom under desc sort
        const int64_t take = std::min(headroom, overflow);
        (*dels)[idx] += take;
        overflow -= take;
    }
    // overflow > 0 here only if the input violated the feasibility precondition
    // (Σ pre-cap dels > Σ rows). The post-condition becomes Σ dels == Σ rows
    // (the maximum feasible) with per-bucket cap honored, matching the
    // best-effort contract in tablet_reshard_helper.h.
}

void set_all_data_files_shared(RowsetMetadataPB* rowset_metadata) {
    auto* shared_segments = rowset_metadata->mutable_shared_segments();
    shared_segments->Clear();
    shared_segments->Resize(rowset_metadata->segments_size(), true);

    for (auto& del : *rowset_metadata->mutable_del_files()) {
        del.set_shared(true);
    }
}

// Marks all data files referenced by an OpWrite as shared. Used for both the
// top-level op_write and every op_replication.op_writes[*], which share the same
// OpWrite schema and the same publish path (apply_opwrite / batch_apply_opwrite).
static void set_all_data_files_shared(TxnLogPB_OpWrite* op_write) {
    if (op_write->has_rowset()) {
        set_all_data_files_shared(op_write->mutable_rowset());
    }
    for (auto& sst : *op_write->mutable_ssts()) {
        sst.set_shared(true);
    }
    // op_write.dels is a plain string list with no per-entry shared flag; populate
    // the parallel `shared_dels` so that apply_opwrite can transfer the shared
    // state into RowsetMetadataPB.del_files when the txn is published on a child.
    auto* shared_dels = op_write->mutable_shared_dels();
    shared_dels->Clear();
    shared_dels->Resize(op_write->dels_size(), true);
}

// Marks all data files referenced by an OpCompaction as shared. Used both for the
// top-level op_compaction and for every nested subtask in op_parallel_compaction,
// so the rule must stay consistent across both.
static void set_all_data_files_shared(TxnLogPB_OpCompaction* op_compaction) {
    if (op_compaction->has_output_rowset()) {
        set_all_data_files_shared(op_compaction->mutable_output_rowset());
    }
    if (op_compaction->has_output_sstable()) {
        op_compaction->mutable_output_sstable()->set_shared(true);
    }
    for (auto& sstable : *op_compaction->mutable_output_sstables()) {
        sstable.set_shared(true);
    }
    for (auto& sst : *op_compaction->mutable_ssts()) {
        sst.set_shared(true);
    }
}

void set_all_data_files_shared(TxnLogPB* txn_log) {
    if (txn_log->has_op_write()) {
        set_all_data_files_shared(txn_log->mutable_op_write());
    }

    if (txn_log->has_op_compaction()) {
        set_all_data_files_shared(txn_log->mutable_op_compaction());
    }

    if (txn_log->has_op_schema_change()) {
        auto* op_schema_change = txn_log->mutable_op_schema_change();
        for (auto& rowset : *op_schema_change->mutable_rowsets()) {
            set_all_data_files_shared(&rowset);
        }
        if (op_schema_change->has_delvec_meta()) {
            for (auto& pair : *op_schema_change->mutable_delvec_meta()->mutable_version_to_file()) {
                pair.second.set_shared(true);
            }
        }
    }

    if (txn_log->has_op_replication()) {
        // Each replication op_write flows through the same apply_opwrite path on
        // publish (see txn_log_applier.cpp apply_write_log). Share the same rule as
        // the top-level op_write so shared_dels / ssts are populated consistently.
        for (auto& op_write : *txn_log->mutable_op_replication()->mutable_op_writes()) {
            set_all_data_files_shared(&op_write);
        }
    }

    if (txn_log->has_op_parallel_compaction()) {
        auto* op_parallel_compaction = txn_log->mutable_op_parallel_compaction();
        // Each subtask is a full OpCompaction that gets republished via
        // publish_primary_compaction (see txn_log_applier.cpp) and whose ssts are
        // merged back into parent compactions (see tablet_parallel_compaction_manager.cpp).
        // Treat its files identically to a top-level OpCompaction.
        for (auto& subtask_op : *op_parallel_compaction->mutable_subtask_compactions()) {
            set_all_data_files_shared(&subtask_op);
        }
        if (op_parallel_compaction->has_output_sstable()) {
            op_parallel_compaction->mutable_output_sstable()->set_shared(true);
        }
        for (auto& sstable : *op_parallel_compaction->mutable_output_sstables()) {
            sstable.set_shared(true);
        }
    }
}

void set_all_data_files_shared(TabletMetadataPB* tablet_metadata, bool skip_delvecs) {
    for (auto& rowset_metadata : *tablet_metadata->mutable_rowsets()) {
        set_all_data_files_shared(&rowset_metadata);
    }

    if (!skip_delvecs && tablet_metadata->has_delvec_meta()) {
        for (auto& pair : *tablet_metadata->mutable_delvec_meta()->mutable_version_to_file()) {
            pair.second.set_shared(true);
        }
    }

    if (tablet_metadata->has_dcg_meta()) {
        for (auto& dcg : *tablet_metadata->mutable_dcg_meta()->mutable_dcgs()) {
            auto* shared_files = dcg.second.mutable_shared_files();
            shared_files->Clear();
            shared_files->Resize(dcg.second.column_files_size(), true);
        }
    }

    if (tablet_metadata->has_sstable_meta()) {
        for (auto& sstable : *tablet_metadata->mutable_sstable_meta()->mutable_sstables()) {
            sstable.set_shared(true);
        }
    }
}

StatusOr<TabletRangePB> intersect_range(const TabletRangePB& lhs_pb, const TabletRangePB& rhs_pb) {
    TabletRange lhs;
    RETURN_IF_ERROR(lhs.from_proto(lhs_pb));
    TabletRange rhs;
    RETURN_IF_ERROR(rhs.from_proto(rhs_pb));
    ASSIGN_OR_RETURN(auto intersected, lhs.intersect(rhs));
    TabletRangePB result;
    intersected.to_proto(&result);
    return result;
}

StatusOr<TabletRangePB> union_range(const TabletRangePB& lhs_pb, const TabletRangePB& rhs_pb) {
    TabletRange lhs;
    RETURN_IF_ERROR(lhs.from_proto(lhs_pb));
    TabletRange rhs;
    RETURN_IF_ERROR(rhs.from_proto(rhs_pb));
    auto result = lhs.union_with(rhs);
    TabletRangePB result_pb;
    result.to_proto(&result_pb);
    return result_pb;
}

Status update_rowset_range(RowsetMetadataPB* rowset, const TabletRangePB& range) {
    DCHECK(rowset != nullptr);
    if (!rowset->has_range()) {
        rowset->mutable_range()->CopyFrom(range);
        return Status::OK();
    }

    ASSIGN_OR_RETURN(auto intersected, intersect_range(rowset->range(), range));
    rowset->mutable_range()->CopyFrom(intersected);
    return Status::OK();
}

Status update_rowset_ranges(TxnLogPB* txn_log, const TabletRangePB& range) {
    if (txn_log->has_op_write() && txn_log->mutable_op_write()->has_rowset()) {
        RETURN_IF_ERROR(update_rowset_range(txn_log->mutable_op_write()->mutable_rowset(), range));
    }
    if (txn_log->has_op_compaction() && txn_log->mutable_op_compaction()->has_output_rowset()) {
        RETURN_IF_ERROR(update_rowset_range(txn_log->mutable_op_compaction()->mutable_output_rowset(), range));
    }
    if (txn_log->has_op_schema_change()) {
        for (auto& rowset : *txn_log->mutable_op_schema_change()->mutable_rowsets()) {
            RETURN_IF_ERROR(update_rowset_range(&rowset, range));
        }
    }
    if (txn_log->has_op_replication()) {
        for (auto& op_write : *txn_log->mutable_op_replication()->mutable_op_writes()) {
            if (op_write.has_rowset()) {
                RETURN_IF_ERROR(update_rowset_range(op_write.mutable_rowset(), range));
            }
        }
    }
    if (txn_log->has_op_parallel_compaction()) {
        for (auto& subtask : *txn_log->mutable_op_parallel_compaction()->mutable_subtask_compactions()) {
            if (subtask.has_output_rowset()) {
                RETURN_IF_ERROR(update_rowset_range(subtask.mutable_output_rowset(), range));
            }
        }
    }
    return Status::OK();
}

void update_rowset_data_stats(RowsetMetadataPB* rowset, int32_t split_count, int32_t split_index) {
    if (split_count <= 1) return;

    if (rowset->has_num_rows()) {
        int64_t num_rows = rowset->num_rows();
        rowset->set_num_rows(num_rows / split_count + (split_index < num_rows % split_count ? 1 : 0));
    }
    if (rowset->has_data_size()) {
        int64_t data_size = rowset->data_size();
        rowset->set_data_size(data_size / split_count + (split_index < data_size % split_count ? 1 : 0));
    }
    if (rowset->has_num_dels()) {
        int64_t num_dels = rowset->num_dels();
        int64_t scaled_num_dels = num_dels / split_count + (split_index < num_dels % split_count ? 1 : 0);
        rowset->set_num_dels(std::min<int64_t>(scaled_num_dels, rowset->num_rows()));
    }
}

namespace {

// Append output-side files of a single OpCompaction. Used both for the
// top-level op_compaction and for every op_parallel_compaction.subtask_compactions[*].
//
// Only files newly written by this compaction are collected. In partial
// compaction the output_rowset.segments() list concatenates uncompacted
// (reused) input segments with newly written ones; the slice
// [new_segment_offset, new_segment_offset + new_segment_count) identifies the
// new ones. output_rowset.del_files() may likewise be carried over from input
// rowsets (PK publish rebuilds them from input del files in
// MetaFileBuilder::apply_opcompaction), so they are not collected here.
// input_rowsets / input_sstables are also excluded — they have been absorbed
// into the merged tablet by the preceding merge publish and remain live.
//
// No path-level dedup is performed: file names in this log are produced by
// UUID-based writers per tablet/txn, so collisions are not expected in
// practice. Follows the same no-dedup pattern as
// transactions.cpp::collect_files_in_log.
void append_compaction_output_files(const TxnLogPB_OpCompaction& op_compaction, int64_t tablet_id,
                                    TabletManager* tablet_manager, std::vector<std::string>* output_paths) {
    if (op_compaction.has_output_rowset()) {
        const auto& segments = op_compaction.output_rowset().segments();
        if (op_compaction.has_new_segment_count()) {
            // Normal or partial compaction: only the
            // [new_segment_offset, new_segment_offset + new_segment_count)
            // slice is newly written; the rest are reused input segments.
            // Defensively skip the whole slice if either field is negative
            // (malformed log), matching the skip-on-empty-count behavior.
            const int32_t new_segment_offset = op_compaction.new_segment_offset();
            const int32_t new_segment_count = op_compaction.new_segment_count();
            if (new_segment_offset >= 0 && new_segment_count > 0) {
                for (int32_t idx = new_segment_offset, taken = 0; idx < segments.size() && taken < new_segment_count;
                     ++idx, ++taken) {
                    output_paths->emplace_back(tablet_manager->segment_location(tablet_id, segments[idx]));
                }
            }
        } else {
            // Synthesized logs without an explicit new-segment marker — notably
            // the merged subtask in op_parallel_compaction built by
            // tablet_parallel_compaction_manager, whose output_rowset only
            // contains segments the subtasks newly produced. Treat all as new.
            for (const auto& segment : segments) {
                output_paths->emplace_back(tablet_manager->segment_location(tablet_id, segment));
            }
        }
    }
    for (const auto& file_meta : op_compaction.ssts()) {
        output_paths->emplace_back(tablet_manager->sst_location(tablet_id, file_meta.name()));
    }
    if (op_compaction.has_output_sstable()) {
        output_paths->emplace_back(tablet_manager->sst_location(tablet_id, op_compaction.output_sstable().filename()));
    }
    for (const auto& sstable : op_compaction.output_sstables()) {
        output_paths->emplace_back(tablet_manager->sst_location(tablet_id, sstable.filename()));
    }
    if (op_compaction.has_lcrm_file()) {
        output_paths->emplace_back(tablet_manager->lcrm_location(tablet_id, op_compaction.lcrm_file().name()));
    }
}

} // namespace

std::vector<std::string> collect_compaction_output_file_paths(const TxnLogPB& txn_log, TabletManager* tablet_manager) {
    DCHECK(tablet_manager != nullptr);

    const int64_t tablet_id = txn_log.tablet_id();
    std::vector<std::string> output_paths;

    if (txn_log.has_op_compaction()) {
        append_compaction_output_files(txn_log.op_compaction(), tablet_id, tablet_manager, &output_paths);
    }
    if (txn_log.has_op_parallel_compaction()) {
        const auto& op_parallel_compaction = txn_log.op_parallel_compaction();
        for (const auto& subtask : op_parallel_compaction.subtask_compactions()) {
            append_compaction_output_files(subtask, tablet_id, tablet_manager, &output_paths);
        }
        if (op_parallel_compaction.has_output_sstable()) {
            output_paths.emplace_back(
                    tablet_manager->sst_location(tablet_id, op_parallel_compaction.output_sstable().filename()));
        }
        for (const auto& sstable : op_parallel_compaction.output_sstables()) {
            output_paths.emplace_back(tablet_manager->sst_location(tablet_id, sstable.filename()));
        }
        // orphan_lcrm_files holds the ORIGINAL per-subtask lcrm files copied
        // by the parallel-compaction manager (tablet_parallel_compaction_manager.cpp),
        // distinct from the merged lcrm placed on each subtask_compactions[i]
        // by _merge_subtask_lcrm_files. Both sets need cleanup on drop.
        for (const auto& file_meta : op_parallel_compaction.orphan_lcrm_files()) {
            output_paths.emplace_back(tablet_manager->lcrm_location(tablet_id, file_meta.name()));
        }
    }
    return output_paths;
}

void update_txn_log_data_stats(TxnLogPB* txn_log, int32_t split_count, int32_t split_index) {
    if (txn_log->has_op_write() && txn_log->mutable_op_write()->has_rowset()) {
        update_rowset_data_stats(txn_log->mutable_op_write()->mutable_rowset(), split_count, split_index);
    }
    if (txn_log->has_op_compaction() && txn_log->mutable_op_compaction()->has_output_rowset()) {
        update_rowset_data_stats(txn_log->mutable_op_compaction()->mutable_output_rowset(), split_count, split_index);
    }
    if (txn_log->has_op_schema_change()) {
        for (auto& rowset : *txn_log->mutable_op_schema_change()->mutable_rowsets()) {
            update_rowset_data_stats(&rowset, split_count, split_index);
        }
    }
    if (txn_log->has_op_replication()) {
        for (auto& op_write : *txn_log->mutable_op_replication()->mutable_op_writes()) {
            if (op_write.has_rowset()) {
                update_rowset_data_stats(op_write.mutable_rowset(), split_count, split_index);
            }
        }
    }
    if (txn_log->has_op_parallel_compaction()) {
        for (auto& subtask : *txn_log->mutable_op_parallel_compaction()->mutable_subtask_compactions()) {
            if (subtask.has_output_rowset()) {
                update_rowset_data_stats(subtask.mutable_output_rowset(), split_count, split_index);
            }
        }
    }
}

} // namespace starrocks::lake::tablet_reshard_helper
