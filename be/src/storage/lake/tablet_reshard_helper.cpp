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

#include "storage/tablet_range.h"

namespace starrocks::lake::tablet_reshard_helper {

void set_all_data_files_shared(RowsetMetadataPB* rowset_metadata) {
    auto* shared_segments = rowset_metadata->mutable_shared_segments();
    shared_segments->Clear();
    shared_segments->Resize(rowset_metadata->segments_size(), true);

    for (auto& del : *rowset_metadata->mutable_del_files()) {
        del.set_shared(true);
    }
}

void set_all_data_files_shared(TxnLogPB* txn_log) {
    if (txn_log->has_op_write()) {
        auto* op_write = txn_log->mutable_op_write();
        if (op_write->has_rowset()) {
            set_all_data_files_shared(op_write->mutable_rowset());
        }
    }

    if (txn_log->has_op_compaction()) {
        auto* op_compaction = txn_log->mutable_op_compaction();
        if (op_compaction->has_output_rowset()) {
            set_all_data_files_shared(op_compaction->mutable_output_rowset());
        }
        if (op_compaction->has_output_sstable()) {
            auto* sstable = op_compaction->mutable_output_sstable();
            sstable->set_shared(true);
        }
        for (auto& sstable : *op_compaction->mutable_output_sstables()) {
            sstable.set_shared(true);
        }
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
        for (auto& op_write : *txn_log->mutable_op_replication()->mutable_op_writes()) {
            if (op_write.has_rowset()) {
                set_all_data_files_shared(op_write.mutable_rowset());
            }
        }
    }

    if (txn_log->has_op_parallel_compaction()) {
        auto* op_parallel_compaction = txn_log->mutable_op_parallel_compaction();
        for (auto& subtask_op : *op_parallel_compaction->mutable_subtask_compactions()) {
            if (subtask_op.has_output_rowset()) {
                set_all_data_files_shared(subtask_op.mutable_output_rowset());
            }
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

} // namespace starrocks::lake::tablet_reshard_helper
