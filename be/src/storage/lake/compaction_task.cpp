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

#include "storage/lake/compaction_task.h"

#include "runtime/exec_env.h"
#include "storage/lake/filenames.h"
#include "storage/lake/tablet.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/vacuum.h"

namespace starrocks::lake {

using RowsetList = CompactionTask::RowsetList;

static int64_t merge_way_count(const RowsetMetadataPB& metadata) {
    if (metadata.overlapped()) {
        return metadata.segments_size();
    } else if (metadata.segments_size() >= 1) {
        return 1;
    } else {
        return 0;
    }
}

struct RowsetSplitContext {
    explicit RowsetSplitContext(RowsetPtr r) : rowset(std::move(r)) {}

    RowsetPtr rowset;
    int64_t split_pos{0};

    bool done() const { return split_pos >= rowset->metadata().segments_size(); }
};

CompactionTask::CompactionTask(VersionedTablet tablet, RowsetList input_rowsets, CompactionTaskContext* context)
        : _txn_id(context->txn_id),
          _tablet(std::move(tablet)),
          _mem_tracker(std::make_unique<MemTracker>(MemTracker::COMPACTION, -1,
                                                    "Compaction-" + std::to_string(_tablet.metadata()->id()),
                                                    GlobalEnv::GetInstance()->compaction_mem_tracker())),
          _context(context),
          _tablet_schema(_tablet.get_schema()),
          _input_rowsets(std::move(input_rowsets)) {}

Status CompactionTask::execute_index_major_compaction(TxnLogPB* txn_log) {
    if (_tablet_schema->keys_type() == KeysType::PRIMARY_KEYS) {
        auto metadata = _tablet.metadata();
        if (metadata->enable_persistent_index() &&
            metadata->persistent_index_type() == PersistentIndexTypePB::CLOUD_NATIVE) {
            return _tablet.tablet_manager()->update_mgr()->execute_index_major_compaction(_tablet.metadata()->id(),
                                                                                          *metadata, txn_log);
        }
    }
    return Status::OK();
}

RowsetPtr CompactionTask::build_rowset_from_metadata(RowsetMetadataPB* metadata, int32_t index) {
    auto tablet_id = _tablet.id();
    auto tablet_mgr = _tablet.tablet_manager();
    auto rowset = std::make_shared<Rowset>(tablet_mgr, tablet_id, metadata, index, _tablet_schema);
    rowset->take_metadata_ownership();
    return rowset;
}

StatusOr<RowsetPtr> CompactionTask::split(RowsetSplitContext* context, int64_t max_merge_way) {
    CHECK_GT(max_merge_way, 0);
    auto inputs = merge_way_count(context->rowset->metadata());
    if (context->split_pos == 0 && max_merge_way >= inputs) {
        context->split_pos = context->rowset->metadata().segments_size();
        return context->rowset;
    } else {
        DCHECK(!context->rowset->metadata().has_delete_predicate());
        auto start_index = context->split_pos;
        auto end_index = std::min<int64_t>(start_index + max_merge_way, context->rowset->metadata().segments_size());
        auto new_rowset_metadata = std::make_unique<RowsetMetadataPB>();
        new_rowset_metadata->set_id(context->rowset->metadata().id());
        new_rowset_metadata->set_overlapped((end_index - start_index) > 1);
        new_rowset_metadata->set_version(context->rowset->metadata().version());
        for (auto i = start_index; i < end_index; i++) {
            new_rowset_metadata->add_segments(context->rowset->metadata().segments(i));
        }

        // Get segment list
        auto io_options = LakeIOOptions{.fill_data_cache = false};
        auto fill_meta_cache = true;
        ASSIGN_OR_RETURN(auto segments, context->rowset->segments(io_options, fill_meta_cache));

        // Calculate "data_size" and "num_rows"
        auto data_size = 0;
        auto num_rows = 0;
        for (auto i = start_index; i < end_index; i++) {
            num_rows += segments[i]->num_rows();
            ASSIGN_OR_RETURN(auto n, segments[i]->get_data_size());
            new_rowset_metadata->add_segment_size(n);
            data_size += n;
        }
        new_rowset_metadata->set_data_size(data_size);
        new_rowset_metadata->set_num_rows(num_rows);

        // Create a new rowset.
        // NOTE: rowset index determines whether to apply delete predicate on the rowset. Here, the rowset index
        // needs to remain unchanged.
        auto new_rowset = build_rowset_from_metadata(new_rowset_metadata.release(), context->rowset->index());
        new_rowset->set_first_segment_id(start_index);
        context->split_pos = end_index;
        return new_rowset;
    }
}

// Split "input_rowsets" into multiple groups. Each group will perform the compaction operation
// independently, and the number of merge ways of each group will not exceed "max_way_count".
// If necessary, one rowset in "input_rowsets" may be split into multiple rowsets to ensure that
// the number of merge ways per group does not exceed "max_way_count".
StatusOr<std::vector<RowsetList>> CompactionTask::split_rowsets(const RowsetList& input_rowsets,
                                                                int64_t max_merge_way) {
    if (max_merge_way < 2) {
        return std::vector<RowsetList>{input_rowsets};
    }
    auto way_count = int64_t{0};
    for (auto& rowset : input_rowsets) {
        way_count += merge_way_count(rowset->metadata());
    }
    if (way_count <= max_merge_way) {
        return std::vector<RowsetList>{input_rowsets};
    }

    auto contexes = std::vector<RowsetSplitContext>{};
    contexes.reserve(input_rowsets.size());
    for (auto& rowset : input_rowsets) {
        contexes.emplace_back(rowset);
    }
    std::reverse(contexes.begin(), contexes.end());

    auto groups = std::vector<RowsetList>{};
    auto curr_group = RowsetList{};
    way_count = 0;
    while (!contexes.empty()) {
        auto& context = contexes.back();
        ASSIGN_OR_RETURN(auto rowset, split(&context, max_merge_way - way_count));
        if (context.done()) {
            contexes.pop_back();
        }
        way_count += merge_way_count(rowset->metadata());
        curr_group.emplace_back(std::move(rowset));
        if (way_count >= max_merge_way) {
            groups.emplace_back().swap(curr_group);
            way_count = 0;
        }
    }
    if (!curr_group.empty()) {
        groups.emplace_back().swap(curr_group);
    }
    return groups;
}

void CompactionTask::delete_rowsets(const RowsetList& rowsets) {
    auto tablet_mgr = _tablet.tablet_manager();
    auto file_paths = std::vector<std::string>{};
    for (auto& rowset : rowsets) {
        for (auto& segment : rowset->metadata().segments()) {
            auto path = tablet_mgr->segment_location(_tablet.id(), segment);
            file_paths.emplace_back(std::move(path));
        }
    }
    delete_files_async(std::move(file_paths));
}

StatusOr<RowsetPtr> CompactionTask::execute(const RowsetList& input_rowsets, const CancelFunc& cancel_func, ThreadPool* flush_pool) {
    if (UNLIKELY(input_rowsets.empty())) {
        auto output_rowset = std::make_unique<RowsetMetadataPB>();
        output_rowset->set_num_rows(0);
        output_rowset->set_data_size(0);
        output_rowset->set_overlapped(false);
        return build_rowset_from_metadata(output_rowset.release(), 0);
    }

    auto round = 0;
    auto inputs = _input_rowsets;
    auto outputs = RowsetList{};
    auto max_merge_way = config::lake_compaction_max_merge_way_count;
    while (true) {
        round++;
        auto defer = DeferOp([=]() {
            if (round > 1) delete_rowsets(inputs);
        });
        ASSIGN_OR_RETURN(auto groups, split_rowsets(inputs, max_merge_way));
        LOG(INFO) << "Compacting rowsets for tablet_id=" << _tablet.id() << " txn_id=" << _txn_id << " round=" << round
                  << " input_rowset_count=" << inputs.size() << " output_rowset_count=" << groups.size();
        outputs.reserve(groups.size());
        for (auto& group : groups) {
            ASSIGN_OR_RETURN(auto output, compact(group, cancel_func, flush_pool));
            CHECK(!output->is_overlapped());
            outputs.emplace_back(std::move(output));
            group.clear();
        }
        if (outputs.size() == 1) {
            break;
        } else {
            inputs = std::move(outputs);
            DCHECK(outputs.empty());
        }
    }
    CHECK_EQ(1, outputs.size());
    return outputs[0];
}

Status CompactionTask::execute(const CancelFunc& cancel_func, ThreadPool* flush_pool) {
    ASSIGN_OR_RETURN(auto output, execute(_input_rowsets, cancel_func, flush_pool));
    auto txn_log = std::make_shared<TxnLog>();
    auto op_compaction = txn_log->mutable_op_compaction();
    txn_log->set_tablet_id(_tablet.id());
    txn_log->set_txn_id(_txn_id);
    for (auto& rowset : _input_rowsets) {
        op_compaction->add_input_rowsets(rowset->id());
    }
    op_compaction->mutable_output_rowset()->CopyFrom(output->metadata());
    RETURN_IF_ERROR(execute_index_major_compaction(txn_log.get()));
    RETURN_IF_ERROR(_tablet.tablet_manager()->put_txn_log(txn_log));
    if (_tablet_schema->keys_type() == KeysType::PRIMARY_KEYS) {
        // preload primary key table's compaction state
        Tablet t(_tablet.tablet_manager(), _tablet.id());
        _tablet.tablet_manager()->update_mgr()->preload_compaction_state(*txn_log, t, _tablet_schema);
    }
    _context->progress.update(100);
    return Status::OK();
}

} // namespace starrocks::lake
