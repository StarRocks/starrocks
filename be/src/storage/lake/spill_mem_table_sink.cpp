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

#include "storage/lake/spill_mem_table_sink.h"

#include "exec/spill/options.h"
#include "exec/spill/serde.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller_factory.h"
#include "runtime/runtime_state.h"
#include "storage/aggregate_iterator.h"
#include "storage/chunk_helper.h"
#include "storage/lake/tablet_internal_parallel_merge_task.h"
#include "storage/lake/tablet_writer.h"
#include "storage/load_spill_block_manager.h"
#include "storage/merge_iterator.h"
#include "storage/storage_engine.h"
#include "util/runtime_profile.h"

namespace starrocks::lake {

SpillMemTableSink::SpillMemTableSink(LoadSpillBlockManager* block_manager, TabletWriter* writer,
                                     RuntimeProfile* profile) {
    _load_chunk_spiller = std::make_unique<LoadChunkSpiller>(block_manager, profile);
    _writer = writer;
    std::string tracker_label =
            "LoadSpillMerge-" + std::to_string(writer->tablet_id()) + "-" + std::to_string(writer->txn_id());
    _merge_mem_tracker = std::make_unique<MemTracker>(MemTrackerType::COMPACTION_TASK, -1, std::move(tracker_label),
                                                      GlobalEnv::GetInstance()->compaction_mem_tracker());
}

Status SpillMemTableSink::flush_chunk(const Chunk& chunk, starrocks::SegmentPB* segment, bool eos,
                                      int64_t* flush_data_size) {
    if (eos && _load_chunk_spiller->empty()) {
        // If there is only one flush, flush it to segment directly
        RETURN_IF_ERROR(_writer->write(chunk, segment, eos));
        return _writer->flush(segment);
    }

    auto res = _load_chunk_spiller->spill(chunk);
    RETURN_IF_ERROR(res.status());
    // record append bytes to `flush_data_size`
    if (flush_data_size != nullptr) {
        *flush_data_size = res.value();
    }
    return Status::OK();
}

Status SpillMemTableSink::flush_chunk_with_deletes(const Chunk& upserts, const Column& deletes,
                                                   starrocks::SegmentPB* segment, bool eos, int64_t* flush_data_size) {
    if (eos && _load_chunk_spiller->empty()) {
        // If there is only one flush, flush it to segment directly
        RETURN_IF_ERROR(_writer->flush_del_file(deletes));
        RETURN_IF_ERROR(_writer->write(upserts, segment, eos));
        return _writer->flush(segment);
    }
    // 1. flush upsert
    RETURN_IF_ERROR(flush_chunk(upserts, segment, eos, flush_data_size));
    // 2. flush deletes
    RETURN_IF_ERROR(_writer->flush_del_file(deletes));
    return Status::OK();
}

Status SpillMemTableSink::merge_blocks_to_segments_parallel(bool do_agg, const SchemaPtr& schema) {
    MonotonicStopWatch timer;
    timer.start();
    auto token =
            StorageEngine::instance()->load_spill_block_merge_executor()->create_tablet_internal_parallel_merge_token();
    // 1. Get all spill block iterators
    ASSIGN_OR_RETURN(auto spill_block_iterator_tasks,
                     _load_chunk_spiller->generate_spill_block_input_tasks(config::load_spill_max_merge_bytes,
                                                                           config::load_spill_memory_usage_per_merge,
                                                                           true /* do_sort */, do_agg));
    // 2. Prepare all tablet writers
    std::vector<std::unique_ptr<TabletWriter>> writers;
    for (size_t i = 0; i < spill_block_iterator_tasks.iterators.size(); ++i) {
        ASSIGN_OR_RETURN(auto writer, _writer->clone());
        writers.push_back(std::move(writer));
    }
    // 3. Prepare all parallel merge tasks
    QuitFlag quit_flag;
    std::vector<std::shared_ptr<TabletInternalParallelMergeTask>> tasks;
    for (size_t i = 0; i < spill_block_iterator_tasks.iterators.size(); ++i) {
        tasks.push_back(std::make_shared<TabletInternalParallelMergeTask>(
                writers[i].get(), spill_block_iterator_tasks.iterators[i].get(), _merge_mem_tracker.get(), schema.get(),
                i, &quit_flag));
    }
    // 4. Submit all tasks to thread pool
    for (size_t i = 0; i < spill_block_iterator_tasks.iterators.size(); ++i) {
        auto submit_st = token->submit(tasks[i]);
        if (!submit_st.ok()) {
            tasks[i]->update_status(submit_st);
            break;
        }
    }
    token->wait();
    // 5. check all task status
    for (const auto& task : tasks) {
        RETURN_IF_ERROR(task->status());
    }
    // 6. merge all writers' result
    RETURN_IF_ERROR(_writer->merge_other_writers(writers));
    timer.stop();

    COUNTER_UPDATE(ADD_COUNTER(_load_chunk_spiller->profile(), "SpillMergeInputGroups", TUnit::UNIT),
                   spill_block_iterator_tasks.group_count);
    COUNTER_UPDATE(ADD_COUNTER(_load_chunk_spiller->profile(), "SpillMergeInputBytes", TUnit::BYTES),
                   spill_block_iterator_tasks.total_block_bytes);
    COUNTER_UPDATE(ADD_COUNTER(_load_chunk_spiller->profile(), "SpillMergeCount", TUnit::UNIT),
                   spill_block_iterator_tasks.iterators.size());
    COUNTER_UPDATE(ADD_COUNTER(_load_chunk_spiller->profile(), "SpillMergeDurationNs", TUnit::TIME_NS),
                   timer.elapsed_time());
    return Status::OK();
}

Status SpillMemTableSink::merge_blocks_to_segments_serial(bool do_agg, const SchemaPtr& schema) {
    auto char_field_indexes = ChunkHelper::get_char_field_indexes(*schema);
    auto write_func = [&char_field_indexes, schema, this](Chunk* chunk) {
        ChunkHelper::padding_char_columns(char_field_indexes, *schema, _writer->tablet_schema(), chunk);
        return _writer->write(*chunk, nullptr);
    };
    auto flush_func = [this]() { return _writer->flush(); };

    Status st = _load_chunk_spiller->merge_write(config::load_spill_max_merge_bytes,
                                                 config::load_spill_memory_usage_per_merge, true /* do_sort */, do_agg,
                                                 write_func, flush_func);
    LOG_IF(WARNING, !st.ok()) << fmt::format(
            "SpillMemTableSink merge blocks to segment failed, txn:{} tablet:{} msg:{}", _writer->txn_id(),
            _writer->tablet_id(), st.message());
    return st;
}

Status SpillMemTableSink::merge_blocks_to_segments() {
    TEST_SYNC_POINT_CALLBACK("SpillMemTableSink::merge_blocks_to_segments", this);
    SCOPED_THREAD_LOCAL_MEM_SETTER(_merge_mem_tracker.get(), false);
    RETURN_IF(_load_chunk_spiller->empty(), Status::OK());
    // merge process needs to control _writer's flush behavior manually
    _writer->set_auto_flush(false);

    SchemaPtr schema = _load_chunk_spiller->schema();
    bool do_agg = schema->keys_type() == KeysType::AGG_KEYS || schema->keys_type() == KeysType::UNIQUE_KEYS;

    if (_load_chunk_spiller->total_bytes() >= config::pk_parallel_execution_threshold_bytes) {
        // When bulk load happens, try to enable pk parallel execution
        _writer->try_enable_pk_parallel_execution();
        // When enable pk parallel execution, that means it will generate sst files when data loading,
        // so we need to make sure not duplicate keys exist in segment files and sst files.
        // That means we need to do aggregation when spill merge.
        if (_writer->enable_pk_parallel_execution()) {
            do_agg = true;
        }
    }

    if (config::enable_load_spill_parallel_merge) {
        return merge_blocks_to_segments_parallel(do_agg, schema);
    } else {
        return merge_blocks_to_segments_serial(do_agg, schema);
    }
}

int64_t SpillMemTableSink::txn_id() {
    return _writer->txn_id();
}

int64_t SpillMemTableSink::tablet_id() {
    return _writer->tablet_id();
}

} // namespace starrocks::lake
