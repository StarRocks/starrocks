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
#include "storage/lake/tablet_writer.h"
#include "storage/load_spill_block_manager.h"
#include "storage/merge_iterator.h"
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

Status SpillMemTableSink::merge_blocks_to_segments() {
    TEST_SYNC_POINT_CALLBACK("SpillMemTableSink::merge_blocks_to_segments", this);
    SCOPED_THREAD_LOCAL_MEM_SETTER(_merge_mem_tracker.get(), false);
    RETURN_IF(_load_chunk_spiller->empty(), Status::OK());
    // merge process needs to control _writer's flush behavior manually
    _writer->set_auto_flush(false);

    SchemaPtr schema = _load_chunk_spiller->schema();
    bool do_agg = schema->keys_type() == KeysType::AGG_KEYS || schema->keys_type() == KeysType::UNIQUE_KEYS;
    auto char_field_indexes = ChunkHelper::get_char_field_indexes(*schema);
    auto write_func = [&char_field_indexes, schema, this](Chunk* chunk) {
        ChunkHelper::padding_char_columns(char_field_indexes, *schema, _writer->tablet_schema(), chunk);
        return _writer->write(*chunk, nullptr);
    };
    auto flush_func = [this]() { return _writer->flush(); };

    Status st = _load_chunk_spiller->merge_write(config::load_spill_max_merge_bytes, true /* do_sort */, do_agg,
                                                 write_func, flush_func);
    LOG_IF(WARNING, !st.ok()) << fmt::format(
            "SpillMemTableSink merge blocks to segment failed, txn:{} tablet:{} msg:{}", _writer->txn_id(),
            _writer->tablet_id(), st.message());
    return st;
}

int64_t SpillMemTableSink::txn_id() {
    return _writer->txn_id();
}

int64_t SpillMemTableSink::tablet_id() {
    return _writer->tablet_id();
}

} // namespace starrocks::lake
