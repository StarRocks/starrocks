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
#include "storage/lake/load_spill_block_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/merge_iterator.h"

namespace starrocks::lake {

Status LoadSpillOutputDataStream::append(RuntimeState* state, const std::vector<Slice>& data, size_t total_write_size,
                                         size_t write_num_rows) {
    _append_rows += write_num_rows;
    size_t total_size = 0;
    // calculate total size
    std::for_each(data.begin(), data.end(), [&](const Slice& slice) { total_size += slice.size; });
    // preallocate block
    RETURN_IF_ERROR(_preallocate(total_size));
    // append data
    _append_bytes += total_size;
    return _block->append(data);
}

Status LoadSpillOutputDataStream::flush() {
    RETURN_IF_ERROR(_freeze_current_block());
    return Status::OK();
}

bool LoadSpillOutputDataStream::is_remote() const {
    return _block ? _block->is_remote() : false;
}

Status LoadSpillOutputDataStream::_freeze_current_block() {
    if (_block == nullptr) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_block->flush());
    RETURN_IF_ERROR(_block_manager->release_block(_block));
    // Save this block into block container.
    _block_manager->block_container()->append_block(_block);
    _block = nullptr;
    return Status::OK();
}

Status LoadSpillOutputDataStream::_preallocate(size_t block_size) {
    // Try to preallocate from current block first.
    if (_block == nullptr || !_block->preallocate(block_size)) {
        // Freeze current block firstly.
        RETURN_IF_ERROR(_freeze_current_block());
        // Acquire new block.
        ASSIGN_OR_RETURN(_block, _block_manager->acquire_block(block_size));
    }
    return Status::OK();
}

SpillMemTableSink::SpillMemTableSink(LoadSpillBlockManager* block_manager, TabletWriter* writer,
                                     RuntimeProfile* profile) {
    _block_manager = block_manager;
    _writer = writer;
    _profile = profile;
    _runtime_state = std::make_shared<RuntimeState>();
    _spiller_factory = spill::make_spilled_factory();
    std::string tracker_label = "LoadSpillMerge-" + std::to_string(_block_manager->tablet_id()) + "-" +
                                std::to_string(_block_manager->txn_id());
    _merge_mem_tracker = std::make_unique<MemTracker>(MemTrackerType::COMPACTION_TASK, -1, std::move(tracker_label),
                                                      GlobalEnv::GetInstance()->compaction_mem_tracker());
}

Status SpillMemTableSink::_prepare(const ChunkPtr& chunk_ptr) {
    if (_spiller == nullptr) {
        // 1. alloc & prepare spiller
        spill::SpilledOptions options;
        options.encode_level = 7;
        _spiller = _spiller_factory->create(options);
        RETURN_IF_ERROR(_spiller->prepare(_runtime_state.get()));
        DCHECK(_profile != nullptr) << "SpillMemTableSink profile is null";
        spill::SpillProcessMetrics metrics(_profile, _runtime_state->mutable_total_spill_bytes());
        _spiller->set_metrics(metrics);
        // 2. prepare serde
        if (const_cast<spill::ChunkBuilder*>(&_spiller->chunk_builder())->chunk_schema()->empty()) {
            const_cast<spill::ChunkBuilder*>(&_spiller->chunk_builder())->chunk_schema()->set_schema(chunk_ptr);
            RETURN_IF_ERROR(_spiller->serde()->prepare());
        }
    }
    return Status::OK();
}

Status SpillMemTableSink::_do_spill(const Chunk& chunk, const spill::SpillOutputDataStreamPtr& output) {
    // 1. caclulate per row memory usage
    const int64_t per_row_memory_usage = chunk.memory_usage() / chunk.num_rows();
    const int64_t spill_rows = std::min(config::load_spill_max_chunk_bytes / (per_row_memory_usage + 1) + 1,
                                        (int64_t)max_merge_chunk_size);
    // 2. serialize chunk
    for (int64_t rowid = 0; rowid < chunk.num_rows(); rowid += spill_rows) {
        int64_t rows = std::min(spill_rows, (int64_t)chunk.num_rows() - rowid);
        ChunkPtr each_chunk = chunk.clone_empty();
        each_chunk->append(chunk, rowid, rows);
        RETURN_IF_ERROR(_prepare(each_chunk));
        spill::SerdeContext ctx;
        RETURN_IF_ERROR(_spiller->serde()->serialize(_runtime_state.get(), ctx, each_chunk, output, true));
    }
    if (!_schema) {
        _schema = chunk.schema();
    }
    return Status::OK();
}

Status SpillMemTableSink::flush_chunk(const Chunk& chunk, starrocks::SegmentPB* segment, bool eos,
                                      int64_t* flush_data_size) {
    if (eos && _block_manager->block_container()->empty()) {
        // If there is only one flush, flush it to segment directly
        RETURN_IF_ERROR(_writer->write(chunk, segment));
        return _writer->flush(segment);
    }
    if (chunk.num_rows() == 0) return Status::OK();
    // 1. create new block group
    _block_manager->block_container()->create_block_group();
    auto output = std::make_shared<LoadSpillOutputDataStream>(_block_manager);
    // 2. spill
    RETURN_IF_ERROR(_do_spill(chunk, output));
    // 3. flush
    RETURN_IF_ERROR(output->flush());
    // record append bytes to `flush_data_size`
    if (flush_data_size != nullptr) {
        *flush_data_size = output->append_bytes();
    }
    return Status::OK();
}

Status SpillMemTableSink::flush_chunk_with_deletes(const Chunk& upserts, const Column& deletes,
                                                   starrocks::SegmentPB* segment, bool eos, int64_t* flush_data_size) {
    if (eos && _block_manager->block_container()->empty()) {
        // If there is only one flush, flush it to segment directly
        RETURN_IF_ERROR(_writer->flush_del_file(deletes));
        RETURN_IF_ERROR(_writer->write(upserts, segment));
        return _writer->flush(segment);
    }
    // 1. flush upsert
    RETURN_IF_ERROR(flush_chunk(upserts, segment, eos, flush_data_size));
    // 2. flush deletes
    RETURN_IF_ERROR(_writer->flush_del_file(deletes));
    return Status::OK();
}

class BlockGroupIterator : public ChunkIterator {
public:
    BlockGroupIterator(Schema schema, spill::Serde& serde, const std::vector<spill::BlockPtr>& blocks)
            : ChunkIterator(std::move(schema)), _serde(serde), _blocks(blocks) {
        auto& metrics = _serde.parent()->metrics();
        _options.read_io_timer = metrics.read_io_timer;
        _options.read_io_count = metrics.read_io_count;
        _options.read_io_bytes = metrics.restore_bytes;
    }

    Status do_get_next(Chunk* chunk) override {
        while (_block_idx < _blocks.size()) {
            if (!_reader) {
                _reader = _blocks[_block_idx]->get_reader(_options);
                RETURN_IF_UNLIKELY(!_reader, Status::InternalError("Failed to get reader"));
            }
            auto st = _serde.deserialize(_ctx, _reader.get());
            if (st.ok()) {
                st.value()->swap_chunk(*chunk);
                return Status::OK();
            } else if (st.status().is_end_of_file()) {
                _block_idx++;
                _reader.reset();
            } else {
                return st.status();
            }
        }
        return Status::EndOfFile("End of block group");
    }

    void close() override {}

private:
    spill::Serde& _serde;
    spill::SerdeContext _ctx;
    spill::BlockReaderOptions _options;
    std::shared_ptr<spill::BlockReader> _reader;
    const std::vector<spill::BlockPtr>& _blocks;
    size_t _block_idx = 0;
};

Status SpillMemTableSink::merge_blocks_to_segments() {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_merge_mem_tracker.get(), false);
    auto& groups = _block_manager->block_container()->block_groups();
    RETURN_IF(groups.empty(), Status::OK());

    MonotonicStopWatch timer;
    timer.start();
    // merge process needs to control _writer's flush behavior manually
    _writer->set_auto_flush(false);
    auto char_field_indexes = ChunkHelper::get_char_field_indexes(*_schema);
    size_t total_blocks = 0;
    size_t total_block_bytes = 0;
    size_t total_merges = 0;
    size_t total_rows = 0;
    size_t total_chunk = 0;

    std::vector<ChunkIteratorPtr> merge_inputs;
    size_t current_input_bytes = 0;
    auto merge_func = [&] {
        total_merges++;
        // PK shouldn't do agg because pk support order key different from primary key,
        // in that case, data is sorted by order key and cannot be aggregated by primary key
        bool do_agg = _schema->keys_type() == KeysType::AGG_KEYS || _schema->keys_type() == KeysType::UNIQUE_KEYS;
        auto tmp_itr = new_heap_merge_iterator(merge_inputs);
        auto merge_itr = do_agg ? new_aggregate_iterator(tmp_itr) : tmp_itr;
        RETURN_IF_ERROR(merge_itr->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
        auto chunk_shared_ptr = ChunkHelper::new_chunk(*_schema, config::vector_chunk_size);
        auto chunk = chunk_shared_ptr.get();
        while (true) {
            chunk->reset();
            auto st = merge_itr->get_next(chunk);
            if (st.is_end_of_file()) {
                break;
            } else if (st.ok()) {
                ChunkHelper::padding_char_columns(char_field_indexes, *_schema, _writer->tablet_schema(), chunk);
                total_rows += chunk->num_rows();
                total_chunk++;
                RETURN_IF_ERROR(_writer->write(*chunk, nullptr));
            } else {
                return st;
            }
        }
        merge_itr->close();
        return _writer->flush();
    };
    for (size_t i = 0; i < groups.size(); ++i) {
        auto& group = groups[i];
        // We need to stop merging if:
        // 1. The current input block group size exceed the load_spill_max_merge_bytes,
        //    because we don't want to generate too large segment file.
        // 2. The input chunks memory usage exceed the load_spill_max_merge_bytes,
        //    because we don't want each thread cost too much memory.
        if (merge_inputs.size() > 0 &&
            (current_input_bytes + group.data_size() >= config::load_spill_max_merge_bytes ||
             merge_inputs.size() * config::load_spill_max_chunk_bytes >= config::load_spill_max_merge_bytes)) {
            RETURN_IF_ERROR(merge_func());
            merge_inputs.clear();
            current_input_bytes = 0;
        }
        merge_inputs.push_back(std::make_shared<BlockGroupIterator>(*_schema, *_spiller->serde(), group.blocks()));
        current_input_bytes += group.data_size();
        total_block_bytes += group.data_size();
        total_blocks += group.blocks().size();
    }
    if (merge_inputs.size() > 0) {
        RETURN_IF_ERROR(merge_func());
    }
    timer.stop();
    auto duration_ms = timer.elapsed_time() / 1000000;
    LOG(INFO) << fmt::format(
            "SpillMemTableSink merge finished, txn:{} tablet:{} blockgroups:{} blocks:{} input_bytes:{} merges:{} "
            "rows:{} chunks:{} duration:{}ms",
            _block_manager->txn_id(), _block_manager->tablet_id(), groups.size(), total_blocks, total_block_bytes,
            total_merges, total_rows, total_chunk, duration_ms);
    ADD_COUNTER(_profile, "SpillMergeInputGroups", TUnit::UNIT)->update(groups.size());
    ADD_COUNTER(_profile, "SpillMergeInputBytes", TUnit::BYTES)->update(total_block_bytes);
    ADD_COUNTER(_profile, "SpillMergeCount", TUnit::UNIT)->update(total_merges);
    ADD_COUNTER(_profile, "SpillMergeDurationNs", TUnit::TIME_NS)->update(duration_ms * 1000000);
    return Status::OK();
}

} // namespace starrocks::lake