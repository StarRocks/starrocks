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

#include "storage/load_chunk_spiller.h"

#include "exec/spill/options.h"
#include "exec/spill/serde.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller_factory.h"
#include "runtime/runtime_state.h"
#include "storage/aggregate_iterator.h"
#include "storage/chunk_helper.h"
#include "storage/load_spill_block_manager.h"
#include "storage/merge_iterator.h"
#include "storage/union_iterator.h"

namespace starrocks {

Status LoadSpillOutputDataStream::append(RuntimeState* state, const std::vector<Slice>& data, size_t total_write_size,
                                         size_t write_num_rows) {
    _append_rows += write_num_rows;
    size_t total_size = 0;
    // calculate total size
    std::for_each(data.begin(), data.end(), [&](const Slice& slice) { total_size += slice.size; });
    // preallocate block
    RETURN_IF_ERROR(_preallocate(total_size));
    // append data
    auto st = _block->append(data);
    if (st.is_capacity_limit_exceeded()) {
        // No space left on device
        // Try to acquire a new block from remote storage.
        RETURN_IF_ERROR(_switch_to_remote_block(total_size));
        st = _block->append(data);
    }
    if (st.ok()) {
        _append_bytes += total_size;
    }
    return st;
}

Status LoadSpillOutputDataStream::flush() {
    RETURN_IF_ERROR(_freeze_current_block());
    return Status::OK();
}

bool LoadSpillOutputDataStream::is_remote() const {
    return _block ? _block->is_remote() : false;
}

// this function will be called when local disk is full
Status LoadSpillOutputDataStream::_switch_to_remote_block(size_t block_size) {
    if (_block->size() > 0) {
        // Freeze current block firstly.
        RETURN_IF_ERROR(_freeze_current_block());
    } else {
        // Release empty block.
        RETURN_IF_ERROR(_block_manager->release_block(_block));
        _block = nullptr;
    }
    // Acquire new block.
    ASSIGN_OR_RETURN(_block, _block_manager->acquire_block(block_size, true /* force remote */));
    return Status::OK();
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
    if (_block == nullptr || !_block->try_acquire_sizes(block_size)) {
        // Freeze current block firstly.
        RETURN_IF_ERROR(_freeze_current_block());
        // Acquire new block.
        ASSIGN_OR_RETURN(_block, _block_manager->acquire_block(block_size));
    }
    return Status::OK();
}

LoadChunkSpiller::LoadChunkSpiller(LoadSpillBlockManager* block_manager, RuntimeProfile* profile)
        : _block_manager(block_manager), _profile(profile) {
    if (_profile == nullptr) {
        // use dummy profile
        _dummy_profile = std::make_unique<RuntimeProfile>("dummy");
        _profile = _dummy_profile.get();
    }
    _runtime_state = std::make_shared<RuntimeState>();
    _spiller_factory = spill::make_spilled_factory();
}

StatusOr<size_t> LoadChunkSpiller::spill(const Chunk& chunk) {
    if (chunk.num_rows() == 0) return 0;
    // 1. create new block group
    _block_manager->block_container()->create_block_group();
    auto output = std::make_shared<LoadSpillOutputDataStream>(_block_manager);
    // 2. spill
    RETURN_IF_ERROR(_do_spill(chunk, output));
    // 3. flush
    RETURN_IF_ERROR(output->flush());
    return output->append_bytes();
}

Status LoadChunkSpiller::_prepare(const ChunkPtr& chunk_ptr) {
    if (_spiller == nullptr) {
        // 1. alloc & prepare spiller
        spill::SpilledOptions options;
        options.encode_level = 7;
        options.wg = ExecEnv::GetInstance()->workgroup_manager()->get_default_workgroup();
        _spiller = _spiller_factory->create(options);
        RETURN_IF_ERROR(_spiller->prepare(_runtime_state.get()));
        DCHECK(_profile != nullptr) << "LoadChunkSpiller profile is null";
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

Status LoadChunkSpiller::_do_spill(const Chunk& chunk, const spill::SpillOutputDataStreamPtr& output) {
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

size_t LoadChunkSpiller::total_bytes() const {
    return _block_manager ? _block_manager->total_bytes() : 0;
}

Status LoadChunkSpiller::merge_write(size_t target_size, bool do_sort, bool do_agg,
                                     std::function<Status(Chunk*)> write_func, std::function<Status()> flush_func) {
    auto& groups = _block_manager->block_container()->block_groups();
    RETURN_IF(groups.empty(), Status::OK());

    MonotonicStopWatch timer;
    timer.start();
    size_t total_blocks = 0;
    size_t total_block_bytes = 0;
    size_t total_merges = 0;
    size_t total_rows = 0;
    size_t total_chunk = 0;

    std::vector<ChunkIteratorPtr> merge_inputs;
    size_t current_input_bytes = 0;
    auto merge_func = [&] {
        total_merges++;
        auto tmp_itr = do_sort ? new_heap_merge_iterator(merge_inputs) : new_union_iterator(merge_inputs);
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
                total_rows += chunk->num_rows();
                total_chunk++;
                RETURN_IF_ERROR(write_func(chunk));
            } else {
                return st;
            }
        }
        merge_itr->close();
        return flush_func();
    };
    for (size_t i = 0; i < groups.size(); ++i) {
        auto& group = groups[i];
        // We need to stop merging if:
        // 1. The current input block group size exceed the target_size,
        //    because we don't want to generate too large segment file.
        // 2. The input chunks memory usage exceed the load_spill_max_merge_bytes,
        //    because we don't want each thread cost too much memory.
        if (merge_inputs.size() > 0 && (current_input_bytes + group.data_size() >= target_size ||
                                        merge_inputs.size() * config::load_spill_max_chunk_bytes >= target_size)) {
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
            "LoadChunkSpiller merge finished, load_id:{} fragment_instance_id:{} blockgroups:{} blocks:{} "
            "input_bytes:{} merges:{} rows:{} chunks:{} duration:{}ms",
            (std::ostringstream() << _block_manager->load_id()).str(),
            (std::ostringstream() << _block_manager->fragment_instance_id()).str(), groups.size(), total_blocks,
            total_block_bytes, total_merges, total_rows, total_chunk, duration_ms);
    COUNTER_UPDATE(ADD_COUNTER(_profile, "SpillMergeInputGroups", TUnit::UNIT), groups.size());
    COUNTER_UPDATE(ADD_COUNTER(_profile, "SpillMergeInputBytes", TUnit::BYTES), total_block_bytes);
    COUNTER_UPDATE(ADD_COUNTER(_profile, "SpillMergeCount", TUnit::UNIT), total_merges);
    COUNTER_UPDATE(ADD_COUNTER(_profile, "SpillMergeDurationNs", TUnit::TIME_NS), duration_ms * 1000000);
    return Status::OK();
}

bool LoadChunkSpiller::empty() {
    return _block_manager->block_container()->empty();
}

} // namespace starrocks
