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
#include "storage/lake/load_spill_block_manager.h"
#include "storage/lake/tablet_writer.h"

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

SpillMemTableSink::SpillMemTableSink(LoadSpillBlockManager* block_manager, TabletWriter* w) {
    _block_manager = block_manager;
    _writer = w;
    _runtime_state = std::make_shared<RuntimeState>();
    _spiller_factory = spill::make_spilled_factory();
}

Status SpillMemTableSink::_prepare(const ChunkPtr& chunk_ptr) {
    if (_spiller == nullptr) {
        // 1. alloc & prepare spiller
        spill::SpilledOptions options;
        options.encode_level = 7;
        _spiller = _spiller_factory->create(options);
        RETURN_IF_ERROR(_spiller->prepare(_runtime_state.get()));
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
    const int64_t spill_rows = config::load_spill_max_chunk_bytes / (per_row_memory_usage + 1) + 1;
    // 2. serialize chunk
    for (int64_t rowid = 0; rowid < chunk.num_rows(); rowid += spill_rows) {
        int64_t rows = std::min(spill_rows, (int64_t)chunk.num_rows() - rowid);
        ChunkPtr each_chunk = chunk.clone_empty();
        each_chunk->append(chunk, rowid, rows);
        RETURN_IF_ERROR(_prepare(each_chunk));
        spill::SerdeContext ctx;
        RETURN_IF_ERROR(_spiller->serde()->serialize(_runtime_state.get(), ctx, each_chunk, output, true));
    }
    return Status::OK();
}

Status SpillMemTableSink::flush_chunk(const Chunk& chunk, starrocks::SegmentPB* segment, bool eos) {
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
    return Status::OK();
}

Status SpillMemTableSink::flush_chunk_with_deletes(const Chunk& upserts, const Column& deletes,
                                                   starrocks::SegmentPB* segment, bool eos) {
    if (eos && _block_manager->block_container()->empty()) {
        // If there is only one flush, flush it to segment directly
        RETURN_IF_ERROR(_writer->flush_del_file(deletes));
        RETURN_IF_ERROR(_writer->write(upserts, segment));
        return _writer->flush(segment);
    }
    // 1. flush upsert
    RETURN_IF_ERROR(flush_chunk(upserts, segment, eos));
    // 2. flush deletes
    RETURN_IF_ERROR(_writer->flush_del_file(deletes));
    return Status::OK();
}

Status SpillMemTableSink::merge_blocks_to_segments() {
    // TODO
    // 1. get blocks from `_block_manager`(LoadSpillBlockManager)
    // 2. merge blocks to segments
    // 3. add these segments to `_writer`(TabletWriter)
    return Status::OK();
}

} // namespace starrocks::lake