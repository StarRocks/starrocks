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

#include "exec/spill/data_stream.h"

#include "common/status.h"
#include "exec/spill/block_manager.h"
#include "exec/spill/executor.h"
#include "exec/spill/input_stream.h"
#include "exec/spill/serde.h"
#include "exec/spill/spiller.h"
#include "runtime/runtime_state.h"

namespace starrocks::spill {
// spill output stream. output serialized chunk data to BlockManager and add handle to block group.
class BlockSpillOutputDataStream final : public SpillOutputDataStream {
public:
    BlockSpillOutputDataStream(Spiller* spiller, BlockGroup* block_group, BlockManager* block_manager)
            : _spiller(spiller), _block_group(block_group), _block_manager(block_manager) {}
    ~BlockSpillOutputDataStream() override = default;

    Status append(RuntimeState* state, const std::vector<Slice>& data, size_t total_write_size,
                  size_t write_num_rows) override;
    Status flush() override;

    bool is_remote() const override {
        if (_cur_block != nullptr) {
            return _cur_block->is_remote();
        }
        return false;
    }

private:
    // acquire block from block manager
    Status _prepare_block(RuntimeState* state, size_t write_size);
    BlockPtr _cur_block;

    Spiller* _spiller{};

    BlockGroup* _block_group{};
    BlockManager* _block_manager{};
};

Status BlockSpillOutputDataStream::_prepare_block(RuntimeState* state, size_t write_size) {
    if (_cur_block == nullptr || !_cur_block->preallocate(write_size)) {
        // flush current block firstly
        RETURN_IF_ERROR(flush());
        // TODO: add profile for acquire block
        spill::AcquireBlockOptions opts;
        opts.query_id = state->query_id();
        opts.fragment_instance_id = state->fragment_instance_id();
        opts.plan_node_id = _spiller->options().plan_node_id;
        opts.name = _spiller->options().name;
        opts.block_size = write_size;
        opts.exclusive = _spiller->options().init_partition_nums > 0 || !_spiller->options().is_unordered;
        ASSIGN_OR_RETURN(auto block, _block_manager->acquire_block(opts));
        // update metrics
        auto block_count = GET_METRICS(block->is_remote(), _spiller->metrics(), block_count);
        COUNTER_UPDATE(block_count, 1);
        TRACE_SPILL_LOG << fmt::format("allocate block [{}]", block->debug_string());
        _cur_block = std::move(block);
        _block_group->append(_cur_block);
    }

    return Status::OK();
}

Status BlockSpillOutputDataStream::append(RuntimeState* state, const std::vector<Slice>& data, size_t total_write_size,
                                          size_t write_num_rows) {
    // acquire block if current block is nullptr or full
    RETURN_IF_ERROR(_prepare_block(state, total_write_size));
    _append_rows += write_num_rows;
    {
        auto write_io_timer = GET_METRICS(_cur_block->is_remote(), _spiller->metrics(), write_io_timer);
        SCOPED_TIMER(write_io_timer);
        TRACE_SPILL_LOG << fmt::format("append block[{}], size[{}]", _cur_block->debug_string(), total_write_size);
        RETURN_IF_ERROR(_cur_block->append(data));
        _cur_block->inc_num_rows(write_num_rows);
        auto flush_bytes = GET_METRICS(_cur_block->is_remote(), _spiller->metrics(), flush_bytes);
        COUNTER_UPDATE(flush_bytes, total_write_size);
        (*_spiller->metrics().total_spill_bytes) += total_write_size;
    }
    return Status::OK();
}

Status BlockSpillOutputDataStream::flush() {
    if (_cur_block == nullptr) {
        return Status::OK();
    }
    {
        auto write_io_timer = GET_METRICS(_cur_block->is_remote(), _spiller->metrics(), write_io_timer);
        SCOPED_TIMER(write_io_timer);
        RETURN_IF_ERROR(_cur_block->flush());
        TRACE_SPILL_LOG << fmt::format("flush block[{}]", _cur_block->debug_string());
    }

    // release block if not exclusive
    RETURN_IF_ERROR(_block_manager->release_block(std::move(_cur_block)));
    DCHECK(_cur_block == nullptr);

    return Status::OK();
}

std::shared_ptr<SpillOutputDataStream> create_spill_output_stream(Spiller* spiller, BlockGroup* block_group,
                                                                  BlockManager* block_manager) {
    return std::make_shared<BlockSpillOutputDataStream>(spiller, block_group, block_manager);
}

Status DataTranster::transfer(workgroup::YieldContext& yield_ctx, RuntimeState* state, Serde* serde,
                              const SpillOutputDataStreamPtr& output, const InputStreamPtr& input_stream) {
    // read data from input stream and append to output stream
    bool need_aligned = state->spill_enable_direct_io();
    auto task_context = std::any_cast<SpillIOTaskContextPtr>(yield_ctx.task_context_data);
    SerdeContext read_ctx;
    while (true) {
        SCOPED_RAW_TIMER(&yield_ctx.time_spent_ns);
        if (!input_stream->is_ready()) {
            workgroup::YieldContext restore_yield_ctx;
            auto restore_task_context = std::make_shared<SpillIOTaskContext>();
            restore_task_context->use_local_io_executor = task_context->use_local_io_executor;
            restore_yield_ctx.task_context_data = restore_task_context;
            YieldableRestoreTask task(input_stream);
            auto st = task.do_read(restore_yield_ctx, read_ctx);
            RETURN_IF(!st.is_ok_or_eof(), st);
            yield_ctx.need_yield = restore_yield_ctx.need_yield;
            task_context->use_local_io_executor = restore_task_context->use_local_io_executor;
            RETURN_IF_YIELD(yield_ctx.need_yield);
        }
        DCHECK(input_stream->is_ready());
        auto chunk_st = input_stream->get_next(yield_ctx, read_ctx);
        RETURN_IF(!chunk_st.status().is_ok_or_eof(), chunk_st.status());
        RETURN_IF(chunk_st.status().is_end_of_file(), Status::OK());
        RETURN_IF_ERROR(serde->serialize(state, read_ctx, std::move(chunk_st.value()), output, need_aligned));
        RETURN_OK_IF_NEED_YIELD(yield_ctx.wg, &yield_ctx.need_yield, yield_ctx.time_spent_ns);
    }
    return Status::OK();
}

} // namespace starrocks::spill