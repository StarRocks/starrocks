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

#include "compute_env/spill/data_stream.h"

#include "base/utility/alignment.h"
#include "common/status.h"
#include "compute_env/spill/block_group.h"
#include "compute_env/spill/block_manager.h"
#include "compute_env/spill/input_stream.h"
#include "compute_env/spill/restore_task.h"
#include "compute_env/spill/serde.h"
#include "compute_env/spill/spiller.h"
#include "compute_env/spill/task_executor.h"
#include "compute_env/spill/yield.h"
#include "gutil/port.h"
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
    // Envelope coalescing: chunk envelopes are packed tight into _batch and written out in
    // >=kBatchBytes aligned batches. Without this, a narrow column (e.g. bool -> ~4KB per chunk
    // envelope) degrades O_DIRECT spill into hundreds of thousands of latency-bound small writes,
    // and the per-envelope 4KB alignment padding erases most of the compression gain (measured:
    // bool_runs 0.008 -> 0.500 flushed/raw). Batching amortizes both. The O_DIRECT tail padding
    // of a batch is absorbed by growing the LAST envelope's declared attachment size (readers
    // treat the attachment size as an upper bound, so the pad bytes are never parsed).
    static constexpr size_t kBatchBytes = 1 * 1024 * 1024;
    Status _flush_batch();

    // acquire block from block manager
    Status _prepare_block(RuntimeState* state, size_t write_size);
    BlockPtr _cur_block;

    Spiller* _spiller{};

    BlockGroup* _block_group{};
    BlockManager* _block_manager{};

    raw::RawStringPage _batch;            // page-aligned base, required for O_DIRECT writes
    size_t _batch_rows = 0;               // rows covered by the buffered envelopes
    size_t _last_env_offset = 0;          // offset of the newest envelope's header inside _batch
    RuntimeState* _batch_state = nullptr; // state of the pending batch (for _prepare_block)
};

Status BlockSpillOutputDataStream::_prepare_block(RuntimeState* state, size_t write_size) {
    if (_cur_block == nullptr) {
        // flush current block firstly
        RETURN_IF_ERROR(flush());
        // TODO: add profile for acquire block
        spill::AcquireBlockOptions opts;
        opts.query_id = state->query_id();
        opts.fragment_instance_id = state->fragment_instance_id();
        opts.plan_node_id = _spiller->options().plan_node_id;
        opts.name = _spiller->options().name;
        opts.block_size = write_size;
        // BUGFIX: direct_io was never propagated, so spill_enable_direct_io only aligned
        // payloads while files stayed buffered -- O_DIRECT never actually engaged.
        opts.direct_io = state->spill_enable_direct_io();
        opts.affinity_group = _block_group->get_affinity_group();
        ASSIGN_OR_RETURN(auto block, _block_manager->acquire_block(opts));
        // update metrics
        bool is_remote = block->is_remote();
        auto block_count = GET_METRICS(is_remote, _spiller->metrics(), block_count);
        COUNTER_UPDATE(block_count, 1);
        if (auto* g = _spiller->metrics().global(is_remote); g != nullptr) {
            g->blocks_write_total->increment(1);
            if (!_spiller->global_spill_triggered().exchange(true)) {
                g->trigger_total->increment(1);
            }
        }
        TRACE_SPILL_LOG << fmt::format("allocate block [{}], affinity group[{}]", block->debug_string(),
                                       opts.affinity_group);
        _cur_block = std::move(block);
        _block_group->append(_cur_block);
    }

    return Status::OK();
}

Status BlockSpillOutputDataStream::append(RuntimeState* state, const std::vector<Slice>& data, size_t total_write_size,
                                          size_t write_num_rows) {
    // Coalesce: pack this envelope tight into the batch; real IO happens in _flush_batch().
    _batch_state = state;
    _last_env_offset = _batch.size();
    for (const auto& slice : data) {
        _batch.append(slice.data, slice.size);
    }
    _batch_rows += write_num_rows;
    if (_batch.size() >= kBatchBytes) {
        RETURN_IF_ERROR(_flush_batch());
    }
    return Status::OK();
}

Status BlockSpillOutputDataStream::_flush_batch() {
    if (_batch.empty()) {
        return Status::OK();
    }
    // Detach the batch FIRST: _prepare_block() below re-enters flush() -> _flush_batch() when it
    // opens a fresh block, which must observe an empty batch (otherwise infinite recursion).
    raw::RawStringPage batch;
    batch.swap(_batch);
    // The swap left _batch with no capacity; the next batch would grow back to kBatchBytes through
    // a chain of geometric reallocations. Re-reserve once instead.
    _batch.reserve(kBatchBytes);
    const size_t write_num_rows = _batch_rows;
    const size_t last_env_offset = _last_env_offset;
    _batch_rows = 0;
    _last_env_offset = 0;
    RuntimeState* state = _batch_state;
    // O_DIRECT writes need a device-block-aligned length: pad the batch tail and grow the last
    // envelope's declared attachment size so a reader consumes the pad as (unparsed) envelope tail.
    if (state->spill_enable_direct_io()) {
        const size_t aligned = ALIGN_UP(batch.size(), AlignedBuffer::kPageSize);
        if (const size_t pad = aligned - batch.size(); pad > 0) {
            auto* env = reinterpret_cast<uint8_t*>(batch.data()) + last_env_offset;
            const int64_t attach = UNALIGNED_LOAD64(env + serde_proto::ATTACHMENT_SIZE_OFFSET);
            UNALIGNED_STORE64(env + serde_proto::ATTACHMENT_SIZE_OFFSET, attach + static_cast<int64_t>(pad));
            batch.resize(aligned, '\0');
        }
    }
    const size_t total_write_size = batch.size();

    // acquire block if current block is nullptr or full
    RETURN_IF_ERROR(_prepare_block(state, total_write_size));
    _append_rows += write_num_rows;
    bool is_remote = _cur_block->is_remote();
    int64_t io_ns = 0;
    Status append_st;
    {
        SCOPED_RAW_TIMER(&io_ns);
        TRACE_SPILL_LOG << fmt::format("append block[{}], size[{}]", _cur_block->debug_string(), total_write_size);
        append_st = _cur_block->append({Slice(batch.data(), total_write_size)});
    }
    COUNTER_UPDATE(GET_METRICS(is_remote, _spiller->metrics(), write_io_timer), io_ns);
    if (auto* g = _spiller->metrics().global(is_remote); g != nullptr) {
        g->write_io_duration_ns_total->increment(io_ns);
    }
    RETURN_IF_ERROR(append_st);
    _cur_block->inc_num_rows(write_num_rows);
    COUNTER_UPDATE(GET_METRICS(is_remote, _spiller->metrics(), flush_bytes), total_write_size);
    (*_spiller->metrics().total_spill_bytes) += total_write_size;
    if (auto* g = _spiller->metrics().global(is_remote); g != nullptr) {
        g->bytes_write_total->increment(total_write_size);
    }
    return Status::OK();
}

Status BlockSpillOutputDataStream::flush() {
    // drain any partially-filled envelope batch before flushing/releasing the block
    RETURN_IF_ERROR(_flush_batch());
    if (_cur_block == nullptr) {
        return Status::OK();
    }
    bool is_remote = _cur_block->is_remote();
    int64_t io_ns = 0;
    Status flush_st;
    {
        SCOPED_RAW_TIMER(&io_ns);
        flush_st = _cur_block->flush();
        TRACE_SPILL_LOG << fmt::format("flush block[{}]", _cur_block->debug_string());
    }
    COUNTER_UPDATE(GET_METRICS(is_remote, _spiller->metrics(), write_io_timer), io_ns);
    if (auto* g = _spiller->metrics().global(is_remote); g != nullptr) {
        g->write_io_duration_ns_total->increment(io_ns);
    }
    RETURN_IF_ERROR(flush_st);

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
    // envelopes are packed tight; the batching output stream owns O_DIRECT alignment now
    bool need_aligned = false;
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
