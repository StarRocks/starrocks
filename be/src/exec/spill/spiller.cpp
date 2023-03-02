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

#include "exec/spill/spiller.h"

#include <butil/iobuf.h>
#include <fmt/core.h>
#include <glog/logging.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>

#include "column/chunk.h"
#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/sort_exec_exprs.h"
#include "exec/spill/mem_table.h"
#include "exec/spill/spilled_stream.h"
#include "exec/spill/spiller_path_provider.h"
#include "gutil/port.h"
#include "runtime/runtime_state.h"
#include "serde/column_array_serde.h"
#include "exec/pipeline/query_context.h"

namespace starrocks {
SpillProcessMetrics::SpillProcessMetrics(RuntimeProfile* profile) {
    spill_timer = ADD_TIMER(profile, "SpillTime");
    spill_rows = ADD_COUNTER(profile, "SpilledRows", TUnit::UNIT);
    flush_timer = ADD_TIMER(profile, "SpillFlushTimer");
    write_io_timer = ADD_TIMER(profile, "SpillWriteIOTimer");
    restore_rows = ADD_COUNTER(profile, "SpillRestoreRows", TUnit::UNIT);
    restore_timer = ADD_TIMER(profile, "SpillRestoreTimer");
}
// Not thread safe
class ColumnSpillFormater : public SpillFormater {
public:
    ColumnSpillFormater(ChunkBuilder chunk_builder) : _chunk_builder(std::move(chunk_builder)) {}
    Status spill_as_fmt(SpillFormatContext& context, std::unique_ptr<WritableFile>& writable,
                        const ChunkPtr& chunk) const noexcept override;
    StatusOr<ChunkUniquePtr> restore_from_fmt(SpillFormatContext& context,
                                              std::unique_ptr<RawInputStreamWrapper>& readable) const override;
    Status flush(std::unique_ptr<WritableFile>& writable) const override;

private:
    size_t _spill_size(const ChunkPtr& chunk) const;
    ChunkBuilder _chunk_builder;
};

size_t ColumnSpillFormater::_spill_size(const ChunkPtr& chunk) const {
    size_t serialize_sz = 0;
    for (const auto& column : chunk->columns()) {
        serialize_sz += serde::ColumnArraySerde::max_serialized_size(*column);
    }
    return serialize_sz + sizeof(serialize_sz);
}

Status ColumnSpillFormater::spill_as_fmt(SpillFormatContext& context, std::unique_ptr<WritableFile>& writable,
                                         const ChunkPtr& chunk) const noexcept {
    size_t serialize_sz = _spill_size(chunk);
    context.io_buffer.resize(serialize_sz);
    DCHECK_GT(serialize_sz, 4);

    auto* buff = reinterpret_cast<uint8_t*>(context.io_buffer.data());
    UNALIGNED_STORE64(buff, serialize_sz);
    buff += sizeof(serialize_sz);

    for (const auto& column : chunk->columns()) {
        buff = serde::ColumnArraySerde::serialize(*column, buff);
    }

    RETURN_IF_ERROR(writable->append(context.io_buffer));
    return Status::OK();
}

StatusOr<ChunkUniquePtr> ColumnSpillFormater::restore_from_fmt(SpillFormatContext& context,
                                                               std::unique_ptr<RawInputStreamWrapper>& readable) const {
    size_t serialize_sz;
    RETURN_IF_ERROR(readable->read_fully(&serialize_sz, sizeof(serialize_sz)));
    DCHECK_GT(serialize_sz, sizeof(serialize_sz));
    context.io_buffer.resize(serialize_sz);
    auto buff = reinterpret_cast<uint8_t*>(context.io_buffer.data());
    RETURN_IF_ERROR(readable->read_fully(buff, serialize_sz - sizeof(serialize_sz)));

    auto chunk = _chunk_builder();
    const uint8_t* read_cursor = buff;
    for (const auto& column : chunk->columns()) {
        read_cursor = serde::ColumnArraySerde::deserialize(read_cursor, column.get());
    }
    return chunk;
}

Status ColumnSpillFormater::flush(std::unique_ptr<WritableFile>& writable) const {
    if (!config::experimental_spill_skip_sync) {
        RETURN_IF_ERROR(writable->flush(WritableFile::FLUSH_ASYNC));
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<SpillFormater>> SpillFormater::create(SpillFormaterType type, ChunkBuilder chunk_builder) {
    if (type == SpillFormaterType::SPILL_BY_COLUMN) {
        return std::make_unique<ColumnSpillFormater>(std::move(chunk_builder));
    } else {
        return Status::InternalError(fmt::format("unsupported spill type:{}", type));
    }
}

Status Spiller::prepare(RuntimeState* state) {
    // prepare
    ASSIGN_OR_RETURN(_spill_fmt, SpillFormater::create(_opts.spill_type, _opts.chunk_builder));

    for (size_t i = 0; i < _opts.mem_table_pool_size; ++i) {
        if (_opts.is_unordered) {
            _mem_table_pool.push(
                    std::make_unique<UnorderedMemTable>(state, _opts.spill_file_size, state->instance_mem_tracker()));
        } else {
            _mem_table_pool.push(std::make_unique<OrderedMemTable>(&_opts.sort_exprs->lhs_ordering_expr_ctxs(),
                                                                   _opts.sort_desc, state, _opts.spill_file_size,
                                                                   state->instance_mem_tracker()));
        }
    }

    _file_group = std::make_shared<SpilledFileGroup>(*_spill_fmt);
    ASSIGN_OR_RETURN(_formatter, spill::create_formatter(&_opts));
    _block_group = std::make_shared<spill::BlockGroup>(_formatter.get());
    _block_manager = state->query_ctx()->spill_block_manager();

    return Status::OK();
}

Status Spiller::_open(RuntimeState* state) {
    std::lock_guard guard(_mutex);
    if (_has_opened) {
        return Status::OK();
    }

    // init path provider
    // ASSIGN_OR_RETURN(_path_provider, _opts.path_provider_factory());
    // RETURN_IF_ERROR(_path_provider->open(state));
    _has_opened = true;

    return Status::OK();
}

Status Spiller::_flush_and_closed(std::unique_ptr<WritableFile>& writable) {
    // flush
    RETURN_IF_ERROR(_spill_fmt->flush(writable));
    // be careful close method return a status
    RETURN_IF_ERROR(writable->close());
    writable.reset();
    return Status::OK();
}

Status Spiller::_run_flush_task(RuntimeState* state, const MemTablePtr& mem_table) {
    if (state->is_cancelled()) {
        LOG(INFO) << "query is cancelled, just return";
        return Status::OK();
    }
    // LOG(INFO) << "invoke flush_mem_table";
    // flush mem table
    // acuire block
    spill::AcquireBlockOptions opts;
    opts.query_id = state->query_id();
    opts.plan_node_id = _opts.plan_node_id;
    opts.name = _opts.name;
    // @TODO plan node id, name
    ASSIGN_OR_RETURN(auto block, _block_manager->acquire_block(opts));
    // LOG(INFO) << "get block";
    spill::FormatterContext ctx;

    size_t num_rows_flushed = 0;
    // @TODO remove callback
    RETURN_IF_ERROR(mem_table->flush([&] (const auto& chunk) {
        num_rows_flushed += chunk->num_rows();
        // LOG(INFO) << "serialize chunk";
        RETURN_IF_ERROR(_formatter->serialize(ctx, chunk, block));
        return Status::OK();
    }));
    // LOG(INFO) << "flush block begin";
    RETURN_IF_ERROR(block->flush());
    _block_manager->release_block(block);
    // LOG(INFO) << "flush block end";
    std::lock_guard<std::mutex> l(_mutex);
    _block_group->append(block);
    LOG(INFO) << "append block: " << block->debug_string();
    return Status::OK();
}

void Spiller::_update_spilled_task_status(Status&& st) {
    std::lock_guard guard(_mutex);
    if (_spilled_task_status.ok() && !st.ok()) {
        _spilled_task_status = std::move(st);
    }
}

StatusOr<std::shared_ptr<spill::InputStream>> Spiller::_acquire_input_stream(RuntimeState* state) {
    if (_opts.is_unordered) {
        return _block_group->as_unordered_stream();
    }
    return _block_group->as_ordered_stream(state, _opts.sort_exprs, _opts.sort_desc);
}

Status Spiller::_decrease_running_flush_tasks() {
    if (_running_flush_tasks.fetch_sub(1) == 1) {
        if (_flush_all_callback) {
            RETURN_IF_ERROR(_flush_all_callback());
            if (_inner_flush_all_callback) {
                RETURN_IF_ERROR(_inner_flush_all_callback());
            }
        }
    }
    return Status::OK();
}

} // namespace starrocks