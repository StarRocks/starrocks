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

    return Status::OK();
}

Status Spiller::_open(RuntimeState* state) {
    std::lock_guard guard(_mutex);
    if (_has_opened) {
        return Status::OK();
    }

    // init path provider
    ASSIGN_OR_RETURN(_path_provider, _opts.path_provider_factory());
    RETURN_IF_ERROR(_path_provider->open(state));
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
    RETURN_IF_ERROR(this->_open(state));
    // prepare current file
    ASSIGN_OR_RETURN(auto file, _path_provider->get_file());
    ASSIGN_OR_RETURN(auto writable, file->as<WritableFile>());
    // TODO: reuse io context
    SpillFormatContext spill_ctx;
    {
        std::lock_guard guard(_mutex);
        _file_group->append_file(std::move(file));
    }
    DCHECK(writable != nullptr);
    {
        SCOPED_TIMER(_metrics.write_io_timer);
        // flush all pending result to spilled files
        size_t num_rows_flushed = 0;
        RETURN_IF_ERROR(mem_table->flush([&](const auto& chunk) {
            num_rows_flushed += chunk->num_rows();
            RETURN_IF_ERROR(_spill_fmt->spill_as_fmt(spill_ctx, writable, chunk));
            return Status::OK();
        }));
        TRACE_SPILL_LOG << "spill flush rows:" << num_rows_flushed << ",spiller:" << this;
    }
    // then release the pending memory
    RETURN_IF_ERROR(_flush_and_closed(writable));
    return Status::OK();
}

void Spiller::_update_spilled_task_status(Status&& st) {
    std::lock_guard guard(_mutex);
    if (_spilled_task_status.ok() && !st.ok()) {
        _spilled_task_status = std::move(st);
    }
}

StatusOr<std::shared_ptr<SpilledInputStream>> Spiller::_acquire_input_stream(RuntimeState* state) {
    DCHECK(_restore_tasks.empty());
    std::vector<SpillRestoreTaskPtr> restore_tasks;
    std::shared_ptr<SpilledInputStream> input_stream;
    if (_opts.is_unordered) {
        ASSIGN_OR_RETURN(auto res, _file_group->as_flat_stream(_parent));
        auto& [stream, tasks] = res;
        input_stream = std::move(stream);
        restore_tasks = std::move(tasks);
    } else {
        ASSIGN_OR_RETURN(auto res, _file_group->as_sorted_stream(_parent, state, _opts.sort_exprs, _opts.sort_desc));
        auto& [stream, tasks] = res;
        input_stream = std::move(stream);
        restore_tasks = std::move(tasks);
    }

    std::lock_guard guard(_mutex);
    {
        // put all restore_tasks to pending lists
        for (auto& task : restore_tasks) {
            _restore_tasks.push(task);
        }
        _total_restore_tasks = _restore_tasks.size();
    }
    return input_stream;
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