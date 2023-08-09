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

#include <atomic>
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
#include "exec/spill/input_stream.h"
#include "exec/spill/mem_table.h"
#include "exec/spill/options.h"
#include "exec/spill/spiller.hpp"
#include "gutil/port.h"
#include "runtime/runtime_state.h"
#include "serde/column_array_serde.h"

namespace starrocks::spill {
SpillProcessMetrics::SpillProcessMetrics(RuntimeProfile* profile, std::atomic_int64_t* total_spill_bytes_) {
    total_spill_bytes = total_spill_bytes_;
    _spiller_metrics = std::make_shared<RuntimeProfile>("SpillerMetrics");
    profile->add_child(_spiller_metrics.get(), true, nullptr);

    append_data_timer = ADD_TIMER(_spiller_metrics.get(), "AppendDataTime");
    spill_rows = ADD_COUNTER(_spiller_metrics.get(), "RowsSpilled", TUnit::UNIT);
    flush_timer = ADD_TIMER(_spiller_metrics.get(), "FlushTime");
    write_io_timer = ADD_TIMER(_spiller_metrics.get(), "WriteIOTime");
    restore_rows = ADD_COUNTER(_spiller_metrics.get(), "RowsRestored", TUnit::UNIT);
    restore_from_buffer_timer = ADD_TIMER(_spiller_metrics.get(), "RestoreTime");
    read_io_timer = ADD_TIMER(_spiller_metrics.get(), "ReadIOTime");
    flush_bytes = ADD_COUNTER(_spiller_metrics.get(), "BytesFlushToDisk", TUnit::BYTES);
    restore_bytes = ADD_COUNTER(_spiller_metrics.get(), "BytesRestoreFromDisk", TUnit::BYTES);
    serialize_timer = ADD_TIMER(_spiller_metrics.get(), "SerializeTime");
    deserialize_timer = ADD_TIMER(_spiller_metrics.get(), "DeserializeTime");
    mem_table_peak_memory_usage = _spiller_metrics->AddHighWaterMarkCounter(
            "MemTablePeakMemoryBytes", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
    input_stream_peak_memory_usage = _spiller_metrics->AddHighWaterMarkCounter(
            "InputStreamPeakMemoryBytes", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));

    shuffle_timer = ADD_TIMER(_spiller_metrics.get(), "ShuffleTime");
    split_partition_timer = ADD_TIMER(_spiller_metrics.get(), "SplitPartitionTime");
    restore_from_mem_table_rows = ADD_COUNTER(_spiller_metrics.get(), "RowsRestoreFromMemTable", TUnit::UNIT);
    restore_from_mem_table_bytes = ADD_COUNTER(_spiller_metrics.get(), "BytesRestoreFromMemTable", TUnit::UNIT);
    partition_writer_peak_memory_usage = _spiller_metrics->AddHighWaterMarkCounter(
            "PartitionWriterPeakMemoryBytes", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
}

Status Spiller::prepare(RuntimeState* state) {
    _chunk_builder.chunk_schema() = std::make_shared<SpilledChunkBuildSchema>();

    ASSIGN_OR_RETURN(_serde, Serde::create_serde(this));

    if (_opts.init_partition_nums > 0) {
        _writer = std::make_unique<PartitionedSpillerWriter>(this, state);
    } else {
        _writer = std::make_unique<RawSpillerWriter>(this, state);
    }

    RETURN_IF_ERROR(_writer->prepare(state));

    _reader = std::make_unique<SpillerReader>(this);

    if (!_opts.is_unordered) {
        DCHECK(_opts.init_partition_nums == -1);
    }

    _block_group = std::make_shared<spill::BlockGroup>();
    _block_manager = _opts.block_manager;

    return Status::OK();
}

Status Spiller::set_partition(const std::vector<const SpillPartitionInfo*>& parititons) {
    DCHECK_GT(_opts.init_partition_nums, 0);
    RETURN_IF_ERROR(down_cast<PartitionedSpillerWriter*>(_writer.get())->reset_partition(parititons));
    return Status::OK();
}

Status Spiller::set_partition(RuntimeState* state, size_t num_partitions) {
    RETURN_IF_ERROR(down_cast<PartitionedSpillerWriter*>(_writer.get())->reset_partition(state, num_partitions));
    return Status::OK();
}

void Spiller::update_spilled_task_status(Status&& st) {
    std::lock_guard guard(_mutex);
    if (_spilled_task_status.ok() && !st.ok()) {
        _spilled_task_status = std::move(st);
    }
}

std::vector<std::shared_ptr<SpillerReader> > Spiller::get_partition_spill_readers(
        const std::vector<const SpillPartitionInfo*>& partitions) {
    std::vector<std::shared_ptr<SpillerReader> > res;

    for (auto partition : partitions) {
        res.emplace_back(std::make_unique<SpillerReader>(this));
        std::shared_ptr<SpillInputStream> stream;
        // TODO check return status
        CHECK(_writer->acquire_stream(partition, &stream).ok());
        res.back()->set_stream(std::move(stream));
    }

    return res;
}

Status Spiller::_acquire_input_stream(RuntimeState* state) {
    std::shared_ptr<SpillInputStream> input_stream;

    RETURN_IF_ERROR(_writer->acquire_stream(&input_stream));
    RETURN_IF_ERROR(_reader->set_stream(std::move(input_stream)));

    return Status::OK();
}
} // namespace starrocks::spill