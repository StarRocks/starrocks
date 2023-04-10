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
#include "exec/spill/input_stream.h"
#include "exec/spill/mem_table.h"
#include "exec/spill/options.h"
#include "exec/spill/spiller.hpp"
#include "gutil/port.h"
#include "runtime/runtime_state.h"
#include "serde/column_array_serde.h"

namespace starrocks::spill {
SpillProcessMetrics::SpillProcessMetrics(RuntimeProfile* profile) {
    spill_timer = ADD_TIMER(profile, "SpillTime");
    spill_rows = ADD_COUNTER(profile, "SpilledRows", TUnit::UNIT);
    flush_timer = ADD_TIMER(profile, "SpillFlushTimer");
    write_io_timer = ADD_TIMER(profile, "SpillWriteIOTimer");
    restore_rows = ADD_COUNTER(profile, "SpillRestoreRows", TUnit::UNIT);
    restore_timer = ADD_TIMER(profile, "SpillRestoreTimer");
    shuffle_timer = ADD_TIMER(profile, "SpillShuffleTimer");
    split_partition_timer = ADD_TIMER(profile, "SplitPartitionTimer");

    flush_bytes = ADD_COUNTER(profile, "SpillFlushBytes", TUnit::BYTES);
    restore_bytes = ADD_COUNTER(profile, "SpillRestoreBytes", TUnit::BYTES);
    serialize_timer = ADD_TIMER(profile, "SpillSerializeTimer");
    deserialize_timer = ADD_TIMER(profile, "SpillDeserializeTimer");
    // log_block_num = ADD_COUNTER(profile, "SpillLogBlockNum");
    // log_block_container_num = ADD_COUNTER(profile, "SpillLogBlockContainerNum");

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