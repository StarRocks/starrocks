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

#include "connector/connector_sink_executor.h"

#include "column/chunk.h"
#include "common/status.h"
#include "connector/partition_chunk_writer.h"
#include "storage/load_chunk_spiller.h"

namespace starrocks::connector {

Status ConnectorSinkSpillExecutor::init() {
    return ThreadPoolBuilder(_executor_name)
            .set_min_threads(0)
            .set_max_threads(calc_max_thread_num())
            .build(&_thread_pool);
}

int ConnectorSinkSpillExecutor::calc_max_thread_num() {
    int dir_count = 0;
    std::vector<starrocks::StorePath> spill_local_storage_paths;
    Status st = parse_conf_store_paths(config::spill_local_storage_dir, &spill_local_storage_paths);
    if (st.ok()) {
        dir_count = spill_local_storage_paths.size();
    }

    int threads = config::lake_flush_thread_num_per_store;
    if (threads == 0) {
        threads = -2;
    }
    if (threads <= 0) {
        threads = -threads;
        threads *= CpuInfo::num_cores();
    }
    dir_count = std::max(1, dir_count);
    dir_count = std::min(8, dir_count);
    return dir_count * threads;
}

void ChunkSpillTask::run() {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker);
    auto res = _load_chunk_spiller->spill(*_chunk);
    if (_cb) {
        _cb(_chunk, res);
    }
    _chunk.reset();
}

void MergeBlockTask::run() {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker);
    auto st = _writer->merge_blocks();
    if (_cb) {
        _cb(st);
    }
}

} // namespace starrocks::connector
