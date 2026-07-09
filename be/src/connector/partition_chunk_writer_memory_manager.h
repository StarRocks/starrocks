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

#pragma once

#include <atomic>
#include <cstdint>
#include <vector>

#include "common/status.h"
#include "connector/partition_chunk_writer.h"
#include "connector_primitive/sink_memory_manager.h"
#include "formats/io/async_flush_stream_poller.h"

namespace starrocks::connector {

/// manage memory of a single partition-writer sink operator
/// not thread-safe except `releasable_memory()` and `writer_occupied_memory()`
class PartitionChunkWriterMemoryManager final : public SinkOperatorMemoryManager {
public:
    PartitionChunkWriterMemoryManager() = default;

    Status init(std::vector<PartitionChunkWriterPtr>* writers, formats::AsyncFlushStreamPoller* io_poller);

    // Register an additional writer list. Used by composite sinks
    // (e.g. IcebergRowDeltaSink) so memory pressure logic can see writers
    // owned by their sub-sinks.
    void add_candidates(std::vector<PartitionChunkWriterPtr>* writers);

    bool kill_victim() override;

    int64_t update_releasable_memory() override;

    int64_t update_writer_occupied_memory() override;

    int64_t releasable_memory() const override { return _releasable_memory.load(); }

    int64_t writer_occupied_memory() const override { return _writer_occupied_memory.load(); }

private:
    // One or more references to writer lists owned by sink operator(s).
    std::vector<std::vector<PartitionChunkWriterPtr>*> _candidate_lists;
    formats::AsyncFlushStreamPoller* _io_poller = nullptr;
    std::atomic_int64_t _releasable_memory{0};
    std::atomic_int64_t _writer_occupied_memory{0};
};

} // namespace starrocks::connector
