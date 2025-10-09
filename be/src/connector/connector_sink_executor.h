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

#include <fmt/format.h>

#include <map>

#include "column/chunk.h"
#include "common/status.h"
#include "connector/utils.h"
#include "util/threadpool.h"

namespace starrocks {
class LoadChunkSpiller;
}

namespace starrocks::connector {

class SpillPartitionChunkWriter;

class ConnectorSinkExecutor {
public:
    ConnectorSinkExecutor(const std::string& executor_name) : _executor_name(executor_name) {}
    virtual ~ConnectorSinkExecutor() {}

    virtual Status init() = 0;

    ThreadPool* get_thread_pool() { return _thread_pool.get(); }

    std::unique_ptr<ThreadPoolToken> create_token() {
        return _thread_pool->new_token(ThreadPool::ExecutionMode::SERIAL);
    }

    Status refresh_max_thread_num() {
        if (_thread_pool != nullptr) {
            return _thread_pool->update_max_threads(calc_max_thread_num());
        }
        return Status::OK();
    }

protected:
    virtual int calc_max_thread_num() = 0;

protected:
    std::string _executor_name;
    std::unique_ptr<ThreadPool> _thread_pool;
};

class ConnectorSinkSpillExecutor : public ConnectorSinkExecutor {
public:
    ConnectorSinkSpillExecutor() : ConnectorSinkExecutor("conn_sink_spill") {}

    Status init() override;

protected:
    int calc_max_thread_num() override;
};

class ChunkSpillTask final : public Runnable {
public:
    ChunkSpillTask(LoadChunkSpiller* load_chunk_spiller, ChunkPtr chunk, MemTracker* mem_tracker,
                   std::function<void(ChunkPtr chunk, const StatusOr<size_t>&)> cb)
            : _load_chunk_spiller(load_chunk_spiller),
              _chunk(std::move(chunk)),
              _mem_tracker(mem_tracker),
              _cb(std::move(cb)) {}

    ~ChunkSpillTask() override = default;

    void run() override;

private:
    LoadChunkSpiller* _load_chunk_spiller;
    ChunkPtr _chunk;
    MemTracker* _mem_tracker;
    std::function<void(ChunkPtr, const StatusOr<size_t>&)> _cb;
};

class MergeBlockTask : public Runnable {
public:
    MergeBlockTask(SpillPartitionChunkWriter* writer, MemTracker* mem_tracker, std::function<void(const Status&)> cb)
            : _writer(writer), _mem_tracker(mem_tracker), _cb(std::move(cb)) {}

    void run() override;

private:
    SpillPartitionChunkWriter* _writer;
    MemTracker* _mem_tracker;
    std::function<void(const Status&)> _cb;
};

} // namespace starrocks::connector
