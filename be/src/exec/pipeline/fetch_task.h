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

#include <memory>

#include "column/column.h"
#include "common/global_types.h"
#include "runtime/runtime_state.h"
#include "util/phmap/phmap.h"

namespace starrocks {
class RuntimeState;
}
namespace starrocks::pipeline {

class FetchProcessor;
class FetchTask;
using FetchTaskPtr = std::shared_ptr<FetchTask>;

// @TODO need a new name?
struct BatchUnit {
    std::vector<ChunkPtr> input_chunks;
    phmap::flat_hash_map<TupleId, std::shared_ptr<std::vector<FetchTaskPtr>>> fetch_tasks;

    int32_t total_request_num = 0;
    std::atomic_int32_t finished_request_num = 0;

    size_t next_output_idx = 0;
    bool build_output_done = false;
    // null rows' position
    phmap::flat_hash_map<uint32_t, ColumnPtr> missing_positions;
    std::string debug_string() const;

    bool all_fetch_done() const {
        return finished_request_num == total_request_num;
    }

    bool reach_end() const { return next_output_idx >= input_chunks.size(); }
    ChunkPtr get_next_chunk() { return input_chunks[next_output_idx++]; }
};
using BatchUnitPtr = std::shared_ptr<BatchUnit>;


class FetchTaskContext {
public:
    FetchTaskContext() = default;
    virtual ~FetchTaskContext() = default;

    FetchProcessor* processor = nullptr;
    BatchUnitPtr unit;
    TupleId request_tuple_id = 0;
    int32_t source_node_id = 0;
    // request chunk, contains all request-related columns
    ChunkPtr request_chunk;
    mutable phmap::flat_hash_map<SlotId, ColumnPtr> response_columns;
    int64_t send_ts = 0; // used to calculate latency
    std::function<void(const Status&)> callback;
};
using FetchTaskContextPtr = std::shared_ptr<FetchTaskContext>;

class FetchTask {
public:
    FetchTask(FetchTaskContextPtr ctx) : _ctx(std::move(ctx)) {}
    virtual ~FetchTask() = default;

    // Submit the task, return OK if success
    virtual Status submit(RuntimeState* state);
    virtual bool is_local() const {
        return false;
    }
    // Check if the task is done
    virtual bool is_done() const {
        return _is_done;
    }
    FetchTaskContextPtr get_ctx() const {
        return _ctx;
    }

protected:
    virtual Status _submit_local_task(RuntimeState* state);
    virtual Status _submit_remote_task(RuntimeState* state);
    FetchTaskContextPtr _ctx;
    std::atomic_bool _is_done = false;
};

// @TODO need LookUpRequest?

class IcebergFetchTask : public FetchTask {
public:
    IcebergFetchTask(FetchTaskContextPtr ctx) : FetchTask(std::move(ctx)) {}
};
using IcebergFetchTaskPtr = std::shared_ptr<IcebergFetchTask>;

}