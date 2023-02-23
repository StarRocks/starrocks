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

#include <future>

#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/pipeline/scan/morsel.h"
#include "exec/workgroup/work_group_fwd.h"
#include "util/exclusive_ptr.h"

namespace starrocks {

class RuntimeState;
class RuntimeProfile;

namespace pipeline {

class BalancedChunkBuffer;
class ChunkBufferToken;
using ChunkBufferTokenPtr = std::unique_ptr<ChunkBufferToken>;

class ChunkSource {
public:
    ChunkSource(int32_t scan_operator_id, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
                BalancedChunkBuffer& chunk_buffer);

    virtual ~ChunkSource() = default;

    virtual Status prepare(RuntimeState* state);

    virtual void close(RuntimeState* state) = 0;

    // Return true if eos is not reached
    // Return false if eos is reached or error occurred
    bool has_next_chunk() const { return _status.ok(); }
    bool has_output() const;
    bool has_shared_output() const;

    StatusOr<ChunkPtr> get_next_chunk_from_buffer();
    Status buffer_next_batch_chunks_blocking(RuntimeState* state, size_t batch_size,
                                             const workgroup::WorkGroup* running_wg);

    // Counters of scan
    int64_t get_cpu_time_spent() const { return _cpu_time_spent_ns; }
    int64_t get_scan_rows() const { return _scan_rows_num; }
    int64_t get_scan_bytes() const { return _scan_bytes; }

    RuntimeProfile::Counter* scan_timer() { return _scan_timer; }
    RuntimeProfile::Counter* io_task_wait_timer() { return _io_task_wait_timer; }
    RuntimeProfile::Counter* io_task_exec_timer() { return _io_task_exec_timer; }

    void pin_chunk_token(ChunkBufferTokenPtr chunk_token);
    void unpin_chunk_token();

protected:
    // MUST be implemented by different ChunkSource
    virtual Status _read_chunk(RuntimeState* state, ChunkPtr* chunk) = 0;
    // The schedule entity of this workgroup for resource group.
    virtual const workgroup::WorkGroupScanSchedEntity* _scan_sched_entity(const workgroup::WorkGroup* wg) const = 0;

    // Yield scan io task when maximum time in nano-seconds has spent in current execution round.
    static constexpr int64_t YIELD_MAX_TIME_SPENT = 100'000'000L;
    // Yield scan io task when maximum time in nano-seconds has spent in current execution round,
    // if it runs in the worker thread owned by other workgroup, which has running drivers.
    static constexpr int64_t YIELD_PREEMPT_MAX_TIME_SPENT = 5'000'000L;

    const int32_t _scan_operator_seq;
    RuntimeProfile* _runtime_profile;
    // The morsel will own by pipeline driver
    MorselPtr _morsel;

    // NOTE: These counters need to be maintained by ChunkSource implementations, and update in realtime
    int64_t _cpu_time_spent_ns = 0;
    int64_t _scan_rows_num = 0;
    int64_t _scan_bytes = 0;

    BalancedChunkBuffer& _chunk_buffer;
    Status _status = Status::OK();
    ChunkBufferTokenPtr _chunk_token;

private:
    // _scan_timer = _io_task_wait_timer + _io_task_exec_timer
    RuntimeProfile::Counter* _scan_timer = nullptr;
    RuntimeProfile::Counter* _io_task_wait_timer = nullptr;
    RuntimeProfile::Counter* _io_task_exec_timer = nullptr;
};

using ChunkSourcePtr = std::shared_ptr<ChunkSource>;
using ChunkSourcePromise = std::promise<ChunkSourcePtr>;
using ChunkSourceFromisePtr = starrocks::exclusive_ptr<ChunkSourcePromise>;
using ChunkSourceFuture = std::future<ChunkSourcePtr>;
using OptionalChunkSourceFuture = std::optional<ChunkSourceFuture>;

} // namespace pipeline
} // namespace starrocks