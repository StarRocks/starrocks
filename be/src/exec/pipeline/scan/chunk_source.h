// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <future>

#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/pipeline/scan/chunk_buffer_limiter.h"
#include "exec/pipeline/scan/morsel.h"
#include "exec/workgroup/work_group_fwd.h"
#include "util/exclusive_ptr.h"

namespace starrocks {

class RuntimeState;
class RuntimeProfile;

namespace pipeline {

class ChunkSource {
public:
    ChunkSource(RuntimeProfile* runtime_profile, MorselPtr&& morsel)
            : _runtime_profile(runtime_profile), _morsel(std::move(morsel)) {}

    virtual ~ChunkSource() = default;

    virtual Status prepare(RuntimeState* state) = 0;

    // Mark that it needn't produce any chunk anymore.
    virtual Status set_finished(RuntimeState* state) = 0;
    virtual void close(RuntimeState* state) = 0;

    // Return true if eos is not reached
    // Return false if eos is reached or error occurred
    virtual bool has_next_chunk() const = 0;

    // Whether cache is empty or not
    virtual bool has_output() const = 0;

    virtual size_t get_buffer_size() const = 0;

    virtual StatusOr<vectorized::ChunkPtr> get_next_chunk_from_buffer() = 0;

    virtual Status buffer_next_batch_chunks_blocking(size_t chunk_size, RuntimeState* state) = 0;
    virtual Status buffer_next_batch_chunks_blocking_for_workgroup(size_t chunk_size, RuntimeState* state,
                                                                   workgroup::WorkGroup* running_wg) = 0;

    // Counters of scan
    int64_t get_cpu_time_spent() { return _cpu_time_spent_ns; }
    int64_t get_scan_rows() const { return _scan_rows_num; }
    int64_t get_scan_bytes() const { return _scan_bytes; }

    void pin_chunk_token(ChunkBufferTokenPtr chunk_token) { _chunk_token = std::move(chunk_token); }
    void unpin_chunk_token() { _chunk_token.reset(nullptr); }

protected:
    RuntimeProfile* _runtime_profile;
    // The morsel will own by pipeline driver
    MorselPtr _morsel;

    // NOTE: These counters need to be maintained by ChunkSource implementations, and update in realtime
    int64_t _cpu_time_spent_ns = 0;
    int64_t _scan_rows_num = 0;
    int64_t _scan_bytes = 0;

    ChunkBufferTokenPtr _chunk_token = nullptr;
};

using ChunkSourcePtr = std::shared_ptr<ChunkSource>;
using ChunkSourcePromise = std::promise<ChunkSourcePtr>;
using ChunkSourceFromisePtr = starrocks::exclusive_ptr<ChunkSourcePromise>;
using ChunkSourceFuture = std::future<ChunkSourcePtr>;
using OptionalChunkSourceFuture = std::optional<ChunkSourceFuture>;

} // namespace pipeline
} // namespace starrocks
