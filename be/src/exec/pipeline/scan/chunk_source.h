// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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

class ChunkSource {
public:
    ChunkSource(RuntimeProfile* runtime_profile, MorselPtr&& morsel)
            : _runtime_profile(runtime_profile), _morsel(std::move(morsel)){};

    virtual ~ChunkSource() = default;

    virtual Status prepare(RuntimeState* state) = 0;

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
                                                                   size_t* num_read_chunks, int worker_id,
                                                                   workgroup::WorkGroupPtr running_wg) = 0;

    // Some statistic of chunk source
    virtual int64_t last_spent_cpu_time_ns() { return 0; }

    virtual int64_t last_scan_rows_num() {
        int64_t res = _last_scan_rows_num;
        _last_scan_rows_num = 0;
        return res;
    }

    virtual int64_t last_scan_bytes() {
        int64_t res = _last_scan_bytes;
        _last_scan_bytes = 0;
        return res;
    }

protected:
    RuntimeProfile* _runtime_profile;
    // The morsel will own by pipeline driver
    MorselPtr _morsel;
    int64_t _last_scan_rows_num = 0;
    int64_t _last_scan_bytes = 0;
};

using ChunkSourcePtr = std::shared_ptr<ChunkSource>;
using ChunkSourcePromise = std::promise<ChunkSourcePtr>;
using ChunkSourceFromisePtr = starrocks::exclusive_ptr<ChunkSourcePromise>;
using ChunkSourceFuture = std::future<ChunkSourcePtr>;
using OptionalChunkSourceFuture = std::optional<ChunkSourceFuture>;
} // namespace pipeline
} // namespace starrocks
