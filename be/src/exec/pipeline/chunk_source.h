// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <future>

#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/pipeline/morsel.h"
#include "util/exclusive_ptr.h"

namespace starrocks {
class RuntimeState;
namespace pipeline {

class ChunkSource {
public:
    ChunkSource(MorselPtr&& morsel) : _morsel(std::move(morsel)) {}

    virtual ~ChunkSource() = default;

    virtual Status prepare(RuntimeState* state) = 0;

    virtual Status close(RuntimeState* state) = 0;

    // Return true if eos is not reached
    // Return false if eos is reached or error occurred
    virtual bool has_next_chunk() const = 0;

    // Whether cache is empty or not
    virtual bool has_output() const = 0;

    virtual size_t get_buffer_size() const = 0;

    virtual StatusOr<vectorized::ChunkPtr> get_next_chunk_from_buffer() = 0;

    virtual Status buffer_next_batch_chunks_blocking(size_t batch_size, bool& can_finish) = 0;

protected:
    // The morsel will own by pipeline driver
    MorselPtr _morsel;
};

using ChunkSourcePtr = std::shared_ptr<ChunkSource>;
using ChunkSourcePromise = std::promise<ChunkSourcePtr>;
using ChunkSourceFromisePtr = starrocks::exclusive_ptr<ChunkSourcePromise>;
using ChunkSourceFuture = std::future<ChunkSourcePtr>;
using OptionalChunkSourceFuture = std::optional<ChunkSourceFuture>;
} // namespace pipeline
} // namespace starrocks
