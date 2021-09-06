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

    virtual bool has_next_chunk() = 0;

    virtual StatusOr<vectorized::ChunkUniquePtr> get_next_chunk() = 0;
    virtual void cache_next_chunk_blocking() = 0;
    virtual StatusOr<vectorized::ChunkUniquePtr> get_next_chunk_nonblocking() = 0;

protected:
    // The morsel will own by pipeline driver
    MorselPtr _morsel;
};

using ChunkSourcePtr = starrocks::exclusive_ptr<ChunkSource>;
using ChunkSourcePromise = std::promise<ChunkSourcePtr>;
using ChunkSourceFromisePtr = starrocks::exclusive_ptr<ChunkSourcePromise>;
using ChunkSourceFuture = std::future<ChunkSourcePtr>;
using OptionalChunkSourceFuture = std::optional<ChunkSourceFuture>;
} // namespace pipeline
} // namespace starrocks