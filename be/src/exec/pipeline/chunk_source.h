// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/pipeline/morsel.h"

namespace starrocks {
class RuntimeState;
namespace pipeline {

class ChunkSource {
public:
    ChunkSource(Morsel* morsel) : _morsel(morsel) {}

    virtual ~ChunkSource() = default;

    virtual Status prepare(RuntimeState* state) = 0;

    virtual Status close(RuntimeState* state) = 0;

    virtual bool has_next_chunk() = 0;

    virtual StatusOr<vectorized::ChunkUniquePtr> get_next_chunk() = 0;

protected:
    // The morsel will own by pipeline driver
    Morsel* _morsel;
};

using ChunkSourcePtr = std::unique_ptr<ChunkSource>;

} // namespace pipeline
} // namespace starrocks