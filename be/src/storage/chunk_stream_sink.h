// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <vector>

#include "common/status.h"

namespace starrocks {
namespace vectorized {
class Chunk;
class Column;
} // namespace vectorized

class ChunkStreamSink {
    using Chunk = vectorized::Chunk;
    using Column = vectorized::Column;

public:
    virtual ~ChunkStreamSink() = default;

    virtual Status init() = 0;
    virtual Status add_chunk(const Chunk& data) = 0;
    virtual Status add_chunk_with_deletes(const Chunk& data, const Column& deletes) = 0;
    virtual Status add_chunk_with_rssid(const Chunk& chunk, const vector<uint32_t>& rssid) = 0;
    virtual Status flush() = 0;
    virtual Status close() = 0;
};

} // namespace starrocks
