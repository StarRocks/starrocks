// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include "column/chunk.h"

namespace starrocks::vectorized {

using UInt8ColumnPtr = std::shared_ptr<vectorized::UInt8Column>;

class StreamChunk;
using StreamChunkPtr = std::shared_ptr<StreamChunk>;

enum StreamRowOp : std::uint8_t { INSERT = 0, DELETE = 1, UPDATE_BEFORE = 2, UPDATE_AFTER = 3 };
using StreamRowOps = std::vector<StreamRowOp>;

/**
 * StreamChunk is used for Stream MV, which contains a Chunk and a StreamRowOp dcolumn.
 */
class StreamChunk : public Chunk {
public:
    StreamChunk(ChunkPtr&& chunk, UInt8ColumnPtr&& ops) : Chunk(std::move(*chunk)), _ops(std::move(ops)) {}

    StreamChunk() = default;

    StreamChunk(StreamChunk&& other) = default;
    StreamChunk& operator=(StreamChunk&& other) = default;

    ~StreamChunk() override = default;

    // Disallow copy and assignment.
    StreamChunk(const StreamChunk& other) = delete;
    StreamChunk& operator=(const StreamChunk& other) = delete;

    // For stream chunk, the last column must be Int8Column and not nullable, convert to StreamRowOp.
    const StreamRowOp* ops() const { return (StreamRowOp*)(_ops->get_data().data()); }
    const UInt8ColumnPtr ops_col() const { return _ops; }

private:
    const UInt8ColumnPtr _ops;
};

} // namespace starrocks::vectorized
