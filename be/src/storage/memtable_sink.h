// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "common/status.h"

namespace starrocks {
class SegmentPB;
}

namespace starrocks::vectorized {

class Chunk;
class Column;

class MemTableSink {
public:
    virtual ~MemTableSink() = default;

    virtual Status flush_chunk(const Chunk& chunk, starrocks::SegmentPB* seg_info = nullptr) = 0;
    virtual Status flush_chunk_with_deletes(const Chunk& upserts, const Column& deletes,
                                            SegmentPB* seg_info = nullptr) = 0;
};

} // namespace starrocks::vectorized
