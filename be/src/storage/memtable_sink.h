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

#include "common/status.h"

namespace starrocks {
class SegmentPB;
}

namespace starrocks {

class Chunk;
class Column;

// MemTableSink defines the interface for flushing memtable data to persistent storage
class MemTableSink {
public:
    virtual ~MemTableSink() = default;

    // Flush a chunk of data to the sink
    // @param chunk: data to be flushed
    // @param seg_info: output segment metadata
    // @param eos: whether this is the end of the stream
    // @param flush_data_size: output parameter for the size of flushed data
    // @param slot_idx: slot index for tracking flush order in parallel flush scenarios.
    //                  Used to ensure correct ordering when merging spilled blocks.
    //                  Default -1 means slot tracking is not needed.
    virtual Status flush_chunk(const Chunk& chunk, starrocks::SegmentPB* seg_info = nullptr, bool eos = false,
                               int64_t* flush_data_size = nullptr, int64_t slot_idx = -1) = 0;
    // Flush a chunk with delete operations for primary key tables
    // @param slot_idx: see flush_chunk() for details
    virtual Status flush_chunk_with_deletes(const Chunk& upserts, const Column& deletes, SegmentPB* seg_info = nullptr,
                                            bool eos = false, int64_t* flush_data_size = nullptr,
                                            int64_t slot_idx = -1) = 0;

    virtual int64_t txn_id() = 0;
    virtual int64_t tablet_id() = 0;
};

} // namespace starrocks
