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

class MemTableSink {
public:
    virtual ~MemTableSink() = default;

    virtual Status flush_chunk(const Chunk& chunk, starrocks::SegmentPB* seg_info = nullptr) = 0;
    virtual Status flush_chunk_with_deletes(const Chunk& upserts, const Column& deletes,
                                            SegmentPB* seg_info = nullptr) = 0;
};

} // namespace starrocks
