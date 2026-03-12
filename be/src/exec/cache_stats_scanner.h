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

#include <memory>

#include "common/status.h"
#include "gen_cpp/InternalService_types.h"

namespace starrocks {

class Chunk;
class RuntimeState;
class TupleDescriptor;
using ChunkPtr = std::shared_ptr<Chunk>;

class CacheStatsScanner {
public:
    explicit CacheStatsScanner(const TupleDescriptor* tuple_desc);
    ~CacheStatsScanner() = default;

    Status init(RuntimeState* state, const TInternalScanRange& scan_range);
    Status open(RuntimeState* state);
    void close(RuntimeState* state);
    Status get_chunk(RuntimeState* state, ChunkPtr* chunk, bool* eos);

private:
    int64_t _tablet_id = 0;
    int64_t _version = 0;
};

} // namespace starrocks
