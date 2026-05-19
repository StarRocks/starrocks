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

#include <cstddef>

#include "column/chunk.h"
#include "common/statusor.h"

namespace starrocks {

class Schema;

class CheckedChunkFactory {
public:
    static StatusOr<Chunk*> new_chunk_pooled_checked(const Schema& schema, size_t n);
    static StatusOr<ChunkUniquePtr> new_chunk_checked(const Schema& schema, size_t n);
    static StatusOr<MutableChunkPtr> new_mutable_chunk_checked(const Schema& schema, size_t n);
};

} // namespace starrocks
