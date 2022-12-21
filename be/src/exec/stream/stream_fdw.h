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

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/hash_set.h"
#include "column/stream_chunk.h"
#include "column/type_traits.h"

namespace starrocks::stream {

template <typename T>
using Buffer = Buffer<T>;
using Columns = Columns;

using Datum = Datum;
using DaumKey = DatumKey;
using DatumRow = std::vector<Datum>;
using DatumRowPtr = std::shared_ptr<DatumRow>;
using DatumRowOpt = std::optional<DatumRow>;
using Chunk = Chunk;
using ChunkPtr = ChunkPtr;
using StreamChunk = StreamChunk;
using StreamChunkPtr = StreamChunkPtr;
using StreamChunkConverter = StreamChunkConverter;
using StreamRowOp = StreamRowOp;

} // namespace starrocks::stream
