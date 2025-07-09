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
#include "column/column.h"

namespace starrocks {
class Schema;

namespace lake {
class KeyToChunkConverter;
using KeyToChunkConverterUPtr = std::unique_ptr<KeyToChunkConverter>;

class KeyToChunkConverter {
public:
    KeyToChunkConverter(Schema pkey_schema, ChunkUniquePtr& pk_chunk, MutableColumnPtr& pk_column)
            : _pkey_schema(pkey_schema), _pk_chunk(std::move(pk_chunk)), _pk_column(std::move(pk_column)) {}
    ~KeyToChunkConverter() = default;

    static StatusOr<KeyToChunkConverterUPtr> create(const TabletSchemaPB& tablet_schema_pb);
    // Convert key from sstable to a chunk
    StatusOr<ChunkUniquePtr> convert_to_chunk(const std::string& key);

private:
    // convert context for key
    Schema _pkey_schema;
    ChunkUniquePtr _pk_chunk;
    MutableColumnPtr _pk_column;
};

} // namespace lake
} // namespace starrocks
