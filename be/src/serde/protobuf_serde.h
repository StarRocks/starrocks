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

#include <streamvbyte.h>

#include <string_view>
#include <vector>

#include "column/chunk.h"
#include "column/chunk_extra_data.h"
#include "common/statusor.h"
#include "gen_cpp/data.pb.h" // ChunkPB
#include "serde/encode_context.h"

namespace starrocks {
class RowDescriptor;
}

namespace starrocks::serde {

class ProtobufChunkDeserializer;

class ProtobufChunkSerde {
public:
    static int64_t max_serialized_size(const Chunk& chunk, const std::shared_ptr<EncodeContext>& context = nullptr);

    // Write the contents of |chunk| to ChunkPB
    static StatusOr<ChunkPB> serialize(const Chunk& chunk, const std::shared_ptr<EncodeContext>& context = nullptr);

    // Like `serialize()` but leave the following fields of ChunkPB unfilled:
    //  - slot_id_map()
    //  - tuple_id_map()
    //  - is_nulls()
    //  - is_consts()
    static StatusOr<ChunkPB> serialize_without_meta(const Chunk& chunk,
                                                    const std::shared_ptr<EncodeContext>& context = nullptr);

    // REQUIRE: the following fields of |chunk_pb| must be non-empty:
    //  - slot_id_map()
    //  - tuple_id_map()
    //  - is_nulls()
    //  - is_consts()
    static StatusOr<Chunk> deserialize(const RowDescriptor& row_desc, const ChunkPB& chunk_pb,
                                       const int encode_level = 0);
};

struct ProtobufChunkMeta {
    std::vector<TypeDescriptor> types;
    std::vector<bool> is_nulls;
    std::vector<bool> is_consts;
    Chunk::SlotHashMap slot_id_to_index;
    // extra data meta
    std::vector<ChunkExtraColumnsMeta> extra_data_metas;
};

class ProtobufChunkDeserializer {
public:
    explicit ProtobufChunkDeserializer(const ProtobufChunkMeta& meta, const ChunkPB* const pb = nullptr,
                                       const int encode_level = 0)
            : _meta(meta) {
        _encode_level.clear();
        // NOTE: to be compatible with older version, during upgrade or downgrade, encode_level should be 0,
        // and older version sends chunks without encode_level fields.
        if (pb != nullptr && encode_level) {
            for (auto i = 0; i < pb->encode_level_size(); ++i) {
                _encode_level.emplace_back(pb->encode_level(i));
            }
        }
    }

    StatusOr<Chunk> deserialize(std::string_view buff, int64_t* deserialized_bytes = nullptr);

private:
    const ProtobufChunkMeta& _meta;
    std::vector<int> _encode_level;
};

StatusOr<Chunk> deserialize_chunk_pb_with_schema(const Schema& schema, std::string_view buff);

StatusOr<ProtobufChunkMeta> build_protobuf_chunk_meta(const RowDescriptor& row_desc, const ChunkPB& chunk_pb);

} // namespace starrocks::serde
