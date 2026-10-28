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
#include "runtime/serde/chunk_encode_context.h"

namespace starrocks {
class RecordDescriptor;
class Schema;
} // namespace starrocks

namespace starrocks::serde {

// ENCODE_ALL_NULL changes the NullableColumn layout, so both ends of a payload must agree on it
// before it may be used. An exchange payload carries no such agreement: its level comes from the
// free-form transmission_encode_level session variable, and a peer that predates the bit treats it
// as unused, so with the variable set to 15 or -1 that peer advertises the bit in ChunkPB while
// emitting the legacy layout -- and the level a receiver applies is the sender's, not its own.
// Strip it from every exchange level in both directions.
inline int exchange_encode_level(int encode_level) {
    return encode_level & ~ENCODE_ALL_NULL;
}

// The tablet sink RPC does establish that agreement: the receiving BE advertises the bits it honors
// in PTabletWriterOpenResult::supported_chunk_encode_level, and the sender ANDs its own level with
// it before the first chunk goes out. That path passes |all_null_negotiated| = true; every other
// caller keeps the exchange behavior of stripping the bit.
inline int wire_encode_level(int encode_level, bool all_null_negotiated) {
    return all_null_negotiated ? encode_level : exchange_encode_level(encode_level);
}

class ProtobufChunkDeserializer;

class ProtobufChunkSerde {
public:
    // |all_null_negotiated| must stay false unless the receiver has agreed to the ENCODE_ALL_NULL
    // layout; see wire_encode_level().
    static int64_t max_serialized_size(const Chunk& chunk, const std::shared_ptr<EncodeContext>& context = nullptr,
                                       bool all_null_negotiated = false);

    // Write the contents of |chunk| to ChunkPB
    static StatusOr<ChunkPB> serialize(const Chunk& chunk, const std::shared_ptr<EncodeContext>& context = nullptr,
                                       bool all_null_negotiated = false);

    // Like `serialize()` but leave the following fields of ChunkPB unfilled:
    //  - slot_id_map()
    //  - tuple_id_map()
    //  - is_nulls()
    //  - is_consts()
    static StatusOr<ChunkPB> serialize_without_meta(const Chunk& chunk,
                                                    const std::shared_ptr<EncodeContext>& context = nullptr,
                                                    bool all_null_negotiated = false);

    // REQUIRE: the following fields of |chunk_pb| must be non-empty:
    //  - slot_id_map()
    //  - tuple_id_map()
    //  - is_nulls()
    //  - is_consts()
    static StatusOr<Chunk> deserialize(const RecordDescriptor& record_desc, const ChunkPB& chunk_pb,
                                       const int encode_level = 0);

    static StatusOr<Chunk> deserialize_with_schema(const Schema& schema, std::string_view buff);
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
                                       const int encode_level = 0, bool all_null_negotiated = false)
            : _meta(meta) {
        _encode_level.clear();
        // NOTE: to be compatible with older version, during upgrade or downgrade, encode_level should be 0,
        // and older version sends chunks without encode_level fields.
        if (pb != nullptr && encode_level) {
            for (auto i = 0; i < pb->encode_level_size(); ++i) {
                _encode_level.emplace_back(wire_encode_level(pb->encode_level(i), all_null_negotiated));
            }
        }
    }

    StatusOr<Chunk> deserialize(std::string_view buff, int64_t* deserialized_bytes = nullptr);

private:
    const ProtobufChunkMeta& _meta;
    std::vector<int> _encode_level;
};

StatusOr<ProtobufChunkMeta> build_protobuf_chunk_meta(const RecordDescriptor& record_desc, const ChunkPB& chunk_pb);

} // namespace starrocks::serde
