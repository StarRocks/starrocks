// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <string_view>
#include <vector>

#include "column/chunk.h"
#include "common/statusor.h"
#include "gen_cpp/data.pb.h" // ChunkPB

namespace starrocks {
class RowDescriptor;
}

namespace starrocks::serde {

class ProtobufChunkDeserializer;

class ProtobufChunkSerde {
public:
    static int64_t max_serialized_size(const vectorized::Chunk& chunk, const int encode_level = 0);

    // Write the contents of |chunk| to ChunkPB
    static StatusOr<ChunkPB> serialize(const vectorized::Chunk& chunk, const int encode_level = 0);

    // Like `serialize()` but leave the following fields of ChunkPB unfilled:
    //  - slot_id_map()
    //  - tuple_id_map()
    //  - is_nulls()
    //  - is_consts()
    static StatusOr<ChunkPB> serialize_without_meta(const vectorized::Chunk& chunk, const int encode_level = 0);

    // REQUIRE: the following fields of |chunk_pb| must be non-empty:
    //  - slot_id_map()
    //  - tuple_id_map()
    //  - is_nulls()
    //  - is_consts()
    static StatusOr<vectorized::Chunk> deserialize(const RowDescriptor& row_desc, const ChunkPB& chunk_pb,
                                                   const int encode_level = 0);
};

struct ProtobufChunkMeta {
    std::vector<TypeDescriptor> types;
    std::vector<bool> is_nulls;
    std::vector<bool> is_consts;
    vectorized::Chunk::SlotHashMap slot_id_to_index;
    vectorized::Chunk::TupleHashMap tuple_id_to_index;
};

class ProtobufChunkDeserializer {
public:
    explicit ProtobufChunkDeserializer(const ProtobufChunkMeta& meta, int encode_level = 0)
            : _meta(meta), _encode_level(encode_level) {}

    StatusOr<vectorized::Chunk> deserialize(std::string_view buff, int64_t* deserialized_bytes = nullptr);

private:
    const ProtobufChunkMeta& _meta;
    int _encode_level;
};

StatusOr<ProtobufChunkMeta> build_protobuf_chunk_meta(const RowDescriptor& row_desc, const ChunkPB& chunk_pb);

} // namespace starrocks::serde
