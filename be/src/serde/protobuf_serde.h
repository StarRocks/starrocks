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
constexpr double EncodeRatioLimit = 0.9;
constexpr uint32_t EncodeSamplingNum = 5;

// EncodeContext adaptively adjusts encode_level according to the compression ratio. In detail,
// for every _frequency chunks, if the compression ratio for the first EncodeSamplingNum chunks is less than
// EncodeRatioLimit, then encode the rest chunks, otherwise not.

class EncodeContext {
public:
    static std::shared_ptr<EncodeContext> get_encode_context_shared_ptr(const int col_num, const int encode_level) {
        return encode_level == 0 ? nullptr : std::make_shared<EncodeContext>(col_num, encode_level);
    }
    EncodeContext(const int col_num, const int encode_level);
    // update encode_level for each column
    void update(const int col_id, uint64_t mem_bytes, uint64_t encode_byte);

    int get_encode_level(const int col_id) {
        DCHECK(_session_encode_level != 0);
        return _column_encode_level[col_id];
    }

    int get_session_encode_level() {
        DCHECK(_session_encode_level != 0);
        return _session_encode_level;
    }

    void set_encode_levels_in_pb(ChunkPB* const res);

private:
    // if encode ratio < EncodeRatioLimit, encode it, otherwise not.
    void _adjust(const int col_id);
    const int _session_encode_level;
    uint64_t _times = 0;
    uint64_t _frequency = 64;
    bool _enable_adjust = false;
    std::vector<uint64_t> _raw_bytes, _encoded_bytes;
    std::vector<uint32_t> _column_encode_level;
};

class ProtobufChunkDeserializer;

class ProtobufChunkSerde {
public:
    static int64_t max_serialized_size(const vectorized::Chunk& chunk,
                                       const std::shared_ptr<EncodeContext>& context = nullptr);

    // Write the contents of |chunk| to ChunkPB
    static StatusOr<ChunkPB> serialize(const vectorized::Chunk& chunk,
                                       const std::shared_ptr<EncodeContext>& context = nullptr);

    // Like `serialize()` but leave the following fields of ChunkPB unfilled:
    //  - slot_id_map()
    //  - tuple_id_map()
    //  - is_nulls()
    //  - is_consts()
    static StatusOr<ChunkPB> serialize_without_meta(const vectorized::Chunk& chunk,
                                                    const std::shared_ptr<EncodeContext>& context = nullptr);

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

    StatusOr<vectorized::Chunk> deserialize(std::string_view buff, int64_t* deserialized_bytes = nullptr);

private:
    const ProtobufChunkMeta& _meta;
    std::vector<int> _encode_level;
};

StatusOr<ProtobufChunkMeta> build_protobuf_chunk_meta(const RowDescriptor& row_desc, const ChunkPB& chunk_pb);

} // namespace starrocks::serde
