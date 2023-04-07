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

#include "exec/spill/serde.h"

#include "exec/spill/options.h"
#include "exec/spill/spiller.h"
#include "gen_cpp/types.pb.h"
#include "gutil/port.h"
#include "runtime/runtime_state.h"
#include "serde/column_array_serde.h"

namespace starrocks::spill {

class ColumnarSerde : public Serde {
public:
    ColumnarSerde(ChunkBuilder chunk_builder, const BlockCompressionCodec* compress_codec)
            : _chunk_builder(std::move(chunk_builder)), _compress_codec(compress_codec) {}
    ~ColumnarSerde() override = default;

    Status serialize(SerdeContext& ctx, const ChunkPtr& chunk, BlockPtr block) override;
    StatusOr<ChunkUniquePtr> deserialize(SerdeContext& ctx, BlockReader* reader) override;

private:
    size_t serialize_size(const ChunkPtr& chunk) const;

    ChunkBuilder _chunk_builder;
    const BlockCompressionCodec* _compress_codec = nullptr;
};

size_t ColumnarSerde::serialize_size(const ChunkPtr& chunk) const {
    size_t total_size = 0;
    for (const auto& column : chunk->columns()) {
        total_size += serde::ColumnArraySerde::max_serialized_size(*column);
    }
    return total_size;
}

Status ColumnarSerde::serialize(SerdeContext& ctx, const ChunkPtr& chunk, BlockPtr block) {
    // 1. serialize
    size_t max_size = serialize_size(chunk);
    auto& serialize_buffer = ctx.serialize_buffer;
    serialize_buffer.resize(max_size);
    auto* buf = reinterpret_cast<uint8_t*>(serialize_buffer.data());
    uint8_t* begin = buf;
    for (const auto& column : chunk->columns()) {
        buf = serde::ColumnArraySerde::serialize(*column, buf);
    }
    size_t uncompressed_size = buf - begin;
    serialize_buffer.resize(uncompressed_size);

    // 2. compress
    auto& compress_buffer = ctx.compress_buffer;
    Slice compress_input(serialize_buffer.data(), uncompressed_size);
    Slice compress_slice;

    int max_compressed_size = _compress_codec->max_compressed_len(uncompressed_size);
    if (compress_buffer.size() < max_compressed_size) {
        compress_buffer.resize(max_compressed_size);
    }
    compress_slice = Slice(compress_buffer.data(), compress_buffer.size());
    RETURN_IF_ERROR(_compress_codec->compress(compress_input, &compress_slice));
    size_t compressed_size = compress_slice.size;
    compress_buffer.resize(compressed_size);

    // 3. append data to block
    uint8_t meta_buf[sizeof(size_t) * 2];
    UNALIGNED_STORE64(meta_buf, compressed_size);
    UNALIGNED_STORE64(meta_buf + sizeof(size_t), uncompressed_size);

    std::vector<Slice> data;
    data.emplace_back(Slice(meta_buf, sizeof(size_t) * 2));
    data.emplace_back(compress_slice);
    RETURN_IF_ERROR(block->append(data));
    TRACE_SPILL_LOG << "serialize chunk to block: " << block->debug_string() << ", compressed size: " << compressed_size
                    << ", uncompressed size: " << uncompressed_size;
    return Status::OK();
}

StatusOr<ChunkUniquePtr> ColumnarSerde::deserialize(SerdeContext& ctx, BlockReader* reader) {
    size_t compressed_size, uncompressed_size;
    RETURN_IF_ERROR(reader->read_fully(&compressed_size, sizeof(size_t)));
    RETURN_IF_ERROR(reader->read_fully(&uncompressed_size, sizeof(size_t)));

    TRACE_SPILL_LOG << "deserialize chunk from block: " << reader->debug_string()
                    << ", compressed size: " << compressed_size << ", uncompressed size: " << uncompressed_size;

    auto& compress_buffer = ctx.compress_buffer;
    auto& serialize_buffer = ctx.serialize_buffer;
    compress_buffer.resize(compressed_size);
    serialize_buffer.resize(uncompressed_size);

    auto buf = reinterpret_cast<uint8_t*>(compress_buffer.data());
    RETURN_IF_ERROR(reader->read_fully(buf, compressed_size));
    // decompress
    Slice input_slice(compress_buffer.data(), compressed_size);
    Slice serialize_slice(serialize_buffer.data(), uncompressed_size);
    RETURN_IF_ERROR(_compress_codec->decompress(input_slice, &serialize_slice));

    // deserialize
    auto chunk = _chunk_builder();
    const uint8_t* read_cursor = reinterpret_cast<uint8_t*>(serialize_buffer.data());
    for (const auto& column : chunk->columns()) {
        read_cursor = serde::ColumnArraySerde::deserialize(read_cursor, column.get());
    }
    return chunk;
}

StatusOr<SerdePtr> Serde::create_serde(const ChunkBuilder& chunk_builder, const CompressionTypePB& compress_type) {
    const BlockCompressionCodec* codec = nullptr;
    RETURN_IF_ERROR(get_block_compression_codec(compress_type, &codec));
    return std::make_shared<ColumnarSerde>(chunk_builder, codec);
}
} // namespace starrocks::spill
