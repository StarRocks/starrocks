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

#include "exec/spill/formatter.h"
#include "exec/spill/spiller.h"

#include "gutil/port.h"
#include "runtime/runtime_state.h"
#include "serde/column_array_serde.h"

namespace starrocks {
namespace spill {

class ColumnarFormatter: public Formatter {
public:
    ColumnarFormatter(ChunkBuilder chunk_builder, const BlockCompressionCodec* compress_codec):
        _chunk_builder(std::move(chunk_builder)), _compress_codec(compress_codec) {}
    ~ColumnarFormatter() = default;

    Status serialize(FormatterContext& ctx, const ChunkPtr& chunk, BlockPtr block) override;
    StatusOr<ChunkUniquePtr> deserialize(FormatterContext& ctx, const BlockPtr block) override;

private:
    size_t serialize_size(const ChunkPtr& chunk) const;

    ChunkBuilder _chunk_builder;
    const BlockCompressionCodec* _compress_codec = nullptr;
};

size_t ColumnarFormatter::serialize_size(const ChunkPtr& chunk) const {
    size_t total_size = 0;
    for (const auto& column : chunk->columns()) {
        total_size += serde::ColumnArraySerde::max_serialized_size(*column);
    }
    return total_size;
}

Status ColumnarFormatter::serialize(FormatterContext& ctx, const ChunkPtr& chunk, BlockPtr block) {
    size_t max_size = serialize_size(chunk);
    auto& serialize_buffer = ctx.serialize_buffer;
    serialize_buffer.resize(max_size);
    auto* buf = reinterpret_cast<uint8_t*>(serialize_buffer.data());
    uint8_t* begin = buf;
    for (const auto& column : chunk->columns()) {
        buf = serde::ColumnArraySerde::serialize(*column, buf);
    }
    size_t real_size = buf - begin;
    serialize_buffer.resize(real_size);
    // LOG(INFO) << "serialize, max_size: " << max_size << ", real size:" << real_size;

    // compress
    auto& compress_buffer = ctx.compress_buffer;
    Slice compress_input(serialize_buffer.data(), real_size);
    Slice compress_slice;

    int max_compressed_size = _compress_codec->max_compressed_len(real_size);
    if (compress_buffer.size() < max_compressed_size) {
        compress_buffer.resize(max_compressed_size);
    }
    compress_slice = Slice(compress_buffer.data(), compress_buffer.size());
    RETURN_IF_ERROR(_compress_codec->compress(compress_input, &compress_slice));
    compress_buffer.resize(compress_slice.size);
    // LOG(INFO) << "uncompressed size: " << real_size << ", compressed size: " << compress_slice.size;

    uint8_t meta_buf[sizeof(size_t) * 2];
    UNALIGNED_STORE64(meta_buf, compress_slice.size);
    UNALIGNED_STORE64(meta_buf + sizeof(size_t), real_size);
    std::vector<Slice> data;
    data.emplace_back(Slice(meta_buf, sizeof(size_t) * 2));
    data.emplace_back(compress_slice);
    RETURN_IF_ERROR(block->append(data));
    return Status::OK();
}

StatusOr<ChunkUniquePtr> ColumnarFormatter::deserialize(FormatterContext& ctx, const BlockPtr block) {
    size_t compress_size, uncompress_size;
    RETURN_IF_ERROR(block->read_fully(&compress_size, sizeof(size_t)));
    RETURN_IF_ERROR(block->read_fully(&uncompress_size, sizeof(size_t)));
    LOG(INFO) << "deserialize, compress size: " << compress_size << ", uncompress size: " << uncompress_size;

    // @TODO decompress
    auto& compress_buffer = ctx.compress_buffer;
    auto& serialize_buffer = ctx.serialize_buffer;
    compress_buffer.resize(compress_size);
    serialize_buffer.resize(uncompress_size);

    auto buf = reinterpret_cast<uint8_t*>(compress_buffer.data());
    RETURN_IF_ERROR(block->read_fully(buf, compress_size));
    // decompress
    Slice input_slice(compress_buffer.data(), compress_size);
    Slice serialize_slice(serialize_buffer.data(), uncompress_size);
    RETURN_IF_ERROR(_compress_codec->decompress(input_slice, &serialize_slice));
    LOG(INFO) << "decompress done";

    auto chunk = _chunk_builder();
    const uint8_t* read_cursor = reinterpret_cast<uint8_t*>(serialize_buffer.data());
    for (const auto& column: chunk->columns()) {
        read_cursor = serde::ColumnArraySerde::deserialize(read_cursor, column.get());
    }
    LOG(INFO) << "return chunk";
    return chunk;
}

StatusOr<FormatterPtr> create_formatter(SpilledOptions* options) {
    // @TODO
    auto compress_type = options->compress_type;
    const BlockCompressionCodec* codec = nullptr;
    RETURN_IF_ERROR(get_block_compression_codec(compress_type, &codec));
    return std::make_shared<ColumnarFormatter>(options->chunk_builder, codec);
}
}
}
