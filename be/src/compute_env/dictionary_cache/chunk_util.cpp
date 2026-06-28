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

#include "compute_env/dictionary_cache/chunk_util.h"

#include <string>
#include <string_view>
#include <utility>

#include "base/compression/block_compression.h"
#include "base/string/faststring.h"
#include "column/chunk.h"
#include "common/config_exec_flow_fwd.h"
#include "runtime/current_thread.h"
#include "serde/protobuf_serde.h"

namespace starrocks {

Status DictionaryCacheChunkUtil::compress_and_serialize_chunk(const Chunk* src, ChunkPB* dst) {
    VLOG_ROW << "serializing " << src->num_rows() << " rows";

    {
        StatusOr<ChunkPB> res = Status::OK();
        TRY_CATCH_BAD_ALLOC(res = serde::ProtobufChunkSerde::serialize(*src));
        if (!res.ok()) {
            LOG(WARNING) << "serialize chunk failed when refreshing dictionary cache, " << res.status().message();
            return res.status();
        }
        res->Swap(dst);
    }
    DCHECK(dst->has_uncompressed_size());
    DCHECK_EQ(dst->uncompressed_size(), dst->data().size());

    size_t uncompressed_size = dst->uncompressed_size();

    BlockCompressionCodec* compress_codec;
    raw::RawString compression_scratch;

    // use ZSTD as default compression method for chunkPB serialization, not configurable
    // ZSTD usually is a best compression method in practice
    (void)get_block_compression_codec(CompressionTypePB::ZSTD,
                                      const_cast<const BlockCompressionCodec**>(&compress_codec));

    DCHECK(compress_codec != nullptr);

    // try compress the ChunkPB data
    if (uncompressed_size > 0) {
        // must be true for ZSTD
        DCHECK(use_compression_pool(compress_codec->type()));

        Slice compressed_slice;
        Slice input(dst->data());
        auto st = compress_codec->compress(input, &compressed_slice, true, uncompressed_size, nullptr,
                                           &compression_scratch);
        if (!st.ok()) {
            LOG(WARNING) << "compress chunk failed when refreshing dictionary cache, " << st.message();
            return st;
        }

        double compress_ratio = (static_cast<double>(uncompressed_size)) / compression_scratch.size();
        if (LIKELY(compress_ratio > config::rpc_compress_ratio_threshold)) {
            VLOG_ROW << "uncompressed size: " << uncompressed_size
                     << ", compressed size: " << compression_scratch.size();

            dst->mutable_data()->swap(reinterpret_cast<std::string&>(compression_scratch));
            dst->set_compress_type(CompressionTypePB::ZSTD);
        }
    }

    return Status::OK();
}

Status DictionaryCacheChunkUtil::uncompress_and_deserialize_chunk(const ChunkPB& pchunk, Chunk& chunk,
                                                                  faststring* uncompressed_buffer,
                                                                  const RowDescriptor& row_desc) {
    // build chunk meta
    StatusOr<serde::ProtobufChunkMeta> res = serde::build_protobuf_chunk_meta(row_desc, pchunk);
    if (!res.ok()) {
        return res.status();
    }
    auto chunk_meta = std::move(res).value();

    // uncompress and deserialize
    if (pchunk.compress_type() == CompressionTypePB::NO_COMPRESSION) {
        TRY_CATCH_BAD_ALLOC({
            serde::ProtobufChunkDeserializer des(chunk_meta);
            StatusOr<Chunk> res = des.deserialize(pchunk.data());
            if (!res.ok()) return res.status();
            chunk = std::move(res).value();
        });
    } else {
        size_t uncompressed_size = 0;
        {
            const BlockCompressionCodec* codec = nullptr;
            (void)get_block_compression_codec(CompressionTypePB::ZSTD, &codec);
            uncompressed_size = pchunk.uncompressed_size();
            TRY_CATCH_BAD_ALLOC(uncompressed_buffer->resize(uncompressed_size));
            Slice output{uncompressed_buffer->data(), uncompressed_size};
            RETURN_IF_ERROR(codec->decompress(pchunk.data(), &output));
        }
        {
            TRY_CATCH_BAD_ALLOC({
                std::string_view buff(reinterpret_cast<const char*>(uncompressed_buffer->data()), uncompressed_size);
                serde::ProtobufChunkDeserializer des(chunk_meta);
                StatusOr<Chunk> res = Status::OK();
                TRY_CATCH_BAD_ALLOC(res = des.deserialize(buff));
                if (!res.ok()) return res.status();
                chunk = std::move(res).value();
            });
        }
    }
    return Status::OK();
}

Status DictionaryCacheChunkUtil::check_chunk_has_null(const Chunk& chunk) {
    for (const auto& column : chunk.columns()) {
        if (column->has_null()) {
            return Status::InternalError("chunk has column with null value");
        }
    }
    return Status::OK();
}

} // namespace starrocks
