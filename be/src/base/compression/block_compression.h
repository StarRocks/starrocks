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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/block_compression.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "base/container/raw_container.h"
#include "base/status.h"
#include "base/string/slice.h"
#include "gen_cpp/segment.pb.h"

namespace starrocks {

// Handles for the per-column ZSTD compression dictionary. Forward-declared (defined
// in zstd_dict.h) so this widely-included header does not pull in <zstd.h>.
namespace compression {
class ZstdCDict;
class ZstdDDict;
} // namespace compression

struct BlockCompressionOptions {
    int32_t lz4_acceleration = 1;
};

// This class is used to encapsulate Compression/Decompression algorithm.
// This class only used to compress a block data, which means all data
// should given when call compress or decompress. This class don't handle
// stream compression.
class BlockCompressionCodec {
public:
    BlockCompressionCodec(CompressionTypePB type) : _type(type) {}

    virtual ~BlockCompressionCodec() = default;

    // This function will compress input data into output.
    // output should be preallocated, and its capacity must be large enough
    // for compressed input, which can be get through max_compressed_len
    // function. Size of compressed data will be set in output's size.
    // If use_compression_buffer is true, then we will first save the compressed result in
    // compression_buffer(compression_context.h), and copy the value in
    // compression_buffer to the compressed_body. In this way, we can avoid
    // allocating a very large block of memory at the beginning and then shrink it lator.
    // This optimization is only used in LZ4F and ZSTD.
    virtual Status compress(const Slice& input, Slice* output, bool use_compression_buffer = false,
                            size_t uncompressed_size = -1, faststring* compressed_body1 = nullptr,
                            raw::RawString* compressed_body2 = nullptr,
                            const BlockCompressionOptions& options = BlockCompressionOptions()) const = 0;

    Status compress(const Slice& input, Slice* output, const BlockCompressionOptions& options) const {
        return compress(input, output, false, -1, nullptr, nullptr, options);
    }

    // Default implementation will merge input list into a big buffer and call
    // compress(Slice) to finish compression. If compression type support
    // digesting slice one by one, it should reimplement this function.
    // If use_compression_buffer is true, then we will first save the compressed result in
    // compression_buffer(compression_context.h), and copy the value in
    // compression_buffer to the compressed_body. In this way, we can avoid
    // allocating a very large block of memory at the beginning and then shrink it later.
    // This optimization is only used in LZ4F and ZSTD.
    virtual Status compress(const std::vector<Slice>& input, Slice* output, bool use_compression_buffer = false,
                            size_t uncompressed_size = -1, faststring* compressed_body1 = nullptr,
                            raw::RawString* compressed_body2 = nullptr,
                            const BlockCompressionOptions& options = BlockCompressionOptions()) const;

    Status compress(const std::vector<Slice>& input, Slice* output, const BlockCompressionOptions& options) const {
        return compress(input, output, false, -1, nullptr, nullptr, options);
    }

    // compress `input` referencing a per-column compression dictionary (a ZSTD dictionary). The
    // compression level is baked into the CDict. Only ZstdBlockCompression
    // overrides this; the base returns NotSupported so any accidental use on a
    // non-ZSTD codec fails loudly instead of writing undecodable bytes.
    virtual Status compress(const std::vector<Slice>& input, Slice* output, bool use_compression_buffer,
                            size_t uncompressed_size, faststring* compressed_body1, raw::RawString* compressed_body2,
                            const compression::ZstdCDict* cdict) const {
        return Status::NotSupported("dict-based compress is not supported by this codec");
    }

    // Decompress input data into output, output's capacity should be large
    // enough for decompressed data. Size of decompressed data will be set in
    // output's size.
    virtual Status decompress(const Slice& input, Slice* output) const = 0;

    // decompress a frame referencing a per-column compression dictionary (a ZSTD dictionary).
    // Only ZstdBlockCompression overrides this; the base returns NotSupported.
    // `use_ctx_cache` selects the decompression context strategy: true keeps a
    // dictionary-loaded context warm in a thread-local slot (so consecutive pages
    // of a column skip re-establishing the dictionary session), false borrows from
    // the shared pool like every other decompression. Reads pass true; false exists
    // so that the pool path -- which is also where a context-allocation failure
    // lands -- stays reachable from tests instead of only under memory pressure.
    virtual Status decompress(const Slice& input, Slice* output, const compression::ZstdDDict* ddict,
                              bool use_ctx_cache = true) const {
        return Status::NotSupported("dict-based decompress is not supported by this codec");
    }

    // Returns an upper bound on the max compressed length.
    virtual size_t max_compressed_len(size_t len) const = 0;

    // If compress algorithm has max_input_size limit,
    // the concrete compress algorithm will implement the virtual function.
    // LZ4 has LZ4_MAX_INPUT_SIZE limit, SNAPPY/LZ4FRAME/ZLIB/ZSTD has no limit.
    virtual bool exceed_max_input_size(size_t len) const { return false; }

    virtual size_t max_input_size() const { return std::numeric_limits<size_t>::max(); }

    CompressionTypePB type() const { return _type; }

protected:
    CompressionTypePB _type;
};

// Get a BlockCompressionCodec through type.
// Return Status::OK if a valid codec is found. If codec is null, it means it is
// NO_COMPRESSION. If codec is not null, user can use it to compress/decompress
// data. And client doesn't have to release the codec.
//
// Return not OK, if error happens.
Status get_block_compression_codec(CompressionTypePB type, const BlockCompressionCodec** codec,
                                   int compression_level = -1);

bool use_compression_pool(CompressionTypePB type);

// Dictionary decompression keeps a few ZSTD contexts warm per thread (see
// DictDCtxCache in the .cpp). Global malloc is hooked, so those ~94 KB allocations are
// already charged to whichever thread-local memory tracker happened to be current when
// a context was created -- usually some query's -- and are only released when the thread
// exits. That leaves the charge on a query that has long finished.
//
// A higher layer installs this scope so the allocation and the free are both attributed
// to a stable, process-level tracker instead. Base must not know what a MemTracker is,
// so it only calls the two hooks around ZSTD_createDCtx / ZSTD_freeDCtx. Both are
// called on the thread that owns the context; leave() must undo exactly what enter()
// did on that same thread. Installing nothing keeps the pre-existing behaviour.
using DictDCtxAllocScopeHook = void (*)();

namespace detail {
// Inline variables rather than definitions in the .cpp: the installer lives in a
// higher-level module, and a fresh cross-archive symbol would depend on static library
// ordering that some test targets do not provide.
inline DictDCtxAllocScopeHook g_dict_dctx_scope_enter = nullptr;
inline DictDCtxAllocScopeHook g_dict_dctx_scope_leave = nullptr;
inline std::atomic<size_t> g_dict_dctx_cache_bytes{0};
} // namespace detail

inline void set_dict_dctx_alloc_scope(DictDCtxAllocScopeHook enter, DictDCtxAllocScopeHook leave) {
    detail::g_dict_dctx_scope_enter = enter;
    detail::g_dict_dctx_scope_leave = leave;
}

// Bytes currently held by the per-thread dictionary decompression contexts, summed over
// every thread. Exposed so the attribution above can be sanity-checked from outside.
inline size_t dict_dctx_cache_memory_bytes() {
    return detail::g_dict_dctx_cache_bytes.load(std::memory_order_relaxed);
}

} // namespace starrocks
