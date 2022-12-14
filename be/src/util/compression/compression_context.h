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

#include <lz4/lz4.h>
#include <lz4/lz4frame.h>
#include <zstd/zstd.h>
#include <zstd/zstd_errors.h>

#include "util/faststring.h"

namespace starrocks::compression {

struct ZSTDCompressionContext {
    ZSTDCompressionContext() {}

    // ZSTD compression context
    ZSTD_CCtx* ctx{nullptr};

    bool compression_fail{false};
    uint32_t compression_count{0};

    // This compression_buffer is shared under the same compress context.
    // We will first save the compression result on this compression_buffer,
    // and then copy the result in compression_buffer to the target output `compressed_body`.
    // Through this way, we can avoid allocate a very large compressed_body first and then shrink it.
    faststring compression_buffer;
};

struct ZSTDDecompressContext {
    ZSTDDecompressContext() {}

    // ZSTD decompression context
    ZSTD_DCtx* ctx{nullptr};
    bool decompression_fail{false};
    uint32_t decompression_count{0};
};

struct LZ4FCompressContext {
    LZ4FCompressContext() {}

    // LZ4F compression context
    LZ4F_compressionContext_t ctx{nullptr};

    bool compression_fail{false};
    uint32_t compression_count{0};

    // This compression_buffer is shared under the same compress context.
    // We will first save the compression result on this compression_buffer,
    // and then copy the result in compression_buffer to the target output `compressed_body`.
    // Through this way, we can avoid allocate a very large compressed_body first and then shrink it.
    faststring compression_buffer;
};

struct LZ4FDecompressContext {
    LZ4FDecompressContext() {}

    // LZ4F decompression context
    LZ4F_decompressionContext_t ctx{nullptr};

    bool decompression_fail{false};
    uint32_t decompression_count{0};
};

struct LZ4CompressContext {
    LZ4CompressContext() {}

    // LZ4 compression context
    LZ4_stream_t* ctx{nullptr};

    bool compression_fail{false};
    uint32_t compression_count{0};

    // This compression_buffer is shared under the same compress context.
    // We will first save the compression result on this compression_buffer,
    // and then copy the result in compression_buffer to the target output `compressed_body`.
    // Through this way, we can avoid allocate a very large compressed_body first and then shrink it.
    faststring compression_buffer;
};

} // namespace starrocks::compression
