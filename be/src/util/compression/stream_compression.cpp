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
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/decompressor.cpp

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

#include "util/compression/stream_compression.h"

#include <bzlib.h>
#include <glog/logging.h>
#include <zlib.h>

#include <limits>
#include <memory>
#include <optional>

#include "fmt/compile.h"
#include "gutil/strings/substitute.h"
#include "util/coding.h"
#include "util/compression/compression_context_pool_singletons.h"
#include "util/compression/compression_headers.h"

namespace orc {
uint64_t lzoDecompress(const char* inputAddress, const char* inputLimit, char* outputAddress, char* outputLimit);
} // namespace orc

namespace starrocks {

class GzipStreamCompression : public StreamCompression {
public:
    GzipStreamCompression(bool is_deflate)
            : StreamCompression(is_deflate ? CompressionTypePB::DEFLATE : CompressionTypePB::GZIP),
              _is_deflate(is_deflate) {}

    ~GzipStreamCompression() override { (void)inflateEnd(&_z_strm); }

    std::string debug_info() override {
        std::stringstream ss;
        ss << "GzipStreamCompression."
           << " is_deflate: " << _is_deflate;
        return ss.str();
    }

    Status init() override;

    Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output, size_t output_len,
                      size_t* output_bytes_written, bool* stream_end) override;

private:
    bool _is_deflate;

    z_stream _z_strm;

    // These are magic numbers from zlib.h.  Not clear why they are not defined
    // there.
    const static int WINDOW_BITS = 15;  // Maximum window size
    const static int DETECT_CODEC = 32; // Determine if this is libz or gzip from header.
};

// Gzip
Status GzipStreamCompression::init() {
    _z_strm = {nullptr};
    _z_strm.zalloc = Z_NULL;
    _z_strm.zfree = Z_NULL;
    _z_strm.opaque = Z_NULL;

    int window_bits = _is_deflate ? WINDOW_BITS : (WINDOW_BITS | DETECT_CODEC);
    int ret = inflateInit2(&_z_strm, window_bits);
    if (ret < 0) {
        std::stringstream ss;
        ss << "Failed to init inflate. status code: " << ret;
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

Status GzipStreamCompression::decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output,
                                         size_t output_len, size_t* output_bytes_written, bool* stream_end) {
    // 1. set input and output
    _z_strm.next_in = input;
    _z_strm.avail_in = input_len;
    _z_strm.next_out = output;
    _z_strm.avail_out = output_len;

    while (_z_strm.avail_out > 0 && _z_strm.avail_in > 0) {
        *stream_end = false;
        // inflate() performs one or both of the following actions:
        //   Decompress more input starting at next_in and update next_in and
        //   avail_in
        //       accordingly.
        //   Provide more output starting at next_out and update next_out and
        //   avail_out
        //       accordingly.
        // inflate() returns Z_OK if some progress has been made (more input
        // processed or more output produced)

        int ret = inflate(&_z_strm, Z_NO_FLUSH);
        *input_bytes_read = input_len - _z_strm.avail_in;
        *output_bytes_written = output_len - _z_strm.avail_out;

        VLOG(10) << "gzip dec ret: " << ret << " input_bytes_read: " << *input_bytes_read
                 << " output_bytes_written: " << *output_bytes_written;

        if (ret == Z_BUF_ERROR) {
            // Z_BUF_ERROR indicates that inflate() could not consume more input
            // or produce more output. inflate() can be called again with more
            // output space or more available input ATTN: even if ret == Z_OK,
            // output_bytes_written may also be zero
            return Status::OK();
        } else if (ret == Z_STREAM_END) {
            *stream_end = true;
            // reset _z_strm to continue decoding a subsequent gzip stream
            ret = inflateReset(&_z_strm);
            if (ret != Z_OK) {
                std::stringstream ss;
                ss << "Failed to inflateRset. return code: " << ret;
                return Status::InternalError(ss.str());
            }
        } else if (ret != Z_OK) {
            std::stringstream ss;
            ss << "Failed to inflate. return code: " << ret;
            return Status::InternalError(ss.str());
        } else {
            // here ret must be Z_OK.
            // we continue if avail_out and avail_in > 0.
            // this means 'inflate' is not done yet.
        }
    }

    return Status::OK();
}

class Bzip2StreamCompression : public StreamCompression {
public:
    Bzip2StreamCompression() : StreamCompression(CompressionTypePB::BZIP2) {}

    ~Bzip2StreamCompression() override { BZ2_bzDecompressEnd(&_bz_strm); }

    std::string debug_info() override {
        std::stringstream ss;
        ss << "Bzip2StreamCompression.";
        return ss.str();
    }

    Status init() override;

    Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output, size_t output_len,
                      size_t* output_bytes_written, bool* stream_end) override;

private:
    bz_stream _bz_strm;
};

// Bzip2
Status Bzip2StreamCompression::init() {
    bzero(&_bz_strm, sizeof(_bz_strm));
    int ret = BZ2_bzDecompressInit(&_bz_strm, 0, 0);
    if (ret != BZ_OK) {
        std::stringstream ss;
        ss << "Failed to init bz2. status code: " << ret;
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

Status Bzip2StreamCompression::decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output,
                                          size_t output_len, size_t* output_bytes_written, bool* stream_end) {
    // 1. set input and output
    _bz_strm.next_in = const_cast<char*>(reinterpret_cast<const char*>(input));
    _bz_strm.avail_in = input_len;
    _bz_strm.next_out = reinterpret_cast<char*>(output);
    _bz_strm.avail_out = output_len;

    while (_bz_strm.avail_out > 0 && _bz_strm.avail_in > 0) {
        *stream_end = false;
        // decompress
        int ret = BZ2_bzDecompress(&_bz_strm);
        *input_bytes_read = input_len - _bz_strm.avail_in;
        *output_bytes_written = output_len - _bz_strm.avail_out;

        if (ret == BZ_DATA_ERROR || ret == BZ_DATA_ERROR_MAGIC) {
            LOG(INFO) << "input_bytes_read: " << *input_bytes_read
                      << " output_bytes_written: " << *output_bytes_written;
            std::stringstream ss;
            ss << "Failed to bz2 decompress. status code: " << ret;
            return Status::InternalError(ss.str());
        } else if (ret == BZ_STREAM_END) {
            *stream_end = true;
            ret = BZ2_bzDecompressEnd(&_bz_strm);
            if (ret != BZ_OK) {
                std::stringstream ss;
                ss << "Failed to end bz2 after meet BZ_STREAM_END. status "
                      "code: "
                   << ret;
                return Status::InternalError(ss.str());
            }

            ret = BZ2_bzDecompressInit(&_bz_strm, 0, 0);
            if (ret != BZ_OK) {
                std::stringstream ss;
                ss << "Failed to init bz2 after meet BZ_STREAM_END. status "
                      "code: "
                   << ret;
                return Status::InternalError(ss.str());
            }
        } else if (ret != BZ_OK) {
            std::stringstream ss;
            ss << "Failed to bz2 decompress. status code: " << ret;
            return Status::InternalError(ss.str());
        } else {
            // continue
        }
    }

    return Status::OK();
}

class Lz4FrameStreamCompression : public StreamCompression {
public:
    Lz4FrameStreamCompression()
            : StreamCompression(CompressionTypePB::LZ4_FRAME),
              _decompress_context(compression::LZ4F_DCtx_Pool::get_default()) {}

    ~Lz4FrameStreamCompression() override { _decompress_context.reset(); }

    std::string debug_info() override {
        std::stringstream ss;
        ss << "Lz4FrameStreamCompression."
           << " expect dec buf size: " << _expect_dec_buf_size;
        return ss.str();
    }

    ssize_t get_block_size(const LZ4F_frameInfo_t* info) {
        switch (info->blockSizeID) {
        case LZ4F_default:
        case LZ4F_max64KB:
            return 1 << 16;
        case LZ4F_max256KB:
            return 1 << 18;
        case LZ4F_max1MB:
            return 1 << 20;
        case LZ4F_max4MB:
            return 1 << 22;
        default:
            // error
            return -1;
        }
    }

    Status init() override;

    Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output, size_t output_len,
                      size_t* output_bytes_written, bool* stream_end) override;

private:
    compression::LZ4F_DCtx_Pool::Ref _decompress_context;
    ssize_t _expect_dec_buf_size{-1};

    const static unsigned STARROCKS_LZ4F_VERSION;
};

Status Lz4FrameStreamCompression::init() {
    StatusOr<compression::LZ4F_DCtx_Pool::Ref> maybe_decompress_context = compression::getLZ4F_DCtx();
    Status status = maybe_decompress_context.status();
    if (!status.ok()) {
        return status;
    }
    _decompress_context = std::move(maybe_decompress_context).value();
    // init as -1
    _expect_dec_buf_size = -1;

    return Status::OK();
}

Status Lz4FrameStreamCompression::decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read,
                                             uint8_t* output, size_t output_len, size_t* output_bytes_written,
                                             bool* stream_end) {
    LZ4F_decompressionContext_t ctx = _decompress_context->ctx;

    uint8_t* src = input;
    size_t src_size = input_len;
    size_t ret = 1;
    *input_bytes_read = 0;

    if (_expect_dec_buf_size == -1) {
        // init expected decompress buf size, and check if output_len is large
        // enough ATTN: _expect_dec_buf_size is uninit, which means this is the
        // first time to call
        //       decompress(), so *input* should point to the head of the
        //       compressed file, where lz4 header section is there.

        if (input_len < 15) {
            std::stringstream ss;
            ss << "Lz4 header size is between 7 and 15 bytes. "
               << "but input size is only: " << input_len;
            return Status::InternalError(ss.str());
        }

        LZ4F_frameInfo_t info;
        ret = LZ4F_getFrameInfo(ctx, &info, (void*)src, &src_size);
        if (LZ4F_isError(ret)) {
            std::stringstream ss;
            ss << "LZ4F_getFrameInfo error: " << std::string(LZ4F_getErrorName(ret));
            return Status::InternalError(ss.str());
        }

        _expect_dec_buf_size = get_block_size(&info);
        if (_expect_dec_buf_size == -1) {
            std::stringstream ss;
            ss << "Impossible lz4 block size unless more block sizes are "
                  "allowed"
               << std::string(LZ4F_getErrorName(ret));
            return Status::InternalError(ss.str());
        }

        *input_bytes_read = src_size;

        src += src_size;
        src_size = input_len - src_size;
    }

    // decompress
    size_t dst_size = output_len;
    ret = LZ4F_decompress(ctx, (void*)output, &dst_size, (void*)src, &src_size,
                          /* LZ4F_decompressOptions_t */ nullptr);
    if (LZ4F_isError(ret)) {
        std::stringstream ss;
        ss << "Decompression error: " << std::string(LZ4F_getErrorName(ret));
        return Status::InternalError(ss.str());
    }

    // update
    *input_bytes_read += src_size;
    *output_bytes_written = dst_size;
    if (ret == 0) {
        *stream_end = true;
    } else {
        *stream_end = false;
    }

    return Status::OK();
}

/// Zstandard is a real-time compression algorithm, providing high compression
/// ratios. It offers a very wide range of compression/speed trade-off.
class ZstandardStreamCompression : public StreamCompression {
public:
    ZstandardStreamCompression()
            : StreamCompression(CompressionTypePB::ZSTD),
              _decompress_context(compression::ZSTD_DCtx_Pool::get_default()) {}

    ~ZstandardStreamCompression() override { _decompress_context.reset(); }

    std::string debug_info() override { return "ZstandardStreamCompression"; }

    Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output, size_t output_len,
                      size_t* output_bytes_write, bool* stream_end) override;

    Status init() override;

private:
    compression::ZSTD_DCtx_Pool::Ref _decompress_context;
};

Status ZstandardStreamCompression::init() {
    StatusOr<compression::ZSTD_DCtx_Pool::Ref> maybe_decompress_context = compression::getZSTD_DCtx();
    Status status = maybe_decompress_context.status();
    if (!status.ok()) {
        return status;
    }

    _decompress_context = std::move(maybe_decompress_context).value();
    return Status::OK();
}

Status ZstandardStreamCompression::decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read,
                                              uint8_t* output, size_t output_len, size_t* output_bytes_written,
                                              bool* stream_end) {
    ZSTD_DCtx* ctx = _decompress_context->ctx;

    *input_bytes_read = 0;
    *output_bytes_written = 0;
    *stream_end = false;

    ZSTD_inBuffer input_buffer{input, static_cast<size_t>(input_len), 0};
    ZSTD_outBuffer output_buffer{output, static_cast<size_t>(output_len), 0};
    size_t ret = ZSTD_decompressStream(ctx, &output_buffer, &input_buffer);
    if (ZSTD_isError(ret)) {
        *output_bytes_written = 0;
        return Status::InternalError(
                strings::Substitute("ZSTD decompress failed. error: $0", ZSTD_getErrorString(ZSTD_getErrorCode(ret))));
    }
    if (ret == 0) {
        *stream_end = true;
    }
    *input_bytes_read = input_buffer.pos;
    *output_bytes_written = output_buffer.pos;
    return Status::OK();
}

class SnappyStreamCompression : public StreamCompression {
public:
    SnappyStreamCompression() : StreamCompression(CompressionTypePB::SNAPPY) {}
    ~SnappyStreamCompression() override = default;
    std::string debug_info() override { return "SnappyStreamCompression"; }

    Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output, size_t output_len,
                      size_t* output_bytes_write, bool* stream_end) override;

    Status init() override;

private:
    struct Context {
        uint32_t block_length = 0;
        uint32_t output_length = 0;

        std::vector<char> buffer;
        char* buffer_data;
        size_t buffer_size = 0;
        size_t buffer_used = 0;
    };
    Context _ctx;
};

Status SnappyStreamCompression::init() {
    return Status::OK();
}

Status SnappyStreamCompression::decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output,
                                           size_t output_len, size_t* output_bytes_write, bool* stream_end) {
    *input_bytes_read = 0;
    *output_bytes_write = 0;
    *stream_end = false;

    Context* ctx = &_ctx;
    Status st = Status::OK();

    // if there is any data in buffer? if not load.
    if (ctx->buffer_used == ctx->buffer_size) {
        // load block length if missing.
        if (ctx->block_length == 0) {
            if (input_len < 4) return st;
            ctx->block_length = decode_fixed32_be(input);
            input += 4;
            input_len -= 4;
            *input_bytes_read += 4;
        }
        if (ctx->block_length == 0) {
            *stream_end = true;
            return st;
        }

        // see if a compressed unit is ready.
        if (input_len < 4) return st;
        uint32_t compressed_len = decode_fixed32_be(input);
        input += 4;
        input_len -= 4;
        if (input_len < compressed_len) {
            set_compressed_block_size(compressed_len);
            return st;
        }

        *input_bytes_read += (compressed_len + 4);
        input_len -= compressed_len;
        size_t uncompressed_len = 0;
        bool ok = snappy::GetUncompressedLength((const char*)input, compressed_len, &uncompressed_len);
        if (!ok) {
            return Status::InternalError("Snappy GetUncompressedLength failed because of data corruption");
        }

        // if output space is not large enough, then we have to allocate space.
        if (uncompressed_len > output_len) {
            ctx->buffer.reserve(uncompressed_len);
            ctx->buffer_data = ctx->buffer.data();
        } else {
            ctx->buffer_data = (char*)output;
        }
        ok = snappy::RawUncompress((const char*)input, compressed_len, ctx->buffer_data);
        if (!ok) {
            return Status::InternalError("Snappy RawUncompress failed because of data corruption");
        }
        ctx->buffer_size = uncompressed_len;
        ctx->buffer_used = 0;
    }

    output_len = std::min(output_len, ctx->buffer_size - ctx->buffer_used);
    if (ctx->buffer_data != (char*)output) {
        std::memcpy(output, ctx->buffer_data + ctx->buffer_used, output_len);
    }
    ctx->buffer_used += output_len;
    ctx->output_length += output_len;
    *output_bytes_write += output_len;
    if (ctx->output_length == ctx->block_length) {
        ctx->block_length = 0;
        ctx->output_length = 0;
        if (input_len == 0) {
            *stream_end = true;
        }
    }
    return st;
}

class Lz4StreamCompression : public StreamCompression {
public:
    Lz4StreamCompression() : StreamCompression(CompressionTypePB::LZ4) {}
    ~Lz4StreamCompression() override = default;
    std::string debug_info() override { return "Lz4StreamCompression"; }

    Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output, size_t output_len,
                      size_t* output_bytes_write, bool* stream_end) override;

    Status init() override;

private:
    enum class Mode { UNKNOWN, FRAME, BLOCK };

    struct Context {
        uint32_t block_length = 0;
        uint32_t output_length = 0;

        std::vector<char> buffer;
        char* buffer_data;
        size_t buffer_size = 0;
        size_t buffer_used = 0;
    };
    Context _ctx;
    Mode _mode = Mode::UNKNOWN;
    std::unique_ptr<Lz4FrameStreamCompression> _frame;
};

Status Lz4StreamCompression::init() {
    return Status::OK();
}

Status Lz4StreamCompression::decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output,
                                        size_t output_len, size_t* output_bytes_write, bool* stream_end) {
    *input_bytes_read = 0;
    *output_bytes_write = 0;
    *stream_end = false;

    if (_mode == Mode::UNKNOWN) {
        if (input_len < sizeof(uint32_t)) {
            return Status::OK();
        }

        bool is_frame_magic = (decode_fixed32_le(input) == LZ4F_MAGICNUMBER);
        if (!is_frame_magic) {
            _mode = Mode::BLOCK;
        } else {
            if (input_len < LZ4F_HEADER_SIZE_MIN) {
                return Status::OK();
            }
            // Verify the frame header before switching to frame mode to avoid misdetecting block data.
            LZ4F_decompressionContext_t ctx = nullptr;
            size_t ret = LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION);
            if (LZ4F_isError(ret)) {
                return Status::InternalError(
                        strings::Substitute("LZ4F create decompression context failed: $0", LZ4F_getErrorName(ret)));
            }
            LZ4F_frameInfo_t info;
            size_t src_size = input_len;
            ret = LZ4F_getFrameInfo(ctx, &info, input, &src_size);
            LZ4F_freeDecompressionContext(ctx);
            if (LZ4F_isError(ret)) {
                // If the buffer can't cover the full header yet, wait for more data before deciding.
                if (input_len < LZ4F_HEADER_SIZE_MAX) {
                    // Request more bytes to determine if this is a valid LZ4 frame header.
                    set_compressed_block_size(LZ4F_HEADER_SIZE_MAX);
                    return Status::OK();
                }
                _mode = Mode::BLOCK;
            } else {
                _frame = std::make_unique<Lz4FrameStreamCompression>();
                RETURN_IF_ERROR(_frame->init());
                _mode = Mode::FRAME;
            }
        }
    }

    if (_mode == Mode::FRAME) {
        return _frame->decompress(input, input_len, input_bytes_read, output, output_len, output_bytes_write,
                                  stream_end);
    }

    Context* ctx = &_ctx;
    Status st = Status::OK();

    if (ctx->buffer_used == ctx->buffer_size) {
        // Block stream format (big-endian):
        //   [block_len][compressed_len][compressed_payload], block_len=0 terminates.
        if (ctx->block_length == 0) {
            if (input_len < 4) return st;
            ctx->block_length = decode_fixed32_be(input);
            input += 4;
            input_len -= 4;
            *input_bytes_read += 4;
        }
        if (ctx->block_length == 0) {
            *stream_end = true;
            return st;
        }

        if (input_len < 4) return st;
        uint32_t compressed_len = decode_fixed32_be(input);
        input += 4;
        input_len -= 4;
        if (input_len < compressed_len) {
            set_compressed_block_size(compressed_len);
            return st;
        }

        *input_bytes_read += (compressed_len + 4);
        input_len -= compressed_len;

        // When decompressing a new block, output_length must be 0 because it's reset
        // when the previous block completes (see the reset at the end of this function).
        DCHECK_EQ(ctx->output_length, 0u);
        uint32_t expected_output_len = ctx->block_length;

        if (expected_output_len > output_len) {
            ctx->buffer.resize(expected_output_len);
            ctx->buffer_data = ctx->buffer.data();
        } else {
            ctx->buffer_data = (char*)output;
        }

        int decoded = LZ4_decompress_safe(reinterpret_cast<const char*>(input), ctx->buffer_data, compressed_len,
                                          expected_output_len);
        if (decoded < 0) {
            return Status::InternalError("LZ4 RawDecompress failed because of data corruption");
        }
        if (static_cast<uint32_t>(decoded) != expected_output_len) {
            return Status::InternalError("LZ4 RawDecompress returned unexpected output size");
        }
        ctx->buffer_size = decoded;
        ctx->buffer_used = 0;
    }

    output_len = std::min(output_len, ctx->buffer_size - ctx->buffer_used);
    if (ctx->buffer_data != (char*)output) {
        std::memcpy(output, ctx->buffer_data + ctx->buffer_used, output_len);
    }
    ctx->buffer_used += output_len;
    ctx->output_length += output_len;
    *output_bytes_write += output_len;
    if (ctx->output_length == ctx->block_length) {
        ctx->block_length = 0;
        ctx->output_length = 0;
        if (input_len == 0) {
            *stream_end = true;
        }
    }
    return st;
}

class LzoStreamCompression : public StreamCompression {
public:
    LzoStreamCompression() : StreamCompression(CompressionTypePB::LZO) {}

    ~LzoStreamCompression() override = default;

    std::string debug_info() override;

    Status init() override;

    Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output, size_t output_len,
                      size_t* output_bytes_written, bool* stream_end) override;

private:
    static constexpr const char* NAME = "LzoStreamCompression";
    // Lzop
    static constexpr uint8_t LZOP_MAGIC[9] = {0x89, 0x4c, 0x5a, 0x4f, 0x00, 0x0d, 0x0a, 0x1a, 0x0a};

    static constexpr uint64_t LZOP_VERSION = 0x1030;
    static constexpr uint64_t MIN_LZO_VERSION = 0x0100;
    // magic(9) + ver(2) + lib_ver(2) + ver_needed(2) + method(1)
    // + lvl(1) + flags(4) + mode/mtime(12) + filename_len(1)
    // without the real file name, extra field and checksum
    static constexpr uint32_t MIN_HEADER_SIZE = 34;
    static constexpr uint32_t LZO_MAX_BLOCK_SIZE = (64 * 1024l * 1024l);

    static constexpr uint32_t CRC32_INIT_VALUE = 0;
    static constexpr uint32_t ADLER32_INIT_VALUE = 1;

    static constexpr uint64_t F_H_CRC32 = 0x00001000L;
    static constexpr uint64_t F_MASK = 0x00003FFFL;
    static constexpr uint64_t F_OS_MASK = 0xff000000L;
    static constexpr uint64_t F_CS_MASK = 0x00f00000L;
    static constexpr uint64_t F_RESERVED = ((F_MASK | F_OS_MASK | F_CS_MASK) ^ 0xffffffffL);
    static constexpr uint64_t F_MULTIPART = 0x00000400L;
    static constexpr uint64_t F_H_FILTER = 0x00000800L;
    static constexpr uint64_t F_H_EXTRA_FIELD = 0x00000040L;
    static constexpr uint64_t F_CRC32_C = 0x00000200L;
    static constexpr uint64_t F_ADLER32_C = 0x00000002L;
    static constexpr uint64_t F_CRC32_D = 0x00000100L;
    static constexpr uint64_t F_ADLER32_D = 0x00000001L;

    uint8_t* get_uint8(uint8_t* ptr, uint8_t* value) {
        *value = *ptr;
        return ptr + sizeof(uint8_t);
    }

    uint8_t* get_uint16(uint8_t* ptr, uint16_t* value) {
        *value = *ptr << 8 | *(ptr + 1);
        return ptr + sizeof(uint16_t);
    }

    uint8_t* get_uint32(uint8_t* ptr, uint32_t* value) {
        *value = (*ptr << 24) | (*(ptr + 1) << 16) | (*(ptr + 2) << 8) | *(ptr + 3);
        return ptr + sizeof(uint32_t);
    }

    enum ChecksumType { CHECK_NONE, CHECK_CRC32, CHECK_ADLER };

    ChecksumType header_type(int flags) { return (flags & F_H_CRC32) ? CHECK_CRC32 : CHECK_ADLER; }

    ChecksumType input_type(int flags) {
        return (flags & F_CRC32_C) ? CHECK_CRC32 : (flags & F_ADLER32_C) ? CHECK_ADLER : CHECK_NONE;
    }

    ChecksumType output_type(int flags) {
        return (flags & F_CRC32_D) ? CHECK_CRC32 : (flags & F_ADLER32_D) ? CHECK_ADLER : CHECK_NONE;
    }

    Status parse_header(uint8_t* input, size_t input_len, bool* more_data);

    Status verify_checksum(ChecksumType type, const std::string& source, uint32_t expected, uint8_t* ptr, size_t len);

    struct Header {
        uint16_t version;
        uint16_t lib_version;
        uint16_t version_needed;
        uint8_t method;
        std::string filename;
        uint32_t header_size;
        ChecksumType header_checksum_type;
        ChecksumType input_checksum_type;
        ChecksumType output_checksum_type;
    };

    bool _is_header_loaded = false;
    struct Header _header;

    struct Context {
        std::vector<uint8_t> buffer;
        uint8_t* buffer_data;
        size_t buffer_size = 0;
        size_t buffer_used = 0;
    };
    Context _ctx;
};

Status LzoStreamCompression::init() {
    return Status::OK();
}

Status LzoStreamCompression::decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output,
                                        size_t output_len, size_t* output_bytes_write, bool* stream_end) {
    *input_bytes_read = 0;
    *output_bytes_write = 0;
    *stream_end = false;
    Context* ctx = &_ctx;
    Status st = Status::OK();

    if (!_is_header_loaded) {
        // this is the first time to call lzo decompress, parse the header info first
        bool more_data = false;
        RETURN_IF_ERROR(parse_header(input, input_len, &more_data));
        if (more_data) {
            return st;
        }
        *input_bytes_read += _header.header_size;
        input += _header.header_size;
        input_len -= _header.header_size;
    }

    if (ctx->buffer_size == ctx->buffer_used) {
        // read compressed block
        // compressed-block ::=
        //   <uncompressed-size>
        //   <compressed-size>
        //   <uncompressed-checksums>
        //   <compressed-checksums>
        //   <compressed-data>

        // in following we use ptr to trace pointer
        // and ptr-input as input read bytes.
        uint8_t* ptr = input;
        uint32_t uncompressed_size = 0;
        uint32_t compressed_size = 0;
        uint32_t uncompressed_checksum = 0;
        uint32_t compressed_checksum = 0;
        bool do_compressed_check = false;

        // 1. uncompressed-size
        if (input_len < sizeof(uint32_t)) {
            return st;
        }
        ptr = get_uint32(ptr, &uncompressed_size);
        input_len -= sizeof(uint32_t);

        // no data in this block.
        if (uncompressed_size == 0) {
            *input_bytes_read += (ptr - input);
            *stream_end = true;
            return Status::OK();
        }

        // 2. compressed size
        if (input_len < sizeof(uint32_t)) {
            return st;
        }
        ptr = get_uint32(ptr, &compressed_size);
        input_len -= sizeof(uint32_t);

        if (compressed_size > LZO_MAX_BLOCK_SIZE) {
            std::stringstream ss;
            ss << NAME << " lzo block size: " << compressed_size
               << " is greater than LZO_MAX_BLOCK_SIZE: " << LZO_MAX_BLOCK_SIZE;
            return Status::InternalError(ss.str());
        }

        // 3. uncompressed data checksum
        if (_header.output_checksum_type != CHECK_NONE) {
            if (input_len < sizeof(uint32_t)) {
                return st;
            }
            ptr = get_uint32(ptr, &uncompressed_checksum);
            input_len -= sizeof(uint32_t);
        }

        // 4. compressed data checksum
        if (compressed_size < uncompressed_size && _header.input_checksum_type != CHECK_NONE) {
            if (input_len < sizeof(uint32_t)) {
                return st;
            }
            ptr = get_uint32(ptr, &compressed_checksum);
            input_len -= sizeof(uint32_t);
            do_compressed_check = true;
        }

        // 5. checksum compressed data
        if (input_len < compressed_size) {
            set_compressed_block_size(compressed_size);
            return st;
        }
        if (do_compressed_check) {
            RETURN_IF_ERROR(verify_checksum(_header.input_checksum_type, "compressed", compressed_checksum, ptr,
                                            compressed_size));
        }

        // 6. decompress data
        if (uncompressed_size > output_len) {
            ctx->buffer.reserve(uncompressed_size);
            ctx->buffer_data = ctx->buffer.data();
        } else {
            ctx->buffer_data = output;
        }
        ctx->buffer_size = uncompressed_size;
        ctx->buffer_used = 0;

        if (compressed_size == uncompressed_size) {
            memcpy(ctx->buffer_data, ptr, compressed_size);
        } else {
            try {
                (void)orc::lzoDecompress((char*)ptr, (char*)ptr + compressed_size, (char*)ctx->buffer_data,
                                         (char*)ctx->buffer_data + uncompressed_size);
            } catch (const std::runtime_error& e) {
                return Status::InternalError(strings::Substitute("$0 decompress failed", NAME));
            }
        }
        RETURN_IF_ERROR(verify_checksum(_header.output_checksum_type, "decompressed", uncompressed_checksum,
                                        ctx->buffer_data, uncompressed_size));
        ptr += compressed_size;
        *input_bytes_read += (ptr - input);
    }

    output_len = std::min(output_len, ctx->buffer_size - ctx->buffer_used);
    if (ctx->buffer_data != output) {
        std::memcpy(output, ctx->buffer_data + ctx->buffer_used, output_len);
    }
    ctx->buffer_used += output_len;
    *output_bytes_write += output_len;
    return Status::OK();
}

// file-header ::=  -- most of this information is not used.
//   <magic>
//   <version>
//   <lib-version>
//   [<version-needed>] -- present for all modern files.
//   <method>
//   <level>
//   <flags>
//   <mode>
//   <mtime>
//   <file-name>
//   <header-checksum>
//   <extra-field> -- presence indicated in flags, not currently used.
Status LzoStreamCompression::parse_header(uint8_t* input, size_t input_len, bool* more) {
    *more = false;
    uint8_t* ptr = input;

    // =======================================
    if (input_len < MIN_HEADER_SIZE) {
        *more = true;
        return Status::OK();
    }

    // 1. magic
    if (memcmp(ptr, LZOP_MAGIC, sizeof(LZOP_MAGIC))) {
        std::stringstream ss;
        ss << NAME << " invalid lzo magic number";
        return Status::InternalError(ss.str());
    }
    ptr += sizeof(LZOP_MAGIC);
    uint8_t* header = ptr;

    // 2. version
    ptr = get_uint16(ptr, &_header.version);
    if (_header.version > LZOP_VERSION) {
        std::stringstream ss;
        ss << NAME << " compressed with later version of lzop: " << &_header.version
           << " must be less than: " << LZOP_VERSION;
        return Status::InternalError(ss.str());
    }

    // 3. lib version
    ptr = get_uint16(ptr, &_header.lib_version);
    if (_header.lib_version < MIN_LZO_VERSION) {
        std::stringstream ss;
        ss << NAME << " compressed with incompatible lzo version: " << &_header.lib_version
           << "must be at least: " << MIN_LZO_VERSION;
        return Status::InternalError(ss.str());
    }

    // 4. version needed
    ptr = get_uint16(ptr, &_header.version_needed);
    if (_header.version_needed > LZOP_VERSION) {
        std::stringstream ss;
        ss << NAME << " compressed with imp incompatible lzo version: " << &_header.version
           << " must be at no more than: " << LZOP_VERSION;
        return Status::InternalError(ss.str());
    }

    // 5. method
    ptr = get_uint8(ptr, &_header.method);
    if (_header.method < 1 || _header.method > 3) {
        std::stringstream ss;
        ss << NAME << " invalid compression method: " << _header.method;
        return Status::InternalError(ss.str());
    }

    // 6. skip level
    ++ptr;

    // 7. flags
    uint32_t flags;
    ptr = get_uint32(ptr, &flags);
    if (flags & (F_RESERVED | F_MULTIPART | F_H_FILTER)) {
        std::stringstream ss;
        ss << NAME << " unsupported lzo flags: " << flags;
        return Status::InternalError(ss.str());
    }
    _header.header_checksum_type = header_type(flags);
    _header.input_checksum_type = input_type(flags);
    _header.output_checksum_type = output_type(flags);

    // 8. skip mode and mtime
    ptr += 3 * sizeof(int32_t);

    // 9. filename
    uint8_t filename_len;
    ptr = get_uint8(ptr, &filename_len);

    // =============================
    // here we already consume (MIN_HEADER_SIZE)
    // from now we have to check left input is enough for each step

    // filename + checksum(uint32_t)
    size_t left = input_len - (ptr - input);
    if (left < (filename_len + sizeof(uint32_t))) {
        *more = true;
        return Status::OK();
    }

    _header.filename = std::string((char*)ptr, (size_t)filename_len);
    ptr += filename_len;
    left -= filename_len;

    // 10. header checksum
    uint32_t expected_checksum;
    uint8_t* header_end = ptr;
    ptr = get_uint32(ptr, &expected_checksum);
    left -= sizeof(uint32_t);

    // 11. skip extra
    if (flags & F_H_EXTRA_FIELD) {
        if (left < sizeof(uint32_t)) {
            *more = true;
            return Status::OK();
        }
        uint32_t extra_len;
        ptr = get_uint32(ptr, &extra_len);
        left -= sizeof(uint32_t);

        // add the checksum and the len to the total ptr size.
        if (left < sizeof(int32_t) + extra_len) {
            *more = true;
            return Status::OK();
        }
        left -= sizeof(int32_t) + extra_len;
        ptr += sizeof(int32_t) + extra_len;
    }

    // ================================
    RETURN_IF_ERROR(
            verify_checksum(_header.header_checksum_type, "header", expected_checksum, header, header_end - header));
    _header.header_size = ptr - input;
    _is_header_loaded = true;
    return Status::OK();
}

Status LzoStreamCompression::verify_checksum(ChecksumType type, const std::string& source, uint32_t expected,
                                             uint8_t* ptr, size_t len) {
    uint32_t computed_checksum;
    switch (type) {
    case CHECK_NONE:
        return Status::OK();
    case CHECK_CRC32:
        computed_checksum = crc32(CRC32_INIT_VALUE, ptr, len);
        break;
    case CHECK_ADLER:
        computed_checksum = adler32(ADLER32_INIT_VALUE, ptr, len);
        break;
    default:
        std::stringstream ss;
        ss << "Invalid checksum type: " << type;
        return Status::InternalError(ss.str());
    }

    if (computed_checksum != expected) {
        std::stringstream ss;
        ss << NAME << " checksum of " << source << " block failed."
           << " computed checksum: " << computed_checksum << " expected: " << expected;
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

std::string LzoStreamCompression::debug_info() {
    std::stringstream ss;
    ss << "LzoStreamCompression."
       << " header version: " << _header.version << ", lib version: " << _header.lib_version
       << ", version needed: " << _header.version_needed << ", method: " << (uint16_t)_header.method
       << ", header size: " << _header.header_size << ", header checksum type: " << _header.header_checksum_type
       << ", input checksum type: " << _header.input_checksum_type
       << ", output checksum type: " << _header.output_checksum_type;
    return ss.str();
}

class GzipStreamCompressor : public StreamCompressor {
public:
    GzipStreamCompressor(CompressionTypePB type, int window_bits) : StreamCompressor(type), _window_bits(window_bits) {}

    ~GzipStreamCompressor() override {
        if (_initialized && !_finished) {
            (void)deflateEnd(&_z_strm);
        }
    }

    Status compress(const uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output,
                    size_t output_len, size_t* output_bytes_written) override;

    Status finish(uint8_t* output, size_t output_len, size_t* output_bytes_written, bool* stream_end) override;

    size_t max_compressed_len(size_t len) const override {
        DCHECK(_initialized) << "max_compressed_len called before init()";
        return deflateBound(const_cast<z_stream*>(&_z_strm), len);
    }

protected:
    Status init() override;

private:
    bool _initialized = false;
    bool _finished = false;
    z_stream _z_strm = {nullptr};
    int _window_bits;

    static constexpr int kMemLevel = 8;
};

Status GzipStreamCompressor::init() {
    _z_strm.zalloc = Z_NULL;
    _z_strm.zfree = Z_NULL;
    _z_strm.opaque = Z_NULL;
    int ret = deflateInit2(&_z_strm, Z_DEFAULT_COMPRESSION, Z_DEFLATED, _window_bits, kMemLevel, Z_DEFAULT_STRATEGY);
    if (ret != Z_OK) {
        return Status::InternalError(strings::Substitute("Fail to init zlib stream compress, res=$0", ret));
    }
    _initialized = true;
    return Status::OK();
}

Status GzipStreamCompressor::compress(const uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output,
                                      size_t output_len, size_t* output_bytes_written) {
    constexpr size_t kMaxAvail = std::numeric_limits<uInt>::max();
    size_t in_len = std::min(input_len, kMaxAvail);
    size_t out_len = std::min(output_len, kMaxAvail);
    *input_bytes_read = 0;
    *output_bytes_written = 0;
    _z_strm.next_in = const_cast<uint8_t*>(input);
    _z_strm.avail_in = static_cast<uInt>(in_len);
    _z_strm.next_out = output;
    _z_strm.avail_out = static_cast<uInt>(out_len);
    int ret = deflate(&_z_strm, Z_NO_FLUSH);
    *input_bytes_read = in_len - _z_strm.avail_in;
    *output_bytes_written = out_len - _z_strm.avail_out;
    if (ret != Z_OK && ret != Z_BUF_ERROR) {
        return Status::InternalError(strings::Substitute("Fail to do zlib stream compress, res=$0", ret));
    }
    return Status::OK();
}

Status GzipStreamCompressor::finish(uint8_t* output, size_t output_len, size_t* output_bytes_written,
                                    bool* stream_end) {
    constexpr size_t kMaxAvail = std::numeric_limits<uInt>::max();
    size_t out_len = std::min(output_len, kMaxAvail);
    *output_bytes_written = 0;
    *stream_end = false;
    if (_finished) {
        *stream_end = true;
        return Status::OK();
    }
    _z_strm.next_in = nullptr;
    _z_strm.avail_in = 0;
    _z_strm.next_out = output;
    _z_strm.avail_out = static_cast<uInt>(out_len);
    int ret = deflate(&_z_strm, Z_FINISH);
    *output_bytes_written = out_len - _z_strm.avail_out;
    if (ret == Z_STREAM_END) {
        *stream_end = true;
        _finished = true;
        (void)deflateEnd(&_z_strm);
        return Status::OK();
    }
    if (ret != Z_OK && ret != Z_BUF_ERROR) {
        return Status::InternalError(strings::Substitute("Fail to finish zlib stream compress, res=$0", ret));
    }
    return Status::OK();
}

class Bzip2StreamCompressor : public StreamCompressor {
public:
    Bzip2StreamCompressor() : StreamCompressor(CompressionTypePB::BZIP2) {}
    ~Bzip2StreamCompressor() override {
        if (_initialized && !_finished) {
            (void)BZ2_bzCompressEnd(&_bz_strm);
        }
    }

    Status compress(const uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output,
                    size_t output_len, size_t* output_bytes_written) override;

    Status finish(uint8_t* output, size_t output_len, size_t* output_bytes_written, bool* stream_end) override;

    // Bzip2 worst-case expansion: 1% + 600 bytes (from bzip2 documentation).
    size_t max_compressed_len(size_t len) const override { return len + (len / 100 + 600); }

protected:
    Status init() override;

private:
    bool _initialized = false;
    bool _finished = false;
    bz_stream _bz_strm = {nullptr};
};

Status Bzip2StreamCompressor::init() {
    _bz_strm.bzalloc = nullptr;
    _bz_strm.bzfree = nullptr;
    _bz_strm.opaque = nullptr;
    int ret = BZ2_bzCompressInit(&_bz_strm, 9, 0, 30);
    if (ret != BZ_OK) {
        return Status::InternalError(strings::Substitute("Fail to init bzip2 stream compress, res=$0", ret));
    }
    _initialized = true;
    return Status::OK();
}

Status Bzip2StreamCompressor::compress(const uint8_t* input, size_t input_len, size_t* input_bytes_read,
                                       uint8_t* output, size_t output_len, size_t* output_bytes_written) {
    constexpr size_t kMaxAvail = std::numeric_limits<unsigned int>::max();
    size_t in_len = std::min(input_len, kMaxAvail);
    size_t out_len = std::min(output_len, kMaxAvail);
    *input_bytes_read = 0;
    *output_bytes_written = 0;
    _bz_strm.next_in = const_cast<char*>(reinterpret_cast<const char*>(input));
    _bz_strm.avail_in = static_cast<unsigned int>(in_len);
    _bz_strm.next_out = reinterpret_cast<char*>(output);
    _bz_strm.avail_out = static_cast<unsigned int>(out_len);
    int ret = BZ2_bzCompress(&_bz_strm, BZ_RUN);
    *input_bytes_read = in_len - _bz_strm.avail_in;
    *output_bytes_written = out_len - _bz_strm.avail_out;
    if (ret != BZ_RUN_OK) {
        return Status::InternalError(strings::Substitute("Fail to do bzip2 stream compress, res=$0", ret));
    }
    return Status::OK();
}

Status Bzip2StreamCompressor::finish(uint8_t* output, size_t output_len, size_t* output_bytes_written,
                                     bool* stream_end) {
    constexpr size_t kMaxAvail = std::numeric_limits<unsigned int>::max();
    size_t out_len = std::min(output_len, kMaxAvail);
    *output_bytes_written = 0;
    *stream_end = false;
    if (_finished) {
        *stream_end = true;
        return Status::OK();
    }
    _bz_strm.next_in = nullptr;
    _bz_strm.avail_in = 0;
    _bz_strm.next_out = reinterpret_cast<char*>(output);
    _bz_strm.avail_out = static_cast<unsigned int>(out_len);
    int ret = BZ2_bzCompress(&_bz_strm, BZ_FINISH);
    *output_bytes_written = out_len - _bz_strm.avail_out;
    if (ret == BZ_STREAM_END) {
        *stream_end = true;
        _finished = true;
        (void)BZ2_bzCompressEnd(&_bz_strm);
        return Status::OK();
    }
    if (ret != BZ_FINISH_OK) {
        return Status::InternalError(strings::Substitute("Fail to finish bzip2 stream compress, res=$0", ret));
    }
    return Status::OK();
}

class ZstdStreamCompressor : public StreamCompressor {
public:
    ZstdStreamCompressor() : StreamCompressor(CompressionTypePB::ZSTD) {}
    ~ZstdStreamCompressor() override = default;

    Status compress(const uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output,
                    size_t output_len, size_t* output_bytes_written) override;

    Status finish(uint8_t* output, size_t output_len, size_t* output_bytes_written, bool* stream_end) override;

    size_t max_compressed_len(size_t len) const override { return ZSTD_compressBound(len); }

protected:
    Status init() override;

private:
    std::optional<compression::ZSTD_CCtx_Pool::Ref> _ref;
    ZSTD_CCtx* _ctx = nullptr;
};

Status ZstdStreamCompressor::init() {
    auto maybe_ref = compression::getZSTD_CCtx();
    if (!maybe_ref.ok()) {
        return maybe_ref.status();
    }
    _ref.emplace(std::move(maybe_ref.value()));
    _ctx = _ref->get()->ctx;
    size_t ret = ZSTD_initCStream(_ctx, ZSTD_CLEVEL_DEFAULT);
    if (ZSTD_isError(ret)) {
        return Status::InternalError(
                strings::Substitute("ZSTD init stream compress failed: $0", ZSTD_getErrorName(ret)));
    }
    return Status::OK();
}

Status ZstdStreamCompressor::compress(const uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output,
                                      size_t output_len, size_t* output_bytes_written) {
    ZSTD_inBuffer in_buf = {input, input_len, 0};
    ZSTD_outBuffer out_buf = {output, output_len, 0};
    size_t ret = ZSTD_compressStream(_ctx, &out_buf, &in_buf);
    if (ZSTD_isError(ret)) {
        return Status::InternalError(strings::Substitute("ZSTD compress failed: $0", ZSTD_getErrorName(ret)));
    }
    *input_bytes_read = in_buf.pos;
    *output_bytes_written = out_buf.pos;
    return Status::OK();
}

Status ZstdStreamCompressor::finish(uint8_t* output, size_t output_len, size_t* output_bytes_written,
                                    bool* stream_end) {
    ZSTD_outBuffer out_buf = {output, output_len, 0};
    size_t ret = ZSTD_endStream(_ctx, &out_buf);
    if (ZSTD_isError(ret)) {
        return Status::InternalError(strings::Substitute("ZSTD finish failed: $0", ZSTD_getErrorName(ret)));
    }
    *output_bytes_written = out_buf.pos;
    *stream_end = (ret == 0);
    return Status::OK();
}

class Lz4FrameStreamCompressor : public StreamCompressor {
public:
    Lz4FrameStreamCompressor() : StreamCompressor(CompressionTypePB::LZ4_FRAME) {}
    ~Lz4FrameStreamCompressor() override {
        if (_ctx != nullptr) {
            LZ4F_freeCompressionContext(_ctx);
        }
    }

    Status compress(const uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output,
                    size_t output_len, size_t* output_bytes_written) override;

    Status finish(uint8_t* output, size_t output_len, size_t* output_bytes_written, bool* stream_end) override;

    size_t max_compressed_len(size_t len) const override {
        return LZ4F_compressBound(len, &_prefs) + LZ4F_HEADER_SIZE_MAX;
    }

protected:
    Status init() override;

private:
    LZ4F_cctx* _ctx = nullptr;
    LZ4F_preferences_t _prefs = {};
    bool _started = false;
};

Status Lz4FrameStreamCompressor::init() {
    size_t ret = LZ4F_createCompressionContext(&_ctx, LZ4F_VERSION);
    if (LZ4F_isError(ret)) {
        return Status::InternalError(
                strings::Substitute("LZ4F create compression context failed: $0", LZ4F_getErrorName(ret)));
    }
    return Status::OK();
}

Status Lz4FrameStreamCompressor::compress(const uint8_t* input, size_t input_len, size_t* input_bytes_read,
                                          uint8_t* output, size_t output_len, size_t* output_bytes_written) {
    *input_bytes_read = 0;
    *output_bytes_written = 0;
    size_t offset = 0;
    if (!_started) {
        size_t header_size = LZ4F_compressBegin(_ctx, output, output_len, &_prefs);
        if (LZ4F_isError(header_size)) {
            return Status::InternalError(
                    strings::Substitute("LZ4F compress begin failed: $0", LZ4F_getErrorName(header_size)));
        }
        offset += header_size;
        _started = true;
    }

    size_t ret = LZ4F_compressUpdate(_ctx, output + offset, output_len - offset, input, input_len, nullptr);
    if (LZ4F_isError(ret)) {
        return Status::InternalError(strings::Substitute("LZ4F compress update failed: $0", LZ4F_getErrorName(ret)));
    }
    *output_bytes_written = offset + ret;
    *input_bytes_read = input_len;
    return Status::OK();
}

Status Lz4FrameStreamCompressor::finish(uint8_t* output, size_t output_len, size_t* output_bytes_written,
                                        bool* stream_end) {
    *output_bytes_written = 0;
    *stream_end = false;
    size_t offset = 0;
    if (!_started) {
        size_t header_size = LZ4F_compressBegin(_ctx, output, output_len, &_prefs);
        if (LZ4F_isError(header_size)) {
            return Status::InternalError(
                    strings::Substitute("LZ4F compress begin failed: $0", LZ4F_getErrorName(header_size)));
        }
        offset += header_size;
        _started = true;
    }
    size_t ret = LZ4F_compressEnd(_ctx, output + offset, output_len - offset, nullptr);
    if (LZ4F_isError(ret)) {
        return Status::InternalError(strings::Substitute("LZ4F compress end failed: $0", LZ4F_getErrorName(ret)));
    }
    *output_bytes_written = offset + ret;
    *stream_end = true;
    return Status::OK();
}

Status StreamCompression::create_decompressor(CompressionTypePB type,
                                              std::unique_ptr<StreamCompression>* decompressor) {
    switch (type) {
    case CompressionTypePB::NO_COMPRESSION:
        *decompressor = nullptr;
        break;
    case CompressionTypePB::SNAPPY:
        *decompressor = std::make_unique<SnappyStreamCompression>();
        break;
    case CompressionTypePB::GZIP:
        *decompressor = std::make_unique<GzipStreamCompression>(false);
        break;
    case CompressionTypePB::DEFLATE:
        *decompressor = std::make_unique<GzipStreamCompression>(true);
        break;
    case CompressionTypePB::ZLIB:
        *decompressor = std::make_unique<GzipStreamCompression>(false);
        break;
    case CompressionTypePB::BZIP2:
        *decompressor = std::make_unique<Bzip2StreamCompression>();
        break;
    case CompressionTypePB::LZ4_FRAME:
        *decompressor = std::make_unique<Lz4FrameStreamCompression>();
        break;
    case CompressionTypePB::LZ4:
        *decompressor = std::make_unique<Lz4StreamCompression>();
        break;
    case CompressionTypePB::ZSTD:
        *decompressor = std::make_unique<ZstandardStreamCompression>();
        break;
    case CompressionTypePB::LZO:
        *decompressor = std::make_unique<LzoStreamCompression>();
        break;
    default:
        return Status::InternalError(fmt::format("Unknown compress type: {}", type));
    }

    Status st = Status::OK();
    if (*decompressor != nullptr) {
        st = (*decompressor)->init();
    }

    return st;
}

Status StreamCompressor::create_compressor(CompressionTypePB type, std::unique_ptr<StreamCompressor>* compressor) {
    switch (type) {
    case CompressionTypePB::GZIP:
        *compressor = std::make_unique<GzipStreamCompressor>(CompressionTypePB::GZIP, MAX_WBITS + 16);
        break;
    case CompressionTypePB::DEFLATE:
        // Use zlib-wrapped deflate to match the existing DEFLATE decompressor behavior.
        *compressor = std::make_unique<GzipStreamCompressor>(CompressionTypePB::DEFLATE, MAX_WBITS);
        break;
    case CompressionTypePB::ZLIB:
        *compressor = std::make_unique<GzipStreamCompressor>(CompressionTypePB::ZLIB, MAX_WBITS);
        break;
    case CompressionTypePB::BZIP2:
        *compressor = std::make_unique<Bzip2StreamCompressor>();
        break;
    case CompressionTypePB::LZ4_FRAME:
        *compressor = std::make_unique<Lz4FrameStreamCompressor>();
        break;
    case CompressionTypePB::ZSTD:
        *compressor = std::make_unique<ZstdStreamCompressor>();
        break;
    default:
        return Status::InternalError(fmt::format("Unknown compress type: {}", type));
    }

    Status st = Status::OK();
    if (*compressor != nullptr) {
        st = (*compressor)->init();
    }
    return st;
}

} // namespace starrocks
