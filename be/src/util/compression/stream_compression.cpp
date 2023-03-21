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
#include <lz4/lz4frame.h>
#include <zlib.h>
#include <zstd/zstd.h>
#include <zstd/zstd_errors.h>

#include <memory>

#include "fmt/compile.h"
#include "gutil/strings/substitute.h"
#include "util/compression/compression_context_pool_singletons.h"

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

// lzo
class LzoStreamCompression : public StreamCompression {
public:
    LzoStreamCompression()
            : StreamCompression(CompressionTypePB::LZO) {}

    ~LzoStreamCompression() override {}

    std::string debug_info() override {
        std::stringstream ss;
        ss << "LzoStreamCompression.";
        return ss.str();
    }

    Status init() override;

    Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output, size_t output_len,
                      size_t* output_bytes_written, bool* stream_end) override;

private:
    const int32_t DEC_32_TABLE[8] = {4, 1, 2, 1, 4, 4, 4, 4};
    const int32_t DEC_64_TABLE[8] = {0, 0, 0, -1, 0, 1, 2, 3};

    const int32_t SIZE_OF_SHORT = 2;
    const int32_t SIZE_OF_INT = 4;
    const static int32_t SIZE_OF_LONG = 8;

    std::string toHex(uint64_t val) {
        std::ostringstream out;
        out << "0x" << std::hex << val;
        return out.str();
    }
};

Status LzoStreamCompression::init() {
    return Status::OK();
}

#define COPY_VALUE(dst, src, type) memcpy(dst, src, sizeof(type))

Status LzoStreamCompression::decompress(uint8_t* inputAddress, size_t input_len, size_t* input_bytes_read,
                                        uint8_t* outputAddress, size_t output_len, size_t* output_bytes_written,
                                        bool* stream_end) {
    *input_bytes_read = 0;
    *output_bytes_written = 0;
    *stream_end = false;

    uint8_t* inputLimit = inputAddress + input_len;
    uint8_t* outputLimit = outputAddress + output_len;

    // nothing compresses to nothing
    if (inputAddress == inputLimit) {
        return Status::OK();
    }

    // maximum offset in buffers to which it's safe to write long-at-a-time
    uint8_t* const fastOutputLimit = outputLimit - SIZE_OF_LONG;

    // LZO can concat two blocks together so, decode until the input data is
    // consumed
    const uint8_t* input = inputAddress;
    uint8_t* output = outputAddress;
    while (input < inputLimit) {
        //
        // Note: For safety some of the code below may stop decoding early or
        // skip decoding, because input is not available.  This makes the code
        // safe, and since LZO requires an explicit "stop" command, the decoder
        // will still throw a exception.
        //

        bool firstCommand = true;
        uint32_t lastLiteralLength = 0;
        while (true) {
            if (input >= inputLimit) {
                std::stringstream ss;
                ss << "LzoStreamCompression error: ";
                return Status::InternalError(ss.str());
            }
            uint32_t command = *(input++) & 0xFF;
            if (command == 0x11) {
                break;
            }

            // Commands are described using a bit pattern notation:
            // 0: bit is not set
            // 1: bit is set
            // L: part of literal length
            // P: part of match offset position
            // M: part of match length
            // ?: see documentation in command decoder

            int32_t matchLength;
            int32_t matchOffset;
            uint32_t literalLength;
            if ((command & 0xf0) == 0) {
                if (lastLiteralLength == 0) {
                    // 0b0000_LLLL (0bLLLL_LLLL)*

                    // copy length :: fixed
                    //   0
                    matchOffset = 0;

                    // copy offset :: fixed
                    //   0
                    matchLength = 0;

                    // literal length - 3 :: variable bits :: valid range [4..]
                    //   3 + variableLength(command bits [0..3], 4)
                    literalLength = command & 0xf;
                    if (literalLength == 0) {
                        literalLength = 0xf;

                        uint32_t nextByte = 0;
                        while (input < inputLimit && (nextByte = *(input++) & 0xFF) == 0) {
                            literalLength += 0xff;
                        }
                        literalLength += nextByte;
                    }
                    literalLength += 3;
                } else if (lastLiteralLength <= 3) {
                    // 0b0000_PPLL 0bPPPP_PPPP

                    // copy length: fixed
                    //   3
                    matchLength = 3;

                    // copy offset :: 12 bits :: valid range [2048..3071]
                    //   [0..1] from command [2..3]
                    //   [2..9] from trailer [0..7]
                    //   [10] unset
                    //   [11] set
                    if (input >= inputLimit) {
                        std::stringstream ss;
                        ss << "LzoStreamCompression error: ";
                        return Status::InternalError(ss.str());
                    }
                    matchOffset = (command & 0xc) >> 2;
                    matchOffset |= (*(input++) & 0xFF) << 2;
                    matchOffset |= 0x800;

                    // literal length :: 2 bits :: valid range [0..3]
                    //   [0..1] from command [0..1]
                    literalLength = (command & 0x3);
                } else {
                    // 0b0000_PPLL 0bPPPP_PPPP

                    // copy length :: fixed
                    //   2
                    matchLength = 2;

                    // copy offset :: 10 bits :: valid range [0..1023]
                    //   [0..1] from command [2..3]
                    //   [2..9] from trailer [0..7]
                    if (input >= inputLimit) {
                        std::stringstream ss;
                        ss << "LzoStreamCompression error: ";
                        return Status::InternalError(ss.str());
                    }
                    matchOffset = (command & 0xc) >> 2;
                    matchOffset |= (*(input++) & 0xFF) << 2;

                    // literal length :: 2 bits :: valid range [0..3]
                    //   [0..1] from command [0..1]
                    literalLength = (command & 0x3);
                }
            } else if (firstCommand) {
                // first command has special handling when high nibble is set
                matchLength = 0;
                matchOffset = 0;
                literalLength = command - 17;
            } else if ((command & 0xf0) == 0x10) {
                // 0b0001_?MMM (0bMMMM_MMMM)* 0bPPPP_PPPP_PPPP_PPLL

                // copy length - 2 :: variable bits :: valid range [3..]
                //   2 + variableLength(command bits [0..2], 3)
                matchLength = command & 0x7;
                if (matchLength == 0) {
                    matchLength = 0x7;

                    int32_t nextByte = 0;
                    while (input < inputLimit && (nextByte = *(input++) & 0xFF) == 0) {
                        matchLength += 0xff;
                    }
                    matchLength += nextByte;
                }
                matchLength += 2;

                // read trailer
                if (input + SIZE_OF_SHORT > inputLimit) {
                    std::stringstream ss;
                    ss << "LzoStreamCompression error: ";
                    return Status::InternalError(ss.str());
                }
                uint32_t trailer = *reinterpret_cast<const uint16_t*>(input) & 0xFFFF;
                input += SIZE_OF_SHORT;

                // copy offset :: 16 bits :: valid range [32767..49151]
                //   [0..13] from trailer [2..15]
                //   [14] if command bit [3] unset
                //   [15] if command bit [3] set
                matchOffset = trailer >> 2;
                if ((command & 0x8) == 0) {
                    matchOffset |= 0x4000;
                } else {
                    matchOffset |= 0x8000;
                }
                matchOffset--;

                // literal length :: 2 bits :: valid range [0..3]
                //   [0..1] from trailer [0..1]
                literalLength = trailer & 0x3;
            } else if ((command & 0xe0) == 0x20) {
                // 0b001M_MMMM (0bMMMM_MMMM)* 0bPPPP_PPPP_PPPP_PPLL

                // copy length - 2 :: variable bits :: valid range [3..]
                //   2 + variableLength(command bits [0..4], 5)
                matchLength = command & 0x1f;
                if (matchLength == 0) {
                    matchLength = 0x1f;

                    int nextByte = 0;
                    while (input < inputLimit && (nextByte = *(input++) & 0xFF) == 0) {
                        matchLength += 0xff;
                    }
                    matchLength += nextByte;
                }
                matchLength += 2;

                // read trailer
                if (input + SIZE_OF_SHORT > inputLimit) {
                    std::stringstream ss;
                    ss << "LzoStreamCompression error: ";
                    return Status::InternalError(ss.str());
                }
                int32_t trailer = *reinterpret_cast<const int16_t*>(input) & 0xFFFF;
                input += SIZE_OF_SHORT;

                // copy offset :: 14 bits :: valid range [0..16383]
                //  [0..13] from trailer [2..15]
                matchOffset = trailer >> 2;

                // literal length :: 2 bits :: valid range [0..3]
                //   [0..1] from trailer [0..1]
                literalLength = trailer & 0x3;
            } else if ((command & 0xc0) != 0) {
                // 0bMMMP_PPLL 0bPPPP_PPPP

                // copy length - 1 :: 3 bits :: valid range [1..8]
                //   [0..2] from command [5..7]
                //   add 1
                matchLength = (command & 0xe0) >> 5;
                matchLength += 1;

                // copy offset :: 11 bits :: valid range [0..4095]
                //   [0..2] from command [2..4]
                //   [3..10] from trailer [0..7]
                if (input >= inputLimit) {
                    std::stringstream ss;
                    ss << "LzoStreamCompression error: ";
                    return Status::InternalError(ss.str());
                }
                matchOffset = (command & 0x1c) >> 2;
                matchOffset |= (*(input++) & 0xFF) << 3;

                // literal length :: 2 bits :: valid range [0..3]
                //   [0..1] from command [0..1]
                literalLength = (command & 0x3);
            } else {
                std::stringstream ss;
                ss << "LzoStreamCompression error: ";
                return Status::InternalError(ss.str());
            }
            firstCommand = false;

            // copy match
            if (matchLength != 0) {
                // lzo encodes match offset minus one
                matchOffset++;

                uint8_t* matchAddress = output - matchOffset;
                if (matchAddress < outputAddress || output + matchLength > outputLimit) {
                    std::stringstream ss;
                    ss << "LzoStreamCompression error: ";
                    return Status::InternalError(ss.str());
                }
                uint8_t* matchOutputLimit = output + matchLength;

                if (output > fastOutputLimit) {
                    // slow match copy
                    while (output < matchOutputLimit) {
                        *(output++) = *(matchAddress++);
                    }
                } else {
                    // copy repeated sequence
                    if (matchOffset < SIZE_OF_LONG) {
                        // 8 bytes apart so that we can copy long-at-a-time below
                        int32_t increment32 = DEC_32_TABLE[matchOffset];
                        int32_t decrement64 = DEC_64_TABLE[matchOffset];

                        output[0] = *matchAddress;
                        output[1] = *(matchAddress + 1);
                        output[2] = *(matchAddress + 2);
                        output[3] = *(matchAddress + 3);
                        output += SIZE_OF_INT;
                        matchAddress += increment32;
                        COPY_VALUE(output, matchAddress, int32_t);
                        output += SIZE_OF_INT;
                        matchAddress -= decrement64;
                    } else {
                        COPY_VALUE(output, matchAddress, int64_t);
                        matchAddress += SIZE_OF_LONG;
                        output += SIZE_OF_LONG;
                    }

                    if (matchOutputLimit >= fastOutputLimit) {
                        if (matchOutputLimit > outputLimit) {
                            std::stringstream ss;
                            ss << "LzoStreamCompression error: ";
                            return Status::InternalError(ss.str());
                        }

                        while (output < fastOutputLimit) {
                            COPY_VALUE(output, matchAddress, int64_t);
                            matchAddress += SIZE_OF_LONG;
                            output += SIZE_OF_LONG;
                        }

                        while (output < matchOutputLimit) {
                            *(output++) = *(matchAddress++);
                        }
                    } else {
                        while (output < matchOutputLimit) {
                            COPY_VALUE(output, matchAddress, int64_t);
                            matchAddress += SIZE_OF_LONG;
                            output += SIZE_OF_LONG;
                        }
                    }
                }
                output = matchOutputLimit; // correction in case we over-copied
            }

            // copy literal
            uint8_t* literalOutputLimit = output + literalLength;
            if (literalOutputLimit > fastOutputLimit || input + literalLength > inputLimit - SIZE_OF_LONG) {
                if (literalOutputLimit > outputLimit) {
                    std::stringstream ss;
                    ss << "LzoStreamCompression error: ";
                    return Status::InternalError(ss.str());
                }

                // slow, precise copy
                memcpy(output, input, literalLength);
                input += literalLength;
                output += literalLength;
            } else {
                // fast copy. We may over-copy but there's enough room in input
                // and output to not overrun them
                do {
                    COPY_VALUE(output, input, int64_t);
                    input += SIZE_OF_LONG;
                    output += SIZE_OF_LONG;
                } while (output < literalOutputLimit);
                // adjust index if we over-copied
                input -= (output - literalOutputLimit);
                output = literalOutputLimit;
            }
            lastLiteralLength = literalLength;
        }

        if (input + SIZE_OF_SHORT > inputLimit && *reinterpret_cast<const int16_t*>(input) != 0) {
            std::stringstream ss;
            ss << "LzoStreamCompression error: ";
            return Status::InternalError(ss.str());
        }
        input += SIZE_OF_SHORT;
    }

    *input_bytes_read = input - inputAddress;
    *output_bytes_written = output - outputAddress;
    *stream_end = true;

    return Status::OK();
}


Status StreamCompression::create_decompressor(CompressionTypePB type,
                                              std::unique_ptr<StreamCompression>* decompressor) {
    switch (type) {
    case CompressionTypePB::NO_COMPRESSION:
        *decompressor = nullptr;
        break;
    case CompressionTypePB::GZIP:
        *decompressor = std::make_unique<GzipStreamCompression>(false);
        break;
    case CompressionTypePB::DEFLATE:
        *decompressor = std::make_unique<GzipStreamCompression>(true);
        break;
    case CompressionTypePB::BZIP2:
        *decompressor = std::make_unique<Bzip2StreamCompression>();
        break;
    case CompressionTypePB::LZ4_FRAME:
        *decompressor = std::make_unique<Lz4FrameStreamCompression>();
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

} // namespace starrocks
