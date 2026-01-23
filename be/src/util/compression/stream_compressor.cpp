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

#include "util/compression/stream_compressor.h"

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

namespace starrocks {

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
    if (ret != Z_OK && ret != Z_BUF_ERROR) {
        return Status::InternalError(strings::Substitute("Fail to do zlib stream compress, res=$0", ret));
    }
    *input_bytes_read = in_len - _z_strm.avail_in;
    *output_bytes_written = out_len - _z_strm.avail_out;
    return Status::OK();
}

Status GzipStreamCompressor::finish(uint8_t* output, size_t output_len, size_t* output_bytes_written,
                                    bool* stream_end) {
    // Check re-enter immediately - if already finished, return as no-op
    if (_finished) {
        *output_bytes_written = 0;
        *stream_end = true;
        return Status::OK();
    }
    constexpr size_t kMaxAvail = std::numeric_limits<uInt>::max();
    size_t out_len = std::min(output_len, kMaxAvail);
    *output_bytes_written = 0;
    *stream_end = false;
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

StatusOr<std::unique_ptr<StreamCompressor>> StreamCompressor::create_compressor(CompressionTypePB type) {
    std::unique_ptr<StreamCompressor> compressor;
    switch (type) {
    case CompressionTypePB::GZIP:
        compressor = std::make_unique<GzipStreamCompressor>(CompressionTypePB::GZIP, MAX_WBITS + 16);
        break;
    case CompressionTypePB::DEFLATE:
        // Use zlib-wrapped deflate to match the existing DEFLATE decompressor behavior.
        compressor = std::make_unique<GzipStreamCompressor>(CompressionTypePB::DEFLATE, MAX_WBITS);
        break;
    case CompressionTypePB::ZLIB:
        compressor = std::make_unique<GzipStreamCompressor>(CompressionTypePB::ZLIB, MAX_WBITS);
        break;
    case CompressionTypePB::BZIP2:
        compressor = std::make_unique<Bzip2StreamCompressor>();
        break;
    case CompressionTypePB::LZ4_FRAME:
        compressor = std::make_unique<Lz4FrameStreamCompressor>();
        break;
    case CompressionTypePB::ZSTD:
        compressor = std::make_unique<ZstdStreamCompressor>();
        break;
    default:
        return Status::InternalError(fmt::format("Unknown compress type: {}", type));
    }

    RETURN_IF_ERROR(compressor->init());
    return compressor;
}

} // namespace starrocks
