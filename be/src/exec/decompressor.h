// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/decompressor.h

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

#include <bzlib.h>
#include <lz4/lz4frame.h>
#include <zlib.h>
#include <zstd/zstd.h>
#include <zstd/zstd_errors.h>

#include "common/status.h"
#include "gen_cpp/types.pb.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

class Decompressor {
public:
    virtual ~Decompressor();

    // implement in derived class
    // input(in):               buf where decompress begin
    // input_len(in):           max length of input buf
    // input_bytes_read(out):   bytes which is consumed by decompressor
    // output(out):             buf where to save decompressed data
    // output_len(in):      max length of output buf
    // output_bytes_written(out):   decompressed data size in output buf
    // stream_end(out):         true if reach the and of stream,
    //                          or normally finished decompressing entire block
    //
    // input and output buf should be allocated and released outside
    virtual Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output,
                              size_t output_len, size_t* output_bytes_written, bool* stream_end) = 0;

public:
    static Status create_decompressor(CompressionTypePB type, Decompressor** decompressor);

    virtual std::string debug_info();

    CompressionTypePB get_type() { return _ctype; }

protected:
    virtual Status init() { return Status::OK(); }

    Decompressor(CompressionTypePB ctype) : _ctype(ctype) {}

    CompressionTypePB _ctype;
};

class GzipDecompressor : public Decompressor {
public:
    ~GzipDecompressor() override;

    Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output, size_t output_len,
                      size_t* output_bytes_written, bool* stream_end) override;

    std::string debug_info() override;

private:
    friend class Decompressor;
    GzipDecompressor(bool is_deflate);
    Status init() override;

private:
    bool _is_deflate;

    z_stream _z_strm;

    // These are magic numbers from zlib.h.  Not clear why they are not defined there.
    const static int WINDOW_BITS = 15;  // Maximum window size
    const static int DETECT_CODEC = 32; // Determine if this is libz or gzip from header.
};

class Bzip2Decompressor : public Decompressor {
public:
    ~Bzip2Decompressor() override;

    Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output, size_t output_len,
                      size_t* output_bytes_written, bool* stream_end) override;

    std::string debug_info() override;

private:
    friend class Decompressor;
    Bzip2Decompressor() : Decompressor(CompressionTypePB::BZIP2) {}
    Status init() override;

private:
    bz_stream _bz_strm;
};

class Lz4FrameDecompressor : public Decompressor {
public:
    ~Lz4FrameDecompressor() override;

    Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output, size_t output_len,
                      size_t* output_bytes_written, bool* stream_end) override;

    std::string debug_info() override;

private:
    friend class Decompressor;
    Lz4FrameDecompressor() : Decompressor(CompressionTypePB::LZ4_FRAME) {}
    Status init() override;

    size_t get_block_size(const LZ4F_frameInfo_t* info);

private:
    LZ4F_dctx* _dctx;
    size_t _expect_dec_buf_size;
    const static unsigned STARROCKS_LZ4F_VERSION;
};

/// Zstandard is a real-time compression algorithm, providing high compression ratios.
/// It offers a very wide range of compression/speed trade-off.
class ZstandardDecompressor : public Decompressor {
public:
    ~ZstandardDecompressor() override;

    Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output, size_t output_len,
                      size_t* output_bytes_write, bool* stream_end) override;

    std::string debug_info() override;

private:
    friend class Decompressor;
    ZstandardDecompressor() : Decompressor(CompressionTypePB::ZSTD) {}

    // Allocate one context per thread, and re-use for many time decompression.
    ZSTD_DCtx* _stream = nullptr;
};

} // namespace starrocks
