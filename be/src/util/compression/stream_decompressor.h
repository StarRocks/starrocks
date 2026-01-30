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

#include <memory>

#include "common/status.h"
#include "common/statusor.h"
#include "gen_cpp/types.pb.h"

namespace starrocks {

class StreamDecompressor {
public:
    virtual ~StreamDecompressor() = default;

    // implement in derived class
    // input(in):               buf where decompress begin
    // input_len(in):           max length of input buf
    // input_bytes_read(out):   bytes which is consumed by decompressor
    // output(in):              buf where to save decompressed data
    // output_len(in):          max length of output buf
    // output_bytes_written(out):   decompressed data size in output buf
    // stream_end(out):         true if reach the end of stream,
    //                          or normally finished decompressing entire block
    //
    // input and output buf should be allocated and released outside
    virtual Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output,
                              size_t output_len, size_t* output_bytes_written, bool* stream_end) = 0;

public:
    static StatusOr<std::unique_ptr<StreamDecompressor>> create_decompressor(CompressionTypePB type);

    virtual std::string debug_info() { return "StreamDecompressor"; }

    CompressionTypePB get_type() { return _ctype; }

    size_t get_compressed_block_size() { return _compressed_block_size; }
    size_t set_compressed_block_size(size_t size) { return _compressed_block_size = size; }

protected:
    virtual Status init() { return Status::OK(); }

    explicit StreamDecompressor(CompressionTypePB ctype) : _ctype(ctype) {}

    CompressionTypePB _ctype;

    size_t _compressed_block_size = 0;
};

} // namespace starrocks
