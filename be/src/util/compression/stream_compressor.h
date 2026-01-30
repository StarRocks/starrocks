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

class StreamCompressor {
public:
    virtual ~StreamCompressor() = default;

    // input(in):               buf where compress begin
    // input_len(in):           max length of input buf
    // input_bytes_read(out):   bytes which is consumed by compressor
    // output(in):              buf where to save compressed data
    // output_len(in):          max length of output buf
    // output_bytes_written(out): compressed data size in output buf
    virtual Status compress(const uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output,
                            size_t output_len, size_t* output_bytes_written) = 0;

    // Finish the compression stream and write any final bytes.
    // stream_end(out): true if the stream is fully finished.
    virtual Status finish(uint8_t* output, size_t output_len, size_t* output_bytes_written, bool* stream_end) = 0;

    // Returns an upper bound on the max compressed length for input length.
    virtual size_t max_compressed_len(size_t len) const = 0;

public:
    static StatusOr<std::unique_ptr<StreamCompressor>> create_compressor(CompressionTypePB type);

    virtual std::string debug_info() { return "StreamCompressor"; }

protected:
    virtual Status init() { return Status::OK(); }

    explicit StreamCompressor(CompressionTypePB ctype) : _ctype(ctype) {}

    CompressionTypePB _ctype;
};

} // namespace starrocks
