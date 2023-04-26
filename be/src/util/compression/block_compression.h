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

#include <cstddef>
#include <vector>

#include "common/status.h"
#include "gen_cpp/segment.pb.h"
#include "util/raw_container.h"
#include "util/slice.h"

namespace starrocks {

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
                            raw::RawString* compressed_body2 = nullptr) const = 0;

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
                            raw::RawString* compressed_body2 = nullptr) const;

    // Decompress input data into output, output's capacity should be large
    // enough for decompressed data. Size of decompressed data will be set in
    // output's size.
    virtual Status decompress(const Slice& input, Slice* output) const = 0;

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
Status get_block_compression_codec(CompressionTypePB type, const BlockCompressionCodec** codec);

bool use_compression_pool(CompressionTypePB type);

} // namespace starrocks
