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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/bitshuffle_wrapper.h

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
#include <cstdint>

// This namespace has wrappers for the Bitshuffle library which do runtime dispatch to
// either AVX2-accelerated or regular SSE2 implementations based on the available CPU.
namespace starrocks::bitshuffle {

// See <bitshuffle.h> for documentation on these functions.
size_t compress_lz4_bound(size_t size, size_t elem_size, size_t block_size);
int64_t compress_lz4(const void* in, void* out, size_t size, size_t elem_size, size_t block_size);
int64_t decompress_lz4(const void* in, void* out, size_t size, size_t elem_size, size_t block_size);

int64_t encode(const void* in, void* out, const size_t size, const size_t elem_size, size_t block_size);
int64_t decode(const void* in, void* out, const size_t size, const size_t elem_size, size_t block_size);

} // namespace starrocks::bitshuffle

namespace starrocks {
struct BitShuffleEncodingLz4 {
    static size_t encoding_size(size_t size, size_t elem_size, size_t block_size) {
        return bitshuffle::compress_lz4_bound(size, elem_size, block_size);
    }
    static size_t encode(const void* in, void* out, size_t size, size_t elem_size, size_t block_size) {
        return bitshuffle::compress_lz4(in, out, size, elem_size, block_size);
    }
    static size_t decode(const void* in, void* out, size_t size, size_t elem_size, size_t block_size) {
        return bitshuffle::decompress_lz4(in, out, size, elem_size, block_size);
    }
};

struct BitShuffleEncodingPlain {
    static size_t encoding_size(size_t size, size_t elem_size, size_t block_size) { return size * elem_size; }

    static size_t encode(const void* in, void* out, size_t size, size_t elem_size, size_t block_size) {
        return bitshuffle::encode(in, out, size, elem_size, block_size);
    }

    static size_t decode(const void* in, void* out, size_t size, size_t elem_size, size_t block_size) {
        return bitshuffle::decode(in, out, size, elem_size, block_size);
    }
};
} // namespace starrocks
