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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/block_split_bloom_filter.h

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

#include "util/bloom_filter.h"

namespace starrocks {

// This Bloom filter is implemented using block-based Bloom filter algorithm
// from Putze et al.'s "Cache-, Hash- and Space-Efficient Bloom filters". The basic
// idea is to hash the item to a tiny Bloom filter which size fit a single cache line
// or smaller. This implementation sets 8 bits in each tiny Bloom filter. Each tiny
// Bloom filter is 32 bytes to take advantage of 32-byte SIMD instruction.
class BlockSplitBloomFilter : public BloomFilter {
public:
    BlockSplitBloomFilter() = default;

    virtual ~BlockSplitBloomFilter() = default;

    void add_hash(uint64_t hash) override;

    bool test_hash(uint64_t hash) const override;

protected:
    void _set_masks(uint32_t key, uint32_t* masks) const {
        for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
            // add some salt to key
            masks[i] = key * SALT[i];
            // masks[i] mod 32
            masks[i] = masks[i] >> 27;
            // set the masks[i]-th bit
            masks[i] = 0x1 << masks[i];
        }
    }

protected:
    // Bytes in a tiny Bloom filter block.
    static const uint32_t BYTES_PER_BLOCK = 32;

    // The number of bits to set in a tiny Bloom filter block
    static const int BITS_SET_PER_BLOCK = 8;

    static const uint32_t SALT[BITS_SET_PER_BLOCK];
};

} // namespace starrocks
