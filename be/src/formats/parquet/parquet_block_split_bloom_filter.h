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

#include "util/block_split_bloom_filter.h"

namespace starrocks {

// This Bloom filter is implemented using block-based Bloom filter algorithm
// from Putze et al.'s "Cache-, Hash- and Space-Efficient Bloom filters". The basic
// idea is to hash the item to a tiny Bloom filter which size fit a single cache line
// or smaller. This implementation sets 8 bits in each tiny Bloom filter. Each tiny
// Bloom filter is 32 bytes to take advantage of 32-byte SIMD instruction.
class ParquetBlockSplitBloomFilter final : public BlockSplitBloomFilter {
public:
    ParquetBlockSplitBloomFilter() = default;

    ~ParquetBlockSplitBloomFilter() override = default;

    void add_hash(uint64_t hash) override;

    bool test_hash(uint64_t hash) const override;
};

} // namespace starrocks
