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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/bloom_filter.cpp

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

#include <cstdint>
#include <functional>
#include <memory>

#include "gen_cpp/segment.pb.h"
#include "gutil/strings/substitute.h"
#include "storage/rowset/block_split_bloom_filter.h"
#include "storage/utils.h"

namespace starrocks {

Status BloomFilter::create(BloomFilterAlgorithmPB algorithm, std::unique_ptr<BloomFilter>* bf) {
    if (algorithm == BLOCK_BLOOM_FILTER) {
        *bf = std::make_unique<BlockSplitBloomFilter>();
    } else {
        return Status::InternalError(strings::Substitute("invalid bloom filter algorithm:$0", algorithm));
    }
    return Status::OK();
}

inline uint32_t used_bits(uint64_t value) {
    // counting leading zero, builtin function, this will generate BSR(Bit Scan Reverse)
    // instruction for X86
    if (value == 0) {
        return 0;
    }
    return 64 - __builtin_clzll(value);
}

uint32_t BloomFilter::_optimal_bit_num(uint64_t n, double fpp) {
    // ref parquet bloom_filter branch(BlockSplitBloomFilter.java)
    uint32_t num_bits = -8 * (double)n / log(1 - pow(fpp, 1.0 / 8));
    uint32_t max_bits = MAXIMUM_BYTES << 3;
    if (num_bits > max_bits || num_bits < 0) {
        num_bits = max_bits;
    }

    // Get closest power of 2 if bits is not power of 2.
    if ((num_bits && (num_bits - 1)) != 0) {
        num_bits = 1 << used_bits(num_bits);
    }
    if (num_bits < MINIMUM_BYTES << 3) {
        num_bits = MINIMUM_BYTES << 3;
    }
    return num_bits;
}

} // namespace starrocks
