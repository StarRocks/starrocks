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

// the following code are modified from RocksDB:
// https://github.com/facebook/rocksdb/blob/master/util/crc32c.h

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "base/string/slice.h"

namespace starrocks::crc32c {

// Return the crc32c of concat(A, data[0,n-1]) where init_crc is the
// crc32c of some string A.  Extend() is often used to maintain the
// crc32c of a stream of data.
extern uint32_t Extend(uint32_t init_crc, const char* data, size_t n);

// Return the crc32c of data[0,n-1]
inline uint32_t Value(const char* data, size_t n) {
    return Extend(0, data, n);
}

// Return the crc32c of data content in all slices
inline uint32_t Value(const std::vector<Slice>& slices) {
    uint32_t crc = 0;
    for (auto& slice : slices) {
        crc = Extend(crc, slice.get_data(), slice.get_size());
    }
    return crc;
}

// Returns true if ARM PMULL hardware acceleration is available and enabled.
// Returns false on non-ARM platforms, when USE_ARM_PMULL is disabled, when
// hardware lacks HWCAP_PMULL, or when STARROCKS_DISABLE_PMULL is set.
bool HasArmPmull();

// Resets cached ARM PMULL availability status for testing purposes.
// On non-ARM platforms or when USE_ARM_PMULL is not defined, this is a no-op.
void ResetArmPmullForTesting();

// Returns true if STARROCKS_DISABLE_PMULL is set to a truthy value ("1", "true", etc.),
// or false if unset, empty, or set to a falsy value ("0", "false", "off", "no").
bool IsPmullDisabledByEnvForTesting();

// Return the crc32c using the fallback implementation (ExtendImpl<Fast_CRC32>),
// bypassing SIMD acceleration paths. Used for verification and equivalence testing.
uint32_t ExtendFallback(uint32_t init_crc, const char* data, size_t n);

inline uint32_t ValueFallback(const char* data, size_t n) {
    return ExtendFallback(0, data, n);
}

#if defined(__ARM_NEON) && defined(__aarch64__) && defined(USE_ARM_PMULL)
uint32_t crc32c_pmull_simd(uint32_t crc, const char* buf, size_t len);
#endif

static const uint32_t kMaskDelta = 0xa282ead8ul;

// Return a masked representation of crc.
//
// Motivation: it is problematic to compute the CRC of a string that
// contains embedded CRCs.  Therefore we recommend that CRCs stored
// somewhere (e.g., in files) should be masked before being stored.
inline uint32_t Mask(uint32_t crc) {
    // Rotate right by 15 bits and add a constant.
    return ((crc >> 15) | (crc << 17)) + kMaskDelta;
}

// Return the crc whose masked representation is masked_crc.
inline uint32_t Unmask(uint32_t masked_crc) {
    uint32_t rot = masked_crc - kMaskDelta;
    return ((rot >> 17) | (rot << 15));
}

} // namespace starrocks::crc32c
