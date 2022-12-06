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

namespace starrocks {

// For HyperLogLog type
constexpr int HLL_COLUMN_PRECISION = 14;
constexpr int HLL_ZERO_COUNT_BITS = (64 - HLL_COLUMN_PRECISION);
constexpr int HLL_EXPLICLIT_INT64_NUM = 160;
constexpr int HLL_SPARSE_THRESHOLD = 4096;
constexpr int HLL_REGISTERS_COUNT = 16 * 1024;
// maximum size in byte of serialized HLL: type(1) + registers (2^14)
constexpr int HLL_COLUMN_DEFAULT_LEN = HLL_REGISTERS_COUNT + 1;

// 1 for type; 1 for hash values count; 8 for hash value
constexpr int HLL_EMPTY_SIZE = 1;

// For JSON type
constexpr int kJsonDefaultSize = 128;
constexpr int kJsonMetaDefaultFormatVersion = 1;

constexpr __int128 MAX_INT128 = ~((__int128)0x01 << 127);
constexpr __int128 MIN_INT128 = ((__int128)0x01 << 127);

} // namespace starrocks
