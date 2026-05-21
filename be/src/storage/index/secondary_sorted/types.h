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

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace starrocks::secondary_sorted {

// PoC: encode (segment_id, rowid_in_segment) into a single int64 column value
// so the index file is a valid segment with schema [idx_cols..., encoded_pos].
//
// Layout (big-endian, signed-safe):
//   bits 63..32 : segment_id (uint32_t in storage; uint16_t logically)
//   bits 31..0  : rowid_in_segment (uint32_t)
//
// We store the result as int64 (signed) because BIGINT is the only widely
// supported 8-byte int type in the StarRocks segment format. Reinterpretation
// between uint64 and int64 is bit-preserving on every architecture we support.
inline int64_t encode_position(uint32_t segment_id, uint32_t rowid) {
    uint64_t packed = (static_cast<uint64_t>(segment_id) << 32) | static_cast<uint64_t>(rowid);
    int64_t result;
    static_assert(sizeof(int64_t) == sizeof(uint64_t), "size mismatch");
    std::memcpy(&result, &packed, sizeof(packed));
    return result;
}

inline void decode_position(int64_t encoded, uint32_t* segment_id, uint32_t* rowid) {
    uint64_t packed;
    std::memcpy(&packed, &encoded, sizeof(packed));
    *segment_id = static_cast<uint32_t>(packed >> 32);
    *rowid = static_cast<uint32_t>(packed & 0xFFFFFFFFULL);
}

// Reserved name for the synthetic int64 position column appended to the
// secondary index file's schema. Chosen to be unlikely to collide with any
// real user column name.
inline constexpr const char* kEncodedPositionColumnName = "__sidx_pos__";

} // namespace starrocks::secondary_sorted
