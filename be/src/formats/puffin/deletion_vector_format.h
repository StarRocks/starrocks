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

#include <zlib.h>

#include <cstddef>
#include <cstdint>

namespace starrocks::formats {

// Single source of truth for the Iceberg deletion-vector-v1 blob framing, shared by
// the writer (build_deletion_vector_blob) and the reader (parse_deletion_vector_blob).
// Layout: length(4B BE = totalSize - 8) | magic | roaring64 portable body | crc32(4B BE over magic+body).
inline constexpr int32_t kDvLengthPrefixBytes = 4;
inline constexpr int32_t kDvMagicBytes = 4;
inline constexpr int32_t kDvCrcBytes = 4;
inline constexpr uint8_t kDvBlobMagic[kDvMagicBytes] = {0xD1, 0xD3, 0x39, 0x64};

// CRC32 over the magic+body region (everything between the length prefix and the trailing CRC).
inline uint32_t dv_blob_crc32(const uint8_t* magic_and_body, size_t len) {
    uLong crc = crc32(0L, Z_NULL, 0);
    crc = crc32(crc, reinterpret_cast<const Bytef*>(magic_and_body), static_cast<uInt>(len));
    return static_cast<uint32_t>(crc);
}

} // namespace starrocks::formats
