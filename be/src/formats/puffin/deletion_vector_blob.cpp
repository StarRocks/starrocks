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

#include "formats/puffin/deletion_vector_blob.h"

#include <cstdint>
#include <cstring>

#include "formats/puffin/deletion_vector_format.h"
#include "gutil/endian.h"
#include "gutil/strings/substitute.h"

namespace starrocks::formats {

StatusOr<std::string> build_deletion_vector_blob(const roaring64_bitmap_t* bitmap) {
    if (bitmap == nullptr) {
        return Status::InvalidArgument("deletion vector bitmap is null");
    }
    // Iceberg row positions are non-negative signed 64-bit values, so the high bit must be clear.
    // A position with the MSB set (e.g. a sentinel cast from -1) round-trips through our own
    // parser but is rejected or misread by other Iceberg readers, so fail closed here. Empty
    // bitmaps report a maximum of 0, which passes.
    const uint64_t max_position = roaring64_bitmap_maximum(bitmap);
    if ((max_position & (uint64_t(1) << 63)) != 0) {
        return Status::InvalidArgument(
                strings::Substitute("deletion vector position out of range: $0 has the high bit set", max_position));
    }
    const size_t body_len = roaring64_bitmap_portable_size_in_bytes(bitmap);
    // Full blob = length + magic + body + crc. Keep the whole blob within INT32_MAX so
    // the big-endian length prefix (uint32) and the zlib crc length (uInt) can never
    // truncate (Iceberg caps a DV blob at Integer.MAX_VALUE).
    const int64_t size = kDvLengthPrefixBytes + kDvMagicBytes + static_cast<int64_t>(body_len) + kDvCrcBytes;
    if (size > INT32_MAX) {
        return Status::InvalidArgument(
                strings::Substitute("deletion vector blob too large: $0 bytes (max $1)", size, INT32_MAX));
    }

    std::string blob;
    blob.resize(size);
    auto* p = reinterpret_cast<uint8_t*>(blob.data());

    // length prefix (BE) = magic + body length.
    BigEndian::Store32(p, static_cast<uint32_t>(size - kDvLengthPrefixBytes - kDvCrcBytes));
    // magic.
    memcpy(p + kDvLengthPrefixBytes, kDvBlobMagic, kDvMagicBytes);
    // roaring64 portable body.
    roaring64_bitmap_portable_serialize(bitmap, blob.data() + kDvLengthPrefixBytes + kDvMagicBytes);
    // crc32 over magic + body (BE).
    BigEndian::Store32(p + size - kDvCrcBytes, dv_blob_crc32(p + kDvLengthPrefixBytes, kDvMagicBytes + body_len));

    return blob;
}

} // namespace starrocks::formats
