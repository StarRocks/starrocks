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

#include "common/runtime_profile.h"
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

StatusOr<roaring64_bitmap_t*> parse_deletion_vector_blob(const uint8_t* data, int64_t size, int64_t record_count,
                                                         IcebergDVBuildStats* stats) {
    const int64_t min_size = kDvLengthPrefixBytes + kDvMagicBytes + kDvCrcBytes;
    if (size < min_size) {
        return Status::Corruption(strings::Substitute("Iceberg DV blob too small: $0 bytes", size));
    }

    // 1) length prefix == size - 8 (length covers magic + body, excludes the 4B prefix and 4B crc).
    uint32_t length = BigEndian::Load32(data);
    if (static_cast<int64_t>(length) != size - kDvLengthPrefixBytes - kDvCrcBytes) {
        return Status::Corruption(strings::Substitute("Iceberg DV length prefix mismatch: prefix=$0 expected=$1",
                                                      length, size - kDvLengthPrefixBytes - kDvCrcBytes));
    }

    // 2) magic.
    if (memcmp(data + kDvLengthPrefixBytes, kDvBlobMagic, kDvMagicBytes) != 0) {
        return Status::Corruption("Iceberg DV magic mismatch");
    }

    // 3) crc32 over magic + body == trailing crc.
    {
        int64_t crc_ns = 0;
        {
            SCOPED_RAW_TIMER(&crc_ns);
            uint32_t crc_expected = BigEndian::Load32(data + size - kDvCrcBytes);
            uint32_t crc = dv_blob_crc32(data + kDvLengthPrefixBytes, size - kDvLengthPrefixBytes - kDvCrcBytes);
            if (crc != crc_expected) {
                return Status::Corruption(
                        strings::Substitute("Iceberg DV crc mismatch: actual=$0 expected=$1", crc, crc_expected));
            }
        }
        if (stats != nullptr) {
            stats->checksum_ns += crc_ns;
        }
    }

    // 4) deserialize the roaring64 portable body (between magic and crc).
    const char* body = reinterpret_cast<const char*>(data + kDvLengthPrefixBytes + kDvMagicBytes);
    int64_t body_len = size - kDvLengthPrefixBytes - kDvMagicBytes - kDvCrcBytes;
    roaring64_bitmap_t* bitmap = nullptr;
    {
        int64_t deser_ns = 0;
        {
            SCOPED_RAW_TIMER(&deser_ns);
            bitmap = roaring64_bitmap_portable_deserialize_safe(body, static_cast<size_t>(body_len));
        }
        if (stats != nullptr) {
            stats->deserialize_ns += deser_ns;
        }
    }
    if (bitmap == nullptr) {
        return Status::Corruption("Iceberg DV roaring64 portable deserialize failed");
    }

    // 5) cardinality check against the manifest record_count.
    uint64_t card = roaring64_bitmap_get_cardinality(bitmap);
    if (record_count >= 0 && card != static_cast<uint64_t>(record_count)) {
        roaring::api::roaring64_bitmap_free(bitmap);
        return Status::Corruption(
                strings::Substitute("Iceberg DV cardinality mismatch: bitmap=$0 record_count=$1", card, record_count));
    }
    if (stats != nullptr) {
        stats->cardinality += static_cast<int64_t>(card);
        stats->build_count += 1;
    }
    return bitmap;
}

} // namespace starrocks::formats
