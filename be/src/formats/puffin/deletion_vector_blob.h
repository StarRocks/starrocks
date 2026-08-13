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

#include <roaring/roaring64.h>

#include <cstdint>
#include <string>

#include "common/statusor.h"

namespace starrocks::formats {

// Iceberg deletion-vector-v1 blob type and metadata property keys.
inline constexpr const char* kDeletionVectorBlobType = "deletion-vector-v1";
// Iceberg MetadataColumns.ROW_POSITION field id (Integer.MAX_VALUE - 2).
inline constexpr int32_t kRowPositionFieldId = 2147483645;
inline constexpr const char* kDvPropReferencedDataFile = "referenced-data-file";
inline constexpr const char* kDvPropCardinality = "cardinality";

// Per-blob accounting, filled in by parse_deletion_vector_blob when a sink is supplied.
struct IcebergDVBuildStats {
    int64_t read_bytes = 0;
    // Whole-build span. The phase timers below do not add up to it: the bitmap merge and the
    // buffer allocation are only accounted for here.
    int64_t build_ns = 0;
    int64_t read_ns = 0;
    int64_t deserialize_ns = 0;
    int64_t checksum_ns = 0;
    int64_t build_count = 0;
    int64_t cardinality = 0;
};

// Serializes a roaring64 bitmap into an Iceberg DV blob:
//   length(4B BE = size-8) | magic D1 D3 39 64 | roaring64 portable body | crc32(4B BE over magic+body)
// The returned bytes are the full blob (its length == content_size_in_bytes) and are
// accepted verbatim by parse_deletion_vector_blob.
// Returns Status::InvalidArgument if `bitmap` is null or the blob would exceed INT32_MAX bytes
// (Iceberg caps a DV blob at Integer.MAX_VALUE; the length prefix / crc length are 32-bit).
StatusOr<std::string> build_deletion_vector_blob(const roaring64_bitmap_t* bitmap);

// Inverse of build_deletion_vector_blob: validates a complete blob buffer and deserializes it.
// Checks the length prefix, magic, crc32 and — when record_count >= 0 — the cardinality against
// the manifest's record count.
// On success returns a NEW roaring64 bitmap that the CALLER OWNS and must free; on failure
// nothing is leaked. `stats` may be null.
StatusOr<roaring64_bitmap_t*> parse_deletion_vector_blob(const uint8_t* data, int64_t size, int64_t record_count,
                                                         IcebergDVBuildStats* stats);

} // namespace starrocks::formats
