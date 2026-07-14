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

// Serializes a roaring64 bitmap into an Iceberg DV blob:
//   length(4B BE = size-8) | magic D1 D3 39 64 | roaring64 portable body | crc32(4B BE over magic+body)
// The returned bytes are the full blob (its length == content_size_in_bytes) and are
// accepted verbatim by IcebergDeletionVectorReader::parse_dv_blob.
// Returns Status::InvalidArgument if `bitmap` is null or the blob would exceed INT32_MAX bytes
// (Iceberg caps a DV blob at Integer.MAX_VALUE; the length prefix / crc length are 32-bit).
StatusOr<std::string> build_deletion_vector_blob(const roaring64_bitmap_t* bitmap);

} // namespace starrocks::formats
