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
#include <memory>

#include "common/status.h"
#include "common/statusor.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "formats/scan_context.h"

namespace starrocks {

struct IcebergDVBuildStats {
    int64_t read_bytes = 0;
    int64_t read_ns = 0;
    int64_t deserialize_ns = 0;
    int64_t checksum_ns = 0;
    int64_t build_count = 0;
    int64_t cardinality = 0;
};

// Reads an Iceberg V3 Puffin deletion-vector blob and produces a Roaring64 skip bitmap.
// Blob layout: length(4B BE) | magic D1 D3 39 64 | roaring64 portable body | crc32(4B BE over magic+body).
// content_size_in_bytes is the FULL blob length (all four sections).
class IcebergDeletionVectorReader {
public:
    explicit IcebergDeletionVectorReader(const HdfsScannerContext& ctx)
            : _descriptor(ctx.table_specific.iceberg_deletion_vector_descriptor), _ctx(ctx) {}

    // Range-read the blob per descriptor, validate, deserialize, and fill skip_rows_ctx->deletion_bitmap.
    Status fill_row_indexes(const SkipRowsContextPtr& skip_rows_ctx);

    // Pure parse of a complete blob buffer. record_count >= 0 enables cardinality validation.
    // On success returns a new roaring64 bitmap (caller owns it). stats may be null.
    static StatusOr<roaring64_bitmap_t*> parse_dv_blob(const uint8_t* data, int64_t size, int64_t record_count,
                                                       IcebergDVBuildStats* stats);

    static constexpr int32_t LENGTH_PREFIX_BYTES = 4;
    static constexpr int32_t MAGIC_BYTES = 4;
    static constexpr int32_t CRC_BYTES = 4;
    static const uint8_t MAGIC[4];

private:
    void update_counter(RuntimeProfile* parent_profile);

    const std::shared_ptr<TIcebergDeletionVectorDescriptor> _descriptor;
    const HdfsScannerContext& _ctx;
    IcebergDVBuildStats _build_stats;
};

} // namespace starrocks