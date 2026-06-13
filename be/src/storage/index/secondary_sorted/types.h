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
#include <memory>
#include <string>
#include <vector>

#include "storage/olap_common.h"
#include "storage/tablet_schema.h"

namespace starrocks::secondary_sorted {

// Encode (segment_id, rowid_in_segment) into a single int64 column value
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

// Build the synthetic TabletSchema used by both SecondaryIndexWriter and
// SecondaryIndexReader. Layout: K cloned index columns (marked key + sort_key
// + optional bloom_filter) + one non-key BIGINT __sidx_pos__ column.
//
// Writer and reader MUST call this with the same source schema + col ids so
// the on-disk segment-v2 file the writer emits parses cleanly on read.
inline TabletSchemaSPtr build_index_tablet_schema(const TabletSchema& source_schema,
                                                  const std::vector<uint32_t>& index_col_ids,
                                                  bool enable_bloom_filter) {
    auto schema = std::make_shared<TabletSchema>();
    schema->set_id(TabletSchema::invalid_id());

    std::vector<ColumnId> sort_key_idxes;
    sort_key_idxes.reserve(index_col_ids.size());
    int32_t next_unique_id = 1; // synthetic; not joined with source
    for (size_t i = 0; i < index_col_ids.size(); ++i) {
        TabletColumn col(source_schema.column(index_col_ids[i]));
        col.set_unique_id(next_unique_id++);
        col.set_is_key(true);
        col.set_is_sort_key(true);
        col.set_aggregation(STORAGE_AGGREGATE_NONE);
        col.set_is_bf_column(enable_bloom_filter); // SegmentWriter will emit bloom per page
        col.set_has_bitmap_index(false);
        schema->append_column(std::move(col));
        sort_key_idxes.push_back(static_cast<ColumnId>(i));
    }

    TabletColumn pos_col(STORAGE_AGGREGATE_NONE, TYPE_BIGINT, /*is_nullable=*/false, next_unique_id++, sizeof(int64_t));
    pos_col.set_name(kEncodedPositionColumnName);
    pos_col.set_is_key(false);
    pos_col.set_is_sort_key(false);
    schema->append_column(std::move(pos_col));

    schema->set_sort_key_idxes(std::move(sort_key_idxes));
    schema->set_num_short_key_columns(static_cast<uint16_t>(index_col_ids.size()));
    return schema;
}

} // namespace starrocks::secondary_sorted
