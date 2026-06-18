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
#include <string_view>

namespace starrocks::formats {

inline constexpr const char* kIcebergRowIdColumnName = "_row_id";
inline constexpr const char* kIcebergLastUpdatedSequenceNumberColumnName = "_last_updated_sequence_number";
inline constexpr const char* kIcebergRowPositionColumnName = "_pos";
inline constexpr const char* kRowSourceIdColumnName = "_row_source_id";
inline constexpr const char* kScanRangeIdColumnName = "_scan_range_id";

// Iceberg v3 spec reserved field IDs for row lineage columns.
// See: https://iceberg.apache.org/spec/#reserved-field-ids
inline constexpr int32_t kIcebergRowIdColumnId = 2147483540;
inline constexpr int32_t kIcebergLastUpdatedSequenceNumberColumnId = 2147483539;

inline bool is_reserved_scan_column_name(std::string_view name) {
    return name == kIcebergRowIdColumnName || name == kRowSourceIdColumnName || name == kScanRangeIdColumnName ||
           name == kIcebergRowPositionColumnName || name == kIcebergLastUpdatedSequenceNumberColumnName;
}

} // namespace starrocks::formats
