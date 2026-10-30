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

#include "storage/tablet_schema.h"

namespace starrocks {
StatusOr<TabletSchemaCSPtr> extend_schema_by_access_paths(const TabletSchemaCSPtr& tablet_schema, size_t next_unique_id,
                                                          const std::vector<ColumnAccessPathPtr>& access_paths);

// The unique id under which `column`'s data is actually stored, which is the id a delta column group
// is keyed by (see DeltaColumnGroup::get_column_idx).
//
// An extended column is a JSON subfield materialized by the JSONV2 path rewrite (the column appended
// by extend_schema_by_access_paths above). It owns no storage: it is derived from its root JSON
// column at read time, and the unique id it carries is synthetic, allocated above every real column
// id so that it can never collide with one. Looking a delta column group up by that synthetic id
// therefore always misses, so the subfield would be read from the base segment and silently return
// the value a column-mode partial update has already replaced. Resolve to the root column instead.
inline ColumnUID storage_column_uid(const TabletColumn& column) {
    const auto* extended_info = column.extended_info();
    if (extended_info != nullptr && extended_info->source_column_uid >= 0) {
        return extended_info->source_column_uid;
    }
    return column.unique_id();
}

} // namespace starrocks
