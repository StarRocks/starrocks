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

#include <string_view>

#include "common/status.h"
#include "storage/tablet_schema.h"

namespace starrocks {
class SlotDescriptor;
class ColumnIterator;
bool is_virtual_column(const std::string_view col_name);
StatusOr<TabletSchemaCSPtr> extend_schema_by_virtual_columns(const TabletSchemaCSPtr& schema,
                                                             const std::vector<SlotDescriptor*>& slots);
StatusOr<TabletSchemaCSPtr> extend_schema_by_virtual_columns(const TabletSchemaCSPtr& schema);
class VirtualColumnFactory {
public:
    struct Options {
        int64_t tablet_id;
        int64_t segment_id;
        int64_t num_rows;
    };
    static StatusOr<ColumnIterator*> create_virtual_column_iterator(const Options& options,
                                                                    const std::string_view col_name);
    static Status append_to_column(const Options& options, const std::string_view col_name, Column* column);
};

} // namespace starrocks