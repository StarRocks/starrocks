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
#include <vector>

#include "column/field.h"
#include "column/vectorized_fwd.h"
#include "common/column_id.h"

namespace starrocks {

class TabletColumn;

class StorageSchemaHelper {
public:
    static Field convert_field(ColumnId id, const TabletColumn& c);
    static SchemaPtr convert_schema(const std::vector<TabletColumn*>& columns,
                                    const std::vector<std::string_view>& col_names);
};

} // namespace starrocks
