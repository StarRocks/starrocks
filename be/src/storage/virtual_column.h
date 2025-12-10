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

#include <cstring>
#include <string>

namespace starrocks {

// Check if a column name represents a virtual column.
// Virtual columns are read-only metadata columns that are not persisted.
// Currently supported: _tablet_id_
inline bool is_virtual_column(const std::string& column_name) {
    return strcasecmp(column_name.c_str(), "_tablet_id_") == 0;
}

} // namespace starrocks