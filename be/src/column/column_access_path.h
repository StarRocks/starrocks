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

#include <string>
#include <vector>

#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {

class Field;
class ObjectPool;
class RuntimeState;

class ColumnAccessPath {
public:
    Status init(const TColumnAccessPath& column_path, RuntimeState* state, ObjectPool* pool);

    const std::string& path() const { return _path; }

    uint32_t index() const { return _column_index; }

    const std::vector<std::unique_ptr<ColumnAccessPath>>& children() { return _children; }

    bool is_key() { return _type == TAccessPathType::type::KEY; }

    bool is_offset() { return _type == TAccessPathType::type::OFFSET; }

    // copy and set index
    std::unique_ptr<ColumnAccessPath> convert_by_index(const Field* filed, uint32_t index);

private:
    TAccessPathType::type _type;

    std::string _path;

    uint32_t _column_index;

    std::vector<std::unique_ptr<ColumnAccessPath>> _children;
};

using ColumnAccessPathPtr = std::unique_ptr<ColumnAccessPath>;

} // namespace starrocks
