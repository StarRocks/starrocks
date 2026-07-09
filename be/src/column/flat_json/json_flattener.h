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

#include <velocypack/vpack.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "column/column.h"
#include "column/flat_json/json_flat_path.h"
#include "types/logical_type.h"

namespace starrocks {

class JsonColumn;

// flattern JsonColumn to flat json A,B,C
class JsonFlattener {
public:
    JsonFlattener(const std::vector<std::string>& paths, const std::vector<LogicalType>& types, bool has_remain);

    ~JsonFlattener() = default;

    // flatten without flat json, input must not flat json
    void flatten(const Column* json_column);

    MutableColumns mutable_result();

private:
    template <bool HAS_REMAIN>
    void _flatten(const Column* json_column, const JsonColumn* json_data);

    template <bool REMAIN>
    bool _flatten_json(const vpack::Slice& value, const JsonFlatPath* root, vpack::Builder* builder,
                       uint32_t* hit_count);

private:
    bool _has_remain = false;
    // note: paths may be less 1 than flat columns
    std::vector<std::string> _dst_paths;
    std::shared_ptr<JsonFlatPath> _dst_root;

    MutableColumns _flat_columns;
    JsonColumn* _remain;
};

} // namespace starrocks
