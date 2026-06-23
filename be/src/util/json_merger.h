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

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "column/column.h"
#include "column/nullable_column.h"
#include "types/logical_type.h"
#include "util/json_flat_path.h"

namespace starrocks {

class JsonColumn;

// merge flat json A,B,C to JsonColumn
class JsonMerger {
public:
    ~JsonMerger() = default;

    JsonMerger(const std::vector<std::string>& paths, const std::vector<LogicalType>& types, bool has_remain = false);

    // for read, only read some leaf node
    void set_root_path(const std::string& base_path);

    // for read, must return nullable column
    void set_output_nullable(bool output_nullable) { _output_nullable = output_nullable; }

    // for compaction, set exclude paths, to remove the path
    void set_exclude_paths(const std::vector<std::string>& exclude_paths);
    // for compaction, set level paths, to generate the level in json
    void add_level_paths(const std::vector<std::string>& level_paths);

    bool has_exclude_paths() const { return !_exclude_paths.empty(); }

    // input nullable-json, output none null json
    ColumnPtr merge(const Columns& columns);

private:
    template <bool IN_TREE>
    void _merge_impl(size_t rows);

    template <bool IN_TREE>
    void _merge_json_with_remain(const JsonFlatPath* root, const vpack::Slice* remain, vpack::Builder* builder,
                                 size_t index);

    void _merge_json(const JsonFlatPath* root, vpack::Builder* builder, size_t index);

    void _add_level_paths_impl(const std::string_view& path, JsonFlatPath* root);

    void _check_has_non_null_values(const JsonFlatPath* root, size_t index, bool* has_non_null_values);

private:
    std::vector<std::string> _src_paths;
    bool _has_remain = false;

    std::shared_ptr<JsonFlatPath> _src_root;
    std::vector<const Column*> _src_columns;
    std::vector<std::string> _exclude_paths;
    std::vector<std::string> _level_paths;
    bool _output_nullable = false;

    MutableColumnPtr _result;
    JsonColumn* _json_result;
    NullColumn* _null_result;
};

} // namespace starrocks
