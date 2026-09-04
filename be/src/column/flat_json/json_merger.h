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
#include "column/flat_json/json_flat_path.h"
#include "column/nullable_column.h"
#include "types/logical_type.h"

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
    void _merge_impl(size_t rows, JsonColumn& json_result, NullColumn& null_result);

    template <bool IN_TREE>
    void _merge_json_with_remain(const JsonFlatPath* root, const vpack::Slice* remain, vpack::Builder* builder,
                                 size_t index);

    void _merge_json(const JsonFlatPath* root, vpack::Builder* builder, size_t index);

    // The remain column is always stored at the document root, but set_root_path() re-roots the flat
    // path tree at `_root_path`. Descend the remain slice by the same levels so the merge only ever
    // sees the sub-slice the new root points at. Returns false when the row's remain doesn't hold
    // that node (the key is absent, or something on the way down isn't an object, e.g. a whole
    // document that is an array or a scalar), which means the remain contributes nothing to the row.
    bool _descend_remain_to_root(const vpack::Slice& remain, vpack::Slice* root_remain) const;

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
    // for read, the path the flat tree was re-rooted at, empty when merging the whole column
    std::string _root_path;
    bool _has_root_path = false;
    // for read, the node set_root() marked OP_ROOT, i.e. the node `_root_path` names. Once a row's
    // remain has been descended to that node there is nothing left to look for above it, so the merge
    // starts here instead of walking the outer levels down a second time. Null when the whole column
    // is merged, and also when the flat tree holds no node for `_root_path`.
    const JsonFlatPath* _root_node = nullptr;
};

} // namespace starrocks
