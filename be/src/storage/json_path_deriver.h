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
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "column/flat_json/json_flat_path.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "types/logical_type.h"

namespace starrocks {

class BloomFilter;
class ColumnReader;
class FlatJsonConfig;

// to deriver json flatten path
class JsonPathDeriver {
public:
    JsonPathDeriver();
    JsonPathDeriver(const std::vector<std::string>& paths, const std::vector<LogicalType>& types, bool has_remain);
    void init_flat_json_config(const FlatJsonConfig* flat_json_config);

    ~JsonPathDeriver() = default;

    // derive paths
    void derived(const std::vector<const Column*>& json_datas);

    StatusOr<size_t> check_null_factor(const std::vector<const Column*>& json_datas);

    void derived(const std::vector<const ColumnReader*>& json_readers);

    bool has_remain_json() const { return _has_remain; }

    void set_generate_filter(bool generate_filter) { _generate_filter = generate_filter; }

    std::shared_ptr<BloomFilter>& remain_fitler() { return _remain_filter; }

    std::shared_ptr<JsonFlatPath>& flat_path_root() { return _path_root; }

    const std::vector<std::string>& flat_paths() const { return _paths; }

    const std::vector<LogicalType>& flat_types() const { return _types; }

private:
    void _derived(const Column* json_data, size_t mark_row);

    JsonFlatPath* _normalize_exists_path(const std::string_view& path, JsonFlatPath* root, uint64_t hits);

    void _finalize();
    uint32_t _dfs_finalize(JsonFlatPath* node, const std::string& absolute_path,
                           std::vector<std::pair<JsonFlatPath*, std::string>>* hit_leaf);

    void _derived_on_flat_json(const std::vector<const Column*>& json_datas);

    void _visit_json_paths(const vpack::Slice& value, JsonFlatPath* root, size_t mark_row);

    // clean sparsity path, to save memory
    void _clean_sparsity_path(const std::string_view& name, JsonFlatPath* root, size_t check_hits_min);

private:
    bool _has_remain = false;
    std::vector<std::string> _paths;
    std::vector<LogicalType> _types;

    double _min_json_sparsity_factory = 0;
    double _max_json_null_factor = 0;
    int _max_column = 0;

    size_t _total_rows;
    std::shared_ptr<JsonFlatPath> _path_root;

    bool _generate_filter = false;
    std::shared_ptr<BloomFilter> _remain_filter = nullptr;
    std::unordered_set<std::string_view> _remain_keys;
};

} // namespace starrocks
