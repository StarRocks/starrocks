// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/expr.h"
#include "storage/rowset/block_split_bloom_filter.h"
#include "storage/rowset/column_reader.h"
#include "types/logical_type.h"
#include "util/phmap/phmap.h"
#include "velocypack/vpack.h"

namespace starrocks {
namespace vpack = arangodb::velocypack;
class ColumnReader;
class BloomFilter;

#ifndef NDEBUG
template <typename K, typename V>
using FlatJsonHashMap = std::unordered_map<K, V>;
#else
template <typename K, typename V>
using FlatJsonHashMap = phmap::flat_hash_map<K, V>;
#endif

class JsonFlatPath {
public:
    using OP = uint8_t;
    static const OP OP_INCLUDE = 0;
    static const OP OP_EXCLUDE = 1;   // for compaction remove extract json
    static const OP OP_IGNORE = 2;    // for merge and read middle json
    static const OP OP_ROOT = 3;      // to mark new root
    static const OP OP_NEW_LEVEL = 4; // for merge flat json use, to mark the path is need

    // for express flat path
    int index = -1; // flat paths array index, only use for leaf, to find column
    LogicalType type = LogicalType::TYPE_JSON;
    bool remain = false;
    OP op = OP_INCLUDE; // merge flat json use, to mark the path is need
    FlatJsonHashMap<std::string_view, std::unique_ptr<JsonFlatPath>> children;

    JsonFlatPath() = default;
    JsonFlatPath(JsonFlatPath&&) = default;
    JsonFlatPath(const JsonFlatPath& rhs) = default;
    ~JsonFlatPath() = default;

    // return the leaf node
    // @info: string_view is not safe memory use, must be careful plz
    static JsonFlatPath* normalize_from_path(const std::string_view& path, JsonFlatPath* root);

    // set new root, other path will set to exclude, the node must include the root path
    static void set_root(const std::string_view& new_root_path, JsonFlatPath* node);

    static std::string debug_flat_json(const std::vector<std::string>& paths, const std::vector<LogicalType>& types,
                                       bool has_remain) {
        if (paths.empty()) {
            return "[]";
        }
        DCHECK_EQ(paths.size(), types.size());
        std::ostringstream ss;
        ss << "[";
        size_t i = 0;
        for (; i < paths.size() - 1; i++) {
            ss << paths[i] << "(" << type_to_string(types[i]) << "), ";
        }
        ss << paths[i] << "(" << type_to_string(types[i]) << ")";
        ss << (has_remain ? "]" : "}");
        return ss.str();
    }

    static std::pair<std::string_view, std::string_view> split_path(const std::string_view& path);
};

// to deriver json flanttern path
class JsonPathDeriver {
public:
    JsonPathDeriver() = default;
    JsonPathDeriver(const std::vector<std::string>& paths, const std::vector<LogicalType>& types, bool has_remain);

    ~JsonPathDeriver() = default;

    // dervie paths
    void derived(const std::vector<const Column*>& json_datas);

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

private:
    struct JsonFlatDesc {
        // json compatible type
        uint8_t type = 31; // JSON_NULL_TYPE_BITS
        // column path hit count, some json may be null or none, so hit use to record the actual value
        // e.g: {"a": 1, "b": 2}, path "$.c" not exist, so hit is 0
        uint64_t hits = 0;

        // for json-uint, json-uint is uint64_t, check the maximum value and downgrade to bigint
        uint64_t max = 0;

        // same key may appear many times in json, so we need avoid duplicate compute hits
        int64_t last_row = -1;
        uint64_t multi_times = 0;
    };

    bool _has_remain = false;
    std::vector<std::string> _paths;
    std::vector<LogicalType> _types;

    double _min_json_sparsity_factory = config::json_flat_sparsity_factor;
    size_t _total_rows;
    FlatJsonHashMap<JsonFlatPath*, JsonFlatDesc> _derived_maps;
    std::shared_ptr<JsonFlatPath> _path_root;

    bool _generate_filter = false;
    std::shared_ptr<BloomFilter> _remain_filter = nullptr;
};

// flattern JsonColumn to flat json A,B,C
class JsonFlattener {
public:
    JsonFlattener(JsonPathDeriver& deriver);

    JsonFlattener(const std::vector<std::string>& paths, const std::vector<LogicalType>& types, bool has_remain);

    ~JsonFlattener() = default;

    // flatten without flat json, input must not flat json
    void flatten(const Column* json_column);

    Columns mutable_result();

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

    Columns _flat_columns;
    JsonColumn* _remain;
};

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

private:
    std::vector<std::string> _src_paths;
    bool _has_remain = false;

    std::shared_ptr<JsonFlatPath> _src_root;
    std::vector<const Column*> _src_columns;
    std::vector<std::string> _exclude_paths;
    std::vector<std::string> _level_paths;
    bool _output_nullable = false;

    ColumnPtr _result;
    JsonColumn* _json_result;
    NullColumn* _null_result;
};

// use read or compaction flat json
//
// to handle input flat json, and output different schema flat json
// e.g:
// COMPACTION: INPUT (A, B(int), C, F, REMAIN), OUTPUT (A, B(JSON), C, D, E, REMAIN)
// - D/E need extract from REMAIN
// - B need cast to JSON type
// - REMAIN need remove D/E, and merge F into REMAIN
//
// READ: STORAGE (A.A1, A.A2, B(int), C(JSON), REMAIN), READ (A, B(string), C.C1, D)
// - A need merge A.A1, A.A2, and other A.subfiled which from remain
// - B need cast to string type
// - C.C1 need extract from C
// - D need extract from remain
class HyperJsonTransformer {
public:
    HyperJsonTransformer(const std::vector<std::string>& paths, const std::vector<LogicalType>& types, bool has_remain);

    ~HyperJsonTransformer() = default;

    // init for read process
    void init_read_task(const std::vector<std::string>& paths, const std::vector<LogicalType>& types, bool has_remain);

    // init for compaction
    void init_compaction_task(const std::vector<std::string>& paths, const std::vector<LogicalType>& types,
                              bool has_remain);

    Status trans(const Columns& columns);

    Columns& result() { return _dst_columns; }

    Columns mutable_result();

    std::vector<std::string> cast_paths() const;

    std::vector<std::string> merge_paths() const;

    std::vector<std::string> flat_paths() const;

    // cast, merge, flat
    std::tuple<int64_t, int64_t, int64_t> cost_ms() const { return {_cast_ms, _merge_ms, _flat_ms}; };

private:
    // equals or merge, dst-src: 1-N/1-1
    struct MergeTask {
        // to avoid create column with find type
        Expr* cast_expr;

        bool is_merge = false;
        bool need_cast = false;

        int dst_index = -1;
        std::vector<int> src_index;

        std::unique_ptr<JsonMerger> merger;
    };

    // flat, dst-src: N-1
    struct FlatTask {
        std::vector<int> dst_index;
        int src_index = -1;

        std::unique_ptr<JsonFlattener> flattener;
    };

    Status _equals(const MergeTask& task, const Columns& columns);
    Status _cast(const MergeTask& task, const ColumnPtr& columns);
    Status _merge(const MergeTask& task, const Columns& columns);
    void _flat(const FlatTask& task, const Columns& columns);

private:
    bool _dst_remain = false;
    std::vector<std::string> _dst_paths;
    std::vector<LogicalType> _dst_types;
    Columns _dst_columns;

    std::vector<std::string> _src_paths;
    std::vector<LogicalType> _src_types;
    std::vector<MergeTask> _merge_tasks;
    std::vector<FlatTask> _flat_tasks;
    ObjectPool _pool;

    int64_t _cast_ms = 0;
    int64_t _flat_ms = 0;
    int64_t _merge_ms = 0;
};

} // namespace starrocks
