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
#include "common/object_pool.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/expr.h"
#include "types/logical_type.h"
#include "velocypack/vpack.h"

namespace starrocks {
namespace vpack = arangodb::velocypack;

class JsonFlatPath {
public:
    using OP = uint8_t;
    static const OP OP_INCLUDE = 0;
    static const OP OP_EXCLUDE = 1; // for compaction remove extract json
    static const OP OP_IGNORE = 2;  // for merge and read middle json
    static const OP OP_ROOT = 3;    // to mark new root
    // for express flat path
    int index = -1; // flat paths array index, only use for leaf, to find column
    LogicalType type = LogicalType::TYPE_JSON;
    bool remain = false;
    OP op = OP_INCLUDE; // merge flat json use, to mark the path is need
    std::unordered_map<std::string, std::unique_ptr<JsonFlatPath>> children;

    JsonFlatPath() = default;
    JsonFlatPath(JsonFlatPath&&) = default;
    JsonFlatPath(const JsonFlatPath& rhs) = default;
    ~JsonFlatPath() = default;

    // return the leaf node
    static JsonFlatPath* normalize_from_path(const std::string& path, JsonFlatPath* root);

    // set new root, other path will set to exclude, the node must include the root path
    static void set_root(const std::string& new_root_path, JsonFlatPath* node);

private:
    static std::pair<std::string, std::string> _split_path(const std::string& path);
};

// to deriver json flanttern path
class JsonPathDeriver {
public:
    JsonPathDeriver() = default;
    JsonPathDeriver(const std::vector<std::string>& paths, const std::vector<LogicalType>& types, bool has_remain);

    ~JsonPathDeriver() = default;

    // dervie paths
    void derived(const std::vector<const Column*>& json_datas);

    bool has_remain_json() const { return _has_remain; }

    std::shared_ptr<JsonFlatPath>& flat_path_root() { return _path_root; }

    const std::vector<std::string>& flat_paths() const { return _paths; }

    const std::vector<LogicalType>& flat_types() const { return _types; }

private:
    void _derived(const Column* json_data, size_t mark_row);

    void _finalize();

    void _derived_on_flat_json(const std::vector<const Column*>& json_datas);

    void _visit_json_paths(vpack::Slice value, JsonFlatPath* root, size_t mark_row);

private:
    struct JsonFlatDesc {
        // json compatible type
        uint8_t type = 255; // JSON_NULL_TYPE_BITS
        // column path hit count, some json may be null or none, so hit use to record the actual value
        // e.g: {"a": 1, "b": 2}, path "$.c" not exist, so hit is 0
        uint64_t hits = 0;
        // how many rows need to be cast to a compatible type
        uint16_t casts = 0;

        // for json-uint, json-uint is uint64_t, check the maximum value and downgrade to bigint
        uint64_t max = 0;

        // same key may appear many times in json, so we need avoid duplicate compute hits
        uint64_t last_row = -1;
        uint64_t multi_times = 0;
    };

    bool _has_remain = false;
    std::vector<std::string> _paths;
    std::vector<LogicalType> _types;

    size_t _total_rows;
    std::unordered_map<JsonFlatPath*, JsonFlatDesc> _derived_maps;
    std::shared_ptr<JsonFlatPath> _path_root;
};

// flattern JsonColumn to flat json A,B,C
class JsonFlattener {
public:
    JsonFlattener(JsonPathDeriver& deriver);

    JsonFlattener(const std::vector<std::string>& paths, const std::vector<LogicalType>& types, bool has_remain);

    ~JsonFlattener() = default;

    // flatten without flat json, input must not flat json
    void flatten(const Column* json_column);

    std::vector<ColumnPtr> mutable_result();

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

    std::vector<ColumnPtr> _flat_columns;
    JsonColumn* _remain;
    std::shared_ptr<JsonFlatPath> _dst_root;
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

    // input nullable-json, output none null json
    ColumnPtr merge(const std::vector<ColumnPtr>& columns);

private:
    template <bool IN_TREE>
    void _merge_impl(size_t rows);

    template <bool IN_TREE>
    void _merge_json_with_remain(const JsonFlatPath* root, const vpack::Slice* remain, vpack::Builder* builder,
                                 size_t index);

    void _merge_json(const JsonFlatPath* root, vpack::Builder* builder, size_t index);

private:
    bool _has_remain = false;
    std::shared_ptr<JsonFlatPath> _src_root;
    std::vector<const Column*> _src_columns;
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
    HyperJsonTransformer(JsonPathDeriver& deriver);

    HyperJsonTransformer(const std::vector<std::string>& paths, const std::vector<LogicalType>& types, bool has_remain);

    ~HyperJsonTransformer() = default;

    // init for read process
    void init_read_task(const std::vector<std::string>& paths, const std::vector<LogicalType>& types, bool has_remain);

    // init for compaction
    void init_compaction_task(JsonColumn* column);

    Status trans(std::vector<ColumnPtr>& columns);

    std::vector<ColumnPtr>& result() { return _dst_columns; }

    std::vector<ColumnPtr> mutable_result();

    void reset();

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

        std::unique_ptr<JsonPathDeriver> deriver;
        std::unique_ptr<JsonMerger> merger;
    };

    // flat, dst-src: N-1
    struct FlatTask {
        std::vector<int> dst_index;
        int src_index = -1;

        std::unique_ptr<JsonFlattener> flattener;
    };

    Status _equals(const MergeTask& task, std::vector<ColumnPtr>& columns);
    Status _cast(const MergeTask& task, ColumnPtr& columns);
    Status _merge(const MergeTask& task, std::vector<ColumnPtr>& columns);
    void _flat(const FlatTask& task, std::vector<ColumnPtr>& columns);

private:
    bool _dst_remain = false;
    std::vector<std::string> _dst_paths;
    std::vector<LogicalType> _dst_types;
    std::vector<ColumnPtr> _dst_columns;

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