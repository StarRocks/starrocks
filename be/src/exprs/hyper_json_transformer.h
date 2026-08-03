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

#include <cstdint>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "column/column.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "types/logical_type.h"

namespace starrocks {

class Expr;
class JsonFlattener;
class JsonMerger;

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

    ~HyperJsonTransformer();

    // init for read process
    void init_read_task(const std::vector<std::string>& paths, const std::vector<LogicalType>& types, bool has_remain);

    // init for compaction
    void init_compaction_task(const std::vector<std::string>& paths, const std::vector<LogicalType>& types,
                              bool has_remain);

    Status trans(const Columns& columns);

    MutableColumns mutable_result();

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
    MutableColumns _dst_columns;

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
