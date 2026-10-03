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
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/flexible_partial_update.h"
#include "common/status.h"
#include "common/statusor.h"

namespace starrocks {

class Schema;

// Flexible partial update: merges the rows that one load writes for the same primary key, so that the
// key ends with, for every column, the value of the last row that declares the column.
//
// A primary key memtable sorts its rows by key and keeps the last row of every key. For a flexible load
// that would lose the columns that only the earlier rows of a key declare, so a flexible memtable hands
// its sorted rows to merge() instead. Every column of the merged row comes from the last row of the key
// that declares it, and the merged row declares the union of the column sets of those rows. Deletes keep
// the semantics of a plain load: when the last row of a key is a delete, the key is deleted, and a delete
// followed by more rows of the key is dropped together with the rows before it, as the memtable of a
// plain load drops them.
//
// The rows arrive with the set ids of the load's dictionary (FlexiblePartialUpdateRegistry), which the
// load's sender assigns and keeps extending, so the writer cannot add the unions to it. Instead every
// writer keeps its own dictionary over the columns of its memtable schema, merge() rewrites every row's
// set id into an id of that dictionary, and the writer records it in the rowset.
//
// Thread-safe.
class FlexibleRowMerger {
public:
    // `schema` is the memtable schema: the key columns, the value columns, "__cset__" at `cset_column`, and
    // "__op" at `op_column`, or -1 when the load has no "__op" column.
    FlexibleRowMerger(int64_t txn_id, const Schema& schema, int cset_column, int op_column);

    // Replaces `*chunk`, whose rows are sorted by primary key with the rows of a key in load order, by one
    // row per key.
    Status merge(ChunkPtr* chunk);

    // This writer's dictionary, index == set id: for every set, the memtable columns it declares.
    std::vector<std::vector<uint32_t>> column_sets() const;

private:
    using Members = std::vector<uint8_t>;

    static constexpr int32_t kUnresolved = -1;

    // Maps the load's set ids of `ids` to this writer's set ids.
    Status _to_local_ids(const int16_t* ids, size_t n, std::vector<ColumnSetId>* local_ids);
    // Takes a new snapshot of the load's dictionary, which must hold at least `min_size` sets.
    Status _refresh_load_sets(size_t min_size);
    // Interns the load's set `load_id` into this writer's dictionary.
    StatusOr<ColumnSetId> _resolve(size_t load_id);
    StatusOr<ColumnSetId> _intern(const Members& members);

    const int64_t _txn_id;
    std::vector<std::string> _field_names;
    const size_t _num_key_fields;
    const int _cset_column;
    const int _op_column;

    mutable std::mutex _mu;
    // The last snapshot of the load's dictionary: set id -> column names.
    std::vector<std::vector<std::string>> _load_sets;
    // The load's set id -> this writer's set id, or kUnresolved while no row used it.
    std::vector<int32_t> _local_of_load;
    // This writer's set id -> for every memtable column, whether the set declares it.
    std::vector<Members> _sets;
    std::unordered_map<std::string, ColumnSetId> _index;
};

} // namespace starrocks
