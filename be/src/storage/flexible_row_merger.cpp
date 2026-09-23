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

#include "storage/flexible_row_merger.h"

#include <fmt/format.h>

#include <boost/algorithm/string/predicate.hpp>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/schema.h"
#include "column/sorting/sorting.h"
#include "gen_cpp/Types_types.h"
#include "gutil/casts.h"

namespace starrocks {

FlexibleRowMerger::FlexibleRowMerger(int64_t txn_id, const Schema& schema, int cset_column, int op_column)
        : _txn_id(txn_id),
          _field_names(schema.field_names()),
          _num_key_fields(schema.num_key_fields()),
          _cset_column(cset_column),
          _op_column(op_column) {
    DCHECK_GE(_cset_column, static_cast<int>(_num_key_fields));
    DCHECK_LT(_cset_column, static_cast<int>(_field_names.size()));
    DCHECK(_op_column < 0 || _op_column < static_cast<int>(_field_names.size()));
}

std::vector<std::vector<uint32_t>> FlexibleRowMerger::column_sets() const {
    std::lock_guard<std::mutex> l(_mu);
    std::vector<std::vector<uint32_t>> sets;
    sets.reserve(_sets.size());
    for (const auto& members : _sets) {
        auto& columns = sets.emplace_back();
        for (uint32_t f = 0; f < members.size(); ++f) {
            if (members[f]) {
                columns.push_back(f);
            }
        }
    }
    return sets;
}

StatusOr<ColumnSetId> FlexibleRowMerger::_intern(const Members& members) {
    std::string key(members.begin(), members.end());
    auto it = _index.find(key);
    if (it != _index.end()) {
        return it->second;
    }
    if (_sets.size() >= kMaxColumnSets) {
        return Status::InvalidArgument(fmt::format(
                "flexible partial update: a tablet received more than {} distinct column sets", kMaxColumnSets));
    }
    auto id = static_cast<ColumnSetId>(_sets.size());
    _sets.push_back(members);
    _index.emplace(std::move(key), id);
    return id;
}

Status FlexibleRowMerger::_refresh_load_sets(size_t min_size) {
    // The dictionary is interned by the load's json scanner and, when this writer runs on another node,
    // shipped here with the chunks that use it (LakeTabletsChannel::add_chunk).
    auto dict = FlexiblePartialUpdateRegistry::instance()->get(_txn_id);
    auto sets = dict != nullptr ? dict->snapshot() : std::vector<std::vector<std::string>>();
    if (sets.size() < min_size) {
        return Status::InternalError(
                fmt::format("flexible partial update: set-id {} of txn {} is not in the column-set dictionary on "
                            "this node",
                            min_size - 1, _txn_id));
    }
    _load_sets = std::move(sets);
    _local_of_load.resize(_load_sets.size(), kUnresolved);
    return Status::OK();
}

StatusOr<ColumnSetId> FlexibleRowMerger::_resolve(size_t load_id) {
    Members members(_field_names.size(), 0);
    for (const auto& name : _load_sets[load_id]) {
        // The dictionary holds the load's source column names, which FE matched to the table columns
        // case-insensitively. A source column that is not written (a skipped field) declares nothing.
        int field = -1;
        for (int f = 0; f < static_cast<int>(_field_names.size()); ++f) {
            if (_field_names[f] == name) {
                field = f;
                break;
            }
        }
        if (field < 0) {
            for (int f = 0; f < static_cast<int>(_field_names.size()); ++f) {
                if (boost::iequals(_field_names[f], name)) {
                    field = f;
                    break;
                }
            }
        }
        if (field >= 0 && field != _cset_column && field != _op_column) {
            members[field] = 1;
        }
    }
    return _intern(members);
}

Status FlexibleRowMerger::_to_local_ids(const int16_t* ids, size_t n, std::vector<ColumnSetId>* local_ids) {
    local_ids->resize(n);
    for (size_t i = 0; i < n; ++i) {
        if (UNLIKELY(ids[i] < 0)) {
            return Status::InternalError(fmt::format("flexible partial update: invalid set-id {}", ids[i]));
        }
        const auto load_id = static_cast<size_t>(ids[i]);
        if (UNLIKELY(load_id >= _local_of_load.size())) {
            RETURN_IF_ERROR(_refresh_load_sets(load_id + 1));
        }
        // Only the sets that rows use enter this writer's dictionary.
        if (UNLIKELY(_local_of_load[load_id] == kUnresolved)) {
            ASSIGN_OR_RETURN(auto local_id, _resolve(load_id));
            _local_of_load[load_id] = local_id;
        }
        (*local_ids)[i] = static_cast<ColumnSetId>(_local_of_load[load_id]);
    }
    return Status::OK();
}

// Replaces the values of a SMALLINT column, nullable or not, by `ids`.
static ColumnPtr make_set_id_column(const Column& like, const std::vector<ColumnSetId>& ids) {
    auto data = Int16Column::create();
    auto& values = data->get_data();
    values.reserve(ids.size());
    for (auto id : ids) {
        values.push_back(static_cast<int16_t>(id));
    }
    if (like.is_nullable()) {
        return NullableColumn::create(std::move(data), NullColumn::create(ids.size(), 0));
    }
    return data;
}

Status FlexibleRowMerger::merge(ChunkPtr* chunk) {
    Chunk& src = **chunk;
    const size_t n = src.num_rows();
    if (n == 0) {
        return Status::OK();
    }
    DCHECK_EQ(src.num_columns(), _field_names.size());
    std::lock_guard<std::mutex> l(_mu);

    const auto cset_column = src.get_column_by_index(_cset_column);
    const auto* load_ids =
            down_cast<const Int16Column*>(ColumnHelper::get_data_column(cset_column.get()))->immutable_data().data();
    std::vector<ColumnSetId> row_ids;
    RETURN_IF_ERROR(_to_local_ids(load_ids, n, &row_ids));

    Tie tie(n, 1);
    for (size_t k = 0; k < _num_key_fields; ++k) {
        build_tie_for_column(src.get_column_by_index(k), &tie);
    }
    tie[0] = 0;

    const int8_t* ops = nullptr;
    if (_op_column >= 0) {
        const auto& op_column = src.get_column_by_index(_op_column);
        ops = down_cast<const Int8Column*>(ColumnHelper::get_data_column(op_column.get()))->immutable_data().data();
    }

    const size_t num_fields = src.num_columns();
    // The last row of every key, and the key's set id.
    std::vector<uint32_t> lasts;
    std::vector<ColumnSetId> ids;
    // For every column, the keys (index in `lasts`) whose value comes from an earlier row, and that row.
    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> earlier(num_fields);
    Members members;
    size_t begin = 0;
    for (size_t end = 1; end <= n; ++end) {
        if (end < n && tie[end]) {
            continue;
        }
        const auto last = static_cast<uint32_t>(end - 1);
        const auto key = static_cast<uint32_t>(lasts.size());
        lasts.push_back(last);
        // The rows of the key that the merge keeps: those after its last delete. A key whose last row is
        // a delete is deleted, so it keeps only that row.
        size_t first = begin;
        if (ops != nullptr) {
            for (size_t r = begin; r < end; ++r) {
                if (ops[r] == TOpType::DELETE) {
                    first = r + 1;
                }
            }
            if (first == end) {
                first = last;
            }
        }
        begin = end;
        if (first == last) {
            ids.push_back(row_ids[last]);
            continue;
        }
        members = _sets[row_ids[last]];
        for (size_t f = _num_key_fields; f < num_fields; ++f) {
            if (static_cast<int>(f) == _cset_column || static_cast<int>(f) == _op_column || members[f]) {
                continue;
            }
            // The last earlier row that declares the column. The last row does not declare it, so when no
            // row does, the column keeps the last row's value, which the apply never reads.
            for (size_t r = last; r-- > first;) {
                if (_sets[row_ids[r]][f]) {
                    members[f] = 1;
                    earlier[f].emplace_back(key, static_cast<uint32_t>(r));
                    break;
                }
            }
        }
        ASSIGN_OR_RETURN(auto id, _intern(members));
        ids.push_back(id);
    }

    if (lasts.size() == n) {
        // No key repeats: only the set ids change.
        src.update_column_by_index(make_set_id_column(*cset_column, ids), _cset_column);
        return Status::OK();
    }
    auto merged = src.clone_empty_with_schema(lasts.size());
    std::vector<uint32_t> picks;
    for (size_t f = 0; f < num_fields; ++f) {
        if (static_cast<int>(f) == _cset_column) {
            continue;
        }
        const auto& from = src.get_column_by_index(f);
        auto* to = merged->get_column_raw_ptr_by_index(f);
        if (earlier[f].empty()) {
            to->append_selective(*from, lasts.data(), 0, lasts.size());
            continue;
        }
        picks = lasts;
        for (const auto& [key, row] : earlier[f]) {
            picks[key] = row;
        }
        to->append_selective(*from, picks.data(), 0, picks.size());
    }
    merged->update_column_by_index(make_set_id_column(*cset_column, ids), _cset_column);
    *chunk = std::move(merged);
    return Status::OK();
}

} // namespace starrocks
