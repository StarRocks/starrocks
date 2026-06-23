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

#include "util/hyper_json_transformer.h"

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/flat_json/json_flattener.h"
#include "column/flat_json/json_merger.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "common/runtime_profile.h"
#include "exprs/cast_expr.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "runtime/descriptors.h"
#include "types/type_descriptor.h"

namespace starrocks {

HyperJsonTransformer::~HyperJsonTransformer() = default;

HyperJsonTransformer::HyperJsonTransformer(const std::vector<std::string>& paths, const std::vector<LogicalType>& types,
                                           bool has_remain)
        : _dst_remain(has_remain), _dst_paths(paths), _dst_types(types) {
    for (size_t i = 0; i < _dst_paths.size(); i++) {
        _dst_columns.emplace_back(ColumnHelper::create_column(TypeDescriptor(types[i]), true));
    }

    if (_dst_remain) {
        _dst_columns.emplace_back(JsonColumn::create());
    }
}

void HyperJsonTransformer::init_read_task(const std::vector<std::string>& paths, const std::vector<LogicalType>& types,
                                          bool has_remain) {
    _src_paths = paths;
    _src_types = types;

    _merge_tasks.clear();
    _flat_tasks.clear();
    _pool.clear();

    std::vector<int> equals;
    std::vector<int> merges;
    std::unordered_set<int> check_dst;
    DCHECK(!_dst_remain);
    // equals & merge
    for (size_t i = 0; i < _dst_paths.size(); i++) {
        equals.clear();
        merges.clear();
        for (size_t j = 0; j < _src_paths.size(); j++) {
            if (_dst_paths[i] == _src_paths[j]) {
                equals.emplace_back(j); // equals
#ifdef NDEBUG
                break; // must only one
#endif
            } else if (_src_paths[j].starts_with(_dst_paths[i] + ".")) {
                merges.emplace_back(j);
            }
        }

        DCHECK(equals.empty() || merges.empty());
        if (!equals.empty()) {
            DCHECK_EQ(equals.size(), 1);
            check_dst.emplace(i);
            auto& mk = _merge_tasks.emplace_back();
            mk.is_merge = false;
            mk.src_index.emplace_back(equals[0]);
            mk.dst_index = i;
            if (_dst_types[i] != _src_types[equals[0]]) {
                mk.need_cast = true;
                SlotDescriptor source_slot(i, "mock_solt", TypeDescriptor(_src_types[equals[0]]));
                ColumnRef* col_ref = _pool.add(new ColumnRef(&source_slot));
                mk.cast_expr = VectorizedCastExprFactory::from_type(TypeDescriptor(_src_types[equals[0]]),
                                                                    TypeDescriptor(_dst_types[i]), col_ref, &_pool);
            }
        } else if (!merges.empty()) {
            check_dst.emplace(i);
            if (_dst_types[i] != TYPE_JSON && !is_string_type(_dst_types[i])) {
                continue;
            }
            auto& mk = _merge_tasks.emplace_back();
            mk.is_merge = true;
            mk.src_index = merges;
            mk.dst_index = i;
            if (_dst_types[i] != TYPE_JSON) {
                // must be to string, merge result must be string
                mk.need_cast = true;
                SlotDescriptor source_slot(i, "mock_solt", TypeDescriptor(TYPE_JSON));
                ColumnRef* col_ref = _pool.add(new ColumnRef(&source_slot));
                mk.cast_expr = VectorizedCastExprFactory::from_type(TypeDescriptor(TYPE_JSON),
                                                                    TypeDescriptor(_dst_types[i]), col_ref, &_pool);
            }
        }
    }

    std::vector<int> flats;
    for (size_t j = 0; j < _src_paths.size(); j++) {
        flats.clear();
        for (size_t i = 0; i < _dst_paths.size(); i++) {
#ifdef NDEBUG
            if (!check_dst.contains(i) && _dst_paths[i].starts_with(_src_paths[j] + ".")) {
#else
            if (_dst_paths[i].starts_with(_src_paths[j] + ".")) {
#endif
                flats.emplace_back(i);
                DCHECK(!check_dst.contains(i));
                check_dst.emplace(i);
            }
        }
        if (!flats.empty()) {
            auto& fk = _flat_tasks.emplace_back();
            fk.src_index = j;
            fk.dst_index = flats;
        }
    }

    if (has_remain && _dst_paths.size() != check_dst.size()) {
        // must from remain
        flats.clear();
        for (size_t i = 0; i < _dst_paths.size(); i++) {
            if (!check_dst.contains(i)) {
                flats.emplace_back(i);
                DCHECK(!check_dst.contains(i));
                check_dst.emplace(i);
            }
        }

        auto& fk = _flat_tasks.emplace_back();
        fk.src_index = _src_paths.size();
        fk.dst_index = flats;
    }

    for (auto& fk : _flat_tasks) {
        std::vector<std::string> p;
        std::vector<LogicalType> t;
        if (fk.src_index == _src_paths.size()) {
            DCHECK(has_remain);
            for (auto& index : fk.dst_index) {
                p.emplace_back(_dst_paths[index]);
                t.emplace_back(_dst_types[index]);
            }
        } else {
            for (auto& index : fk.dst_index) {
                // compute flatten path
                auto dp = _dst_paths[index].substr(_src_paths[fk.src_index].size() + 1);
                p.emplace_back(dp);
                t.emplace_back(_dst_types[index]);
            }
        }
        fk.flattener = std::make_unique<JsonFlattener>(p, t, false);
    }

    for (auto& mk : _merge_tasks) {
        if (mk.is_merge) {
            std::vector<std::string> p;
            std::vector<LogicalType> t;
            for (auto& index : mk.src_index) {
                p.emplace_back(_src_paths[index]);
                t.emplace_back(_src_types[index]);
            }
            if (has_remain) {
                mk.src_index.emplace_back(paths.size());
            }
            mk.merger = std::make_unique<JsonMerger>(p, t, has_remain);
            mk.merger->set_root_path(_dst_paths[mk.dst_index]);
            mk.merger->set_output_nullable(true);
        }
    }
}

void HyperJsonTransformer::init_compaction_task(const std::vector<std::string>& paths,
                                                const std::vector<LogicalType>& types, bool has_remain) {
    _src_paths = paths;
    _src_types = types;

    _merge_tasks.clear();
    _flat_tasks.clear();
    _pool.clear();

    std::unordered_set<std::string> _src_set(_src_paths.begin(), _src_paths.end());

    // output remain, must put merge task at first
    if (_dst_remain) {
        _merge_tasks.emplace_back();
        _merge_tasks.back().dst_index = _dst_paths.size();
        _merge_tasks.back().is_merge = true;
    }

    for (size_t j = 0; j < _src_paths.size(); j++) {
        size_t i = 0;
        for (; i < _dst_paths.size(); i++) {
            DCHECK(!_src_paths[j].starts_with(_dst_paths[i] + "."));
            DCHECK(!_dst_paths[i].starts_with(_src_paths[j] + "."));
            if (_dst_paths[i] == _src_paths[j]) {
                auto& mk = _merge_tasks.emplace_back();
                mk.is_merge = false;
                mk.is_merge = false;
                mk.src_index.emplace_back(j);
                mk.dst_index = i;
                if (_dst_types[i] != _src_types[j]) {
                    mk.need_cast = true;
                    SlotDescriptor source_slot(i, "mock_solt", TypeDescriptor(_src_types[j]));
                    ColumnRef* col_ref = _pool.add(new ColumnRef(&source_slot));
                    mk.cast_expr = VectorizedCastExprFactory::from_type(TypeDescriptor(_src_types[j]),
                                                                        TypeDescriptor(_dst_types[i]), col_ref, &_pool);
                }
                break;
            }
        }

        if (i >= _dst_paths.size() && _dst_remain) {
            _merge_tasks[0].src_index.emplace_back(j);
        }
    }

    std::vector<std::string> all_flat_paths; // for remove from remain
    if (has_remain) {
        for (size_t i = 0; i < _dst_paths.size(); i++) {
            if (_src_set.find(_dst_paths[i]) == _src_set.end()) {
                // merge to remain
                if (_flat_tasks.empty()) {
                    _flat_tasks.emplace_back();
                    _flat_tasks.back().src_index = _src_paths.size();
                }
                _flat_tasks.back().dst_index.emplace_back(i);
            }
        }

        for (size_t i = 0; i < _flat_tasks.size(); i++) {
            auto& fk = _flat_tasks[i];
            std::vector<std::string> p;
            std::vector<LogicalType> t;
            for (auto& index : fk.dst_index) {
                all_flat_paths.emplace_back(_dst_paths[index]);
                p.emplace_back(_dst_paths[index]);
                t.emplace_back(_dst_types[index]);
            }
            fk.flattener = std::make_unique<JsonFlattener>(p, t, false);
        }
    }

    if (_dst_remain) {
        auto& mk = _merge_tasks[0];
        DCHECK(mk.is_merge);
        std::vector<std::string> p;
        std::vector<LogicalType> t;

        for (auto& index : mk.src_index) {
            p.emplace_back(_src_paths[index]);
            t.emplace_back(_src_types[index]);
        }
        mk.merger = std::make_unique<JsonMerger>(p, t, has_remain);
        mk.merger->add_level_paths(_dst_paths);
        mk.merger->set_exclude_paths(all_flat_paths);
        if (has_remain) {
            _merge_tasks[0].src_index.emplace_back(_src_paths.size());
        }
    }

    for (size_t i = 1; i < _merge_tasks.size(); i++) {
        DCHECK(!_merge_tasks[i].is_merge);
    }
}

Status HyperJsonTransformer::trans(const Columns& columns) {
    DCHECK(_dst_remain ? _dst_columns.size() == _dst_paths.size() + 1 : _dst_columns.size() == _dst_paths.size());
    for (auto& col : _dst_columns) {
        DCHECK_EQ(col->size(), 0);
    }
    {
        SCOPED_RAW_TIMER(&_cast_ms);
        for (auto& task : _merge_tasks) {
            if (!task.is_merge) {
                RETURN_IF_ERROR(_equals(task, columns));
            }
        }
    }
    {
        SCOPED_RAW_TIMER(&_merge_ms);
        for (auto& task : _merge_tasks) {
            if (task.is_merge) {
                RETURN_IF_ERROR(_merge(task, columns));
            }
        }
    }
    {
        SCOPED_RAW_TIMER(&_flat_ms);
        for (auto& task : _flat_tasks) {
            _flat(task, columns);
        }
    }

    size_t rows = columns[0]->size();
    for (size_t i = 0; i < _dst_columns.size(); i++) {
        if (_dst_columns[i]->size() == 0) {
            _dst_columns[i]->append_default(rows);
        } else {
            DCHECK_EQ(rows, _dst_columns[i]->size());
        }
    }

    // IMPORTANT: Check column integrity to prevent NullableColumn inconsistency
    for (auto& col : _dst_columns) {
        col->check_or_die();
    }

    return Status::OK();
}

Status HyperJsonTransformer::_equals(const MergeTask& task, const Columns& columns) {
    DCHECK(task.src_index.size() == 1);
    if (task.need_cast) {
        auto& col = columns[task.src_index[0]];
        return _cast(task, col);
    }
    _dst_columns[task.dst_index] = columns[task.src_index[0]]->as_mutable_ptr();
    return Status::OK();
}

Status HyperJsonTransformer::_cast(const MergeTask& task, const ColumnPtr& col) {
    DCHECK(task.need_cast);
    Chunk chunk;
    chunk.append_column(col, task.dst_index);
    ASSIGN_OR_RETURN(auto res_col, task.cast_expr->evaluate_checked(nullptr, &chunk));
    auto res = res_col->as_mutable_ptr();
    res->set_delete_state(col->delete_state());

    if (res->only_null()) {
        auto check = _dst_columns[task.dst_index]->append_nulls(col->size());
        DCHECK(check);
    } else if (res->is_constant()) {
        auto data = down_cast<ConstColumn*>(res.get())->data_column();
        _dst_columns[task.dst_index]->append_value_multiple_times(*data, 0, col->size());
    } else if (_dst_columns[task.dst_index]->is_nullable() && !res->is_nullable()) {
        auto nl = NullColumn::create(col->size(), 0);
        _dst_columns[task.dst_index] = NullableColumn::create(std::move(res), std::move(nl));
    } else {
        DCHECK_EQ(_dst_columns[task.dst_index]->is_nullable(), res->is_nullable());
        _dst_columns[task.dst_index].swap(res);
    }
    return Status::OK();
}

Status HyperJsonTransformer::_merge(const MergeTask& task, const Columns& columns) {
    if (task.dst_index == _dst_paths.size()) {
        DCHECK(_dst_remain);
        // output to remain
        if (task.src_index.size() == 1 && task.src_index[0] == _src_paths.size() && !task.merger->has_exclude_paths()) {
            // only use remain
            _dst_columns[task.dst_index] = columns[task.src_index[0]]->as_mutable_ptr();
            return Status::OK();
        }
    }

    if (task.src_index.empty()) {
        // input has no remain and hit columns, but need output remain...
        DCHECK(_dst_remain);
        _dst_columns[task.dst_index]->append_default(columns[0]->size());
        return Status::OK();
    }

    Columns cols;
    for (auto& index : task.src_index) {
        cols.emplace_back(columns[index]);
    }
    auto result = task.merger->merge(cols);
    if (task.need_cast) {
        return _cast(task, result);
    } else {
        _dst_columns[task.dst_index] = result->as_mutable_ptr();
    }
    return Status::OK();
}

void HyperJsonTransformer::_flat(const FlatTask& task, const Columns& columns) {
    if (task.dst_index.empty()) {
        return;
    }

    if (task.src_index != _src_types.size() && _src_types[task.src_index] != LogicalType::TYPE_JSON) {
        // not json column, don't need to flatten
        for (size_t i = 0; i < task.dst_index.size(); i++) {
            auto check = _dst_columns[task.dst_index[i]]->append_nulls(columns[0]->size());
            DCHECK(check);
        }
        return;
    }

    task.flattener->flatten(columns[task.src_index].get());
    auto result = task.flattener->mutable_result();

    for (size_t i = 0; i < task.dst_index.size(); i++) {
        _dst_columns[task.dst_index[i]] = std::move(result[i]);
    }
}

MutableColumns HyperJsonTransformer::mutable_result() {
    MutableColumns res;
    for (size_t i = 0; i < _dst_columns.size(); i++) {
        auto cloned = _dst_columns[i]->clone_empty();
        res.emplace_back(std::move(_dst_columns[i]));
        _dst_columns[i] = std::move(cloned);
    }
    return res;
}

std::vector<std::string> HyperJsonTransformer::cast_paths() const {
    std::vector<std::string> res;
    for (auto& task : _merge_tasks) {
        if (task.is_merge) {
            continue;
        }
        for (auto& index : task.src_index) {
            if (index == _src_paths.size()) {
                res.emplace_back("remain");
            } else {
                res.emplace_back(_src_paths[index]);
            }
        }
    }
    return res;
}

std::vector<std::string> HyperJsonTransformer::merge_paths() const {
    std::vector<std::string> res;
    for (auto& task : _merge_tasks) {
        if (!task.is_merge) {
            continue;
        }
        for (auto& index : task.src_index) {
            if (index == _src_paths.size()) {
                res.emplace_back("remain");
            } else {
                res.emplace_back(_src_paths[index]);
            }
        }
    }
    return res;
}

std::vector<std::string> HyperJsonTransformer::flat_paths() const {
    std::vector<std::string> res;
    for (auto& task : _flat_tasks) {
        if (task.src_index == _src_paths.size()) {
            res.emplace_back("remain");
        } else {
            res.emplace_back(_src_paths[task.src_index]);
        }
    }
    return res;
}

} // namespace starrocks
