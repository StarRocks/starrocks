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

#include "storage/rowset/json_column_iterator.h"

#include <memory>
#include <utility>
#include <vector>

#include "column/column_helper.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/function_context.h"
#include "exprs/json_functions.h"
#include "exprs/jsonpath.h"
#include "gutil/casts.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_iterator_decorator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/scalar_column_iterator.h"
#include "types/logical_type.h"
#include "util/json_flattener.h"
#include "util/runtime_profile.h"

namespace starrocks {

class JsonFlatColumnIterator final : public ColumnIterator {
public:
    JsonFlatColumnIterator(ColumnReader* reader, std::unique_ptr<ColumnIterator> null_iter,
                           std::vector<std::unique_ptr<ColumnIterator>> field_iters,
                           const std::vector<std::string>& target_paths, const std::vector<LogicalType>& target_types,
                           const std::vector<std::string>& source_paths, const std::vector<LogicalType>& source_types,
                           bool need_remain)
            : _reader(reader),
              _null_iter(std::move(null_iter)),
              _flat_iters(std::move(field_iters)),
              _target_paths(std::move(target_paths)),
              _target_types(std::move(target_types)),
              _source_paths(std::move(source_paths)),
              _source_types(std::move(source_types)),
              _need_remain(need_remain){};

    ~JsonFlatColumnIterator() override {
        if (transformer != nullptr) {
            auto [c, m, f] = transformer->cost_ms();
            _opts.stats->json_cast_ns += c;
            _opts.stats->json_merge_ns += m;
            _opts.stats->json_flatten_ns += f;
        }
    }

    Status init(const ColumnIteratorOptions& opts) override;

    Status next_batch(size_t* n, Column* dst) override;

    Status next_batch(const SparseRange<>& range, Column* dst) override;

    Status seek_to_first() override;

    Status seek_to_ordinal(ordinal_t ord) override;

    ordinal_t get_current_ordinal() const override { return _flat_iters[0]->get_current_ordinal(); }

    ordinal_t num_rows() const override { return _flat_iters[0]->num_rows(); }

    Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                      const ColumnPredicate* del_predicate, SparseRange<>* row_ranges,
                                      CompoundNodeType pred_relation, const Range<>* src_range = nullptr) override;

    std::string name() const override { return "JsonFlatColumnIterator"; }

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override;

private:
    template <typename FUNC>
    Status _read(JsonColumn* json_column, FUNC fn);

private:
    ColumnReader* _reader;

    std::unique_ptr<ColumnIterator> _null_iter;
    std::vector<std::unique_ptr<ColumnIterator>> _flat_iters;
    std::vector<std::string> _target_paths;
    std::vector<LogicalType> _target_types;
    std::vector<std::string> _source_paths;
    std::vector<LogicalType> _source_types;
    bool _need_remain = false;

    Columns _source_column_modules;
    // to avoid create column with find type

    bool _is_direct = false;
    std::unique_ptr<HyperJsonTransformer> transformer;
};

Status JsonFlatColumnIterator::init(const ColumnIteratorOptions& opts) {
    RETURN_IF_ERROR(ColumnIterator::init(opts));
    if (_null_iter != nullptr) {
        RETURN_IF_ERROR(_null_iter->init(opts));
    }

    for (auto& iter : _flat_iters) {
        RETURN_IF_ERROR(iter->init(opts));
    }

    bool has_remain = _source_paths.size() != _flat_iters.size();
    VLOG(2) << "JsonFlatColumnIterator init, target: "
            << JsonFlatPath::debug_flat_json(_target_paths, _target_types, _need_remain)
            << ", source: " << JsonFlatPath::debug_flat_json(_source_paths, _source_types, has_remain);

    for (int i = 0; i < _source_paths.size(); i++) {
        auto column = ColumnHelper::create_column(TypeDescriptor(_source_types[i]), true);
        _source_column_modules.emplace_back(std::move(column));
    }

    DCHECK_EQ(_source_column_modules.size(), _source_paths.size());
    if (has_remain) {
        _source_column_modules.emplace_back(JsonColumn::create());
    }

    if (!opts.has_preaggregation && config::enable_lazy_dynamic_flat_json) {
        _is_direct = true;
        // update stats
        for (int i = 0; i < _source_paths.size(); i++) {
            opts.stats->flat_json_hits[_source_paths[i]] += 1;
        }
        if (has_remain) {
            opts.stats->flat_json_hits["remain"] += 1;
        }
        return Status::OK();
    }

    {
        SCOPED_RAW_TIMER(&_opts.stats->json_init_ns);
        if (_need_remain) {
            transformer = std::make_unique<HyperJsonTransformer>(_target_paths, _target_types, true);
            transformer->init_compaction_task(_source_paths, _source_types, has_remain);
        } else {
            transformer = std::make_unique<HyperJsonTransformer>(_target_paths, _target_types, false);
            transformer->init_read_task(_source_paths, _source_types, has_remain);
        }
    }

    // update stats
    {
        auto cp = transformer->cast_paths();
        for (int i = 0; i < cp.size(); i++) {
            opts.stats->flat_json_hits[cp[i]] += 1;
        }
        auto mp = transformer->merge_paths();
        for (int i = 0; i < mp.size(); i++) {
            opts.stats->merge_json_hits[mp[i]] += 1;
        }
        auto fp = transformer->flat_paths();
        for (int i = 0; i < fp.size(); i++) {
            opts.stats->dynamic_json_hits[fp[i]] += 1;
        }
        if (has_remain) {
            opts.stats->flat_json_hits["remain"] += 1;
        }
    }

    return Status::OK();
}

template <typename FUNC>
Status JsonFlatColumnIterator::_read(JsonColumn* json_column, FUNC read_fn) {
    Columns columns;
    for (int i = 0; i < _source_column_modules.size(); i++) {
        columns.emplace_back(_source_column_modules[i]->clone_empty());
    }

    for (int i = 0; i < _flat_iters.size(); i++) {
        RETURN_IF_ERROR(read_fn(_flat_iters[i].get(), columns[i].get()));
    }

    if (_is_direct) {
        json_column->set_flat_columns(_source_paths, _source_types, std::move(columns));
    } else {
        RETURN_IF_ERROR(transformer->trans(std::move(columns)));
        auto result = transformer->mutable_result();
        json_column->set_flat_columns(_target_paths, _target_types, std::move(result));
    }
    return Status::OK();
}

Status JsonFlatColumnIterator::next_batch(size_t* n, Column* dst) {
    JsonColumn* json_column = nullptr;
    NullColumn* null_column = nullptr;
    if (dst->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(dst);

        json_column = down_cast<JsonColumn*>(nullable_column->data_column().get());
        null_column = down_cast<NullColumn*>(nullable_column->null_column().get());
    } else {
        json_column = down_cast<JsonColumn*>(dst);
    }

    // 1. Read null column
    if (_null_iter != nullptr) {
        RETURN_IF_ERROR(_null_iter->next_batch(n, null_column));
        down_cast<NullableColumn*>(dst)->update_has_null();
    }

    // 2. Read flat column
    auto read = [&](ColumnIterator* iter, Column* column) { return iter->next_batch(n, column); };
    auto ret = _read(json_column, read);
    dst->check_or_die();
    return ret;
}

Status JsonFlatColumnIterator::next_batch(const SparseRange<>& range, Column* dst) {
    JsonColumn* json_column = nullptr;
    NullColumn* null_column = nullptr;
    if (dst->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(dst);
        json_column = down_cast<JsonColumn*>(nullable_column->data_column().get());
        null_column = down_cast<NullColumn*>(nullable_column->null_column().get());
    } else {
        json_column = down_cast<JsonColumn*>(dst);
    }

    CHECK((_null_iter == nullptr && null_column == nullptr) || (_null_iter != nullptr && null_column != nullptr));

    // 1. Read null column
    if (_null_iter != nullptr) {
        RETURN_IF_ERROR(_null_iter->next_batch(range, null_column));
        down_cast<NullableColumn*>(dst)->update_has_null();
    }

    // 2. Read flat column
    auto read = [&](ColumnIterator* iter, Column* column) { return iter->next_batch(range, column); };
    auto ret = _read(json_column, read);
    dst->check_or_die();
    return ret;
}

Status JsonFlatColumnIterator::fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) {
    JsonColumn* json_column = nullptr;
    NullColumn* null_column = nullptr;
    // 1. Read null column
    if (_null_iter != nullptr) {
        auto* nullable_column = down_cast<NullableColumn*>(values);
        json_column = down_cast<JsonColumn*>(nullable_column->data_column().get());
        null_column = down_cast<NullColumn*>(nullable_column->null_column().get());
        RETURN_IF_ERROR(_null_iter->fetch_values_by_rowid(rowids, size, null_column));
        nullable_column->update_has_null();
    } else {
        json_column = down_cast<JsonColumn*>(values);
    }

    // 2. Read flat column
    auto read = [&](ColumnIterator* iter, Column* column) { return iter->fetch_values_by_rowid(rowids, size, column); };

    auto ret = _read(json_column, read);
    values->check_or_die();
    return ret;
}

Status JsonFlatColumnIterator::seek_to_first() {
    if (_null_iter != nullptr) {
        RETURN_IF_ERROR(_null_iter->seek_to_first());
    }
    for (int i = 0; i < _flat_iters.size(); i++) {
        RETURN_IF_ERROR(_flat_iters[i]->seek_to_first());
    }
    return Status::OK();
}

Status JsonFlatColumnIterator::seek_to_ordinal(ordinal_t ord) {
    if (_null_iter != nullptr) {
        RETURN_IF_ERROR(_null_iter->seek_to_ordinal(ord));
    }
    for (int i = 0; i < _flat_iters.size(); i++) {
        RETURN_IF_ERROR(_flat_iters[i]->seek_to_ordinal(ord));
    }
    return Status::OK();
}

Status JsonFlatColumnIterator::get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                          const ColumnPredicate* del_predicate,
                                                          SparseRange<>* row_ranges, CompoundNodeType pred_relation,
                                                          const Range<>* src_range) {
    row_ranges->add({0, static_cast<rowid_t>(_reader->num_rows())});
    return Status::OK();
}

class JsonDynamicFlatIterator final : public ColumnIterator {
public:
    JsonDynamicFlatIterator(std::unique_ptr<ScalarColumnIterator>& json_iter, std::vector<std::string> target_paths,
                            std::vector<LogicalType> target_types, bool need_remain)
            : _json_iter(std::move(json_iter)),
              _target_paths(std::move(target_paths)),
              _target_types(std::move(target_types)),
              _need_remain(need_remain){};

    ~JsonDynamicFlatIterator() override = default;

    Status init(const ColumnIteratorOptions& opts) override;

    Status next_batch(size_t* n, Column* dst) override;

    Status next_batch(const SparseRange<>& range, Column* dst) override;

    Status seek_to_first() override;

    Status seek_to_ordinal(ordinal_t ord) override;

    ordinal_t get_current_ordinal() const override { return _json_iter->get_current_ordinal(); }

    ordinal_t num_rows() const override { return _json_iter->num_rows(); }

    std::string name() const override { return "JsonDynamicColumnIterator"; }

    /// for vectorized engine
    Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                      const ColumnPredicate* del_predicate, SparseRange<>* row_ranges,
                                      CompoundNodeType pred_relation, const Range<>* src_range = nullptr) override;

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override;

private:
    template <typename FUNC>
    Status _dynamic_flat(Column* dst, FUNC read_fn);

private:
    std::unique_ptr<ScalarColumnIterator> _json_iter;
    std::vector<std::string> _target_paths;
    std::vector<LogicalType> _target_types;
    bool _need_remain = false;

    bool _is_direct = false;
    std::unique_ptr<JsonFlattener> _flattener;
};

Status JsonDynamicFlatIterator::init(const ColumnIteratorOptions& opts) {
    RETURN_IF_ERROR(ColumnIterator::init(opts));
    RETURN_IF_ERROR(_json_iter->init(opts));

    for (auto& p : _target_paths) {
        opts.stats->dynamic_json_hits[p] += 1;
    }

    if (!opts.has_preaggregation && config::enable_lazy_dynamic_flat_json) {
        _is_direct = true;
        return Status::OK();
    }

    SCOPED_RAW_TIMER(&_opts.stats->json_init_ns);
    _flattener = std::make_unique<JsonFlattener>(_target_paths, _target_types, _need_remain);
    VLOG(2) << "JsonDynamicFlatIterator init, target: "
            << JsonFlatPath::debug_flat_json(_target_paths, _target_types, _need_remain);
    return Status::OK();
}

template <typename FUNC>
Status JsonDynamicFlatIterator::_dynamic_flat(Column* output, FUNC read_fn) {
    if (_is_direct) {
        return read_fn(_json_iter.get(), output);
    }
    auto proxy = output->clone_empty();
    RETURN_IF_ERROR(read_fn(_json_iter.get(), proxy.get()));
    output->set_delete_state(proxy->delete_state());

    SCOPED_RAW_TIMER(&_opts.stats->json_flatten_ns);
    JsonColumn* json_data = nullptr;

    // 1. null column handle
    if (output->is_nullable()) {
        // append null column
        auto* output_nullable = down_cast<NullableColumn*>(output);
        auto* output_null = down_cast<NullColumn*>(output_nullable->null_column().get());

        auto* input_nullable = down_cast<NullableColumn*>(proxy.get());
        auto* input_null = down_cast<NullColumn*>(input_nullable->null_column().get());

        output_null->append(*input_null, 0, input_null->size());
        output_nullable->set_has_null(input_nullable->has_null() | output_nullable->has_null());

        // json column
        json_data = down_cast<JsonColumn*>(output_nullable->data_column().get());
    } else {
        json_data = down_cast<JsonColumn*>(output);
    }

    // 2. flat
    _flattener->flatten(proxy.get());
    auto result = _flattener->mutable_result();
    json_data->set_flat_columns(_target_paths, _target_types, std::move(result));
    output->check_or_die();
    return Status::OK();
}

Status JsonDynamicFlatIterator::next_batch(size_t* n, Column* dst) {
    auto read = [&](ColumnIterator* iter, Column* column) { return iter->next_batch(n, column); };
    return _dynamic_flat(dst, read);
}

Status JsonDynamicFlatIterator::next_batch(const SparseRange<>& range, Column* dst) {
    auto read = [&](ColumnIterator* iter, Column* column) { return iter->next_batch(range, column); };
    return _dynamic_flat(dst, read);
}

Status JsonDynamicFlatIterator::fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) {
    auto read = [&](ColumnIterator* iter, Column* column) { return iter->fetch_values_by_rowid(rowids, size, column); };
    return _dynamic_flat(values, read);
}

Status JsonDynamicFlatIterator::seek_to_first() {
    return _json_iter->seek_to_first();
}

Status JsonDynamicFlatIterator::seek_to_ordinal(ordinal_t ord) {
    return _json_iter->seek_to_ordinal(ord);
}

Status JsonDynamicFlatIterator::get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                           const ColumnPredicate* del_predicate,
                                                           SparseRange<>* row_ranges, CompoundNodeType pred_relation,
                                                           const Range<>* src_range) {
    return _json_iter->get_row_ranges_by_zone_map(predicates, del_predicate, row_ranges, pred_relation, src_range);
}

class JsonMergeIterator final : public ColumnIterator {
public:
    JsonMergeIterator(ColumnReader* reader, std::unique_ptr<ColumnIterator> null_iter,
                      std::vector<std::unique_ptr<ColumnIterator>> all_iter, const std::vector<std::string>& src_paths,
                      const std::vector<LogicalType>& src_types)
            : _reader(reader),
              _null_iter(std::move(null_iter)),
              _all_iter(std::move(all_iter)),
              _src_paths(std::move(src_paths)),
              _src_types(std::move(src_types)){};

    ~JsonMergeIterator() override = default;

    [[nodiscard]] Status init(const ColumnIteratorOptions& opts) override;

    [[nodiscard]] Status next_batch(size_t* n, Column* dst) override;

    [[nodiscard]] Status next_batch(const SparseRange<>& range, Column* dst) override;

    [[nodiscard]] Status seek_to_first() override;

    [[nodiscard]] Status seek_to_ordinal(ordinal_t ord) override;

    ordinal_t get_current_ordinal() const override { return _all_iter[0]->get_current_ordinal(); }

    ordinal_t num_rows() const override { return _all_iter[0]->num_rows(); }

    /// for vectorized engine
    [[nodiscard]] Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                    const ColumnPredicate* del_predicate, SparseRange<>* row_ranges,
                                                    CompoundNodeType pred_relation,
                                                    const Range<>* src_range = nullptr) override;

    [[nodiscard]] Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override;

    StatusOr<std::vector<std::pair<int64_t, int64_t>>> get_io_range_vec(const SparseRange<>& range,
                                                                        Column* dst) override;

    std::string name() const override { return "JsonMergeIterator"; }

private:
    template <typename FUNC>
    Status _merge(JsonColumn* dst, FUNC func);

private:
    ColumnReader* _reader;

    std::unique_ptr<ColumnIterator> _null_iter;
    std::vector<std::unique_ptr<ColumnIterator>> _all_iter;
    std::vector<std::string> _src_paths;
    std::vector<LogicalType> _src_types;
    Columns _src_column_modules;

    std::unique_ptr<JsonMerger> _merger;
};

Status JsonMergeIterator::init(const ColumnIteratorOptions& opts) {
    RETURN_IF_ERROR(ColumnIterator::init(opts));
    if (_null_iter != nullptr) {
        RETURN_IF_ERROR(_null_iter->init(opts));
    }

    for (auto& iter : _all_iter) {
        RETURN_IF_ERROR(iter->init(opts));
    }

    bool has_remain = _all_iter.size() != _src_paths.size();
    VLOG(2) << "JsonMergeIterator init, source: " << JsonFlatPath::debug_flat_json(_src_paths, _src_types, has_remain);
    DCHECK(_all_iter.size() == _src_paths.size() || _all_iter.size() == _src_paths.size() + 1);
    for (int i = 0; i < _src_paths.size(); i++) {
        auto column = ColumnHelper::create_column(TypeDescriptor(_src_types[i]), true);
        _src_column_modules.emplace_back(std::move(column));
    }

    if (_all_iter.size() != _src_paths.size()) {
        // remain
        _src_column_modules.emplace_back(JsonColumn::create());
    }

    opts.stats->merge_json_hits["MergeAllSubfield"] += 1;
    SCOPED_RAW_TIMER(&_opts.stats->json_init_ns);
    _merger = std::make_unique<JsonMerger>(_src_paths, _src_types, has_remain);

    return Status::OK();
}

template <typename FUNC>
Status JsonMergeIterator::_merge(JsonColumn* dst, FUNC func) {
    Columns all_columns;
    for (size_t i = 0; i < _all_iter.size(); i++) {
        auto iter = _all_iter[i].get();
        auto c = _src_column_modules[i]->clone_empty();
        RETURN_IF_ERROR(func(iter, c.get()));
        all_columns.emplace_back(std::move(c));
    }

    SCOPED_RAW_TIMER(&_opts.stats->json_merge_ns);
    auto json = _merger->merge(all_columns);
    dst->append(*json, 0, json->size());
    dst->check_or_die();
    return Status::OK();
}

Status JsonMergeIterator::next_batch(size_t* n, Column* dst) {
    JsonColumn* json_column = nullptr;
    NullColumn* null_column = nullptr;
    if (dst->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(dst);
        json_column = down_cast<JsonColumn*>(nullable_column->data_column().get());
        null_column = down_cast<NullColumn*>(nullable_column->null_column().get());
    } else {
        json_column = down_cast<JsonColumn*>(dst);
    }

    CHECK((_null_iter == nullptr && null_column == nullptr) || (_null_iter != nullptr && null_column != nullptr));

    // 1. Read null column
    if (_null_iter != nullptr) {
        RETURN_IF_ERROR(_null_iter->next_batch(n, null_column));
        down_cast<NullableColumn*>(dst)->update_has_null();
    }

    auto func = [&](ColumnIterator* iter, Column* column) { return iter->next_batch(n, column); };
    auto ret = _merge(json_column, func);
    dst->check_or_die();
    return ret;
}

Status JsonMergeIterator::next_batch(const SparseRange<>& range, Column* dst) {
    JsonColumn* json_column = nullptr;
    NullColumn* null_column = nullptr;
    if (dst->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(dst);
        json_column = down_cast<JsonColumn*>(nullable_column->data_column().get());
        null_column = down_cast<NullColumn*>(nullable_column->null_column().get());
    } else {
        json_column = down_cast<JsonColumn*>(dst);
    }

    CHECK((_null_iter == nullptr && null_column == nullptr) || (_null_iter != nullptr && null_column != nullptr));

    // 1. Read null column
    if (_null_iter != nullptr) {
        RETURN_IF_ERROR(_null_iter->next_batch(range, null_column));
        down_cast<NullableColumn*>(dst)->update_has_null();
    }

    auto func = [&](ColumnIterator* iter, Column* column) { return iter->next_batch(range, column); };
    auto ret = _merge(json_column, func);
    dst->check_or_die();
    return ret;
}

Status JsonMergeIterator::fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* dst) {
    JsonColumn* json_column = nullptr;
    NullColumn* null_column = nullptr;
    if (dst->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(dst);
        json_column = down_cast<JsonColumn*>(nullable_column->data_column().get());
        null_column = down_cast<NullColumn*>(nullable_column->null_column().get());
    } else {
        json_column = down_cast<JsonColumn*>(dst);
    }

    CHECK((_null_iter == nullptr && null_column == nullptr) || (_null_iter != nullptr && null_column != nullptr));

    // 1. Read null column
    if (_null_iter != nullptr) {
        RETURN_IF_ERROR(_null_iter->fetch_values_by_rowid(rowids, size, null_column));
        down_cast<NullableColumn*>(dst)->update_has_null();
    }

    auto func = [&](ColumnIterator* iter, Column* column) { return iter->fetch_values_by_rowid(rowids, size, column); };
    auto ret = _merge(json_column, func);
    dst->check_or_die();
    return ret;
}

Status JsonMergeIterator::seek_to_first() {
    for (auto& iter : _all_iter) {
        RETURN_IF_ERROR(iter->seek_to_first());
    }

    if (_null_iter != nullptr) {
        RETURN_IF_ERROR(_null_iter->seek_to_first());
    }
    return Status::OK();
}

Status JsonMergeIterator::seek_to_ordinal(ordinal_t ord) {
    for (auto& iter : _all_iter) {
        RETURN_IF_ERROR(iter->seek_to_ordinal(ord));
    }

    if (_null_iter != nullptr) {
        RETURN_IF_ERROR(_null_iter->seek_to_ordinal(ord));
    }
    return Status::OK();
}

Status JsonMergeIterator::get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                     const ColumnPredicate* del_predicate, SparseRange<>* row_ranges,
                                                     CompoundNodeType pred_relation, const Range<>* src_range) {
    row_ranges->add({0, static_cast<rowid_t>(_reader->num_rows())});
    return Status::OK();
}

StatusOr<std::vector<std::pair<int64_t, int64_t>>> JsonMergeIterator::get_io_range_vec(const SparseRange<>& range,
                                                                                       Column* dst) {
    std::vector<std::pair<int64_t, int64_t>> res;
    if (_null_iter != nullptr) {
        ASSIGN_OR_RETURN(auto vec, _null_iter->get_io_range_vec(range, dst));
        res.insert(res.end(), vec.begin(), vec.end());
    }

    for (size_t i = 0; i < _all_iter.size(); i++) {
        ASSIGN_OR_RETURN(auto vec, _all_iter[i]->get_io_range_vec(range, dst));
        res.insert(res.end(), vec.begin(), vec.end());
    }
    return res;
}

class JsonExtractIterator final : public ColumnIteratorDecorator {
public:
    explicit JsonExtractIterator(ColumnIteratorUPtr source_iter, bool source_nullable, JsonPath path, LogicalType type)
            : ColumnIteratorDecorator(source_iter.release(), kTakesOwnership),
              _path(std::move(path)),
              _type(type),
              _state(ExecEnv::GetInstance()),
              _mem_pool(),
              _source_chunk() {
        // prepare the source chunk
        auto column = ColumnHelper::create_column(TypeDescriptor::create_json_type(), source_nullable);
        _source_chunk.append_column(std::move(column), SlotId(0));
        auto path_column = ColumnHelper::create_const_column<TYPE_VARCHAR>(_path.to_string(), 1);
        _source_chunk.append_column(std::move(path_column), SlotId(1));

        // prepare the function
        TypeDescriptor return_type(_type);
        std::vector<TypeDescriptor> arg_types = {TypeDescriptor::create_json_type(), TypeDescriptor(TYPE_VARCHAR)};
        _fn_context.reset(FunctionContext::create_context(&_state, &_mem_pool, return_type, arg_types));
        auto const_path = ColumnHelper::create_const_column<TYPE_VARCHAR>(_path.to_string(), 1);
        _fn_context->set_constant_columns(Columns{nullptr, const_path});
    }

    ~JsonExtractIterator() override {
        auto st = JsonFunctions::native_json_path_close(_fn_context.get(),
                                                        FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        LOG_IF(WARNING, !st.ok()) << "close json path function context failed path=" << _path.to_string()
                                  << " status=" << st;
    }

    Status init(const ColumnIteratorOptions& opts) override {
        RETURN_IF_ERROR(JsonFunctions::native_json_path_prepare(_fn_context.get(),
                                                                FunctionContext::FunctionStateScope::FRAGMENT_LOCAL));
        // record the hits into stats
        opts.stats->extract_json_hits[_path.to_string()]++;
        return _parent->init(opts);
    }

    DISALLOW_COPY_AND_MOVE(JsonExtractIterator);

    Status next_batch(size_t* n, Column* dst) override {
        _source_chunk.reset();
        auto source_column = _source_chunk.get_column_by_index(0);
        RETURN_IF_ERROR(_parent->next_batch(n, source_column.get()));
        return do_extract(dst);
    }

    Status next_batch(const SparseRange<>& range, Column* dst) override {
        _source_chunk.reset();
        auto source_column = _source_chunk.get_column_by_index(0);
        RETURN_IF_ERROR(_parent->next_batch(range, source_column.get()));
        return do_extract(dst);
    }

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override {
        _source_chunk.reset();
        auto source_column = _source_chunk.get_column_by_index(0);
        RETURN_IF_ERROR(_parent->fetch_values_by_rowid(rowids, size, source_column.get()));
        return do_extract(values);
    }

    StatusOr<std::vector<std::pair<int64_t, int64_t>>> get_io_range_vec(const SparseRange<>& range,
                                                                        Column* dst) override {
        auto source_column = _source_chunk.get_column_by_index(0);
        return _parent->get_io_range_vec(range, source_column.get());
    }

    std::string name() const override { return "JsonExtractIterator"; }

    // Not support various index
    bool has_original_bloom_filter_index() const override { return false; }
    bool has_ngram_bloom_filter_index() const override { return false; }

    Status get_row_ranges_by_bloom_filter(const std::vector<const ColumnPredicate*>& predicates,
                                          SparseRange<>* row_ranges) override {
        return Status::OK();
    }

private:
    Status do_extract(Column* target) {
        StatusOr<ColumnPtr> output_column;
        switch (_type) {
        case TYPE_BOOLEAN:
            output_column = JsonFunctions::get_native_json_bool(_fn_context.get(), _source_chunk.columns());
            break;
        case TYPE_INT:
            output_column = JsonFunctions::get_native_json_int(_fn_context.get(), _source_chunk.columns());
            break;
        case TYPE_BIGINT:
            output_column = JsonFunctions::get_native_json_bigint(_fn_context.get(), _source_chunk.columns());
            break;
        case TYPE_DOUBLE:
            output_column = JsonFunctions::get_native_json_double(_fn_context.get(), _source_chunk.columns());
            break;
        case TYPE_CHAR:
        case TYPE_VARCHAR:
            output_column = JsonFunctions::get_native_json_string(_fn_context.get(), _source_chunk.columns());
            break;
        default:
            return Status::RuntimeError(
                    fmt::format("JsonExtractIterator not supported type: {}", logical_type_to_string(_type)));
            break;
        }
        RETURN_IF_ERROR(output_column);

        ColumnPtr result = output_column.value();
        VLOG_ROW << "JsonExtractIterator extract from: " << _source_chunk.get_column_by_index(0)->debug_string();
        VLOG_ROW << "JsonExtractIterator extract: " << result->debug_string();
        if ((target->is_nullable() == result->is_nullable()) && (target->size() == 0)) {
            target->swap_column(*result);
        } else if (!target->is_nullable() && result->is_nullable()) {
            auto sz = result->size();
            target->append(*(down_cast<NullableColumn*>(result.get())->data_column()), 0, sz);
        } else {
            target->append(*result, 0, result->size());
        }

        return {};
    }

    JsonPath _path;
    LogicalType _type;
    RuntimeState _state;
    MemPool _mem_pool;
    std::unique_ptr<FunctionContext> _fn_context;

    // Hold the data fetched from the parent iterator
    Chunk _source_chunk;
};

StatusOr<std::unique_ptr<ColumnIterator>> create_json_flat_iterator(
        ColumnReader* reader, std::unique_ptr<ColumnIterator> null_iter,
        std::vector<std::unique_ptr<ColumnIterator>> iters, const std::vector<std::string>& target_paths,
        const std::vector<LogicalType>& target_types, const std::vector<std::string>& source_paths,
        const std::vector<LogicalType>& source_types, bool need_remain) {
    VLOG(2) << fmt::format("create_json_flat_iterator: num_iters={} source_paths={} target_paths={} need_remain={}",
                           iters.size(), fmt::join(source_paths, ","), fmt::join(target_paths, ","), need_remain);
    return std::make_unique<JsonFlatColumnIterator>(reader, std::move(null_iter), std::move(iters), target_paths,
                                                    target_types, source_paths, source_types, need_remain);
}

StatusOr<std::unique_ptr<ColumnIterator>> create_json_dynamic_flat_iterator(
        std::unique_ptr<ScalarColumnIterator> json_iter, const std::vector<std::string>& target_paths,
        const std::vector<LogicalType>& target_types, bool need_remain) {
    VLOG(2) << fmt::format("create_json_dynamic_flat_iterator: target_paths={} need_remain={}",
                           fmt::join(target_paths, ","), need_remain);
    return std::make_unique<JsonDynamicFlatIterator>(json_iter, target_paths, target_types, need_remain);
}

StatusOr<std::unique_ptr<ColumnIterator>> create_json_merge_iterator(
        ColumnReader* reader, std::unique_ptr<ColumnIterator> null_iter,
        std::vector<std::unique_ptr<ColumnIterator>> all_iters, const std::vector<std::string>& merge_paths,
        const std::vector<LogicalType>& merge_types) {
    VLOG(2) << fmt::format("create_json_merge_iterator: null={} merge_paths={}", null_iter != nullptr,
                           fmt::join(merge_paths, ","));
    return std::make_unique<JsonMergeIterator>(reader, std::move(null_iter), std::move(all_iters), merge_paths,
                                               merge_types);
}

StatusOr<ColumnIteratorUPtr> create_json_extract_iterator(ColumnIteratorUPtr source_iter, bool source_nullable,
                                                          const std::string& path, LogicalType type) {
    VLOG(2) << fmt::format("create_json_extract_iterator: path={} type={}", path, type);
    ASSIGN_OR_RETURN(auto normalized_path, JsonPath::parse(path));
    return std::make_unique<JsonExtractIterator>(std::move(source_iter), source_nullable, normalized_path, type);
}

} // namespace starrocks
