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
#include "gutil/casts.h"
#include "runtime/types.h"
#include "storage/rowset/column_iterator.h"
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

    [[nodiscard]] Status init(const ColumnIteratorOptions& opts) override;

    [[nodiscard]] Status next_batch(size_t* n, Column* dst) override;

    [[nodiscard]] Status next_batch(const SparseRange<>& range, Column* dst) override;

    [[nodiscard]] Status seek_to_first() override;

    [[nodiscard]] Status seek_to_ordinal(ordinal_t ord) override;

    ordinal_t get_current_ordinal() const override { return _flat_iters[0]->get_current_ordinal(); }

    ordinal_t num_rows() const override { return _flat_iters[0]->num_rows(); }

    [[nodiscard]] Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                    const ColumnPredicate* del_predicate,
                                                    SparseRange<>* row_ranges) override;

    [[nodiscard]] Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override;

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

    std::vector<ColumnPtr> _source_column_modules;
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
    VLOG(1) << "JsonFlatColumnIterator init, target: "
            << JsonFlatPath::debug_flat_json(_target_paths, _target_types, _need_remain)
            << ", source: " << JsonFlatPath::debug_flat_json(_source_paths, _source_types, has_remain);

    for (int i = 0; i < _source_paths.size(); i++) {
        auto column = ColumnHelper::create_column(TypeDescriptor(_source_types[i]), true);
        _source_column_modules.emplace_back(column);
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
    std::vector<ColumnPtr> columns;
    for (int i = 0; i < _source_column_modules.size(); i++) {
        columns.emplace_back(_source_column_modules[i]->clone_empty());
    }

    for (int i = 0; i < _flat_iters.size(); i++) {
        RETURN_IF_ERROR(read_fn(_flat_iters[i].get(), columns[i].get()));
    }

    if (_is_direct) {
        json_column->set_flat_columns(_source_paths, _source_types, columns);
    } else {
        RETURN_IF_ERROR(transformer->trans(columns));
        auto result = transformer->mutable_result();
        json_column->set_flat_columns(_target_paths, _target_types, result);
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
    return _read(json_column, read);
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
    return _read(json_column, read);
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

    return _read(json_column, read);
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
                                                          SparseRange<>* row_ranges) {
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

    [[nodiscard]] Status init(const ColumnIteratorOptions& opts) override;

    [[nodiscard]] Status next_batch(size_t* n, Column* dst) override;

    [[nodiscard]] Status next_batch(const SparseRange<>& range, Column* dst) override;

    [[nodiscard]] Status seek_to_first() override;

    [[nodiscard]] Status seek_to_ordinal(ordinal_t ord) override;

    ordinal_t get_current_ordinal() const override { return _json_iter->get_current_ordinal(); }

    ordinal_t num_rows() const override { return _json_iter->num_rows(); }

    /// for vectorized engine
    [[nodiscard]] Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                    const ColumnPredicate* del_predicate,
                                                    SparseRange<>* row_ranges) override;

    [[nodiscard]] Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override;

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
    VLOG(1) << "JsonDynamicFlatIterator init, target: "
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
    json_data->set_flat_columns(_target_paths, _target_types, result);
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
                                                           SparseRange<>* row_ranges) {
    return _json_iter->get_row_ranges_by_zone_map(predicates, del_predicate, row_ranges);
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
                                                    const ColumnPredicate* del_predicate,
                                                    SparseRange<>* row_ranges) override;

    [[nodiscard]] Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override;

private:
    template <typename FUNC>
    Status _merge(JsonColumn* dst, FUNC func);

private:
    ColumnReader* _reader;

    std::unique_ptr<ColumnIterator> _null_iter;
    std::vector<std::unique_ptr<ColumnIterator>> _all_iter;
    std::vector<std::string> _src_paths;
    std::vector<LogicalType> _src_types;
    std::vector<ColumnPtr> _src_column_modules;

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
    VLOG(1) << "JsonMergeIterator init, source: " << JsonFlatPath::debug_flat_json(_src_paths, _src_types, has_remain);
    DCHECK(_all_iter.size() == _src_paths.size() || _all_iter.size() == _src_paths.size() + 1);
    for (int i = 0; i < _src_paths.size(); i++) {
        auto column = ColumnHelper::create_column(TypeDescriptor(_src_types[i]), true);
        _src_column_modules.emplace_back(column);
    }

    if (_all_iter.size() != _src_paths.size()) {
        // remain
        _src_column_modules.emplace_back(JsonColumn::create());
    }

    for (auto& p : _src_paths) {
        opts.stats->merge_json_hits[p] += 1;
    }
    if (has_remain) {
        opts.stats->merge_json_hits["remain"] += 1;
    }
    SCOPED_RAW_TIMER(&_opts.stats->json_init_ns);
    _merger = std::make_unique<JsonMerger>(_src_paths, _src_types, has_remain);

    return Status::OK();
}

template <typename FUNC>
Status JsonMergeIterator::_merge(JsonColumn* dst, FUNC func) {
    std::vector<ColumnPtr> all_columns;
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
    return _merge(json_column, func);
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
    return _merge(json_column, func);
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
    return _merge(json_column, func);
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
                                                     const ColumnPredicate* del_predicate, SparseRange<>* row_ranges) {
    row_ranges->add({0, static_cast<rowid_t>(_reader->num_rows())});
    return Status::OK();
}

StatusOr<std::unique_ptr<ColumnIterator>> create_json_flat_iterator(
        ColumnReader* reader, std::unique_ptr<ColumnIterator> null_iter,
        std::vector<std::unique_ptr<ColumnIterator>> iters, const std::vector<std::string>& target_paths,
        const std::vector<LogicalType>& target_types, const std::vector<std::string>& source_paths,
        const std::vector<LogicalType>& source_types, bool need_remain) {
    return std::make_unique<JsonFlatColumnIterator>(reader, std::move(null_iter), std::move(iters), target_paths,
                                                    target_types, source_paths, source_types, need_remain);
}

StatusOr<std::unique_ptr<ColumnIterator>> create_json_dynamic_flat_iterator(
        std::unique_ptr<ScalarColumnIterator> json_iter, const std::vector<std::string>& target_paths,
        const std::vector<LogicalType>& target_types, bool need_remain) {
    return std::make_unique<JsonDynamicFlatIterator>(json_iter, target_paths, target_types, need_remain);
}

StatusOr<std::unique_ptr<ColumnIterator>> create_json_merge_iterator(
        ColumnReader* reader, std::unique_ptr<ColumnIterator> null_iter,
        std::vector<std::unique_ptr<ColumnIterator>> all_iters, const std::vector<std::string>& merge_paths,
        const std::vector<LogicalType>& merge_types) {
    return std::make_unique<JsonMergeIterator>(reader, std::move(null_iter), std::move(all_iters), merge_paths,
                                               merge_types);
}
} // namespace starrocks
