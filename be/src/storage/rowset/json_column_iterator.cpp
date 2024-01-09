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

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "column/column_access_path.h"
#include "column/const_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "common/status.h"
#include "gutil/casts.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/scalar_column_iterator.h"
#include "util/json_util.h"

namespace starrocks {

class JsonFlatColumnIterator final : public ColumnIterator {
public:
    JsonFlatColumnIterator(ColumnReader* _reader, std::unique_ptr<ColumnIterator>& null_iter,
                           std::vector<std::unique_ptr<ColumnIterator>>& field_iters,
                           std::vector<std::string>& flat_paths, ColumnAccessPath* path)
            : _reader(_reader),
              _null_iter(std::move(null_iter)),
              _flat_iters(std::move(field_iters)),
              _flat_paths(std::move(flat_paths)),
              _path(path) {}

    ~JsonFlatColumnIterator() override = default;

    [[nodiscard]] Status init(const ColumnIteratorOptions& opts) override;

    [[nodiscard]] Status next_batch(size_t* n, Column* dst) override;

    [[nodiscard]] Status next_batch(const SparseRange<>& range, Column* dst) override;

    [[nodiscard]] Status seek_to_first() override;

    [[nodiscard]] Status seek_to_ordinal(ordinal_t ord) override;

    ordinal_t get_current_ordinal() const override { return _flat_iters[0]->get_current_ordinal(); }

    /// for vectorized engine
    [[nodiscard]] Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                    const ColumnPredicate* del_predicate,
                                                    SparseRange<>* row_ranges) override;

    [[nodiscard]] Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override;

private:
    ColumnReader* _reader;

    std::unique_ptr<ColumnIterator> _null_iter;
    std::vector<std::unique_ptr<ColumnIterator>> _flat_iters;
    std::vector<std::string> _flat_paths;
    ColumnAccessPath* _path;
};

Status JsonFlatColumnIterator::init(const ColumnIteratorOptions& opts) {
    if (_null_iter != nullptr) {
        RETURN_IF_ERROR(_null_iter->init(opts));
    }

    for (auto& iter : _flat_iters) {
        RETURN_IF_ERROR(iter->init(opts));
    }

    //  update stats
    DCHECK(_path != nullptr);
    auto abs_path = _path->absolute_path();
    if (opts.stats->flat_json_hits.count(abs_path) == 0) {
        opts.stats->flat_json_hits[abs_path] = 1;
    } else {
        opts.stats->flat_json_hits[abs_path] = opts.stats->flat_json_hits[abs_path] + 1;
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
    json_column->init_flat_columns(_flat_paths);
    for (int i = 0; i < _flat_iters.size(); i++) {
        Column* flat_column = json_column->get_flat_field(i).get();
        RETURN_IF_ERROR(_flat_iters[i]->next_batch(n, flat_column));
    }

    return Status::OK();
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
    json_column->init_flat_columns(_flat_paths);
    for (int i = 0; i < _flat_iters.size(); i++) {
        Column* flat_column = json_column->get_flat_field(i).get();
        RETURN_IF_ERROR(_flat_iters[i]->next_batch(range, flat_column));
    }
    return Status::OK();
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
    json_column->init_flat_columns(_flat_paths);
    for (int i = 0; i < _flat_iters.size(); i++) {
        Column* flat_column = json_column->get_flat_field(i).get();
        RETURN_IF_ERROR(_flat_iters[i]->fetch_values_by_rowid(rowids, size, flat_column));
    }
    return Status::OK();
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
    JsonDynamicFlatIterator(std::unique_ptr<ScalarColumnIterator>& json_iter, std::vector<std::string>& flat_paths,
                            ColumnAccessPath* path)
            : _json_iter(std::move(json_iter)), _flat_paths(std::move(flat_paths)), _path(path){};

    ~JsonDynamicFlatIterator() override = default;

    [[nodiscard]] Status init(const ColumnIteratorOptions& opts) override;

    [[nodiscard]] Status next_batch(size_t* n, Column* dst) override;

    [[nodiscard]] Status next_batch(const SparseRange<>& range, Column* dst) override;

    [[nodiscard]] Status seek_to_first() override;

    [[nodiscard]] Status seek_to_ordinal(ordinal_t ord) override;

    ordinal_t get_current_ordinal() const override { return _json_iter->get_current_ordinal(); }

    /// for vectorized engine
    [[nodiscard]] Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                    const ColumnPredicate* del_predicate,
                                                    SparseRange<>* row_ranges) override;

    [[nodiscard]] Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override;

private:
    Status _flat_json(Column* input, Column* output);

private:
    std::unique_ptr<ScalarColumnIterator> _json_iter;
    std::vector<std::string> _flat_paths;
    ColumnAccessPath* _path;
};

Status JsonDynamicFlatIterator::init(const ColumnIteratorOptions& opts) {
    DCHECK(_path != nullptr);
    auto abs_path = _path->absolute_path();
    if (opts.stats->dynamic_json_hits.count(abs_path) == 0) {
        opts.stats->dynamic_json_hits[abs_path] = 1;
    } else {
        opts.stats->dynamic_json_hits[abs_path] = opts.stats->dynamic_json_hits[abs_path] + 1;
    }
    return _json_iter->init(opts);
}

Status JsonDynamicFlatIterator::_flat_json(Column* input, Column* output) {
    JsonColumn* json_data = nullptr;

    // 1. null column handle
    if (output->is_nullable()) {
        // append null column
        auto* output_nullable = down_cast<NullableColumn*>(output);
        auto* output_null = down_cast<NullColumn*>(output_nullable->null_column().get());

        auto* input_nullable = down_cast<NullableColumn*>(input);
        auto* input_null = down_cast<NullColumn*>(input_nullable->null_column().get());

        output_null->append(*input_null, 0, input_null->size());
        output_nullable->set_has_null(input_nullable->has_null() | output_nullable->has_null());

        // json column
        json_data = down_cast<JsonColumn*>(output_nullable->data_column().get());
    } else {
        json_data = down_cast<JsonColumn*>(output);
    }

    // 2. flat
    json_data->init_flat_columns(_flat_paths);

    JsonFlater flater(_flat_paths);
    flater.flatten(input, &(json_data->get_flat_fields()));
    return Status::OK();
}

Status JsonDynamicFlatIterator::next_batch(size_t* n, Column* dst) {
    auto proxy = dst->clone_empty();
    RETURN_IF_ERROR(_json_iter->next_batch(n, proxy.get()));
    return _flat_json(proxy.get(), dst);
}

Status JsonDynamicFlatIterator::next_batch(const SparseRange<>& range, Column* dst) {
    auto proxy = dst->clone_empty();
    RETURN_IF_ERROR(_json_iter->next_batch(range, proxy.get()));
    return _flat_json(proxy.get(), dst);
}

Status JsonDynamicFlatIterator::fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) {
    auto proxy = values->clone_empty();
    RETURN_IF_ERROR(_json_iter->fetch_values_by_rowid(rowids, size, proxy.get()));
    return _flat_json(proxy.get(), values);
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

StatusOr<std::unique_ptr<ColumnIterator>> create_json_flat_iterater(
        ColumnReader* reader, std::unique_ptr<ColumnIterator> null_iter,
        std::vector<std::unique_ptr<ColumnIterator>> field_iters, std::vector<std::string>& full_paths,
        ColumnAccessPath* path) {
    return std::make_unique<JsonFlatColumnIterator>(reader, null_iter, field_iters, full_paths, path);
}

StatusOr<std::unique_ptr<ColumnIterator>> create_json_dynamic_flat_iterater(
        std::unique_ptr<ScalarColumnIterator> json_iter, std::vector<std::string>& flat_paths, ColumnAccessPath* path) {
    return std::make_unique<JsonDynamicFlatIterator>(json_iter, flat_paths, path);
}

} // namespace starrocks
