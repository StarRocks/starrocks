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

#include "storage/rowset/struct_column_iterator.h"

#include "column/column_access_path.h"
#include "column/const_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/scalar_column_iterator.h"

namespace starrocks {

class StructColumnIterator final : public ColumnIterator {
public:
    StructColumnIterator(ColumnReader* _reader, std::unique_ptr<ColumnIterator> null_iter,
                         std::vector<std::unique_ptr<ColumnIterator>> field_iters, const ColumnAccessPath* path);

    ~StructColumnIterator() override = default;

    [[nodiscard]] Status init(const ColumnIteratorOptions& opts) override;

    [[nodiscard]] Status next_batch(size_t* n, Column* dst) override;

    [[nodiscard]] Status next_batch(const SparseRange<>& range, Column* dst) override;

    [[nodiscard]] Status seek_to_first() override;

    [[nodiscard]] Status seek_to_ordinal(ordinal_t ord) override;

    ordinal_t get_current_ordinal() const override { return _field_iters[0]->get_current_ordinal(); }

    /// for vectorized engine
    [[nodiscard]] Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                    const ColumnPredicate* del_predicate,
                                                    SparseRange<>* row_ranges) override;

    [[nodiscard]] Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override;

    [[nodiscard]] Status next_batch(size_t* n, Column* dst, ColumnAccessPath* path) override;

    [[nodiscard]] Status next_batch(const SparseRange<>& range, Column* dst, ColumnAccessPath* path) override;

    [[nodiscard]] Status fetch_subfield_by_rowid(const rowid_t* rowids, size_t size, Column* values) override;

private:
    ColumnReader* _reader;

    std::unique_ptr<ColumnIterator> _null_iter;
    std::vector<std::unique_ptr<ColumnIterator>> _field_iters;
    const ColumnAccessPath* _path;

    std::vector<uint8_t> _access_flags;
};

StatusOr<std::unique_ptr<ColumnIterator>> create_struct_iter(ColumnReader* _reader,
                                                             std::unique_ptr<ColumnIterator> null_iter,
                                                             std::vector<std::unique_ptr<ColumnIterator>> field_iters,
                                                             const ColumnAccessPath* path) {
    return std::make_unique<StructColumnIterator>(_reader, std::move(null_iter), std::move(field_iters),
                                                  std::move(path));
}

StructColumnIterator::StructColumnIterator(ColumnReader* reader, std::unique_ptr<ColumnIterator> null_iter,
                                           std::vector<std::unique_ptr<ColumnIterator>> field_iters,
                                           const ColumnAccessPath* path)
        : _reader(reader), _null_iter(std::move(null_iter)), _field_iters(std::move(field_iters)), _path(path) {}

Status StructColumnIterator::init(const ColumnIteratorOptions& opts) {
    if (_null_iter != nullptr) {
        RETURN_IF_ERROR(_null_iter->init(opts));
    }

    for (auto& iter : _field_iters) {
        RETURN_IF_ERROR(iter->init(opts));
    }

    if (_path != nullptr && !_path->children().empty()) {
        _access_flags.resize(_field_iters.size(), 0);
        for (const auto& child : _path->children()) {
            _access_flags[child->index()] = 1;
        }
    } else {
        _access_flags.resize(_field_iters.size(), 1);
    }

    return Status::OK();
}

Status StructColumnIterator::next_batch(size_t* n, Column* dst) {
    StructColumn* struct_column = nullptr;
    NullColumn* null_column = nullptr;
    if (dst->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(dst);

        struct_column = down_cast<StructColumn*>(nullable_column->data_column().get());
        null_column = down_cast<NullColumn*>(nullable_column->null_column().get());
    } else {
        struct_column = down_cast<StructColumn*>(dst);
    }

    // 1. Read null column
    if (_null_iter != nullptr) {
        RETURN_IF_ERROR(_null_iter->next_batch(n, null_column));
        down_cast<NullableColumn*>(dst)->update_has_null();
    }

    size_t row_count = 0;
    auto& fields = struct_column->fields_column();
    for (int i = 0; i < _field_iters.size(); ++i) {
        if (_access_flags[i]) {
            auto num_to_read = *n;
            RETURN_IF_ERROR(_field_iters[i]->next_batch(&num_to_read, fields[i].get()));
            row_count = fields[i]->size();
        }
    }

    for (int i = 0; i < _field_iters.size(); ++i) {
        if (!_access_flags[i]) {
            if (!fields[i]->is_constant()) {
                fields[i]->append_default(1);
                fields[i] = ConstColumn::create(fields[i], 1);
            }
            fields[i]->resize(row_count);
        }
    }

    return Status::OK();
}

Status StructColumnIterator::next_batch(const SparseRange<>& range, Column* dst) {
    StructColumn* struct_column = nullptr;
    NullColumn* null_column = nullptr;
    if (dst->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(dst);

        struct_column = down_cast<StructColumn*>(nullable_column->data_column().get());
        null_column = down_cast<NullColumn*>(nullable_column->null_column().get());
    } else {
        struct_column = down_cast<StructColumn*>(dst);
    }
    // Read null column
    if (_null_iter != nullptr) {
        RETURN_IF_ERROR(_null_iter->next_batch(range, null_column));
        down_cast<NullableColumn*>(dst)->update_has_null();
    }
    // Read all fields
    size_t row_count = 0;
    auto& fields = struct_column->fields_column();
    for (int i = 0; i < _field_iters.size(); ++i) {
        if (_access_flags[i]) {
            RETURN_IF_ERROR(_field_iters[i]->next_batch(range, fields[i].get()));
            row_count = fields[i]->size();
        }
    }

    for (int i = 0; i < _field_iters.size(); ++i) {
        if (!_access_flags[i]) {
            if (!fields[i]->is_constant()) {
                fields[i]->append_default(1);
                fields[i] = ConstColumn::create(fields[i], 1);
            }
            fields[i]->resize(row_count);
        }
    }
    return Status::OK();
}

Status StructColumnIterator::fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) {
    StructColumn* struct_column = nullptr;
    NullColumn* null_column = nullptr;
    // 1. Read null column
    if (_null_iter != nullptr) {
        auto* nullable_column = down_cast<NullableColumn*>(values);
        struct_column = down_cast<StructColumn*>(nullable_column->data_column().get());
        null_column = down_cast<NullColumn*>(nullable_column->null_column().get());
        RETURN_IF_ERROR(_null_iter->fetch_values_by_rowid(rowids, size, null_column));
        nullable_column->update_has_null();
    } else {
        struct_column = down_cast<StructColumn*>(values);
    }

    // read all fields
    auto& fields = struct_column->fields_column();
    size_t row_count = 0;
    for (int i = 0; i < _field_iters.size(); ++i) {
        if (_access_flags[i]) {
            RETURN_IF_ERROR(_field_iters[i]->fetch_values_by_rowid(rowids, size, fields[i].get()));
            row_count = fields[i]->size();
        }
    }

    for (int i = 0; i < _field_iters.size(); ++i) {
        if (!_access_flags[i]) {
            if (!fields[i]->is_constant()) {
                fields[i]->append_default(1);
                fields[i] = ConstColumn::create(fields[i], 1);
            }
            fields[i]->resize(row_count);
        }
    }
    return Status::OK();
}

Status StructColumnIterator::seek_to_first() {
    if (_null_iter != nullptr) {
        RETURN_IF_ERROR(_null_iter->seek_to_first());
    }
    for (auto& iter : _field_iters) {
        RETURN_IF_ERROR(iter->seek_to_first());
    }
    return Status::OK();
}

Status StructColumnIterator::seek_to_ordinal(ordinal_t ord) {
    if (_null_iter != nullptr) {
        RETURN_IF_ERROR(_null_iter->seek_to_ordinal(ord));
    }
    for (auto& iter : _field_iters) {
        RETURN_IF_ERROR(iter->seek_to_ordinal(ord));
    }
    return Status::OK();
}

Status StructColumnIterator::get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                        const ColumnPredicate* del_predicate,
                                                        SparseRange<>* row_ranges) {
    row_ranges->add({0, static_cast<rowid_t>(_reader->num_rows())});
    return Status::OK();
}

Status StructColumnIterator::next_batch(size_t* n, Column* dst, ColumnAccessPath* path) {
    if (path == nullptr || path->children().empty()) {
        return next_batch(n, dst);
    }

    // 1. init predicate access path
    std::vector<uint8_t> predicate_access_flags;
    std::vector<ColumnAccessPath*> predicate_child_paths;
    predicate_access_flags.resize(_field_iters.size(), 0);
    predicate_child_paths.resize(_field_iters.size(), nullptr);
    for (const auto& child : path->children()) {
        predicate_access_flags[child->index()] = 1;
        predicate_child_paths[child->index()] = child.get();
    }

    StructColumn* struct_column = nullptr;
    NullColumn* null_column = nullptr;
    if (dst->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(dst);
        struct_column = down_cast<StructColumn*>(nullable_column->data_column().get());
        null_column = down_cast<NullColumn*>(nullable_column->null_column().get());
    } else {
        struct_column = down_cast<StructColumn*>(dst);
    }

    // 2. Read null column
    if (_null_iter != nullptr) {
        RETURN_IF_ERROR(_null_iter->next_batch(n, null_column));
        down_cast<NullableColumn*>(dst)->update_has_null();
    }

    // 3. Read fields
    size_t row_count = 0;
    auto& fields = struct_column->fields_column();
    for (int i = 0; i < _field_iters.size(); ++i) {
        if (predicate_access_flags[i]) {
            auto num_to_read = *n;
            RETURN_IF_ERROR(_field_iters[i]->next_batch(&num_to_read, fields[i].get(), predicate_child_paths[i]));
            row_count = fields[i]->size();
        }
    }

    for (int i = 0; i < _field_iters.size(); ++i) {
        if (!predicate_access_flags[i]) {
            if (!fields[i]->is_constant()) {
                fields[i]->append_default(1);
                fields[i] = ConstColumn::create(fields[i], 1);
            }
            fields[i]->resize(row_count);
        }
    }
    return Status::OK();
}

Status StructColumnIterator::next_batch(const SparseRange<>& range, Column* dst, ColumnAccessPath* path) {
    if (path == nullptr || path->children().empty()) {
        return next_batch(range, dst);
    }

    std::vector<uint8_t> predicate_access_flags;
    std::vector<ColumnAccessPath*> predicate_child_paths;
    predicate_access_flags.resize(_field_iters.size(), 0);
    predicate_child_paths.resize(_field_iters.size(), nullptr);
    for (const auto& child : path->children()) {
        predicate_access_flags[child->index()] = 1;
        predicate_child_paths[child->index()] = child.get();
    }

    StructColumn* struct_column = nullptr;
    NullColumn* null_column = nullptr;
    if (dst->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(dst);

        struct_column = down_cast<StructColumn*>(nullable_column->data_column().get());
        null_column = down_cast<NullColumn*>(nullable_column->null_column().get());
    } else {
        struct_column = down_cast<StructColumn*>(dst);
    }
    // Read null column
    if (_null_iter != nullptr) {
        RETURN_IF_ERROR(_null_iter->next_batch(range, null_column));
        down_cast<NullableColumn*>(dst)->update_has_null();
    }
    // Read all fields
    size_t row_count = 0;
    auto& fields = struct_column->fields_column();
    for (int i = 0; i < _field_iters.size(); ++i) {
        if (!predicate_access_flags[i]) {
            continue;
        }
        RETURN_IF_ERROR(_field_iters[i]->next_batch(range, fields[i].get(), predicate_child_paths[i]));
        row_count = fields[i]->size();
    }

    for (int i = 0; i < _field_iters.size(); ++i) {
        if (!predicate_access_flags[i]) {
            if (!fields[i]->is_constant()) {
                fields[i]->append_default(1);
                fields[i] = ConstColumn::create(fields[i], 1);
            }
            fields[i]->resize(row_count);
        }
    }
    return Status::OK();
}

Status StructColumnIterator::fetch_subfield_by_rowid(const rowid_t* rowids, size_t size, Column* values) {
    StructColumn* struct_column = nullptr;
    // 1. null column was readed
    if (_null_iter != nullptr) {
        auto* nullable_column = down_cast<NullableColumn*>(values);
        struct_column = down_cast<StructColumn*>(nullable_column->data_column().get());
    } else {
        struct_column = down_cast<StructColumn*>(values);
    }

    // read all fields
    auto& fields = struct_column->fields_column();
    size_t row_count = 0;
    for (int i = 0; i < _field_iters.size(); ++i) {
        if (_access_flags[i]) {
            if (fields[i]->is_constant()) {
                // doesn't meterialized
                fields[i] = down_cast<ConstColumn*>(fields[i].get())->data_column()->clone_empty();
                RETURN_IF_ERROR(_field_iters[i]->fetch_values_by_rowid(rowids, size, fields[i].get()));
            } else {
                // handle nested struct
                // structA -> structB -> e (meterialized)
                //                    -> f (un-meterialized)
                RETURN_IF_ERROR(_field_iters[i]->fetch_subfield_by_rowid(rowids, size, fields[i].get()));
            }
            row_count = fields[i]->size();
        }
    }

    for (int i = 0; i < _field_iters.size(); ++i) {
        if (!_access_flags[i]) {
            if (!fields[i]->is_constant()) {
                fields[i]->append_default(1);
                fields[i] = ConstColumn::create(fields[i], 1);
            }
            fields[i]->resize(row_count);
        }
    }
    return Status::OK();
}

} // namespace starrocks
