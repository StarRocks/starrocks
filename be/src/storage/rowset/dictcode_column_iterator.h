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

#include <memory>

#include "column/column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/expr_context.h"
#include "runtime/global_dict/config.h"
#include "runtime/global_dict/dict_column.h"
#include "runtime/global_dict/types.h"
#include "simd/gather.h"
#include "storage/range.h"
<<<<<<< HEAD
#include "storage/rowset/column_iterator.h"
=======
#include "storage/rowset/column_iterator_decorator.h"
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
#include "storage/rowset/scalar_column_iterator.h"

namespace starrocks {
// DictCodeColumnIterator is a wrapper/proxy on another column iterator that will
// transform the invoking of `next_batch(size_t*, Column*)` to the invoking of
// `next_dict_codes(size_t*, Column*)`.
<<<<<<< HEAD
class DictCodeColumnIterator final : public ColumnIterator {
public:
    using Column = starrocks::Column;
    using ColumnPredicate = starrocks::ColumnPredicate;
    // does not take the ownership of |iter|.
    DictCodeColumnIterator(ColumnId cid, ColumnIterator* iter) : _cid(cid), _col_iter(iter) {}
=======
class DictCodeColumnIterator final : public ColumnIteratorDecorator {
public:
    // does not take the ownership of |iter|.
    explicit DictCodeColumnIterator(ColumnId cid, ColumnIterator* iter)
            : ColumnIteratorDecorator(iter, kDontTakeOwnership), _cid(cid) {}
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    ~DictCodeColumnIterator() override = default;

    ColumnId column_id() const { return _cid; }

<<<<<<< HEAD
    ColumnIterator* column_iterator() const { return _col_iter; }

    Status next_batch(size_t* n, Column* dst) override { return _col_iter->next_dict_codes(n, dst); }

    Status next_batch(const SparseRange& range, Column* dst) override { return _col_iter->next_dict_codes(range, dst); }

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override {
        return _col_iter->fetch_values_by_rowid(rowids, size, values);
    }

    Status seek_to_first() override { return _col_iter->seek_to_first(); }

    Status seek_to_ordinal(ordinal_t ord) override { return _col_iter->seek_to_ordinal(ord); }

    ordinal_t get_current_ordinal() const override { return _col_iter->get_current_ordinal(); }

    bool all_page_dict_encoded() const override { return _col_iter->all_page_dict_encoded(); }

    int dict_lookup(const Slice& word) override { return _col_iter->dict_lookup(word); }

    Status next_dict_codes(size_t* n, Column* dst) override { return _col_iter->next_dict_codes(n, dst); }

    Status decode_dict_codes(const int32_t* codes, size_t size, Column* words) override {
        return _col_iter->decode_dict_codes(codes, size, words);
    }

    Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                      const ColumnPredicate* del_predicate, SparseRange* row_ranges) override {
        return _col_iter->get_row_ranges_by_zone_map(predicates, del_predicate, row_ranges);
    }

private:
    ColumnId _cid;
    ColumnIterator* _col_iter;
=======
    Status next_batch(size_t* n, Column* dst) override { return _parent->next_dict_codes(n, dst); }

    Status next_batch(const SparseRange<>& range, Column* dst) override { return _parent->next_dict_codes(range, dst); }

private:
    ColumnId _cid;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
};

// GlobalDictCodeColumnIterator is similar to DictCodeColumnIterator
// used in global dict optimize
<<<<<<< HEAD
class GlobalDictCodeColumnIterator final : public ColumnIterator {
public:
    using Column = starrocks::Column;
    using ColumnPredicate = starrocks::ColumnPredicate;
    using GlobalDictMap = starrocks::GlobalDictMap;
    using LowCardDictColumn = starrocks::LowCardDictColumn;

    GlobalDictCodeColumnIterator(ColumnId cid, ColumnIterator* iter, int16_t* code_convert_data, GlobalDictMap* gdict)
            : _cid(cid), _col_iter(iter), _local_to_global(code_convert_data) {}
=======
class GlobalDictCodeColumnIterator final : public ColumnIteratorDecorator {
public:
    explicit GlobalDictCodeColumnIterator(ColumnId cid, ColumnIterator* iter, int16_t* code_convert_data)
            : ColumnIteratorDecorator(iter, kDontTakeOwnership), _cid(cid), _local_to_global(code_convert_data) {}
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    ~GlobalDictCodeColumnIterator() override = default;

    ColumnId column_id() const { return _cid; }

<<<<<<< HEAD
    ColumnIterator* column_iterator() const { return _col_iter; }

    Status next_batch(size_t* n, Column* dst) override { return _col_iter->next_dict_codes(n, dst); }

    Status next_batch(const SparseRange& range, Column* dst) override { return _col_iter->next_dict_codes(range, dst); }

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override {
        if (_local_dict_code_col == nullptr) {
            _local_dict_code_col = _new_local_dict_col(values->is_nullable());
        }
        _local_dict_code_col->reset_column();
        RETURN_IF_ERROR(_col_iter->fetch_dict_codes_by_rowid(rowids, size, _local_dict_code_col.get()));
=======
    Status next_batch(size_t* n, Column* dst) override { return _parent->next_dict_codes(n, dst); }

    Status next_batch(const SparseRange<>& range, Column* dst) override { return _parent->next_dict_codes(range, dst); }

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override {
        if (_local_dict_code_col == nullptr) {
            _local_dict_code_col = _new_local_dict_col(values);
        }
        _local_dict_code_col->reset_column();
        RETURN_IF_ERROR(_parent->fetch_dict_codes_by_rowid(rowids, size, _local_dict_code_col.get()));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        RETURN_IF_ERROR(decode_dict_codes(*_local_dict_code_col, values));
        _swap_null_columns(_local_dict_code_col.get(), values);
        values->set_delete_state(_local_dict_code_col->delete_state());
        return Status::OK();
    }

<<<<<<< HEAD
    Status seek_to_first() override { return _col_iter->seek_to_first(); }

    Status seek_to_ordinal(ordinal_t ord) override { return _col_iter->seek_to_ordinal(ord); }

    ordinal_t get_current_ordinal() const override { return _col_iter->get_current_ordinal(); }

    bool all_page_dict_encoded() const override { return _col_iter->all_page_dict_encoded(); }

    // used for rewrite predicate
    // we need return local dict code
    int dict_lookup(const Slice& word) override { return _col_iter->dict_lookup(word); }

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    Status next_dict_codes(size_t* n, Column* dst) override {
        return Status::NotSupported("GlobalDictCodeColumnIterator does not support next_dict_codes");
    }

    Status decode_dict_codes(const Column& codes, Column* words) override;

    Status decode_dict_codes(const int32_t* codes, size_t size, Column* words) override {
        return Status::NotSupported("unsupport decode_dict_codes in GlobalDictCodeColumnIterator");
    }

<<<<<<< HEAD
    Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                      const ColumnPredicate* del_predicate, SparseRange* row_ranges) override {
        return _col_iter->get_row_ranges_by_zone_map(predicates, del_predicate, row_ranges);
    }

    static Status build_code_convert_map(ScalarColumnIterator* file_column_iter, GlobalDictMap* global_dict,
                                         std::vector<int16_t>* code_convert_map);

private:
    // create a new empty local dict column
    ColumnPtr _new_local_dict_col(bool nullable);
=======
    static Status build_code_convert_map(ColumnIterator* file_column_iter, GlobalDictMap* global_dict,
                                         std::vector<int16_t>* code_convert_map);

private:
    Status decode_array_dict_codes(const Column& codes, Column* words);

    Status decode_string_dict_codes(const Column& codes, Column* words);

private:
    // create a new empty local dict column
    ColumnPtr _new_local_dict_col(Column* src);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    // swap null column between src and dst column
    void _swap_null_columns(Column* src, Column* dst);

    ColumnId _cid;
<<<<<<< HEAD
    ColumnIterator* _col_iter;
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    // _local_to_global[-1] is accessable
    int16_t* _local_to_global;

    ColumnPtr _local_dict_code_col;
};

} // namespace starrocks
