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
#include "storage/rowset/column_iterator_decorator.h"
#include "storage/rowset/scalar_column_iterator.h"

namespace starrocks {
// DictCodeColumnIterator is a wrapper/proxy on another column iterator that will
// transform the invoking of `next_batch(size_t*, Column*)` to the invoking of
// `next_dict_codes(size_t*, Column*)`.
class DictCodeColumnIterator final : public ColumnIteratorDecorator {
public:
    // does not take the ownership of |iter|.
    explicit DictCodeColumnIterator(ColumnId cid, ColumnIterator* iter)
            : ColumnIteratorDecorator(iter, kDontTakeOwnership), _cid(cid) {}

    ~DictCodeColumnIterator() override = default;

    ColumnId column_id() const { return _cid; }

    Status next_batch(size_t* n, Column* dst) override { return _parent->next_dict_codes(n, dst); }

    Status next_batch(const SparseRange<>& range, Column* dst) override { return _parent->next_dict_codes(range, dst); }

private:
    ColumnId _cid;
};

// GlobalDictCodeColumnIterator is similar to DictCodeColumnIterator
// used in global dict optimize
class GlobalDictCodeColumnIterator final : public ColumnIteratorDecorator {
public:
    explicit GlobalDictCodeColumnIterator(ColumnId cid, ColumnIterator* iter, int16_t* code_convert_data,
                                          int32_t dict_size)
            : ColumnIteratorDecorator(iter, kDontTakeOwnership),
              _cid(cid),
              _local_to_global(code_convert_data),
              _dict_size(dict_size) {}

    ~GlobalDictCodeColumnIterator() override = default;

    ColumnId column_id() const { return _cid; }

    Status next_batch(size_t* n, Column* dst) override { return _parent->next_dict_codes(n, dst); }

    Status next_batch(const SparseRange<>& range, Column* dst) override { return _parent->next_dict_codes(range, dst); }

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override {
        if (_local_dict_code_col == nullptr) {
            _local_dict_code_col = _new_local_dict_col(values);
        }
        _local_dict_code_col->reset_column();
        RETURN_IF_ERROR(_parent->fetch_dict_codes_by_rowid(rowids, size, _local_dict_code_col.get()));
        RETURN_IF_ERROR(decode_dict_codes(*_local_dict_code_col, values));
        _swap_null_columns(_local_dict_code_col.get(), values);
        values->set_delete_state(_local_dict_code_col->delete_state());
        return Status::OK();
    }

    Status next_dict_codes(size_t* n, Column* dst) override {
        return Status::NotSupported("GlobalDictCodeColumnIterator does not support next_dict_codes");
    }

    Status decode_dict_codes(const Column& codes, Column* words) override;

    Status decode_dict_codes(const int32_t* codes, size_t size, Column* words) override {
        return Status::NotSupported("unsupport decode_dict_codes in GlobalDictCodeColumnIterator");
    }

    static Status build_code_convert_map(ColumnIterator* file_column_iter, GlobalDictMap* global_dict,
                                         std::vector<int16_t>* code_convert_map);

private:
    Status decode_array_dict_codes(const Column& codes, Column* words);

    Status decode_string_dict_codes(const Column& codes, Column* words);

private:
    // create a new empty local dict column
    MutableColumnPtr _new_local_dict_col(Column* src);
    // swap null column between src and dst column
    void _swap_null_columns(Column* src, Column* dst);

    ColumnId _cid;

    // _local_to_global[-1] is accessable
    int16_t* _local_to_global;
    int32_t _dict_size;

    ColumnPtr _local_dict_code_col;
};

} // namespace starrocks
