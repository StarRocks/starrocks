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

#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/rowset/column_iterator.h"
#include "util/raw_container.h"

namespace starrocks {

class TypeInfo;
using TypeInfoPtr = std::shared_ptr<TypeInfo>;

// this column iterator is used to generate global row ids for each row in a segment.
class GlobalRowIdColumnIterator final : public ColumnIterator {
public:
    GlobalRowIdColumnIterator(uint32_t be_id, uint32_t seg_id) : _be_id(be_id), _seg_id(seg_id) {}

    Status init(const ColumnIteratorOptions& opts) override { return Status::OK(); }

    Status seek_to_first() override {
        _current_rowid = 0;
        return Status::OK();
    }

    Status seek_to_ordinal(ordinal_t ord_idx) override {
        _current_rowid = ord_idx;
        return Status::OK();
    }

    Status next_batch(size_t* n, Column* dst) override {
        auto row_id_column = down_cast<RowIdColumn*>(dst);
        const size_t num = row_id_column->size();
        auto& be_ids = row_id_column->be_ids_column()->get_data();
        raw::stl_vector_resize_uninitialized(&be_ids, num + *n);
        for (size_t i = 0; i < *n; i++) {
            be_ids[num + i] = _be_id;
        }
        auto& seg_ids = row_id_column->seg_ids_column()->get_data();
        raw::stl_vector_resize_uninitialized(&seg_ids, num + *n);
        for (size_t i = 0; i < *n; i++) {
            seg_ids[num + i] = _seg_id;
        }
        auto& ord_ids = row_id_column->ord_ids_column()->get_data();
        raw::stl_vector_resize_uninitialized(&ord_ids, num + *n);
        for (size_t i = 0; i < *n; i++) {
            ord_ids[num + i] = _current_rowid + i;
        }
        _current_rowid += *n;

        return Status::OK();
    }

    Status next_batch(const SparseRange<>& range, Column* dst) override {
        auto row_id_column = down_cast<RowIdColumn*>(dst);
        SparseRangeIterator<> iter = range.new_iterator();
        size_t to_read = range.span_size();
        while (to_read > 0) {
            _current_rowid = iter.begin();
            Range<> r = iter.next(to_read);
            auto& be_ids = row_id_column->be_ids_column()->get_data();
            auto& seg_ids = row_id_column->seg_ids_column()->get_data();
            auto& ord_ids = row_id_column->ord_ids_column()->get_data();
            const size_t num = be_ids.size();
            raw::stl_vector_resize_uninitialized(&be_ids, num + r.span_size());
            for (size_t i = 0; i < r.span_size(); i++) {
                be_ids[num + i] = _be_id;
            }
            raw::stl_vector_resize_uninitialized(&seg_ids, num + r.span_size());
            for (size_t i = 0; i < r.span_size(); i++) {
                seg_ids[num + i] = _seg_id;
            }
            raw::stl_vector_resize_uninitialized(&ord_ids, num + r.span_size());
            for (size_t i = 0; i < r.span_size(); i++) {
                ord_ids[num + i] = _current_rowid + i;
            }

            _current_rowid += r.span_size();
            to_read -= r.span_size();
        }
        return Status::OK();
    }

    ordinal_t get_current_ordinal() const override { return _current_rowid; }

    ordinal_t num_rows() const override { return 0; }

    Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                      const ColumnPredicate* del_predicate, SparseRange<>* row_ranges,
                                      CompoundNodeType pred_relation) override {
        return Status::NotSupported("PlaceHolderColumnIterator does not support");
    }

    bool all_page_dict_encoded() const override { return false; }

    int dict_lookup(const Slice& word) override { return -1; }

    Status next_dict_codes(size_t* n, Column* dst) override {
        return Status::NotSupported("PlaceHolderColumnIterator does not support");
    }

    Status decode_dict_codes(const int32_t* codes, size_t size, Column* words) override {
        return Status::NotSupported("PlaceHolderColumnIterator does not support");
    }

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* dst) override {
        auto row_id_column = down_cast<RowIdColumn*>(dst);
        const size_t num = row_id_column->size();
        auto& be_ids = row_id_column->be_ids_column()->get_data();
        raw::stl_vector_resize_uninitialized(&be_ids, num + size);
        for (size_t i = 0; i < size; i++) {
            be_ids[num + i] = _be_id;
        }
        auto& seg_ids = row_id_column->seg_ids_column()->get_data();
        raw::stl_vector_resize_uninitialized(&seg_ids, num + size);
        for (size_t i = 0; i < size; i++) {
            seg_ids[num + i] = _seg_id;
        }
        auto& ord_ids = row_id_column->ord_ids_column()->get_data();
        raw::stl_vector_resize_uninitialized(&ord_ids, num + size);
        for (size_t i = 0; i < size; i++) {
            ord_ids[num + i] = rowids[i];
        }
        return Status::OK();
    }

private:
    uint32_t _be_id;
    uint32_t _seg_id;
    uint32_t _current_rowid = 0;
};

} // namespace starrocks
