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

#include "gutil/casts.h"
#include "storage/range.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/common.h"
#include "util/raw_container.h"

namespace starrocks {

// Instead of return a batch of column values, RowIdColumnIterator just return a batch
// of row id when you call `next_batch`.
// This is used for late materialization, check `SegmentIterator` for a reference.
class RowIdColumnIterator final : public starrocks::ColumnIterator {
    using ColumnIterator = starrocks::ColumnIterator;
    using ColumnIteratorOptions = starrocks::ColumnIteratorOptions;
    using ordinal_t = starrocks::ordinal_t;
    using rowid_t = starrocks::rowid_t;

public:
    RowIdColumnIterator() = default;

    ~RowIdColumnIterator() override = default;

    [[nodiscard]] Status init(const ColumnIteratorOptions& opts) override {
        _opts = opts;
        return Status::OK();
    }

    [[nodiscard]] Status seek_to_first() override {
        _current_rowid = 0;
        return Status::OK();
    }

    [[nodiscard]] Status seek_to_ordinal(ordinal_t ord) override {
        _current_rowid = ord;
        return Status::OK();
    }

    [[nodiscard]] Status next_batch(size_t* n, Column* dst) override {
        Buffer<rowid_t>& v = down_cast<FixedLengthColumn<rowid_t>*>(dst)->get_data();
        const size_t sz = v.size();
        raw::stl_vector_resize_uninitialized(&v, sz + *n);
        rowid_t* ptr = &v[sz];
        for (size_t i = 0; i < *n; i++) {
            ptr[i] = _current_rowid + i;
        }
        _current_rowid += *n;
        return Status::OK();
    }

    [[nodiscard]] Status next_batch(const SparseRange<>& range, Column* dst) override {
        SparseRangeIterator<> iter = range.new_iterator();
        size_t to_read = range.span_size();
        while (to_read > 0) {
            _current_rowid = iter.begin();
            Range<> r = iter.next(to_read);
            Buffer<rowid_t>& v = down_cast<FixedLengthColumn<rowid_t>*>(dst)->get_data();
            const size_t sz = v.size();
            raw::stl_vector_resize_uninitialized(&v, sz + r.span_size());
            rowid_t* ptr = &v[sz];
            for (size_t i = 0; i < r.span_size(); i++) {
                ptr[i] = _current_rowid + i;
            }
            _current_rowid += r.span_size();
            to_read -= r.span_size();
        }
        return Status::OK();
    }

    [[nodiscard]] Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override {
        return Status::NotSupported("Not supported by RowIdColumnIterator: fetch_values_by_rowid");
    }

    ordinal_t get_current_ordinal() const override { return _current_rowid; }

    [[nodiscard]] Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                    const ColumnPredicate* del_predicate,
                                                    SparseRange<>* row_ranges) override {
        return Status::NotSupported("Not supported by RowIdColumnIterator: get_row_ranges_by_zone_map");
    }

    bool all_page_dict_encoded() const override { return false; }

    int dict_lookup(const Slice& word) override { return -1; }

    [[nodiscard]] Status next_dict_codes(size_t* n, Column* dst) override {
        return Status::NotSupported("Not supported by RowIdColumnIterator: next_dict_codes");
    }

    [[nodiscard]] Status decode_dict_codes(const int32_t* codes, size_t size, Column* words) override {
        return Status::NotSupported("Not supported by RowIdColumnIterator: decode_dict_codes");
    }

private:
    ColumnIteratorOptions _opts;
    ordinal_t _current_rowid = 0;
};

} // namespace starrocks
