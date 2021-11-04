// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "gutil/casts.h"
#include "storage/rowset/segment_v2/column_iterator.h"
#include "storage/rowset/segment_v2/common.h"
#include "storage/vectorized/range.h"
#include "util/raw_container.h"

namespace starrocks::vectorized {

// Instead of return a batch of column values, RowIdColumnIterator just return a batch
// of row id when you call `next_batch`.
// This is used for late materialization, check `SegmentIterator` for a reference.
class RowIdColumnIterator final : public starrocks::segment_v2::ColumnIterator {
    using ColumnIterator = starrocks::segment_v2::ColumnIterator;
    using ColumnIteratorOptions = starrocks::segment_v2::ColumnIteratorOptions;
    using ordinal_t = starrocks::segment_v2::ordinal_t;
    using rowid_t = starrocks::segment_v2::rowid_t;
    using RowRanges = starrocks::segment_v2::RowRanges;

public:
    RowIdColumnIterator() {}

    ~RowIdColumnIterator() override = default;

    Status init(const ColumnIteratorOptions& opts) override {
        _opts = opts;
        return Status::OK();
    }

    Status seek_to_first() override {
        _current_rowid = 0;
        return Status::OK();
    }

    Status seek_to_ordinal(ordinal_t ord) override {
        _current_rowid = ord;
        return Status::OK();
    }

    Status next_batch(size_t* n, vectorized::Column* dst) override {
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

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, vectorized::Column* values) override {
        return Status::NotSupported("Not supported by RowIdColumnIterator: fetch_values_by_rowid");
    }

    ordinal_t get_current_ordinal() const override { return _current_rowid; }

    Status next_batch(size_t* n, ColumnBlockView* dst, bool* has_null) override {
        return Status::NotSupported("Not supported by RowIdColumnIterator: next_batch");
    }

    Status get_row_ranges_by_zone_map(CondColumn* cond_column, CondColumn* delete_condition,
                                      RowRanges* row_ranges) override {
        return Status::NotSupported("Not supported by RowIdColumnIterator: get_row_ranges_by_zone_map");
    }

    Status get_row_ranges_by_bloom_filter(CondColumn* cond_column, RowRanges* row_ranges) override {
        return Status::NotSupported("Not supported by RowIdColumnIterator: get_row_ranges_by_bloom_filter");
    }

    Status get_row_ranges_by_zone_map(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                                      const vectorized::ColumnPredicate* del_predicate,
                                      vectorized::SparseRange* row_ranges) override {
        return Status::NotSupported("Not supported by RowIdColumnIterator: get_row_ranges_by_zone_map");
    }

    bool all_page_dict_encoded() const override { return false; }

    int dict_lookup(const Slice& word) override { return -1; }

    Status next_dict_codes(size_t* n, vectorized::Column* dst) override {
        return Status::NotSupported("Not supported by RowIdColumnIterator: next_dict_codes");
    }

    Status decode_dict_codes(const int32_t* codes, size_t size, vectorized::Column* words) override {
        return Status::NotSupported("Not supported by RowIdColumnIterator: decode_dict_codes");
    }

private:
    ColumnIteratorOptions _opts;
    ordinal_t _current_rowid = 0;
};

} // namespace starrocks::vectorized
