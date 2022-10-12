// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "storage/range.h"
#include "storage/rowset/column_iterator.h"

namespace starrocks {

class ColumnBlockView;
class ColumnVectorBatch;

namespace vectorized {
class Column;
} // namespace vectorized

class ArrayColumnIterator final : public ColumnIterator {
public:
    ArrayColumnIterator(ColumnIterator* null_iterator, ColumnIterator* array_size_iterator,
                        ColumnIterator* element_iterator);

    ~ArrayColumnIterator() override = default;

    Status init(const ColumnIteratorOptions& opts) override;

    Status next_batch(size_t* n, ColumnBlockView* dst, bool* has_null) override;

    Status next_batch(size_t* n, vectorized::Column* dst) override;

    Status next_batch(const vectorized::SparseRange& range, vectorized::Column* dst) override;

    Status seek_to_first() override;

    Status seek_to_ordinal(ordinal_t ord) override;

    ordinal_t get_current_ordinal() const override { return _array_size_iterator->get_current_ordinal(); }

    /// for vectorized engine
    Status get_row_ranges_by_zone_map(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                                      const vectorized::ColumnPredicate* del_predicate,
                                      vectorized::SparseRange* row_ranges) override {
        CHECK(false) << "array column does not has zone map index";
        return Status::OK();
    }

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, vectorized::Column* values) override;

private:
    std::unique_ptr<ColumnIterator> _null_iterator;
    std::unique_ptr<ColumnIterator> _array_size_iterator;
    std::unique_ptr<ColumnIterator> _element_iterator;

    std::unique_ptr<ColumnVectorBatch> _null_batch;
    std::unique_ptr<ColumnVectorBatch> _array_size_batch;
};

} // namespace starrocks
