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

#include "column/column_access_path.h"
#include "storage/range.h"
#include "storage/rowset/column_iterator.h"

namespace starrocks {
// FillSubfieldIterator is a wrapper/proxy on complex type(STRUCT) column iterator that will
// transform the invoking of `next_batch(size_t*, Column*)`
// For early materialize subfield
class FillSubfieldIterator final : public ColumnIterator {
public:
    using Column = starrocks::Column;
    using ColumnPredicate = starrocks::ColumnPredicate;
    // does not take the ownership of |iter|.
    FillSubfieldIterator(ColumnId cid, ColumnAccessPath* predicate_path, ColumnIterator* column_iter)
            : _cid(cid), _predicate_path(predicate_path), _column_iter(column_iter) {}

    ~FillSubfieldIterator() override = default;

    ColumnId column_id() const { return _cid; }

    [[nodiscard]] Status next_batch(size_t* n, Column* dst) override;

    [[nodiscard]] Status next_batch(const SparseRange<>& range, Column* dst) override;

    [[nodiscard]] Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override;

    [[nodiscard]] Status seek_to_first() override;

    [[nodiscard]] Status seek_to_ordinal(ordinal_t ord);

    ordinal_t get_current_ordinal() const override;

    [[nodiscard]] Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                    const ColumnPredicate* del_predicate,
                                                    SparseRange<>* row_ranges) override {
        return Status::NotSupported("");
    }

private:
    ColumnId _cid;
    ColumnAccessPath* _predicate_path;
    ColumnIterator* _column_iter;
};

} // namespace starrocks
