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

#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"

namespace starrocks {
class ColumnAccessPath;

class MapColumnIterator final : public ColumnIterator {
public:
    MapColumnIterator(ColumnReader* reader, std::unique_ptr<ColumnIterator> nulls,
                      std::unique_ptr<ColumnIterator> offsets, std::unique_ptr<ColumnIterator> keys,
                      std::unique_ptr<ColumnIterator> values, const ColumnAccessPath* path);

    ~MapColumnIterator() override = default;

    [[nodiscard]] Status init(const ColumnIteratorOptions& opts) override;

    [[nodiscard]] Status next_batch(size_t* n, Column* dst) override;

    [[nodiscard]] Status next_batch(const SparseRange<>& range, Column* dst) override;

    [[nodiscard]] Status seek_to_first() override;

    [[nodiscard]] Status seek_to_ordinal(ordinal_t ord) override;

    ordinal_t get_current_ordinal() const override { return _offsets->get_current_ordinal(); }

    /// for vectorized engine
    [[nodiscard]] Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                    const ColumnPredicate* del_predicate,
                                                    SparseRange<>* row_ranges) override;

    [[nodiscard]] Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override;

private:
    ColumnReader* _reader;

    std::unique_ptr<ColumnIterator> _nulls;
    std::unique_ptr<ColumnIterator> _offsets;
    std::unique_ptr<ColumnIterator> _keys;
    std::unique_ptr<ColumnIterator> _values;
    const ColumnAccessPath* _path;

    bool _access_keys;
    bool _access_values;
};

} // namespace starrocks
