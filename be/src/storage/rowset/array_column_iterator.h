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

#include "storage/range.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"

namespace starrocks {

class Column;
class ColumnAccessPath;

class ArrayColumnIterator final : public ColumnIterator {
public:
    ArrayColumnIterator(ColumnReader* reader, std::unique_ptr<ColumnIterator> null_iterator,
                        std::unique_ptr<ColumnIterator> array_size_iterator,
                        std::unique_ptr<ColumnIterator> element_iterator, const ColumnAccessPath* paths);

    ~ArrayColumnIterator() override = default;

    [[nodiscard]] Status init(const ColumnIteratorOptions& opts) override;

    [[nodiscard]] Status next_batch(size_t* n, Column* dst) override;

    [[nodiscard]] Status next_batch(const SparseRange<>& range, Column* dst) override;

    [[nodiscard]] Status seek_to_first() override;

    [[nodiscard]] Status seek_to_ordinal(ordinal_t ord) override;

    ordinal_t get_current_ordinal() const override { return _array_size_iterator->get_current_ordinal(); }

    /// for vectorized engine
    [[nodiscard]] Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                    const ColumnPredicate* del_predicate,
                                                    SparseRange<>* row_ranges) override;

    [[nodiscard]] Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override;

    // for support array<string>
    bool all_page_dict_encoded() const override;

    [[nodiscard]] Status fetch_all_dict_words(std::vector<Slice>* words) const override;

    [[nodiscard]] Status next_dict_codes(size_t* n, Column* dst) override;

    [[nodiscard]] Status next_dict_codes(const SparseRange<>& range, Column* dst) override;

    [[nodiscard]] Status fetch_dict_codes_by_rowid(const rowid_t* rowids, size_t size, Column* values) override;

    [[nodiscard]] Status decode_dict_codes(const int32_t* codes, size_t size, Column* words) override;

    int dict_size() override { return _element_iterator->dict_size(); }

private:
    [[nodiscard]] Status next_batch_null_offsets(size_t* n, UInt32Column* offsets, UInt8Column* nulls,
                                                 size_t* element_rows);
    [[nodiscard]] Status next_batch_null_offsets(const SparseRange<>& range, UInt32Column* offsets, UInt8Column* nulls,
                                                 SparseRange<>* element_range, size_t* element_rows);

private:
    ColumnReader* _reader;

    std::unique_ptr<ColumnIterator> _null_iterator;
    std::unique_ptr<ColumnIterator> _array_size_iterator;
    std::unique_ptr<ColumnIterator> _element_iterator;
    const ColumnAccessPath* _path;

    bool _access_values = true;
    bool _is_string_element = false;
};

} // namespace starrocks
