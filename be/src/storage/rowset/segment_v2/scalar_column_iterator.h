// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/column_reader.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "column/fixed_length_column.h"
#include "storage/rowset/segment_v2/column_iterator.h"
#include "storage/rowset/segment_v2/ordinal_page_index.h"
#include "storage/rowset/segment_v2/page_handle.h"
#include "storage/rowset/segment_v2/parsed_page.h"

namespace starrocks {
namespace segment_v2 {

// TODO: rename to ScalarColumnIterator
class ScalarColumnIterator final : public ColumnIterator {
public:
    explicit ScalarColumnIterator(ColumnReader* reader);
    ~ScalarColumnIterator() override;

    Status init(const ColumnIteratorOptions& opts) override;

    Status seek_to_first() override;

    Status seek_to_ordinal(ordinal_t ord) override;

    Status seek_to_ordinal_and_calc_element_ordinal(ordinal_t ord) override;

    Status next_batch(size_t* n, ColumnBlockView* dst, bool* has_null) override;

    Status next_batch(size_t* n, vectorized::Column* dst) override;

    ordinal_t get_current_ordinal() const override { return _current_ordinal; }

    // get row ranges by zone map
    // - cond_column is user's query predicate
    // - delete_condition is delete predicate of one version
    Status get_row_ranges_by_zone_map(CondColumn* cond_column, CondColumn* delete_condition,
                                      RowRanges* row_ranges) override;

    Status get_row_ranges_by_bloom_filter(CondColumn* cond_column, RowRanges* row_ranges) override;

    Status get_row_ranges_by_zone_map(const std::vector<const vectorized::ColumnPredicate*>& predicate,
                                      const vectorized::ColumnPredicate* del_predicate,
                                      vectorized::SparseRange* range) override;

    Status get_row_ranges_by_bloom_filter(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                                          vectorized::SparseRange* range) override;

    bool all_page_dict_encoded() const override { return _all_dict_encoded; }

    Status fetch_all_dict_words(std::vector<Slice>* words) const override;

    int dict_lookup(const Slice& word) override;

    Status next_dict_codes(size_t* n, vectorized::Column* dst) override;

    Status decode_dict_codes(const int32_t* codes, size_t size, vectorized::Column* words) override;

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, vectorized::Column* values) override;

    Status fetch_dict_codes_by_rowid(const rowid_t* rowids, size_t size, vectorized::Column* values) override;

    ParsedPage* get_current_page() { return _page.get(); }

    bool is_nullable();

    int64_t element_ordinal() const override { return _element_ordinal; }

    // only work when all_page_dict_encoded was true.
    // used to acquire load local dict
    int dict_size();

private:
    static void _seek_to_pos_in_page(ParsedPage* page, ordinal_t offset_in_page);
    Status _load_next_page(bool* eos);
    Status _read_data_page(const OrdinalPageIndexIterator& iter);

    template <FieldType Type>
    int _do_dict_lookup(const Slice& word);

    template <FieldType Type>
    Status _do_next_dict_codes(size_t* n, vectorized::Column* dst);

    template <FieldType Type>
    Status _do_decode_dict_codes(const int32_t* codes, size_t size, vectorized::Column* words);

    template <FieldType Type>
    Status _do_init_dict_decoder();

    template <FieldType Type>
    Status _fetch_all_dict_words(std::vector<Slice>* words) const;

    template <typename ParseFunc>
    Status _fetch_by_rowid(const rowid_t* rowids, size_t size, vectorized::Column* values, ParseFunc&& page_parse);

    Status _load_dict_page();

    bool _contains_deleted_row(uint32_t page_index) const;

    ColumnReader* _reader;

    // 1. The _page represents current page.
    // 2. We define an operation is one seek and following read,
    //    If new seek is issued, the _page will be reset.
    // 3. When _page is null, it means that this reader can not be read.
    std::unique_ptr<ParsedPage> _page;

    // keep dict page decoder
    std::unique_ptr<PageDecoder> _dict_decoder;

    // keep dict page handle to avoid released
    PageHandle _dict_page_handle;

    // page iterator used to get next page when current page is finished.
    // This value will be reset when a new seek is issued
    OrdinalPageIndexIterator _page_iter;

    // current value ordinal
    ordinal_t _current_ordinal = 0;

    // page indexes those are DEL_PARTIAL_SATISFIED
    std::unordered_set<uint32_t> _delete_partial_satisfied_pages;

    int (ScalarColumnIterator::*_dict_lookup_func)(const Slice&) = nullptr;
    Status (ScalarColumnIterator::*_next_dict_codes_func)(size_t* n, vectorized::Column* dst) = nullptr;
    Status (ScalarColumnIterator::*_decode_dict_codes_func)(const int32_t* codes, size_t size,
                                                            vectorized::Column* words) = nullptr;
    Status (ScalarColumnIterator::*_init_dict_decoder_func)() = nullptr;

    Status (ScalarColumnIterator::*_fetch_all_dict_words_func)(std::vector<Slice>* words) const = nullptr;

    // whether all data pages are dict-encoded.
    bool _all_dict_encoded = false;

    // variable used for array column(offset, element)
    // It's used to get element ordinal for specfied offset value.
    int64_t _element_ordinal = 0;

    vectorized::UInt32Column _array_size;
};

} // namespace segment_v2
} // namespace starrocks
