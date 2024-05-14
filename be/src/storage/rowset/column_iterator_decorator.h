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

#include "common/ownership.h"
#include "gutil/macros.h"
#include "storage/rowset/column_iterator.h"

namespace starrocks {

class ColumnIteratorDecorator : public ColumnIterator {
public:
    explicit ColumnIteratorDecorator(ColumnIterator* parent, Ownership ownership)
            : _parent(parent), _release_guard((ownership == kTakesOwnership) ? _parent : nullptr) {}

    ~ColumnIteratorDecorator() override = default;

    DISALLOW_COPY_AND_MOVE(ColumnIteratorDecorator);

    ColumnIterator* parent() const { return _parent; }

    Status init(const ColumnIteratorOptions& opts) override { return _parent->init(opts); }

    Status seek_to_first() override { return _parent->seek_to_first(); }

    Status seek_to_ordinal(ordinal_t ord) override { return _parent->seek_to_ordinal(ord); }

    ordinal_t num_rows() const override { return _parent->num_rows(); }

    Status next_batch(size_t* n, Column* dst) override { return _parent->next_batch(n, dst); }

    Status next_batch(const SparseRange<>& range, Column* dst) override { return _parent->next_batch(range, dst); }

    ordinal_t get_current_ordinal() const override { return _parent->get_current_ordinal(); }

    Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                      const ColumnPredicate* del_predicate, SparseRange<>* row_ranges,
                                      CompoundNodeType pred_relation) override {
        return _parent->get_row_ranges_by_zone_map(predicates, del_predicate, row_ranges, pred_relation);
    }

    bool has_original_bloom_filter_index() const override { return _parent->has_original_bloom_filter_index(); }
    bool has_ngram_bloom_filter_index() const override { return _parent->has_ngram_bloom_filter_index(); }
    Status get_row_ranges_by_bloom_filter(const std::vector<const ColumnPredicate*>& predicates,
                                          SparseRange<>* row_ranges) override {
        return _parent->get_row_ranges_by_bloom_filter(predicates, row_ranges);
    }

    bool all_page_dict_encoded() const override { return _parent->all_page_dict_encoded(); }

    Status fetch_all_dict_words(std::vector<Slice>* words) const override {
        return _parent->fetch_all_dict_words(words);
    }

    int dict_size() override { return _parent->dict_size(); }

    int dict_lookup(const Slice& word) override { return _parent->dict_lookup(word); }

    Status next_dict_codes(size_t* n, Column* dst) override { return _parent->next_dict_codes(n, dst); }

    Status next_dict_codes(const SparseRange<>& range, Column* dst) override {
        return _parent->next_dict_codes(range, dst);
    }

    Status decode_dict_codes(const int32_t* codes, size_t size, Column* words) override {
        return _parent->decode_dict_codes(codes, size, words);
    }

    int64_t element_ordinal() const override { return _parent->element_ordinal(); }

    Status seek_to_ordinal_and_calc_element_ordinal(ordinal_t ord) override {
        return _parent->seek_to_ordinal_and_calc_element_ordinal(ord);
    }

    Status decode_dict_codes(const Column& codes, Column* words) override {
        return _parent->decode_dict_codes(codes, words);
    }

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override {
        return _parent->fetch_values_by_rowid(rowids, size, values);
    }

    Status fetch_dict_codes_by_rowid(const rowid_t* rowids, size_t size, Column* values) override {
        return _parent->fetch_dict_codes_by_rowid(rowids, size, values);
    }

    Status next_batch(size_t* n, Column* dst, ColumnAccessPath* path) override {
        return _parent->next_batch(n, dst, path);
    }

    Status next_batch(const SparseRange<>& range, Column* dst, ColumnAccessPath* path) override {
        return _parent->next_batch(range, dst, path);
    }

    Status fetch_subfield_by_rowid(const rowid_t* rowids, size_t size, Column* values) override {
        return _parent->fetch_subfield_by_rowid(rowids, size, values);
    }

protected:
    ColumnIterator* _parent;
    std::unique_ptr<ColumnIterator> _release_guard;
};

} // namespace starrocks
