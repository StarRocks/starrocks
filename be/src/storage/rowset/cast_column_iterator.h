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

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "gutil/macros.h"
#include "storage/rowset/column_iterator_decorator.h"

namespace starrocks {

class Column;
class ColumnRef;
class Expr;
class ObjectPool;

class CastColumnIterator : public ColumnIteratorDecorator {
public:
    // REQUIRES:
    //  - |source_iter| cannot be NULL
    //  - |source_type| and |target_type| both are scalar type
    explicit CastColumnIterator(std::unique_ptr<ColumnIterator> source_iter, const TypeDescriptor& source_type,
                                const TypeDescriptor& target_type, bool nullable_source);

    ~CastColumnIterator() override;

    DISALLOW_COPY_AND_MOVE(CastColumnIterator);

    Status next_batch(size_t* n, Column* dst) override;

    Status next_batch(const SparseRange<>& range, Column* dst) override;

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override;

    // Disable bloom filter in CastColumnIterator
    bool has_original_bloom_filter_index() const override { return false; }
    bool has_ngram_bloom_filter_index() const override { return false; }
    Status get_row_ranges_by_bloom_filter(const std::vector<const ColumnPredicate*>& predicates,
                                          SparseRange<>* row_ranges) override {
        return Status::OK();
    }

    // A zone map holds the source column's min/max, but ColumnReader parses those bytes with the
    // *predicate's* type. That is deliberate -- it is what lets a page written before a fast schema
    // evolution still be pruned by a predicate carrying the new type -- and it only holds while both
    // types read the same bytes the same way. This iterator exists precisely because the two types
    // differ, and for a pair such as BIGINT on disk against a VARCHAR predicate the reinterpretation
    // is nonsense: a page of a flat JSON subfield whose numeric min/max are 1 and 4096 is compared as
    // the strings "1" and "4096", under which "9" sorts above the maximum, so the page is dropped and
    // the matching row silently disappears. Forward the zone map only for the pairs whose stored form
    // orders alike, and hand back the whole column otherwise -- the same answer
    // JsonFlatColumnIterator and JsonMergeIterator give, and the counterpart of the bloom filter and
    // dictionary already disabled above.
    Status get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                      const ColumnPredicate* del_predicate, SparseRange<>* row_ranges,
                                      CompoundNodeType pred_relation, const Range<>* src_range = nullptr) override {
        if (!_zone_map_forwardable) {
            return ColumnIterator::get_row_ranges_by_zone_map(predicates, del_predicate, row_ranges, pred_relation,
                                                              src_range);
        }
        return ColumnIteratorDecorator::get_row_ranges_by_zone_map(predicates, del_predicate, row_ranges, pred_relation,
                                                                   src_range);
    }

    // Disable dict encoding
    bool all_page_dict_encoded() const override { return false; }
    int dict_lookup(const Slice& word) override {
        CHECK(false) << "unreachable";
        return 0;
    }

    StatusOr<std::vector<std::pair<int64_t, int64_t>>> get_io_range_vec(const SparseRange<>& range,
                                                                        Column* dst) override;

    std::string name() const override { return "CastColumnIterator"; }

private:
    Status do_cast(Column* target);

    std::unique_ptr<ObjectPool> _obj_pool;
    // managed by |_obj_pool|
    Expr* _cast_expr{nullptr};
    // Chunk for holding data read from the source column iterator
    Chunk _source_chunk;
    // Whether the source column's zone map can be parsed as the target type without changing the
    // value or the ordering. Computed once from the two types by the constructor.
    bool _zone_map_forwardable{false};
};

} // namespace starrocks
