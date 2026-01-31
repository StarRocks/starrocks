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

#include <optional>
#include <vector>

#include "roaring/roaring.hh"
#include "storage/column_predicate.h"
#include "storage/olap_common.h"

namespace starrocks {

class ColumnExprPredicate;

// Wrapper for ColumnExprPredicate that adds segment-specific inverted index bitmap state.
// Used for fallback evaluation of MATCH predicates in OR queries when multiple segments
// are processed concurrently by heap merge iterator without any data race conditions.
//
// This class is thread-safe because each SegmentIterator has its own instance with
// segment-specific state (_bitmap, _rowid_buffer pointer), and we use `_tmp_select`
// as a reusable buffer for evaluate_and/evaluate_or operations.
class InvertedIndexFallbackPredicate final : public ColumnPredicate {
public:
    InvertedIndexFallbackPredicate(const ColumnExprPredicate* wrapped_predicate, roaring::Roaring bitmap,
                                   const std::vector<rowid_t>* rowid_buffer);

    ~InvertedIndexFallbackPredicate() override = default;

    Status evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override;
    Status evaluate_and(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const override;
    Status evaluate_or(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const override;

    bool zone_map_filter(const ZoneMapDetail& detail) const override;
    bool support_original_bloom_filter() const override;
    bool support_ngram_bloom_filter() const override;
    bool ngram_bloom_filter(const BloomFilter* bf, const NgramBloomFilterReaderOptions& reader_options) const override;

    PredicateType type() const override { return PredicateType::kGinFallback; }
    bool can_vectorized() const override;
    
    // Fallback predicates already have their bitmap loaded, shouldn't call seek_inverted_index again
    Status seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                              roaring::Roaring* row_bitmap) const override {
        return Status::InternalError("seek_inverted_index should not be called on InvertedIndexFallbackPredicate");
    }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override;

    std::string debug_string() const override;

    // Access to the wrapped predicate
    const ColumnExprPredicate* wrapped_predicate() const { return _wrapped_predicate; }
    
    // Forward to wrapped predicate
    bool is_negated_expr() const;
    
    // Access to the segment-specific bitmap for optimization decisions
    const roaring::Roaring& get_bitmap() const { return _bitmap; }

private:
    roaring::Roaring _bitmap;                       // Segment-specific bitmap
    const ColumnExprPredicate* _wrapped_predicate;  // Shared across segments
    const std::vector<rowid_t>* _rowid_buffer;      // Pointer to SegmentIterator's rowid buffer
    mutable std::vector<uint8_t> _tmp_select;       // Reusable buffer for evaluate_and/evaluate_or
};

} // namespace starrocks
