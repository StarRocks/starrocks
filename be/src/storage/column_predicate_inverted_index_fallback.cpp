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

#include "storage/column_predicate_inverted_index_fallback.h"

#include "storage/column_expr_predicate.h"

namespace starrocks {

InvertedIndexFallbackPredicate::InvertedIndexFallbackPredicate(const ColumnExprPredicate* wrapped_predicate,
                                                               roaring::Roaring bitmap,
                                                               const std::vector<rowid_t>* rowid_buffer)
        : ColumnPredicate(wrapped_predicate->type_info_ptr(), wrapped_predicate->column_id()),
          _bitmap(std::move(bitmap)),
          _wrapped_predicate(wrapped_predicate),
          _rowid_buffer(rowid_buffer) {
    // Inherit properties from wrapped predicate
    set_index_filter_only(wrapped_predicate->is_index_filter_only());
    _is_expr_predicate = wrapped_predicate->is_expr_predicate();
}

Status InvertedIndexFallbackPredicate::evaluate(const Column* column, uint8_t* selection, uint16_t from,
                                                uint16_t to) const {
    // Does not support range evaluation
    DCHECK(from == 0);

    // Use bitmap for fallback evaluation
    const bool is_negated = is_negated_expr();
    const uint8_t hit_value = is_negated ? 0 : 1;
    const uint8_t miss_value = is_negated ? 1 : 0;
    roaring::BulkContext ctx;
    for (size_t i = 0; i < _rowid_buffer->size(); i++) {
        selection[i] = _bitmap.containsBulk(ctx, (*_rowid_buffer)[i]) ? hit_value : miss_value;
    }
    return Status::OK();
}

bool InvertedIndexFallbackPredicate::is_negated_expr() const {
    return _wrapped_predicate->is_negated_expr();
}

Status InvertedIndexFallbackPredicate::evaluate_and(const Column* column, uint8_t* sel, uint16_t from,
                                                    uint16_t to) const {
    // Does not support range evaluation
    DCHECK(from == 0);

    uint16_t size = to - from;
    _tmp_select.reserve(size);
    uint8_t* tmp = _tmp_select.data();
    
    RETURN_IF_ERROR(evaluate(column, tmp, 0, size));
    for (uint16_t i = 0; i < size; i++) {
        sel[i + from] &= tmp[i];
    }
    return Status::OK();
}

Status InvertedIndexFallbackPredicate::evaluate_or(const Column* column, uint8_t* sel, uint16_t from,
                                                   uint16_t to) const {
    // Does not support range evaluation
    DCHECK(from == 0);

    uint16_t size = to - from;
    _tmp_select.reserve(size);
    uint8_t* tmp = _tmp_select.data();
    
    RETURN_IF_ERROR(evaluate(column, tmp, 0, size));
    for (uint16_t i = 0; i < size; i++) {
        sel[i + from] |= tmp[i];
    }
    return Status::OK();
}

bool InvertedIndexFallbackPredicate::zone_map_filter(const ZoneMapDetail& detail) const {
    return _wrapped_predicate->zone_map_filter(detail);
}

bool InvertedIndexFallbackPredicate::support_original_bloom_filter() const {
    return _wrapped_predicate->support_original_bloom_filter();
}

bool InvertedIndexFallbackPredicate::support_ngram_bloom_filter() const {
    return _wrapped_predicate->support_ngram_bloom_filter();
}

bool InvertedIndexFallbackPredicate::ngram_bloom_filter(const BloomFilter* bf,
                                                        const NgramBloomFilterReaderOptions& reader_options) const {
    return _wrapped_predicate->ngram_bloom_filter(bf, reader_options);
}

bool InvertedIndexFallbackPredicate::can_vectorized() const {
    return _wrapped_predicate->can_vectorized();
}

Status InvertedIndexFallbackPredicate::convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                                                  ObjectPool* obj_pool) const {
    return _wrapped_predicate->convert_to(output, target_type_info, obj_pool);
}

std::string InvertedIndexFallbackPredicate::debug_string() const {
    return "InvertedIndexFallback(" + _wrapped_predicate->debug_string() + ", bitmap_size=" +
           std::to_string(_bitmap.cardinality()) + ")";
}

} // namespace starrocks
