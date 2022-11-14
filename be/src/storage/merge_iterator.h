// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <vector>

#include "storage/chunk_iterator.h"
#include "storage/row_source_mask.h"

namespace starrocks::vectorized {

// new_heap_merge_iterator create a sorted iterator based on merge-sort algorithm.
// the order of rows is determined by the key columns.
// if two rows compared equal, their order is determinate by the index of the source iterator
// in the vector |children|. the one with a lower index will come first.
// if |children| has only one element, the element will be returned directly.
//
// REQUIRES:
//  - |children| not empty.
//  - |children| have the same schemas.
//  - |children| are sorted iterators, i.e, each iterator in |children|
//    should return rows in an ascending order based on the key columns.
// one typical usage of this iterator is merging rows of the segments in the same `rowset`.
//
ChunkIteratorPtr new_heap_merge_iterator(const std::vector<ChunkIteratorPtr>& children);

// new_mask_merge_iterator create a merge iterator based on source masks.
// the order of rows is determined by mask sequence.
ChunkIteratorPtr new_mask_merge_iterator(const std::vector<ChunkIteratorPtr>& children,
                                         RowSourceMaskBuffer* mask_buffer);

} // namespace starrocks::vectorized
