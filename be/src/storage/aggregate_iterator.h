// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "storage/chunk_iterator.h"
#include "storage/row_source_mask.h"

namespace starrocks::vectorized {

// new_aggregate_iterator create an aggregate iterator based on chunk aggregator.
// the rows from child iterator is sorted by heap merge.
//
// |factor| aggregate factor.
ChunkIteratorPtr new_aggregate_iterator(ChunkIteratorPtr child, int factor = 0);

// new_aggregate_iterator create an aggregate iterator based on chunk aggregator for vertical compaction.
// the rows from child iterator is sorted by heap merge (key columns) or mask merge (value columns).
//
// |is_key| chunk schema is key columns or value columns.
ChunkIteratorPtr new_aggregate_iterator(ChunkIteratorPtr child, bool is_key);

} // namespace starrocks::vectorized
