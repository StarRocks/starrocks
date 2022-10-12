// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <vector>

#include "storage/chunk_iterator.h"

namespace starrocks {
class Segment;

namespace vectorized {

class ColumnPredicate;
class Schema;
class SegmentReadOptions;

ChunkIteratorPtr new_segment_iterator(const std::shared_ptr<Segment>& segment, const vectorized::Schema& schema,
                                      const SegmentReadOptions& options);
} // namespace vectorized

} // namespace starrocks
