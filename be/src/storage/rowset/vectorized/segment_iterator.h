// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <memory>
#include <vector>

#include "storage/vectorized/chunk_iterator.h"

namespace starrocks::segment_v2 {
class Segment;
}

namespace starrocks::vectorized {

class ColumnPredicate;
class Schema;
class SegmentReadOptions;

ChunkIteratorPtr new_segment_iterator(const std::shared_ptr<segment_v2::Segment>& segment,
                                      const vectorized::Schema& schema, const SegmentReadOptions& options);

} // namespace starrocks::vectorized
