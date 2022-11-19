// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <vector>

#include "storage/chunk_iterator.h"

namespace starrocks {
class Segment;

class ColumnPredicate;
class VectorizedSchema;
class SegmentReadOptions;

ChunkIteratorPtr new_segment_iterator(const std::shared_ptr<Segment>& segment, const VectorizedSchema& schema,
                                      const SegmentReadOptions& options);

} // namespace starrocks
