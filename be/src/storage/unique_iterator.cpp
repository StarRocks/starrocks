// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/aggregate_iterator.h"

namespace starrocks::vectorized {

ChunkIteratorPtr new_unique_iterator(const ChunkIteratorPtr& child) {
    return new_aggregate_iterator(child);
}

} // namespace starrocks::vectorized
