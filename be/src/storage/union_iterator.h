// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <vector>

#include "storage/chunk_iterator.h"

namespace starrocks::vectorized {

// new_union_iterator create an iterator that will union the results of |children|.
// |children| must not empty and iterators in it must have the same schema.
ChunkIteratorPtr new_union_iterator(std::vector<ChunkIteratorPtr> children);

} // namespace starrocks::vectorized
