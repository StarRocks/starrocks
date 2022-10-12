// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "storage/chunk_iterator.h"

namespace starrocks::vectorized {

// A projection iterator will fetch chunk from its |child| iterator and choosing the columns
// specified in |schema|.
// it is undefined if a column specified in |schema| not exists in the |child|'s schema.
// the relative order of columns in |schema| can be different from that in |child|'s schema, e.g,
// |child| may return columns in the order `c1`, `c2`, `c3`, the projection iterator may return
// columns in the order `c3`, `c1`.
ChunkIteratorPtr new_projection_iterator(const Schema& schema, const ChunkIteratorPtr& child);

} //  namespace starrocks::vectorized
