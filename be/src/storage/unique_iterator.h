// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "storage/chunk_iterator.h"

namespace starrocks::vectorized {

// A unique iterator will removes all but the *last* record from every
// consecutive group of records with equivalent keys.
ChunkIteratorPtr new_unique_iterator(const ChunkIteratorPtr& child);

} // namespace starrocks::vectorized
