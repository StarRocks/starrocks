// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "storage/vectorized/chunk_iterator.h"

namespace starrocks::vectorized {

// A unique iterator will removes all but the *last* record from every
// consecutive group of records with equivalent keys.
ChunkIteratorPtr new_unique_iterator(const ChunkIteratorPtr& child);

} // namespace starrocks::vectorized
