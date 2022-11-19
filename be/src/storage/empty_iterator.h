// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "storage/chunk_iterator.h"

namespace starrocks {

ChunkIteratorPtr new_empty_iterator(VectorizedSchema&& schema, int chunk_size);
ChunkIteratorPtr new_empty_iterator(const VectorizedSchema& schema, int chunk_size);

} // namespace starrocks
